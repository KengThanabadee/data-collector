"""Bounded local book snapshot materialization from committed receive-asof/2 data.

One local closure and one explicit owner path per binding. Owner transitions,
trade/reference projections, checkpoint acceleration and publication/export are
separate integration steps. No current network state participates in replay.
"""
from dataclasses import asdict,dataclass,replace
from pathlib import Path
import tempfile

from .books import SnapshotBook
from .bootstrap import BufferedBook
from .contracts import RawRecord,StreamKey,canonical,digest,integer,sha256,strict_json
from .cutoffs import CutoffCoordinator
from .decoders import decode_depth,decode_seed,native_json
from .journal import Journal
from .ledger import DurableLedger
from .metadata import MetadataHistory,MetadataVersion
from .segments import iter_segment_records
from .validity import FeedTarget,LedgerPin,ValidityLedger


class ReplayResourceError(ValueError):
    pass


@dataclass(frozen=True)
class BookBinding:
    target: FeedTarget
    ws_source: StreamKey
    metadata_source: StreamKey
    metadata_version_id: str
    seed_source: StreamKey | None = None
    depth: int | None = None
    ws_start_seq: int = 0
    seed_start_seq: int = 0
    signed_prices: bool = False
    channel: str | None = None

    def __post_init__(self):
        if not isinstance(self.target,FeedTarget) or not isinstance(self.ws_source,StreamKey) or not isinstance(self.metadata_source,StreamKey):
            raise ValueError('typed target and owner/dependency streams required')
        sha256(self.metadata_version_id,'metadata_version_id')
        if self.seed_source is not None and not isinstance(self.seed_source,StreamKey):raise ValueError('typed REST seed stream required')
        for name in ('ws_start_seq','seed_start_seq'):integer(getattr(self,name),name)
        if self.depth is not None:integer(self.depth,'depth',1)
        if type(self.signed_prices) is not bool:raise ValueError('explicit price convention required')
        self.native_channel

    @property
    def native_channel(self):
        symbol=self.target.instrument.native_symbol;profile=self.target.profile
        if profile=='deribit_full':choices={f'book.{symbol}.100ms',f'book.{symbol}.agg2'};default=f'book.{symbol}.100ms'
        elif profile=='bybit_standard':choices={f'orderbook.{self.depth}.{symbol}'};default=next(iter(choices))
        elif profile=='bybit_full':choices={f'orderbook.full.{symbol}'};default=next(iter(choices))
        elif profile in {'binance_spot','binance_um'}:choices={symbol.lower()+'@depth',symbol.lower()+'@depth@100ms'};default=symbol.lower()+'@depth'
        else:raise ValueError('unresolved native book profile')
        selected=default if self.channel is None else self.channel
        if selected not in choices:raise ValueError('channel differs from resolved book profile')
        return selected


@dataclass(frozen=True)
class SnapshotPlan:
    bindings: tuple[BookBinding,...]
    config_sha256: str

    def __post_init__(self):
        sha256(self.config_sha256,'config_sha256')
        if type(self.bindings) is not tuple or not self.bindings or any(not isinstance(b,BookBinding) for b in self.bindings):
            raise ValueError('immutable book bindings required')
        if len({(b.target.instrument,b.target.profile) for b in self.bindings})!=len(self.bindings):
            raise ValueError('duplicate target/profile in snapshot plan')

    @property
    def dependencies(self):
        instruments={b.target.instrument.key_id:asdict(b.target.instrument) for b in self.bindings}
        return dict(config=self.config_sha256,required_feeds=digest(canonical(asdict(self))),
            universe=digest(canonical([instruments[k] for k in sorted(instruments)])),
            metadata=digest(canonical(sorted({b.metadata_version_id for b in self.bindings}))))


@dataclass(frozen=True)
class ReplayLimits:
    max_segments: int = 1024
    max_records: int = 200000
    max_scan_bytes: int = 256*1024*1024
    max_books: int = 256
    max_total_levels: int = 200000
    max_payload_bytes: int = 16*1024*1024
    max_metadata_bytes: int = 8*1024*1024
    max_diagnostic_bytes: int = 65536

    def __post_init__(self):
        for name,value in asdict(self).items():integer(value,name,1)


def _book_message(record,binding):
    value=native_json(record.body)
    profile=binding.target.profile;symbol=binding.target.instrument.native_symbol
    if 'error' in value or value.get('success') is False:raise ValueError('native subscription/control error')
    if profile=='deribit_full':
        if value.get('method')!='subscription':
            if 'id' in value and 'result' in value and 'params' not in value:return None
            if value.get('method')=='heartbeat':return None
            raise ValueError('unrecognized or malformed Deribit book/control envelope')
        params=value.get('params')
        if not isinstance(params,dict):raise ValueError('malformed Deribit subscription')
        channel=params.get('channel')
        if not isinstance(channel,str):raise ValueError('missing native channel')
        if record.instrument is not None and channel.startswith('book.') and not channel.startswith('book.'+record.instrument.native_symbol+'.'):
            raise ValueError('native channel contradicts explicit raw instrument')
        if channel!=binding.native_channel:return None
        if not isinstance(params.get('data'),dict) or params['data'].get('instrument_name')!=symbol:
            raise ValueError('malformed or contradictory symbol on bound Deribit book')
    elif profile.startswith('bybit'):
        channel=value.get('topic')
        if channel is None:
            if value.get('op') in {'subscribe','unsubscribe','ping','pong'} and 'data' not in value:return None
            raise ValueError('unrecognized or malformed Bybit book/control envelope')
        if not isinstance(channel,str):raise ValueError('malformed native topic')
        if record.instrument is not None and channel.startswith('orderbook.') and not channel.endswith('.'+record.instrument.native_symbol):
            raise ValueError('native channel contradicts explicit raw instrument')
        if channel!=binding.native_channel:return None
        data=value.get('data')
        if not isinstance(data,dict):raise ValueError('malformed native book data')
        if data.get('s')!=symbol:raise ValueError('malformed or contradictory symbol on bound Bybit book')
    else:
        if 'id' in value and 'result' in value:return None
        channel=value.get('stream',record.channel)
        data=value.get('data',value)
        if not isinstance(data,dict):raise ValueError('malformed Binance combined depth')
        if data.get('e')=='depthUpdate' and data.get('s')==symbol and channel!=binding.native_channel:
            if not isinstance(channel,str) or not channel.startswith(symbol.lower()+'@depth'):
                raise ValueError('native depth has no matching channel identity')
        if channel!=binding.native_channel:return None
        if data.get('e')!='depthUpdate' or data.get('s')!=symbol:raise ValueError('malformed or contradictory symbol on bound Binance book')
    if record.instrument is not None and record.instrument!=binding.target.instrument:
        raise ValueError('native book conflicts with explicit raw instrument identity')
    if (record.venue,record.environment,record.native_product)!=(
            binding.target.instrument.venue,binding.target.instrument.environment,binding.target.instrument.native_product):
        raise ValueError('native book namespace differs from binding')
    return decode_depth(replace(record,instrument=binding.target.instrument,channel=channel),profile,depth=binding.depth)


def materialize_snapshot(segments,close_record_id,plan:SnapshotPlan,*,limits=ReplayLimits()):
    return _materialize_snapshot(segments,close_record_id,plan,limits=limits)[0]


def _materialize_snapshot(segments,close_record_id,plan:SnapshotPlan,*,limits=ReplayLimits()):
    """Artifact plus transient pre-correction validity; legacy artifact bytes stay fixed."""
    sha256(close_record_id,'close_record_id')
    if not isinstance(plan,SnapshotPlan) or len(plan.bindings)>limits.max_books:raise ValueError('bounded typed snapshot plan required')
    paths=[]
    for path in segments:
        if len(paths)>=limits.max_segments:raise ValueError('bounded distinct committed segment paths required')
        paths.append(Path(path))
    if not paths or len(set(paths))!=len(paths):raise ValueError('bounded distinct committed segment paths required')
    used=[];close=None;count=size=0
    # Stop dependencies at the segment containing the durable close. Adding later
    # segments must not alter this snapshot's identity or admission membership.
    for path in paths:
        used.append(path)
        for record in iter_segment_records(path):
            count+=1;size+=len(record.to_bytes())
            if count>limits.max_records or size>limits.max_scan_bytes:raise ValueError('snapshot prefix scan bound')
            if record.record_id==close_record_id:
                close=record;break
        if close is not None:break
    if close is None or close.kind!='control':raise ValueError('committed cutoff close unavailable')
    control=strict_json(close.body)
    if control.get('schema')!='dc.cutoff-control/2' or control.get('kind')!='closed':raise ValueError('durable closed cutoff required')
    scope=control['scope'];cutoff_id=control['payload']['cutoff_id']
    def prefix():
        n=total=0
        for path in used:
            for record in iter_segment_records(path):
                n+=1;total+=len(record.to_bytes())
                if n>limits.max_records or total>limits.max_scan_bytes:raise ValueError('snapshot prefix scan bound')
                yield record
                if record.record_id==close_record_id:return
        raise ValueError('closed prefix changed during replay')
    with tempfile.TemporaryDirectory(prefix='dc-replay-') as directory:
        with Journal(Path(directory)/'reader.wal',venue=close.venue,environment=close.environment,create=True) as empty:
            coordinator=CutoffCoordinator(empty,capture_scope=scope,capture_id=close.receipt.capture_id,
                process_session_id='replay-reader',provenance=close.provenance,prior_records=prefix)
            allocated=coordinator.allocations[cutoff_id]['payload']
            owner_dependencies=()
            for record in prefix():
                if record.channel=='owner_plans:'+scope or record.receipt.connection_id=='control:owners:'+scope:
                    from .owners import OwnerPlans
                    owners=OwnerPlans(empty,capture_scope=scope,capture_id=close.receipt.capture_id,
                        process_session_id=close.receipt.process_session_id,provenance=close.provenance,prior_records=prefix,
                        max_records=limits.max_records,max_scan_bytes=limits.max_scan_bytes,
                        max_plan_bytes=limits.max_payload_bytes,max_metadata_bytes=limits.max_metadata_bytes)
                    owner_dependencies=owners.proof_for(cutoff_id,plan)
                    break
                if record.record_id==coordinator.allocations[cutoff_id]['record_id']:break
            closed=coordinator.resolutions[cutoff_id]
            if closed['record_id']!=close_record_id or closed['hash']!=digest(close.to_bytes()):raise ValueError('cutoff closure dependency mismatch')
            if 'clock_binding' in closed['payload']['barrier']:
                from .clockcutoffs import verify_clock_binding
                verify_clock_binding(prefix,close,max_records=limits.max_records,max_scan_bytes=limits.max_scan_bytes)
            if allocated['dependencies']!=plan.dependencies:raise ValueError('plan differs from allocated dependency versions')
            pin=LedgerPin(**closed['payload']['ledger_pin'])
            ledger=DurableLedger(empty,ledger_id=pin.ledger_id,capture_scope=scope,capture_id=close.receipt.capture_id,
                process_session_id='replay-reader',provenance=close.provenance,prior_records=prefix,
                max_scan_records=limits.max_records,max_scan_bytes=limits.max_scan_bytes,expected_pin=pin)
            if ledger.pin()!=pin:raise ValueError('close pin excludes corrections already durable before closure')
            ledger_dependencies=[asdict(d) for d in ledger.dependencies(pin)]
            known_ledger=ValidityLedger(pin.ledger_id)
            for entry in ledger.entries(pin):known_ledger.add(entry)
    admitted={StreamKey(**row['stream']):row for row in closed['payload']['streams']}
    models=[];diagnostics=[dict(count=0,first_reason=None,first_record_id=None,last_record_id=None) for _ in plan.bindings]
    diagnostic_bytes=0
    def note(i,reason,record_id):
        nonlocal diagnostic_bytes
        row=diagnostics[i];row['count']+=1;row['last_record_id']=record_id
        if row['first_reason'] is None:
            bounded=str(reason).encode('utf-8')[:512].decode('utf-8','ignore')
            diagnostic_bytes+=len(bounded.encode('utf-8'))+128
            if diagnostic_bytes>limits.max_diagnostic_bytes:raise ReplayResourceError('replay diagnostic projection bound')
            row['first_reason']=bounded;row['first_record_id']=record_id
    for binding in plan.bindings:
        sources=[binding.ws_source,binding.metadata_source]+([binding.seed_source] if binding.seed_source else [])
        if any(s not in admitted for s in sources):raise ValueError('owner/metadata/seed source omitted from closure')
        if binding.target.instrument.venue!=close.venue or binding.target.instrument.environment!=close.environment:
            raise ValueError('aggregate multi-venue closure requires separate local materializations')
        if binding.target.profile in {'deribit_full','bybit_standard'}:
            if binding.seed_source is not None:raise ValueError('snapshot feed cannot inherit an unrelated REST seed')
            model=SnapshotBook(binding.target.instrument,binding.target.profile,depth=binding.depth,signed_prices=binding.signed_prices)
        else:
            if binding.seed_source is None or binding.signed_prices:raise ValueError('buffered native book needs an explicit seed source and unsigned prices')
            model=None # first archived receipt supplies the local monotonic origin
        models.append(model)
    history=MetadataHistory();superseded_metadata=set();metadata_bytes=0;positions={};last_hashes={}
    for record in prefix():
        receipt=record.receipt;stream=receipt.stream_key
        if stream not in admitted or admitted[stream]['admitted_seq'] is None or receipt.receive_seq>admitted[stream]['admitted_seq']:continue
        if receipt.receive_seq!=positions.get(stream,-1)+1:raise ValueError('admitted raw prefix missing or out of order')
        positions[stream]=receipt.receive_seq;last_hashes[stream]=digest(record.to_bytes())
        if record.provenance.config_sha256!=plan.config_sha256:raise ValueError('admitted source config differs from allocated version')
        if record.kind=='metadata_version' and record.instrument is not None:
            metadata_bytes+=len(record.body)
            if metadata_bytes>limits.max_metadata_bytes:raise ValueError('metadata projection byte bound')
            value=strict_json(record.body)
            if set(value)!={'schema','effective_at_ns','attributes','supersedes'} or value['schema']!='dc.metadata/2':raise ValueError('unknown metadata schema')
            history.add(MetadataVersion(record.instrument,receipt,value['effective_at_ns'],canonical(value['attributes']),value['supersedes']))
            if value['supersedes'] is not None:superseded_metadata.add(value['supersedes'])
        for i,binding in enumerate(plan.bindings):
            model=models[i]
            if stream not in {binding.ws_source,binding.seed_source}:continue
            if stream==binding.ws_source and receipt.receive_seq<binding.ws_start_seq:continue
            if stream==binding.seed_source and receipt.receive_seq<binding.seed_start_seq:continue
            routed_event=None;routing_error=None
            if owner_dependencies:
                # Managed streams may multiplex symbols; unrelated input cannot
                # start the bootstrap clock or invalidate this selected owner.
                if stream==binding.ws_source and record.kind=='ws_message':
                    try:routed_event=_book_message(record,binding)
                    except ValueError as exc:routing_error=exc
                    if routed_event is None and routing_error is None:continue
                elif stream==binding.seed_source and record.kind=='rest_response':
                    if record.instrument is not None and record.instrument!=binding.target.instrument:continue
                else:continue
            if model is None:
                model=BufferedBook(binding.target.instrument,binding.target.profile,now_ns=receipt.monotonic_ns)
                models[i]=model
            try:
                if routing_error is not None:raise routing_error
                if stream==binding.seed_source and record.kind=='rest_response':
                    outcome=model.supply_seed(decode_seed(record,binding.target.profile),now_ns=receipt.monotonic_ns)
                    if outcome=='invalid':
                        note(i,model.reason,record.record_id)
                        model.reset(now_ns=receipt.monotonic_ns,reason='native_reseed_required')
                elif stream==binding.ws_source and record.kind=='ws_message':
                    event=routed_event if owner_dependencies else _book_message(record,binding)
                    if event is not None:
                        if isinstance(model,BufferedBook) and model.state=='no_book':model.reset(now_ns=receipt.monotonic_ns,reason='native_availability_recheck')
                        outcome=model.receive(event) if isinstance(model,SnapshotBook) else model.receive(event,now_ns=receipt.monotonic_ns)
                        if outcome=='invalid':
                            note(i,model.reason,record.record_id)
                            if isinstance(model,BufferedBook):
                                model.reset(now_ns=receipt.monotonic_ns,reason='native_reseed_required')
                                model.receive(event,now_ns=receipt.monotonic_ns)
            except ReplayResourceError:
                raise
            except ValueError as exc:
                note(i,str(exc),record.record_id)
                model.invalidate('native_decode_or_apply_failed')
                if isinstance(model,BufferedBook):model.reset(now_ns=receipt.monotonic_ns,reason='native_reseed_required')
        if sum(m.book.level_count for m in models if m is not None)>limits.max_total_levels:raise ValueError('aggregate L2 projection bound')
    applied=[]
    for stream,row in admitted.items():
        if row['admitted_seq'] is not None and (positions.get(stream)!=row['admitted_seq'] or last_hashes.get(stream)!=row['record_hash']):
            raise ValueError('applied position/hash does not equal durable admission')
        applied.append(dict(stream=asdict(stream),receive_seq=positions.get(stream)))
    books=[];base_validities=[]
    for binding,model,diagnostic in zip(plan.bindings,models,diagnostics):
        metadata=history.at_version(binding.metadata_version_id,positions)
        if metadata.instrument!=binding.target.instrument or metadata.receipt.stream_key!=binding.metadata_source:raise ValueError('metadata target/source mismatch')
        if metadata.effective_at_ns is not None and metadata.effective_at_ns>allocated['utc_ns']:raise ValueError('metadata is not effective at cutoff')
        view=model.view() if model is not None else dict(state='bootstrapping',reason='no_native_book')
        reasons=[] if view['state']=='live' else [view.get('reason') or view['state']]
        status=closed['payload']['validity']
        if view['state']!='live':status='invalid' if status!='missing' else 'missing'
        if metadata.version_id in superseded_metadata:
            status='invalid' if status!='missing' else 'missing';reasons.append('metadata_superseded_since_plan')
        if metadata.effective_at_ns is not None:
            try:latest=history.known_at(binding.target.instrument,positions,effective_cutoff_ns=allocated['utc_ns'])
            except ValueError:latest=None
            if latest is None or latest.version_id!=metadata.version_id:
                status='invalid' if status!='missing' else 'missing';reasons.append('metadata_changed_since_plan')
        base_validities.append(status)
        status=known_ledger.assess(scope,binding.target,cutoff_id,status,pin)['assessed_validity']
        books.append(dict(target=asdict(binding.target),metadata_version=metadata.version_id,metadata=metadata.attributes,
                          metadata_effective_at_ns=metadata.effective_at_ns,
                          validity_as_published=status,reasons=reasons,replay_diagnostics=diagnostic,book=view))
    payload=canonical(dict(books=books))
    if len(payload)>limits.max_payload_bytes:raise ValueError('snapshot payload byte bound')
    descriptor=dict(schema='dc.snapshot/2',policy='receive-asof/2',capture_scope=scope,cutoff_id=cutoff_id,
        utc_ns=allocated['utc_ns'],close_record_id=close_record_id,close_sha256=digest(close.to_bytes()),
        ledger_pin=asdict(pin),admitted_positions=closed['payload']['streams'],applied_positions=applied,
        plan=asdict(plan),dependencies=allocated['dependencies'],ledger_raw_dependencies=ledger_dependencies,
        raw_segments=[dict(context_sha256=strict_json((p/'manifest.json').read_bytes())['context_sha256'],
                           manifest_sha256=digest((p/'manifest.json').read_bytes())) for p in used],
        payload_sha256=digest(payload))
    if owner_dependencies:descriptor['owner_plan_dependencies']=list(owner_dependencies)
    identity=digest(canonical(descriptor))
    return dict(snapshot_id=identity,descriptor=descriptor,payload=payload),tuple(base_validities)
