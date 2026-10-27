"""Native gap evidence to durable, open cutoff-range corrections.

A caller reports the raw discovery ID; replay independently derives the last
continuous native proof and the range. Only downgrades are emitted here. Recovery
closures, clock faults, owner transitions and adapter scheduling are separate.
"""
from dataclasses import asdict, dataclass
from types import SimpleNamespace

from .books import SnapshotBook
from .bootstrap import BufferedBook
from .contracts import RawRecord, Receipt, canonical, digest, integer, sha256, strict_json
from .cutoffs import CutoffCoordinator
from .decoders import decode_seed
from .ledger import DurableLedger
from .replay import SnapshotPlan, _book_message
from .validity import GapCorrection, LedgerEntry, LedgerPin

SCHEMA='dc.native-gap-mapping/2'
RULE='barrier-after-last-native-proof/1'


@dataclass(frozen=True, slots=True)
class GapMapping:
    mapping_record_id: str
    mapping_sha256: str
    correction_id: str
    first_cutoff_id: int
    ledger_pin: LedgerPin


def mapping_payload(value,record_id):
    """Shared exact relation between an audit proof and its negative ledger entry."""
    from .contracts import InstrumentKey
    from .validity import FeedTarget
    target=dict(value['target']);target['instrument']=InstrumentKey(**target['instrument'])
    return GapCorrection(value['scope'],FeedTarget(**target),value['first_cutoff_id'],None,
        value['discovery_record_id'],record_id,value['reason'],value['status'])


def _boundary(record):
    ordinal=strict_json(record.native_fields_json).get('collector_ingress_ordinal')
    if type(ordinal) is not int or not 0<=ordinal<2**63-1:return None
    return dict(process_session_id=record.receipt.process_session_id,clock_epoch=record.receipt.clock_epoch,
        monotonic_ns=record.receipt.monotonic_ns,ordinal=ordinal+1)


def _first_affected(coordinator,proof):
    """An exact post-message boundary is safe; every strictly later quiet barrier is not."""
    if proof is None:return 0,False
    p=_boundary(proof)
    if p is None:return 0,False
    point=(p['monotonic_ns'],p['ordinal'])
    for identity,allocation in coordinator.allocations.items():
        resolution=coordinator.resolutions.get(identity)
        body=allocation['payload'];anchor=body['clock_anchor']
        if anchor is None or anchor['process_session_id']!=p['process_session_id']:return identity,False
        if resolution is None or 'barrier' not in resolution['payload']:
            if anchor['clock_epoch']!=p['clock_epoch']:return identity,False
            if body['deadline_ns']>=point[0]:return identity,False
            continue
        barrier=resolution['payload']['barrier']
        anchors=barrier['epoch_anchors'];epochs={a['clock_epoch']:i for i,a in enumerate(anchors)}
        index=epochs.get(p['clock_epoch'])
        if index is None:return identity,False
        start=(anchors[index]['epoch_started_ns'],anchors[index]['epoch_started_ordinal'])
        end=None if index+1==len(anchors) else (anchors[index+1]['epoch_started_ns'],anchors[index+1]['epoch_started_ordinal'])
        raw_point=(p['monotonic_ns'],p['ordinal']-1)
        if raw_point<start or (end is not None and raw_point>=end):return identity,False
        if (barrier['deadline_ns'],barrier['ordinal'])>point:return identity,True
    return len(coordinator.allocations),True


def map_native_gap(ledger,coordinator,plan,binding_index,discovery_record_id,*,utc_ns,monotonic_ns,clock_epoch,
                   max_records=200000,max_scan_bytes=256*1024*1024,max_dependencies=256,max_mapping_bytes=65536):
    """Serialized disk-owner call; audit fsync then ledger fsync before return.

    Retrying an admitted request retains its first audit timestamp and original
    cutoff knowledge. Unknown receipt/epoch mappings widen the open range; they
    do not make a positive claim. All evidence is already durable on entry.
    """
    if (not isinstance(ledger,DurableLedger) or not isinstance(coordinator,CutoffCoordinator) or
            ledger.journal is not coordinator.journal or ledger.scope!=coordinator.scope or
            ledger.capture_id!=coordinator.capture_id or ledger.session!=coordinator.session):
        raise ValueError('same serialized cutoff and ledger owners required')
    if not isinstance(plan,SnapshotPlan) or len(plan.bindings)>256:raise ValueError('bounded typed gap snapshot plan required')
    from .publisher import _charge
    _charge(plan,1024*1024)
    integer(binding_index,'gap binding index');sha256(discovery_record_id,'gap discovery')
    if binding_index>=len(plan.bindings):raise ValueError('gap binding absent from plan')
    for name,value in [('records',max_records),('scan bytes',max_scan_bytes),('dependencies',max_dependencies),('mapping bytes',max_mapping_bytes)]:integer(value,name,1)
    binding=plan.bindings[binding_index];target=binding.target
    sources={binding.ws_source}|({binding.seed_source} if binding.seed_source else set())
    if (any(s.capture_id!=ledger.capture_id or s.process_session_id!=binding.ws_source.process_session_id for s in sources) or
            (target.instrument.venue,target.instrument.environment)!=ledger.journal.identity):
        raise ValueError('same-process native owner sources and venue required')
    request_id=digest(canonical(dict(scope=ledger.scope,ledger_id=ledger.ledger_id,plan=asdict(plan),
        binding_index=binding_index,discovery_record_id=discovery_record_id)))
    def all_records():
        count=size=0
        for record in ledger._all_records():
            count+=1;size+=len(record.to_bytes())+64
            if count>max_records or size>max_scan_bytes:raise ValueError('native gap scan bound; checkpoint required')
            yield record
    existing=None;next_seq=0;known_negatives=set()
    connection='control:gap-mapping:'+ledger.scope;channel='gap_mapping:'+ledger.scope
    for record in all_records():
        if record.channel==channel:
            if record.receipt.process_session_id==ledger.session:next_seq=record.receipt.receive_seq+1
            value=strict_json(record.body)
            if value.get('schema')==SCHEMA and value.get('target')==asdict(target):
                known_negatives.add(value['discovery_record_id'])
            if value.get('request_id')==request_id:
                if existing is not None:raise ValueError('duplicate mapping request identity')
                existing=record
    def prefix():
        for record in all_records():
            if existing is not None and record.record_id==existing.record_id:return
            yield record
        if existing is not None:raise ValueError('mapping prefix changed')
    empty=SimpleNamespace(identity=ledger.journal.identity,limits=ledger.journal.limits,
        encoded_records=lambda **kwargs:iter(()))
    restored=CutoffCoordinator(empty,capture_scope=ledger.scope,capture_id=ledger.capture_id,
        process_session_id=ledger.session,provenance=ledger.provenance,prior_records=prefix,
        max_cutoffs=coordinator.max_cutoffs,max_control_bytes=coordinator.max_control_bytes,
        max_projection_bytes=coordinator.max_projection_bytes)
    if existing is None and restored.pin!=coordinator.pin:raise ValueError('cutoff owner differs from durable gap prefix')
    dependencies={};model=None;proof=None;discovery=None;reason=None
    def depend(record):
        if record.record_id not in dependencies:
            if len(dependencies)>=max_dependencies:raise ValueError('gap raw dependency bound; checkpoint required')
            dependencies[record.record_id]=dict(record_id=record.record_id,raw_sha256=digest(record.to_bytes()))
    for record in prefix():
        if record.channel=='cutoffs:'+ledger.scope:depend(record)
    positions={};last_mono=None
    for record in prefix():
        r=record.receipt
        if r.stream_key not in sources:continue
        start=binding.ws_start_seq if r.stream_key==binding.ws_source else binding.seed_start_seq
        if r.receive_seq<start:continue
        if r.receive_seq!=positions.get(r.stream_key,start-1)+1:raise ValueError('native owner receipt prefix missing')
        positions[r.stream_key]=r.receive_seq
        if last_mono is not None and r.monotonic_ns<last_mono:raise ValueError('native owner monotonic order invalid')
        last_mono=r.monotonic_ns
        if record.provenance.config_sha256!=plan.config_sha256:raise ValueError('native gap plan/source config differs')
        depend(record)
        if not ((r.stream_key==binding.seed_source and record.kind=='rest_response') or
                (r.stream_key==binding.ws_source and record.kind=='ws_message')):
            if record.record_id==discovery_record_id:raise ValueError('native data discovery required')
            continue
        error=None;event=None
        if r.stream_key==binding.ws_source:
            try:event=_book_message(record,binding)
            except ValueError as exc:error=exc
            if event is None and error is None:
                if record.record_id==discovery_record_id:raise ValueError('control delivery cannot prove a native gap')
                continue
        elif record.instrument is not None and record.instrument!=target.instrument:
            if record.record_id==discovery_record_id:raise ValueError('native discovery belongs to another instrument')
            continue
        if model is None:
            model=(SnapshotBook(target.instrument,target.profile,depth=binding.depth,signed_prices=binding.signed_prices)
                if target.profile in {'deribit_full','bybit_standard'} else BufferedBook(target.instrument,target.profile,now_ns=r.monotonic_ns))
        failed_exception=False
        try:
            if error is not None:raise error
            if record.instrument is not None and record.instrument!=target.instrument:raise ValueError('routed native instrument differs')
            if r.stream_key==binding.seed_source:
                outcome=model.supply_seed(decode_seed(record,target.profile),now_ns=r.monotonic_ns)
            else:outcome=model.receive(event) if isinstance(model,SnapshotBook) else model.receive(event,now_ns=r.monotonic_ns)
        except ValueError as exc:
            failed_exception=True
            outcome='invalid';reason=str(exc).encode()[:256].decode(errors='ignore')
            model.invalidate('native_decode_or_apply_failed')
        if outcome=='control':
            if record.record_id==discovery_record_id:raise ValueError('control delivery cannot prove a native gap')
            continue
        if outcome=='invalid':
            if record.record_id!=discovery_record_id:
                if record.record_id not in known_negatives:raise ValueError('earlier native gap must be mapped first')
                if isinstance(model,BufferedBook):
                    model.reset(now_ns=r.monotonic_ns,reason='native_reseed_required')
                    if not failed_exception and r.stream_key==binding.ws_source:model.receive(event,now_ns=r.monotonic_ns)
                reason=None;continue
            discovery=record;reason=reason or model.reason or 'native_continuity_invalid';break
        if model.state=='live' and outcome in {'applied','live'} and target.profile!='bybit_standard':proof=record
        if record.record_id==discovery_record_id:raise ValueError('reported record does not establish a native gap')
    if discovery is None:raise ValueError('durable native gap discovery unavailable')
    first,supported=_first_affected(restored,proof)
    body=canonical(dict(schema=SCHEMA,rule=RULE,scope=ledger.scope,request_id=request_id,target=asdict(target),
        plan_sha256=digest(canonical(asdict(plan))),binding_index=binding_index,discovery_record_id=discovery.record_id,
        native_proof_record_id=None if proof is None else proof.record_id,
        proof_boundary=None if proof is None else _boundary(proof),cutoff_pin=asdict(restored.pin),
        first_cutoff_id=first,recovery_cutoff_id=None,status='invalid' if supported else 'unknown',
        reason=reason,proof_receive_utc_ns=None if proof is None else proof.receipt.receive_utc_ns,
        discovery_receive_utc_ns=discovery.receipt.receive_utc_ns,utc_interval='not_inferred',
        raw_dependencies=list(dependencies.values())))
    if len(body)>min(max_mapping_bytes,65536):raise ValueError('gap mapping metadata bound; checkpoint required')
    if existing is not None:
        if existing.body!=body:raise ValueError('mapping audit differs from native/barrier replay')
        marker=existing
    else:
        marker=RawRecord(Receipt(ledger.capture_id,ledger.session,connection,next_seq,utc_ns,monotonic_ns,clock_epoch),
            *ledger.journal.identity,'control',channel,'coverage_event',body,ledger.provenance)
        if len(marker.to_bytes())>ledger.journal.limits.max_record_bytes:raise ValueError('gap audit record byte bound')
    correction=mapping_payload(strict_json(body),marker.record_id)
    if existing is None:
        ledger.initialize_native_mapping(utc_ns=utc_ns,monotonic_ns=monotonic_ns,clock_epoch=clock_epoch)
        ledger.journal.append(marker)
    # A pending audit is durable negative knowledge. Only its exact correction
    # may complete it; public ledger pins/assessments stay blocked meanwhile.
    for record in all_records():
        if record.kind=='coverage_event' and record.channel==ledger.channel:
            entry=LedgerEntry.from_bytes(record.body)
            if isinstance(entry.payload,GapCorrection) and entry.payload.correction_id==correction.correction_id:
                return GapMapping(marker.record_id,digest(marker.to_bytes()),correction.correction_id,first,ledger.recovery_state()[0])
    ledger._append(correction,utc_ns=utc_ns,monotonic_ns=monotonic_ns,clock_epoch=clock_epoch,_completing_mappings=True)
    pin=ledger.recovery_state()[0] # continuity tip only; public certification replays all raw discoveries
    return GapMapping(marker.record_id,digest(marker.to_bytes()),correction.correction_id,first,pin)
