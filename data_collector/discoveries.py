"""Bounded native failures recoverable from managed owner declarations and raw.

No new journal/schema: negative knowledge comes from existing immutable input.
Legacy paths without an owner declaration retain their original replay contract.
"""
from dataclasses import asdict,dataclass

from .books import SnapshotBook
from .bootstrap import BufferedBook
from .contracts import canonical,integer
from .decoders import decode_seed
from .owners import OwnerPlans,_path
from .replay import SnapshotPlan,_book_message


@dataclass(frozen=True,slots=True)
class NativeDiscovery:
    plan: SnapshotPlan
    binding_index: int
    record_id: str


def pending_native_discoveries(ledger,*,max_paths=10000,max_levels=200000,
                               max_buffer_events=4096,max_buffer_bytes=16*1024*1024):
    for name,value in (('paths',max_paths),('levels',max_levels),
                       ('buffer events',max_buffer_events),('buffer bytes',max_buffer_bytes)):
        integer(value,name,1)
    owners=OwnerPlans(ledger.journal,capture_scope=ledger.scope,capture_id=ledger.capture_id,
        process_session_id=ledger.session,provenance=ledger.provenance,prior_records=ledger.prior_records,
        max_records=ledger.max_scan_records,max_scan_bytes=ledger.max_scan_bytes,max_plans=max_paths)
    declarations={p.record_id:p.plan for p in owners.plans}
    if not declarations:return ()
    audited={(canonical(value['target']),value['discovery_record_id']) for value in ledger._mappings.values()
        if value.get('schema')=='dc.native-gap-mapping/2'}
    paths={};pending=[]
    for record in owners._scan_records():
        if record.record_id in declarations:
            plan=declarations[record.record_id]
            for index,binding in enumerate(plan.bindings):
                snapshot=binding.target.profile in {'deribit_full','bybit_standard'}
                if snapshot and binding.seed_source is not None:raise ValueError('snapshot owner cannot inherit REST seed')
                if not snapshot and (binding.seed_source is None or binding.signed_prices):
                    raise ValueError('buffered native book needs an explicit seed source and unsigned prices')
                identity=(binding.target,)+_path(binding)
                # A same-source reset has a durable fence against open cutoffs.
                # New receipts belong to the new epoch, not the retired model.
                for previous,row in tuple(paths.items()):
                    old=row['binding']
                    if (old.ws_source==binding.ws_source and old.native_channel==binding.native_channel and previous!=identity):
                        paths.pop(previous)
                if identity not in paths:
                    if len(paths)>=max_paths:raise ValueError('native discovery path bound; checkpoint required')
                    paths[identity]=dict(model=None,positions={},last_mono=None)
                paths[identity].update(binding=binding,plan=plan,index=index)
        for row in paths.values():
            binding=row['binding'];r=record.receipt;stream=r.stream_key
            if stream not in {binding.ws_source,binding.seed_source}:continue
            start=binding.ws_start_seq if stream==binding.ws_source else binding.seed_start_seq
            if r.receive_seq<start:continue
            if r.receive_seq!=row['positions'].get(stream,start-1)+1:raise ValueError('managed native receipt prefix missing')
            row['positions'][stream]=r.receive_seq
            if row['last_mono'] is not None and r.monotonic_ns<row['last_mono']:raise ValueError('managed native clock order invalid')
            row['last_mono']=r.monotonic_ns
            if record.provenance.config_sha256!=row['plan'].config_sha256:raise ValueError('managed native config differs')
            error=event=None
            if stream==binding.ws_source and record.kind=='ws_message':
                try:event=_book_message(record,binding)
                except ValueError as exc:error=exc
                if event is None and error is None:continue
            elif stream==binding.seed_source and record.kind=='rest_response':
                if record.instrument is not None and record.instrument!=binding.target.instrument:continue
            else:continue
            model=row['model']
            if model is None:
                model=(SnapshotBook(binding.target.instrument,binding.target.profile,depth=binding.depth,signed_prices=binding.signed_prices)
                    if binding.target.profile in {'deribit_full','bybit_standard'} else
                    BufferedBook(binding.target.instrument,binding.target.profile,now_ns=r.monotonic_ns))
                row['model']=model
            failed_exception=False
            try:
                if error is not None:raise error
                if stream==binding.seed_source:
                    outcome=model.supply_seed(decode_seed(record,binding.target.profile),now_ns=r.monotonic_ns)
                else:
                    if isinstance(model,BufferedBook) and model.state=='no_book':
                        model.reset(now_ns=r.monotonic_ns,reason='native_availability_recheck')
                    outcome=model.receive(event) if isinstance(model,SnapshotBook) else model.receive(event,now_ns=r.monotonic_ns)
                if isinstance(model,BufferedBook) and model.state=='invalid':outcome='invalid'
            except ValueError:
                failed_exception=True
                outcome=model.invalidate('native_decode_or_apply_failed')
            if outcome=='invalid' and (canonical(asdict(binding.target)),record.record_id) not in audited:
                if len(pending)>=max_paths:raise ValueError('native discovery count bound; checkpoint required')
                pending.append(NativeDiscovery(row['plan'],row['index'],record.record_id))
            if outcome=='invalid' and isinstance(model,BufferedBook):
                model.reset(now_ns=r.monotonic_ns,reason='native_reseed_required')
                if not failed_exception and stream==binding.ws_source:model.receive(event,now_ns=r.monotonic_ns)
            # Charge after every path transition, including retained bootstrap
            # seeds and last deltas that are no longer in the event buffer.
            if isinstance(model,BufferedBook):
                retained={id(delta) for delta in model.buffer}
                extra=[]
                for delta in (model.last,model.buffer_last):
                    if delta is not None and id(delta) not in retained:
                        retained.add(id(delta));extra.append(delta)
                row['aux_bytes']=model.buffer_bytes+sum(len(canonical(asdict(d))) for d in extra)
                row['seed_levels']=0 if model.seed is None else len(model.seed.bids)+len(model.seed.asks)
                if model.seed is not None:row['aux_bytes']+=len(canonical(asdict(model.seed)))
                row['buffer_events']=len(model.buffer)
            if sum(p['model'].book.level_count+p.get('seed_levels',0) for p in paths.values() if p['model'] is not None)>max_levels:
                raise ValueError('native discovery aggregate level bound; checkpoint required')
            if sum(p.get('buffer_events',0) for p in paths.values())>max_buffer_events:
                raise ValueError('native discovery aggregate buffer event bound; checkpoint required')
            if sum(p.get('aux_bytes',0) for p in paths.values())>max_buffer_bytes:
                raise ValueError('native discovery aggregate buffer byte bound; checkpoint required')
    return tuple(pending)
