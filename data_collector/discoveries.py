"""Bounded native failures recoverable from managed owner declarations and raw.

No new journal/schema: negative knowledge comes from existing immutable input.
Legacy paths without an owner declaration retain their original replay contract.
"""
from dataclasses import asdict,dataclass

from .books import SnapshotBook
from .contracts import canonical
from .owners import OwnerPlans,_path
from .replay import SnapshotPlan,_book_message


@dataclass(frozen=True,slots=True)
class NativeDiscovery:
    plan: SnapshotPlan
    binding_index: int
    record_id: str


def pending_native_discoveries(ledger,*,max_paths=10000,max_levels=200000):
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
                if binding.target.profile not in {'deribit_full','bybit_standard'}:continue
                if binding.seed_source is not None:raise ValueError('snapshot owner cannot inherit REST seed')
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
            else:continue
            model=row['model']
            if model is None:
                model=SnapshotBook(binding.target.instrument,binding.target.profile,depth=binding.depth,signed_prices=binding.signed_prices)
                row['model']=model
            try:
                if error is not None:raise error
                outcome=model.receive(event)
            except ValueError:
                outcome=model.invalidate('native_decode_or_apply_failed')
            if outcome=='invalid' and (canonical(asdict(binding.target)),record.record_id) not in audited:
                if len(pending)>=max_paths:raise ValueError('native discovery count bound; checkpoint required')
                pending.append(NativeDiscovery(row['plan'],row['index'],record.record_id))
        if sum(row['model'].book.level_count for row in paths.values() if row['model'] is not None)>max_levels:
            raise ValueError('native discovery aggregate level bound; checkpoint required')
    return tuple(pending)
