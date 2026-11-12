"""Select a native recovery closure by replay, without repairing past gaps."""
from dataclasses import asdict,dataclass
from types import SimpleNamespace

from .contracts import RawRecord,Receipt,canonical,digest,integer,sha256,strict_json
from .cutoffs import CutoffCoordinator
from .gapmap import SCHEMA as GAP_SCHEMA
from .ledger import DurableLedger
from .replay import ReplayLimits,SnapshotPlan,_materialize_snapshot
from .segments import iter_segment_records
from .validity import CloseGap,GapCorrection,LedgerEntry,LedgerPin

SCHEMA='dc.native-gap-recovery/2'
RULE='first-accepted-native-recovery-closure/1'


@dataclass(frozen=True,slots=True)
class NativeRecovery:
    mapping_record_id: str
    mapping_sha256: str
    correction_id: str
    recovery_cutoff_id: int
    ledger_pin: LedgerPin


def recovery_payload(value,record_id):
    return CloseGap(value['correction_id'],value['recovery_cutoff_id'],value['anchor_record_id'],record_id)


def map_native_recovery(ledger,coordinator,segments,plan,binding_index,correction_id,*,utc_ns,monotonic_ns,clock_epoch,
                        limits=ReplayLimits(),max_dependencies=256,max_mapping_bytes=65536):
    """Disk-owner operation over sealed inputs; None means no qualifying closure.

    The same frozen plan/owner path as the native gap must be available. An
    accepted anchor must follow discovery and be incorporated by a healthy
    closure. The complete replay prefix is retained as portable proof. An orphan
    positive audit leaves the original negative open until an exact retry.
    """
    if (not isinstance(ledger,DurableLedger) or not isinstance(coordinator,CutoffCoordinator) or
            ledger.journal is not coordinator.journal or ledger.scope!=coordinator.scope or
            ledger.capture_id!=coordinator.capture_id or ledger.session!=coordinator.session):
        raise ValueError('same serialized cutoff and ledger owners required')
    if not isinstance(limits,ReplayLimits) or not isinstance(plan,SnapshotPlan) or len(plan.bindings)>limits.max_books:
        raise ValueError('bounded typed native recovery plan required')
    from .publisher import _charge
    _charge(plan,1024*1024)
    integer(binding_index,'native recovery binding');sha256(correction_id,'native correction')
    integer(max_dependencies,'native recovery dependencies',1);integer(max_mapping_bytes,'native recovery audit bytes',1)
    if binding_index>=len(plan.bindings):raise ValueError('native recovery binding absent')
    binding=plan.bindings[binding_index]
    def records():
        count=size=0
        for record in ledger._all_records():
            count+=1;size+=len(record.to_bytes())+64
            if count>limits.max_records or size>limits.max_scan_bytes:raise ValueError('native recovery scan bound; checkpoint required')
            yield record
    pin=ledger.pin();gap=resolution=gap_audit=existing=None;next_seq=0
    request_id=digest(canonical(dict(operation='native_recovery',scope=ledger.scope,ledger_id=ledger.ledger_id,
        plan=asdict(plan),binding_index=binding_index,correction_id=correction_id)))
    for record in records():
        if record.channel==ledger.channel and record.kind=='coverage_event':
            payload=LedgerEntry.from_bytes(record.body).payload
            if isinstance(payload,GapCorrection) and payload.correction_id==correction_id:gap=payload
            if isinstance(payload,CloseGap) and payload.correction_id==correction_id:resolution=payload
        if record.channel=='gap_mapping:'+ledger.scope:
            value=strict_json(record.body)
            if record.receipt.process_session_id==ledger.session:next_seq=record.receipt.receive_seq+1
            if value.get('request_id')==request_id:
                if existing is not None:raise ValueError('duplicate native recovery request')
                existing=record
    if gap is None or gap.target!=binding.target:raise ValueError('native gap target differs from recovery plan')
    for record in records():
        if record.record_id==gap.proof_record_id:gap_audit=record;break
    if (gap_audit is None or strict_json(gap_audit.body).get('schema')!=GAP_SCHEMA or
            strict_json(gap_audit.body).get('plan_sha256')!=digest(canonical(asdict(plan)))):
        raise ValueError('exact native gap plan and audit required; owner transitions are separate')
    if resolution is not None and (existing is None or resolution!=recovery_payload(strict_json(existing.body),existing.record_id)):
        raise ValueError('native gap already resolved by another proof')
    def prefix():
        for record in records():
            if existing is not None and record.record_id==existing.record_id:return
            yield record
        if existing is not None:raise ValueError('native recovery audit prefix changed')
    empty=SimpleNamespace(identity=ledger.journal.identity,limits=ledger.journal.limits,encoded_records=lambda **kwargs:iter(()))
    restored=CutoffCoordinator(empty,capture_scope=ledger.scope,capture_id=ledger.capture_id,process_session_id=ledger.session,
        provenance=ledger.provenance,prior_records=prefix,max_cutoffs=coordinator.max_cutoffs,
        max_control_bytes=coordinator.max_control_bytes,max_projection_bytes=coordinator.max_projection_bytes)
    if existing is None and restored.pin!=coordinator.pin:raise ValueError('native recovery cutoff prefix differs')
    # Bound/verify the path list once even if there is no eligible candidate.
    paths=[]
    for path in segments:
        if len(paths)>=limits.max_segments:raise ValueError('native recovery segment bound')
        paths.append(path)
    if not paths or len(set(paths))!=len(paths):raise ValueError('distinct sealed native recovery segments required')
    selected=None
    for identity,allocation_record in restored.allocations.items():
        if identity<gap.first_cutoff_id:continue
        closed=restored.resolutions.get(identity)
        if closed is None:break # an earlier unclosed cutoff cannot be skipped
        allocation=allocation_record['payload']
        if allocation['dependencies']!=plan.dependencies:break # requires explicit owner/plan transition proof
        if identity==gap.first_cutoff_id or 'barrier' not in closed['payload']:continue
        base,validities=_materialize_snapshot(paths,closed['record_id'],plan,limits=limits)
        view=strict_json(base['payload'])['books'][binding_index]['book']
        if validities[binding_index]!='valid' or view['state']!='live':continue
        anchor=view.get('anchor_source');seen_discovery=after=False
        for record in prefix():
            if record.record_id==gap.evidence_record_id:seen_discovery=True
            if record.record_id==anchor:after=seen_discovery;break
        if after:
            selected=(identity,closed,base,anchor);break
    if selected is None:
        if existing is not None:raise ValueError('saved native recovery proof no longer available')
        return None
    identity,closed,base,anchor=selected
    dependencies={};replay_rows=[];found=False
    for record in prefix():
        row=dict(record_id=record.record_id,raw_sha256=digest(record.to_bytes()))
        # Delayed earlier resolutions may follow the selected close in raw order.
        # Retain every cutoff control needed to restore the selection's full pin.
        if not found or record.channel=='cutoffs:'+ledger.scope:
            if record.record_id not in dependencies:
                if len(dependencies)>=max_dependencies:raise ValueError('native recovery dependency bound; checkpoint required')
                dependencies[record.record_id]=row
        if not found:replay_rows.append(row)
        if record.record_id==closed['record_id']:found=True
    if not found or anchor not in dependencies:raise ValueError('native recovery source/closure unavailable')
    # Supplied sealed replay inputs must equal the owned durable prefix, not just
    # share receipt identities. Verify full bytes through the selected closure.
    expected=tuple(replay_rows);position=0;matched=False
    for path in paths:
        for record in iter_segment_records(path):
            if position>=len(expected) or expected[position]!=dict(record_id=record.record_id,raw_sha256=digest(record.to_bytes())):
                raise ValueError('sealed native recovery inputs differ from owned prefix')
            position+=1
            if record.record_id==closed['record_id']:matched=True;break
        if matched:break
    if not matched or position!=len(expected):raise ValueError('sealed native recovery prefix incomplete')
    body=canonical(dict(schema=SCHEMA,rule=RULE,scope=ledger.scope,request_id=request_id,target=asdict(gap.target),
        correction_id=correction_id,recovery_cutoff_id=identity,anchor_record_id=anchor,close_record_id=closed['record_id'],
        snapshot_id=base['snapshot_id'],snapshot_descriptor=base['descriptor'],cutoff_pin=asdict(restored.pin),
        raw_dependencies=list(dependencies.values())))
    if len(body)>min(max_mapping_bytes,65536):raise ValueError('native recovery audit byte bound')
    if existing is not None:
        if existing.body!=body:raise ValueError('native recovery audit differs from replay')
        marker=existing
    else:
        marker=RawRecord(Receipt(ledger.capture_id,ledger.session,'control:gap-mapping:'+ledger.scope,next_seq,
            utc_ns,monotonic_ns,clock_epoch),*ledger.journal.identity,'control','gap_mapping:'+ledger.scope,'coverage_event',
            body,ledger.provenance)
        if len(marker.to_bytes())>ledger.journal.limits.max_record_bytes:raise ValueError('native recovery record byte bound')
        ledger.journal.append(marker)
    payload=recovery_payload(strict_json(body),marker.record_id)
    pin=ledger.pin() if resolution is not None else ledger.append(payload,utc_ns=utc_ns,monotonic_ns=monotonic_ns,clock_epoch=clock_epoch)
    return NativeRecovery(marker.record_id,digest(marker.to_bytes()),correction_id,identity,pin)
