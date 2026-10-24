"""Reconcile pinned cutoff history into a new serialized local writer.

Only unresolved allocations become interrupted. Existing closes are unchanged;
this does not finish snapshots, certify delivery, sample clocks or resume timers.
A durable recovery marker commits the input pins and resulting allocation tip.
"""
from dataclasses import asdict,dataclass
from pathlib import Path

from .contracts import Provenance,RawRecord,Receipt,StreamKey,canonical,digest,integer,sha256,strict_json,text
from .cutoffs import CONTROL_SCHEMA,CutoffCoordinator,CutoffPin,MINUTE_NS
from .segments import SegmentLimits,iter_segment_records
from .validity import LedgerPin

SCHEMA='dc.cutoff-recovery/2'


@dataclass(frozen=True, slots=True)
class RecoverySegment:
    path: Path
    context_sha256: str
    manifest_sha256: str


@dataclass(frozen=True, slots=True)
class CutoffRecovery:
    source_pin: CutoffPin
    recovered_pin: CutoffPin
    next_cutoff_id: int
    next_cutoff_utc_ns: int | None
    failed_cutoff_ids: tuple[int,...]
    closed_cutoff_ids: tuple[int,...]
    marker_record_id: str
    marker_sha256: str


@dataclass(frozen=True, slots=True)
class RecoveryRequest:
    """Caller-selected immutable history and authoritative cutoff/ledger tips."""
    segments: tuple[RecoverySegment,...]
    cutoff_pin: CutoffPin
    ledger_pin: LedgerPin | None


def _read(path,limit):
    with path.open('rb') as source:body=source.read(limit+1)
    if len(body)>limit:raise ValueError('cutoff recovery metadata byte bound')
    return body


def iter_recovery_prefix(segments, *, venue, environment, capture_id, process_session_id,
                         max_records=200000,max_scan_bytes=256*1024*1024):
    """Read pinned old-process segments under conservative per-pass bounds."""
    count=size=0
    for segment in segments:
        remaining=max_scan_bytes-size
        if remaining<=0:raise ValueError('cutoff recovery metadata scan bound')
        context=_read(segment.path/'context.json',min(65536,remaining))
        remaining-=len(context)
        if remaining<=0:raise ValueError('cutoff recovery metadata scan bound')
        manifest=_read(segment.path/'manifest.json',min(SegmentLimits().max_manifest_bytes,remaining))
        if digest(context)!=segment.context_sha256 or digest(manifest)!=segment.manifest_sha256:
            raise ValueError('cutoff recovery pinned metadata differs or exceeds bound')
        value=strict_json(context)
        if (value['venue'],value['environment'],value['capture_id'])!=(venue,environment,capture_id) or value['process_session_id']==process_session_id:
            raise ValueError('recovery requires same capture/venue and a new process identity')
        stats=strict_json(manifest)
        for field in ('records','uncompressed_bytes','archive_bytes'):integer(stats[field],'recovery segment '+field)
        recoveries=stats['recovery_evidence']
        if type(recoveries) is not list or len(recoveries)>128:raise ValueError('recovery evidence list bound')
        evidence=0
        for item in recoveries:
            integer(item['tail_bytes'],'recovery evidence bytes')
            evidence+=65536+item['tail_bytes'] # conservative repeated-tail/marker reservation
        # Whole-segment verification and exposed rows each read raw once.
        cost=(3*len(context)+2*len(manifest)+stats['archive_bytes']+2*stats['uncompressed_bytes']+
              64*stats['records']+evidence)
        count+=stats['records'];size+=cost
        if count>max_records or size>max_scan_bytes:raise ValueError('cutoff recovery prefix scan bound')
        defaults=SegmentLimits()
        limits=SegmentLimits(max_record_bytes=min(defaults.max_record_bytes,max(1,stats['uncompressed_bytes'])),
            max_uncompressed_bytes=min(defaults.max_uncompressed_bytes,max(1,stats['uncompressed_bytes'])),
            max_compressed_bytes=min(defaults.max_compressed_bytes,max(1,stats['archive_bytes'])),
            max_evidence_bytes=min(defaults.max_evidence_bytes,max(1,evidence)))
        for record in iter_segment_records(segment.path,limits=limits):
            yield record


def reconcile_cutoffs(writer, segments, expected_pin, *, capture_scope, capture_id, process_session_id,
                      provenance, utc_ns, monotonic_ns, clock_epoch, max_segments=1024,
                      max_records=200000,max_scan_bytes=256*1024*1024,max_cutoffs=10000,
                      max_control_bytes=1024*1024,max_projection_bytes=16*1024*1024):
    """Disk-owner operation. Publish the returned marker before starting capture.

    The caller supplies an authoritative expected source tip, not a guessed tip
    from a potentially incomplete directory listing. Prefix objects stay pinned
    on every read. This marker is not a checkpoint or allocation-resume facade.
    """
    if not isinstance(expected_pin,CutoffPin) or not isinstance(provenance,Provenance):
        raise ValueError('authoritative cutoff pin and typed provenance required')
    for name,value in (('segments',max_segments),('records',max_records),('scan bytes',max_scan_bytes),
                       ('cutoffs',max_cutoffs),('control bytes',max_control_bytes),('projection bytes',max_projection_bytes)):
        integer(value,name,1)
    for name,value in (('scope',capture_scope),('capture',capture_id),('process',process_session_id),('epoch',clock_epoch)):
        text(value,name)
        if len(value.encode())>128:raise ValueError('cutoff recovery identity bound')
    integer(utc_ns,'recovery UTC');integer(monotonic_ns,'recovery monotonic')
    if max(utc_ns,monotonic_ns)>2**63-1:raise ValueError('cutoff recovery clock bound')
    if type(segments) is not tuple or not 1<=len(segments)<=max_segments:raise ValueError('bounded pinned recovery segments required')
    for segment in segments:
        if not isinstance(segment,RecoverySegment) or not isinstance(segment.path,Path):raise ValueError('typed recovery segment required')
        sha256(segment.context_sha256,'recovery context');sha256(segment.manifest_sha256,'recovery manifest')
    if len({s.path for s in segments})!=len(segments) or len({s.context_sha256 for s in segments})!=len(segments):
        raise ValueError('distinct recovery segments required')
    writer._ready()
    if hasattr(writer,'descriptor'):
        identity=writer.descriptor['identity']
        if (identity['capture_id'],identity['process_session_id'])!=(capture_id,process_session_id) or writer.descriptor['provenance']!=asdict(provenance):
            raise ValueError('recovery writer identity/provenance differs')
    def prior():
        return iter_recovery_prefix(segments,venue=writer.identity[0],environment=writer.identity[1],
            capture_id=capture_id,process_session_id=process_session_id,max_records=max_records,max_scan_bytes=max_scan_bytes)
    # A retry may contain only this recovery's already-fsynced interruption suffix
    # and final marker. It cannot be invoked over a live capture's new allocations.
    target=[];target_bytes=0;marker=None
    if writer.state.records>max_records or writer.state.valid_bytes>max_scan_bytes:
        raise ValueError('recovery target WAL scan bound')
    for encoded in writer.encoded_records(allow_unsealed=True):
        target_bytes+=len(encoded)+64
        if len(target)>=max_records or target_bytes>max_scan_bytes:raise ValueError('recovery target scan bound')
        record=RawRecord.from_bytes(encoded);r=record.receipt
        if ((r.capture_id,r.process_session_id)!=(capture_id,process_session_id) or
                (record.venue,record.environment)!=writer.identity or record.provenance!=provenance or
                record.kind!='control' or record.native_product!='control' or record.instrument is not None or record.native_fields_json!=b'{}' or
                len(record.body)>max_control_bytes or r.clock_epoch!=clock_epoch or
                max(r.receive_seq,r.receive_utc_ns,r.monotonic_ns)>2**63-1 or r.monotonic_ns>monotonic_ns):
            raise ValueError('recovery target must contain only its own controls')
        if record.channel=='cutoff_recovery:'+capture_scope:
            if marker is not None or r.connection_id!='control:cutoff-recovery:'+capture_scope or r.receive_seq!=0:
                raise ValueError('conflicting recovery marker')
            marker=record
        else:
            value=strict_json(record.body)
            if (marker is not None or record.channel!='cutoffs:'+capture_scope or r.connection_id!='control:'+capture_scope or
                    not isinstance(value,dict) or value.get('kind')!='failed' or not isinstance(value.get('payload'),dict) or
                    value['payload'].get('reason')!='interrupted_before_close'):
                raise ValueError('target contains non-recovery work')
        target.append(record)
    coordinator=CutoffCoordinator(writer,capture_scope=capture_scope,capture_id=capture_id,
        process_session_id=process_session_id,provenance=provenance,prior_records=prior,
        max_cutoffs=max_cutoffs,max_control_bytes=max_control_bytes,max_projection_bytes=max_projection_bytes,
        expected_prior_pin=expected_pin)
    unresolved=tuple(i for i in coordinator.allocations if i not in coordinator.resolutions)
    completed=tuple(strict_json(r.body)['payload']['cutoff_id'] for r in target if r is not marker)
    failed=tuple(sorted(completed+unresolved))
    if len(set(failed))!=len(failed) or completed!=tuple(sorted(completed)) or (completed and unresolved and completed[-1]>=unresolved[0]):
        raise ValueError('noncanonical recovery resolution order')
    closed=tuple(sorted(i for i,v in coordinator.resolutions.items() if 'barrier' in v['payload']))
    next_id=len(coordinator.allocations)
    next_utc=coordinator.allocations[next_id-1]['payload']['utc_ns']+MINUTE_NS if next_id else None
    if next_utc is not None and next_utc>2**63-1:raise ValueError('recovery cutoff UTC range exhausted')
    references=[dict(context_sha256=s.context_sha256,manifest_sha256=s.manifest_sha256) for s in segments]
    def marker_body(pin):
        value=dict(schema=SCHEMA,scope=capture_scope,source_pin=asdict(expected_pin),recovered_pin=asdict(pin),
            input_segments=references,failed_cutoff_ids=failed,closed_cutoff_ids=closed,next_cutoff_id=next_id,next_cutoff_utc_ns=next_utc)
        if not next_id:
            value['preallocation_interruption']=dict(continuity='not_proven',last_closed_cutoff_id=None,
                first_affected_cutoff_id=None,time_bound='not_inferred')
        return canonical(value)
    def marker_record(body):
        return RawRecord(Receipt(capture_id,process_session_id,'control:cutoff-recovery:'+capture_scope,0,
            utc_ns,monotonic_ns,clock_epoch),*writer.identity,'control','cutoff_recovery:'+capture_scope,'control',body,provenance)
    predicted=CutoffPin(coordinator.pin.count+len(unresolved),'f'*64,'f'*64) if unresolved else coordinator.pin
    preview=marker_body(predicted)
    preview_record=marker if marker is not None else marker_record(preview)
    if len(preview)>max_control_bytes or len(preview_record.to_bytes())>writer.limits.max_record_bytes:
        raise ValueError('recovery marker byte bound')
    extra=sum(len(canonical(dict(cutoff_id=i,allocation_hash=coordinator.allocations[i]['hash'],reason='interrupted_before_close'))) for i in unresolved)
    if coordinator.projection_bytes+extra>max_projection_bytes:raise ValueError('recovery projection byte bound')
    projected_records=len(target)+len(unresolved)+int(marker is None)
    if projected_records>max_records:raise ValueError('recovery target output record bound')
    streams=set(writer.state.positions)
    if unresolved:streams.add(StreamKey(capture_id,process_session_id,'control:'+capture_scope))
    streams.add(StreamKey(capture_id,process_session_id,'control:cutoff-recovery:'+capture_scope))
    if len(streams)>writer.limits.max_streams:raise ValueError('recovery target output stream bound')
    # Preview exact envelope sizes. Every future previous_hash is a64-byte hex
    # string, so substituting the current head preserves canonical byte lengths.
    projected_bytes=target_bytes
    for offset,identity in enumerate(unresolved):
        body=canonical(dict(schema=CONTROL_SCHEMA,scope=capture_scope,sequence=coordinator.control_seq+offset,
            previous_hash=coordinator.head,kind='failed',payload=dict(cutoff_id=identity,
                allocation_hash=coordinator.allocations[identity]['hash'],reason='interrupted_before_close')))
        candidate=RawRecord(Receipt(capture_id,process_session_id,'control:'+capture_scope,
            coordinator.next_receipt_seq+offset,utc_ns,monotonic_ns,clock_epoch),*writer.identity,
            'control','cutoffs:'+capture_scope,'control',body,provenance)
        encoded=candidate.to_bytes()
        if len(body)>max_control_bytes or len(encoded)>writer.limits.max_record_bytes:
            raise ValueError('recovery interruption record byte bound')
        projected_bytes+=len(encoded)+64
    if marker is None:projected_bytes+=len(preview_record.to_bytes())+64
    if projected_bytes>max_scan_bytes:raise ValueError('recovery target output byte bound')
    if marker is not None:
        if unresolved or marker.body!=marker_body(coordinator.pin):raise ValueError('recovery marker differs from input/result pins')
    else:
        for identity in unresolved:
            coordinator.fail(identity,'interrupted_before_close',utc_ns=utc_ns,monotonic_ns=monotonic_ns,clock_epoch=clock_epoch)
        # Recheck immutable prefix before declaring the complete recovery ready.
        for _ in prior():pass
        marker=marker_record(marker_body(coordinator.pin));writer.append(marker)
    return CutoffRecovery(expected_pin,coordinator.pin,next_id,next_utc,failed,closed,marker.record_id,digest(marker.to_bytes()))


def verify_recovery_marker(segments,marker,target_records,*,max_records=200000,max_scan_bytes=256*1024*1024):
    """Replay an existing marker without writing or trusting its declared tip."""
    from types import SimpleNamespace
    from .journal import JournalLimits
    if (not isinstance(marker,RawRecord) or type(target_records) is not tuple or not target_records or
            target_records[-1]!=marker or len(target_records)>max_records):
        raise ValueError('bounded recovery prefix ending at marker required')
    encoded=[];size=0;positions={}
    for record in target_records:
        data=record.to_bytes();size+=len(data)+64
        if size>max_scan_bytes:raise ValueError('recovery verification target byte bound')
        encoded.append(data);positions[record.receipt.stream_key]=record.receipt.receive_seq
    class Reader:
        identity=(marker.venue,marker.environment)
        limits=JournalLimits()
        state=SimpleNamespace(records=len(encoded),valid_bytes=size,positions=positions)
        def _ready(self):pass
        def encoded_records(self,*,allow_unsealed=False):yield from encoded
        def append(self,record):raise ValueError('missing complete recovery marker; verification cannot write')
    value=strict_json(marker.body)
    return reconcile_cutoffs(Reader(),segments,CutoffPin(**value['source_pin']),capture_scope=value['scope'],
        capture_id=marker.receipt.capture_id,process_session_id=marker.receipt.process_session_id,provenance=marker.provenance,
        utc_ns=marker.receipt.receive_utc_ns,monotonic_ns=marker.receipt.monotonic_ns,clock_epoch=marker.receipt.clock_epoch,
        max_records=max_records,max_scan_bytes=max_scan_bytes)
