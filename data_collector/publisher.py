"""Single-job snapshot publisher over immutable, locally owned input prefixes.

Runs separately from the raw disk owner. A durable delivery receipt records the
observed completion of snapshot commit/verification, not the later receipt fsync.
The local monotonic origin must match the capture process; do not move this
publisher to another host/process clock domain. A supervisor must bound a hung
thread. Raw-control delivery integration and recurring scheduling are separate.
"""
import asyncio
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, dataclass, fields, is_dataclass
import fcntl
import os
from pathlib import Path

from .clockcutoffs import _contract_policy
from .clocklog import CONTROL, ClockPin, replay_clock
from .contracts import canonical, digest, integer, sha256, strict_json, text
from .journal import _publish_bytes
from .lane import DurableSnapshotInputs, SnapshotOrigin, SnapshotSegmentInput
from .replay import ReplayLimits, SnapshotPlan
from .segments import SegmentLimits, _mkdir, iter_segment_records
from .snapshots import PublicationLimits, publish_snapshot, read_snapshot
from .validity import LedgerEntry, ValidityLedger

from .deliveries import DELIVERY_NS, delivery_base, validate_delivery
from .recovery import RecoverySegment, verify_recovery_marker


class PublisherBusy(ValueError):
    pass


class PublicationUnconfirmed(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class SnapshotDelivery:
    outcome: str
    reason: str
    close_record_id: str
    snapshot_id: str | None
    receipt_path: Path
    receipt_sha256: str
    receipt_body: bytes


def _read(path, limit):
    with Path(path).open('rb') as source:data=source.read(limit+1)
    if len(data)>limit:raise ValueError('publisher metadata byte bound')
    return data


def _charge(value, remaining):
    """Conservative retained-metadata charge before JSON/dictionary copies."""
    remaining-=128
    if remaining<0:raise ValueError('publisher input metadata bound')
    if isinstance(value,Path):
        if sum(len(p) for p in value.parts)>remaining:raise ValueError('publisher path bound')
        return _charge(os.fsdecode(value),remaining)
    if isinstance(value,str):
        if len(value)>remaining:raise ValueError('publisher string bound')
        remaining-=6*len(value.encode('utf-8'))+2
    elif type(value) in (int,bool) or value is None:
        if type(value) is int and value.bit_length()>63:raise ValueError('publisher integer range')
        remaining-=32
    elif isinstance(value,tuple):
        for item in value:remaining=_charge(item,remaining)
    elif is_dataclass(value):
        for field in fields(value):
            remaining=_charge(field.name,remaining);remaining=_charge(getattr(value,field.name),remaining)
    else:raise ValueError('unsupported publisher metadata')
    if remaining<0:raise ValueError('publisher input metadata bound')
    return remaining


def _records(inputs):
    count=size=0
    for segment in inputs.segments:
        for record in iter_segment_records(segment.path):
            count+=1;size+=len(record.to_bytes())
            if count>inputs.replay_limits.max_records or size>inputs.replay_limits.max_scan_bytes:
                raise ValueError('publisher raw scan bound')
            yield record


def _verify(inputs, plan):
    total=0;source_hash=None;old=[];local=False
    for segment in inputs.segments:
        manifest=_read(segment.path/'manifest.json',SegmentLimits().max_manifest_bytes)
        context=_read(segment.path/'context.json',65536)
        total+=len(manifest)+len(context)
        if total>inputs.replay_limits.max_scan_bytes:raise ValueError('publisher metadata scan bound')
        if digest(manifest)!=segment.manifest_sha256 or digest(context)!=segment.context_sha256:
            raise ValueError('pinned publisher input changed')
        value=strict_json(context);origin=inputs.origin
        if (value['venue'],value['environment'],value['capture_id'])!=(origin.venue,origin.environment,origin.capture_id):
            raise ValueError('publisher input origin/config differs')
        if value['process_session_id']!=origin.process_session_id:
            if local:raise ValueError('historical segments must precede current process')
            old.append(RecoverySegment(segment.path,segment.context_sha256,segment.manifest_sha256))
        else:
            local=True
            if value['config_sha256']!=plan.config_sha256:raise ValueError('publisher local config differs')
            if source_hash is None:source_hash=value['source_sha256']
            if value['source_sha256']!=source_hash:raise ValueError('publisher source bundle changed within process')
    if not local:raise ValueError('publisher current process segments missing')
    close=policy=last=ledger=marker=None;ledger_positions={};recovery_records=[]
    for record in _records(inputs):
        if record.record_id==inputs.close_record_id:
            if close is not None or digest(record.to_bytes())!=inputs.close_sha256:
                raise ValueError('pinned publisher close missing/conflicting')
            if (record.venue,record.environment,record.receipt.capture_id,record.receipt.process_session_id)!=(
                    origin.venue,origin.environment,origin.capture_id,origin.process_session_id):
                raise ValueError('publisher close belongs to another process origin')
            close=record
        current=(record.receipt.capture_id,record.receipt.process_session_id)==(origin.capture_id,origin.process_session_id)
        if current and marker is None and old:
            if ((record.kind,record.channel) not in {('control','cutoffs:'+origin.capture_scope),
                    ('control','cutoff_recovery:'+origin.capture_scope),('coverage_event','validity:'+origin.capture_scope),
                    ('coverage_event','gap_mapping:'+origin.capture_scope)} or
                    len(recovery_records)>=inputs.replay_limits.max_records or len(record.body)>1024*1024):
                raise ValueError('recovery marker must precede new live/clock work')
            recovery_records.append(record)
        if current and record.channel=='cutoff_recovery:'+origin.capture_scope:
            if marker is not None:raise ValueError('multiple local recovery markers')
            marker=record
        if current and record.receipt.connection_id==CONTROL:
            if policy is None:policy=_contract_policy(strict_json(record.body)['policy'])
            if record.record_id==inputs.clock_pin.last_record_id:last=record
        if record.kind=='coverage_event' and record.channel=='validity:'+inputs.origin.capture_scope:
            entry=LedgerEntry.from_bytes(record.body)
            if (record.receipt.connection_id!='control:validity:'+entry.ledger_id or
                    record.receipt.receive_seq!=ledger_positions.get(record.receipt.stream_key,-1)+1 or
                    getattr(entry.payload,'capture_scope',inputs.origin.capture_scope)!=inputs.origin.capture_scope):
                raise ValueError('publisher correction ledger namespace/order differs')
            if ledger is None:ledger=ValidityLedger(entry.ledger_id)
            ledger.add(entry);ledger_positions[record.receipt.stream_key]=record.receipt.receive_seq
    if close is None or policy is None or last is None or ledger is None:raise ValueError('publisher clock/close/ledger evidence missing')
    if old:
        if marker is None or marker.provenance!=close.provenance:raise ValueError('historical publisher prefix requires recovery marker')
        verify_recovery_marker(tuple(old),marker,tuple(recovery_records),max_records=inputs.replay_limits.max_records,
            max_scan_bytes=inputs.replay_limits.max_scan_bytes)
    elif marker is not None:raise ValueError('recovery marker source segments missing')
    body=strict_json(close.body)
    if (close.kind!='control' or body.get('schema')!='dc.cutoff-control/2' or body.get('kind')!='closed' or
            body['scope']!=inputs.origin.capture_scope or body['payload']['cutoff_id']!=inputs.cutoff_id or
            body['payload']['barrier']['deadline_ns']!=inputs.deadline_ns):
        raise ValueError('publisher close identity/deadline mismatch')
    origin=inputs.origin
    clock=replay_clock(_records(inputs),venue=origin.venue,environment=origin.environment,
        capture_id=origin.capture_id,process_session_id=origin.process_session_id,provenance=close.provenance,
        policy=policy,expected_pin=inputs.clock_pin,max_assessments=inputs.replay_limits.max_records,
        max_scan_records=inputs.replay_limits.max_records,max_scan_bytes=inputs.replay_limits.max_scan_bytes)
    return close,clock,policy,last,asdict(ledger.pin())


def _quality(inputs, evidence, observation):
    if (not isinstance(observation,dict) or set(observation)!={'utc_ns','monotonic_ns','process_timing_epoch','valid'} or
            observation['valid'] is not True or observation['process_timing_epoch']!=inputs.origin.process_timing_epoch):
        return 'process_clock_invalid'
    for name in ('utc_ns','monotonic_ns'):
        if type(observation[name]) is not int or not 0<=observation[name]<=2**63-1:return 'process_clock_invalid'
    close,clock,policy,last,_=evidence
    assessment=clock.assessment;mono=observation['monotonic_ns'];utc=observation['utc_ns']
    if mono<inputs.deadline_ns or mono<last.receipt.monotonic_ns:return 'process_clock_invalid'
    if mono>inputs.deadline_ns+DELIVERY_NS:return 'publication_deadline_missed'
    if (not assessment.aligned or assessment.anchor.clock_epoch!=close.receipt.clock_epoch or
            mono-assessment.anchor.monotonic_ns>policy['max_sample_age_ns']):
        return 'clock_alignment_unavailable'
    for old_utc,old_mono in ((assessment.anchor.utc_ns,assessment.anchor.monotonic_ns),
                            (last.receipt.receive_utc_ns,last.receipt.monotonic_ns)):
        if abs((utc-old_utc)-(mono-old_mono))>policy['step_tolerance_ns']:
            return 'clock_alignment_unavailable'
    return 'on_time'




class SnapshotPublisher:
    """No queued backlog: one owned worker job, with deferred cancellation."""
    def __init__(self, root, origin, *, observe, max_jobs=10000, max_receipt_bytes=1024*1024,max_input_bytes=8*1024*1024):
        if not isinstance(origin,SnapshotOrigin) or not callable(observe):raise ValueError('local snapshot origin/observer required')
        integer(max_jobs,'publisher job bound',1);integer(max_receipt_bytes,'publisher receipt byte bound',1)
        integer(max_input_bytes,'publisher input metadata bound',1)
        self.root,self.origin,self.observe=Path(root),origin,observe
        self.max_jobs,self.max_receipt_bytes=max_jobs,max_receipt_bytes
        self.max_input_bytes=max_input_bytes
        self._last_observation=None;self._clock_failed=False
        self._submitted=self._completed=self._unconfirmed=0
        self._closing=self._closed=False;self._task=None
        if not self._observation()['valid']:raise ValueError('healthy local publisher process clock required')
        _mkdir(self.root)
        self._lock=os.open(self.root/'publisher.lock',os.O_RDWR|os.O_CREAT,0o600)
        try:fcntl.flock(self._lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
        except BaseException:
            os.close(self._lock);raise
        try:self._executor=ThreadPoolExecutor(max_workers=1,thread_name_prefix='dc-publisher-'+origin.venue)
        except BaseException:
            os.close(self._lock);raise

    def _observation(self):
        try:
            value=self.observe()
            if type(value) is not tuple or len(value)!=3:raise ValueError('publisher observation tuple required')
            utc,mono,epoch=value
            integer(utc,'publisher UTC');integer(mono,'publisher monotonic')
            text(epoch,'publisher process timing epoch')
            if len(canonical(epoch))>130:raise ValueError('publisher epoch byte bound')
            if max(utc,mono)>2**63-1:raise ValueError('publisher observation range')
            if epoch!=self.origin.process_timing_epoch or (self._last_observation is not None and mono<self._last_observation):
                self._clock_failed=True
            if not self._clock_failed:self._last_observation=mono
            return dict(utc_ns=utc,monotonic_ns=mono,process_timing_epoch=epoch,valid=not self._clock_failed)
        except Exception:
            self._clock_failed=True
            return dict(utc_ns=None,monotonic_ns=None,process_timing_epoch=None,valid=False)

    def submit(self, inputs, plan):
        if self._closing or self._closed:raise ValueError('publisher stopped')
        if self._task is not None and not self._task.done():raise PublisherBusy('one publisher job at a time')
        if self._submitted>=self.max_jobs:raise ValueError('publisher job bound; rotate scope')
        if not isinstance(inputs,DurableSnapshotInputs) or inputs.origin!=self.origin or not isinstance(plan,SnapshotPlan):
            raise ValueError('typed locally owned snapshot inputs/plan required')
        if not isinstance(inputs.replay_limits,ReplayLimits):raise ValueError('typed publisher replay limits required')
        sha256(inputs.close_record_id,'publisher close');sha256(inputs.close_sha256,'publisher close hash')
        integer(inputs.cutoff_id,'publisher cutoff');integer(inputs.deadline_ns,'publisher deadline')
        if (inputs.cutoff_id>=10000 or inputs.deadline_ns>2**63-1-DELIVERY_NS or type(inputs.timing_valid) is not bool or
                not isinstance(inputs.clock_pin,ClockPin) or not inputs.clock_pin.count):
            raise ValueError('publisher timing contract')
        if (type(inputs.segments) is not tuple or not inputs.segments or len(inputs.segments)>inputs.replay_limits.max_segments or
                any(not isinstance(s,SnapshotSegmentInput) or not isinstance(s.path,Path) for s in inputs.segments)):
            raise ValueError('bounded immutable publisher segments required')
        if len({s.path for s in inputs.segments})!=len(inputs.segments) or len({s.context_sha256 for s in inputs.segments})!=len(inputs.segments):
            raise ValueError('distinct pinned publisher segments required')
        for segment in inputs.segments:
            sha256(segment.context_sha256,'publisher context');sha256(segment.manifest_sha256,'publisher manifest')
        if len(plan.bindings)>inputs.replay_limits.max_books:raise ValueError('publisher book population bound')
        _charge(plan,_charge(inputs,self.max_input_bytes))
        future=asyncio.get_running_loop().create_future()
        self._submitted+=1
        self._task=asyncio.create_task(self._run(inputs,plan,future))
        return future

    async def _run(self, inputs, plan, client):
        try:
            work=asyncio.get_running_loop().run_in_executor(self._executor,self._execute,inputs,plan)
            while True:
                try:
                    result=await asyncio.shield(work);break
                except asyncio.CancelledError:
                    self._closing=True # drain before releasing ownership
                    if work.done():
                        if work.cancelled():raise PublicationUnconfirmed('owned publication work was cancelled')
                        result=work.result();break
            self._completed+=1
            if not client.done():client.set_result(result)
        except Exception as exc:
            self._unconfirmed+=1
            if not client.done():client.set_exception(PublicationUnconfirmed(type(exc).__name__))
        finally:
            if self._clock_failed:self._closing=True

    def _base(self, inputs, plan):
        return delivery_base(inputs,plan)

    def _result(self, path, body):
        value=strict_json(body)
        return SnapshotDelivery(value['outcome'],value['reason'],value['close_record_id'],value['snapshot_id'],path,digest(body),body)

    def _delivery_quality(self, inputs, evidence, observation):
        """Retain a locally detected step even after UTC returns or we reopen.

        Recovery requires a different raw epoch with replay-proven alignment.
        Historical receipt verification deliberately uses the pure _quality.
        """
        close,clock,policy,last,_=evidence
        epoch=clock.assessment.anchor.clock_epoch
        marker=canonical(dict(schema='dc.publisher-clock-invalidation/2',
            origin=asdict(self.origin),clock_epoch=epoch))
        path=self.root/'clock-invalidations'/(digest(marker)+'.json')
        try:
            if path.exists():
                if _read(path,4096)!=marker:raise ValueError('publisher clock invalidation changed')
                _publish_bytes(path,marker) # also confirm a previously ambiguous fsync
                return 'clock_alignment_unavailable'
            stepped=(observation['valid'] and any(
                abs((observation['utc_ns']-utc)-(observation['monotonic_ns']-mono))>policy['step_tolerance_ns']
                for utc,mono in ((clock.assessment.anchor.utc_ns,clock.assessment.anchor.monotonic_ns),
                                 (last.receipt.receive_utc_ns,last.receipt.monotonic_ns))))
            if stepped:
                _mkdir(path.parent);_publish_bytes(path,marker)
                return 'clock_alignment_unavailable'
        except Exception:
            self._clock_failed=True # unknown durable invalidation: require owner recovery
            raise
        return _quality(inputs,evidence,observation)

    def _execute(self, inputs, plan):
        path=self.root/'deliveries'/(inputs.close_record_id+'.json');base=self._base(inputs,plan)
        limits=PublicationLimits(replay=inputs.replay_limits)
        if path.exists():
            body=_read(path,self.max_receipt_bytes);saved=validate_delivery(body,max_bytes=self.max_receipt_bytes)
            if canonical(saved)!=body or any(saved.get(k)!=v for k,v in base.items()):
                raise ValueError('immutable delivery input conflict')
            if set(saved)!=set(base)|{'started','completed','outcome','reason','snapshot_id','error_type'}:
                raise ValueError('delivery receipt shape')
            if saved['outcome']=='published':
                evidence=_verify(inputs,plan)
                if (saved['reason']!='on_time' or saved['error_type'] is not None or
                        _quality(inputs,evidence,saved['started'])!='on_time' or
                        _quality(inputs,evidence,saved['completed'])!='on_time' or
                        saved['completed']['monotonic_ns']<saved['started']['monotonic_ns']):
                    raise ValueError('delivery timing evidence invalid')
                snapshot=read_snapshot(self.root/'snapshots',saved['snapshot_id'],[s.path for s in inputs.segments],limits=limits)
                if (snapshot['descriptor']['close_sha256']!=inputs.close_sha256 or
                        digest(canonical(snapshot['descriptor']['plan']))!=base['plan_sha256'] or
                        snapshot['descriptor']['publication_ledger_pin']!=evidence[4]):
                    raise ValueError('delivery close/plan/ledger knowledge changed')
            elif saved['outcome']!='failed' or saved['reason']=='on_time':raise ValueError('delivery outcome')
            _publish_bytes(path,body) # confirm any previously ambiguous receipt fsync
            return self._result(path,body)
        started=self._observation();snapshot_id=None;error=None;evidence=None
        reason='input_timing_invalid' if not inputs.timing_valid else 'process_clock_invalid'
        try:
            if inputs.timing_valid and started['valid']:
                evidence=_verify(inputs,plan)
                reason=self._delivery_quality(inputs,evidence,started)
                before=self._observation()
                before_reason=self._delivery_quality(inputs,evidence,before)
                if reason=='on_time':reason=before_reason
                if reason=='on_time':
                    result=publish_snapshot(self.root/'snapshots',[s.path for s in inputs.segments],inputs.close_record_id,plan,
                        limits=limits,expected_close_sha256=inputs.close_sha256)
                    snapshot_id=result['snapshot_id']
                    if result['descriptor']['publication_ledger_pin']!=evidence[4]:
                        raise ValueError('existing snapshot excludes publication-start ledger knowledge')
                    _verify(inputs,plan) # recheck pinned files before delivery certification
        except Exception as exc:
            reason='publication_failed';error=type(exc).__name__[:128]
        completed=self._observation()
        if evidence is not None:
            completed_reason=self._delivery_quality(inputs,evidence,completed)
            if reason=='on_time':reason=completed_reason
        value=dict(base,started=started,completed=completed,outcome='published' if reason=='on_time' else 'failed',
            reason=reason,snapshot_id=snapshot_id,error_type=error)
        body=canonical(value)
        validate_delivery(body,max_bytes=self.max_receipt_bytes)
        _mkdir(path.parent);_publish_bytes(path,body)
        return self._result(path,body)

    async def close(self):
        self._closing=True;cancelled=False
        while self._task is not None and not self._task.done():
            try:await asyncio.shield(self._task)
            except asyncio.CancelledError:cancelled=True
        if not self._closed:
            self._closed=True
            self._executor.shutdown(wait=False)
            os.close(self._lock)
        if cancelled:raise asyncio.CancelledError

    def health(self):
        return dict(accepting=not self._closing and not self._closed,busy=self._task is not None and not self._task.done(),
            submitted=self._submitted,completed=self._completed,unconfirmed=self._unconfirmed,process_clock_valid=not self._clock_failed)
