"""One venue's serialized archive worker and durable runtime-failure controls.

Not a transport supervisor or native continuity assessor. A returned Future only
confirms fsynced raw bytes, never book validity. Use one lane per venue process;
external process limits are required for a hung writer or noncooperative adapter.
"""
import asyncio
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, dataclass, replace
from functools import partial
from pathlib import Path
import time
import sys

from .contracts import VENUES, Provenance, RawRecord, Receipt, StreamKey, canonical, digest, integer, sha256, strict_json, text
from .cutoffs import CutoffCoordinator, IngressWriter, MINUTE_NS
from .clockcutoffs import ClockBoundCutoffs, _contract_policy
from .ledger import DurableLedger
from .queues import ByteQueue, QueueBacklog, QueueCapacity
from .journal import Journal
from .clock import ClockAssessment, ClockSample
from .clocklog import ClockPin, DurableClock, SOURCE, CONTROL, validate_clock_source
from .replay import ReplayLimits
from .deliveries import delivery_base, validate_delivery
from .recovery import CutoffRecovery, RecoveryRequest, RecoverySegment, iter_recovery_prefix, reconcile_cutoffs
from .cutoffs import CutoffPin
from .validity import LedgerPin
from .gapmap import GapMapping, map_native_gap, map_deribit_end
from .gaprecovery import NativeRecovery, map_native_recovery
from .owners import OwnerPlan,OwnerPlans

PENDING_CLOCK_EPOCH = 'pending:alignment'


class LaneStopped(ValueError):
    pass


class DurabilityUnknown(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class DurableAck:
    """Raw persistence and processing latency, never UTC-alignment validity.

    Measured-clock lanes use process monotonic timing; legacy lanes additionally
    require paired UTC/monotonic agreement. Consult measured checks for alignment.
    """
    record_id: str
    raw_sha256: str
    ingress_ordinal: int
    receive_to_durable_ns: int | None
    timing_valid: bool
    clock_epoch: str | None = None


@dataclass(frozen=True, slots=True)
class DurableClockAck:
    assessment: ClockAssessment
    pin: ClockPin
    receive_to_durable_ns: int | None
    timing_valid: bool


@dataclass(frozen=True, slots=True)
class DurableCutoffAck:
    event: dict
    record_id: str
    raw_sha256: str
    clock_pin: ClockPin
    receive_to_durable_ns: int | None
    timing_valid: bool


@dataclass(frozen=True, slots=True)
class SnapshotOrigin:
    venue: str
    environment: str
    capture_id: str
    process_session_id: str
    capture_scope: str
    process_timing_epoch: str

    def __post_init__(self):
        for name,value in asdict(self).items():
            text(value,name)
            if len(canonical(value))>130:raise ValueError('snapshot origin byte bound')
        if self.venue not in VENUES:raise ValueError('snapshot origin venue')


@dataclass(frozen=True, slots=True)
class SnapshotSegmentInput:
    path: Path
    context_sha256: str
    manifest_sha256: str


@dataclass(frozen=True, slots=True)
class DurableSnapshotInputs:
    """Immutable committed input set; not a published snapshot or deadline proof."""
    close_record_id: str
    close_sha256: str
    cutoff_id: int
    deadline_ns: int
    origin: SnapshotOrigin
    segments: tuple[SnapshotSegmentInput, ...]
    replay_limits: ReplayLimits
    clock_pin: ClockPin
    receive_to_durable_ns: int | None
    timing_valid: bool


@dataclass(frozen=True, slots=True)
class DurableDeliveryAck:
    """Durable publisher report, not independently verified snapshot validity."""
    record_id: str
    raw_sha256: str
    receipt_sha256: str
    receive_seq: int
    receive_to_durable_ns: int | None
    timing_valid: bool


@dataclass(frozen=True, slots=True)
class DurableRecoveryAck:
    recovery: CutoffRecovery
    ledger_pin: LedgerPin | None
    elapsed_ns: int | None
    timing_valid: bool


@dataclass(frozen=True, slots=True)
class DurableOwnerPlanAck:
    declaration: OwnerPlan
    receive_to_durable_ns: int | None
    timing_valid: bool


@dataclass(frozen=True, slots=True)
class DurableGapAck:
    mapping: GapMapping | NativeRecovery | None
    receive_to_durable_ns: int | None
    timing_valid: bool


@dataclass(slots=True)
class _Pending:
    record_id: str
    receipt: Receipt
    future: asyncio.Future
    clock_sample: ClockSample | None = None
    timing_epoch: str | None = None
    clock_check: bool = False
    stamp_clock: bool = False
    cutoff_operation: str | None = None
    snapshot_inputs: bool = False
    snapshot_delivery: bool = False
    recovery: bool = False
    owner_plan: bool = False
    gap_mapping: bool = False


class ArchiveLane:
    """Single event-loop producer API; one dedicated thread owns all disk calls.

    submit() is bounded/nonblocking and returns a Future for durable completion.
    stop() closes ingress and drains accepted bytes. Cancellation is deferred
    while a disk operation is running: do not race abort against a live write.
    A process supervisor must enforce the hard stop for a permanently hung disk.
    """
    def __init__(self, writer, queue, *, emergency_writer, observe=None, max_connections=128,
                 clock_tolerance_ns=100_000_000, clock_policy=None,
                 cutoff_scope=None, cutoff_ledger_id=None,snapshot_limits=ReplayLimits(),recovery=None,
                 max_recovery_ns=300_000_000_000):
        if not isinstance(queue, ByteQueue):
            raise ValueError('typed byte queue required')
        status = queue.health()
        if status['bytes'] or not status['accepting'] or not status['clock_valid']:
            raise ValueError('fresh empty byte queue required')
        integer(max_connections, 'connection count bound', 1)
        integer(clock_tolerance_ns, 'wall/monotonic clock tolerance', 1)
        if not isinstance(snapshot_limits,ReplayLimits):raise ValueError('typed snapshot replay limits required')
        integer(max_recovery_ns,'startup recovery time bound',1)
        if recovery is not None:
            if (not isinstance(recovery,RecoveryRequest) or not isinstance(recovery.cutoff_pin,CutoffPin) or
                    (recovery.ledger_pin is not None and not isinstance(recovery.ledger_pin,LedgerPin)) or
                    (recovery.cutoff_pin.count and recovery.ledger_pin is None) or
                    type(recovery.segments) is not tuple or not 1<=len(recovery.segments)<=snapshot_limits.max_segments or
                    (recovery.ledger_pin is not None and recovery.ledger_pin.ledger_id!=cutoff_ledger_id) or cutoff_scope is None):
                raise ValueError('bounded pinned cutoff/ledger recovery request required')
            for segment in recovery.segments:
                if not isinstance(segment,RecoverySegment) or not isinstance(segment.path,Path):raise ValueError('typed recovery segments required')
                if len(str(segment.path).encode())>4096:raise ValueError('recovery path bound')
                sha256(segment.context_sha256,'recovery context');sha256(segment.manifest_sha256,'recovery manifest')
        if (cutoff_scope is None) != (cutoff_ledger_id is None):
            raise ValueError('explicit cutoff scope and ledger identity required together')
        if cutoff_scope is not None:
            for name,value in (('cutoff scope',cutoff_scope),('cutoff ledger',cutoff_ledger_id)):
                text(value,name)
                if len(value.encode()) > 128:raise ValueError('cutoff identity byte bound')
            if clock_policy is None:raise ValueError('cutoffs require measured clock policy')
            _contract_policy(clock_policy)
            namespaces=[SOURCE,CONTROL,'control:runtime:'+writer.identity[0],
                'control:emergency:'+writer.identity[0],'control:clock-check-request','control:cutoff-request',
                'control:snapshot-input-request',
                'control:snapshot-delivery-request','control:snapshot-deliveries:'+cutoff_scope,
                'control:cutoff-recovery-request','control:cutoff-recovery:'+cutoff_scope,
                'control:gap-mapping-request','control:gap-mapping:'+cutoff_scope,
                'control:owner-plan-request','control:owners:'+cutoff_scope,
                'control:'+cutoff_scope,'control:validity:'+cutoff_ledger_id,'control:clock-cutoffs:'+cutoff_scope]
            if len(set(namespaces))!=len(namespaces):
                raise ValueError('cutoff and runtime control namespaces must be distinct')
        control_bound = max_connections * 8192 + 65536
        control_streams = (3 if clock_policy is not None else 1)+(6 if cutoff_scope is not None else 0)+int(recovery is not None)
        if max_connections + control_streams > writer.limits.max_streams or control_bound > writer.limits.max_record_bytes:
            raise ValueError('writer limits lack bounded runtime-control headroom')
        if (not isinstance(emergency_writer, Journal) or emergency_writer.identity != writer.identity or
                emergency_writer.state.records or emergency_writer.limits.max_record_bytes < control_bound):
            raise ValueError('fresh independent emergency-control WAL with sufficient headroom required')
        emergency_writer._ready()
        if Path(emergency_writer.path).resolve().is_relative_to(writer.directory.resolve()):
            raise ValueError('emergency WAL must be outside the primary writer session directory')
        identity = writer.descriptor['identity']
        for name in ('capture_id', 'process_session_id'):
            if len(canonical(identity[name])) > 130:
                raise ValueError('runtime capture/session identifier byte bound')
        self.writer, self.queue, self.identity = writer, queue, identity
        self.emergency_writer = emergency_writer
        self.provenance = Provenance(**writer.descriptor['provenance'])
        self.observe = observe or (lambda:(time.time_ns(), time.monotonic_ns(), 'initial'))
        self._last_observation = self._sample()
        self._clock_failed = False
        self.clock_tolerance_ns = clock_tolerance_ns
        self.max_connections = max_connections
        self.ingress = IngressWriter(writer, identity['capture_id'], identity['process_session_id'], max_streams=max_connections)
        self._clock = None if clock_policy is None else DurableClock(writer,capture_id=identity['capture_id'],
            process_session_id=identity['process_session_id'],provenance=self.provenance,policy=clock_policy)
        self._clock_submitted = 0
        self._clock_checks_submitted = 0
        self._cutoff_scope,self._cutoff_ledger_id = cutoff_scope,cutoff_ledger_id
        self._cutoffs = None
        self._owner_plan_requests=0
        self._gap_requests_submitted=0
        self._recovery_request=recovery;self._recovery_result=None
        self._recovery_submitted=False;self._recovery_ready=recovery is None
        self.max_recovery_ns=max_recovery_ns
        self._history_count=self._source_records=self._source_bytes=0
        self._cutoff_commands_submitted = self._cutoff_allocations_submitted = 0
        self._cutoff_last_utc = self._cutoff_active = None
        self._cutoff_deadline_ns = None
        self._last_closed = self._snapshot_inputs = None
        self._snapshot_inputs_pending = False
        self._snapshot_requests_submitted = 0
        self._delivery_handoffs = {}
        self._delivery_receipts = {}
        self._delivery_requests_submitted = 0
        self._deliveries = {}
        self._delivery_bytes = 0
        self.snapshot_limits = snapshot_limits
        self._executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix='dc-'+identity['venue'])
        self._pending = {}
        self._submitted, self._durable = {}, {}
        self._failed_connections, self._global_failure = {}, None
        self._controls = deque()
        self._control_seq = self._admission_count = self._durable_count = 0
        self._last_submitted_mono = None
        self._wakeup = asyncio.Event()
        self._running = self._started = self._stopping = self._cancelled = False
        self._terminal = False
        self._stop_reason = 'operator_stop'
        self.control_records = []
        self._failure_control_count = 0
        self.emergency_control_id = None
        self.writer_error = None
        self.segments = ()

    def _sample(self):
        value = self.observe()
        if type(value) is not tuple or len(value) != 3:
            raise ValueError('explicit UTC/monotonic/clock-epoch observation required')
        integer(value[0], 'observed UTC nanoseconds')
        integer(value[1], 'observed monotonic nanoseconds')
        if max(value[0], value[1]) > 2**63-1:
            raise ValueError('runtime clock integer range')
        text(value[2], 'clock epoch')
        if len(canonical(value[2])) > 130:
            raise ValueError('clock epoch byte bound')
        return value

    def _observation(self):
        try:
            stamp = self._sample()
        except Exception:
            self._clock_failed = True
            return self._last_observation, False, 'last_healthy_observation'
        previous = self._last_observation
        if (stamp[2] != previous[2] or stamp[1] < previous[1] or
                (self._clock is None and
                 abs((stamp[0]-previous[0])-(stamp[1]-previous[1])) > self.clock_tolerance_ns)):
            self._clock_failed = True
        if not self._clock_failed:
            self._last_observation = stamp
        return stamp, not self._clock_failed, 'observed_invalid_clock' if self._clock_failed else 'observed'

    def _failure(self, reason, record=None, *, global_scope=False):
        if global_scope:
            if self._global_failure is not None:
                return
        elif record.receipt.stream_key in self._failed_connections:
            return
        event = dict(reason=reason, scope='venue' if global_scope else 'connection',
                     stream=None if global_scope else asdict(record.receipt.stream_key), interval_open=True,
                     rejected=None if record is None else dict(record_id=record.record_id, receipt=asdict(record.receipt),
                         body_sha256=digest(record.body), body_bytes=len(record.body)),
                     last_durable_at_detection=[self._durable[key] for key in sorted(self._durable, key=str)] if global_scope else
                         [self._durable[record.receipt.stream_key]] if record.receipt.stream_key in self._durable else [])
        if global_scope:
            self._global_failure = event
            self._stopping = True
            self.queue.close()
        else:
            self._failed_connections[record.receipt.stream_key] = event
        self._controls.append(event)
        self._wakeup.set()

    def submit(self, record):
        """Admit an already labelled raw record; caller owns its epoch label."""
        return self._submit(record, stamp_clock=False)

    def submit_received(self, record):
        """Stamp alignment at the serialized durable FIFO position.

        Supply exact receive clocks and PENDING_CLOCK_EPOCH, not a guessed or
        cached alignment label. A measured sample must already be admitted.
        The disk owner selects the last preceding committed clock assessment;
        a failed clock write prevents dependent raw acknowledgements. This is
        an epoch assignment, not a freshness check or proof of UTC alignment.
        The acknowledgement reports the stored epoch and expanded raw hash.
        """
        if self._clock is None or not self._clock_submitted:
            raise ValueError('admit a measured clock sample before received raw')
        if not isinstance(record, RawRecord) or record.receipt.clock_epoch != PENDING_CLOCK_EPOCH:
            raise ValueError('explicit pending alignment receipt required')
        return self._submit(record, stamp_clock=True)

    def _submit(self, record, *, stamp_clock):
        self._require_recovery()
        if self._stopping or self._terminal:
            raise LaneStopped('venue archive ingress stopped')
        if not isinstance(record, RawRecord):
            raise ValueError('typed raw record required')
        r = record.receipt
        if not stamp_clock and r.clock_epoch == PENDING_CLOCK_EPOCH:
            raise ValueError('pending alignment receipts require submit_received')
        if (record.venue, record.environment, r.capture_id, r.process_session_id,
            record.provenance.source_sha256, record.provenance.config_sha256) != (
                self.identity['venue'], self.identity['environment'], self.identity['capture_id'], self.identity['process_session_id'],
                self.provenance.source_sha256, self.provenance.config_sha256) or r.connection_id.startswith('control:'):
            raise ValueError('foreign raw/collector-control namespace')
        if len(canonical(r.connection_id)) > 130 or len(canonical(r.clock_epoch)) > 130:
            raise ValueError('runtime connection/clock identifier byte bound')
        if any(value > 2**63-1 for value in (r.receive_seq,r.receive_utc_ns,r.monotonic_ns)):
            raise ValueError('runtime receipt integer range')
        stream = r.stream_key
        if stream in self._failed_connections:
            raise LaneStopped('connection halted; use a fresh connection identity after recovery')
        if stream not in self._submitted and stream not in self._failed_connections and len(
                self._submitted.keys() | self._failed_connections.keys()) >= self.max_connections:
            self._failure('session_connection_bound', record, global_scope=True)
            raise QueueCapacity('runtime session connection bound')
        if r.receive_seq != self._submitted.get(stream, -1) + 1:
            raise ValueError('submitted receipt prefix must start at zero and remain consecutive')
        if self._last_submitted_mono is not None and r.monotonic_ns < self._last_submitted_mono:
            self._failure('receipt_monotonic_regression', record, global_scope=True)
            raise LaneStopped('receipt clock failed; new process epoch required')
        fields = strict_json(record.native_fields_json)
        if 'collector_ingress_ordinal' in fields:
            raise ValueError('reserved collector ingress ordinal')
        # Bound the actual expanded envelope before admitting its original bytes.
        maximum = replace(record, native_fields_json=canonical(dict(fields, collector_ingress_ordinal=2**63-1)))
        if stamp_clock:
            # ClockMonitor permits up to1024 epochs and a128-byte session ID.
            # Preflight the eventual envelope, not its shorter pending label.
            maximum = replace(maximum, receipt=replace(r,
                clock_epoch=self.identity['process_session_id']+':clock:1023'))
        if self._admission_count+self._cutoff_commands_submitted >= 2**63 or len(maximum.to_bytes()) > self.writer.limits.max_record_bytes:
            self._failure('expanded_record_bound', record)
            raise QueueCapacity('expanded ingress record exceeds writer bound')
        future = asyncio.get_running_loop().create_future()
        try:
            ticket = self.queue.offer(record.to_bytes(), extra_bytes=4096)
        except Exception as exc:
            future.cancel()
            reason = 'queue_backlog' if isinstance(exc, QueueBacklog) else 'queue_capacity' if isinstance(exc, QueueCapacity) else 'queue_clock_or_closed'
            self._failure(reason, record, global_scope=not isinstance(exc, QueueCapacity))
            raise
        # A measured alignment epoch classifies receive-asof eligibility; it
        # is not the observer's process/monotonic integrity epoch. Losing UTC
        # alignment must not by itself stop durable raw capture. Retain the
        # legacy receipt/observer equality rule when measured clocks are off.
        timing_epoch = self._last_observation[2] if self._clock is not None else None
        self._pending[ticket] = _Pending(record.record_id, r, future,
                                        timing_epoch=timing_epoch, stamp_clock=stamp_clock)
        self._submitted[stream] = r.receive_seq
        self._last_submitted_mono = r.monotonic_ns
        self._admission_count += 1
        self._wakeup.set()
        return future

    def _require_clock(self):
        self._require_recovery()
        if self._stopping or self._terminal:
            raise LaneStopped('venue archive ingress stopped')
        if self._clock is None:
            raise ValueError('explicit clock policy required to enable measured clock writes')
        if self._clock_submitted+self._clock_checks_submitted+self._cutoff_commands_submitted >= self._clock.max_assessments:
            self._failure('clock_log_bound',global_scope=True)
            raise QueueCapacity('clock operation bound; rotate process session')

    def _require_recovery(self):
        if not self._recovery_ready:raise ValueError('await durable cutoff recovery before live ingress')

    def submit_recovery(self, *, utc_ns,monotonic_ns):
        if self._stopping or self._terminal:raise LaneStopped('venue archive ingress stopped')
        if self._recovery_request is None or self._recovery_submitted:
            raise ValueError('one configured startup recovery command required')
        integer(utc_ns,'recovery UTC');integer(monotonic_ns,'recovery monotonic')
        if max(utc_ns,monotonic_ns)>2**63-1:raise ValueError('recovery observation bound')
        body=canonical(dict(cutoff_pin=asdict(self._recovery_request.cutoff_pin),ledger_pin=None if self._recovery_request.ledger_pin is None else asdict(self._recovery_request.ledger_pin),
            segments=[dict(path=str(s.path),context_sha256=s.context_sha256,manifest_sha256=s.manifest_sha256) for s in self._recovery_request.segments]))
        request=RawRecord(Receipt(self.identity['capture_id'],self.identity['process_session_id'],
            'control:cutoff-recovery-request',0,utc_ns,monotonic_ns,self._last_observation[2]),
            *self.writer.identity,'control','cutoff_recovery_request','control',body,self.provenance)
        future=asyncio.get_running_loop().create_future()
        try:ticket=self.queue.offer(request.to_bytes(),extra_bytes=4096)
        except Exception:
            future.cancel();self._failure('cutoff_recovery_queue_admission_failed',global_scope=True);raise
        self._pending[ticket]=_Pending(request.record_id,request.receipt,future,timing_epoch=self._last_observation[2],recovery=True)
        self._recovery_submitted=True;self._last_submitted_mono=monotonic_ns;self._wakeup.set()
        return future

    def _resume_cutoffs(self,request):
        value=strict_json(request.body)
        segments=tuple(RecoverySegment(Path(s['path']),s['context_sha256'],s['manifest_sha256']) for s in value['segments'])
        prior=lambda:iter_recovery_prefix(segments,venue=self.identity['venue'],environment=self.identity['environment'],
            capture_id=self.identity['capture_id'],process_session_id=self.identity['process_session_id'],
            max_records=self.snapshot_limits.max_records,max_scan_bytes=self.snapshot_limits.max_scan_bytes)
        expected_ledger=None if value['ledger_pin'] is None else LedgerPin(**value['ledger_pin'])
        from types import SimpleNamespace
        empty=SimpleNamespace(identity=self.writer.identity,limits=self.writer.limits,encoded_records=lambda **kwargs:iter(()))
        args=dict(ledger_id=self._cutoff_ledger_id,capture_scope=self._cutoff_scope,
            capture_id=self.identity['capture_id'],process_session_id=self.identity['process_session_id'],provenance=self.provenance,
            prior_records=prior,max_scan_records=self.snapshot_limits.max_records,max_scan_bytes=self.snapshot_limits.max_scan_bytes)
        source=DurableLedger(empty,**args)
        if source.recovery_state()[0]!=expected_ledger:raise ValueError('startup ledger knowledge differs from expected pin')
        ledger=DurableLedger(self.writer,expected_pin=expected_ledger,**args)
        ledger.complete_native_mappings(utc_ns=request.receipt.receive_utc_ns,monotonic_ns=request.receipt.monotonic_ns,
            clock_epoch=request.receipt.clock_epoch)
        native_coordinator=CutoffCoordinator(self.writer,capture_scope=self._cutoff_scope,capture_id=self.identity['capture_id'],
            process_session_id=self.identity['process_session_id'],provenance=self.provenance,prior_records=prior,
            expected_prior_pin=CutoffPin(**value['cutoff_pin']))
        ledger.complete_native_discoveries(native_coordinator,utc_ns=request.receipt.receive_utc_ns,
            monotonic_ns=request.receipt.monotonic_ns,clock_epoch=request.receipt.clock_epoch)
        result=reconcile_cutoffs(self.writer,segments,CutoffPin(**value['cutoff_pin']),capture_scope=self._cutoff_scope,
            capture_id=self.identity['capture_id'],process_session_id=self.identity['process_session_id'],provenance=self.provenance,
            utc_ns=request.receipt.receive_utc_ns,monotonic_ns=request.receipt.monotonic_ns,clock_epoch=request.receipt.clock_epoch,
            max_segments=self.snapshot_limits.max_segments,max_records=self.snapshot_limits.max_records,max_scan_bytes=self.snapshot_limits.max_scan_bytes)
        coordinator=CutoffCoordinator(self.writer,capture_scope=self._cutoff_scope,capture_id=self.identity['capture_id'],
            process_session_id=self.identity['process_session_id'],provenance=self.provenance,prior_records=prior,
            expected_prior_pin=CutoffPin(**value['cutoff_pin']))
        self._cutoffs=ClockBoundCutoffs(self._clock,self.ingress,coordinator,ledger,recovery=result)
        for record in prior():self._source_records+=1;self._source_bytes+=len(record.to_bytes())+64
        return result,ledger.pin(allow_empty=True)

    def submit_clock(self, sample, source):
        """Queue one measured clock pair in FIFO order with ordinary raw writes.

        The returned assessment is historical measured alignment; consumers must
        also check timing_valid and current freshness before using its anchor.
        Source acquisition/parser trust and snapshot integration remain separate.
        """
        self._require_clock()
        validate_clock_source(source,sample,self.writer.identity,self.identity['capture_id'],
                              self.identity['process_session_id'],self.provenance,self._clock_submitted)
        if self._last_submitted_mono is not None and sample.monotonic_ns < self._last_submitted_mono:
            self._failure('receipt_monotonic_regression',source,global_scope=True)
            raise LaneStopped('clock request regressed behind admitted ingress')
        future = asyncio.get_running_loop().create_future()
        try:
            ticket = self.queue.offer(source.to_bytes(),extra_bytes=4096)
        except Exception:
            future.cancel()
            self._failure('clock_queue_admission_failed',source,global_scope=True)
            raise
        self._pending[ticket] = _Pending(source.record_id,source.receipt,future,sample,self._last_observation[2])
        self._clock_submitted += 1
        self._last_submitted_mono = sample.monotonic_ns
        self._wakeup.set()
        return future

    def submit_clock_check(self, *, utc_ns, monotonic_ns, boot_id):
        """Queue a paired observation; it never refreshes reference evidence.

        Stamps are captured by the producer before admission, not when the disk
        worker eventually processes this request. The request envelope is only
        queue storage; DurableClock commits its exact observations in the check
        control. An unaligned check can complete durably while raw capture drains
        or continues; alignment is separate from durable completion timing.
        """
        self._require_clock()
        if not self._clock_submitted:
            raise ValueError('admit a measured clock sample before checking alignment')
        integer(utc_ns,'checked UTC');integer(monotonic_ns,'checked monotonic')
        text(boot_id,'checked boot')
        if max(utc_ns,monotonic_ns) > 2**63-1 or len(boot_id.encode()) > 128:
            raise ValueError('clock request integer/identity bound')
        epoch = self._last_observation[2]
        request = RawRecord(Receipt(self.identity['capture_id'],self.identity['process_session_id'],
            'control:clock-check-request',self._clock_checks_submitted,utc_ns,monotonic_ns,epoch),
            *self.writer.identity,'control','clock_check_request','control',canonical(dict(boot_id=boot_id)),self.provenance)
        if self._last_submitted_mono is not None and monotonic_ns < self._last_submitted_mono:
            self._failure('receipt_monotonic_regression',request,global_scope=True)
            raise LaneStopped('clock check regressed behind admitted ingress')
        future = asyncio.get_running_loop().create_future()
        try:
            ticket = self.queue.offer(request.to_bytes(),extra_bytes=4096)
        except Exception:
            future.cancel()
            self._failure('clock_queue_admission_failed',request,global_scope=True)
            raise
        self._pending[ticket] = _Pending(request.record_id,request.receipt,future,timing_epoch=epoch,clock_check=True)
        self._clock_checks_submitted += 1
        self._last_submitted_mono = monotonic_ns
        self._wakeup.set()
        return future

    def _cutoff_streams(self, streams):
        if not isinstance(streams,(tuple,list)) or len(streams)>self.max_connections:
            raise ValueError('bounded local cutoff stream vector required')
        if any(not isinstance(s,StreamKey) or (s.capture_id,s.process_session_id)!=(
                self.identity['capture_id'],self.identity['process_session_id']) or
                s.connection_id.startswith('control:') or len(canonical(s.connection_id))>130 for s in streams):
            raise ValueError('local native cutoff streams required')
        if len(set(streams))!=len(streams):raise ValueError('duplicate cutoff stream')
        return [asdict(s) for s in streams]

    def _submit_cutoff(self, operation, payload, *, utc_ns, monotonic_ns, boot_id):
        self._require_clock()
        if self._cutoff_scope is None or not self._clock_submitted:
            raise ValueError('configured cutoff owner and admitted measurement required')
        integer(utc_ns,'cutoff request UTC');integer(monotonic_ns,'cutoff request monotonic')
        text(boot_id,'cutoff request boot')
        if max(utc_ns,monotonic_ns)>2**63-1 or len(boot_id.encode())>128:
            raise ValueError('cutoff request observation bound')
        if self._admission_count+self._cutoff_commands_submitted>=2**63-1:
            self._failure('cutoff_ingress_bound',global_scope=True)
            raise QueueCapacity('cutoff ingress ordinal bound')
        request=RawRecord(Receipt(self.identity['capture_id'],self.identity['process_session_id'],
            'control:cutoff-request',self._cutoff_commands_submitted,utc_ns,monotonic_ns,self._last_observation[2]),
            *self.writer.identity,'control','cutoff_request','control',
            canonical(dict(operation=operation,boot_id=boot_id,payload=payload)),self.provenance)
        if self._last_submitted_mono is not None and monotonic_ns<self._last_submitted_mono:
            self._failure('receipt_monotonic_regression',request,global_scope=True)
            raise LaneStopped('cutoff request regressed behind admitted ingress')
        future=asyncio.get_running_loop().create_future()
        try:
            ticket=self.queue.offer(request.to_bytes(),extra_bytes=4096)
        except Exception:
            future.cancel();self._failure('cutoff_queue_admission_failed',request,global_scope=True)
            raise
        self._pending[ticket]=_Pending(request.record_id,request.receipt,future,
            timing_epoch=self._last_observation[2],cutoff_operation=operation)
        self._cutoff_commands_submitted+=1
        self._last_submitted_mono=monotonic_ns
        self._wakeup.set()
        return future

    def submit_cutoff_allocation(self, cutoff_utc_ns, required_streams, dependencies, *, utc_ns, monotonic_ns, boot_id):
        """Queue one future minute allocation; await its result before closing.

        Only one unresolved allocation is allowed per lane. Failed/missing
        allocations resolve durably and permit the next consecutive minute.
        """
        integer(cutoff_utc_ns,'cutoff UTC label')
        streams=self._cutoff_streams(required_streams)
        if not streams or cutoff_utc_ns>2**63-1 or cutoff_utc_ns%MINUTE_NS:
            raise ValueError('minute cutoff and nonempty required streams required')
        if (self._cutoff_active is not None or self._cutoff_allocations_submitted>=10000 or
                (self._cutoff_last_utc is not None and cutoff_utc_ns!=self._cutoff_last_utc+MINUTE_NS)):
            raise ValueError('one unresolved cutoff, consecutive minute and bounded scope required')
        if not isinstance(dependencies,dict) or set(dependencies)!={'config','universe','metadata','required_feeds'}:
            raise ValueError('explicit cutoff dependency versions required')
        for name,value in dependencies.items():sha256(value,name)
        payload=dict(cutoff_utc_ns=cutoff_utc_ns,required_streams=streams,dependencies=dependencies)
        future=self._submit_cutoff('allocate',payload,utc_ns=utc_ns,monotonic_ns=monotonic_ns,boot_id=boot_id)
        self._cutoff_active=dict(cutoff_id=self._cutoff_allocations_submitted,state='allocating')
        self._cutoff_allocations_submitted+=1
        self._cutoff_last_utc=cutoff_utc_ns
        return future

    def submit_cutoff_close(self, cutoff_id, healthy_streams, *, utc_ns, monotonic_ns, boot_id):
        """Queue a barrier at this exact observation, ordered with raw receipts."""
        integer(cutoff_id,'cutoff identity')
        health=self._cutoff_streams(healthy_streams)
        if self._cutoff_active!={'cutoff_id':cutoff_id,'state':'allocated'}:
            raise ValueError('await a successful allocation before one close')
        integer(monotonic_ns,'cutoff close monotonic')
        if monotonic_ns<self._cutoff_deadline_ns:
            raise ValueError('cutoff close precedes its saved monotonic deadline')
        payload=dict(cutoff_id=cutoff_id,healthy_streams=health)
        future=self._submit_cutoff('close',payload,utc_ns=utc_ns,monotonic_ns=monotonic_ns,boot_id=boot_id)
        self._cutoff_active=dict(cutoff_id=cutoff_id,state='closing')
        return future

    def submit_cutoff_failure(self, cutoff_id, *, utc_ns, monotonic_ns, boot_id):
        """Resolve an allocated timer stopped before its barrier; no fake close."""
        integer(cutoff_id,'cutoff identity')
        if self._cutoff_active!={'cutoff_id':cutoff_id,'state':'allocated'}:
            raise ValueError('await a successful allocation before failure')
        future=self._submit_cutoff('fail',dict(cutoff_id=cutoff_id),utc_ns=utc_ns,
            monotonic_ns=monotonic_ns,boot_id=boot_id)
        self._cutoff_active=dict(cutoff_id=cutoff_id,state='failing')
        return future

    def halt_snapshot_schedule(self, reason):
        """Visible venue-local capture halt when bounded scheduling cannot run."""
        if reason not in {'snapshot_handoff_backlog','snapshot_schedule_failure','snapshot_schedule_clock_invalid',
                          'snapshot_publication_failure'}:
            raise ValueError('explicit snapshot schedule failure required')
        self._failure(reason,global_scope=True)

    def _cutoff_owners(self):
        if self._cutoffs is None:
            coordinator=CutoffCoordinator(self.writer,capture_scope=self._cutoff_scope,
                capture_id=self.identity['capture_id'],process_session_id=self.identity['process_session_id'],
                provenance=self.provenance)
            ledger=DurableLedger(self.writer,ledger_id=self._cutoff_ledger_id,capture_scope=self._cutoff_scope,
                capture_id=self.identity['capture_id'],process_session_id=self.identity['process_session_id'],
                provenance=self.provenance)
            self._cutoffs=ClockBoundCutoffs(self._clock,self.ingress,coordinator,ledger)
        return self._cutoffs

    def _owner_plans(self):
        owners=self._cutoff_owners()
        return OwnerPlans(self.writer,capture_scope=self._cutoff_scope,capture_id=self.identity['capture_id'],
            process_session_id=self.identity['process_session_id'],provenance=self.provenance,
            # Owner control admission has its own bounded scan budget. Snapshot input
            # limits govern publication, not whether legacy raw cutoffs can allocate.
            prior_records=owners.ledger.prior_records,max_plan_bytes=self.snapshot_limits.max_payload_bytes,
            max_metadata_bytes=self.snapshot_limits.max_metadata_bytes)

    def submit_owner_plan(self,plan,*,utc_ns,monotonic_ns):
        """Freeze a managed owner plan on the disk FIFO before allocating its cutoff."""
        from .replay import SnapshotPlan
        from .publisher import _charge
        self._require_clock()
        if self._cutoff_scope is None or not self._clock_submitted:raise ValueError('configured measured cutoff scope required')
        if not isinstance(plan,SnapshotPlan) or len(plan.bindings)>self.snapshot_limits.max_books:raise ValueError('bounded owner plan required')
        _charge(plan,1024*1024)
        integer(utc_ns,'owner UTC');integer(monotonic_ns,'owner monotonic')
        if max(utc_ns,monotonic_ns)>2**63-1:raise ValueError('owner observation bound')
        if self._last_submitted_mono is not None and monotonic_ns<self._last_submitted_mono:raise ValueError('owner observation regressed')
        if self._owner_plan_requests>=10000:
            self._failure('owner_plan_request_bound',global_scope=True);raise QueueCapacity('owner plan request bound')
        request=RawRecord(Receipt(self.identity['capture_id'],self.identity['process_session_id'],'control:owner-plan-request',
            self._owner_plan_requests,utc_ns,monotonic_ns,self._last_observation[2]),*self.writer.identity,
            'control','owner_plan_request','control',canonical(asdict(plan)),self.provenance)
        future=asyncio.get_running_loop().create_future()
        try:ticket=self.queue.offer(request.to_bytes(),extra_bytes=4096)
        except Exception:
            future.cancel();self._failure('owner_plan_queue_admission_failed',global_scope=True);raise
        self._pending[ticket]=_Pending(request.record_id,request.receipt,future,timing_epoch=self._last_observation[2],owner_plan=True)
        self._owner_plan_requests+=1;self._last_submitted_mono=monotonic_ns;self._wakeup.set()
        return future

    def _execute_owner_plan(self,request):
        from .snapshots import plan_from_descriptor
        r=request.receipt
        return self._owner_plans().declare(plan_from_descriptor(strict_json(request.body)),utc_ns=r.receive_utc_ns,
            monotonic_ns=r.monotonic_ns,clock_epoch=r.clock_epoch)

    def submit_native_gap(self, plan, binding_index, discovery_record_id, *, utc_ns,monotonic_ns):
        """Queue a durable native discovery for independently derived open correction."""
        return self._submit_native_mapping(plan,binding_index,discovery_record_id,'gap',utc_ns,monotonic_ns)

    def submit_deribit_end(self,plan,binding_index,discovery_record_id,*,utc_ns,monotonic_ns):
        """Queue independently replayed Deribit connection-end negative knowledge."""
        return self._submit_native_mapping(plan,binding_index,discovery_record_id,'deribit_end',utc_ns,monotonic_ns)

    def submit_native_recovery(self,plan,binding_index,correction_id,*,utc_ns,monotonic_ns):
        """Queue recovery selection; a None mapping leaves the existing gap open."""
        return self._submit_native_mapping(plan,binding_index,correction_id,'recovery',utc_ns,monotonic_ns)

    def _submit_native_mapping(self,plan,binding_index,identity,operation,utc_ns,monotonic_ns):
        from .replay import SnapshotPlan
        from .publisher import _charge
        self._require_clock()
        if self._cutoff_scope is None or not self._clock_submitted:raise ValueError('configured cutoff scope and measurement required')
        if not isinstance(plan,SnapshotPlan) or len(plan.bindings)>self.snapshot_limits.max_books:
            raise ValueError('bounded native gap snapshot plan required')
        integer(binding_index,'native gap binding');sha256(identity,'native mapping identity')
        if binding_index>=len(plan.bindings):raise ValueError('native gap binding differs from plan')
        binding=plan.bindings[binding_index]
        if binding.ws_source.process_session_id==self.identity['process_session_id'] and plan.config_sha256!=self.provenance.config_sha256:
            raise ValueError('native gap plan differs from current capture config')
        _charge(plan,1024*1024)
        integer(utc_ns,'gap UTC');integer(monotonic_ns,'gap monotonic')
        if max(utc_ns,monotonic_ns)>2**63-1:raise ValueError('gap observation bound')
        if self._last_submitted_mono is not None and monotonic_ns<self._last_submitted_mono:raise ValueError('gap observation regressed')
        if self._gap_requests_submitted>=10000:
            self._failure('native_gap_request_bound',global_scope=True);raise QueueCapacity('native gap request bound')
        request=RawRecord(Receipt(self.identity['capture_id'],self.identity['process_session_id'],'control:gap-mapping-request',
            self._gap_requests_submitted,utc_ns,monotonic_ns,self._last_observation[2]),*self.writer.identity,'control',
            'gap_mapping_request','control',canonical(dict(plan=asdict(plan),binding_index=binding_index,
            operation=operation,**({'discovery_record_id':identity} if operation in {'gap','deribit_end'} else {'correction_id':identity}))),self.provenance)
        future=asyncio.get_running_loop().create_future()
        try:ticket=self.queue.offer(request.to_bytes(),extra_bytes=4096)
        except Exception:
            future.cancel();self._failure('native_gap_queue_admission_failed',global_scope=True);raise
        self._pending[ticket]=_Pending(request.record_id,request.receipt,future,timing_epoch=self._last_observation[2],gap_mapping=True)
        self._gap_requests_submitted+=1;self._last_submitted_mono=monotonic_ns;self._wakeup.set()
        return future

    def _execute_gap_mapping(self,request):
        from .snapshots import plan_from_descriptor
        value=strict_json(request.body);owners=self._cutoff_owners();r=request.receipt
        if value.get('operation')=='recovery':
            old=() if self._recovery_request is None else self._recovery_request.segments
            if len(old)+len(self.writer.committed)+int(bool(self.writer.active.state.records))>self.snapshot_limits.max_segments:
                raise ValueError('native recovery segment bound')
            if self.writer.active.state.records:self.writer.rotate()
            return map_native_recovery(owners.ledger,owners.coordinator,[s.path for s in old]+list(self.writer.committed),
                plan_from_descriptor(value['plan']),value['binding_index'],value['correction_id'],
                utc_ns=r.receive_utc_ns,monotonic_ns=r.monotonic_ns,clock_epoch=r.clock_epoch,limits=self.snapshot_limits)
        mapper=map_deribit_end if value.get('operation')=='deribit_end' else map_native_gap
        return mapper(owners.ledger,owners.coordinator,plan_from_descriptor(value['plan']),value['binding_index'],
            value['discovery_record_id'],utc_ns=r.receive_utc_ns,monotonic_ns=r.monotonic_ns,clock_epoch=r.clock_epoch,
            max_records=self.snapshot_limits.max_records,max_scan_bytes=self.snapshot_limits.max_scan_bytes)

    def _repair_native_discoveries(self,receipt):
        owners=self._cutoff_owners()
        return owners.ledger.complete_native_discoveries(owners.coordinator,utc_ns=receipt.receive_utc_ns,
            monotonic_ns=receipt.monotonic_ns,clock_epoch=self._clock.assessment.anchor.clock_epoch)

    def _execute_cutoff(self, request):
        """Disk-owner call only: all bound controls share the raw FIFO owner."""
        body=strict_json(request.body);r=request.receipt
        self._repair_native_discoveries(r)
        args=dict(utc_ns=r.receive_utc_ns,monotonic_ns=r.monotonic_ns,boot_id=body['boot_id'])
        payload=body['payload']
        if body['operation']=='allocate':
            self._owner_plans().check_next_allocation(payload['dependencies'],[StreamKey(**s) for s in payload['required_streams']])
            self._cutoffs.ledger.initialize(utc_ns=r.receive_utc_ns,monotonic_ns=r.monotonic_ns,
                clock_epoch=self._clock.assessment.anchor.clock_epoch)
            event=self._cutoffs.allocate(payload['cutoff_utc_ns'],
                [StreamKey(**s) for s in payload['required_streams']],payload['dependencies'],**args)
        elif body['operation']=='close':
            event=self._cutoffs.close(payload['cutoff_id'],
                healthy_streams=[StreamKey(**s) for s in payload['healthy_streams']],**args)
        else:
            event=self._cutoffs.coordinator.fail(payload['cutoff_id'],'snapshot_schedule_stopped',
                utc_ns=r.receive_utc_ns,monotonic_ns=r.monotonic_ns,
                clock_epoch=self._clock.assessment.anchor.clock_epoch)
        records=self._cutoffs.coordinator.allocations if event['kind']=='allocated' else self._cutoffs.coordinator.resolutions
        reference=records[event['payload']['cutoff_id']]
        return event,reference['record_id'],reference['hash'],self._clock.pin

    def submit_snapshot_inputs(self, close_record_id, *, utc_ns, monotonic_ns):
        """Queue a seal and freeze immutable inputs for the latest closed cutoff.

        The caller may publish/replay the returned segments on a separate worker
        while raw capture continues. A retry for the same close retains the first
        frozen input set and clock pin, even after later raw/clock controls arrive.
        Only one request is in flight; no publication or five-second guarantee is
        implied by this durable-input acknowledgement.
        """
        self._require_recovery()
        if self._stopping or self._terminal:raise LaneStopped('venue archive ingress stopped')
        sha256(close_record_id,'snapshot close identity')
        if self._last_closed is None or self._last_closed['close_record_id']!=close_record_id:
            raise ValueError('latest durable closed cutoff required for snapshot inputs')
        if self._snapshot_inputs_pending:raise ValueError('one snapshot input request at a time')
        integer(utc_ns,'snapshot request UTC');integer(monotonic_ns,'snapshot request monotonic')
        if max(utc_ns,monotonic_ns)>2**63-1:raise ValueError('snapshot request clock bound')
        if self._snapshot_requests_submitted>=10000:
            self._failure('snapshot_input_request_bound',global_scope=True)
            raise QueueCapacity('snapshot input request count bound; rotate scope')
        request=RawRecord(Receipt(self.identity['capture_id'],self.identity['process_session_id'],
            'control:snapshot-input-request',self._snapshot_requests_submitted,utc_ns,monotonic_ns,
            self._last_observation[2]),*self.writer.identity,'control','snapshot_input_request','control',
            canonical(dict(self._last_closed,origin=asdict(SnapshotOrigin(*self.writer.identity,
                self.identity['capture_id'],self.identity['process_session_id'],self._cutoff_scope,
                self._last_observation[2])))),self.provenance)
        if self._last_submitted_mono is not None and monotonic_ns<self._last_submitted_mono:
            self._failure('receipt_monotonic_regression',request,global_scope=True)
            raise LaneStopped('snapshot request regressed behind admitted ingress')
        future=asyncio.get_running_loop().create_future()
        try:
            ticket=self.queue.offer(request.to_bytes(),extra_bytes=4096)
        except Exception:
            future.cancel();self._failure('snapshot_input_queue_admission_failed',request,global_scope=True)
            raise
        self._pending[ticket]=_Pending(request.record_id,request.receipt,future,
            timing_epoch=self._last_observation[2],snapshot_inputs=True)
        self._snapshot_inputs_pending=True
        self._snapshot_requests_submitted+=1
        self._last_submitted_mono=monotonic_ns
        self._wakeup.set()
        return future

    def _seal_snapshot_inputs(self, request):
        self._repair_native_discoveries(request.receipt)
        """Disk-owner call: seal before exposing any path to a live publisher."""
        value=strict_json(request.body)
        if self._snapshot_inputs is not None and self._snapshot_inputs[0]==value:
            return self._snapshot_inputs
        old=() if self._recovery_request is None else self._recovery_request.segments
        expected=len(old)+len(self.writer.committed)+int(bool(self.writer.active.state.records))
        if (expected>self.snapshot_limits.max_segments or self._source_records+self.writer.state.records>self.snapshot_limits.max_records or
                self._source_bytes+self.writer.state.valid_bytes>self.snapshot_limits.max_scan_bytes):
            raise QueueCapacity('snapshot committed prefix bound; checkpoint/session required')
        self.writer.rotate()
        paths=tuple(self.writer.committed)
        if not paths or len(paths)>self.snapshot_limits.max_segments or len(paths)!=len(self.writer.references):
            raise ValueError('bounded committed snapshot inputs required')
        # The close was produced by this lane. The rotating writer verifies all
        # sealed bytes; later publication verifies its exact close hash/plan.
        references=tuple(SnapshotSegmentInput(path,ref['context_sha256'],ref['manifest_sha256'])
                         for path,ref in zip(paths,self.writer.references))
        references=tuple(SnapshotSegmentInput(s.path,s.context_sha256,s.manifest_sha256) for s in old)+references
        self._snapshot_inputs=(value,references,self._clock.pin)
        return self._snapshot_inputs

    def submit_snapshot_delivery(self, inputs, receipt_body, *, utc_ns, monotonic_ns):
        """Archive exact canonical publisher report after this lane's handoff.

        Call with fresh archive-admission clocks, not the older publication
        completion clocks. Reported success is a claim; artifact verification
        and correction mapping remain separate. No filesystem read on ingress.
        """
        if self._stopping or self._terminal:raise LaneStopped('venue archive ingress stopped')
        if not isinstance(inputs,DurableSnapshotInputs):raise ValueError('typed delivery inputs required')
        value=validate_delivery(receipt_body)
        if (not isinstance(inputs.origin,SnapshotOrigin) or not isinstance(inputs.clock_pin,ClockPin) or
                type(inputs.timing_valid) is not bool or type(inputs.segments) is not tuple or
                len(inputs.segments)>min(self.snapshot_limits.max_segments,1024)):
            raise ValueError('bounded delivery input identity required')
        sha256(inputs.close_record_id,'delivery close');sha256(inputs.close_sha256,'delivery close hash')
        for segment in inputs.segments:
            if not isinstance(segment,SnapshotSegmentInput):raise ValueError('typed delivery segment required')
            sha256(segment.context_sha256,'delivery context');sha256(segment.manifest_sha256,'delivery manifest')
        base=delivery_base(inputs)
        if any(value[name]!=item for name,item in base.items()):raise ValueError('delivery differs from pinned inputs')
        handoff=self._delivery_handoffs.get(inputs.close_record_id)
        if handoff is None or handoff!=(digest(canonical(base)),inputs.timing_valid):
            raise ValueError('delivery requires this lane immutable handoff')
        if value['outcome']=='published' and not inputs.timing_valid:raise ValueError('untimely handoff cannot report success')
        receipt_hash=digest(receipt_body)
        if inputs.close_record_id in self._delivery_receipts and self._delivery_receipts[inputs.close_record_id]!=receipt_hash:
            raise ValueError('immutable delivery report conflicts')
        integer(utc_ns,'delivery archive UTC');integer(monotonic_ns,'delivery archive monotonic')
        if max(utc_ns,monotonic_ns)>2**63-1:raise ValueError('delivery archive clock bound')
        if value['completed']['valid'] and monotonic_ns<value['completed']['monotonic_ns']:
            raise ValueError('delivery admission precedes reported completion')
        if self._delivery_requests_submitted>=10000:
            self._failure('snapshot_delivery_request_bound',global_scope=True)
            raise QueueCapacity('delivery request count bound; rotate scope')
        request=RawRecord(Receipt(self.identity['capture_id'],self.identity['process_session_id'],
            'control:snapshot-delivery-request',self._delivery_requests_submitted,utc_ns,monotonic_ns,
            self._last_observation[2]),*self.writer.identity,'control','snapshot_delivery_request','control',
            receipt_body,self.provenance)
        maximum=replace(request,receipt=replace(request.receipt,connection_id='control:snapshot-deliveries:'+self._cutoff_scope,
            receive_seq=9999,clock_epoch=self.identity['process_session_id']+':clock:1023'),
            channel='snapshot_deliveries:'+self._cutoff_scope)
        if len(maximum.to_bytes())>self.writer.limits.max_record_bytes:raise ValueError('delivery raw envelope byte bound')
        if self._last_submitted_mono is not None and monotonic_ns<self._last_submitted_mono:
            self._failure('receipt_monotonic_regression',request,global_scope=True)
            raise LaneStopped('delivery request regressed behind admitted ingress')
        future=asyncio.get_running_loop().create_future()
        try:ticket=self.queue.offer(request.to_bytes(),extra_bytes=4096)
        except Exception:
            future.cancel();self._failure('snapshot_delivery_queue_admission_failed',request,global_scope=True)
            raise
        self._pending[ticket]=_Pending(request.record_id,request.receipt,future,
            timing_epoch=self._last_observation[2],snapshot_delivery=True)
        self._delivery_requests_submitted+=1
        self._delivery_receipts[inputs.close_record_id]=receipt_hash
        self._last_submitted_mono=monotonic_ns
        self._wakeup.set()
        return future

    def _append_delivery(self, request):
        """Raw disk owner only: immutable report identity and idempotent retries."""
        value=validate_delivery(request.body);close=value['close_record_id'];body_hash=digest(request.body)
        if close in self._deliveries:
            old=self._deliveries[close]
            if old[2]!=body_hash:raise ValueError('immutable delivery report conflicts')
            return old
        if len(self._deliveries)>=10000 or self._delivery_bytes+len(request.body)>64*1024*1024:
            raise QueueCapacity('delivery report retained byte/count bound')
        record=replace(request,receipt=replace(request.receipt,
            connection_id='control:snapshot-deliveries:'+self._cutoff_scope,receive_seq=len(self._deliveries),
            clock_epoch=self._clock.assessment.anchor.clock_epoch),channel='snapshot_deliveries:'+self._cutoff_scope)
        self.writer.append(record)
        result=(record.record_id,digest(record.to_bytes()),body_hash,record.receipt.receive_seq)
        self._deliveries[close]=result;self._delivery_bytes+=len(request.body)
        return result

    def stop(self, reason='operator_stop'):
        if reason not in {'operator_stop','budget_complete','shutdown'}:
            raise ValueError('explicit graceful stop reason required')
        self._stop_reason, self._stopping = reason, True
        self.queue.close()
        self._wakeup.set()

    async def _disk(self, function, *args):
        future = asyncio.get_running_loop().run_in_executor(self._executor, function, *args)
        while True:
            try:
                return await asyncio.shield(future)
            except asyncio.CancelledError:
                self._cancelled = True
                if not self._stopping:
                    self._failure('worker_cancelled', global_scope=True)
                if future.done():
                    try:
                        return future.result()
                    except asyncio.CancelledError as exc:
                        raise DurabilityUnknown('disk operation cancelled without a confirmed result') from exc

    def _watermarks(self):
        return [dict(stream=asdict(stream), last_submitted_seq=seq,
                     last_durable=self._durable.get(stream)) for stream, seq in sorted(self._submitted.items(), key=lambda value:str(value[0]))]

    def _clock_status(self):
        return None if self._clock is None else dict(submitted=self._clock_submitted,
            checks_submitted=self._clock_checks_submitted,pin=asdict(self._clock.pin),
            cutoff_commands_submitted=self._cutoff_commands_submitted,
            cutoff_scope=self._cutoff_scope,cutoff_allocations_submitted=self._cutoff_allocations_submitted,
            gap_requests_submitted=self._gap_requests_submitted,
            cutoff_history_count=self._history_count,recovery_ready=self._recovery_ready,
            recovery_submitted=self._recovery_submitted,recovery_marker_id=None if self._recovery_result is None else self._recovery_result.marker_record_id,
            recovery_next_utc_ns=None if self._recovery_result is None else self._recovery_result.next_cutoff_utc_ns,
            active_cutoff=None if self._cutoff_active is None else dict(self._cutoff_active),
            last_closed_record_id=None if self._last_closed is None else self._last_closed['close_record_id'],
            snapshot_requests_submitted=self._snapshot_requests_submitted,
            snapshot_inputs_pending=self._snapshot_inputs_pending,
            delivery_requests_submitted=self._delivery_requests_submitted,delivery_reports_durable=len(self._deliveries),
            delivery_handoffs_without_report=len(self._delivery_handoffs)-len(self._deliveries))

    def _timing(self, lease, receipt, *, epoch=None):
        stamp, valid, _ = self._observation()
        latency = stamp[1]-receipt.monotonic_ns
        try:
            queue_valid = self.queue.health()['clock_valid']
        except Exception:
            queue_valid = False
        valid = (lease.clock_valid and queue_valid and valid and latency >= 0 and
                 stamp[2] == (receipt.clock_epoch if epoch is None else epoch) and
                 (self._clock is not None or
                  abs((stamp[0]-receipt.receive_utc_ns)-latency) <= self.clock_tolerance_ns))
        if not valid:
            self._clock_failed = True
        timely = valid and not lease.late and latency < self.queue.limits.max_age_ns
        if not timely:
            self._failure('receive_durable_deadline_or_clock_invalid',global_scope=True)
        return latency if valid else None,timely

    async def _control(self, kind, payload):
        stamp, valid, source = self._observation()
        if not valid:
            self._failure('runtime_clock_invalid', global_scope=True)
        body = canonical(dict(schema='dc.runtime-control/2', kind=kind, venue=self.identity['venue'],
                              control_time_valid=valid, control_time_source=source,
                              timing_basis=self.timing_basis,
                              payload=payload))
        record = RawRecord(Receipt(self.identity['capture_id'], self.identity['process_session_id'],
                           'control:runtime:'+self.identity['venue'], self._control_seq, *stamp),
                           self.identity['venue'], self.identity['environment'], 'control', 'runtime_events', 'control',
                           body, self.provenance)
        if len(record.to_bytes()) > self.writer.limits.max_record_bytes:
            raise ValueError('bounded runtime control does not fit writer')
        await self._disk(self.writer.append, record)
        self._control_seq += 1
        self.control_records.append(record.record_id)
        if kind == 'ingress_halted':
            self._failure_control_count += 1

    async def _emergency_control(self):
        stamp, valid, source = self._observation()
        body = canonical(dict(schema='dc.runtime-control/2', kind='writer_unconfirmed',
            venue=self.identity['venue'], control_time_valid=valid,
            control_time_source=source,
            timing_basis=self.timing_basis,
            payload=dict(writer_error=self.writer_error, first_failure=self._global_failure,
                         unpersisted_controls=[event for event in self._controls if event is not self._global_failure],
                         watermarks=self._watermarks(), clock=self._clock_status(), unconfirmed_records=len(self._pending))))
        record = RawRecord(Receipt(self.identity['capture_id'], self.identity['process_session_id'],
                           'control:emergency:'+self.identity['venue'], 0, *stamp),
                           self.identity['venue'], self.identity['environment'], 'control', 'runtime_events', 'control',
                           body, self.provenance)
        await self._disk(self.emergency_writer.append, record)
        self.emergency_control_id = record.record_id

    async def run(self):
        if self._started:
            raise LaneStopped('archive lane can run only once per process session')
        self._started = self._running = True
        try:
            while True:
                while self._controls:
                    event = self._controls[0]
                    await self._control('ingress_halted', event)
                    self._controls.popleft()
                if self.queue.queued_count:
                    try:
                        lease = await self.queue.get()
                    except asyncio.CancelledError:
                        self._cancelled = True
                        self._failure('worker_cancelled', global_scope=True)
                        continue
                    pending = self._pending[lease.ordinal]
                    if (lease.late and not pending.recovery) or not lease.clock_valid:
                        self._failure('persist_backlog_or_clock_invalid', global_scope=True)
                    record = RawRecord.from_bytes(lease.payload)
                    if pending.recovery:
                        result,pin=await self._disk(self._resume_cutoffs,record)
                        stamp,valid,_=self._observation();latency=stamp[1]-record.receipt.monotonic_ns
                        valid=(valid and lease.clock_valid and self.queue.health()['clock_valid'] and
                            stamp[2]==pending.timing_epoch and 0<=latency<=self.max_recovery_ns)
                        self._recovery_result=result
                        self._recovery_ready=valid
                        self._history_count=self._cutoff_allocations_submitted=result.next_cutoff_id
                        self._cutoff_last_utc=None if result.next_cutoff_utc_ns is None else result.next_cutoff_utc_ns-MINUTE_NS
                        if not valid:self._failure('cutoff_recovery_deadline_or_clock_invalid',global_scope=True)
                        self.queue.ack(lease);self._pending.pop(lease.ordinal)
                        if not pending.future.done():pending.future.set_result(DurableRecoveryAck(result,pin,latency if valid else None,valid))
                        continue
                    if pending.snapshot_delivery:
                        fields=await self._disk(self._append_delivery,record)
                        latency,timely=self._timing(lease,record.receipt,epoch=pending.timing_epoch)
                        self.queue.ack(lease);self._pending.pop(lease.ordinal)
                        if not pending.future.done():pending.future.set_result(DurableDeliveryAck(*fields,latency,timely))
                        continue
                    if pending.snapshot_inputs:
                        value,paths,pin=await self._disk(self._seal_snapshot_inputs,record)
                        latency,timely=self._timing(lease,record.receipt,epoch=pending.timing_epoch)
                        fields=dict(value);origin=SnapshotOrigin(**fields.pop('origin'))
                        result=DurableSnapshotInputs(**fields,origin=origin,segments=paths,clock_pin=pin,replay_limits=self.snapshot_limits,
                            receive_to_durable_ns=latency,timing_valid=timely)
                        self._delivery_handoffs[result.close_record_id]=(digest(canonical(delivery_base(result))),result.timing_valid)
                        self._snapshot_inputs_pending=False
                        self.queue.ack(lease);self._pending.pop(lease.ordinal)
                        if not pending.future.done():pending.future.set_result(result)
                        continue
                    if pending.owner_plan:
                        declaration=await self._disk(self._execute_owner_plan,record)
                        latency,timely=self._timing(lease,record.receipt,epoch=pending.timing_epoch)
                        self.queue.ack(lease);self._pending.pop(lease.ordinal)
                        if not pending.future.done():pending.future.set_result(DurableOwnerPlanAck(declaration,latency,timely))
                        continue
                    if pending.gap_mapping:
                        mapping=await self._disk(self._execute_gap_mapping,record)
                        latency,timely=self._timing(lease,record.receipt,epoch=pending.timing_epoch)
                        self.queue.ack(lease);self._pending.pop(lease.ordinal)
                        if not pending.future.done():pending.future.set_result(DurableGapAck(mapping,latency,timely))
                        continue
                    if pending.cutoff_operation is not None:
                        event,record_id,raw_hash,pin=await self._disk(self._execute_cutoff,record)
                        latency,timely=self._timing(lease,record.receipt,epoch=pending.timing_epoch)
                        self._cutoff_active=(dict(cutoff_id=event['payload']['cutoff_id'],state='allocated')
                                             if event['kind']=='allocated' else None)
                        self._cutoff_deadline_ns=event['payload'].get('deadline_ns') if event['kind']=='allocated' else None
                        if event['kind']=='closed':
                            self._last_closed=dict(close_record_id=record_id,close_sha256=raw_hash,
                                cutoff_id=event['payload']['cutoff_id'],deadline_ns=event['payload']['barrier']['deadline_ns'])
                        result=DurableCutoffAck(event,record_id,raw_hash,pin,latency,timely)
                        self.queue.ack(lease);self._pending.pop(lease.ordinal)
                        if not pending.future.done():pending.future.set_result(result)
                        continue
                    if pending.clock_sample is not None or pending.clock_check:
                        if pending.clock_check:
                            operation = partial(self._clock.check,utc_ns=record.receipt.receive_utc_ns,
                                monotonic_ns=record.receipt.monotonic_ns,boot_id=strict_json(record.body)['boot_id'],
                                ingress_ordinal=self.ingress.ordinal)
                        else:
                            operation = partial(self._clock.sample,pending.clock_sample,record,ingress_ordinal=self.ingress.ordinal)
                        assessment = await self._disk(operation)
                        latency,timely = self._timing(lease,record.receipt,epoch=pending.timing_epoch)
                        result = DurableClockAck(assessment,self._clock.pin,latency,timely)
                        self.queue.ack(lease)
                        self._pending.pop(lease.ordinal)
                        if not pending.future.done():pending.future.set_result(result)
                        continue
                    if pending.stamp_clock:
                        assessment = self._clock.assessment
                        if assessment is None:
                            raise DurabilityUnknown('preceding clock assessment is not committed')
                        record = replace(record, receipt=replace(record.receipt,
                            clock_epoch=assessment.anchor.clock_epoch))
                    stored = await self._disk(self.ingress.append, record)
                    latency,timing_valid = self._timing(lease,stored.receipt,epoch=pending.timing_epoch)
                    receipt = dict(stream=asdict(stored.receipt.stream_key), receive_seq=stored.receipt.receive_seq,
                                   record_id=stored.record_id, raw_sha256=digest(stored.to_bytes()),
                                   monotonic_ns=stored.receipt.monotonic_ns, receive_utc_ns=stored.receipt.receive_utc_ns,
                                   clock_epoch=stored.receipt.clock_epoch)
                    self._durable[stored.receipt.stream_key] = receipt
                    self.queue.ack(lease)
                    self._durable_count += 1
                    self._pending.pop(lease.ordinal)
                    if not pending.future.done():
                        pending.future.set_result(DurableAck(stored.record_id, receipt['raw_sha256'],
                            strict_json(stored.native_fields_json)['collector_ingress_ordinal'], latency, timing_valid,
                            stored.receipt.clock_epoch))
                    continue
                if self._stopping:
                    break
                self._wakeup.clear()
                try:
                    await self._wakeup.wait()
                except asyncio.CancelledError:
                    self._cancelled = True
                    self._failure('worker_cancelled', global_scope=True)
            if self._pending:
                raise DurabilityUnknown('pending receipts remain outside the owned queue; cannot finalize capture')
            if self._cutoff_active is not None:
                stamp,_,_=self._observation()
                await self._disk(partial(self._cutoffs.coordinator.fail,self._cutoff_active['cutoff_id'],
                    'capture_stopped_before_cutoff',utc_ns=stamp[0],monotonic_ns=stamp[1],
                    clock_epoch=self._clock.assessment.anchor.clock_epoch))
                self._cutoff_active=None
                self._cutoff_deadline_ns=None
            await self._control('capture_stopped', dict(reason=self._stop_reason, watermarks=self._watermarks(),
                                                        clock=self._clock_status(), failures=len(self._failed_connections)+int(self._global_failure is not None)))
            # A final control-time observation can itself discover a clock fault.
            while self._controls:
                event = self._controls[0]
                await self._control('ingress_halted', event)
                self._controls.popleft()
            self.segments = await self._disk(self.writer.finish)
        except Exception as exc:
            self.writer_error = type(exc).__name__
            self._failure('writer_or_control_failure', global_scope=True)
            for pending in self._pending.values():
                if not pending.future.done():
                    pending.future.set_exception(DurabilityUnknown('raw durability unconfirmed; recover the session WAL before retrying'))
            try:
                await self._emergency_control()
            except Exception:
                pass # A fully failed disk cannot promise a durable failure record.
            sys.stderr.write(canonical(dict(event='archive_writer_failure', venue=self.identity['venue'],
                emergency_durable=self.emergency_control_id is not None, unconfirmed_records=len(self._pending))).decode()+'\n')
            # No acknowledgement/release of the unconfirmed volatile tail.
            await self._disk(self.writer.abort)
        finally:
            self._running, self._terminal = False, True
            await self._disk(self.emergency_writer.close)
            self._executor.shutdown(wait=False)
        if self._cancelled:
            raise asyncio.CancelledError
        return self.health()

    @property
    def timing_basis(self):
        return 'process_monotonic' if self._clock is not None else 'paired_utc_monotonic'

    def health(self):
        return dict(venue=self.identity['venue'], running=self._running, terminal=self._terminal,
                    timing_basis=self.timing_basis,
                    accepting=not self._stopping and not self._terminal, durable_records=self._durable_count,
                    unconfirmed_records=len(self._pending), failed_connections=len(self._failed_connections),
                    venue_failure=None if self._global_failure is None else self._global_failure['reason'],
                    writer_error=self.writer_error, runtime_controls_durable=len(self.control_records),
                    failure_controls_durable=self._failure_control_count+int(self.emergency_control_id is not None),
                    emergency_control_id=self.emergency_control_id, clock=self._clock_status(),segments=len(self.segments))
