"""Fsync clock measurement bytes before publishing their derived assessment.

Serialized owner API over Journal/SessionWriter. Replaying the complete bounded
log verifies the original measurement envelopes and recomputes every assessment.
This does not fetch OS/NTP measurements, resume a crashed process identity, or
apply clock invalidations to the separate correction ledger.
"""
from copy import deepcopy
from dataclasses import asdict, dataclass

from .clock import ClockAssessment, ClockMonitor, ClockSample
from .contracts import Provenance, RawRecord, Receipt, StreamKey, canonical, digest, integer, sha256, strict_json, text

SOURCE = 'control:clock-source'
CONTROL = 'control:clock-assessment'
CHANNEL = 'clock_assessments'
DEFAULT_POLICY = dict(sample_interval_ns=60_000_000_000, max_sample_age_ns=60_000_000_000,
                      max_offset_ns=100_000_000, step_tolerance_ns=1_000_000, max_epochs=1024)


@dataclass(frozen=True)
class ClockPin:
    count: int
    head_sha256: str | None
    last_record_id: str | None

    def __post_init__(self):
        integer(self.count,'clock pin count')
        if self.count:
            sha256(self.head_sha256,'clock pin head')
            sha256(self.last_record_id,'clock pin record')
        elif self.head_sha256 is not None or self.last_record_id is not None:
            raise ValueError('empty clock pin cannot claim a head')


@dataclass(frozen=True)
class ClockReplay:
    assessment: ClockAssessment | None
    prior_anchors: tuple
    pin: ClockPin


def _policy(value):
    policy = dict(DEFAULT_POLICY if value is None else value)
    if set(policy) != set(DEFAULT_POLICY):
        raise ValueError('complete pinned clock policy required')
    ClockMonitor('validate', **policy)
    return policy


def validate_clock_source(source, sample, identity, capture, session, provenance, sequence):
    if not isinstance(source, RawRecord) or not isinstance(sample, ClockSample):
        raise ValueError('typed clock sample and source raw required')
    if ((source.venue,source.environment) != identity or source.provenance != provenance or
            source.receipt != Receipt(capture,session,SOURCE,sequence,sample.utc_ns,sample.monotonic_ns,'measurement') or
            source.kind != 'control' or source.native_product != 'control' or source.channel != 'clock_measurement' or
            source.instrument is not None or source.native_fields_json != b'{}' or digest(source.to_bytes()) != sample.evidence_sha256):
        raise ValueError('clock measurement evidence/namespace mismatch')


def _record(sample, assessment, source, ordinal, policy, sequence, head):
    body = canonical(dict(schema='dc.clock-assessment/2', sequence=sequence, previous_sha256=head,
        policy=policy, sample=asdict(sample), ingress_ordinal=ordinal,
        source_record_id=source.record_id, assessment=asdict(assessment)))
    return RawRecord(Receipt(source.receipt.capture_id,source.receipt.process_session_id,CONTROL,sequence,
        sample.utc_ns,sample.monotonic_ns,assessment.anchor.clock_epoch),source.venue,source.environment,
        'control',CHANNEL,'control',body,source.provenance)


def _check_record(assessment, observation, policy, sequence, head, identity, capture, session, provenance):
    body = canonical(dict(schema='dc.clock-check/2',sequence=sequence,previous_sha256=head,
        policy=policy,observation=observation,assessment=asdict(assessment)))
    return RawRecord(Receipt(capture,session,CONTROL,sequence,observation['utc_ns'],observation['monotonic_ns'],
        assessment.anchor.clock_epoch),*identity,'control',CHANNEL,'control',body,provenance)


class DurableClock:
    """Only expose an assessment after both measurement and control fsync.

    Failed writes poison the owner: recover bytes offline and start a fresh
    process/session rather than guessing whether either append committed.
    A rejected clock sample also requires a fresh owner; no false recovery.
    """
    def __init__(self, journal, *, capture_id, process_session_id, provenance, policy=None,
                 max_assessments=10000):
        text(capture_id,'capture'); text(process_session_id,'process session')
        integer(max_assessments,'clock assessment bound',1)
        if not isinstance(provenance,Provenance):
            raise ValueError('typed clock provenance required')
        for connection in (SOURCE,CONTROL):
            if StreamKey(capture_id,process_session_id,connection) in journal.state.positions:
                raise ValueError('clock owner requires fresh process/session streams')
        if hasattr(journal,'descriptor'):
            identity = journal.descriptor['identity']
            if (identity['capture_id'],identity['process_session_id']) != (capture_id,process_session_id):
                raise ValueError('clock owner differs from writer session')
            if journal.descriptor['provenance'] != asdict(provenance):
                raise ValueError('clock owner provenance differs from writer')
        self.journal,self.capture,self.session,self.provenance = journal,capture_id,process_session_id,provenance
        self._policy = _policy(policy)
        self._monitor = ClockMonitor(process_session_id,**self._policy)
        self.max_assessments = max_assessments
        self._pin = ClockPin(0,None,None)
        self._assessment = None
        self._sample_count = 0
        self.poisoned = False

    @property
    def pin(self):return self._pin

    def sample(self, sample, source, *, ingress_ordinal):
        if self.poisoned:
            raise ValueError('clock owner poisoned; recover and start fresh session')
        if self._pin.count >= self.max_assessments:
            raise ValueError('clock log bound; checkpoint/session rotation required')
        validate_clock_source(source,sample,self.journal.identity,self.capture,self.session,self.provenance,self._sample_count)
        candidate = deepcopy(self._monitor)
        try:
            assessment = candidate.sample(sample,ingress_ordinal=ingress_ordinal)
        except Exception:
            self.poisoned = True
            raise
        record = _record(sample,assessment,source,ingress_ordinal,self._policy,self._pin.count,self._pin.head_sha256)
        if max(len(source.to_bytes()),len(record.to_bytes())) > self.journal.limits.max_record_bytes:
            raise ValueError('clock evidence/control exceeds writer bound')
        try:
            self.journal.append(source)
            self.journal.append(record)
        except BaseException:
            self.poisoned = True
            raise
        self._monitor,self._assessment = candidate,assessment
        self._sample_count += 1
        self._pin = ClockPin(self._pin.count+1,digest(record.to_bytes()),record.record_id)
        return assessment

    def check(self, *, utc_ns, monotonic_ns, boot_id, ingress_ordinal):
        """Commit a paired freshness/step check without refreshing NTP evidence."""
        if self.poisoned or self._pin.count >= self.max_assessments:
            raise ValueError('clock owner poisoned or log bound; start fresh session')
        observation = dict(utc_ns=utc_ns,monotonic_ns=monotonic_ns,boot_id=boot_id,ingress_ordinal=ingress_ordinal)
        candidate = deepcopy(self._monitor)
        try:
            result = candidate.check(**observation)
        except Exception:
            self.poisoned = True
            raise
        record = _check_record(result,observation,self._policy,self._pin.count,self._pin.head_sha256,
                               self.journal.identity,self.capture,self.session,self.provenance)
        if len(record.to_bytes()) > self.journal.limits.max_record_bytes:
            raise ValueError('clock check exceeds writer bound')
        try:
            self.journal.append(record)
        except BaseException:
            self.poisoned = True
            raise
        self._monitor,self._assessment = candidate,result
        self._pin = ClockPin(self._pin.count+1,digest(record.to_bytes()),record.record_id)
        return result


def replay_clock(records, *, venue, environment, capture_id, process_session_id, provenance,
                 policy=None, expected_pin, max_assessments=10000, max_scan_records=200000,
                 max_scan_bytes=256*1024*1024):
    """Verify an exact pinned complete log; incomplete/orphan source fails closed.

    records must stream verified immutable raw/WAL prefix in original order.
    No continuation of the returned historical monitor is exposed.
    """
    for name,value in [('max_assessments',max_assessments),('max_scan_records',max_scan_records),
                       ('max_scan_bytes',max_scan_bytes)]:integer(value,name,1)
    if not isinstance(expected_pin,ClockPin) or not isinstance(provenance,Provenance):
        raise ValueError('typed expected clock pin and provenance required')
    policy = _policy(policy)
    monitor = ClockMonitor(process_session_id,**policy)
    pending,assessment = None,None
    count,samples,scanned,size = 0,0,0,0
    pin = ClockPin(0,None,None)
    for record in records:
        if not isinstance(record,RawRecord):raise ValueError('verified typed raw required')
        scanned += 1; size += len(record.to_bytes())
        if scanned > max_scan_records or size > max_scan_bytes:
            raise ValueError('clock replay scan bound')
        local = (record.receipt.capture_id,record.receipt.process_session_id)==(capture_id,process_session_id)
        if local and record.receipt.connection_id == SOURCE:
            if pending is not None:raise ValueError('orphan clock measurement')
            pending = record
            continue
        if local and record.receipt.connection_id == CONTROL:
            if count >= max_assessments:
                raise ValueError('clock assessment bound')
            body = strict_json(record.body)
            if body.get('schema') == 'dc.clock-check/2':
                if pending is not None:raise ValueError('clock measurement/control pair interrupted by check')
                result = monitor.check(**body['observation'])
                expected = _check_record(result,body['observation'],policy,count,pin.head_sha256,
                                         (venue,environment),capture_id,process_session_id,provenance)
            else:
                if pending is None:raise ValueError('missing clock measurement')
                sample = ClockSample(**body['sample'])
                validate_clock_source(pending,sample,(venue,environment),capture_id,process_session_id,provenance,samples)
                result = monitor.sample(sample,ingress_ordinal=body['ingress_ordinal'])
                expected = _record(sample,result,pending,body['ingress_ordinal'],policy,count,pin.head_sha256)
                samples += 1
            if record.to_bytes() != expected.to_bytes():raise ValueError('clock control replay mismatch')
            count += 1
            pin = ClockPin(count,digest(record.to_bytes()),record.record_id)
            pending,assessment = None,result
        elif pending is not None:
            raise ValueError('clock source/control pair was interleaved')
    if pending is not None or pin != expected_pin:
        raise ValueError('incomplete clock log or pinned prefix mismatch')
    return ClockReplay(assessment,monitor.prior_anchors,pin)
