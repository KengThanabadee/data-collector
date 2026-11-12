"""Durable cutoff allocation/closure and serialized ingress barriers.

One local process and owned journal. Transport callbacks and barrier calls must
run on the same serialized executor. No cross-host monotonic comparisons.
"""
from copy import copy
from dataclasses import asdict, dataclass, replace

from .contracts import RawRecord, Receipt, StreamKey, canonical, digest, integer, sha256, strict_json, text

MINUTE_NS = 60_000_000_000
POLICY = "receive-asof/2"
CONTROL_SCHEMA = "dc.cutoff-control/2"


@dataclass(frozen=True, slots=True)
class CutoffPin:
    count: int
    head_sha256: str | None
    last_record_sha256: str | None

    def __post_init__(self):
        integer(self.count,'cutoff pin count')
        if self.count:
            sha256(self.head_sha256,'cutoff body head');sha256(self.last_record_sha256,'cutoff raw head')
        elif self.head_sha256 is not None or self.last_record_sha256 is not None:
            raise ValueError('empty cutoff pin cannot claim a head')


@dataclass(frozen=True)
class ClockAnchor:
    process_session_id: str
    clock_epoch: str
    utc_ns: int
    monotonic_ns: int
    uncertainty_ns: int
    healthy_samples: int
    epoch_started_ns: int = 0
    epoch_started_ordinal: int = 0

    def __post_init__(self):
        text(self.process_session_id, "process_session_id")
        text(self.clock_epoch, "clock_epoch")
        for name in ("utc_ns", "monotonic_ns", "uncertainty_ns", "healthy_samples", "epoch_started_ns", "epoch_started_ordinal"):
            integer(getattr(self, name), name)
        if self.epoch_started_ns > self.monotonic_ns:
            raise ValueError("clock anchor precedes epoch boundary")

    def deadline(self, utc_ns):
        deadline = self.monotonic_ns + utc_ns - self.utc_ns
        integer(deadline, "deadline")
        return deadline

    @property
    def boundary(self):
        return self.epoch_started_ns, self.epoch_started_ordinal


class IngressWriter:
    """Assign order and archive before acknowledging ingress or a barrier.

    The input RawRecord's receipt supplies the exact receipt clocks. Sequence and
    session are checked; ingress ordinal is reserved collector metadata.
    """
    def __init__(self, journal, capture_id, process_session_id, *, max_streams=4096):
        text(capture_id, "capture_id"); text(process_session_id, "process_session_id")
        integer(max_streams, "max_streams", 1)
        self.journal, self.capture_id, self.session = journal, capture_id, process_session_id
        self.max_streams, self.ordinal, self.last_mono = max_streams, 0, None
        self.positions, self.poisoned = {}, False
        # Restart is a new ingress epoch, never resume a reused identity/ordinal.
        if any(s.capture_id == capture_id and s.process_session_id == process_session_id
               for s in journal.state.positions):
            raise ValueError("new ingress requires a fresh process session")

    def _prepare(self, record):
        if self.poisoned:
            raise ValueError("ingress requires restart after write/clock failure")
        r = record.receipt
        if (r.capture_id, r.process_session_id) != (self.capture_id, self.session) or r.connection_id.startswith('control:'):
            raise ValueError("foreign ingress/control identity")
        expected = self.positions.get(r.stream_key, -1) + 1
        if r.receive_seq != expected:
            raise ValueError("ingress receipt sequence must start at zero and be consecutive")
        if r.stream_key not in self.positions and len(self.positions) >= self.max_streams:
            raise ValueError("ingress stream bound")
        if self.last_mono is not None and r.monotonic_ns < self.last_mono:
            self.poisoned = True
            raise ValueError("monotonic clock regressed; new process epoch required")
        fields = strict_json(record.native_fields_json)
        if "collector_ingress_ordinal" in fields:
            raise ValueError("reserved ingress ordinal")
        fields["collector_ingress_ordinal"] = self.ordinal
        stored = replace(record, native_fields_json=canonical(fields))
        return stored

    def _advance(self, record):
        r=record.receipt
        self.positions[r.stream_key] = r.receive_seq
        self.last_mono = r.monotonic_ns
        self.ordinal += 1

    def append(self, record):
        stored=self._prepare(record)
        try:self.journal.append(stored)
        except BaseException:
            self.poisoned=True
            raise
        self._advance(record)
        return stored

    def append_batch(self, records):
        candidate=copy(self)
        candidate.positions=dict(self.positions)
        stored=[]
        for record in records:
            stored.append(candidate._prepare(record))
            candidate._advance(record)
        try:self.journal.append_batch(tuple(stored))
        except BaseException:
            self.poisoned=True
            raise
        self.positions,self.ordinal,self.last_mono=candidate.positions,candidate.ordinal,candidate.last_mono
        return tuple(stored)

    def barrier(self, anchor, cutoff_utc_ns, *, now_monotonic_ns, healthy_streams, prior_anchors=()):
        if self.poisoned or anchor.process_session_id != self.session:
            raise ValueError("unavailable ingress/foreign anchor")
        integer(now_monotonic_ns, "now_monotonic_ns")
        deadline = anchor.deadline(cutoff_utc_ns)
        if now_monotonic_ns < deadline or (self.last_mono is not None and now_monotonic_ns < self.last_mono):
            raise ValueError("barrier before deadline/previous ingress")
        health = tuple(healthy_streams)
        if len(health) > self.max_streams or len(set(health)) != len(health) or any(
                not isinstance(s, StreamKey) or s.capture_id != self.capture_id or s.process_session_id != self.session for s in health):
            raise ValueError("bounded local stream health required")
        anchors = tuple(prior_anchors) + (anchor,)
        if len(anchors) > 1024 or any(not isinstance(a, ClockAnchor) or a.process_session_id != self.session for a in anchors):
            raise ValueError("bounded same-process clock history required")
        if len({a.clock_epoch for a in anchors}) != len(anchors) or any(
                a.boundary >= b.boundary for a,b in zip(anchors,anchors[1:])):
            raise ValueError("clock epochs require unique ordered boundaries")
        if any(a.monotonic_ns > now_monotonic_ns for a in anchors):
            raise ValueError("clock sample cannot follow the barrier")
        result = dict(anchor=asdict(anchor), deadline_ns=deadline, ordinal=self.ordinal,
                      observed_monotonic_ns=now_monotonic_ns, healthy_streams=[asdict(s) for s in health],
                      epoch_anchors=[asdict(a) for a in anchors])
        self.ordinal += 1
        self.last_mono = now_monotonic_ns
        return result


class CutoffCoordinator:
    """Replayable allocation ledger in the raw WAL; fsync precedes state advance.

    prior_records may stream verified older segments in original control order.
    Memory/record bounds fail closed until explicit checkpoint integration.
    """
    def __init__(self, journal, *, capture_scope, capture_id, process_session_id, provenance,
                 prior_records=lambda: (), max_cutoffs=10000, max_control_bytes=1024*1024,
                 max_projection_bytes=16*1024*1024, expected_prior_pin=None):
        for name, value in (("capture_scope", capture_scope), ("capture_id", capture_id), ("process_session_id", process_session_id)):
            text(value, name)
        integer(max_cutoffs, "max_cutoffs", 1); integer(max_control_bytes, "max_control_bytes", 1)
        integer(max_projection_bytes, "max_projection_bytes", 1)
        self.max_projection_bytes, self.projection_bytes = max_projection_bytes, 0
        self.journal, self.scope, self.capture_id, self.session = journal, capture_scope, capture_id, process_session_id
        self.provenance, self.max_cutoffs, self.max_control_bytes = provenance, max_cutoffs, max_control_bytes
        if not callable(prior_records):
            raise ValueError("replayable verified prior-record factory required")
        self.prior_records = prior_records
        self.allocations, self.resolutions = {}, {}
        self._control_positions = {}
        if expected_prior_pin is not None and not isinstance(expected_prior_pin,CutoffPin):
            raise ValueError('typed expected cutoff prefix pin required')
        self.control_seq, self.head, self._last_raw_sha256 = 0, None, None
        for record in prior_records():
            self._restore(record)
        if expected_prior_pin is not None and self.pin!=expected_prior_pin:
            raise ValueError('pinned prior cutoff prefix unavailable or mismatched')
        for encoded in journal.encoded_records(allow_unsealed=True):
            self._restore(RawRecord.from_bytes(encoded))
        self.next_receipt_seq = self._control_positions.get(StreamKey(capture_id, process_session_id, 'control:'+capture_scope), -1)+1

    def _restore(self, record):
        if record.channel != 'cutoffs:'+self.scope and record.receipt.connection_id != 'control:'+self.scope:
            return
        if (record.kind!='control' or record.native_product!='control' or record.instrument is not None or
                record.native_fields_json!=b'{}' or record.channel!='cutoffs:'+self.scope or
                record.receipt.capture_id!=self.capture_id or record.receipt.connection_id!='control:'+self.scope or
                (record.venue,record.environment)!=self.journal.identity):
            raise ValueError("foreign cutoff ledger identity")
        if record.receipt.receive_seq != self._control_positions.get(record.receipt.stream_key,-1)+1:
            raise ValueError("cutoff control receipt prefix missing/conflicting")
        if len(record.body) > self.max_control_bytes:
            raise ValueError("cutoff control exceeds bound")
        value = strict_json(record.body)
        if (not isinstance(value, dict) or set(value) != {'schema','scope','sequence','previous_hash','kind','payload'}
                or value['schema'] != CONTROL_SCHEMA or value['scope'] != self.scope
                or value['sequence'] != self.control_seq or value['previous_hash'] != self.head):
            raise ValueError("cutoff control prefix missing/forked")
        self._check(value['kind'], value['payload'])
        if value['kind'] == 'allocated':
            self._check_schedule_observation(value['payload'],record.receipt)
        self._apply(value['kind'], value['payload'], record)

    @staticmethod
    def _check_schedule_observation(payload, receipt):
        if payload['clock_anchor'] is None:
            return
        anchor = ClockAnchor(**payload['clock_anchor'])
        if (receipt.process_session_id != anchor.process_session_id or receipt.clock_epoch != anchor.clock_epoch or
                not anchor.monotonic_ns <= receipt.monotonic_ns <= payload['deadline_ns'] or
                receipt.receive_utc_ns > payload['utc_ns'] or
                abs((receipt.receive_utc_ns-anchor.utc_ns)-(receipt.monotonic_ns-anchor.monotonic_ns)) > anchor.uncertainty_ns):
            raise ValueError("allocation observation missed/contradicted schedule; use allocate_missing")

    def _check(self, kind, payload):
        if not isinstance(payload, dict):
            raise ValueError("control payload object required")
        if self.projection_bytes + len(canonical(payload)) > self.max_projection_bytes:
            raise ValueError("cutoff projection byte bound; checkpoint required")
        identity = payload.get('cutoff_id')
        integer(identity, "cutoff_id")
        if kind == 'allocated':
            if set(payload) != {'cutoff_id','utc_ns','policy','required_streams','dependencies','clock_anchor','deadline_ns'}:
                raise ValueError("unknown allocation fields")
            integer(payload['utc_ns'], 'utc_ns')
            if payload['policy'] != POLICY or payload['utc_ns'] % MINUTE_NS:
                raise ValueError("UTC minute/policy required")
            if payload['clock_anchor'] is None:
                if payload['deadline_ns'] is not None:
                    raise ValueError("missing schedule cannot invent a deadline")
            else:
                anchor = ClockAnchor(**payload['clock_anchor'])
                if payload['deadline_ns'] != anchor.deadline(payload['utc_ns']) or payload['deadline_ns'] < anchor.monotonic_ns:
                    raise ValueError("allocation must freeze a future clock deadline")
            if identity != len(self.allocations) or len(self.allocations) >= self.max_cutoffs:
                raise ValueError("cutoff allocation sequence/bound")
            if identity and payload['utc_ns'] != self.allocations[identity-1]['payload']['utc_ns'] + MINUTE_NS:
                raise ValueError("allocate every intervening minute; closed labels cannot reopen")
            streams = payload['required_streams']
            if not isinstance(streams, list) or not streams or len(streams) > self.journal.limits.max_streams:
                raise ValueError("bounded required streams")
            keys = [StreamKey(**s) for s in streams]
            if len(set(keys)) != len(keys) or any(s.capture_id != self.capture_id or s.connection_id.startswith('control:') for s in keys):
                raise ValueError("foreign/duplicate required stream")
            deps = payload['dependencies']
            if not isinstance(deps, dict) or set(deps) != {'config','universe','metadata','required_feeds'}:
                raise ValueError("explicit dependency versions required")
            for name, version in deps.items():
                sha256(version, name)
        elif kind in {'closed','failed'}:
            if identity not in self.allocations or identity in self.resolutions:
                raise ValueError("missing or already resolved allocation")
            expected = {'cutoff_id','allocation_hash','reason'} if kind == 'failed' else {
                'cutoff_id','allocation_hash','barrier','streams','validity','ledger_pin','historical_clock_invalid'}
            if set(payload) != expected or payload['allocation_hash'] != self.allocations[identity]['hash']:
                raise ValueError("resolution allocation dependency mismatch")
            if kind == 'failed': text(payload['reason'], 'reason')
            else:
                if payload['validity'] not in {'valid','invalid','missing'}:
                    raise ValueError("explicit close validity required")
                # Shape is fully generated by close(); restoring is conservative
                # even for externally supplied but hash-valid raw controls.
                if not isinstance(payload['streams'], list) or len(payload['streams']) != len(self.allocations[identity]['payload']['required_streams']):
                    raise ValueError("incomplete closure stream vector")
        else:
            raise ValueError("unknown cutoff control")

    def _apply(self, kind, payload, record):
        value = dict(payload=payload, hash=digest(record.to_bytes()), record_id=record.record_id)
        (self.allocations if kind == 'allocated' else self.resolutions)[payload['cutoff_id']] = value
        self.head = digest(record.body)
        self._last_raw_sha256 = value['hash']
        self.control_seq += 1
        self.projection_bytes += len(canonical(payload))
        self._control_positions[record.receipt.stream_key] = record.receipt.receive_seq

    @property
    def pin(self):
        return CutoffPin(self.control_seq,self.head,self._last_raw_sha256)

    def _append(self, kind, payload, *, utc_ns, monotonic_ns, clock_epoch):
        self._check(kind, payload)
        body = canonical(dict(schema=CONTROL_SCHEMA, scope=self.scope, sequence=self.control_seq,
                              previous_hash=self.head, kind=kind, payload=payload))
        if len(body) > self.max_control_bytes:
            raise ValueError("cutoff control bound")
        receipt = Receipt(self.capture_id, self.session, 'control:'+self.scope, self.next_receipt_seq,
                          utc_ns, monotonic_ns, clock_epoch)
        if kind == 'allocated':
            self._check_schedule_observation(payload,receipt)
        record = RawRecord(receipt, *self.journal.identity, 'control', 'cutoffs:'+self.scope,
                           'control', body, self.provenance)
        self.journal.append(record)
        self._apply(kind, strict_json(body)['payload'], record)
        self.next_receipt_seq += 1
        return strict_json(body)

    def allocate(self, utc_ns, required_streams, dependencies, *, anchor, observed_utc_ns, monotonic_ns, clock_epoch):
        if not isinstance(anchor,ClockAnchor) or anchor.process_session_id != self.session or anchor.clock_epoch != clock_epoch:
            raise ValueError("allocation requires current local clock anchor")
        payload = dict(cutoff_id=len(self.allocations), utc_ns=utc_ns, policy=POLICY,
                       required_streams=[asdict(s) for s in required_streams], dependencies=dependencies,
                       clock_anchor=asdict(anchor),deadline_ns=anchor.deadline(utc_ns))
        return self._append('allocated', payload, utc_ns=observed_utc_ns, monotonic_ns=monotonic_ns, clock_epoch=clock_epoch)

    def fail(self, cutoff_id, reason, *, utc_ns, monotonic_ns, clock_epoch):
        return self._append('failed', dict(cutoff_id=cutoff_id, allocation_hash=self.allocations[cutoff_id]['hash'],
                            reason=reason), utc_ns=utc_ns, monotonic_ns=monotonic_ns, clock_epoch=clock_epoch)

    def allocate_missing(self, utc_ns, required_streams, dependencies, *, observed_utc_ns, monotonic_ns, clock_epoch):
        """Reserve a missed label without pretending its ingress deadline survived."""
        payload = dict(cutoff_id=len(self.allocations), utc_ns=utc_ns, policy=POLICY,
                       required_streams=[asdict(s) for s in required_streams], dependencies=dependencies,
                       clock_anchor=None,deadline_ns=None)
        return self._append('allocated',payload,utc_ns=observed_utc_ns,monotonic_ns=monotonic_ns,clock_epoch=clock_epoch)

    def close(self, cutoff_id, barrier, *, ledger_pin, utc_ns, monotonic_ns, clock_epoch):
        """utc_ns/monotonic_ns are a paired observation at closure append time."""
        integer(utc_ns,'utc_ns'); integer(monotonic_ns,'monotonic_ns')
        allocation = self.allocations[cutoff_id]
        if allocation['payload']['clock_anchor'] is None:
            raise ValueError("missing cutoff cannot be closed with a later barrier; record failure")
        cutoff_utc = allocation['payload']['utc_ns']
        # Copy now: mutating a caller's dictionaries cannot change committed state.
        barrier = strict_json(canonical(barrier))
        anchor = ClockAnchor(**barrier['anchor'])
        if (barrier['anchor'] != allocation['payload']['clock_anchor'] or
                barrier['deadline_ns'] != allocation['payload']['deadline_ns']):
            raise ValueError("allocated clock/deadline changed; record an explicit failed cutoff")
        anchors = [ClockAnchor(**a) for a in barrier['epoch_anchors']]
        if (not anchors or len(anchors)>1024 or anchors[-1] != anchor or
                any(a.process_session_id != self.session for a in anchors) or
                len({a.clock_epoch for a in anchors}) != len(anchors) or any(
                a.boundary >= b.boundary for a,b in zip(anchors,anchors[1:]))):
            raise ValueError("invalid clock epoch history")
        epochs = {a.clock_epoch:(a, anchors[i+1].boundary if i+1<len(anchors) else None)
                  for i,a in enumerate(anchors)}
        if (anchor.process_session_id != self.session or anchor.clock_epoch != clock_epoch or
                anchor.deadline(cutoff_utc) != barrier['deadline_ns'] or
                barrier['deadline_ns'] < anchor.monotonic_ns or
                barrier['observed_monotonic_ns'] < barrier['deadline_ns']):
            raise ValueError("barrier/session/deadline mismatch")
        if monotonic_ns < barrier['observed_monotonic_ns']:
            raise ValueError("closure observation precedes barrier")
        close_clock_bad = abs((utc_ns-anchor.utc_ns)-(monotonic_ns-anchor.monotonic_ns)) > anchor.uncertainty_ns
        from .validity import LedgerPin
        if not isinstance(ledger_pin, LedgerPin):
            raise ValueError("explicit verified correction prefix required")
        required = [StreamKey(**s) for s in allocation['payload']['required_streams']]
        health = {StreamKey(**s) for s in barrier['healthy_streams']}
        states = {s: dict(stream=asdict(s), admitted_seq=None, record_hash=None, validity='missing', reason='no_durable_receipt') for s in required}
        stopped, invalid, sequences, historical_clock_invalid = set(), set(), {}, set()
        previous_ordinal = -1
        process_clock_bad = False
        def all_records():
            yield from self.prior_records()
            for encoded in self.journal.encoded_records(allow_unsealed=True):
                yield RawRecord.from_bytes(encoded)
        for record in all_records():
            encoded = record.to_bytes(); r = record.receipt
            if (r.capture_id,r.process_session_id) != (self.capture_id,self.session):
                if r.stream_key in states:
                    invalid.add(r.stream_key)
                continue
            if r.connection_id.startswith('control:'):
                continue
            fields = strict_json(record.native_fields_json)
            ordinal = fields.get('collector_ingress_ordinal')
            if type(ordinal) is not int or ordinal < 0 or ordinal <= previous_ordinal:
                process_clock_bad = True
                continue
            previous_ordinal = ordinal
            if ordinal >= barrier['ordinal']:
                continue
            eligible = r.monotonic_ns <= barrier['deadline_ns'] and ordinal < barrier['ordinal']
            epoch, end = epochs.get(r.clock_epoch, (None,None))
            if (r.process_session_id != anchor.process_session_id or epoch is None or
                    (r.monotonic_ns,ordinal) < epoch.boundary or (end is not None and (r.monotonic_ns,ordinal) >= end)):
                process_clock_bad = True
                continue
            # A clock step observed before the barrier invalidates this closure,
            # even if that receipt is beyond the saved admission deadline.
            offset_bad = abs((r.receive_utc_ns-epoch.utc_ns)-(r.monotonic_ns-epoch.monotonic_ns)) > epoch.uncertainty_ns
            if offset_bad:
                if epoch.clock_epoch == anchor.clock_epoch:
                    process_clock_bad = True
                else:
                    historical_clock_invalid.add(epoch.clock_epoch)
            if r.stream_key not in states:
                continue
            if r.receive_seq != sequences.get(r.stream_key, -1)+1:
                invalid.add(r.stream_key)
            sequences[r.stream_key] = r.receive_seq
            if eligible:
                if (r.stream_key in stopped or r.receive_utc_ns > cutoff_utc or
                        (offset_bad and epoch.clock_epoch == anchor.clock_epoch)):
                    invalid.add(r.stream_key)
                    stopped.add(r.stream_key)
                    continue
                state = states[r.stream_key]
                state.update(admitted_seq=r.receive_seq, record_hash=digest(encoded), validity='valid', reason=None)
            else:
                stopped.add(r.stream_key)
        for stream, state in states.items():
            if stream in invalid or anchor.healthy_samples < 2 or close_clock_bad or process_clock_bad:
                state.update(validity='invalid', reason='clock_or_prefix_invalid')
            elif stream not in health:
                state.update(validity='missing', reason='barrier_health_missing')
        validity = 'valid' if all(s['validity'] == 'valid' for s in states.values()) else (
            'invalid' if any(s['validity'] == 'invalid' for s in states.values()) else 'missing')
        payload = dict(cutoff_id=cutoff_id, allocation_hash=allocation['hash'], barrier=barrier,
                       streams=list(states.values()), validity=validity, ledger_pin=asdict(ledger_pin),
                       historical_clock_invalid=sorted(historical_clock_invalid))
        return self._append('closed', payload, utc_ns=utc_ns,
                            monotonic_ns=monotonic_ns, clock_epoch=clock_epoch)
