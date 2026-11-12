"""Local clock-bound cutoff creation and independently verified replay links.

All objects share one serialized disk owner. This does not schedule timers,
recover an interrupted allocation, publish snapshots, or map clock faults into
late correction ranges. A durable close remains separate from timely publication.
"""
from dataclasses import asdict

from .clock import ClockMonitor
from .clocklog import ClockPin, DurableClock, DEFAULT_POLICY, replay_clock
from .contracts import RawRecord, Receipt, StreamKey, canonical, digest, integer, strict_json
from .cutoffs import ClockAnchor, CutoffCoordinator, IngressWriter
from .ledger import DurableLedger
from .recovery import CutoffRecovery

LINK_SCHEMA = 'dc.clock-cutoff-link/2'
BINDING_SCHEMA = 'dc.clock-cutoff-binding/2'


def _contract_policy(policy):
    policy=dict(policy)
    if set(policy)!=set(DEFAULT_POLICY):raise ValueError('complete clock binding policy required')
    ClockMonitor('validate',**policy)
    if (policy['max_offset_ns']>100_000_000 or policy['sample_interval_ns']>60_000_000_000 or
            policy['max_sample_age_ns']>60_000_000_000):
        raise ValueError('clock policy exceeds receive-asof/2 ceiling')
    return policy


def _link(allocation, clock, coordinator, utc_ns, mono, epoch, sequence):
    body = canonical(dict(schema=LINK_SCHEMA,scope=coordinator.scope,
        allocation_record_id=allocation['record_id'],allocation_sha256=allocation['hash'],
        clock_pin=asdict(clock.pin),clock_policy=clock.policy))
    return RawRecord(Receipt(coordinator.capture_id,coordinator.session,'control:clock-cutoffs:'+coordinator.scope,
        sequence,utc_ns,mono,epoch),*coordinator.journal.identity,'control','clock_cutoff_links','control',body,coordinator.provenance)


class ClockBoundCutoffs:
    """Bind fresh checks to immutable allocations and admission barriers.

    Fresh local scope or verified startup recovery. Calls must be serialized with receipt
    stamping/IngressWriter writes; asynchronous command integration is separate.
    Any failed operation poisons this facade: recover WAL evidence offline.
    """
    def __init__(self, clock, ingress, coordinator, ledger, *, max_link_bytes=8*1024*1024,recovery=None):
        if not isinstance(clock,DurableClock) or not isinstance(ingress,IngressWriter) or not isinstance(coordinator,CutoffCoordinator) or not isinstance(ledger,DurableLedger):
            raise ValueError('typed local clock/ingress/cutoff/ledger owners required')
        integer(max_link_bytes,'clock link projection bound',1)
        _contract_policy(clock.policy)
        if (clock.journal is not ingress.journal or coordinator.journal is not ingress.journal or
                ledger.journal is not ingress.journal or
                len({clock.capture,ingress.capture_id,coordinator.capture_id,ledger.capture_id}) != 1 or
                len({clock.session,ingress.session,coordinator.session,ledger.session}) != 1 or
                coordinator.scope != ledger.scope or clock.provenance != coordinator.provenance or
                ledger.provenance != coordinator.provenance or (coordinator.allocations and recovery is None) or
                StreamKey(clock.capture,clock.session,'control:clock-cutoffs:'+coordinator.scope) in clock.journal.state.positions):
            raise ValueError('same serialized local owners and fresh cutoff scope required')
        if recovery is not None:
            if (not isinstance(recovery,CutoffRecovery) or recovery.recovered_pin!=coordinator.pin or
                    recovery.next_cutoff_id!=len(coordinator.allocations) or
                    set(coordinator.allocations)!=set(coordinator.resolutions)):
                raise ValueError('fully reconciled pinned cutoff history required')
            found=False;size=0
            for index,encoded in enumerate(clock.journal.encoded_records(allow_unsealed=True)):
                size+=len(encoded)
                if index>=coordinator.max_cutoffs+1 or size>max_link_bytes:raise ValueError('recovery marker scan bound')
                record=RawRecord.from_bytes(encoded)
                if record.record_id==recovery.marker_record_id:
                    if found or digest(encoded)!=recovery.marker_sha256:raise ValueError('recovery marker mismatch')
                    if (record.receipt.capture_id,record.receipt.process_session_id,record.receipt.connection_id,record.channel)!=(
                            clock.capture,clock.session,'control:cutoff-recovery:'+coordinator.scope,'cutoff_recovery:'+coordinator.scope):
                        raise ValueError('foreign recovery marker')
                    found=True
            if not found:raise ValueError('durable recovery marker required')
        self.clock,self.ingress,self.coordinator,self.ledger = clock,ingress,coordinator,ledger
        self._links = {}
        self._link_bytes = 0
        self.max_link_bytes = max_link_bytes
        self.poisoned = False

    def _ready(self):
        if self.poisoned or self.clock.poisoned or self.ingress.poisoned or self.ledger.poisoned:
            raise ValueError('clock-bound cutoff owner requires recovery')

    def allocate(self, cutoff_utc_ns, required_streams, dependencies, *, utc_ns, monotonic_ns, boot_id):
        self._ready()
        try:
            check = self.clock.check(utc_ns=utc_ns,monotonic_ns=monotonic_ns,boot_id=boot_id,ingress_ordinal=self.ingress.ordinal)
            epoch = check.anchor.clock_epoch
            scheduled = (check.aligned and utc_ns <= cutoff_utc_ns and
                monotonic_ns <= check.anchor.deadline(cutoff_utc_ns) and
                abs((utc_ns-check.anchor.utc_ns)-(monotonic_ns-check.anchor.monotonic_ns)) <= check.anchor.uncertainty_ns)
            if scheduled:
                event = self.coordinator.allocate(cutoff_utc_ns,required_streams,dependencies,anchor=check.anchor,
                    observed_utc_ns=utc_ns,monotonic_ns=monotonic_ns,clock_epoch=epoch)
            else:
                event = self.coordinator.allocate_missing(cutoff_utc_ns,required_streams,dependencies,
                    observed_utc_ns=utc_ns,monotonic_ns=monotonic_ns,clock_epoch=epoch)
            identity = event['payload']['cutoff_id']
            allocation = self.coordinator.allocations[identity]
            record = _link(allocation,self.clock,self.coordinator,utc_ns,monotonic_ns,epoch,len(self._links))
            size = len(record.to_bytes())
            if self._link_bytes+size > self.max_link_bytes or size > self.coordinator.journal.limits.max_record_bytes:
                raise ValueError('clock allocation link projection/record bound')
            self.coordinator.journal.append(record)
            self._links[identity] = record
            self._link_bytes += size
            if not scheduled:
                reason = 'clock_alignment_unavailable' if not check.aligned else 'cutoff_schedule_unavailable'
                return self.coordinator.fail(identity,reason,utc_ns=utc_ns,
                    monotonic_ns=monotonic_ns,clock_epoch=epoch)
            return event
        except BaseException:
            self.poisoned = True
            raise

    def close(self, cutoff_id, *, utc_ns, monotonic_ns, boot_id, healthy_streams):
        self._ready()
        if cutoff_id not in self._links or cutoff_id in self.coordinator.resolutions:
            raise ValueError('unknown or resolved clock-bound cutoff')
        try:
            check = self.clock.check(utc_ns=utc_ns,monotonic_ns=monotonic_ns,boot_id=boot_id,ingress_ordinal=self.ingress.ordinal)
            allocation = self.coordinator.allocations[cutoff_id]['payload']
            anchor = ClockAnchor(**allocation['clock_anchor'])
            if not check.aligned or check.anchor.clock_epoch != anchor.clock_epoch:
                return self.coordinator.fail(cutoff_id,'clock_changed_or_stale',utc_ns=utc_ns,
                    monotonic_ns=monotonic_ns,clock_epoch=check.anchor.clock_epoch)
            barrier = self.ingress.barrier(anchor,allocation['utc_ns'],now_monotonic_ns=monotonic_ns,
                healthy_streams=healthy_streams,prior_anchors=self.clock.prior_anchors)
            link = self._links[cutoff_id]
            barrier['clock_binding'] = dict(schema=BINDING_SCHEMA,allocation_link_record_id=link.record_id,
                allocation_link_sha256=digest(link.to_bytes()),close_clock_pin=asdict(self.clock.pin))
            return self.coordinator.close(cutoff_id,barrier,ledger_pin=self.ledger.pin(),utc_ns=utc_ns,
                monotonic_ns=monotonic_ns,clock_epoch=anchor.clock_epoch)
        except BaseException:
            self.poisoned = True
            raise


def verify_clock_binding(records, close, *, max_records=200000, max_scan_bytes=256*1024*1024):
    """Recompute both pinned clock prefixes from a verified raw prefix factory.

    The factory ends at the target close. Existing unbound receive-asof/2
    closures retain their prior semantics; only explicitly bound closures enter
    this verifier. Missing/changed binding evidence fails, never degrades to valid.
    """
    integer(max_records,'clock binding scan records',1);integer(max_scan_bytes,'clock binding scan bytes',1)
    if not callable(records) or not isinstance(close,RawRecord):
        raise ValueError('verified raw prefix factory and close record required')
    body = strict_json(close.body)
    payload = body['payload']
    if (body.get('schema')!='dc.cutoff-control/2' or body.get('kind')!='closed' or close.kind!='control' or
            close.channel!='cutoffs:'+body['scope'] or close.receipt.connection_id!='control:'+body['scope'] or
            close.native_product!='control' or close.instrument is not None or close.native_fields_json!=b'{}'):
        raise ValueError('clock binding requires a durable closed cutoff')
    barrier = payload['barrier']
    binding = barrier['clock_binding']
    if set(binding) != {'schema','allocation_link_record_id','allocation_link_sha256','close_clock_pin'} or binding['schema'] != BINDING_SCHEMA:
        raise ValueError('unknown clock cutoff binding')
    def scan():
        count=size=0
        for record in records():
            if not isinstance(record,RawRecord):raise ValueError('typed verified raw prefix required')
            count+=1;size+=len(record.to_bytes())
            if count > max_records or size > max_scan_bytes:raise ValueError('clock binding scan bound')
            yield record
    allocation=link=None
    found_close=False
    for record in scan():
        if record.record_id==binding['allocation_link_record_id']:
            if link is not None or allocation is None:raise ValueError('clock allocation link order/conflict')
            link=record
        if digest(record.to_bytes())==payload['allocation_hash']:
            if allocation is not None or link is not None:raise ValueError('clock allocation order/conflict')
            allocation=record
        if record.record_id==close.record_id:
            if record.to_bytes()!=close.to_bytes():raise ValueError('clock-bound close bytes changed')
            found_close=True;break
    if not found_close or allocation is None or link is None or digest(link.to_bytes())!=binding['allocation_link_sha256']:
        raise ValueError('clock allocation/link/close evidence missing')
    a = strict_json(allocation.body)
    link_body = strict_json(link.body)
    if (a.get('schema')!='dc.cutoff-control/2' or a.get('kind')!='allocated' or a.get('scope')!=body['scope'] or
            allocation.kind!='control' or allocation.channel!=close.channel or allocation.native_product!='control' or
            allocation.receipt.connection_id!=close.receipt.connection_id or allocation.instrument is not None or
            allocation.native_fields_json!=b'{}' or allocation.provenance!=close.provenance or
            (allocation.venue,allocation.environment)!=(close.venue,close.environment) or
            a['payload']['cutoff_id']!=payload['cutoff_id'] or
            set(link_body)!={'schema','scope','allocation_record_id','allocation_sha256','clock_pin','clock_policy'} or
            link_body['schema']!=LINK_SCHEMA or link_body['scope']!=body['scope'] or
            link_body['allocation_record_id']!=allocation.record_id or link_body['allocation_sha256']!=payload['allocation_hash'] or
            link.receipt.capture_id!=close.receipt.capture_id or link.receipt.process_session_id!=close.receipt.process_session_id or
            link.receipt.connection_id!='control:clock-cutoffs:'+body['scope'] or link.kind!='control' or
            link.channel!='clock_cutoff_links' or link.native_product!='control' or link.instrument is not None or
            link.native_fields_json!=b'{}' or link.provenance!=close.provenance or
            (link.venue,link.environment)!=(close.venue,close.environment)):
        raise ValueError('clock allocation link identity/content mismatch')
    policy=_contract_policy(link_body['clock_policy'])
    CutoffCoordinator._check_schedule_observation(a['payload'],allocation.receipt)
    def check_prefix(stop, pin, receipt, ordinal=None):
        pin=ClockPin(**pin)
        last=None
        def prefix():
            nonlocal last
            for record in scan():
                if record.record_id==stop:return
                if record.record_id==pin.last_record_id:last=record
                yield record
            raise ValueError('clock binding stop missing')
        result = replay_clock(prefix(),venue=close.venue,environment=close.environment,
            capture_id=close.receipt.capture_id,process_session_id=close.receipt.process_session_id,
            provenance=close.provenance,policy=policy,expected_pin=pin,max_assessments=max_records,
            max_scan_records=max_records,max_scan_bytes=max_scan_bytes)
        if last is None or strict_json(last.body).get('schema')!='dc.clock-check/2' or not result.assessment.aligned:
            raise ValueError('fresh paired clock check required for cutoff')
        observation=strict_json(last.body)['observation']
        if (observation['utc_ns']!=receipt.receive_utc_ns or observation['monotonic_ns']!=receipt.monotonic_ns or
                result.assessment.anchor.clock_epoch!=receipt.clock_epoch or
                (ordinal is not None and observation['ingress_ordinal']!=ordinal)):
            raise ValueError('clock check does not match cutoff observation/barrier')
        return result
    initial=check_prefix(allocation.record_id,link_body['clock_pin'],allocation.receipt)
    final=check_prefix(close.record_id,binding['close_clock_pin'],close.receipt,barrier['ordinal'])
    if (asdict(initial.assessment.anchor)!=a['payload']['clock_anchor'] or
            barrier['anchor']!=a['payload']['clock_anchor'] or
            final.assessment.anchor.clock_epoch!=initial.assessment.anchor.clock_epoch or
            a['payload']['deadline_ns']!=barrier['deadline_ns'] or
            barrier['deadline_ns']!=initial.assessment.anchor.deadline(a['payload']['utc_ns']) or
            barrier['deadline_ns']>barrier['observed_monotonic_ns'] or
            barrier['epoch_anchors']!=[asdict(anchor) for anchor in final.prior_anchors]+[barrier['anchor']] or
            barrier['observed_monotonic_ns']!=close.receipt.monotonic_ns or link.receipt!=Receipt(
                allocation.receipt.capture_id,allocation.receipt.process_session_id,link.receipt.connection_id,
                link.receipt.receive_seq,allocation.receipt.receive_utc_ns,allocation.receipt.monotonic_ns,allocation.receipt.clock_epoch)):
        raise ValueError('allocated clock anchor/epoch/barrier changed')
    return binding
