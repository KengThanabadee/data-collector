"""Bounded clock-health state for receive-asof/2; no network or OS-time changes.

The caller supplies measured reference offset/uncertainty and its evidence hash.
This is not an NTP client. Persist each returned assessment as a raw control before
using its anchor, and route invalidated epochs to the durable correction ledger.
Receipt UTC values are never adjusted by the measured offset.
"""
from dataclasses import dataclass, replace

from .contracts import integer, sha256, text
from .cutoffs import ClockAnchor


class ClockRestartRequired(ValueError):
    pass


@dataclass(frozen=True)
class ClockSample:
    utc_ns: int
    monotonic_ns: int
    offset_ns: int
    uncertainty_ns: int
    boot_id: str
    evidence_sha256: str

    def __post_init__(self):
        integer(self.utc_ns, 'clock UTC')
        integer(self.monotonic_ns, 'clock monotonic')
        integer(self.uncertainty_ns, 'clock uncertainty')
        if type(self.offset_ns) is not int:
            raise ValueError('signed integer reference offset required')
        if max(self.utc_ns, self.monotonic_ns, abs(self.offset_ns), self.uncertainty_ns) > 2**63-1:
            raise ValueError('clock integer range')
        text(self.boot_id, 'boot identity')
        if len(self.boot_id.encode()) > 128:
            raise ValueError('boot identity bound')
        sha256(self.evidence_sha256, 'clock evidence')


@dataclass(frozen=True)
class ClockAssessment:
    anchor: ClockAnchor
    evidence_sha256: str
    reason: str
    aligned: bool
    invalidated_epoch: str | None


class ClockMonitor:
    """One process/boot, bounded epochs, two spaced healthy samples to align.

    Wall/monotonic discrepancies beyond step_tolerance_ns indicate a step;
    smaller differences remain measurement noise, not proof of perfect UTC.
    Sampling itself is owned by the supervisor; stale samples cannot align.
    """
    def __init__(self, process_session_id, *, sample_interval_ns=60_000_000_000,
                 max_sample_age_ns=60_000_000_000, max_offset_ns=100_000_000,
                 step_tolerance_ns=1_000_000, max_epochs=1024):
        text(process_session_id, 'process session')
        if len(process_session_id.encode()) > 128:
            raise ValueError('process session bound')
        for name, value in locals().copy().items():
            if name not in {'self', 'process_session_id'}:
                integer(value, name, 1)
        if max_sample_age_ns < sample_interval_ns or max_epochs > 1024:
            raise ValueError('clock age/epoch bounds')
        self.session = process_session_id
        self.sample_interval_ns, self.max_age_ns = sample_interval_ns, max_sample_age_ns
        self.max_offset_ns, self.step_tolerance_ns = max_offset_ns, step_tolerance_ns
        self.max_epochs = max_epochs
        self._sample = self._anchor = None
        self._history = []
        self._last_healthy_ns = None
        self._last_ordinal = 0
        self._observed_mono = None
        self._last_pair = None
        self._restart = False

    @property
    def prior_anchors(self):
        return tuple(self._history)

    def _poison(self, reason):
        self._restart = True
        raise ClockRestartRequired(reason)

    def sample(self, value, *, ingress_ordinal):
        if self._restart:
            raise ClockRestartRequired('new process/session required after clock restart fault')
        if not isinstance(value, ClockSample):
            raise ValueError('typed measured clock sample required')
        integer(ingress_ordinal, 'clock boundary ingress ordinal')
        if ingress_ordinal > 2**63-1:
            raise ValueError('clock boundary integer range')
        previous = self._sample
        if self._observed_mono is not None and value.monotonic_ns < self._observed_mono:
            self._poison('sample monotonic observation regressed')
        if previous is not None and (value.boot_id != previous.boot_id or
                value.monotonic_ns < previous.monotonic_ns or ingress_ordinal < self._last_ordinal):
            self._poison('boot/monotonic/ingress order changed')
        if previous is not None and value.monotonic_ns == previous.monotonic_ns:
            raise ValueError('clock samples must have distinct monotonic observations')
        reason = 'healthy'
        if abs(value.offset_ns)+value.uncertainty_ns > self.max_offset_ns:
            reason = 'reference_offset_or_uncertainty'
        if previous is not None:
            elapsed = value.monotonic_ns-previous.monotonic_ns
            if elapsed > self.max_age_ns and self._anchor.epoch_started_ns <= previous.monotonic_ns:
                reason = 'sample_stale'
            if (abs((value.utc_ns-self._last_pair[0])-(value.monotonic_ns-self._last_pair[1])) > self.step_tolerance_ns or
                    abs((value.utc_ns-self._anchor.utc_ns)-(value.monotonic_ns-self._anchor.monotonic_ns)) > self.step_tolerance_ns):
                reason = 'wall_step'
        invalidated = None
        # A new fault closes the old epoch. Repeated bad reference samples stay
        # in the recovery epoch; every new wall step creates its own boundary.
        transition = previous is not None and reason != 'healthy' and (
            self._anchor.healthy_samples > 0 or reason in {'wall_step','sample_stale'})
        if transition:
            if (len(self._history)+1 >= self.max_epochs or
                    (value.monotonic_ns,ingress_ordinal) <= self._anchor.boundary):
                self._poison('clock epoch bound; rotate process session')
            invalidated = self._anchor.clock_epoch
            self._history.append(replace(self._anchor, healthy_samples=0))
            self._anchor = None
            self._last_healthy_ns = None
        if self._anchor is None:
            epoch = self.session+':clock:'+str(len(self._history))
            self._anchor = ClockAnchor(self.session, epoch, value.utc_ns, value.monotonic_ns,
                abs(value.offset_ns)+value.uncertainty_ns, 0, value.monotonic_ns, ingress_ordinal)
        count = self._anchor.healthy_samples
        if reason != 'healthy':
            count, self._last_healthy_ns = 0, None
        elif self._last_healthy_ns is None or value.monotonic_ns-self._last_healthy_ns >= self.sample_interval_ns:
            count = min(count+1, 2)
            self._last_healthy_ns = value.monotonic_ns
        self._anchor = replace(self._anchor, utc_ns=value.utc_ns, monotonic_ns=value.monotonic_ns,
            uncertainty_ns=abs(value.offset_ns)+value.uncertainty_ns, healthy_samples=count)
        self._sample, self._last_ordinal = value, ingress_ordinal
        self._observed_mono = value.monotonic_ns
        self._last_pair = value.utc_ns,value.monotonic_ns
        aligned = reason == 'healthy' and count >= 2
        return ClockAssessment(self._anchor, value.evidence_sha256,
                               'warming_up' if reason == 'healthy' and not aligned else reason,
                               aligned, invalidated)

    def check(self, *, utc_ns, monotonic_ns, boot_id, ingress_ordinal):
        """Validate a paired observation without refreshing reference evidence.

        Checks do not count as reference samples. A stale reference or observed
        step closes alignment; two later measured samples are still required.
        """
        integer(utc_ns,'checked UTC'); integer(monotonic_ns,'checked monotonic')
        integer(ingress_ordinal,'checked ingress ordinal')
        text(boot_id,'checked boot')
        if max(utc_ns,monotonic_ns,ingress_ordinal) > 2**63-1 or len(boot_id.encode()) > 128:
            raise ValueError('clock check integer/identity bound')
        if self._restart:
            raise ClockRestartRequired('new process/session required')
        if self._sample is None:
            raise ValueError('measured reference sample required before checking')
        if boot_id != self._sample.boot_id or monotonic_ns < self._observed_mono or ingress_ordinal < self._last_ordinal:
            self._poison('checked boot/monotonic/ingress order changed')
        reason = 'healthy'
        if monotonic_ns-self._sample.monotonic_ns > self.max_age_ns:
            reason = 'sample_stale'
        if (abs((utc_ns-self._last_pair[0])-(monotonic_ns-self._last_pair[1])) > self.step_tolerance_ns or
                abs((utc_ns-self._anchor.utc_ns)-(monotonic_ns-self._anchor.monotonic_ns)) > self.step_tolerance_ns):
            reason = 'wall_step'
        invalidated = None
        if reason != 'healthy' and (self._anchor.healthy_samples > 0 or reason == 'wall_step'):
            if len(self._history)+1 >= self.max_epochs or (monotonic_ns,ingress_ordinal) <= self._anchor.boundary:
                self._poison('clock check epoch boundary/bound requires new session')
            invalidated = self._anchor.clock_epoch
            self._history.append(replace(self._anchor,healthy_samples=0))
            self._anchor = ClockAnchor(self.session,self.session+':clock:'+str(len(self._history)),
                utc_ns,monotonic_ns,self._anchor.uncertainty_ns,0,monotonic_ns,ingress_ordinal)
            self._last_healthy_ns = None
        self._observed_mono,self._last_ordinal = monotonic_ns,ingress_ordinal
        self._last_pair = utc_ns,monotonic_ns
        aligned = reason == 'healthy' and self._anchor.healthy_samples >= 2
        return ClockAssessment(self._anchor,self._sample.evidence_sha256,
            'warming_up' if reason == 'healthy' and not aligned else reason,aligned,invalidated)

    def anchor(self, *, now_monotonic_ns):
        """Current anchor, downgraded if overdue; never fabricate a fresh sample."""
        integer(now_monotonic_ns, 'current monotonic time')
        if self._restart:
            raise ClockRestartRequired('new process/session required')
        if self._anchor is None:
            raise ValueError('measured clock sample required')
        if now_monotonic_ns < self._observed_mono:
            self._poison('current monotonic clock regressed')
        self._observed_mono = now_monotonic_ns
        if now_monotonic_ns-self._sample.monotonic_ns > self.max_age_ns:
            return replace(self._anchor, healthy_samples=0)
        return self._anchor
