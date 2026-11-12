"""Bounded REST/WS L2 synchronization projections; not a network adapter.

Inputs must already be durably archived. CM/options remain gated until their
native seed/bridge fixtures exist. Binance spot uses the stricter documented
first-range boundary (seed itself in the range); live boundary proof is pending.
"""
from dataclasses import asdict, dataclass

from .books import BookLimits, DepthEvent, L2Book
from .contracts import canonical, digest, integer


@dataclass(frozen=True)
class BufferedDelta:
    event: DepthEvent
    first_update_id: int | None = None

    def __post_init__(self):
        if not isinstance(self.event, DepthEvent) or self.event.kind != "delta":
            raise ValueError("native delta required")
        if self.first_update_id is not None:
            integer(self.first_update_id, "first_update_id")
            if self.first_update_id > self.event.update_id:
                raise ValueError("reversed native update range")

    @property
    def fingerprint(self):
        return digest(canonical(dict(event=self.event.fingerprint, first=self.first_update_id)))


@dataclass(frozen=True)
class BootstrapLimits:
    max_events: int = 4096
    max_bytes: int = 16 * 1024 * 1024
    max_wait_ns: int = 30_000_000_000

    def __post_init__(self):
        for name, value in asdict(self).items():
            integer(value, name, 1)


class BufferedBook:
    """One instrument, connection and owner epoch. Calls must be serialized.

    poll() must be called by the transport watchdog even during a quiet socket.
    Invalid states require explicit reset; callers archive the gap/reset event.
    """
    def __init__(self, instrument, profile, *, now_ns, limits=BootstrapLimits(), book_limits=BookLimits()):
        profiles = {"binance_spot": ("binance", {"spot"}, 5000),
                    "binance_um": ("binance", {"linear", "um"}, 1000),
                    "bybit_full": ("bybit", {"spot", "linear", "inverse"}, 10000)}
        if profile not in profiles:
            raise ValueError("profile unresolved or unsupported; native fixture gate required")
        venue, products, self.seed_depth = profiles[profile]
        if instrument.venue != venue or instrument.native_product not in products:
            raise ValueError("native namespace/profile mismatch")
        if book_limits.max_levels_per_side < self.seed_depth:
            raise ValueError("resource bound cannot hold the selected REST seed")
        self.book = L2Book(instrument, limits=book_limits)
        self.profile, self.limits, self.epoch = profile, limits, 0
        self.reset(now_ns=now_ns)

    def reset(self, *, now_ns, reason="initial_or_reconnect"):
        integer(now_ns, "now_ns")
        self.epoch += 1
        self.book.clear()
        self.state, self.reason = "bootstrapping", reason
        self.start_ns = self.clock_ns = now_ns
        self.buffer, self.buffer_bytes, self.seed = [], 0, None
        self.last = self.buffer_last = None
        self.anchor_source = self.bridge_source = self.last_source = None

    def invalidate(self, reason):
        self.state, self.reason = "invalid", reason
        self.buffer, self.buffer_bytes, self.seed = [], 0, None
        return "invalid"

    def poll(self, *, now_ns):
        integer(now_ns, "now_ns")
        if now_ns < self.clock_ns:
            return self.invalidate("monotonic_clock_regressed")
        self.clock_ns = now_ns
        if self.state == "bootstrapping" and now_ns - self.start_ns >= self.limits.max_wait_ns:
            return self.invalidate("bootstrap_timeout")
        return self.state

    def _validate(self, delta):
        if not isinstance(delta, BufferedDelta) or delta.event.instrument != self.book.instrument:
            raise ValueError("foreign or untyped delta")
        if self.profile.startswith("binance") and delta.first_update_id is None:
            raise ValueError("Binance native U required")
        if self.profile == "binance_um" and delta.event.previous_id is None:
            raise ValueError("UM native pu required")
        if self.profile == "bybit_full" and delta.event.cross_sequence is None:
            raise ValueError("Bybit native seq required")

    def receive(self, delta, *, now_ns):
        self._validate(delta)
        self.poll(now_ns=now_ns)
        if self.state in {"invalid", "no_book"}:
            return "ignored_until_reset"
        event = delta.event
        if self.profile == "bybit_full" and event.update_id == 1 and (self.state == "live" or self.buffer_last is not None):
            return self.invalidate("native_reset_requires_reseed")
        if self.state == "live":
            return self._apply(delta)
        if self.buffer_last is not None:
            prior = self.buffer_last
            if event.update_id == prior.event.update_id:
                return "duplicate" if delta.fingerprint == prior.fingerprint else self.invalidate("conflicting_duplicate")
            if self.profile == "bybit_full":
                if event.cross_sequence < prior.event.cross_sequence or event.update_id < prior.event.update_id:
                    return "stale"
                if event.update_id != prior.event.update_id + 1:
                    return self.invalidate("buffer_u_gap")
        size = len(canonical(asdict(delta)))
        if len(self.buffer) >= self.limits.max_events or self.buffer_bytes + size > self.limits.max_bytes:
            return self.invalidate("bootstrap_buffer_bound")
        self.buffer.append(delta)
        self.buffer_bytes += size
        self.buffer_last = delta
        return self._bootstrap() if self.seed is not None else "buffered"

    def supply_seed(self, seed, *, now_ns):
        if not isinstance(seed, DepthEvent) or seed.kind != "snapshot" or seed.instrument != self.book.instrument:
            raise ValueError("typed REST snapshot for this incarnation required")
        self.poll(now_ns=now_ns)
        if self.state != "bootstrapping":
            return "ignored_until_reset" if self.state in {"invalid", "no_book"} else "already_live"
        if max(len(seed.bids), len(seed.asks)) > self.seed_depth:
            return self.invalidate("seed_exceeds_native_depth")
        if self.profile == "bybit_full" and seed.cross_sequence is None:
            return self.invalidate("seed_missing_seq")
        self.seed = seed
        return self._bootstrap()

    def _retry_seed(self):
        self.seed = None
        return "retry_seed"

    def _bootstrap(self):
        seed = self.seed
        if not self.buffer:
            return "waiting_for_bridge"
        bridge = None
        for i, delta in enumerate(self.buffer):
            event = delta.event
            if self.profile == "bybit_full":
                if event.cross_sequence < seed.cross_sequence:
                    continue
                if event.cross_sequence > seed.cross_sequence or event.update_id != seed.update_id:
                    return self._retry_seed()
                bridge = i
                break
            # The spot docs discard <= seed; UM discards strictly < seed.
            if event.update_id < seed.update_id or (self.profile == "binance_spot" and event.update_id == seed.update_id):
                continue
            if delta.first_update_id > seed.update_id:
                return self._retry_seed()
            bridge = i
            break
        if bridge is None:
            return "waiting_for_bridge"
        try:
            self.book.apply(seed, replace=True)
        except ValueError as exc:
            return self.invalidate(str(exc))
        self.anchor_source = seed.source_record_id
        self.bridge_source = self.buffer[bridge].event.source_record_id
        self.last_source = seed.source_record_id
        if self.profile == "bybit_full" and seed.update_id == 1 and not seed.bids and not seed.asks:
            self.last = self.buffer[bridge]
            self.state, self.reason = "no_book", "empty_reset_seed_unavailable"
            self.buffer, self.buffer_bytes, self.buffer_last, self.seed = [], 0, None, None
            return "no_book"
        self.state, self.reason = "live", None
        self.last = self.buffer[bridge] if self.profile == "bybit_full" else None
        pending = self.buffer[bridge + (self.profile == "bybit_full"):]
        for delta in pending:
            outcome = self._apply(delta)
            if outcome == "invalid":
                return outcome
        self.buffer, self.buffer_bytes, self.buffer_last, self.seed = [], 0, None, None
        return "live"

    def _apply(self, delta):
        event, prior = delta.event, self.last
        if prior is not None:
            last = prior.event
            if event.update_id == last.update_id:
                return "duplicate" if delta.fingerprint == prior.fingerprint else self.invalidate("conflicting_duplicate")
            if event.update_id < last.update_id:
                return "stale"
            if self.profile == "bybit_full":
                if event.cross_sequence < last.cross_sequence:
                    return "stale"
                if event.update_id != last.update_id + 1:
                    return self.invalidate("u_gap")
            elif self.profile == "binance_um":
                if event.previous_id != last.update_id:
                    return self.invalidate("pu_gap")
            elif delta.first_update_id > last.update_id + 1:
                return self.invalidate("range_gap")
        try:
            self.book.apply(event)
        except ValueError as exc:
            return self.invalidate(str(exc))
        self.last, self.last_source = delta, event.source_record_id
        return "applied"

    def view(self):
        return dict(instrument=asdict(self.book.instrument), profile=self.profile,
                    state=self.state, reason=self.reason, epoch=self.epoch,
                    update_id=self.last.event.update_id if self.last else None,
                    anchor_source=self.anchor_source, bridge_source=self.bridge_source,
                    last_source=self.last_source, buffered_events=len(self.buffer), buffered_bytes=self.buffer_bytes,
                    seeded_depth_limit=self.seed_depth, untouched_deeper_levels="unknown",
                    excludes_rpi=self.profile == "bybit_full", live_readiness="unverified",
                    continuity_confidence="sequence_proven" if self.state == "live" else "invalid",
                    **self.book.levels())
