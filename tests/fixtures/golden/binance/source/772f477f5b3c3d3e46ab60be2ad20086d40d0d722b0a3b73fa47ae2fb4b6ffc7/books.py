"""Decimal L2 projections for snapshot-seeded public feeds.

Pure state machines: callers persist native messages first and archive every reset /
invalidation. No socket, trading, timestamp imputation or private liquidity inference.
REST-buffered Binance/Bybit-full bootstrap is a separate model, not this profile.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass
from decimal import Decimal, InvalidOperation

from .contracts import InstrumentKey, canonical, digest, integer, sha256


def decimal_value(value):
    # Adapters parse native JSON decimals losslessly; accepting a float here would
    # silently inherit an already rounded exchange price or quantity.
    if not isinstance(value, str) or not value or len(value) > 128:
        raise ValueError("bounded decimal string required")
    try:
        result = Decimal(value)
    except InvalidOperation as exc:
        raise ValueError("invalid decimal") from exc
    if not result.is_finite() or abs(result.as_tuple().exponent) > 100 or abs(result.adjusted()) > 100:
        raise ValueError("decimal outside configured numeric domain")
    return result


def decimal_text(value):
    if value == 0:
        return "0"
    result = format(value, "f")
    return result.rstrip("0").rstrip(".") if "." in result else result


@dataclass(frozen=True)
class LevelChange:
    price: str
    amount: str
    action: str = "set"

    def __post_init__(self):
        decimal_value(self.price)
        if decimal_value(self.amount) < 0:
            raise ValueError("absolute book quantity cannot be negative")
        if self.action not in {"set", "new", "change", "delete"}:
            raise ValueError("unknown L2 action")


@dataclass(frozen=True)
class DepthEvent:
    instrument: InstrumentKey
    source_record_id: str
    kind: str
    update_id: int
    bids: tuple[LevelChange, ...] = ()
    asks: tuple[LevelChange, ...] = ()
    previous_id: int | None = None
    cross_sequence: int | None = None

    def __post_init__(self):
        if not isinstance(self.instrument, InstrumentKey) or self.kind not in {"snapshot", "delta"}:
            raise ValueError("typed instrument and native depth kind required")
        sha256(self.source_record_id, "source_record_id")
        integer(self.update_id, "update_id")
        for name in ("previous_id", "cross_sequence"):
            value = getattr(self, name)
            if value is not None:
                integer(value, name)
        for side in (self.bids, self.asks):
            if type(side) is not tuple or len(side) > 40000 or any(not isinstance(x, LevelChange) for x in side):
                raise ValueError("bounded immutable level tuple required")

    @property
    def fingerprint(self):
        value = asdict(self)
        value.pop("source_record_id")
        return digest(canonical(value))


@dataclass(frozen=True)
class BookLimits:
    max_levels_per_side: int = 20000

    def __post_init__(self):
        integer(self.max_levels_per_side, "max_levels_per_side", 1)


class L2Book:
    def __init__(self, instrument, *, limits=BookLimits(), signed_prices=False):
        if not isinstance(instrument, InstrumentKey) or type(signed_prices) is not bool:
            raise ValueError("typed instrument and explicit price convention required")
        self.instrument, self.limits, self.signed_prices = instrument, limits, signed_prices
        self._bids, self._asks = {}, {}

    def apply(self, event, *, replace=False):
        if event.instrument != self.instrument:
            raise ValueError("foreign instrument event")
        sides = ({}, {}) if replace else (self._bids.copy(), self._asks.copy())
        for target, changes in zip(sides, (event.bids, event.asks)):
            seen = set()
            for change in changes:
                price, amount = decimal_value(change.price), decimal_value(change.amount)
                if not self.signed_prices and price <= 0:
                    raise ValueError("nonpositive price outside this instrument convention")
                if replace and price in seen:
                    raise ValueError("duplicate snapshot price level")
                seen.add(price)
                if change.action == "delete" or amount == 0:
                    target.pop(price, None)
                else:
                    target[price] = amount
            # One native message is atomic. An insertion can precede a deletion at
            # capacity; temporary work is bounded by current cap + 40000changes.
            if len(target) > self.limits.max_levels_per_side:
                raise ValueError("L2 resource bound exceeded; resync with a sized profile")
        self._bids, self._asks = sides

    def clear(self):
        self._bids, self._asks = {}, {}

    @property
    def level_count(self):
        return len(self._bids)+len(self._asks)

    def levels(self):
        return dict(bids=[(decimal_text(p), decimal_text(q)) for p, q in sorted(self._bids.items(), reverse=True)],
                    asks=[(decimal_text(p), decimal_text(q)) for p, q in sorted(self._asks.items())])


class SnapshotBook:
    """Deribit full L2 or Bybit standard/options/spread snapshot/delta semantics.

    A reconnect/owner change must call reset before applying data from that new
    session. Bybit standard u/seq are not given the full-book u+1 assumption.
    """
    def __init__(self, instrument, profile, *, depth=None, limits=BookLimits(), signed_prices=False):
        if profile not in {"deribit_full", "bybit_standard"}:
            raise ValueError("unsupported snapshot-book profile")
        if instrument.venue != ("deribit" if profile == "deribit_full" else "bybit"):
            raise ValueError("venue/profile mismatch")
        if profile == "bybit_standard":
            integer(depth, "depth", 1)
            depths = {"option": {25, 100}, "spread": {25}, "spot": {1, 50, 200, 1000},
                      "linear": {1, 50, 200, 1000}, "inverse": {1, 50, 200, 1000}}
            if depth not in depths.get(instrument.native_product, set()):
                raise ValueError("depth/product absent from Bybit standard profile")
            if depth > limits.max_levels_per_side:
                raise ValueError("profile depth exceeds resource bound")
        elif depth is not None:
            raise ValueError("Deribit full feed has no depth cap")
        self.book = L2Book(instrument, limits=limits, signed_prices=signed_prices)
        self.profile, self.depth = profile, depth
        self.epoch = 0
        self.reset("initial")

    def reset(self, reason="reconnect"):
        self.epoch += 1
        self.book.clear()
        self.state, self.reason = "bootstrapping", reason
        self.last_id = self.last_cross = self.last_fingerprint = None
        self.observed_id = self.observed_cross = None
        self.anchor_source = self.last_source = None

    def invalidate(self, reason):
        self.state, self.reason = "invalid", reason
        return "invalid"

    def receive(self, event: DepthEvent):
        if not isinstance(event, DepthEvent) or event.instrument != self.book.instrument:
            raise ValueError("foreign or untyped event")
        if self.profile == "bybit_standard" and event.cross_sequence is None:
            return self.invalidate("missing_cross_sequence")
        self.observed_id = max(event.update_id,self.observed_id or 0)
        if event.cross_sequence is not None:
            self.observed_cross = max(event.cross_sequence,self.observed_cross or 0)
        if event.kind == "snapshot":
            if self.profile == "deribit_full" and event.update_id < self.observed_id:
                return "stale"
            # An invalid same-owner book retains its watermark. A retransmitted
            # old anchor cannot prove recovery; only reset() starts a new owner.
            if self.profile == "deribit_full" and self.last_id is not None:
                if event.update_id == self.last_id:
                    if event.fingerprint == self.last_fingerprint:
                        return "duplicate"
                    return self.invalidate("conflicting_duplicate_update")
                if event.update_id < self.last_id:
                    return "stale"
            if self.profile == "bybit_standard" and self.last_id is not None:
                if event.update_id == self.last_id and event.fingerprint == self.last_fingerprint:
                    return "duplicate"
                if event.update_id != 1 and self.state == "invalid" and event.update_id <= self.last_id:
                    return "stale"
            if self.profile == "bybit_standard" and event.update_id != 1 and (
                    event.update_id < self.observed_id or event.cross_sequence < self.observed_cross):
                return "stale"
            try:
                self.book.apply(event, replace=True)
            except ValueError as exc:
                return self.invalidate(str(exc))
            self.epoch += 1
            self.state, self.reason = "live", None
            self.anchor_source = event.source_record_id
            if self.profile == "bybit_standard" and event.update_id == 1:
                self.observed_id,self.observed_cross = event.update_id,event.cross_sequence
        else:
            if self.state != "live":
                return "ignored_until_snapshot"
            # Native Bybit reset must be handled before treating low u as stale.
            if self.profile == "bybit_standard" and event.update_id == 1:
                return self.invalidate("native_reset_requires_snapshot")
            if event.update_id == self.last_id:
                if event.fingerprint == self.last_fingerprint:
                    return "duplicate"
                return self.invalidate("conflicting_duplicate_update")
            if self.profile == "deribit_full":
                if event.previous_id != self.last_id or event.update_id <= self.last_id:
                    return self.invalidate("broken_prev_change_id")
            else:
                if event.update_id < self.last_id:
                    return "stale"
                if event.cross_sequence is not None and self.last_cross is not None and event.cross_sequence < self.last_cross:
                    return "stale"
            try:
                self.book.apply(event)
            except ValueError as exc:
                return self.invalidate(str(exc))
        self.last_id = event.update_id
        self.last_cross = event.cross_sequence
        self.last_fingerprint = event.fingerprint
        self.last_source = event.source_record_id
        return "applied"

    def view(self):
        levels = self.book.levels()
        bids, asks = levels["bids"], levels["asks"]
        flags = []
        if not bids: flags.append("missing_bid")
        if not asks: flags.append("missing_ask")
        if bids and asks and decimal_value(bids[0][0]) > decimal_value(asks[0][0]):
            flags.append("crossed_book")
        return dict(instrument=asdict(self.book.instrument), profile=self.profile, depth=self.depth,
            state=self.state, reason=self.reason, epoch=self.epoch, update_id=self.last_id,
            cross_sequence=self.last_cross, anchor_source=self.anchor_source, last_source=self.last_source,
            continuity_confidence="sequence_proven" if self.profile == "deribit_full" and self.state == "live"
                else "health_only" if self.state == "live" else "invalid",
            excludes_rpi=self.profile == "bybit_standard", quality_flags=flags, **levels)
