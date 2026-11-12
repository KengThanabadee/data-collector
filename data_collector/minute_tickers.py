"""Deribit agg2 receive cache and append-only native metadata for the minute grid."""
import asyncio
from dataclasses import asdict, dataclass
import gzip
import json
from pathlib import Path
import uuid

import aiohttp

from .minute_files import body_bytes, body_fields, dumps, sha, utc, write_bytes

from .minute_native import METHOD, PARAMS, native_json
from .minute_reference import ReferenceAttempts, read_reference
from .minute_storage import read_sealed_references


WS_URLS = {
    "production": "wss://www.deribit.com/ws/api/v2",
    "testnet": "wss://test.deribit.com/ws/api/v2",
}
BATCH_SIZE = 500
CONTROL_INTERVAL = 1
SOCKET_TIMEOUT = 10


@dataclass(frozen=True)
class Instrument:
    native_id: int
    created_ms: int
    expires_ms: int


@dataclass(frozen=True)
class _Control:
    request_id: int
    method: str
    changes: dict[str, Instrument]
    started: float
    deadline: float

    @property
    def request(self):
        return dict(jsonrpc="2.0", id=self.request_id, method=self.method,
                    params={"channels": list(map(_channel, self.changes))})


def read_deribit_retirements(root, slot):
    """Read immutable retirement controls initiated in this UTC minute."""
    if type(slot) is not int or slot % 60:
        raise ValueError("expected a UTC minute boundary in Unix seconds")
    controls = []
    for raw in _read_deribit_slot_files(root, slot, "deribit-retirements").values():
        event = json.loads(gzip.decompress(raw))
        body = body_bytes(event)
        if (event["schema_version"] != 1 or not slot <= event["started_epoch_ms"] / 1000 < slot + 60
                or (sha(body) if body is not None else None) != event["body_sha256"]
                or len(body or b"") != event["body_byte_count"]):
            raise ValueError("Deribit retirement evidence differs")
        controls.append(event)
    return sorted(controls, key=lambda event: (event["observed_epoch_ms"], event["connection_id"],
                                               event["request"]["id"], event["status"] != "requested"))


def _catalog(record):
    raw = body_bytes(record)
    if sha(raw) != record["body_sha256"]:
        raise ValueError("Deribit metadata body checksum mismatch")
    rows = native_json(raw, max_bytes=16 * 1024 * 1024).get("result")
    if not isinstance(rows, list) or not rows:
        raise ValueError("empty Deribit metadata")
    catalog, inventory = {}, {}
    for row in rows:
        if not isinstance(row, dict):
            raise ValueError("invalid Deribit metadata row")
        name = row.get("instrument_name")
        if not isinstance(name, str) or not name or "." in name or name in catalog:
            raise ValueError("ambiguous Deribit instrument name")
        catalog[name] = row
        base = row.get("base_currency")
        if base not in {"BTC", "ETH"}:
            continue
        kind = row.get("kind")
        inverse_option = (kind == "option" and row.get("settlement_currency") == base
                          and row.get("instrument_type") != "linear")
        dated_future = kind == "future" and row.get("settlement_period") != "perpetual"
        if inverse_option or dated_future:
            identity = row.get("instrument_id"), row.get("creation_timestamp"), row.get("expiration_timestamp")
            if any(type(value) is not int or value <= 0 for value in identity):
                raise ValueError("invalid Deribit listing identity or expiry")
            inventory[name] = Instrument(*identity)
    if not inventory:
        raise ValueError("empty Deribit sampler inventory")
    return catalog, inventory


def read_deribit_metadata(root, version):
    """Read one immutable native metadata response by its sampler version."""
    if type(version) is not int or version < 1:
        raise ValueError("expected a positive Deribit metadata version")
    path = Path(root) / "deribit-metadata" / f"{version:012d}.json.gz"
    record = json.loads(gzip.decompress(path.read_bytes()))
    raw = body_bytes(record)
    if raw is None or sha(raw) != record["body_sha256"] or len(raw) != record["body_byte_count"]:
        raise ValueError("Deribit metadata body checksum differs")
    return record


def read_deribit_metadata_failures(root, slot):
    """Read failed discovery checks attributed to a UTC minute."""
    if type(slot) is not int or slot % 60:
        raise ValueError("expected a UTC minute boundary in Unix seconds")
    files = _read_deribit_slot_files(root, slot, "deribit-metadata-failures")
    return [read_reference(Path(root) / name, archived=raw) for name, raw in sorted(files.items())]


def _read_deribit_slot_files(root, slot, family):
    root = Path(root)
    directory = Path(family) / utc(slot)[:10]
    prefix = f"{slot:012d}-"
    files = read_sealed_references(root, slot, f"{directory}/{prefix}")
    for path in (root / directory).glob(f"{prefix}*.json.gz"):
        raw = path.read_bytes()
        if files.setdefault(str(path.relative_to(root)), raw) != raw:
            raise ValueError("local Deribit reference differs from verified daily archive")
    return files


class TickerSampler:
    def __init__(self, root, url, environment, *, wall):
        self.metadata_dir = Path(root) / "deribit-metadata"
        self.metadata_failures = Path(root) / "deribit-metadata-failures"
        self.retirement_dir = Path(root) / "deribit-retirements"
        self.url, self.environment, self.wall = url, environment, wall
        self.inventory, self.catalog, self.latest = {}, {}, {}
        self.version = None
        self.metadata_status = "unchecked"
        self.connection = None
        self.acknowledged = {}
        self.retirements_idle = asyncio.Event()
        self.retirements_idle.set()
        self.cutoff, self.frozen = None, None
        head = max(self.metadata_dir.glob("*.json.gz"), default=None)
        if head is not None:
            self.version = int(head.name.split(".")[0])
            self.catalog, self.inventory = _catalog(read_deribit_metadata(root, self.version))

    async def refresh(self, client, slot, deadline):
        archive = ReferenceAttempts(self.environment, "deribit", slot, run_id=uuid.uuid4().hex)
        try:
            record = await client.fetch(archive, "deribit:metadata", METHOD, PARAMS, deadline)
        except asyncio.CancelledError:
            if archive.records:
                self._write_metadata_failure(archive, slot, "interrupted", archive.records)
            raise
        if record is None:
            self._metadata_failed(archive, slot, "missed")
            return
        if not record["ok"]:
            status = "timeout" if record["transport_error"] == "TimeoutError" else "error"
            self._metadata_failed(archive, slot, status)
            return
        try:
            catalog, inventory = _catalog(record)
        except ValueError:
            self._metadata_failed(archive, slot, "error")
            return
        failed_attempts = [attempt for attempt in archive.records if not attempt["ok"]]
        if failed_attempts:
            self._write_metadata_failure(archive, slot, "recovered", failed_attempts)
        if catalog != self.catalog:
            version = 1 if self.version is None else self.version + 1
            path = self.metadata_dir / f"{version:012d}.json.gz"
            write_bytes(path, gzip.compress(dumps(record), compresslevel=6, mtime=0))
            self._freeze_if_due(self.wall())
            self.version, self.catalog, self.inventory = version, catalog, inventory
            self.latest = {
                name: frame for name, frame in self.latest.items()
                if inventory.get(name) == frame[0] and frame[0].expires_ms > self.wall() * 1000
            }
        self._metadata_result("ok")

    def _write_metadata_failure(self, archive, slot, status, responses):
        path = self.metadata_failures / utc(slot)[:10] / f"{slot:012d}-{archive.context['run_id']}.json.gz"
        entry = dict(schema_version=1, captured_slot_utc=utc(slot), status=status, responses=responses)
        write_bytes(path, gzip.compress(dumps(entry), compresslevel=6, mtime=0))

    def _metadata_failed(self, archive, slot, status):
        self._write_metadata_failure(archive, slot, status, archive.records)
        self._metadata_result(status)

    def _metadata_result(self, status):
        self._freeze_if_due(self.wall())
        self.metadata_status = status

    def schedule(self, slot):
        if self.cutoff != slot:
            self.cutoff, self.frozen = slot, None

    def snapshot(self, slot):
        if self.cutoff != slot or self.wall() < slot:
            raise ValueError("sampler cutoff differs from the minute grid")
        self._freeze_if_due(self.wall())
        return self.frozen

    def _freeze_if_due(self, now):
        if self.cutoff is None or self.frozen is not None or now < self.cutoff:
            return
        samples = {}
        for name, listing in self.inventory.items():
            if listing.expires_ms <= self.cutoff * 1000:
                continue
            last = self.latest.get(name)
            ack_listing, ack_ms = self.acknowledged.get(name, (None, None))
            ack_ms = ack_ms if ack_listing == listing else None
            if last is None:
                samples[name] = dict(status="missing", connection_id=None, received_epoch_ms=None,
                                     subscribe_ack_epoch_ms=ack_ms, venue_epoch_ms=None, **body_fields(None))
            else:
                _, connection, acknowledged, received, venue_time, raw = last
                status = "ok" if connection == self.connection and acknowledged else "stale"
                samples[name] = dict(status=status, connection_id=connection, received_epoch_ms=received,
                                     subscribe_ack_epoch_ms=ack_ms, venue_epoch_ms=venue_time, **body_fields(raw))
        self.frozen = dict(schema_version=1, scheduled_slot_utc=utc(self.cutoff), samples=samples,
                           metadata_version=self.version, metadata_status=self.metadata_status,
                           current_connection_id=self.connection)

    async def run(self, session):
        clock = asyncio.get_running_loop().time
        delay = 1
        while True:
            started = clock()
            try:
                async with asyncio.timeout(SOCKET_TIMEOUT):
                    socket = await session.ws_connect(
                        self.url, max_msg_size=2 * 1024 * 1024, compress=15, heartbeat=15,
                        timeout=aiohttp.ClientWSTimeout(ws_close=2),
                    )
                async with socket:
                    self._freeze_if_due(self.wall())
                    self.connection = uuid.uuid4().hex
                    try:
                        await self._receive(socket)
                    finally:
                        self._disconnect()
            except (aiohttp.ClientError, OSError, TimeoutError, ValueError):
                pass
            if clock() - started >= 30:
                delay = 1
            await asyncio.sleep(delay)
            delay = min(delay * 2, 30)

    def _disconnect(self):
        if self.connection is not None:
            self._freeze_if_due(self.wall())
            self.connection = None
            self.acknowledged.clear()

    def _record_retirement(self, control, status, observed, response=None):
        slot = int(control.started // 60) * 60
        phase = "request" if status == "requested" else "outcome"
        directory = self.retirement_dir / utc(slot)[:10]
        path = directory / f"{slot:012d}-{self.connection}-{control.request_id:06d}-{phase}.json.gz"
        record = dict(schema_version=1, environment=self.environment, connection_id=self.connection,
                      request=control.request, instruments={name: asdict(listing)
                                                           for name, listing in control.changes.items()},
                      started_epoch_ms=control.started * 1000, observed_epoch_ms=observed * 1000,
                      status=status, **body_fields(response))
        try:
            write_bytes(path, gzip.compress(dumps(record), compresslevel=6, mtime=0))
        except OSError as exc:
            raise RuntimeError("could not preserve Deribit retirement control") from exc

    async def _receive(self, socket):
        clock = asyncio.get_running_loop().time
        await socket.send_json(dict(jsonrpc="2.0", id=1, method="public/set_heartbeat", params={"interval": 10}))
        receive = asyncio.create_task(socket.receive())
        requested, active = {}, {}
        pending = None
        failure = "disconnected"
        request_id, next_control, next_reconcile = 10, 0, 0
        try:
            while True:
                done, _ = await asyncio.wait({receive}, timeout=.25)
                if done:
                    message = receive.result()
                    received = self.wall()
                    self._freeze_if_due(received)
                    if message.type != aiohttp.WSMsgType.TEXT:
                        return
                    value = json.loads(message.data)
                    if not isinstance(value, dict):
                        raise ValueError("invalid Deribit WebSocket envelope")
                    params = value.get("params")
                    if value.get("method") == "heartbeat" and isinstance(params, dict) and params.get("type") == "test_request":
                        async with asyncio.timeout(SOCKET_TIMEOUT):
                            await socket.send_json(dict(jsonrpc="2.0", id=2, method="public/test"))
                    elif pending is not None and value.get("id") == pending.request_id:
                        control, pending = pending, None
                        changes = control.changes
                        if control.method == "public/subscribe":
                            channels = value.get("result")
                            if (value.get("error") is not None or not isinstance(channels, list)
                                    or any(not isinstance(channel, str) for channel in channels)
                                    or sorted(channels) != sorted(map(_channel, changes))):
                                raise ValueError("Deribit ticker subscription acknowledgement differs")
                            active.update(changes)
                            self.acknowledged.update({name: (listing, received * 1000)
                                                      for name, listing in changes.items()})
                        else:
                            status = _retirement_status(control, value)
                            try:
                                self._record_retirement(control, status, received, message.data.encode("utf-8"))
                            finally:
                                self.retirements_idle.set()
                            if status not in ("acknowledged", "already_expired"):
                                raise ValueError("Deribit ticker retirement acknowledgement differs")
                            for name in changes:
                                active.pop(name)
                                requested.pop(name)
                                self.acknowledged.pop(name, None)
                    elif value.get("method") == "subscription" and isinstance(params, dict):
                        self._ticker(params, message.data, received, requested, active)
                    receive = asyncio.create_task(socket.receive())

                now = clock()
                if pending is not None and now >= pending.deadline:
                    failure = "timeout"
                    raise TimeoutError("Deribit ticker subscription acknowledgement timed out")
                if pending is not None or now < max(next_control, next_reconcile):
                    continue
                next_reconcile = now + .25
                started = self.wall()
                desired = {name: key for name, key in self.inventory.items() if key.expires_ms > started * 1000}
                removals = [name for name in active if active[name] != desired.get(name)][:BATCH_SIZE]
                additions = [name for name in desired if name not in active][:BATCH_SIZE]
                if removals:
                    method = "public/unsubscribe"
                    changes = {name: active[name] for name in removals}
                elif additions:
                    method = "public/subscribe"
                    changes = {name: desired[name] for name in additions}
                    requested.update(changes)
                else:
                    continue
                control = _Control(request_id, method, changes, started, now + SOCKET_TIMEOUT)
                if method == "public/unsubscribe":
                    self._record_retirement(control, "requested", started)
                    self.retirements_idle.clear()
                pending = control
                next_control = now + CONTROL_INTERVAL
                failure = "send_error"
                async with asyncio.timeout_at(control.deadline):
                    await socket.send_json(control.request)
                failure = "disconnected"
                request_id += 1
        except asyncio.CancelledError:
            failure = "interrupted"
            raise
        finally:
            receive.cancel()
            try:
                if pending is not None and pending.method == "public/unsubscribe":
                    self._record_retirement(pending, failure, self.wall())
            finally:
                self.retirements_idle.set()
                self._disconnect()
                await asyncio.gather(receive, return_exceptions=True)

    def _ticker(self, params, text, received, requested, active):
        data = params.get("data")
        if not isinstance(data, dict):
            return
        name, venue_time = data.get("instrument_name"), data.get("timestamp")
        listing = requested.get(name)
        if (listing is None or listing != self.inventory.get(name) or listing.expires_ms <= received * 1000
                or params.get("channel") != _channel(name) or type(venue_time) is not int):
            return
        self.latest[name] = (listing, self.connection, active.get(name) == listing,
                             received * 1000, venue_time, text.encode("utf-8"))


def _channel(name):
    return f"ticker.{name}.agg2"


def _retirement_status(control, reply):
    if reply.get("error") is not None:
        return "rejected"
    channels = reply.get("result")
    if not isinstance(channels, list) or any(not isinstance(channel, str) for channel in channels):
        return "invalid_ack"
    acknowledged = set(channels)
    expected = {_channel(name) for name in control.changes}
    # Deribit may already have removed expired channels. An omission for a
    # listing that had not expired when we sent the request is still a failure.
    required = {_channel(name) for name, listing in control.changes.items()
                if listing.expires_ms > control.started * 1000}
    if len(acknowledged) != len(channels) or not required <= acknowledged <= expected:
        return "invalid_ack"
    return "acknowledged" if acknowledged else "already_expired"
