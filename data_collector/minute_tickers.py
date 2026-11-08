"""Deribit agg2 receive cache and append-only native metadata for the minute grid."""
import asyncio
from dataclasses import dataclass
import gzip
import json
from pathlib import Path
import uuid

import aiohttp

from option_collector.common import body_bytes, body_fields, dumps, sha, utc, write_bytes

from .decoders import native_json
from .discovery import METHOD, PARAMS


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


class _MetadataAttempt:
    """The v1 HTTP transport writes attempts here before the change check."""

    def __init__(self, environment):
        self.context = dict(schema_version=1, run_id=uuid.uuid4().hex, job_kind="minute",
                            environment=environment, currency="deribit", scheduled_slot_utc=None)
        self.records = []

    def append(self, record):
        self.records.append(record)
        return record


class TickerSampler:
    def __init__(self, root, url, environment, *, wall):
        self.metadata_dir = Path(root) / "deribit-metadata"
        self.url, self.environment, self.wall = url, environment, wall
        self.inventory, self.catalog, self.latest = {}, {}, {}
        self.version = None
        self.metadata_status = "unchecked"
        self.connection = None
        self.acknowledged = {}
        self.cutoff, self.frozen = None, None
        head = max(self.metadata_dir.glob("*.json.gz"), default=None)
        if head is not None:
            self.version = int(head.name.split(".")[0])
            self.catalog, self.inventory = _catalog(read_deribit_metadata(root, self.version))

    async def refresh(self, client, deadline):
        archive = _MetadataAttempt(self.environment)
        record = await client.fetch(archive, "deribit:metadata", METHOD, PARAMS, deadline)
        if record is None:
            self._metadata_result("missed")
            return
        if not record["ok"]:
            self._metadata_result("timeout" if record["transport_error"] == "TimeoutError" else "error")
            return
        try:
            catalog, inventory = _catalog(record)
        except ValueError:
            self._metadata_result("error")
            return
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
                    await self._receive(socket)
            except (aiohttp.ClientError, OSError, TimeoutError, ValueError):
                pass
            finally:
                self._freeze_if_due(self.wall())
                self.connection = None
                self.acknowledged.clear()
            if clock() - started >= 30:
                delay = 1
            await asyncio.sleep(delay)
            delay = min(delay * 2, 30)

    async def _receive(self, socket):
        clock = asyncio.get_running_loop().time
        await socket.send_json(dict(jsonrpc="2.0", id=1, method="public/set_heartbeat", params={"interval": 10}))
        receive = asyncio.create_task(socket.receive())
        requested, active = {}, {}
        pending = None
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
                        await socket.send_json(dict(jsonrpc="2.0", id=2, method="public/test"))
                    elif pending is not None and value.get("id") == pending[0]:
                        _, method, changes, _ = pending
                        channels = value.get("result")
                        if (not isinstance(channels, list) or any(not isinstance(channel, str) for channel in channels)
                                or sorted(channels) != sorted(map(_channel, changes))):
                            raise ValueError("Deribit ticker subscription acknowledgement differs")
                        if method == "public/subscribe":
                            active.update(changes)
                            self.acknowledged.update({name: (listing, received * 1000)
                                                      for name, listing in changes.items()})
                        else:
                            for name in changes:
                                active.pop(name)
                                requested.pop(name)
                                self.acknowledged.pop(name, None)
                        pending = None
                    elif value.get("method") == "subscription" and isinstance(params, dict):
                        self._ticker(params, message.data, received, requested, active)
                    receive = asyncio.create_task(socket.receive())

                now = clock()
                if pending is not None and now >= pending[3]:
                    raise TimeoutError("Deribit ticker subscription acknowledgement timed out")
                if pending is not None or now < max(next_control, next_reconcile):
                    continue
                next_reconcile = now + .25
                wall_ms = self.wall() * 1000
                desired = {name: key for name, key in self.inventory.items() if key.expires_ms > wall_ms}
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
                pending = request_id, method, changes, now + SOCKET_TIMEOUT
                next_control = now + CONTROL_INTERVAL
                await socket.send_json(dict(jsonrpc="2.0", id=request_id, method=method,
                                            params={"channels": list(map(_channel, changes))}))
                request_id += 1
        finally:
            receive.cancel()
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
