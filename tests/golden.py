"""Capture golden archives through `python -m data_collector` from recorded venue replies.

Every capture goes through `main(argv)`. Recorded REST replies are served on
loopback; event sockets are fakes that replay recorded pushes. Run this module
as a script to regenerate the fixtures (see tests/fixtures/golden/README.md).
"""
import asyncio
from contextlib import redirect_stdout
from datetime import datetime, timezone
import heapq
import io
import json
from pathlib import Path
import shutil
import sys
import tempfile
import time
from unittest.mock import patch

import aiohttp
from aiohttp import web

import data_collector.cli as cli


FIXTURES = Path(__file__).parent / "fixtures"
GOLDEN = FIXTURES / "golden"
DAY = "2026-11-08"
FIRST_SLOT = int(datetime(2026, 11, 8, 23, 58, tzinfo=timezone.utc).timestamp())
MINUTES = 2
BINANCE_SYMBOLS = "BTCUSDT,ETHUSDT"
REPLY_DATE = "Sun, 08 Nov 2026 23:58:00 GMT"
MINUTE_ARGS = ["--rest-only", "--minutes", str(MINUTES), "--binance-symbols", BINANCE_SYMBOLS,
               "--bybit-depth-btc", "2", "--bybit-depth-eth", "2", "--bybit-expiries", "1"]

DERIBIT = {
    (row["method"], tuple(sorted(row["params"].items()))): row["body"].encode()
    for row in json.loads((FIXTURES / "dc_deribit_minute_rest.json").read_bytes())
}
BYBIT = json.loads((FIXTURES / "dc_bybit_minute_rest.json").read_bytes())
BINANCE = json.loads((FIXTURES / "dc_binance_minute_rest.json").read_bytes())
BINANCE_EVENTS = json.loads((FIXTURES / "dc_binance_selected.json").read_bytes())["samples"]
BYBIT_EVENTS = json.loads((FIXTURES / "dc_bybit_options.json").read_bytes())["samples"]

# Event configs mirror the shipped Binance and Bybit event configs, bounded to one second.
EVENT_CONFIGS = {
    "binance": """[data_collector]
schema = "dc.config/2"
venue = "binance"
environment = "production"
data_root = "data"
capture_mode = "raw"
event_streams = true
raw_processes = 1
raw_segment_bytes = 8388608
max_response_bytes = 8388608
timeout_seconds = 60
warmup_timeout_seconds = 180
bootstrap_timeout_seconds = 600
duration_seconds = 1
max_frames = 0
max_bytes = 0
""",
    "bybit": """[data_collector]
schema = "dc.config/2"
venue = "bybit"
environment = "production"
data_root = "data"
capture_mode = "raw"
event_streams = true
bybit_liquidation_symbols = ["BTCUSDT", "ETHUSDT"]
raw_processes = 1
raw_segment_bytes = 8388608
bybit_rest_host = "api.bytick.com"
bybit_ws_host = "stream.bytick.com"
bootstrap_timeout_seconds = 180
timeout_seconds = 30
warmup_timeout_seconds = 180
chronyc = "chronyc"
duration_seconds = 1
max_frames = 0
max_bytes = 0
""",
}
# Representative hedge-perp push using the documented all-liquidation schema.
BYBIT_LIQUIDATION = (b'{"topic":"allLiquidation.ETHUSDT", "type":"snapshot", "ts":1739502303204, "data":['
                     b'{"T":1739502302929,"s":"ETHUSDT","S":"Sell","v":"0.34","p":"2258.9400"},'
                     b'{"T":1739502303040,"s":"ETHUSDT","S":"Buy","v":"0.01","p":"2258.9500"}]}')


def run_cli(argv):
    """Run the command line and return its exit status and parsed JSON output."""
    output = io.StringIO()
    with redirect_stdout(output):
        code = cli.main([str(value) for value in argv])
    return code, json.loads(output.getvalue())


# Minute grid ------------------------------------------------------------------

class MinuteVenues:
    """Loopback venue serving recorded Deribit, Bybit and Binance REST replies.

    Date and Server headers are fixed so that archived response headers do not
    depend on when or with which aiohttp release the fixtures were produced.
    """

    def __init__(self, clock):
        self.clock = clock
        self.binance_used = {}

    async def deribit(self, request):
        body = DERIBIT.get((request.match_info["method"], tuple(sorted(request.query.items()))))
        return web.Response(body=body, status=200 if body is not None else 404)

    async def bybit(self, request):
        method, params = request.match_info["method"], dict(request.query)
        symbol = params.get("symbol")
        if params.get("category") == "option" and method in ("tickers", "instruments-info"):
            kind = "options" if method == "tickers" else "metadata"
            body = BYBIT[f"{kind}-{params['baseCoin'].lower()}"]
        elif method == "tickers":
            body = BYBIT["perp-btc"].replace("BTCUSDT", symbol)
        elif method == "instruments-info":
            body = BYBIT["linear-meta-btc"].replace("BTCUSDT", symbol)
        elif method == "risk-limit":
            body = BYBIT["risk-btc"].replace("BTCUSDT", symbol)
        elif method == "new-delivery-price":
            body = BYBIT["delivery-btc-usdc" if params["settleCoin"] == "USDC" else "delivery-btc"]
        elif method == "orderbook":
            body = ('{"retCode":0,"retMsg":"OK","result":{"s":"%s","b":[["1.2300","0.50"]],'
                    '"a":[],"ts":1794124740123},"time":1794124740125}' % symbol)
        else:
            return web.Response(status=404)
        return web.Response(body=body.encode())

    async def binance(self, request):
        method, params = request.match_info["method"], dict(request.query)
        bucket = int(self.clock.wall) // 60 * 60
        used = self.binance_used.get(bucket, 0) + (2 if method == "depth" else 1)
        self.binance_used[bucket] = used
        if method == "depth":
            body = BINANCE["depth"]
        elif method == "openInterest":
            body = BINANCE["open_interest"].replace("BTCUSDT", params["symbol"])
        elif method == "exchangeInfo":
            body = BINANCE["metadata"]
        else:
            return web.Response(status=404)
        return web.Response(body=body.encode(), headers={"X-MBX-USED-WEIGHT-1M": str(used)})

    @web.middleware
    async def fixed_headers(self, request, handler):
        response = await handler(request)
        response.headers["Date"] = REPLY_DATE
        response.headers["Server"] = "golden-replay"
        return response

    async def start(self):
        app = web.Application(middlewares=[self.fixed_headers])
        app.router.add_get("/public/{method}", self.deribit)
        app.router.add_get("/v5/market/{method}", self.bybit)
        app.router.add_get("/fapi/v1/{method}", self.binance)
        self.runner = web.AppRunner(app)
        await self.runner.setup()
        site = web.TCPSite(self.runner, "127.0.0.1", 0)
        await site.start()
        return f"http://127.0.0.1:{self.runner.addresses[0][1]}/"


class MinuteClock:
    """Discrete-event wall and monotonic clocks for the minute grid.

    HTTP exchanges run one at a time in the order they are requested, and the
    clocks jump to the earliest pending sleep only once no exchange is pending
    and every other task is waiting. Each response is therefore received at the
    time its request was sent, and records keep one order whatever the loopback
    latency.
    """

    QUIET_YIELDS = 100

    def __init__(self, wall):
        self.wall = wall
        self.elapsed = 0.0
        self.exchanges = 0
        self.exchange_lock = asyncio.Lock()
        self.sleepers = []
        self.sequence = 0

    async def sleep(self, seconds):
        wake = asyncio.get_running_loop().create_future()
        self.sequence += 1
        heapq.heappush(self.sleepers, (self.elapsed + max(0, seconds), self.sequence, wake))
        await wake

    async def drive(self):
        while True:
            quiet = 0
            while quiet < self.QUIET_YIELDS:
                if self.exchanges:
                    quiet = 0
                    await asyncio.sleep(.001)
                else:
                    quiet += 1
                    await asyncio.sleep(0)
            if self.sleepers:
                when, _, wake = heapq.heappop(self.sleepers)
                self.wall += when - self.elapsed
                self.elapsed = when
                if not wake.done():
                    wake.set_result(None)

    def tracked_get(self, real_get):
        clock = self

        class Exchange:
            def __init__(self, request):
                self.request = request

            async def __aenter__(self):
                clock.exchanges += 1
                try:
                    await clock.exchange_lock.acquire()
                    try:
                        return await self.request.__aenter__()
                    except BaseException:
                        clock.exchange_lock.release()
                        raise
                except BaseException:
                    clock.exchanges -= 1
                    raise

            async def __aexit__(self, *exc):
                try:
                    return await self.request.__aexit__(*exc)
                finally:
                    clock.exchange_lock.release()
                    clock.exchanges -= 1

        return lambda session, *args, **kwargs: Exchange(real_get(session, *args, **kwargs))


def capture_minute_archive(root):
    """Capture two minutes ending the UTC day through `minute-capture`, then seal the day."""
    clock = MinuteClock(FIRST_SLOT - 10)
    real_capture = cli.capture_minutes

    async def replayed(config, *, bybit_base_url, binance_base_url, **kwargs):
        venues = MinuteVenues(clock)
        url = await venues.start()
        driver = asyncio.create_task(clock.drive())
        try:
            with (patch.object(type(config), "base_url", property(lambda _: url)),
                  patch.object(aiohttp.ClientSession, "get", clock.tracked_get(aiohttp.ClientSession.get))):
                return await real_capture(
                    config, bybit_base_url=url if bybit_base_url else None,
                    binance_base_url=url if binance_base_url else None,
                    wall=lambda: clock.wall, clock=lambda: clock.elapsed, sleep=clock.sleep, **kwargs)
        finally:
            driver.cancel()
            await asyncio.gather(driver, return_exceptions=True)
            await venues.runner.cleanup()

    with patch.object(cli, "capture_minutes", replayed):
        capture = run_cli(["minute-capture", "--data-root", root, *MINUTE_ARGS])
    seal = run_cli(["minute-seal", "--data-root", root, "--day", DAY])
    return capture, seal


# Event streams ----------------------------------------------------------------

class Socket:
    """Fake WebSocket that answers each subscription with recorded pushes."""

    def __init__(self, replies):
        self.incoming = asyncio.Queue()
        self.sent = []
        self.replies = replies

    def put(self, body):
        self.incoming.put_nowait(aiohttp.WSMessage(aiohttp.WSMsgType.BINARY, body, ""))

    async def send_str(self, body):
        value = json.loads(body)
        self.sent.append(value)
        for reply in self.replies(value):
            self.put(reply)

    async def receive(self):
        return await self.incoming.get()

    async def close(self):
        pass


class EventSession:
    def __init__(self, venue):
        self.venue = venue
        self.sockets = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        pass

    def replies(self, url, value):
        if self.venue == "binance":
            return [BINANCE_EVENTS["ack"].encode(), *(BINANCE_EVENTS[topic].encode() for topic in value["params"])]
        if value.get("op") != "subscribe":
            return []
        if url.endswith("/linear"):
            return [b'{"success":true,"ret_msg":"","conn_id":"linear-test","req_id":"","op":"subscribe"}',
                    BYBIT_LIQUIDATION]
        ack = json.dumps(dict(success=True, type="COMMAND_RESP", conn_id="option-test",
                              data=dict(failTopics=[], successTopics=["publicTrade.ETH", "publicTrade.BTC"])),
                         separators=(",", ":")).encode()
        return [ack, BYBIT_EVENTS["trade"]["body"].encode()]

    async def ws_connect(self, url, **kwargs):
        socket = Socket(lambda value: self.replies(url, value))
        self.sockets.append(url)
        return socket


def _chrony_tracking(utc_ns):
    fields = ["CB00710F", "192.0.2.1", "3", f"{utc_ns / 1e9:.9f}", "0", "0", "0", "0", "0", "0", "0", "0",
              "64.2", "Normal"]
    return (",".join(fields) + "\n").encode()


class EventClock:
    """Real clocks shifted forward when the clock sampler waits, as the app tests do."""

    def __init__(self):
        self.offset = 0
        self.reads = 0

    def observe(self):
        return time.time_ns() + self.offset, time.monotonic_ns() + self.offset, "golden-boot"

    async def read(self, *args):
        self.reads += 1
        return 0, _chrony_tracking(self.observe()[0]), b""

    async def wait(self, deadline):
        if self.reads < 4:
            self.offset += max(0, deadline - self.observe()[1])
            await asyncio.sleep(0)
        else:
            await asyncio.Event().wait()


def capture_event_run(directory, venue):
    """Capture one second of the event streams through `capture`; return the run root."""
    directory = Path(directory)
    config = directory / f"{venue}-events.toml"
    config.write_text(EVENT_CONFIGS[venue])
    (directory / "data").mkdir()
    session = EventSession(venue)
    clock = EventClock()
    real_run = cli.run

    async def replayed(config, **kwargs):
        return await real_run(config, session_factory=lambda: session, observer=clock.observe,
                              clock_read=clock.read, clock_wait=clock.wait, **kwargs)

    with patch.object(cli, "run", replayed):
        code, result = run_cli(["capture", "--config", config])
    return code, result


def generate(target):
    """Write fresh golden fixtures under `target` from the checked-out code."""
    target = Path(target)
    if target.exists():
        raise FileExistsError(target)
    with tempfile.TemporaryDirectory() as temp:
        temp = Path(temp)
        minute = temp / "minute"
        minute.mkdir()
        (capture, seal) = capture_minute_archive(minute)
        assert capture[0] == 0 and seal[0] == 0, (capture, seal)
        shutil.copytree(minute, target / "minute", ignore=shutil.ignore_patterns("*.lock"))
        for venue in ("binance", "bybit"):
            directory = temp / venue
            directory.mkdir()
            code, result = capture_event_run(directory, venue)
            assert code == 0 and result["status"] == "raw_capture_complete", result
            shutil.copytree(result["root"], target / venue, ignore=shutil.ignore_patterns("*.lock"))


if __name__ == "__main__":
    generate(sys.argv[1])
