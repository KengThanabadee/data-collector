"""Strict configuration for v2 discovery and selected option capture.

Loading validates configuration only: it does not bind a data root, start chrony,
contact an exchange or certify a full production release configuration.
"""
from dataclasses import asdict, dataclass, fields
from pathlib import Path
import re
import tomllib

from .contracts import canonical, digest, integer, text

SCHEMA = "dc.config/2"
MAX_CONFIG_BYTES = 64 * 1024


@dataclass(frozen=True)
class DiscoveryConfig:
    data_root: Path
    environment: str
    schema: str = SCHEMA
    venue: str = "deribit"
    max_response_bytes: int = 8 * 1024 * 1024
    max_instruments: int = 10000
    timeout_seconds: int = 30
    warmup_timeout_seconds: int = 180
    chronyc: str = "chronyc"
    symbols: tuple[str, ...] = ()
    duration_seconds: int = 0
    max_frames: int = 0
    max_bytes: int = 0
    capture_mode: str = "raw"
    rediscover_options: bool = False
    all_options: bool = False
    all_futures: bool = False
    raw_processes: int = 4
    raw_segment_bytes: int = 8 * 1024 * 1024
    supervisor_pid: int | None = None
    raw_feeds: tuple[str, ...] = ('book',)
    source_discovery_root: Path | None = None
    subscription_budget_file: Path | None = None
    subscription_batch_size: int = 500
    bootstrap_timeout_seconds: int = 180
    bybit_rest_host: str | None = None
    bybit_ws_host: str | None = None
    option_partition_count: int = 1
    option_partition_index: int = 0
    binance_symbols: tuple[str, ...] = ('BTCUSDT','ETHUSDT')
    discovery_interval_seconds: int = 60
    shard_arg_chars: int = 14000
    shard_max_args: int = 1000
    event_streams: bool = False
    bybit_liquidation_symbols: tuple[str, ...] = ('BTCUSDT', 'ETHUSDT')

    def __post_init__(self):
        if self.schema != SCHEMA or self.venue not in ("deribit","binance","bybit"):
            raise ValueError("supported v2 discovery schema/venue required")
        if self.environment not in ("production", "testnet"):
            raise ValueError("explicit production or testnet environment required")
        if type(self.event_streams) is not bool or (self.event_streams and (
                self.venue not in ('binance','bybit') or self.environment!='production'
                or self.capture_mode!='raw' or self.all_options or self.all_futures or self.symbols
                or self.rediscover_options or self.raw_processes!=1)):
            raise ValueError('event_streams requires production single-process Binance or Bybit raw capture')
        if self.venue=='bybit':
            for name,default in (('bybit_rest_host','api.bybit.com'),('bybit_ws_host','stream.bybit.com')):
                if getattr(self,name) is None:object.__setattr__(self,name,default)
            if not self.event_streams and (self.environment!='production' or self.capture_mode!='raw' or not self.all_options
                    or not self.rediscover_options or self.raw_processes!=1 or self.symbols
                    or self.option_partition_count!=1 or self.option_partition_index!=0
                    or self.raw_feeds!=('ticker','trades')):
                raise ValueError('Bybit requires production single-process all_options rediscovery with ticker/trades')
            for name,low,high in (('discovery_interval_seconds',1,100),('shard_arg_chars',1000,21000),
                    ('shard_max_args',1,2000)):
                integer(getattr(self,name),name,low)
                if getattr(self,name)>high:raise ValueError('Bybit bound exceeded: '+name)
        for name in ('bybit_rest_host','bybit_ws_host'):
            host=getattr(self,name)
            if host is not None:
                if (not isinstance(host,str) or len(host)>253 or not host or any(
                        re.fullmatch(r'[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?',label) is None
                        for label in host.split('.'))):
                    raise ValueError('plain DNS hostname required: '+name)
        if self.venue=='binance' and (self.environment!='production' or self.capture_mode!='raw'
                or self.all_options or self.all_futures or self.symbols):
            raise ValueError('Binance public profile requires production raw mode and whole-universe selection')
        if (type(self.binance_symbols) is not tuple or not self.binance_symbols
                or len(set(self.binance_symbols))!=len(self.binance_symbols)
                or any(not isinstance(v,str) or re.fullmatch(r'[A-Z0-9_]{1,40}',v) is None for v in self.binance_symbols)):
            raise ValueError('distinct native Binance symbols required')
        if len(self.binance_symbols)*2>256:raise ValueError('Binance configured list exceeds route limit')
        hedge_symbols = self.bybit_liquidation_symbols
        if (type(hedge_symbols) is not tuple or not 1<=len(hedge_symbols)<=100
                or any(not isinstance(v,str) or re.fullmatch(r'[A-Z0-9]{1,40}',v) is None for v in hedge_symbols)
                or len(set(hedge_symbols))!=len(hedge_symbols)):
            raise ValueError('one to 100 distinct native Bybit linear symbols required')
        if not isinstance(self.data_root, Path) or not self.data_root.is_absolute():
            raise ValueError("absolute data_root required")
        for name, maximum in (("max_response_bytes", 16 * 1024 * 1024),
                              ("max_instruments", 10000), ("timeout_seconds", 60),
                              ("warmup_timeout_seconds", 600)):
            value = getattr(self, name)
            integer(value, name, 1)
            if value > maximum:
                raise ValueError("discovery bound exceeded: " + name)
        for name in ('duration_seconds', 'max_frames', 'max_bytes'):
            integer(getattr(self, name), name, 0)
        if self.capture_mode not in ('raw', 'snapshots'):
            raise ValueError('capture_mode must be raw or snapshots')
        if self.capture_mode == 'snapshots' and not (1 <= self.duration_seconds <= 120 and 1 <= self.max_frames <= 64):
            raise ValueError('snapshot mode requires bounded duration and frames')
        if self.supervisor_pid is not None:integer(self.supervisor_pid,'supervisor pid',2)
        integer(self.raw_processes,'raw processes',1)
        if self.raw_processes>8:raise ValueError('at most eight raw processes')
        integer(self.raw_segment_bytes,'raw segment bytes',1024*1024)
        if self.raw_segment_bytes>64*1024*1024:raise ValueError('raw segment byte bound')
        if type(self.raw_feeds) is not tuple or not self.raw_feeds or len(set(self.raw_feeds))!=len(self.raw_feeds) or set(self.raw_feeds)-{'book','ticker','trades'}:
            raise ValueError('distinct supported raw feeds required')
        for name in ('source_discovery_root','subscription_budget_file'):
            value=getattr(self,name)
            if value is not None and (not isinstance(value,Path) or not value.is_absolute() or self.capture_mode!='raw'):
                raise ValueError('absolute raw capture reference path required')
        if (self.all_options or self.all_futures) and self.source_discovery_root is not None:
            raise ValueError('whole-family capture requires fresh discovery')
        if type(self.rediscover_options) is not bool or (self.rediscover_options and (
                self.venue not in ('deribit','bybit') or not self.all_options or self.capture_mode!='raw')):
            raise ValueError('rediscover_options requires Deribit/Bybit all_options raw capture')
        integer(self.option_partition_count,'option partition count',1)
        integer(self.option_partition_index,'option partition index',0)
        if (self.option_partition_count>8 or self.option_partition_index>=self.option_partition_count or
                (self.option_partition_count>1 and (not self.rediscover_options or self.raw_processes!=1))):
            raise ValueError('distinct live option worker partition required')
        if self.all_options and self.raw_processes>1 and (self.max_frames or self.max_bytes):
            raise ValueError('positive frame/byte budgets require raw_processes=1')
        if type(self.all_options) is not bool or (self.all_options and (self.symbols or self.capture_mode!='raw')):
            raise ValueError('all_options requires raw mode and no explicit symbols')
        if type(self.all_futures) is not bool or (self.all_futures and (
                self.symbols or self.all_options or self.capture_mode!='raw')):
            raise ValueError('all_futures requires exclusive raw selection')
        for name,maximum in (('subscription_batch_size',500),('bootstrap_timeout_seconds',600)):
            integer(getattr(self,name),name,1)
            if getattr(self,name)>maximum:raise ValueError('subscription bound exceeded: '+name)
        if self.rediscover_options and self.venue=='deribit' and self.subscription_batch_size<5:
            raise ValueError('live option batches must fit the five global channels')
        text(self.chronyc, "chronyc executable")
        # An executable name/path, never a shell command or argument list.
        if self.chronyc.startswith("-") or "\x7f" in self.chronyc:
            raise ValueError("chronyc executable name/path required")
        if type(self.symbols) is not tuple or len(self.symbols)>(2 if self.capture_mode=='snapshots' else self.max_instruments) or len(set(self.symbols))!=len(self.symbols):
            raise ValueError("distinct explicit symbols within the instrument bound required")
        for symbol in self.symbols:
            text(symbol, "native symbol")
            if len(symbol)>128 or any(c in symbol for c in '/\\.'):
                raise ValueError("plain Deribit native symbol required")

    def to_bytes(self):
        """Normalized effective values, including defaults and resolved root."""
        value = asdict(self)
        value["data_root"] = str(self.data_root)
        for name in ('source_discovery_root','subscription_budget_file'):
            if value[name] is not None:value[name]=str(value[name])
        return canonical(value)

    @property
    def config_sha256(self):
        return digest(self.to_bytes())


def load(path):
    """Resolve paths relative to TOML, reject v1/unknown fields, perform no writes."""
    path = Path(path).expanduser().resolve()
    with path.open("rb") as source:
        encoded = source.read(MAX_CONFIG_BYTES + 1)
    if len(encoded) > MAX_CONFIG_BYTES:
        raise ValueError("configuration exceeds byte bound")
    value = tomllib.loads(encoded.decode("utf-8"))
    if set(value) != {"data_collector"} or not isinstance(value["data_collector"], dict):
        raise ValueError("only v2 [data_collector] is supported; v1 uses [collector]")
    values = value["data_collector"]
    allowed = {field.name for field in fields(DiscoveryConfig)}
    if values.keys() - allowed:
        raise ValueError("unknown discovery configuration field")
    if not {"schema", "environment", "data_root"} <= values.keys():
        raise ValueError("explicit schema, environment and data_root required")
    root = values["data_root"]
    text(root, "data_root")
    root = Path(root).expanduser()
    values["data_root"] = (path.parent / root).resolve()
    for name in ('source_discovery_root','subscription_budget_file'):
        if name in values:
            text(values[name],name)
            values[name]=(path.parent/Path(values[name]).expanduser()).resolve()
    for name in ('raw_feeds','binance_symbols','bybit_liquidation_symbols'):
        if name in values:
            if not isinstance(values[name],list):raise ValueError(name+' array required')
            values[name]=tuple(values[name])
    if "symbols" in values:
        if not isinstance(values["symbols"], list) or any(not isinstance(s,str) for s in values["symbols"]):
            raise ValueError("symbols must be a TOML string array")
        values["symbols"] = tuple(values["symbols"])
    return DiscoveryConfig(**values)
