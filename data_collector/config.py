"""Strict configuration for v2 discovery and selected option capture.

Loading validates configuration only: it does not bind a data root, start chrony,
contact an exchange or certify a full production release configuration.
"""
from dataclasses import asdict, dataclass, fields
from pathlib import Path
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

    def __post_init__(self):
        if self.schema != SCHEMA or self.venue not in ("deribit","binance"):
            raise ValueError("supported v2 discovery schema/venue required")
        if self.environment not in ("production", "testnet"):
            raise ValueError("explicit production or testnet environment required")
        if self.venue=='binance' and (self.environment!='production' or self.capture_mode!='raw'
                or self.all_options or self.all_futures or self.symbols):
            raise ValueError('Binance public profile requires production raw mode and whole-universe selection')
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
    if 'raw_feeds' in values:
        if not isinstance(values['raw_feeds'],list):raise ValueError('raw_feeds array required')
        values['raw_feeds']=tuple(values['raw_feeds'])
    if "symbols" in values:
        if not isinstance(values["symbols"], list) or any(not isinstance(s,str) for s in values["symbols"]):
            raise ValueError("symbols must be a TOML string array")
        values["symbols"] = tuple(values["symbols"])
    return DiscoveryConfig(**values)
