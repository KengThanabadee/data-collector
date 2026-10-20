from dataclasses import asdict, dataclass
import math
from pathlib import Path
import tomllib

from .common import dumps, sha, write_json, mkdir

BASE_URLS = {"production": "https://www.deribit.com/api/v2/", "testnet": "https://test.deribit.com/api/v2/"}
VISIBLE = ("open", "settlement", "locked", "halted")


@dataclass(frozen=True)
class Config:
    data_root: Path = Path("data/production")
    environment: str = "production"
    currencies: tuple = ("BTC", "ETH")
    depth: int = 10000
    rate: float = 10
    workers: int = 8
    discovery_rate: float = 1
    attempt_seconds: float = 15
    attempts: int = 3
    body_cap: int = 16 * 1024 * 1024
    acquisition_seconds: float = 240
    hard_seconds: float = 270
    require_ntp: bool = True
    disk_warn: float = .20
    disk_stop: float = .05
    disk_min_bytes: int = 2 * 1024**3
    temporary_reserve_bytes: int = 256 * 1024**2
    notification_file: str = ""

    @property
    def base_url(self):
        return BASE_URLS[self.environment]

    def validate(self):
        for name in ("workers", "attempts", "depth", "body_cap", "disk_min_bytes", "temporary_reserve_bytes"):
            if type(getattr(self, name)) is not int:
                raise ValueError("integer required: " + name)
        if self.environment not in BASE_URLS:
            raise ValueError("invalid environment")
        if not self.currencies or len(set(self.currencies)) != len(self.currencies) or set(self.currencies) - {"BTC", "ETH"}:
            raise ValueError("currencies must be unique BTC/ETH values")
        if self.depth not in (1, 5, 10, 20, 50, 100, 1000, 10000):
            raise ValueError("unsupported depth")
        for name in ("rate", "discovery_rate", "attempt_seconds", "acquisition_seconds", "hard_seconds", "disk_warn", "disk_stop"):
            x = getattr(self, name)
            if isinstance(x, bool) or not isinstance(x, (float, int)) or not math.isfinite(x) or x <= 0:
                raise ValueError("invalid " + name)
        if not (self.rate <= 10 and self.discovery_rate <= 1 and 1 <= self.workers <= 8 and 1 <= self.attempts <= 3):
            raise ValueError("rate/workers/attempts exceed reviewed bounds")
        if not (self.attempt_seconds <= 15 and self.acquisition_seconds <= 240 and self.acquisition_seconds < self.hard_seconds <= 270):
            raise ValueError("invalid deadline bounds")
        if not (0 < self.disk_stop < self.disk_warn < 1) or self.disk_min_bytes < 0 or self.temporary_reserve_bytes < 0:
            raise ValueError("invalid disk thresholds")
        if not isinstance(self.require_ntp, bool) or self.body_cap < 1024 or self.body_cap > 16*1024**2:
            raise ValueError("invalid ntp/body cap")
        return self

    def provenance(self):
        value = asdict(self)
        value["data_root"] = str(self.data_root)
        value.pop("notification_file")
        return value


def load(path):
    path = Path(path).resolve()
    value = tomllib.loads(path.read_text())
    if set(value) != {"collector"}:
        raise ValueError("only [collector] is supported")
    fields = value["collector"]
    root = Path(fields.get("data_root", "data/production")).expanduser()
    fields["data_root"] = (path.parent / root).resolve() if not root.is_absolute() else root.resolve()
    if "currencies" in fields:
        fields["currencies"] = tuple(fields["currencies"])
    return Config(**fields).validate()


def bind_root(config):
    path = mkdir(config.data_root) / "environment.json"
    if path.exists():
        import json
        if json.loads(path.read_bytes()) != {"environment": config.environment}:
            raise ValueError("data root belongs to another environment")
    else:
        write_json(path, {"environment": config.environment})


def source_version():
    package = Path(__file__).parent
    paths = sorted(package.glob("*.py")) + [package.parent / n for n in ("requirements.lock", "pyproject.toml")]
    files = {str(p.relative_to(package.parent)): sha(p.read_bytes()) for p in paths if p.exists()}
    return {"source_sha256": sha(dumps(files)), "files": files}


def retain_source(config):
    """Keep the exact collector source referenced by capture provenance."""
    from .common import write_bytes
    version=source_version()
    base=Path(__file__).parent.parent
    target=config.data_root/"provenance"/version["source_sha256"]
    for name,digest in version["files"].items():
        data=(base/name).read_bytes()
        if sha(data)!=digest:
            raise ValueError("source changed while recording provenance")
        p=target/name
        if p.exists():
            if sha(p.read_bytes())!=digest:raise ValueError("source provenance conflict")
        else:write_bytes(p,data)
    return version
