"""REST configuration and run context for the minute grid."""
from dataclasses import asdict, dataclass
import math
from pathlib import Path
import uuid

from .minute_files import dumps, sha, utc

BASE_URLS = {"production": "https://www.deribit.com/api/v2/", "testnet": "https://test.deribit.com/api/v2/"}


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


def source_version():
    """Hash the collector source, which is the flat data_collector package."""
    package = Path(__file__).parent
    paths = sorted(package.glob("*.py")) + [package.parent / n for n in ("requirements.lock", "pyproject.toml")]
    files = {str(p.relative_to(package.parent)): sha(p.read_bytes()) for p in paths if p.exists()}
    return {"source_sha256": sha(dumps(files)), "files": files}


def context(config, currency, kind, slot=None, run_id=None):
    run_id = run_id or str(uuid.uuid4())
    now = utc()
    day = (slot or now)[:10].replace("-", "/")
    destination = f"{kind}/{day}/{run_id}/{currency}"
    public = config.provenance()
    return dict(schema_version=1, capture_span_version=2, run_id=run_id, currency=currency, job_kind=kind,
                environment=config.environment, scheduled_slot_utc=slot, started_utc=now,
                destination=destination, code=source_version(), config=public, config_sha256=sha(dumps(public)),
                required_jobs=[], universe=[], universe_trusted=False)
