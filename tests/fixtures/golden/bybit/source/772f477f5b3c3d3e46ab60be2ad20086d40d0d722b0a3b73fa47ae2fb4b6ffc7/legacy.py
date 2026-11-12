"""Explicit legacy dispatch. Never relabel rolling sweeps as streaming snapshots."""
from pathlib import Path

from .contracts import strict_json


def read_legacy_archive(path, *, include_incomplete=False, max_manifest_bytes=None):
    from option_collector.archive import read_archive
    path = Path(path)
    with (path / "manifest.json").open("rb") as handle:
        if max_manifest_bytes is None:
            # Match v1's supported sizes; its verifier also loads the full archive.
            data = handle.read()
        else:
            from .contracts import integer
            integer(max_manifest_bytes, "max_manifest_bytes", 1)
            data = handle.read(max_manifest_bytes + 1)
            if len(data) > max_manifest_bytes:
                raise ValueError("legacy manifest exceeds explicitly requested bound")
    manifest = strict_json(data)
    if type(manifest.get("schema_version")) is not int or manifest["schema_version"] != 1:
        raise ValueError("not a legacy schema-v1 archive")
    return read_archive(path, include_incomplete=include_incomplete)


def legacy_observations(path, *, include_incomplete=False):
    """Return verified v1 observations; no inferred as-of/stream validity."""
    manifest, rows = read_legacy_archive(path, include_incomplete=include_incomplete)
    for row in rows:
        yield dict(kind="legacy_observation", profile="rolling-sweep" if
                   manifest["job_kind"] == "snapshot" else manifest["job_kind"],
                   streaming_coverage="unknown", shared_asof=None,
                   manifest=manifest, original=row)
