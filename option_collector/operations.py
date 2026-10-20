"""Preflight, slot evidence, monitoring and verified export."""
import asyncio
import math
from pathlib import Path
import shutil
import subprocess
import time
import uuid
import zlib
from urllib.parse import urlparse

import aiohttp
from .archive import verify, read_archive
from .common import epoch, event, mkdir, process_lock, read_json, utc, write_bytes, write_json


def clock_synced():
    try:
        result = subprocess.run(["timedatectl", "show", "--property=NTPSynchronized", "--value"], capture_output=True, text=True, timeout=2)
        return result.returncode == 0 and result.stdout.strip() == "yes"
    except (OSError, subprocess.TimeoutExpired):
        return False


def preflight(config):
    disk = shutil.disk_usage(config.data_root)
    reasons = []
    if disk.free/disk.total < config.disk_stop or disk.free < max(config.disk_min_bytes,config.temporary_reserve_bytes):
        reasons.append("disk_low")
    synchronized = clock_synced() if config.require_ntp else None
    clock_ok = not config.require_ntp or synchronized
    if read_json(config.data_root/"state"/"clock.json",{}).get("unsafe",False):
        clock_ok=False
    if not clock_ok:
        reasons.append("clock_unsafe")
    return dict(ok=not reasons, reasons=reasons, disk_free_bytes=disk.free, disk_total_bytes=disk.total,
                disk_warning=disk.free/disk.total < config.disk_warn, clock_required=config.require_ntp, clock_synced=synchronized)


def probe_clock(config):
    try:
        with process_lock(config.data_root,"clock.lock"):
            return _probe_clock(config)
    except BlockingIOError:
        return not read_json(config.data_root/"state"/"clock.json",{}).get("unsafe",False)


def _probe_clock(config):
    """Linux boot-scoped comparison detects wall-clock steps between processes."""
    boot=boot_id()
    wall,mono=time.time(),time.monotonic()
    path=config.data_root/"state"/"clock.json"
    previous=read_json(path,{})
    drift=0
    if boot is not None and previous.get("boot")==boot:
        drift=(wall-previous["wall"])-(mono-previous["monotonic"])
    unsafe=abs(drift)>1 or (config.require_ntp and not clock_synced())
    write_json(path,dict(wall=wall,monotonic=mono,boot=boot,unsafe=unsafe,drift_seconds=drift),replace=True)
    if unsafe!=previous.get("unsafe",False):
        event(config.data_root,"clock_unsafe" if unsafe else "clock_resumed",drift_seconds=drift)
    return not unsafe


def boot_id():
    boot_path=Path("/proc/sys/kernel/random/boot_id")
    return boot_path.read_text().strip() if boot_path.exists() else None


def slot_label(slot):
    return utc(slot)


def terminal_events(config, slot, reason, *, status="failed"):
    return [event(config.data_root,"terminal" if status == "failed" else "skip",job_kind="snapshot",currency=c,
                  scheduled_slot_utc=slot_label(slot),run_id=str(uuid.uuid4()),status=status,reason=reason,archive=None)
            for c in config.currencies]


def claim(config, slot):
    directory = mkdir(config.data_root / "state" / "slots")
    name = str(int(slot))
    write_json(directory / (name + ".json"), dict(scheduled_slot_utc=slot_label(slot), claimed_utc=utc()))


def lifecycle_events(root):
    events = [read_json(p) for p in (root/"events"/"lifecycle").glob("*.json")]
    events = [e for e in events if e["kind"] in ("pause", "resume")]
    # Legacy events have no sequence; their observed time is the best evidence.
    return sorted(events, key=lambda e: (e["effective_epoch"], e.get("sequence", 0),
                                         e["observed_utc"], e["event_id"]))


def enabled_at(events, slot):
    enabled = False
    for e in events:
        if e["effective_epoch"] <= slot:
            enabled = e["kind"] == "resume"
    return enabled


def pause_overlaps(root, start, end):
    events = lifecycle_events(root)
    # Replay the state before the window and collapse commands at one boundary.
    boundaries = sorted({start} | {e["effective_epoch"] for e in events
                                      if start < e["effective_epoch"] < end})
    for boundary in boundaries:
        active = [e for e in events if e["effective_epoch"] <= boundary]
        if active and not enabled_at(active, boundary):
            return True
    return False


def schedule(config, enabled):
    with process_lock(config.data_root, "schedule.lock"):
        now = time.time()
        effective = math.ceil(now/300)*300
        events = lifecycle_events(config.data_root)
        sequence = max((e.get("sequence", 0) for e in events), default=0)+1
        event(config.data_root,"resume" if enabled else "pause", effective_epoch=effective, sequence=sequence)
        state = read_json(config.data_root / "state" / "schedule.json", {})
        state.update(enabled=enabled, effective_epoch=effective, sequence=sequence)
        state.setdefault("enabled_from_epoch",effective)
        write_json(config.data_root / "state" / "schedule.json",state,replace=True)
        return state


def save_result(config, result):
    path=config.data_root / "state" / f"latest-{result['job_kind']}-{result['currency']}.json"
    old=read_json(path)
    def started(value):
        return epoch(read_json(config.data_root/value["destination"]/"context.json")["started_utc"])
    if old is None or started(result)>=started(old):
        write_json(path,result,replace=True)
    if result.get("scheduled_slot_utc") is not None:
        scheduled=config.data_root/"state"/("latest-scheduled-"+result['currency']+".json")
        old=read_json(scheduled)
        if old is None or epoch(result["scheduled_slot_utc"])>=epoch(old["scheduled_slot_utc"]):
            write_json(scheduled,result,replace=True)


def events_on(root, day):
    return [read_json(p) for p in (Path(root)/"events"/day).glob("*.json")]


def reconcile(config, now=None, limit=288):
    try:
        with process_lock(config.data_root,"reconcile.lock"):
            return _reconcile(config,now,limit)
    except BlockingIOError:
        return []


def _reconcile(config, now=None, limit=288):
    """Rebuild derived outcomes from verified commits and immutable events."""
    now = time.time() if now is None else now
    state = read_json(config.data_root / "state" / "schedule.json")
    if not state:
        return []
    cursor_path = config.data_root / "state" / "reconcile.json"
    cursor = read_json(cursor_path, {"next_slot":state["enabled_from_epoch"]})["next_slot"]
    lifecycle = lifecycle_events(config.data_root)
    out=[]
    # Advance the backlog first, then spend the remaining bounded budget checking
    # recent derived outcomes again. A cached success is never commit evidence.
    slots=[]
    next_slot=cursor
    while next_slot+270 < now and len(slots)<limit:
        slots.append(next_slot)
        next_slot+=300
    recent_end=min(cursor, math.ceil((now-270)/300)*300)
    slots += list(range(max(state["enabled_from_epoch"], recent_end-(limit-len(slots))*300),
                        recent_end, 300))
    for slot in slots:
        enabled=enabled_at(lifecycle,slot)
        day=utc(slot)[:10].replace("-","/")
        day_events=events_on(config.data_root,day)
        for currency in config.currencies:
            target=config.data_root/"state"/"outcomes"/str(int(slot))/(currency+".json")
            manifests=list((config.data_root/"snapshots"/day).glob(f"{slot_label(slot).replace(':','').replace('+','_')}/*/{currency}/manifest.json"))
            results=[];corrupt=False
            for path in manifests:
                try:
                    m,_=verify(path.parent)
                    if (m["scheduled_slot_utc"]!=slot_label(slot) or m["currency"]!=currency
                            or m["environment"]!=config.environment or m["job_kind"]!="snapshot"):
                        raise ValueError("slot identity mismatch")
                    results.append(m)
                except (ValueError,OSError,KeyError,TypeError,EOFError,zlib.error):
                    corrupt=True
            terms=[e for e in day_events if e.get("currency")==currency and e.get("scheduled_slot_utc")==slot_label(slot) and e["kind"]=="terminal"]
            statuses={m["status"] for m in results+terms}
            claimed=(config.data_root/"state"/"slots"/(str(int(slot))+".json")).exists()
            status=("corrupt" if corrupt else "conflict" if len(statuses)>1 or len(manifests)>1 else results[0]["status"] if results else terms[0]["status"] if terms else "interrupted" if claimed else "missing") if enabled else "paused"
            skips=[e for e in day_events if e.get("currency")==currency and e.get("scheduled_slot_utc")==slot_label(slot) and e["kind"]=="skip"]
            item=dict(scheduled_slot_utc=slot_label(slot),currency=currency,status=status,reason=(skips[-1]["reason"] if skips else "unknown") if status=="missing" else None)
            if read_json(target)!=item:
                write_json(target,item,replace=True);out.append(item)
    write_json(cursor_path,{"next_slot":next_slot},replace=True)
    return out


def status(config):
    now=time.time()
    recent=[]
    for slot in range(int(now//300)*300-86400,int(now//300)*300+1,300):
        for p in (config.data_root/"state"/"outcomes"/str(slot)).glob("*.json"):
            item=read_json(p)
            if item["status"] not in ("complete","paused"):recent.append(item)
    return dict(health=preflight(config), schedule=read_json(config.data_root/"state"/"schedule.json"),
                latest={p.stem:read_json(p) for p in (config.data_root/"state").glob("latest-*.json")},
                reconciliation=read_json(config.data_root/"state"/"reconcile.json"),gaps_last_24h=recent)


async def notify(config, key, success, deadline):
    if not config.notification_file or deadline-time.monotonic() <= .05:
        return False
    values=read_json(config.notification_file,{})
    url=values.get(key)
    if not url:
        return False
    parsed=urlparse(url)
    if parsed.scheme!="https" or parsed.hostname!="hc-ping.com" or parsed.query or parsed.fragment or parsed.username or parsed.password:
        raise ValueError("invalid Healthchecks ping URL")
    try:
        async with asyncio.timeout(min(5,deadline-time.monotonic())):
            async with aiohttp.ClientSession(trust_env=False) as client:
                async with client.get(url.rstrip("/")+("" if success else "/fail"),allow_redirects=False) as response:
                    return response.status==200
    except (TimeoutError,aiohttp.ClientError):
        return False


def export_archive(source, destination, include_incomplete=False):
    manifest,_=read_archive(source,include_incomplete=include_incomplete)
    dest=Path(destination)
    if dest.exists():
        raise ValueError("export destination must be new")
    mkdir(dest)
    for name in ("context.json","responses.jsonl.gz","manifest.json","truncated-tail.bin"):
        path=Path(source)/name
        if path.exists():
            write_bytes(dest/name,path.read_bytes())
    verify(dest)
    return manifest
