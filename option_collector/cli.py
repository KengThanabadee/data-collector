import argparse
import asyncio
import json
import math
import os
from pathlib import Path
import signal
import subprocess
import sys
import time

from .archive import recover_all, verify
from .audit import audit
from .collector import collect
from .common import epoch, event, mkdir, process_lock, read_json, utc, write_json
from .config import bind_root, load, retain_source
from .delivery import chunk
from .operations import claim, export_archive, notify, preflight, probe_clock, reconcile, save_result, schedule, slot_label, status, terminal_events
from .transport import Client


def parser():
    p=argparse.ArgumentParser(prog="option-collector")
    p.add_argument("--config",default="config/local.toml")
    p.add_argument("--worker",action="store_true",help=argparse.SUPPRESS)
    p.add_argument("--hard-stop",type=float,help=argparse.SUPPRESS)
    sub=p.add_subparsers(dest="command",required=True)
    c=sub.add_parser("collect-once")
    c.add_argument("--scheduled-slot",help="timezone-qualified UTC slot, or auto (systemd)")
    d=sub.add_parser("delivery");d.add_argument("--due",action="store_true",required=True)
    for name in ("recover","status","monitor-once","pause","resume"):
        c=sub.add_parser(name)
        if name=="status":c.add_argument("--json",action="store_true")
    sub.add_parser("verify").add_argument("path")
    c=sub.add_parser("export");c.add_argument("source");c.add_argument("destination");c.add_argument("--include-incomplete",action="store_true")
    c=sub.add_parser("audit-soak");c.add_argument("--start",required=True);c.add_argument("--end",required=True)
    return p


def supervise(argv, seconds, *, command=None):
    """External monotonic wall-clock limit; includes TERM grace, then KILL."""
    if seconds<=0:return 5
    stop=time.monotonic()+seconds
    child=subprocess.Popen(command or [sys.executable,"-m","option_collector","--worker","--hard-stop",str(stop),*argv],start_new_session=True)
    old=signal.signal(signal.SIGTERM,lambda *_: os.killpg(child.pid,signal.SIGTERM) if child.poll() is None else None)
    try:
        try:
            code=child.wait(timeout=max(.01,stop-time.monotonic()-2))
            return 4 if code<0 else code
        except subprocess.TimeoutExpired:
            os.killpg(child.pid,signal.SIGTERM)
            try:
                code=child.wait(timeout=max(.01,stop-time.monotonic()))
                return 4 if code<0 else code
            except subprocess.TimeoutExpired:
                os.killpg(child.pid,signal.SIGKILL);child.wait();return 4
    finally:
        signal.signal(signal.SIGTERM,old)
        if child.poll() is None:
            os.killpg(child.pid,signal.SIGKILL);child.wait()


def result_code(results):
    statuses={r["status"] for r in results}
    if "interrupted" in statuses:return 4
    if "failed" in statuses:return 3
    if "partial" in statuses:return 2
    return 0


async def run_network(args,config,slot,deadline,hard_deadline):
    task=asyncio.current_task()
    loop=asyncio.get_running_loop()
    loop.add_signal_handler(signal.SIGTERM,task.cancel)
    async with Client(config) as client:
        if args.command=="collect-once":
            results=await collect(config,client,deadline,slot_label(slot) if slot is not None else None)
            for r in results:
                save_result(config,r)
                if slot is not None:
                    await notify(config,r["currency"],r["status"]=="complete",hard_deadline)
        else:
            results=await chunk(config,client,deadline)
            for r in results:save_result(config,r)
            if len(results)==len(config.currencies) and all(r["status"]=="complete" for r in results):
                sent=config.data_root/"state"/("delivery-notified-"+utc()[:10]+".json")
                if not sent.exists() and await notify(config,"delivery",True,hard_deadline):
                    write_json(sent,{"date":utc()[:10]})
    # Full manifests live on disk; logs contain compact operational summaries.
    print(json.dumps({"results":[{k:r[k] for k in ("job_kind","currency","status","destination","attempts","archive_bytes","capture_span_seconds")} | {"missing_jobs":len(r["missing_jobs"])} for r in results]},ensure_ascii=False))
    return result_code(results)


def delivery_budget(now):
    # Cut off the entire process (including TERM grace and finalization) ten
    # seconds before the next snapshot. The parent owns this absolute bound.
    return min(20, (math.floor(now/300)+1)*300-now-10)


def main(argv=None):
    argv=list(sys.argv[1:] if argv is None else argv)
    args=parser().parse_args(argv)
    try:
        config=load(args.config)
        now,mono=time.time(),time.monotonic()
        slot=None
        if args.command=="collect-once" and args.scheduled_slot:
            slot=math.floor(now/300)*300 if args.scheduled_slot=="auto" else epoch(args.scheduled_slot)
            if slot%300!=0 or slot>now:
                raise ValueError("scheduled slot must be an elapsed five-minute boundary")
            if args.scheduled_slot=="auto":
                argv=[slot_label(slot) if v=="auto" else v for v in argv]
        if not args.worker and args.command in ("collect-once","delivery"):
            seconds=(slot+config.hard_seconds-now if slot is not None else config.hard_seconds) if args.command=="collect-once" else delivery_budget(now)
            if args.command=="delivery" and seconds<=5:return 0
            return supervise(argv,seconds)
        mkdir(config.data_root)
        if args.command=="status":
            print(json.dumps(status(config)));return 0
        if args.command=="verify":
            m,_=verify(args.path);print(json.dumps(m));return 0
        if args.command=="audit-soak":
            report=audit(config,args.start,args.end);print(json.dumps(report));return 0 if report["data_gate_pass"] else 3
        if args.command=="monitor-once":
            # Monitoring must run while collection owns its network/archive lock.
            reconcile(config)
            probe_clock(config)
            health=preflight(config)
            asyncio.run(notify(config,"health",health["ok"] and not health["disk_warning"],time.monotonic()+5))
            if int(utc()[11:13])>=11:
                daily=read_json(config.data_root/"state"/("delivery-"+utc()[:10]+".json"),{})
                complete=len(daily)==len(config.currencies) and all(
                    read_json(config.data_root/info["destination"]/"manifest.json",{}).get("status")=="complete"
                    for info in daily.values())
                if not complete:asyncio.run(notify(config,"delivery",False,time.monotonic()+5))
            print(json.dumps(health));return 0 if health["ok"] else 3
        with process_lock(config.data_root):
            bind_root(config)
            if args.command=="pause" or args.command=="resume":
                print(json.dumps(schedule(config,args.command=="resume")));return 0
            if args.command=="export":
                print(json.dumps(export_archive(args.source,args.destination,args.include_incomplete)));return 0
            if args.command=="recover":
                results=recover_all(config.data_root)
                for r in results:save_result(config,r)
                reconcile(config)
                print(json.dumps({"recovered":results}));return result_code(results)
            if slot is not None:
                if now-slot>30:
                    terminal_events(config,slot,"late_start",status="skipped");return 5
                state=read_json(config.data_root/"state"/"schedule.json",{})
                if not state.get("enabled",False) or slot<state.get("effective_epoch",0):
                    terminal_events(config,slot,"schedule_paused",status="skipped");return 5
                try:claim(config,slot)
                except FileExistsError:
                    event(config.data_root,"duplicate",scheduled_slot_utc=slot_label(slot));return 5
            probe_clock(config)
            health=preflight(config)
            if not health["ok"]:
                if slot is not None:terminal_events(config,slot,",".join(health["reasons"]))
                print(json.dumps(health));return 3
            if args.command=="delivery":
                day_start=math.floor(now/86400)*86400
                if not day_start+9*3600<=now<day_start+11*3600:return 0
                safe=delivery_budget(now)
                if safe<=5:return 0
                hard=mono+safe
                deadline=hard-5
            else:
                hard=mono+(slot+config.hard_seconds-now if slot is not None else config.hard_seconds)
                deadline=mono+(slot+config.acquisition_seconds-now if slot is not None else config.acquisition_seconds)
            if args.hard_stop is not None:
                hard=min(hard,args.hard_stop)
                deadline=min(deadline,hard-5)
            # Recovery never emits historical success pings.
            for r in recover_all(config.data_root):save_result(config,r)
            retain_source(config)
            return asyncio.run(run_network(args,config,slot,deadline,hard))
    except asyncio.CancelledError:
        return 4
    except BlockingIOError:
        if 'config' in locals() and slot is not None:
            terminal_events(config,slot,"lock_busy",status="skipped")
        return 0 if args.command=="delivery" else 5
    except (ValueError,OSError,TypeError,KeyError) as exc:
        print(json.dumps({"error":type(exc).__name__,"message":"operation failed; inspect verified state and configuration"}),file=sys.stderr)
        return 4 if args.command in ("recover","verify") else 3
