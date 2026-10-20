"""Resumable multi-page chunks. Yield is distinct from a crashed run."""
from datetime import date
import time
import uuid

from .archive import Archive
from .collector import context
from .common import dumps, read_json, sha, utc, write_json
from .schema import payload
from .config import source_version


def rebuild_state(config,day):
    """Working journals and committed archives are authoritative, not the cursor."""
    state={}
    for p in (config.data_root/"delivery"/day.replace("-","/")).glob("*/*/manifest.json"):
        from .archive import verify
        m,_=verify(p.parent)
        if m["status"]=="complete" and m["currency"] in config.currencies:
            state[m["currency"]]=dict(destination=str(p.parent.relative_to(config.data_root)),work="work/committed/"+m["currency"])
    for p in (config.data_root/"work").glob("*/*/context.json"):
        c=read_json(p)
        if c["job_kind"]!="delivery" or c.get("job_date")!=day or c["currency"] not in config.currencies:
            continue
        a=Archive(config.data_root,{},existing=p.parent)
        if a.clean_yield():
            if c["currency"] in state:
                raise ValueError("conflicting delivery resume candidates")
            state[c["currency"]]=dict(destination=c["destination"],work=str(p.parent.relative_to(config.data_root)))
    return state


def progress(records):
    offset,total,dates=0,None,set()
    for rec in records:
        if not rec["ok"]:
            continue
        result=payload(rec)["result"]
        if rec["parameters"]["offset"]!=offset:
            raise ValueError("non-progressing/duplicate delivery offset")
        if total is not None and total!=result["records_total"]:
            raise ValueError("delivery total changed")
        total=result["records_total"]
        rows=result["data"]
        for row in rows:
            date.fromisoformat(row["date"])
            if row["date"] in dates:
                raise ValueError("duplicate delivery date")
            dates.add(row["date"])
        if not rows and offset<total:
            raise ValueError("empty delivery page before total")
        offset+=len(rows)
        if offset>total:
            raise ValueError("delivery over-count")
    return offset,total


async def chunk(config,client,deadline,day=None):
    day=day or utc()[:10]
    state_path=config.data_root/"state"/("delivery-"+day+".json")
    state=read_json(state_path)
    if state is None:
        state=rebuild_state(config,day)
    results=[]
    for currency in config.currencies:
        archive=None
        if currency in state:
            info=state[currency]
            dest=config.data_root/info["destination"]
            if (dest/"manifest.json").exists():
                from .archive import verify
                m,_=verify(dest)
                if m["status"]=="complete":
                    results.append(m);continue
            path=config.data_root/info["work"]
            if path.exists():
                old=Archive(config.data_root,{},existing=path)
                if old.clean_yield():
                    if (old.context["config_sha256"]!=sha(dumps(config.provenance()))
                            or old.context.get("code")!=source_version()
                            or old.context.get("schema_version")!=1
                            or old.context.get("capture_span_version")!=2):
                        # Preserve each attempt under the source/config that produced it.
                        old.checkpoint("in_progress")
                        old.finalize(interrupted=True)
                    else:
                        archive=old
                else:
                    old.finalize(interrupted=True)
        if time.monotonic()+.05>=deadline:
            break
        if archive is None:
            c=context(config,currency,"delivery",run_id=str(uuid.uuid4()))
            c.update(job_date=day,pagination_complete=False,
                     destination=f"delivery/{day.replace('-','/')}/{c['run_id']}/{currency}")
            archive=Archive(config.data_root,c)
            state[currency]=dict(work=str(archive.path.relative_to(config.data_root)),destination=archive.context["destination"])
            write_json(state_path,state,replace=True)
        archive.checkpoint("in_progress")
        complete=False
        try:
            offset,total=progress(archive.records)
            while time.monotonic()+.05<deadline:
                key="page:"+str(offset)
                required=archive.context["required_jobs"]
                if key not in required:
                    archive.update_context(required_jobs=required+[key])
                rec=await client.fetch(archive,key,"public/get_delivery_prices",dict(index_name=currency.lower()+"_usd",offset=offset,count=1000),deadline)
                if rec is None:
                    break
                if not rec["ok"]:
                    if len([r for r in archive.records if r["logical_request_id"]==key])>=config.attempts:
                        raise ValueError("delivery attempts exhausted")
                    break
                offset,total=progress(archive.records)
                archive.update_context(next_offset=offset,records_total=total)
                if offset==total:
                    archive.update_context(pagination_complete=True)
                    complete=True
                    break
        except ValueError as exc:
            archive.update_context(failure_reason=str(exc))
            results.append(archive.finalize())
            continue
        if complete:
            results.append(archive.finalize())
        else:
            archive.checkpoint("yielded")
        if time.monotonic()>=deadline:
            break
    return results
