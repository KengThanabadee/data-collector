import asyncio
from itertools import zip_longest
from pathlib import Path
import time
import uuid

from .archive import Archive
from .common import dumps, sha, utc
from .config import source_version
from .schema import payload, universe


def context(config, currency, kind, slot=None, run_id=None):
    run_id = run_id or str(uuid.uuid4())
    now = utc()
    day = (slot or now)[:10].replace("-", "/")
    label = (slot or "manual").replace(":", "").replace("+", "_")
    destination = f"snapshots/{day}/{label}/{run_id}/{currency}" if kind=="snapshot" else f"{kind}/{day}/{run_id}/{currency}"
    public = config.provenance()
    return dict(schema_version=1, capture_span_version=2, run_id=run_id, currency=currency, job_kind=kind,
                environment=config.environment, scheduled_slot_utc=slot, started_utc=now,
                destination=destination, code=source_version(), config=public, config_sha256=sha(dumps(public)),
                required_jobs=[], universe=[], universe_trusted=False)


async def collect(config, client, deadline, slot=None):
    run = str(uuid.uuid4())
    archives = {c: Archive(config.data_root, context(config,c,"snapshot",slot,run)) for c in config.currencies}
    async def references(currency, archive):
        required = ["metadata:option", "metadata:future", "summary:option", "summary:future", "index"]
        archive.update_context(required_jobs=required)
        for kind in ("option", "future"):
            rec = await client.fetch(archive,"metadata:"+kind,"public/get_instruments",
                                    dict(currency=currency,kind=kind,expired=False,extended=True),deadline)
            if kind == "option" and rec and rec["ok"]:
                names, excluded = universe(payload(rec)["result"], currency)
                archive.update_context(universe=names, exclusions=excluded, universe_trusted=True)
        for kind in ("option", "future"):
            await client.fetch(archive,"summary:"+kind,"public/get_book_summary_by_currency",dict(currency=currency,kind=kind),deadline)
        await client.fetch(archive,"index","public/get_index_price",dict(index_name=currency.lower()+"_usd"),deadline)

    try:
        async with asyncio.timeout_at(deadline):
            await asyncio.gather(*(references(c,a) for c,a in archives.items()))
            lists = []
            for currency, archive in archives.items():
                names = archive.context["universe"]
                shift = int((time.time() if slot is None else __import__("datetime").datetime.fromisoformat(slot).timestamp())//300) % max(1,len(names))
                names = names[shift:]+names[:shift]
                lists.append([(currency,n) for n in names])
            jobs = [x for pair in zip_longest(*lists) for x in pair if x is not None]
            queue = asyncio.Queue()
            for job in jobs:
                queue.put_nowait(job)
            async def worker():
                while not queue.empty():
                    currency, name = queue.get_nowait()
                    await client.fetch(archives[currency],"book:"+name,"public/get_order_book",dict(instrument_name=name,depth=config.depth),deadline)
            await asyncio.gather(*(worker() for _ in range(config.workers)))
    except TimeoutError:
        pass
    except asyncio.CancelledError:
        # SIGTERM drains/cancels requests first. Durable prefix is still published.
        pass
    return [a.finalize() for a in archives.values()]
