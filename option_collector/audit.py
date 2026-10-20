"""Data-side soak metrics. Operational fault evidence is a separate gate."""
from collections import Counter
import math
import zlib
from .archive import verify
from .common import epoch,utc
from .operations import pause_overlaps
from .schema import capture_span


def audit(config,start,end):
    start,end=epoch(start),epoch(end)
    if start%300 or end%300 or end-start<86400 or (end-start)%300:
        raise ValueError("audit window must cover >=24h aligned five-minute slots")
    expected=int((end-start)/300)
    markets={}
    paused=pause_overlaps(config.data_root,start,end)
    for currency in config.currencies:
        counts=Counter();spans=[];archive_bytes=body_bytes=0
        for slot in range(int(start),int(end),300):
            day=utc(slot)[:10].replace("-","/")
            label=utc(slot).replace(":","").replace("+","_")
            paths=list((config.data_root/"snapshots"/day/label).glob(f"*/{currency}/manifest.json"))
            if len(paths)!=1:
                counts["missing" if not paths else "conflict"]+=1;continue
            try:
                m,records=verify(paths[0].parent)
                if m["scheduled_slot_utc"]!=utc(slot) or m["currency"]!=currency or m["environment"]!=config.environment:
                    raise ValueError("slot identity mismatch")
                counts[m["status"]]+=1
                archive_bytes+=m["archive_bytes"]
                body_bytes+=sum(r["body_byte_count"] for r in records)
                if m["status"]=="complete":spans.append(capture_span(records, m["expected_jobs"]))
            except (ValueError,OSError,KeyError,TypeError,EOFError,zlib.error):counts["corrupt"]+=1
        spans.sort()
        p95=spans[math.ceil(.95*len(spans))-1] if spans else None
        markets[currency]=dict(expected=expected,statuses=dict(counts),complete_fraction=counts['complete']/expected,
                               p95_complete_span_seconds=p95,archive_bytes=archive_bytes,response_body_bytes=body_bytes)
    passed=not paused and all(m['complete_fraction']>=.99 and m['p95_complete_span_seconds'] is not None and m['p95_complete_span_seconds']<=240 and not any(m['statuses'].get(s,0) for s in ('corrupt','conflict')) for m in markets.values())
    return dict(start_utc=utc(start),end_utc=utc(end),intentional_pause=paused,markets=markets,data_gate_pass=passed,
                production_readiness_confirmed=False,remaining_evidence=['actual VPS/Linux host','reboot/timeout/disk/clock tests','Telegram received alert','network/temporary disk measurements'])
