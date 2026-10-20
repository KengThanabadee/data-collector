"""Explicitly synthetic fixtures; never exchange observations."""
import json
import time
from option_collector.common import body_fields, utc


def record(archive,key,result,params=None,method="public/get_index_price",ok=True,raw=None,at=None):
    at=time.time() if at is None else at
    body=raw if raw is not None else json.dumps({"jsonrpc":"2.0","result":result}).encode()
    r={k:archive.context[k] for k in ("schema_version","run_id","job_kind","environment","currency","scheduled_slot_utc")}
    r.update(logical_request_id=key,attempt=1,method=method,parameters=params or {},ok=ok,
             request_started_epoch_ms=at*1000,response_received_epoch_ms=(at+.1)*1000,
             request_started_utc=utc(at),response_received_utc=utc(at+.1),quality_flags=[],body_complete=True,
             http_status=200,**body_fields(body))
    return archive.append(r)


def instrument(currency="BTC"):
    return dict(instrument_name=currency+"-SYNTHETIC-C",kind="option",settlement_currency=currency,
                state="open",expiration_timestamp=1900000000000,strike=100000,option_type="call")
