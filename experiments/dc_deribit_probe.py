"""Bounded public BTC/ETH native-book evidence, not the continuous v2 runtime.

Run from repository root with .venv/bin/python -m experiments.dc_deribit_probe.
No credentials, trading, background service or automatic retries. Exact raw
responses and application messages are sealed using the v2 archive contract.
"""
import asyncio
from dataclasses import replace
from datetime import datetime, timezone
from decimal import localcontext
import json
from pathlib import Path
import time
import uuid

import aiohttp

from data_collector.books import SnapshotBook, decimal_value
from data_collector.contracts import InstrumentKey, Provenance, RawRecord, Receipt, canonical, digest
from data_collector.decoders import decode_depth, native_json
from data_collector.journal import Journal, JournalLimits, _publish_bytes, sync_directory
from data_collector.segments import SegmentContext, iter_segment_records, seal_segment, verify_segment

HTTP = 'https://www.deribit.com/api/v2/public/get_instruments'
INDEX_HTTP = 'https://www.deribit.com/api/v2/public/get_index_price'
WS = 'wss://www.deribit.com/ws/api/v2'
MAX_RESPONSE = 4*1024*1024
MAX_WS_MESSAGE = 2*1024*1024
MAX_TOTAL = 20*1024*1024
MAX_FRAMES = 200
DURATION = 90


def choose_instrument(value, currency, now_ms, index_price=None):
    if not isinstance(value, dict) or 'error' in value or not isinstance(value.get('result'),list):
        raise ValueError('unsuccessful instrument discovery')
    candidates = [x for x in value['result'] if x.get('kind') == 'option' and x.get('is_active') is True
                  and x.get('base_currency') == currency and type(x.get('expiration_timestamp')) is int
                  and x['expiration_timestamp'] > now_ms and type(x.get('creation_timestamp')) is int
                  and isinstance(x.get('instrument_name'),str)]
    if not candidates:
        raise ValueError('no eligible currently listed native option')
    reference=decimal_value(str(index_price))
    if reference <= 0:raise ValueError('positive native index reference required')
    # Near-index selection is an experiment design, not a liquidity claim.
    # Actual advancing deltas are still required by the evidence gate.
    calls = [x for x in candidates if x.get('option_type') == 'call']
    if not calls:raise ValueError('no eligible calls for declared selection profile')
    with localcontext() as context:
        context.prec=512
        return min(calls,key=lambda x:(x['expiration_timestamp'],abs(decimal_value(str(x['strike']))-reference),x['instrument_name']))


async def bounded_response(response):
    parts, size = [], 0
    async for chunk in response.content.iter_chunked(65536):
        size += len(chunk)
        if size > MAX_RESPONSE:
            raise ValueError('REST response byte cap')
        parts.append(chunk)
    return b''.join(parts)


def replay_books(segment, keys):
    states={symbol:SnapshotBook(key,'deribit_full') for symbol,key in keys.items()}
    for record in iter_segment_records(segment):
        if record.kind != 'ws_message':continue
        value=native_json(record.body,max_bytes=MAX_WS_MESSAGE)
        context=subscription_context(value)
        if context is None:continue
        symbol,channel=context
        if symbol in states:
            states[symbol].receive(decode_depth(replace(record,instrument=keys[symbol],channel=channel),'deribit_full'))
    return {symbol:state.view() for symbol,state in states.items()}


def subscription_context(value):
    if value.get('method') != 'subscription':return None
    params=value.get('params')
    if not isinstance(params,dict) or not isinstance(params.get('data'),dict):
        raise ValueError('malformed native subscription params/data')
    symbol=params['data'].get('instrument_name');channel=params.get('channel')
    if not isinstance(symbol,str) or not symbol or not isinstance(channel,str) or not channel:
        raise ValueError('malformed native subscription instrument/channel')
    return symbol,channel


async def run(output_root=Path('data/v2-probes')):
    identity = uuid.uuid4().hex
    capture, process = 'probe-'+identity, 'process-'+identity
    root = Path(output_root)/identity
    root.mkdir(parents=True,exist_ok=False)
    sync_directory(root.parent.parent);sync_directory(root.parent)
    config = dict(mode='public_native_book_probe',currencies=['BTC','ETH'],duration_seconds=DURATION,
                  max_frames=MAX_FRAMES,max_total_bytes=MAX_TOTAL,max_response_bytes=MAX_RESPONSE,
                  max_ws_message_bytes=MAX_WS_MESSAGE,http=HTTP,ws=WS)
    config.update(index_http=INDEX_HTTP,max_rest_requests=4,selection='nearest_expiry_call_nearest_native_index')
    config['overflow_reserve_bytes']=MAX_WS_MESSAGE
    config_hash = digest(canonical(config))
    source = Path('experiments/results/2026-10-23-dc-v2-docs-public/D1.body')
    source_body = source.read_bytes()
    expected = json.loads(Path('docs/v2/source-evidence.json').read_text())['sources'][0]['sha256']
    if digest(source_body) != expected:
        raise ValueError('frozen Deribit book source body is unavailable or changed')
    source_hash = digest(source_body)
    discovery, selected = [], []
    total = 0
    timeout = aiohttp.ClientTimeout(total=15)
    async with aiohttp.ClientSession(timeout=timeout,trust_env=False) as session:
        async def fetch(currency, endpoint, params, label):
            nonlocal total
            requested = time.time_ns()
            async with session.get(endpoint,params=params,allow_redirects=False) as response:
                body=await bounded_response(response)
                utc,mono=time.time_ns(),time.monotonic_ns()
                status=response.status
            # Completed responses survive all later validation/discovery errors.
            _publish_bytes(root/f'{label}-{currency}.body',body)
            _publish_bytes(root/f'{label}-{currency}.json',canonical(dict(currency=currency,
                endpoint=endpoint,params=params,http_status=status,requested_utc_ns=requested,
                received_utc_ns=utc,received_monotonic_ns=mono,body_sha256=digest(body))))
            total += len(body)
            discovery.append((currency+':'+label,body,utc,mono,status,requested,params,endpoint))
            if status != 200:
                (root/'failed-discovery.json').write_bytes(canonical(dict(status=status,currency=currency,body_sha256=digest(body))))
                (root/'failed-discovery.body').write_bytes(body)
                raise ValueError(f'Deribit public discovery HTTP {status}; no retry')
            await asyncio.sleep(1.1)
            return native_json(body,max_bytes=MAX_RESPONSE),utc
        references={}
        for currency in ('BTC','ETH'):
            instruments,utc=await fetch(currency,HTTP,dict(currency=currency,kind='option',expired='false'),'discovery')
            index,index_utc=await fetch(currency,INDEX_HTTP,dict(index_name=currency.lower()+'_usd'),'index')
            if 'error' in index or not isinstance(index.get('result'),dict) or 'index_price' not in index['result']:
                raise ValueError('unsuccessful index price response')
            price=decimal_value(str(index['result']['index_price']))
            selected.append(choose_instrument(instruments,currency,max(utc,index_utc)//1_000_000,price))
            references[currency]=str(price)
        universe = [dict(instrument_name=x['instrument_name'],creation_timestamp=x['creation_timestamp'],
                         expiration_timestamp=x['expiration_timestamp'],base_currency=x['base_currency'],
                         selected_strike=str(x['strike']),selection_index_price=references[x['base_currency']]) for x in selected]
        universe_hash=digest(canonical(universe))
        provenance=Provenance(source_hash,config_hash,universe_hash)
        keys={x['instrument_name']:InstrumentKey('deribit','production','option',x['instrument_name'],
              'creation:'+str(x['creation_timestamp'])) for x in selected}
        states={symbol:SnapshotBook(key,'deribit_full') for symbol,key in keys.items()}
        counts={symbol:dict(snapshots=0,deltas=0,applied=0,applied_snapshots=0,applied_deltas=0,invalid=0) for symbol in keys}
        frames, sequence, reason = 0, 0, 'duration_bound'
        ack_channels = []
        capture_schema_ok = True
        def record(connection,seq,utc,mono,channel,kind,body,*,instrument=None,fields=None):
            return RawRecord(Receipt(capture,process,connection,seq,utc,mono,'probe-clock'),
                'deribit','production','option',channel,kind,body,provenance,instrument,
                canonical(fields or {}))
        with Journal(root/'capture.wal',venue='deribit',environment='production',create=True,
                     limits=JournalLimits(target_bytes=32*1024*1024,max_record_bytes=10*1024*1024)) as wal:
            wal.append(record('source:D1',0,time.time_ns(),time.monotonic_ns(),'source_bundle:D1','metadata_version',
                source_body,fields=dict(sha256=source_hash,description='Frozen public Deribit book schema source')))
            wal.append(record('probe:manifest',0,time.time_ns(),time.monotonic_ns(),'probe_manifest','control',
                              canonical(dict(config=config,universe=universe))))
            for connection,body,utc,mono,status,requested,params,endpoint in discovery:
                wal.append(record('rest:'+connection,0,utc,mono,'public/'+endpoint.rsplit('/',1)[-1],'rest_response',body,
                    fields=dict(requested_utc_ns=requested,params=params,http_status=status,endpoint=endpoint)))
            try:
                async with asyncio.timeout(DURATION):
                    async with session.ws_connect(WS,max_msg_size=MAX_WS_MESSAGE,autoping=True,compress=0) as socket:
                        channels=[f'book.{symbol}.100ms' for symbol in keys]
                        request=canonical(dict(jsonrpc='2.0',id=1,method='public/subscribe',params=dict(channels=channels)))
                        wal.append(record('ws:books',sequence,time.time_ns(),time.monotonic_ns(),
                                          'public/subscribe','control',request,fields=dict(direction='outbound',endpoint=WS)))
                        sequence += 1
                        await socket.send_str(request.decode())
                        while frames < MAX_FRAMES:
                            message=await socket.receive()
                            utc,mono=time.time_ns(),time.monotonic_ns()
                            if message.type not in {aiohttp.WSMsgType.TEXT,aiohttp.WSMsgType.BINARY}:
                                reason='websocket_closed_or_error'
                                break
                            body=message.data.encode('utf-8') if message.type == aiohttp.WSMsgType.TEXT else message.data
                            total += len(body);frames += 1
                            crossed_bound=total>MAX_TOTAL
                            # Archive first, including ACK/error/unknown notifications.
                            archived=record('ws:books',sequence,utc,mono,'multiplex','ws_message',body,fields=dict(endpoint=WS))
                            wal.append(archived);sequence+=1
                            value=native_json(body,max_bytes=MAX_WS_MESSAGE)
                            if 'error' in value:
                                reason='subscription_error';capture_schema_ok=False;break
                            if value.get('id') == 1 and isinstance(value.get('result'),list):
                                if any(not isinstance(channel,str) for channel in value['result']):
                                    raise ValueError('malformed ACK channel array')
                                ack_channels=value['result']
                            context=subscription_context(value)
                            if context is not None:
                                symbol,channel=context
                                if symbol in states:
                                    # Binding is derived context; archived envelope stays unchanged.
                                    event=decode_depth(replace(archived,instrument=keys[symbol],channel=channel),'deribit_full')
                                    counts[symbol]['snapshots' if event.kind=='snapshot' else 'deltas']+=1
                                    outcome=states[symbol].receive(event)
                                    if outcome in {'applied','invalid'}:counts[symbol][outcome]+=1
                                    if outcome=='applied':
                                        counts[symbol]['applied_snapshots' if event.kind=='snapshot' else 'applied_deltas']+=1
                            if crossed_bound:
                                reason='total_byte_bound';break
                        else:
                            reason='frame_bound'
            except TimeoutError:
                reason='duration_bound'
            except ValueError as exc:
                capture_schema_ok=False
                reason=type(exc).__name__+': '+str(exc)[:300]
            except (aiohttp.ClientError,OSError) as exc:
                reason=type(exc).__name__+': '+str(exc)[:300]
            wal.append(record('ws:books',sequence,time.time_ns(),time.monotonic_ns(),'probe_stop','control',
                              canonical(dict(reason=reason,frames=frames,payload_bytes=total))))
            context=SegmentContext('deribit','production',capture,process,'probe-segment',source_hash,config_hash)
            manifest=seal_segment(wal,root/'archive',context)
        verified=verify_segment(manifest)
    captured_books={s:state.view() for s,state in states.items()}
    replay_error=None
    try:
        replayed_books=replay_books(manifest,keys)
        replay_equal=canonical(replayed_books)==canonical(captured_books)
    except (ValueError,TypeError,KeyError) as exc:
        replay_error=type(exc).__name__+': '+str(exc)[:300]
        replay_equal=False
    report=dict(schema='dc.probe-evidence/2',created_utc=datetime.now(timezone.utc).isoformat(),config=config,
        root=str(root.resolve()),manifest=str(Path(manifest).resolve()),source_sha256=source_hash,
        capture_id=capture,universe=universe,stop_reason=reason,frames=frames,payload_bytes=total,
        counts=counts,books=captured_books,archive_verified=bool(verified),
        book_replay_equal=replay_equal,book_replay_error=replay_error,book_payload_sha256=digest(canonical(captured_books)),
        subscription_ack_channels=ack_channels,
        capture_schema_ok=capture_schema_ok,
        initial_native_book_evidence=capture_schema_ok and replay_equal and set(ack_channels)=={f'book.{s}.100ms' for s in keys} and all(
            c['applied_snapshots']>=1 and c['applied_deltas']>=2 and c['invalid']==0 for c in counts.values()),
        limitations=['Bounded Deribit BTC/ETH option sample only','No REST reconciliation, reconnect or capacity/soak proof',
                     'No shared-time snapshot, historical quote sufficiency or production readiness claim'])
    (root/'report.json').write_bytes(canonical(report))
    return report


if __name__=='__main__':
    print(json.dumps(asyncio.run(run()),indent=2))
