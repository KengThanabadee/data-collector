"""Lossless native depth decoding from already archived application messages.

Unknown fields remain in RawRecord.body. Numeric book fields are never converted
through binary floats. Unverified CM/options bridges are intentionally rejected.
"""
import json
from decimal import Decimal

from .books import DepthEvent, LevelChange, decimal_value
from .bootstrap import BufferedDelta
from .contracts import RawRecord, integer


def _pairs(items):
    result = {}
    for name, value in items:
        if name in result:
            raise ValueError('duplicate native JSON key')
        result[name] = value
    return result


def _int(value):
    if len(value) > 128:
        raise ValueError('native integer exceeds numeric bound')
    return int(value)


def native_json(body, *, max_bytes=2*1024*1024):
    integer(max_bytes, 'max_bytes', 1)
    if type(body) is not bytes or len(body) > max_bytes:
        raise ValueError('native message byte bound')
    def reject(value):
        raise ValueError('nonfinite native JSON number')
    try:
        value = json.loads(body, parse_float=decimal_value, parse_int=_int,
                           parse_constant=reject, object_pairs_hook=_pairs)
    except (RecursionError, UnicodeError) as exc:
        raise ValueError('invalid native JSON nesting/encoding') from exc
    if not isinstance(value, dict):
        raise ValueError('native message must be an object')
    return value


def _number(value, *, numeric=False):
    if not isinstance(value, str):
        if not numeric or type(value) not in {int, Decimal}:
            raise ValueError('native decimal string required')
        value = str(value)
    decimal_value(value)
    return value


def _levels(value, *, deribit=False):
    if not isinstance(value, list) or len(value) > 40000:
        raise ValueError('bounded native level array required')
    result = []
    for row in value:
        if not isinstance(row, list) or len(row) != (3 if deribit else 2):
            raise ValueError('unknown native L2 tuple shape')
        if deribit:
            action, price, quantity = row
            if action not in {'new','change','delete'}:
                raise ValueError('unknown Deribit level action')
        else:
            action, (price, quantity) = 'set', row
        result.append(LevelChange(_number(price,numeric=deribit), _number(quantity,numeric=deribit), action))
    return tuple(result)


def _identity(record, profile):
    if not isinstance(record, RawRecord) or record.instrument is None:
        raise ValueError('archived record with explicit instrument incarnation required')
    profiles = {'deribit_full': ('deribit',None), 'bybit_standard': ('bybit',None),
                'bybit_full': ('bybit',{'spot','linear','inverse'}),
                'binance_spot': ('binance',{'spot'}), 'binance_um': ('binance',{'linear','um'})}
    if profile not in profiles:
        raise ValueError('profile lacks validated native schema/bridge')
    venue, products = profiles[profile]
    if record.venue != venue or (products is not None and record.native_product not in products):
        raise ValueError('native product/profile mismatch')
    return record.instrument


def decode_depth(record, profile, *, depth=None, max_bytes=2*1024*1024):
    instrument = _identity(record, profile)
    if record.kind != 'ws_message':
        raise ValueError('WebSocket application record required')
    value = native_json(record.body, max_bytes=max_bytes)
    symbol = instrument.native_symbol
    try:
        if profile == 'deribit_full':
            if value.get('jsonrpc') != '2.0' or value.get('method') != 'subscription':
                raise ValueError('Deribit subscription envelope required')
            params = value['params']; channel = params['channel']; data = params['data']
            if channel not in {f'book.{symbol}.100ms',f'book.{symbol}.agg2'} or channel != record.channel:
                raise ValueError('wrong Deribit channel or authenticated profile required')
            if data['instrument_name'] != symbol or data['type'] not in {'snapshot','change'}:
                raise ValueError('foreign Deribit instrument/type')
            integer(data['timestamp'], 'timestamp')
            kind = 'snapshot' if data['type'] == 'snapshot' else 'delta'
            return DepthEvent(instrument,record.record_id,kind,data['change_id'],
                _levels(data['bids'],deribit=True),_levels(data['asks'],deribit=True),
                data['prev_change_id'] if kind == 'delta' else None)
        if profile.startswith('bybit'):
            if profile == 'bybit_standard':
                integer(depth,'depth',1)
                allowed = {'option':{25,100},'spread':{25},'spot':{1,50,200,1000},
                           'linear':{1,50,200,1000},'inverse':{1,50,200,1000}}
                if depth not in allowed.get(instrument.native_product,set()):
                    raise ValueError('unsupported Bybit depth/product')
                topic = f'orderbook.{depth}.{symbol}'
            else:
                topic = f'orderbook.full.{symbol}'
            data = value['data']
            if value['topic'] != topic or record.channel != topic or data['s'] != symbol:
                raise ValueError('foreign Bybit native topic/symbol')
            if value['type'] not in {'snapshot','delta'} or (profile == 'bybit_full' and value['type'] != 'delta'):
                raise ValueError('unknown Bybit message type')
            if profile == 'bybit_standard' and depth == 1 and value['type'] != 'snapshot':
                raise ValueError('Bybit L1 is snapshot only')
            integer(value['ts'],'ts')
            event = DepthEvent(instrument,record.record_id,value['type'],data['u'],
                               _levels(data['b']),_levels(data['a']),cross_sequence=data['seq'])
            return BufferedDelta(event) if profile == 'bybit_full' else event
        topics = {symbol.lower()+'@depth',symbol.lower()+'@depth@100ms'}
        if record.channel not in topics:
            raise ValueError('foreign Binance channel')
        if 'stream' in value:
            if value['stream'] != record.channel:
                raise ValueError('foreign Binance combined stream')
            value = value['data']
        if value['e'] != 'depthUpdate' or value['s'] != symbol:
            raise ValueError('foreign Binance event/symbol')
        if 'st' in value and (profile != 'binance_um' or type(value['st']) is not int or value['st'] != 1):
            raise ValueError('merged native identity requires another gated profile')
        integer(value['E'],'E')
        event = DepthEvent(instrument,record.record_id,'delta',value['u'],_levels(value['b']),_levels(value['a']),
                           value['pu'] if profile == 'binance_um' else None)
        return BufferedDelta(event,value['U'])
    except (KeyError,TypeError,IndexError) as exc:
        raise ValueError('native depth schema mismatch') from exc


def decode_seed(record, profile, *, max_bytes=2*1024*1024):
    instrument = _identity(record, profile)
    if record.kind != 'rest_response':
        raise ValueError('archived REST response required')
    value = native_json(record.body,max_bytes=max_bytes)
    paths = {'binance_spot':'/api/v3/depth','binance_um':'/fapi/v1/depth',
             'bybit_full':'/v5/market/full_orderbook'}
    if profile not in paths or record.channel != paths[profile]:
        raise ValueError('wrong REST seed route/profile')
    try:
        if profile == 'bybit_full':
            fields = native_json(record.native_fields_json)
            if fields.get('request_category') != instrument.native_product or fields.get('request_symbol') != instrument.native_symbol:
                raise ValueError('Bybit seed request category/symbol evidence missing')
            if type(value['retCode']) is not int or value['retCode'] != 0:
                raise ValueError('unsuccessful Bybit REST response')
            data = value['result']
            if data['s'] != instrument.native_symbol:
                raise ValueError('foreign Bybit seed symbol')
            return DepthEvent(instrument,record.record_id,'snapshot',data['u'],
                              _levels(data['b']),_levels(data['a']),cross_sequence=data['seq'])
        if 'code' in value:
            raise ValueError('Binance error response cannot seed a book')
        # Binance seeds have no native symbol. Require the archived request identity
        # independently; never infer it from the caller's desired book.
        fields = native_json(record.native_fields_json)
        if fields.get('request_symbol') != instrument.native_symbol:
            raise ValueError('Binance seed request symbol evidence missing')
        return DepthEvent(instrument,record.record_id,'snapshot',value['lastUpdateId'],
                          _levels(value['bids']),_levels(value['asks']))
    except (KeyError,TypeError,IndexError) as exc:
        raise ValueError('native seed schema mismatch') from exc


def deribit_session_end(record):
    """Recognize only the receiver's saved terminal controls, never native data."""
    if record.kind!='control' or record.channel!='deribit_session' or record.venue!='deribit':return None
    value=native_json(record.body,max_bytes=4096)
    if record.instrument is not None:raise ValueError('Deribit session control cannot select one instrument')
    if value.get('event')=='socket_event':
        if set(value)!={'event','type'} or type(value['type']) is not int:
            raise ValueError('malformed Deribit socket control')
        # WebSocket CLOSE, aiohttp CLOSING/CLOSED/ERROR; PING/PONG are not loss.
        return 'deribit_socket_ended' if value['type'] in {8,256,257,258} else None
    if value.get('schema')=='dc.deribit-session/2' and value.get('event')=='stopped':
        if (set(value)!={'schema','event','reason','frames','bytes','corrections'} or
                not isinstance(value['reason'],str) or len(value['reason'])>256):
            raise ValueError('malformed Deribit session stop')
        for name in ('frames','bytes','corrections'):integer(value[name],name)
        return 'deribit_session_stopped'
    return None
