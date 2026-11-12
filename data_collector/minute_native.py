"""Lossless native JSON and the Deribit instrument request for the minute ticker sampler."""
import json

from .books import decimal_value
from .contracts import integer

METHOD = 'public/get_instruments'
PARAMS = dict(currency='any', kind='any', expired='false', extended='true')


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
