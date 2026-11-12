"""Lossless native model state, not archive validity or a substitute for raw."""
from dataclasses import asdict,dataclass

from .books import BookLimits,DepthEvent,LevelChange,SnapshotBook,decimal_value
from .contracts import InstrumentKey,canonical,integer,sha256

SCHEMA='dc.snapshot-book-state/1'
_FIELDS={'book','profile','depth','epoch','state','reason','last_id','last_cross',
         'last_fingerprint','observed_id','observed_cross','anchor_source','last_source'}
_SCALARS=_FIELDS-{'book','profile','depth'}


@dataclass(frozen=True,slots=True)
class BookStateLimits:
    max_bytes: int = 16*1024*1024
    max_levels_per_side: int = 20000

    def __post_init__(self):
        for name,value in asdict(self).items():integer(value,name,1)


def snapshot_state(model,*,limits=BookStateLimits()):
    if not isinstance(model,SnapshotBook) or set(vars(model))!=_FIELDS:
        raise ValueError('supported complete snapshot-book model required')
    value=dict(schema=SCHEMA,instrument=asdict(model.book.instrument),profile=model.profile,depth=model.depth,
        signed_prices=model.book.signed_prices,max_levels_per_side=model.book.limits.max_levels_per_side,
        levels=model.book.levels(),**{name:getattr(model,name) for name in _SCALARS})
    # The same strict contract is used in both directions. In particular a
    # display view omits observed watermarks and cannot be restored as state.
    restore_snapshot_state(value,limits=limits)
    return value


def restore_snapshot_state(value,*,limits=BookStateLimits()):
    if not isinstance(limits,BookStateLimits):raise ValueError('typed native-state limits required')
    required={'schema','instrument','profile','depth','signed_prices','max_levels_per_side','levels'}|_SCALARS
    if type(value) is not dict or set(value)!=required or value['schema']!=SCHEMA:
        raise ValueError('unknown snapshot-book state shape/schema')
    if len(canonical(value))>limits.max_bytes:raise ValueError('snapshot-book state byte bound')
    cap=value['max_levels_per_side'];integer(cap,'stored level cap',1)
    if cap>limits.max_levels_per_side:raise ValueError('snapshot-book state level cap exceeds profile')
    if type(value['signed_prices']) is not bool:raise ValueError('explicit native price convention required')
    if type(value['instrument']) is not dict:raise ValueError('typed native instrument state required')
    try:instrument=InstrumentKey(**value['instrument'])
    except TypeError as exc:raise ValueError('native instrument state shape') from exc
    if not isinstance(value['profile'],str):raise ValueError('native profile name required')
    model=SnapshotBook(instrument,value['profile'],depth=value['depth'],limits=BookLimits(cap),signed_prices=value['signed_prices'])
    integer(value['epoch'],'native epoch',1)
    if not isinstance(value['state'],str) or value['state'] not in {'bootstrapping','live','invalid'}:raise ValueError('unknown native state')
    reason=value['reason']
    if reason is not None and (not isinstance(reason,str) or len(reason)>512):raise ValueError('bounded native reason required')
    if value['state']=='live' and reason is not None:raise ValueError('live native state cannot have failure reason')
    for name in ('last_id','last_cross','observed_id','observed_cross'):
        if value[name] is not None:integer(value[name],name)
    for name in ('last_fingerprint','anchor_source','last_source'):
        if value[name] is not None:sha256(value[name],name)
    applied=[value[name] is not None for name in ('last_id','last_fingerprint','anchor_source','last_source')]
    if any(applied)!=all(applied):raise ValueError('incomplete native anchor state')
    if not any(applied) and value['last_cross'] is not None:raise ValueError('cross watermark without applied anchor')
    if value['state']=='live' and not all(applied):raise ValueError('live native state requires applied anchor')
    if value['state']=='bootstrapping' and any(applied):raise ValueError('bootstrap cannot inherit applied anchor')
    for last,observed in (('last_id','observed_id'),('last_cross','observed_cross')):
        if value[last] is not None and (value[observed] is None or value[observed]<value[last]):
            raise ValueError('native observed watermark precedes applied state')
    sides=value['levels']
    if type(sides) is not dict or set(sides)!={'bids','asks'}:raise ValueError('native sides required')
    changes=[]
    for name in ('bids','asks'):
        side=sides[name]
        if type(side) not in (list,tuple) or len(side)>cap:raise ValueError('snapshot-book side level bound')
        rows=[]
        for row in side:
            if type(row) not in (list,tuple) or len(row)!=2:raise ValueError('native level pair required')
            if decimal_value(row[1])<=0:raise ValueError('stored native quantity must be positive')
            rows.append(LevelChange(row[0],row[1]))
        changes.append(tuple(rows))
    if any(changes):
        if not all(applied):raise ValueError('stored levels without applied anchor')
        model.book.apply(DepthEvent(instrument,value['last_source'],'snapshot',value['last_id'],*changes),replace=True)
    if canonical(model.book.levels())!=canonical(sides):raise ValueError('stored native levels must be canonical and ordered')
    for name in _SCALARS:setattr(model,name,value[name])
    return model
