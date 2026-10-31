"""Bounded option membership and subscription evidence for live rediscovery.

This is a projection of already durable native metadata. Callers archive each
returned revision/control before sending its plan. No raw body is rewritten.
Only current members and pending subscriptions stay in memory; old revisions
belong to the append-only raw archive, not a growing in-memory history.
"""
from dataclasses import asdict, dataclass

from .contracts import canonical, digest, integer, text

DEADLINE_NS = 120_000_000_000


@dataclass(frozen=True)
class Option:
    venue: str
    symbol: str
    base: str
    incarnation: str
    state: str
    expiry_ms: int
    launch_ms: int | None
    created_ms: int | None
    native: dict

    def __post_init__(self):
        if self.venue not in {'deribit', 'bybit'} or self.base not in {'BTC', 'ETH'}:
            raise ValueError('selected native option identity required')
        text(self.symbol, 'option symbol');text(self.incarnation, 'listing incarnation')
        integer(self.expiry_ms, 'option expiry ms', 1)
        for name in ('launch_ms', 'created_ms'):
            if getattr(self, name) is not None:integer(getattr(self, name), name, 1)
        if not isinstance(self.native, dict):raise ValueError('native metadata required')

    @property
    def key(self):
        return self.venue, self.symbol, self.incarnation


def deribit_scope(universe):
    """Select inverse options by explicit native base/settlement metadata."""
    result=[];excluded=[]
    for row in universe['selected']:
        identity=row['instrument'];native=row['attributes']['native']
        if identity['native_product']!='option':continue
        base=native['base_currency'];settlement=native.get('settlement_currency')
        if not isinstance(settlement,str) or not settlement:
            raise ValueError('native option settlement currency required; never infer from symbol')
        if base not in {'BTC','ETH'} or settlement!=base or native.get('instrument_type')=='linear':
            excluded.append(dict(native_symbol=identity['native_symbol'],status='not_enabled',
                reason='nearly_all_liquidity_is_in_inverse_options'))
            continue
        result.append(Option('deribit',identity['native_symbol'],base,
            identity['listing_incarnation'],native.get('state','open'),
            native['expiration_timestamp'],None,native['creation_timestamp'],native))
    return tuple(result),tuple(excluded)


def deribit_options(universe):
    return deribit_scope(universe)[0]


def bybit_options(pages, *, base, max_instruments=10000):
    """Project one complete base-coin walk, including PreLaunch by default.

    pages is (request params, decoded native body) in acquisition order. Caller
    archives exact bodies and request references before using this projection.
    No status filter is allowed: the native default includes PreLaunch, Trading
    and Delivering. Partial/repeated-cursor walks cannot retire old members.
    """
    if base not in {'BTC','ETH'}:raise ValueError('selected option base required')
    integer(max_instruments,'option count',1)
    if not pages or len(pages)>100:raise ValueError('bounded complete discovery pages required')
    cursor='';seen=set();symbols=set();result=[]
    for index,(params,body) in enumerate(pages):
        expected=dict(category='option',baseCoin=base,limit=1000)
        if cursor:expected['cursor']=cursor
        if params!=expected:raise ValueError('complete unfiltered option walk required')
        if body.get('retCode')!=0:raise ValueError('successful option discovery required')
        value=body.get('result')
        if not isinstance(value,dict) or value.get('category')!='option' or not isinstance(value.get('list'),list):
            raise ValueError('native option page required')
        for native in value['list']:
            symbol=native.get('symbol');state=native.get('status')
            if (native.get('baseCoin')!=base or not isinstance(symbol,str) or symbol in symbols
                    or state not in {'PreLaunch','Trading','Delivering','Closed'}):
                raise ValueError('unambiguous option metadata required')
            symbols.add(symbol)
            def millis(name):
                raw=native.get(name)
                if not isinstance(raw,str) or not raw.isascii() or not raw.isdigit():
                    raise ValueError('native option time required: '+name)
                value=int(raw);integer(value,name,1);return value
            launch,expiry=millis('launchTime'),millis('deliveryTime')
            if expiry<=launch:raise ValueError('option expiry must follow launch')
            # Native symbolId is preferred where present. launchTime distinguishes
            # listing incarnations on older responses lacking that field.
            identity=native.get('symbolId')
            if identity is not None:integer(identity,'native symbol id',1)
            incarnation=f'launch:{launch}' if identity is None else f'id:{identity}:launch:{launch}'
            result.append(Option('bybit',symbol,base,incarnation,state,expiry,launch,None,native))
            if len(result)>max_instruments:raise ValueError('option count bound; no prefix selected')
        cursor=value.get('nextPageCursor','')
        if not isinstance(cursor,str) or len(cursor)>4096:raise ValueError('bounded native cursor required')
        if not cursor:
            if index!=len(pages)-1:raise ValueError('pages after completed discovery')
        else:
            if cursor in seen:raise ValueError('discovery cursor made no progress')
            seen.add(cursor)
        if index==len(pages)-1 and cursor:raise ValueError('incomplete discovery walk')
    if not result:raise ValueError('empty target discovery; retain previous universe as stale')
    return tuple(result)


def deribit_lifecycle(value):
    """Validate lifecycle payload; metadata lookup establishes product/base.

    Current schemas use open/delivered etc.; older observed release names
    created/started/closed/terminated are also retained without guessing expiry
    from settlement. A lifecycle notification alone is not full metadata.
    """
    if not isinstance(value,dict):raise ValueError('native lifecycle object required')
    symbol=value.get('instrument_name');state=value.get('state');stamp=value.get('timestamp')
    text(symbol,'native lifecycle symbol');integer(stamp,'lifecycle timestamp ms',1)
    if state not in {'open','settlement','delivered','inactive','locked','halted','archivized',
                     'created','started','closed','terminated'}:
        raise ValueError('unrecognized lifecycle state; retain last universe as stale')
    return dict(symbol=symbol,state=state,timestamp_ms=stamp,
        terminal=state in {'delivered','archivized','closed','terminated'},
        refresh=True)


class OptionUniverse:
    """Current desired membership; revisions and successful ACKs are separate.

    Missing contracts in a healthy complete refresh retire. Invalid or empty
    refreshes raise before mutation. Poll failures use stale() and preserve the
    previous list. There is no count cap on revisions or ACKs over time.
    """
    def __init__(self, venue, *, max_instruments=10000):
        if venue not in {'deribit','bybit'}:raise ValueError('option venue required')
        integer(max_instruments,'option bound',1)
        self.venue,self.max_instruments=venue,max_instruments
        self.members={};self.pending={};self.acknowledged=set();self.requests={}
        self.revision=0;self.revision_sha256=None;self.unhealthy=False

    def refresh(self, rows, *, source_ids, utc_ns, monotonic_ns, base=None, exclusions=()):
        integer(utc_ns,'observation UTC',1);integer(monotonic_ns,'observation monotonic',0)
        if base not in {None,'BTC','ETH'}:raise ValueError('selected refresh partition required')
        if not isinstance(source_ids,(list,tuple)) or not source_ids:
            raise ValueError('durable raw source IDs required')
        for source in source_ids:text(source,'source record ID')
        rows=tuple(rows)
        if not rows:raise ValueError('empty refresh; retain previous universe as stale')
        incoming={}
        for row in rows:
            if not isinstance(row,Option) or row.venue!=self.venue or (base and row.base!=base):
                raise ValueError('matching option refresh required')
            if row.symbol in incoming:raise ValueError('duplicate option symbol')
            incoming[row.symbol]=row
        # Validate all rows before removing expired ones; zero live rows can be
        # a valid expiry transition, whereas zero returned rows is uncertain.
        eligible={name:row for name,row in incoming.items() if row.expiry_ms*1_000_000>utc_ns
            and row.state not in {'Closed','delivered','archivized','closed','terminated'}}
        retained={name:row for name,row in self.members.items() if base and row.base!=base}
        proposed=dict(retained,**eligible)
        if len(proposed)>self.max_instruments:raise ValueError('option bound; no prefix selected')
        previous=self.members
        added=sorted(name for name,row in proposed.items() if name not in previous or previous[name].key!=row.key)
        retired=sorted(name for name,row in previous.items() if name not in proposed or proposed[name].key!=row.key)
        changed=sorted(name for name in proposed.keys()&previous.keys() if proposed[name]!=previous[name])
        body=dict(schema='dc.option-universe/1',venue=self.venue,revision=self.revision+1,
            parent_sha256=self.revision_sha256,source_record_ids=list(source_ids),
            observed_at_utc_ns=utc_ns,observed_at_monotonic_ns=monotonic_ns,
            partition=base,exclusions=list(exclusions),members=[asdict(proposed[name]) for name in sorted(proposed)],
            added=added,retired=retired,changed=changed,
            retirements=[dict(symbol=name,incarnation=previous[name].incarnation,
                retirement_utc_ns=utc_ns,retirement_monotonic_ns=monotonic_ns) for name in retired])
        encoded=canonical(body)
        for name in retired:
            self.pending.pop(name,None);self.acknowledged.discard(name);self.requests.pop(name,None)
        for name in added:
            self.pending[name]=dict(observed_utc_ns=utc_ns,observed_monotonic_ns=monotonic_ns,
                deadline_monotonic_ns=monotonic_ns+DEADLINE_NS)
        self.members=proposed;self.revision+=1;self.revision_sha256=digest(encoded);self.unhealthy=False
        return encoded

    def stale(self, reason):
        text(reason,'discovery failure');self.unhealthy=True
        return canonical(dict(schema='dc.option-discovery-health/1',venue=self.venue,
            revision_sha256=self.revision_sha256,stale=True,reason=reason))

    def reconnect(self, *, utc_ns, monotonic_ns):
        integer(utc_ns,'reconnect UTC',1);integer(monotonic_ns,'reconnect monotonic',0)
        self.acknowledged.clear();self.requests.clear()
        for name in self.members:
            # Existing pending deadlines survive reconnect; a disconnect cannot
            # erase evidence that a newly observed listing missed its deadline.
            self.pending.setdefault(name,dict(observed_utc_ns=utc_ns,
                observed_monotonic_ns=monotonic_ns,deadline_monotonic_ns=monotonic_ns+DEADLINE_NS))

    def requested(self, symbols, *, source_id, connection, utc_ns, monotonic_ns):
        """Bind a durable outbound request to the currently pending incarnation.

        Transport-specific code also checks exact requested channels and ACK
        contents. Reconnect clears requests; a late old-connection ACK cannot
        confirm the replacement connection.
        """
        text(source_id,'subscription request record');text(connection,'request connection')
        integer(utc_ns,'request UTC',1);integer(monotonic_ns,'request monotonic',0)
        symbols=tuple(symbols)
        if not symbols or len(set(symbols))!=len(symbols) or any(name not in self.pending for name in symbols):
            raise ValueError('request must bind pending option incarnations')
        for name in symbols:
            if monotonic_ns<self.pending[name]['observed_monotonic_ns']:
                raise ValueError('request precedes observation')
        for name in symbols:self.requests[name]=dict(request_record_id=source_id,connection=connection,
            request_utc_ns=utc_ns,request_monotonic_ns=monotonic_ns,incarnation=self.members[name].incarnation)

    def acknowledge(self, symbols, *, source_id, utc_ns, monotonic_ns, connection):
        integer(utc_ns,'ACK UTC',1);integer(monotonic_ns,'ACK monotonic',0);text(source_id,'durable ACK source')
        symbols=tuple(symbols)
        if not symbols or len(set(symbols))!=len(symbols) or any(name not in self.pending for name in symbols):
            raise ValueError('ACK must bind pending option subscriptions')
        rows=[]
        for name in symbols:
            pending=self.pending[name];option=self.members[name];request=self.requests.get(name)
            if request is None or request['connection']!=connection or request['incarnation']!=option.incarnation:
                raise ValueError('ACK has no matching current-connection request')
            if monotonic_ns<request['request_monotonic_ns']:raise ValueError('ACK precedes request')
            rows.append(dict(symbol=name,**request,
                observed_utc_ns=pending['observed_utc_ns'],observed_monotonic_ns=pending['observed_monotonic_ns'],
                observed_to_ack_ns=monotonic_ns-pending['observed_monotonic_ns'],
                creation_timestamp_ms=option.created_ms,
                creation_to_request_ns=None if option.created_ms is None else request['request_utc_ns']-option.created_ms*1_000_000,
                creation_to_ack_ns=None if option.created_ms is None else utc_ns-option.created_ms*1_000_000,
                observation_deadline_met=monotonic_ns<=pending['deadline_monotonic_ns'],
                creation_deadline_met=None if option.created_ms is None else utc_ns-option.created_ms*1_000_000<=DEADLINE_NS))
        for name in symbols:self.pending.pop(name);self.requests.pop(name);self.acknowledged.add(name)
        return canonical(dict(schema='dc.option-subscription-evidence/1',venue=self.venue,
            universe_sha256=self.revision_sha256,ack_record_id=source_id,
            ack_utc_ns=utc_ns,ack_monotonic_ns=monotonic_ns,options=rows))

    def overdue(self, monotonic_ns):
        integer(monotonic_ns,'deadline observation monotonic',0)
        return tuple(sorted(name for name,value in self.pending.items()
            if monotonic_ns>value['deadline_monotonic_ns']))
