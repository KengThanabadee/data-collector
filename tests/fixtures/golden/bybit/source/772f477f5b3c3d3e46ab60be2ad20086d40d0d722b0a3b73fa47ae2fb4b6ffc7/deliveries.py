"""Canonical publisher reports in raw: claims, not independent snapshot proof.

Reports retain exact publisher receipt bytes and separate archival receipt time.
Replay validates report structure/order, not the referenced snapshot artifact.
"""
from dataclasses import asdict, dataclass

from .contracts import VENUES, canonical, digest, integer, sha256, strict_json, text

DELIVERY_NS = 5_000_000_000
MAX_REPORT_BYTES = 65536
REASONS = frozenset({'on_time','input_timing_invalid','process_clock_invalid',
                     'publication_deadline_missed','clock_alignment_unavailable','publication_failed'})
ORIGIN_FIELDS = {'venue','environment','capture_id','process_session_id','capture_scope','process_timing_epoch'}
BASE_FIELDS = {'schema','origin','close_record_id','close_sha256','cutoff_id','cutoff_deadline_ns',
               'publication_deadline_ns','input_clock_pin','input_segments','plan_sha256'}


def delivery_base(inputs, plan=None):
    result=dict(schema='dc.snapshot-delivery/2',origin=asdict(inputs.origin),close_record_id=inputs.close_record_id,
        close_sha256=inputs.close_sha256,cutoff_id=inputs.cutoff_id,cutoff_deadline_ns=inputs.deadline_ns,
        publication_deadline_ns=inputs.deadline_ns+DELIVERY_NS,input_clock_pin=asdict(inputs.clock_pin),
        input_segments=[dict(context_sha256=s.context_sha256,manifest_sha256=s.manifest_sha256) for s in inputs.segments])
    if plan is not None:result['plan_sha256']=digest(canonical(asdict(plan)))
    return result


def _receipt_fields(saved, origin):
    if (type(saved['reason']) is not str or type(saved['outcome']) is not str or
            saved['reason'] not in REASONS or saved['outcome'] not in {'published','failed'}):
        raise ValueError('delivery reason/outcome')
    if (saved['outcome']=='published')!=(saved['reason']=='on_time'):raise ValueError('delivery reason conflicts with outcome')
    if saved['snapshot_id'] is not None:sha256(saved['snapshot_id'],'delivery snapshot')
    if saved['outcome']=='published' and saved['snapshot_id'] is None:raise ValueError('published snapshot missing')
    error=saved['error_type']
    if error is not None:
        text(error,'delivery error type')
        if len(error)>128:raise ValueError('delivery error type bound')
    if (error is not None)!=(saved['reason']=='publication_failed'):raise ValueError('delivery error/reason mismatch')
    for name in ('started','completed'):
        observation=saved[name]
        if (not isinstance(observation,dict) or set(observation)!={'utc_ns','monotonic_ns','process_timing_epoch','valid'} or
                type(observation['valid']) is not bool):raise ValueError('delivery observation shape')
        values=[observation[k] for k in ('utc_ns','monotonic_ns','process_timing_epoch')]
        if any(v is None for v in values):
            if observation['valid'] or any(v is not None for v in values):raise ValueError('delivery missing clock observation')
            continue
        for field in ('utc_ns','monotonic_ns'):
            integer(observation[field],'delivery '+field)
            if observation[field]>2**63-1:raise ValueError('delivery observation range')
        text(observation['process_timing_epoch'],'delivery process epoch')
        if len(canonical(observation['process_timing_epoch']))>130:raise ValueError('delivery process epoch bound')
        if observation['valid'] and observation['process_timing_epoch']!=origin['process_timing_epoch']:
            raise ValueError('delivery clock origin changed')


def validate_delivery(body, *, max_bytes=MAX_REPORT_BYTES):
    if type(body) is not bytes or len(body)>max_bytes:raise ValueError('delivery byte bound')
    value=strict_json(body)
    if (not isinstance(value,dict) or set(value)!=BASE_FIELDS|{'started','completed','outcome','reason','snapshot_id','error_type'} or
            value['schema']!='dc.snapshot-delivery/2' or canonical(value)!=body):
        raise ValueError('canonical delivery schema required')
    origin=value['origin']
    if not isinstance(origin,dict) or set(origin)!=ORIGIN_FIELDS:raise ValueError('delivery origin shape')
    for name,item in origin.items():
        text(item,name)
        if len(canonical(item))>130:raise ValueError('delivery origin byte bound')
    if origin['venue'] not in VENUES:raise ValueError('delivery origin venue')
    for name in ('close_record_id','close_sha256','plan_sha256'):sha256(value[name],name)
    for name in ('cutoff_id','cutoff_deadline_ns','publication_deadline_ns'):
        integer(value[name],name)
        if value[name]>2**63-1:raise ValueError('delivery integer bound')
    if value['cutoff_id']>=10000 or value['publication_deadline_ns']!=value['cutoff_deadline_ns']+DELIVERY_NS:
        raise ValueError('delivery deadline/cutoff contract')
    pin=value['input_clock_pin']
    if not isinstance(pin,dict) or set(pin)!={'count','head_sha256','last_record_id'}:raise ValueError('delivery clock pin shape')
    integer(pin['count'],'delivery clock count',1)
    if pin['count']>2**63-1:raise ValueError('delivery clock count bound')
    sha256(pin['head_sha256'],'delivery clock head');sha256(pin['last_record_id'],'delivery clock record')
    segments=value['input_segments']
    if type(segments) is not list or not 1<=len(segments)<=1024:raise ValueError('delivery segment bound')
    contexts=set()
    for segment in segments:
        if not isinstance(segment,dict) or set(segment)!={'context_sha256','manifest_sha256'}:raise ValueError('delivery segment shape')
        for name,item in segment.items():sha256(item,name)
        if segment['context_sha256'] in contexts:raise ValueError('duplicate delivery segment')
        contexts.add(segment['context_sha256'])
    _receipt_fields(value,origin)
    if value['outcome']=='published':
        start,end=value['started'],value['completed']
        if (not start['valid'] or not end['valid'] or
                not value['cutoff_deadline_ns']<=start['monotonic_ns']<=end['monotonic_ns']<=value['publication_deadline_ns']):
            raise ValueError('published delivery timing contract')
    return value


@dataclass(frozen=True, slots=True)
class DeliveryReport:
    close_record_id: str
    receipt_sha256: str
    record_id: str
    raw_sha256: str
    outcome: str
    reason: str


def replay_delivery_reports(records, *, origin, max_records=200000, max_scan_bytes=256*1024*1024,
                            max_reports=10000, max_report_bytes=MAX_REPORT_BYTES):
    """Verify bounded raw report order; referenced snapshot proof stays separate."""
    for name,value in (('records',max_records),('scan bytes',max_scan_bytes),('reports',max_reports),('report bytes',max_report_bytes)):
        integer(value,name,1)
    expected=asdict(origin);connection='control:snapshot-deliveries:'+origin.capture_scope
    channel='snapshot_deliveries:'+origin.capture_scope
    reports=[];seen=set();size=0;last_mono=None
    for index,record in enumerate(records):
        if index>=max_records:raise ValueError('delivery replay record bound')
        size+=len(record.to_bytes())
        if size>max_scan_bytes:raise ValueError('delivery replay scan bound')
        if record.receipt.connection_id!=connection and record.channel!=channel:continue
        if (record.receipt.connection_id!=connection or record.channel!=channel or record.kind!='control' or
                record.native_product!='control' or record.instrument is not None or record.native_fields_json!=b'{}' or
                (record.venue,record.environment,record.receipt.capture_id,record.receipt.process_session_id)!=(
                    origin.venue,origin.environment,origin.capture_id,origin.process_session_id) or
                record.receipt.receive_seq!=len(reports)):
            raise ValueError('delivery raw namespace/order differs')
        if len(reports)>=max_reports:raise ValueError('delivery replay report bound')
        if (max(record.receipt.receive_seq,record.receipt.monotonic_ns,record.receipt.receive_utc_ns)>2**63-1 or
                (last_mono is not None and record.receipt.monotonic_ns<last_mono)):
            raise ValueError('delivery archive receipt clock/order bound')
        value=validate_delivery(record.body,max_bytes=max_report_bytes)
        if value['origin']!=expected or value['close_record_id'] in seen:raise ValueError('delivery origin/close conflict')
        if value['completed']['valid'] and record.receipt.monotonic_ns<value['completed']['monotonic_ns']:
            raise ValueError('delivery archived before reported completion')
        seen.add(value['close_record_id'])
        last_mono=record.receipt.monotonic_ns
        reports.append(DeliveryReport(value['close_record_id'],digest(record.body),record.record_id,
            digest(record.to_bytes()),value['outcome'],value['reason']))
    return tuple(reports)
