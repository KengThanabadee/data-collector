"""Immutable local snapshot publication, verified against committed raw inputs.

Serialized publisher API. The supplied segment set defines the durable knowledge
available at publication start; live capture must coordinate its WAL/segment seal
with this call. This module never treats a mutable WAL or preview as committed.
"""
from dataclasses import asdict, dataclass
from pathlib import Path
import tempfile

from .contracts import InstrumentKey, StreamKey, canonical, digest, integer, sha256, strict_json
from .journal import Journal, _publish_bytes
from .ledger import DurableLedger
from .replay import BookBinding, ReplayLimits, SnapshotPlan, _materialize_snapshot
from .segments import _mkdir, iter_segment_records
from .validity import FeedTarget, LedgerEntry, LedgerPin, ValidityLedger


@dataclass(frozen=True)
class PublicationLimits:
    replay: ReplayLimits = ReplayLimits()
    max_descriptor_bytes: int = 16*1024*1024
    max_manifest_bytes: int = 65536

    def __post_init__(self):
        if not isinstance(self.replay,ReplayLimits):raise ValueError('typed replay limits required')
        integer(self.max_descriptor_bytes,'max_descriptor_bytes',1)
        integer(self.max_manifest_bytes,'max_manifest_bytes',1)


def _paths(segments,limits):
    paths=[]
    for path in segments:
        if len(paths)>=limits.max_segments:raise ValueError('snapshot segment count bound')
        paths.append(Path(path))
    if not paths or len(set(paths))!=len(paths):raise ValueError('distinct committed segments required')
    return paths


def _records(paths,limits):
    count=size=0
    for path in paths:
        for record in iter_segment_records(path):
            count+=1;size+=len(record.to_bytes())
            if count>limits.max_records or size>limits.max_scan_bytes:raise ValueError('publication raw scan bound')
            yield path,record


def _segment_reference(path):
    # iter_segment_records verifies the full manifest and its fixed archive files.
    encoded=(path/'manifest.json').read_bytes()
    return dict(context_sha256=strict_json(encoded)['context_sha256'],manifest_sha256=digest(encoded))


def plan_from_descriptor(value):
    """Restore typed frozen bindings; constructor validation rejects unknown keys."""
    try:
        bindings=[]
        for raw in value['bindings']:
            row=dict(raw);target=dict(row['target'])
            target['instrument']=InstrumentKey(**target['instrument']);row['target']=FeedTarget(**target)
            for name in ('ws_source','metadata_source','seed_source'):
                if row.get(name) is not None:row[name]=StreamKey(**row[name])
            bindings.append(BookBinding(**row))
        if set(value)!={'bindings','config_sha256'}:raise ValueError('unknown snapshot plan')
        return SnapshotPlan(tuple(bindings),value['config_sha256'])
    except (KeyError,TypeError) as exc:raise ValueError('invalid snapshot plan') from exc


def _ledger_view(paths,base,*,pin=None,limits):
    descriptor=base['descriptor'];closed_pin=LedgerPin(**descriptor['ledger_pin'])
    close=None
    for _,record in _records(paths,limits):
        if record.record_id==descriptor['close_record_id']:
            if digest(record.to_bytes())!=descriptor['close_sha256']:raise ValueError('close raw hash conflict')
            close=record;break
    if close is None:raise ValueError('committed close raw dependency missing')
    def ledger_prefix():
        for _,record in _records(paths,limits):
            yield record
            if pin is not None and record.kind=='coverage_event' and record.channel=='validity:'+descriptor['capture_scope']:
                entry=LedgerEntry.from_bytes(record.body)
                if entry.ledger_id==pin.ledger_id and entry.sequence==pin.sequence:
                    if entry.entry_hash!=pin.head_hash:raise ValueError('requested ledger pin hash mismatch')
                    return
        if pin is not None:raise ValueError('requested ledger pin unavailable')
    with tempfile.TemporaryDirectory(prefix='dc-publication-') as directory:
        with Journal(Path(directory)/'reader.wal',venue=close.venue,environment=close.environment,create=True) as empty:
            durable=DurableLedger(empty,ledger_id=closed_pin.ledger_id,capture_scope=descriptor['capture_scope'],
                capture_id=close.receipt.capture_id,process_session_id='publication-reader',provenance=close.provenance,
                prior_records=ledger_prefix,
                max_scan_records=limits.max_records,max_scan_bytes=limits.max_scan_bytes,expected_pin=closed_pin)
            selected=durable.pin() if pin is None else pin
            if not isinstance(selected,LedgerPin) or selected.sequence<closed_pin.sequence:
                raise ValueError('publication ledger cannot precede closure knowledge')
            projection=ValidityLedger(selected.ledger_id)
            for entry in durable.entries(selected):projection.add(entry)
            projection.verify_pin(closed_pin)
            dependencies=[asdict(row) for row in durable.dependencies(selected)]
    needed={row['record_id']:row['raw_sha256'] for row in dependencies};found=set();references={}
    for path,record in _records(paths,limits):
        if record.record_id in needed:
            if digest(record.to_bytes())!=needed[record.record_id]:raise ValueError('ledger raw dependency hash conflict')
            found.add(record.record_id);reference=_segment_reference(path)
            references[reference['context_sha256']]=reference
            if found==set(needed):break
    if found!=set(needed):raise ValueError('ledger raw dependency missing')
    return projection,selected,dependencies,[references[key] for key in sorted(references)]


def _publication(base,paths,*,pin=None,limits,policy='ledger-at-start/1',base_validities=None):
    if policy not in {'ledger-at-start/1','ledger-at-start/2'}:raise ValueError('unknown snapshot publication policy')
    ledger,pin,dependencies,segments=_ledger_view(paths,base,pin=pin,limits=limits)
    descriptor=dict(base['descriptor']);payload=strict_json(base['payload'])
    plan=plan_from_descriptor(descriptor['plan'])
    if len(payload['books'])!=len(plan.bindings):raise ValueError('snapshot book population mismatch')
    if policy=='ledger-at-start/2' and (type(base_validities) is not tuple or len(base_validities)!=len(plan.bindings) or
            any(v not in {'valid','invalid','unknown','missing'} for v in base_validities)):
        raise ValueError('independent replay validity required for publication')
    for i,(book,binding) in enumerate(zip(payload['books'],plan.bindings)):
        original=book['validity_as_published']
        baseline=base_validities[i] if policy=='ledger-at-start/2' else original
        assessment=ledger.assess(descriptor['capture_scope'],binding.target,descriptor['cutoff_id'],baseline,pin)
        book['validity_at_close']=original
        book['validity_as_published']=assessment['assessed_validity']
        book['publication_corrections']=assessment['corrections']
    body=canonical(payload)
    if len(body)>limits.max_payload_bytes:raise ValueError('publication payload byte bound')
    descriptor.update(materialization_id=base['snapshot_id'],publication_policy=policy,
        publication_ledger_pin=asdict(pin),publication_ledger_dependencies=dependencies,
        publication_ledger_segments=segments,payload_sha256=digest(body))
    encoded=canonical(descriptor)
    return dict(snapshot_id=digest(encoded),descriptor=strict_json(encoded),payload=body)


def _bounded_bytes(path,limit):
    with path.open('rb') as source:data=source.read(limit+1)
    if len(data)>limit:raise ValueError('snapshot file byte bound')
    return data


def _manifest(result):
    descriptor=canonical(result['descriptor']);payload=result['payload']
    return dict(schema='dc.snapshot-commit/2',snapshot_id=result['snapshot_id'],
        descriptor_sha256=digest(descriptor),descriptor_bytes=len(descriptor),
        payload_sha256=digest(payload),payload_bytes=len(payload))


def _index(result):
    return dict(schema='dc.snapshot-index/2',close_record_id=result['descriptor']['close_record_id'],snapshot_id=result['snapshot_id'])


def _load(root,snapshot_id,limits):
    sha256(snapshot_id,'snapshot_id');directory=Path(root)/'objects'/snapshot_id
    descriptor_bytes=_bounded_bytes(directory/'descriptor.json',limits.max_descriptor_bytes)
    descriptor=strict_json(descriptor_bytes)
    if canonical(descriptor)!=descriptor_bytes or digest(descriptor_bytes)!=snapshot_id:
        raise ValueError('snapshot descriptor identity mismatch')
    payload=_bounded_bytes(directory/'payload.json',limits.replay.max_payload_bytes)
    if descriptor.get('schema')!='dc.snapshot/2' or descriptor.get('publication_policy') not in {'ledger-at-start/1','ledger-at-start/2'}:
        raise ValueError('unknown snapshot publication schema')
    result=dict(snapshot_id=snapshot_id,descriptor=descriptor,payload=payload)
    marker=strict_json(_bounded_bytes(directory/'manifest.json',limits.max_manifest_bytes))
    if marker!=_manifest(result) or descriptor['payload_sha256']!=digest(payload):
        raise ValueError('snapshot commit/hash mismatch')
    sha256(descriptor['close_record_id'],'close_record_id')
    index=strict_json(_bounded_bytes(Path(root)/'closures'/(descriptor['close_record_id']+'.json'),limits.max_manifest_bytes))
    if index!=_index(result):raise ValueError('snapshot publication index conflict')
    return result


def _pinned_paths(paths,descriptor):
    required={}
    for reference in descriptor['raw_segments']+descriptor['publication_ledger_segments']:
        manifest=reference['manifest_sha256'];context=reference['context_sha256']
        sha256(manifest,'manifest_sha256');sha256(context,'context_sha256')
        if manifest in required and required[manifest]!=context:raise ValueError('conflicting pinned segment identity')
        required[manifest]=context
    selected=[];found=set()
    for path in paths:
        try:encoded=_bounded_bytes(path/'manifest.json',8*1024*1024)
        except (FileNotFoundError,ValueError):continue # unrelated unavailable suffix is not a dependency
        identity=digest(encoded)
        if identity not in required:continue
        if identity in found:raise ValueError('duplicate copy of pinned segment')
        if strict_json(encoded)['context_sha256']!=required[identity]:raise ValueError('pinned segment context mismatch')
        found.add(identity);selected.append(path)
    if found!=set(required):raise ValueError('pinned committed segment dependency missing')
    return selected


def _commit(root,result,limits):
    descriptor=canonical(result['descriptor']);marker=canonical(_manifest(result));index=canonical(_index(result))
    if len(descriptor)>limits.max_descriptor_bytes or max(len(marker),len(index))>limits.max_manifest_bytes:
        raise ValueError('snapshot publication metadata bound')
    directory=root/'objects'/result['snapshot_id']
    _mkdir(root);_mkdir(root/'objects');_mkdir(directory);_mkdir(root/'closures')
    _publish_bytes(directory/'payload.json',result['payload'])
    _publish_bytes(directory/'descriptor.json',descriptor)
    _publish_bytes(directory/'manifest.json',marker)
    _publish_bytes(root/'closures'/(result['descriptor']['close_record_id']+'.json'),index)


def read_snapshot(root,snapshot_id,segments,*,limits=PublicationLimits()):
    """Return immutable original publication bytes after full raw dependency replay."""
    result=_load(root,snapshot_id,limits);descriptor=result['descriptor']
    paths=_pinned_paths(_paths(segments,limits.replay),descriptor)
    base,validities=_materialize_snapshot(paths,descriptor['close_record_id'],plan_from_descriptor(descriptor['plan']),limits=limits.replay)
    replayed=_publication(base,paths,pin=LedgerPin(**descriptor['publication_ledger_pin']),limits=limits.replay,
        policy=descriptor['publication_policy'],base_validities=validities)
    if replayed!=result:raise ValueError('published snapshot differs from verified raw replay')
    return result


def publish_snapshot(root,segments,close_record_id,plan,*,limits=PublicationLimits(),expected_close_sha256=None):
    """Manifest then close index commit; retries retain the first pinned publication.

    A different plan/content for an already published close is an error. No object
    is overwritten, including after crashes. Unindexed objects remain provisional.
    """
    sha256(close_record_id,'close_record_id');paths=_paths(segments,limits.replay);root=Path(root)
    if expected_close_sha256 is not None:sha256(expected_close_sha256,'expected close hash')
    index_path=root/'closures'/(close_record_id+'.json')
    if index_path.exists():
        index=strict_json(_bounded_bytes(index_path,limits.max_manifest_bytes))
        existing=read_snapshot(root,index['snapshot_id'],paths,limits=limits)
        if expected_close_sha256 is not None and existing['descriptor']['close_sha256']!=expected_close_sha256:
            raise ValueError('snapshot close differs from pinned acquisition hash')
        if existing['descriptor']['close_record_id']!=close_record_id or canonical(existing['descriptor']['plan'])!=canonical(asdict(plan)):
            raise ValueError('different snapshot plan/content for published close')
        # A previous link may have survived while the caller never observed its
        # directory fsync. Repeat durable publication even when all names exist.
        _commit(root,existing,limits)
        return existing
    base,validities=_materialize_snapshot(paths,close_record_id,plan,limits=limits.replay)
    if expected_close_sha256 is not None and base['descriptor']['close_sha256']!=expected_close_sha256:
        raise ValueError('snapshot close differs from pinned acquisition hash')
    result=_publication(base,paths,limits=limits.replay,policy='ledger-at-start/2',base_validities=validities)
    _commit(root,result,limits)
    return read_snapshot(root,result['snapshot_id'],paths,limits=limits)
