"""Reproducible immutable exports with explicit original/assessed knowledge modes.

Original snapshot payloads are retained verbatim through canonical JSON. Quality
assessments live alongside them; later archive knowledge never changes features.
"""
from dataclasses import asdict,dataclass
from pathlib import Path

from .contracts import canonical,digest,integer,sha256,strict_json
from .journal import _publish_bytes
from .replay import _materialize_snapshot
from .segments import _mkdir
from .snapshots import (PublicationLimits,_bounded_bytes,_ledger_view,_paths,_pinned_paths,
                        _publication,plan_from_descriptor,read_snapshot)
from .validity import LedgerPin


@dataclass(frozen=True)
class ExportLimits:
    publication: PublicationLimits = PublicationLimits()
    max_snapshots: int = 128
    max_payload_bytes: int = 32*1024*1024
    max_descriptor_bytes: int = 16*1024*1024
    max_manifest_bytes: int = 65536

    def __post_init__(self):
        if not isinstance(self.publication,PublicationLimits):raise ValueError('typed publication limits required')
        for name in ('max_snapshots','max_payload_bytes','max_descriptor_bytes','max_manifest_bytes'):
            integer(getattr(self,name),name,1)


def _ids(values,limits):
    result=[]
    for identity in values:
        if len(result)>=limits.max_snapshots:raise ValueError('export snapshot population bound')
        sha256(identity,'snapshot_id');result.append(identity)
    if not result or len(set(result))!=len(result):raise ValueError('nonempty distinct snapshot population required')
    return sorted(result)


def _render(originals,paths,mode,pin,limits,*,_historical=False):
    if mode not in {'as_published','assessed_at_ledger'}:raise ValueError('explicit export read mode required')
    if mode=='as_published' and pin is not None:raise ValueError('as_published has no assessment pin')
    first=originals[0]['descriptor']
    def scope(row):
        descriptor=row['descriptor']
        capture=descriptor['plan']['bindings'][0]['ws_source']['capture_id']
        return descriptor['capture_scope'],descriptor['ledger_pin']['ledger_id'],capture
    if len({scope(row) for row in originals})!=1:raise ValueError('aggregate cross-scope export requires separate local exports')
    ledger=None;entries=[];dependencies=[];ledger_segments=[]
    if mode=='assessed_at_ledger':
        ledger,pin,dependencies,ledger_segments=_ledger_view(paths,originals[0],pin=pin,limits=limits.publication.replay,_historical=_historical)
        for original in originals:
            published_pin=LedgerPin(**original['descriptor']['publication_ledger_pin'])
            if pin.sequence<published_pin.sequence:raise ValueError('assessment pin precedes original publication knowledge')
            ledger.verify_pin(published_pin)
        entries=[strict_json(entry.to_bytes()) for entry in ledger.entries(pin)]
    observations=[];references={};population={status:0 for status in ('valid','invalid','missing','unknown')}
    estimated_bytes=0
    for original in originals:
        descriptor=original['descriptor'];payload=strict_json(original['payload'])
        if canonical(payload)!=original['payload']:raise ValueError('original snapshot payload is not canonical')
        assessments=[]
        plan=plan_from_descriptor(descriptor['plan'])
        if len(payload['books'])!=len(plan.bindings):raise ValueError('export book population mismatch')
        for book,binding in zip(payload['books'],plan.bindings):
            if ledger is None:population[book['validity_as_published']]+=1
            else:
                assessment=ledger.assess(descriptor['capture_scope'],binding.target,descriptor['cutoff_id'],
                    book['validity_as_published'],pin)
                assessments.append(dict(target=asdict(binding.target),**assessment))
                population[assessment['assessed_validity']]+=1
        row=dict(snapshot_id=original['snapshot_id'],descriptor=descriptor,original_payload=payload,assessments=assessments)
        estimated_bytes+=len(canonical(row))
        if estimated_bytes>limits.max_payload_bytes:raise ValueError('export payload byte bound')
        observations.append(row)
        for reference in descriptor['raw_segments']+descriptor['publication_ledger_segments']:
            references[reference['manifest_sha256']]=reference
    for reference in ledger_segments:references[reference['manifest_sha256']]=reference
    body=canonical(dict(schema='dc.export-payload/2',observations=observations))
    if len(body)>limits.max_payload_bytes:raise ValueError('export payload byte bound')
    descriptor=dict(schema='dc.export/2',policy='receive-asof/2',mode=mode,
        snapshot_ids=[row['snapshot_id'] for row in originals],capture_scope=first['capture_scope'],
        ledger_pin=asdict(pin) if ledger else None,ledger_entries=entries,ledger_raw_dependencies=dependencies,
        correction_matching_rule='scope-target-half-open-cutoff/1',population=population,
        assessment_feature_policy='archive quality knowledge; do not silently use later corrections as causal input features',
        raw_segments=[references[key] for key in sorted(references)],payload_sha256=digest(body))
    encoded=canonical(descriptor)
    if len(encoded)>limits.max_descriptor_bytes:raise ValueError('export descriptor byte bound')
    return dict(export_id=digest(encoded),descriptor=strict_json(encoded),payload=body)


def _manifest(result):
    descriptor=canonical(result['descriptor'])
    return dict(schema='dc.export-commit/2',export_id=result['export_id'],descriptor_sha256=digest(descriptor),
        descriptor_bytes=len(descriptor),payload_sha256=digest(result['payload']),payload_bytes=len(result['payload']))


def export_snapshots(snapshot_root,export_root,snapshot_ids,segments,*,mode='assessed_at_ledger',ledger_pin=None,limits=ExportLimits()):
    """Pin the latest fully committed available ledger once, retain every row.

    Explicit older pins reproduce prior exports when later corrections arrive.
    No account, network connection, current quote or mutable WAL participates.
    """
    identities=_ids(snapshot_ids,limits);paths=_paths(segments,limits.publication.replay)
    originals=[];original_bytes=0
    for identity in identities:
        original=read_snapshot(snapshot_root,identity,paths,limits=limits.publication)
        original_bytes+=len(original['payload'])+len(canonical(original['descriptor']))
        if original_bytes>limits.max_payload_bytes:raise ValueError('export original population byte bound')
        originals.append(original)
    result=_render(originals,paths,mode,ledger_pin,limits)
    descriptor=canonical(result['descriptor']);marker=canonical(_manifest(result))
    if len(marker)>limits.max_manifest_bytes:raise ValueError('export manifest byte bound')
    root=Path(export_root);directory=root/result['export_id'];_mkdir(root);_mkdir(directory)
    _publish_bytes(directory/'payload.json',result['payload'])
    _publish_bytes(directory/'descriptor.json',descriptor)
    _publish_bytes(directory/'manifest.json',marker)
    return read_export(root,result['export_id'],paths,limits=limits)


def read_export(export_root,export_id,segments,*,limits=ExportLimits()):
    """Verify a portable export using only its files and committed raw segments."""
    sha256(export_id,'export_id');directory=Path(export_root)/export_id
    encoded=_bounded_bytes(directory/'descriptor.json',limits.max_descriptor_bytes)
    descriptor=strict_json(encoded)
    if canonical(descriptor)!=encoded or digest(encoded)!=export_id or descriptor.get('schema')!='dc.export/2':
        raise ValueError('export descriptor identity/schema mismatch')
    payload=_bounded_bytes(directory/'payload.json',limits.max_payload_bytes)
    result=dict(export_id=export_id,descriptor=descriptor,payload=payload)
    marker=strict_json(_bounded_bytes(directory/'manifest.json',limits.max_manifest_bytes))
    if marker!=_manifest(result) or descriptor['payload_sha256']!=digest(payload):raise ValueError('export commit/hash mismatch')
    paths=_pinned_paths(_paths(segments,limits.publication.replay),dict(raw_segments=descriptor['raw_segments'],publication_ledger_segments=[]))
    value=strict_json(payload)
    if value.get('schema')!='dc.export-payload/2':raise ValueError('export payload schema mismatch')
    rows=value['observations'];identities=_ids((row['snapshot_id'] for row in rows),limits)
    if identities!=descriptor['snapshot_ids'] or [row['snapshot_id'] for row in rows]!=identities:raise ValueError('export population mismatch')
    originals=[]
    for row in rows:
        original=dict(snapshot_id=row['snapshot_id'],descriptor=row['descriptor'],payload=canonical(row['original_payload']))
        snapshot_descriptor=original['descriptor']
        if digest(canonical(snapshot_descriptor))!=original['snapshot_id']:raise ValueError('embedded snapshot identity mismatch')
        selected=_pinned_paths(paths,snapshot_descriptor)
        base,validities=_materialize_snapshot(selected,snapshot_descriptor['close_record_id'],plan_from_descriptor(snapshot_descriptor['plan']),limits=limits.publication.replay,_historical=True)
        replayed=_publication(base,selected,pin=LedgerPin(**snapshot_descriptor['publication_ledger_pin']),limits=limits.publication.replay,
            policy=snapshot_descriptor['publication_policy'],base_validities=validities,_historical=True)
        if replayed!=original:raise ValueError('embedded original differs from raw publication replay')
        originals.append(original)
    pin=LedgerPin(**descriptor['ledger_pin']) if descriptor['ledger_pin'] is not None else None
    if _render(originals,paths,descriptor['mode'],pin,limits,_historical=True)!=result:raise ValueError('export differs from pinned raw/ledger replay')
    return result
