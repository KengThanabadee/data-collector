"""Verified native continuation state at an immutable cutoff frontier.

First integration supports snapshot-seeded book profiles. Loading replays raw;
native suffix rendering does not yet accelerate capture or restore control projections.
"""
from dataclasses import asdict,dataclass
from pathlib import Path

from .bookstate import BookStateLimits,restore_snapshot_state,snapshot_state
from .contracts import StreamKey,canonical,digest,integer,sha256,strict_json
from .gapmap import _boundary
from .journal import _publish_bytes
from .replay import ReplayLimits,SnapshotPlan,_materialize_snapshot
from .segments import _mkdir,iter_segment_records
from .snapshots import _bounded_bytes,plan_from_descriptor

SCHEMA='dc.native-checkpoint/1'


@dataclass(frozen=True,slots=True)
class NativeCheckpointLimits:
    replay: ReplayLimits = ReplayLimits()
    book: BookStateLimits = BookStateLimits()
    max_bytes: int = 32*1024*1024

    def __post_init__(self):
        if not isinstance(self.replay,ReplayLimits) or not isinstance(self.book,BookStateLimits):
            raise ValueError('typed native checkpoint bounds required')
        integer(self.max_bytes,'checkpoint bytes',1)


@dataclass(frozen=True,slots=True)
class VerifiedNativeState:
    checkpoint_id: str
    frontier: dict
    models: tuple
    terminated: tuple[bool,...]
    stream_frontiers: tuple[dict,...]
    last_applied: tuple
    diagnostics: tuple[dict,...]
    transport_policies: frozenset
    buffered_policies: frozenset


def _build(paths,close_id,plan,limits,*,_aux=None):
    if not isinstance(plan,SnapshotPlan):raise ValueError('typed native checkpoint plan required')
    if any(b.target.profile not in {'deribit_full','bybit_standard'} for b in plan.bindings):
        raise ValueError('snapshot-seeded native checkpoint profiles only')
    native={}
    artifact,_=_materialize_snapshot(paths,close_id,plan,limits=limits.replay,_native_state=native)
    frontier=artifact['descriptor']
    wanted={model.last_source for model in native['models'] if model.last_source is not None}
    admitted={StreamKey(**row['stream']):row for row in frontier['admitted_positions']}
    receipts={};proofs={};count=size=0;found=False
    for path in paths:
        for record in iter_segment_records(path):
            count+=1;size+=len(record.to_bytes())
            if count>limits.replay.max_records or size>limits.replay.max_scan_bytes:
                raise ValueError('native checkpoint prefix bound')
            stream=record.receipt.stream_key;row=admitted.get(stream)
            if row is not None and row['admitted_seq'] is not None and record.receipt.receive_seq<=row['admitted_seq']:
                receipts[stream]=asdict(record.receipt)
            if record.record_id in wanted:
                proofs[record.record_id]=dict(record_id=record.record_id,raw_sha256=digest(record.to_bytes()),
                    receipt=asdict(record.receipt),boundary=_boundary(record))
            if record.record_id==close_id:found=True;break
        if found:break
    if not found or set(proofs)!=wanted:raise ValueError('native checkpoint applied raw dependency missing')
    value=dict(schema=SCHEMA,frontier=frontier,
        stream_frontiers=[dict(row,last_receipt=receipts.get(StreamKey(**row['stream']))) for row in frontier['admitted_positions']],
        books=[dict(state=snapshot_state(model,limits=limits.book),terminated=ended,
                    last_applied=proofs.get(model.last_source))
            for model,ended in zip(native['models'],native['terminated'])])
    encoded=canonical(value)
    if _aux is not None:_aux.update(native)
    if len(encoded)>limits.max_bytes:raise ValueError('native checkpoint byte bound')
    return encoded


def make_native_checkpoint(segments,close_id,plan,*,limits=NativeCheckpointLimits()):
    """Derive complete native state without dropping ledger or raw dependencies."""
    if not isinstance(limits,NativeCheckpointLimits):raise ValueError('typed native checkpoint limits required')
    return _build(_checkpoint_paths(segments,limits),close_id,plan,limits)


def _checkpoint_paths(segments,limits):
    paths=[]
    for path in segments:
        if len(paths)>=limits.replay.max_segments:raise ValueError('native checkpoint segment bound')
        paths.append(Path(path))
    return paths


def verify_native_checkpoint(encoded,checkpoint_id,segments,*,limits=NativeCheckpointLimits()):
    """Revalidate against raw, not only the checkpoint's self-consistent hash.

    The returned models are continuation state, not current ledger certification.
    Control history, terminal latches and open gaps must not be discarded by a
    live capture consumer. Runtime checkpoint selection is not installed here.
    """
    if not isinstance(limits,NativeCheckpointLimits):raise ValueError('typed native checkpoint limits required')
    sha256(checkpoint_id,'checkpoint_id')
    if type(encoded) is not bytes or len(encoded)>limits.max_bytes:raise ValueError('native checkpoint byte bound')
    value=strict_json(encoded)
    if (type(value) is not dict or set(value)!={'schema','frontier','stream_frontiers','books'} or
            value['schema']!=SCHEMA or canonical(value)!=encoded or digest(encoded)!=checkpoint_id):
        raise ValueError('native checkpoint identity/schema differs')
    if type(value['frontier']) is not dict:raise ValueError('checkpoint frontier required')
    plan=plan_from_descriptor(value['frontier'].get('plan'))
    if type(value['books']) is not list or len(value['books'])!=len(plan.bindings) or len(plan.bindings)>limits.replay.max_books:
        raise ValueError('native checkpoint book population differs')
    models=[];ended=[];last=[];total_levels=0
    for row in value['books']:
        if type(row) is not dict or set(row)!={'state','terminated','last_applied'} or type(row['terminated']) is not bool:
            raise ValueError('native checkpoint state/latch shape')
        state=row['state']
        if type(state) is not dict or type(state.get('levels')) is not dict:
            raise ValueError('native checkpoint book levels required')
        for side in ('bids','asks'):
            if type(state['levels'].get(side)) not in (list,tuple):raise ValueError('native checkpoint side required')
            total_levels+=len(state['levels'][side])
        if total_levels>limits.replay.max_total_levels:raise ValueError('native checkpoint aggregate level bound')
        models.append(restore_snapshot_state(state,limits=limits.book));ended.append(row['terminated']);last.append(row['last_applied'])
    aux={}
    expected=_build(_checkpoint_paths(segments,limits),value['frontier'].get('close_record_id'),plan,limits,_aux=aux)
    if encoded!=expected:raise ValueError('native checkpoint differs from complete raw replay')
    return VerifiedNativeState(checkpoint_id,value['frontier'],tuple(models),tuple(ended),
        tuple(value['stream_frontiers']),tuple(last),tuple(aux['diagnostics']),
        aux['transport_policies'],aux['buffered_policies'])


def _marker(encoded):
    return dict(schema='dc.native-checkpoint-commit/1',checkpoint_id=digest(encoded),bytes=len(encoded))


def publish_native_checkpoint(root,segments,close_id,plan,*,limits=NativeCheckpointLimits()):
    encoded=make_native_checkpoint(segments,close_id,plan,limits=limits)
    identity=digest(encoded);directory=Path(root)/'objects'/identity
    _mkdir(Path(root));_mkdir(Path(root)/'objects');_mkdir(directory)
    _publish_bytes(directory/'checkpoint.json',encoded)
    _publish_bytes(directory/'manifest.json',canonical(_marker(encoded)))
    return identity


def read_native_checkpoint(root,checkpoint_id,segments,*,limits=NativeCheckpointLimits()):
    if not isinstance(limits,NativeCheckpointLimits):raise ValueError('typed native checkpoint limits required')
    sha256(checkpoint_id,'checkpoint_id');directory=Path(root)/'objects'/checkpoint_id
    encoded=_bounded_bytes(directory/'checkpoint.json',limits.max_bytes)
    marker=_bounded_bytes(directory/'manifest.json',4096)
    if marker!=canonical(_marker(encoded)) or digest(encoded)!=checkpoint_id:
        raise ValueError('native checkpoint commit/hash differs')
    return verify_native_checkpoint(encoded,checkpoint_id,segments,limits=limits)


def materialize_from_checkpoint(encoded,checkpoint_id,segments,target_close_id,plan,*,limits=NativeCheckpointLimits(),stats=None):
    """Re-verify native state and continue to a later cutoff of the same plan.

    Control/ledger/metadata certification still validates the full prefix. A new
    audit policy, owner replay context or nonextending admission falls back to
    raw native replay.
    Stats count renderer binding dispatches only, excluding verification scans.
    """
    if not isinstance(limits,NativeCheckpointLimits):raise ValueError('typed native checkpoint limits required')
    if stats is not None and type(stats) is not dict:raise ValueError('mutable checkpoint statistics dict required')
    paths=_checkpoint_paths(segments,limits)
    verified=verify_native_checkpoint(encoded,checkpoint_id,paths,limits=limits)
    observed={}
    artifact,_=_materialize_snapshot(paths,target_close_id,plan,limits=limits.replay,
        _native_resume=verified,_native_state=observed)
    if stats is not None:
        stats.update(native_mode=observed['native_mode'],renderer_binding_dispatches=observed['binding_dispatches'],
            renderer_checkpoint_skips=observed['checkpoint_skips'],checkpoint_verification='full_raw_prefix',
            control_certification='full_raw_prefix')
    return artifact
