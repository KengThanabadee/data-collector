"""Owned capture inventory and automatic pinned startup selection.

The registry is fsynced before a writer is exposed. An opened session may never
vanish; an interrupted reservation may only be abandoned after proving it empty.
The root lock stays held until its current writer stops. This is local disk-owner
work; callers must run it before live ingress and outside the event loop.
"""
from dataclasses import asdict, dataclass
import os
from pathlib import Path
from types import SimpleNamespace

from .contracts import Provenance, RawRecord, Receipt, canonical, digest, integer, strict_json, text
from .cutoffs import CutoffCoordinator
from .journal import Journal, JournalLimits, MAGIC, _publish_bytes
from .ledger import DurableLedger
from .recovery import RecoveryRequest, RecoverySegment, iter_recovery_prefix
from .segments import SegmentLimits, _mkdir
from .writer import SessionWriter, _archive_root, _descriptor, _lock, _opened_bytes, recover_session


@dataclass(frozen=True, slots=True)
class StartupLimits:
    max_sessions: int = 128
    max_segments: int = 1024
    max_records: int = 200000
    max_scan_bytes: int = 256 * 1024 * 1024
    max_disk_bytes: int = 512 * 1024 * 1024
    max_catalog_bytes: int = 8 * 1024 * 1024

    def __post_init__(self):
        for name, value in asdict(self).items():integer(value, name, 1)
        if self.max_sessions > 4096 or self.max_segments > 10000:
            raise ValueError('startup inventory count bound exceeds supported profile')


@dataclass(frozen=True, slots=True)
class PreparedSession:
    writer: SessionWriter
    recovery: RecoveryRequest | None
    inventory_sha256: str


class _EmptyJournal:
    def __init__(self, identity):
        self.identity = identity
        self.limits = JournalLimits()
        self.state = SimpleNamespace(records=0, valid_bytes=0, positions={})
    def encoded_records(self, *, allow_unsealed=False):return iter(())


class CaptureStore:
    """Exclusive local root with durable reservation/opened inventory.

    Create a new owned root explicitly; existing unregistered archives are never
    adopted from directory order. close() refuses while a returned writer lives.
    Recovery is repeatable and preserves old WAL/tail/segment evidence.
    """
    def __init__(self, root, *, venue, environment, capture_id, capture_scope, ledger_id,
                 create=False, limits=StartupLimits(), checkpoint=lambda stage: None):
        if not isinstance(limits, StartupLimits):raise ValueError('typed startup limits required')
        for name, value in [('venue',venue),('environment',environment),('capture',capture_id),('scope',capture_scope),('ledger',ledger_id)]:
            text(value,name)
            if len(value.encode())>128:raise ValueError('capture store identity byte bound')
        if environment not in {'production','testnet'}:raise ValueError('explicit archive environment required')
        self.root=Path(root);self.limits=limits;self.checkpoint=checkpoint
        self.identity=(venue,environment);self.capture_id=capture_id;self.scope=capture_scope;self.ledger_id=ledger_id
        self._owner=_lock(self.root);self.catalog=None;self.writer=None;self.poisoned=False
        self.metadata=dict(schema='dc.capture-store/2',venue=venue,environment=environment,capture_id=capture_id,
                           capture_scope=capture_scope,ledger_id=ledger_id)
        try:
            if create:
                if list(self.root.iterdir())!=[self.root/'owner.lock']:
                    raise ValueError('new empty owned capture root required')
                _publish_bytes(self.root/'capture.json',canonical(self.metadata))
                self.catalog=Journal(self.root/'registry.wal',venue=venue,environment=environment,
                                     limits=self._catalog_limits(),create=True)
            else:
                if self._read(self.root/'capture.json',65536)!=canonical(self.metadata):
                    raise ValueError('capture root identity differs')
                self.catalog=Journal(self.root/'registry.wal',venue=venue,environment=environment,limits=self._catalog_limits())
            if self.catalog.state.reason:raise ValueError('capture registry damaged; retain evidence and repair explicitly')
            self._load()
            self._layout()
        except BaseException:
            if self.catalog is not None:self.catalog.close()
            os.close(self._owner);self._owner=None
            raise

    def _catalog_limits(self):
        return JournalLimits(max_record_bytes=131072,target_bytes=self.limits.max_catalog_bytes,max_streams=self.limits.max_sessions)

    @staticmethod
    def _read(path, bound):
        with path.open('rb') as source:data=source.read(bound+1)
        if len(data)>bound:raise ValueError('startup metadata byte bound')
        return data

    def _ready(self):
        if self._owner is None or self.poisoned:raise ValueError('capture store closed or unconfirmed; reopen before startup')
        if self.writer is not None and not self.writer.closed:raise ValueError('stop current writer before capture startup')

    def _load(self):
        self.sessions=[];self._head=None;self._entries=0
        for encoded in self.catalog.encoded_records(allow_unsealed=True):
            if self._entries>=2*self.limits.max_sessions:raise ValueError('capture registry entry bound')
            record=RawRecord.from_bytes(encoded);value=strict_json(record.body)
            if (not isinstance(value,dict) or set(value)!={'schema','sequence','previous_sha256','phase','descriptor'} or
                    value['schema']!='dc.capture-session/2' or value['sequence']!=self._entries or
                    value['previous_sha256']!=self._head or value['phase'] not in {'reserved','opened','abandoned'}):
                raise ValueError('capture registry missing/forked entry')
            desc=value['descriptor'];identity=desc['identity']
            expected=_descriptor(identity['venue'],identity['environment'],identity['capture_id'],identity['process_session_id'],
                Provenance(**desc['provenance']),desc['started_utc_ns'],JournalLimits(**desc['journal_limits']),
                SegmentLimits(**desc['segment_limits']),desc['rotate_ns'],desc['max_segments'])
            if desc!=expected or (identity['venue'],identity['environment'],identity['capture_id'])!=(*self.identity,self.capture_id):
                raise ValueError('capture registry session identity differs')
            r=record.receipt
            phase=value['phase'];seq=0 if phase=='reserved' else 1
            if (r.capture_id,r.process_session_id,r.connection_id,r.receive_seq)!=(self.capture_id,identity['process_session_id'],'control:capture-registry',seq) or (
                    record.kind,record.channel,record.native_product,record.instrument,record.native_fields_json)!=(
                    'control','capture_registry','control',None,b'{}') or record.provenance!=Provenance(**desc['provenance']):
                raise ValueError('foreign capture registry envelope')
            if phase=='reserved':
                if len(self.sessions)>=self.limits.max_sessions or (self.sessions and self.sessions[-1]['phase']=='reserved') or any(
                        s['descriptor']['session_id']==desc['session_id'] for s in self.sessions):
                    raise ValueError('duplicate or unresolved capture reservation')
                self.sessions.append(dict(phase=phase,descriptor=desc))
            else:
                if not self.sessions or self.sessions[-1]!=dict(phase='reserved',descriptor=desc):
                    raise ValueError('capture session activation without matching reservation')
                self.sessions[-1]['phase']=phase
            self._head=digest(encoded);self._entries+=1

    def _layout(self):
        """Bound all filesystem enumeration before touching recovery WALs."""
        known={s['descriptor']['session_id']:s for s in self.sessions}
        roots={str(_archive_root(self.root/'archive',s['descriptor']).relative_to(self.root/'archive')) for s in self.sessions}
        count=size=wal_count=0
        stack=[self.root]
        while stack:
            directory=stack.pop()
            with os.scandir(directory) as entries:
                for entry in entries:
                    count+=1
                    if count>32*self.limits.max_segments+16*self.limits.max_sessions+16:
                        raise ValueError('startup filesystem entry bound')
                    if entry.is_symlink():raise ValueError('capture store symlinks are not supported')
                    path=Path(entry.path);parts=path.relative_to(self.root).parts
                    if len(parts)>10:raise ValueError('capture store directory depth bound')
                    if parts[0] not in {'owner.lock','capture.json','registry.wal','sessions','archive'}:
                        raise ValueError('unregistered capture root entry')
                    if parts[0]=='sessions' and len(parts)>=2 and parts[1] not in known:
                        raise ValueError('unregistered capture session; do not guess missing registry history')
                    if parts[0]=='archive':
                        relative='/'.join(parts[1:])
                        if relative and not any(root==relative or root.startswith(relative+'/') or relative.startswith(root+'/') for root in roots):
                            raise ValueError('unregistered archive session')
                    if entry.is_dir(follow_symlinks=False):stack.append(path)
                    elif entry.is_file(follow_symlinks=False):
                        size+=entry.stat(follow_symlinks=False).st_size
                        if size>self.limits.max_disk_bytes:raise ValueError('startup disk byte scan bound')
                        if parts[0]=='sessions' and len(parts)==3 and path.suffix=='.wal':wal_count+=1
                        if wal_count>self.limits.max_segments:raise ValueError('startup WAL segment count bound')
                    else:raise ValueError('regular capture store entries required')

    def _unused(self,desc):
        """A reservation was never exposed: prove no raw data before abandoning."""
        directory=self.root/'sessions'/desc['session_id']
        archive=_archive_root(self.root/'archive',desc)
        if archive.exists():raise ValueError('unopened session has archive evidence')
        if not directory.exists():return
        owner=_lock(directory)
        try:
            names={p.name for p in directory.iterdir()}
            if not names<= {'owner.lock','session.json','00000000.wal','opened'}:raise ValueError('unopened session has unexpected evidence')
            if 'session.json' in names and self._read(directory/'session.json',65536)!=canonical(desc):
                raise ValueError('unopened descriptor differs')
            if 'opened' in names:
                proofs=list((directory/'opened').iterdir())
                if proofs and (len(proofs)!=1 or proofs[0].name!='00000000.json' or
                        self._read(proofs[0],2048)!=_opened_bytes(desc,0) or '00000000.wal' not in names):
                    raise ValueError('unopened WAL proof differs')
            if '00000000.wal' in names:
                if 'session.json' not in names:raise ValueError('unopened WAL lacks descriptor')
                with Journal(directory/'00000000.wal',venue=self.identity[0],environment=self.identity[1],
                             limits=JournalLimits(**desc['journal_limits'])) as wal:
                    if wal.state.records or wal.state.reason or wal.state.valid_bytes!=len(MAGIC):
                        raise ValueError('unopened session contains unregistered raw data')
        finally:os.close(owner)

    def recover(self, *, next_process_session_id):
        """Recover every registered old session and derive authoritative tips."""
        self._ready();text(next_process_session_id,'next process session')
        if len(next_process_session_id.encode())>128:raise ValueError('process session identity byte bound')
        if any(s['descriptor']['identity']['process_session_id']==next_process_session_id for s in self.sessions):
            raise ValueError('new process session identity required')
        self._load();self._layout();references=[]
        for session in self.sessions:
            desc=session['descriptor'];directory=self.root/'sessions'/desc['session_id']
            if session['phase']!='opened':self._unused(desc);continue
            if not (directory/'session.json').is_file():raise ValueError('opened capture session missing')
            if self._read(directory/'session.json',65536)!=canonical(desc):raise ValueError('opened session descriptor changed')
            paths=recover_session(directory,self.root/'archive',checkpoint=self.checkpoint,require_opened=True)
            for path in paths:
                if len(references)>=self.limits.max_segments:raise ValueError('startup immutable segment bound')
                context=self._read(path/'context.json',65536)
                manifest=self._read(path/'manifest.json',SegmentLimits().max_manifest_bytes)
                references.append(RecoverySegment(path,digest(context),digest(manifest)))
        if not references:return None
        references=tuple(references)
        prior=lambda:iter_recovery_prefix(references,venue=self.identity[0],environment=self.identity[1],capture_id=self.capture_id,
            process_session_id=next_process_session_id,max_records=self.limits.max_records,max_scan_bytes=self.limits.max_scan_bytes)
        empty=_EmptyJournal(self.identity)
        provenance=Provenance(**self.sessions[-1]['descriptor']['provenance'])
        observed=0;uncertain=False
        for record in prior():observed+=1
        for reference in references:
            manifest=strict_json(self._read(reference.path/'manifest.json',SegmentLimits().max_manifest_bytes))
            uncertain=uncertain or bool(manifest['recovery_evidence'])
        if not observed:
            if uncertain:raise ValueError('empty recovered history has raw loss uncertainty')
            return None
        coordinator=CutoffCoordinator(empty,capture_scope=self.scope,capture_id=self.capture_id,process_session_id=next_process_session_id,
            provenance=provenance,prior_records=prior)
        ledger=DurableLedger(empty,ledger_id=self.ledger_id,capture_scope=self.scope,capture_id=self.capture_id,
            process_session_id=next_process_session_id,provenance=provenance,prior_records=prior,
            max_scan_records=self.limits.max_records,max_scan_bytes=self.limits.max_scan_bytes)
        if not coordinator.pin.count:
            raise ValueError('raw history without cutoff allocation needs explicit startup gap reconciliation')
        if not ledger.pin().head_hash:raise ValueError('cutoff history lacks validity ledger')
        return RecoveryRequest(references,coordinator.pin,ledger.pin())

    def _append(self,phase,desc,utc_ns,monotonic_ns,clock_epoch):
        value=dict(schema='dc.capture-session/2',sequence=self._entries,previous_sha256=self._head,phase=phase,descriptor=desc)
        record=RawRecord(Receipt(self.capture_id,desc['identity']['process_session_id'],'control:capture-registry',
            0 if phase=='reserved' else 1,utc_ns,monotonic_ns,clock_epoch),*self.identity,'control','capture_registry','control',
            canonical(value),Provenance(**desc['provenance']))
        self.catalog.append(record);self._load()
        self.checkpoint('after_registry_'+phase)

    def prepare_session(self, *, process_session_id, provenance, started_utc_ns, monotonic_ns, clock_epoch,
                        now_ns, limits=JournalLimits(),segment_limits=SegmentLimits(),rotate_ns=60_000_000_000,max_segments=10000):
        """Return a fresh writer and the automatically verified lane recovery input."""
        self._ready()
        recovery=self.recover(next_process_session_id=process_session_id)
        if len(self.sessions)>=self.limits.max_sessions:raise ValueError('capture session inventory bound')
        desc=_descriptor(*self.identity,self.capture_id,process_session_id,provenance,started_utc_ns,limits,segment_limits,rotate_ns,max_segments)
        # Receipt validation and serialized size preflight occur before reservation.
        Receipt(self.capture_id,process_session_id,'control:capture-registry',0,started_utc_ns,monotonic_ns,clock_epoch)
        self.poisoned=True
        try:
            if self.sessions and self.sessions[-1]['phase']=='reserved':
                self._append('abandoned',self.sessions[-1]['descriptor'],started_utc_ns,monotonic_ns,clock_epoch)
            self._append('reserved',desc,started_utc_ns,monotonic_ns,clock_epoch)
            self.writer=SessionWriter(self.root/'sessions',self.root/'archive',venue=self.identity[0],environment=self.identity[1],
                capture_id=self.capture_id,process_session_id=process_session_id,provenance=provenance,started_utc_ns=started_utc_ns,
                now_ns=now_ns,limits=limits,segment_limits=segment_limits,rotate_ns=rotate_ns,max_segments=max_segments,checkpoint=self.checkpoint)
            self.checkpoint('after_writer_create')
            self._append('opened',desc,started_utc_ns,monotonic_ns,clock_epoch)
            result=PreparedSession(self.writer,recovery,self._head)
            self.poisoned=False
            return result
        except BaseException:
            if self.writer is not None:self.writer.abort()
            raise

    def close(self):
        if self._owner is None:return
        if self.writer is not None and not self.writer.closed:raise ValueError('stop current writer before closing capture store')
        self.catalog.close();os.close(self._owner);self._owner=None

    def __enter__(self):return self
    def __exit__(self,*args):self.close()
