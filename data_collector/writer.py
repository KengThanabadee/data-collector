"""Serialized rotating WAL owner; receipt and control order survives rotation.

A process session is created once. Crash recovery seals its durable prefix but
never resumes that ingress identity. The supervisor records interruption/gap
controls in a fresh process session before making continuity claims.
"""
from dataclasses import asdict
from datetime import datetime, timezone
import fcntl
import os
from pathlib import Path
import time

from .contracts import Provenance, RawRecord, canonical, digest, integer, sha256, strict_json, text
from .journal import FRAME, MAGIC, Journal, JournalDamaged, JournalFull, JournalLimits, Scan, _publish_bytes, sync_directory
from .segments import SegmentContext, SegmentLimits, _mkdir, iter_segment_records, seal_segment

MAX_TERMINAL_BYTES = 8 * 1024 * 1024


def _read_json(path, limit):
    with path.open('rb') as source:
        encoded = source.read(limit + 1)
    if len(encoded) > limit:
        raise ValueError('writer metadata byte bound')
    return strict_json(encoded)


def _terminal_bytes(value):
    encoded = canonical(value)
    if len(encoded) > MAX_TERMINAL_BYTES:
        raise ValueError('writer terminal metadata bound')
    return encoded


def _lock(directory):
    _mkdir(directory)
    fd = os.open(directory / 'owner.lock', os.O_RDWR | os.O_CREAT | getattr(os, 'O_NOFOLLOW', 0), 0o600)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        os.fsync(fd)
        sync_directory(directory)
        return fd
    except BaseException:
        os.close(fd)
        raise


def _descriptor(venue, environment, capture_id, session, provenance, started_utc_ns, limits, segment_limits, rotate_ns, max_segments):
    if not isinstance(provenance, Provenance):
        raise ValueError('typed provenance required')
    for name, value in [('capture_id', capture_id), ('process_session_id', session), ('environment', environment)]:
        text(value, name)
    if environment not in {'production', 'testnet'}:
        raise ValueError('explicit supported archive environment required')
    for name, value in [('started_utc_ns', started_utc_ns), ('rotate_ns', rotate_ns), ('max_segments', max_segments)]:
        integer(value, name, 1)
    if not isinstance(limits, JournalLimits) or not isinstance(segment_limits, SegmentLimits):
        raise ValueError('typed writer limits required')
    if limits.max_record_bytes > segment_limits.max_record_bytes:
        raise ValueError('segment record bound cannot archive journal records')
    if limits.max_streams > segment_limits.max_streams:
        raise ValueError('segment stream bound cannot archive journal streams')
    if max_segments > 10000:
        raise ValueError('session segment index exceeds supported bounded profile')
    if max(limits.target_bytes, limits.max_record_bytes + 1) > segment_limits.max_uncompressed_bytes:
        raise ValueError('segment byte bound lacks WAL rotation headroom')
    maximum = max(limits.target_bytes, limits.max_record_bytes + 1)
    # Conservative deflate/gzip expansion allowance, independent of compression ratio.
    compressed_bound = maximum + (maximum >> 12) + (maximum >> 14) + (maximum >> 25) + 64
    if compressed_bound > segment_limits.max_compressed_bytes:
        raise ValueError('compressed segment bound lacks worst-case gzip headroom')
    if 16384 + segment_limits.max_recoveries * 1024 > segment_limits.max_manifest_bytes:
        raise ValueError('segment manifest bound lacks control/recovery metadata headroom')
    date = datetime.fromtimestamp(started_utc_ns // 1_000_000_000, timezone.utc).strftime('%Y-%m-%d')
    identity = dict(venue=venue, environment=environment, capture_id=capture_id, process_session_id=session)
    session_id = digest(canonical(identity))
    SegmentContext(venue, environment, capture_id, session, '0', provenance.source_sha256, provenance.config_sha256)
    descriptor = dict(schema='dc.writer-session/2', identity=identity, session_id=session_id,
                provenance=asdict(provenance), started_utc_ns=started_utc_ns, date=date,
                journal_limits=asdict(limits), segment_limits=asdict(segment_limits),
                rotate_ns=rotate_ns, max_segments=max_segments)
    if len(canonical(descriptor)) > 65536:
        raise ValueError('writer session metadata bound')
    return descriptor


def _archive_root(root, descriptor):
    identity = descriptor['identity']
    return Path(root) / 'v2' / identity['venue'] / identity['environment'] / descriptor['date'] / descriptor['session_id']


def _context(descriptor, ordinal):
    identity = descriptor['identity']
    return SegmentContext(**identity, segment_id=str(ordinal),
                          source_sha256=descriptor['provenance']['source_sha256'],
                          config_sha256=descriptor['provenance']['config_sha256'])


def _opened_bytes(descriptor, ordinal):
    return canonical(dict(schema='dc.writer-opened/2',session_sha256=digest(canonical(descriptor)),
                          ordinal=ordinal,context_sha256=_context(descriptor,ordinal).context_id))


def _publish_opened(directory, descriptor, ordinal):
    _mkdir(directory/'opened')
    _publish_bytes(directory/'opened'/f'{ordinal:08d}.json',_opened_bytes(descriptor,ordinal))


def _reference(path, ordinal):
    encoded = (path / 'manifest.json').read_bytes()
    return dict(ordinal=ordinal, context_sha256=strict_json(encoded)['context_sha256'], manifest_sha256=digest(encoded))


class SessionWriter:
    """Journal-compatible facade for one fresh session, with bounded segment index.

    Use tick() for quiet-feed rotation. finish() seals and closes gracefully;
    abort() releases ownership while preserving the current WAL for recovery.
    No method drops old files or starts a new connection/clock epoch.
    """
    def __init__(self, wal_root, archive_root, *, venue, environment, capture_id,
                 process_session_id, provenance, started_utc_ns, now_ns=time.monotonic_ns,
                 limits=JournalLimits(), segment_limits=SegmentLimits(), rotate_ns=60_000_000_000,
                 max_segments=10000, checkpoint=lambda stage: None):
        self.descriptor = _descriptor(venue, environment, capture_id, process_session_id, provenance,
                                      started_utc_ns, limits, segment_limits, rotate_ns, max_segments)
        self.directory = Path(wal_root) / self.descriptor['session_id']
        self._owner = _lock(self.directory)
        self.identity = (venue, environment)
        self.limits, self.segment_limits = limits, segment_limits
        self.archive_root = _archive_root(archive_root, self.descriptor)
        self.now_ns, self.checkpoint = now_ns, checkpoint
        self.committed, self.references = [], []
        self._segment_streams, self._segment_versions = {}, set()
        self._stream_metadata_bytes = 0
        self.state = Scan()
        self.closed = self.poisoned = False
        self.active = None
        try:
            if (self.directory / 'session.json').exists():
                raise ValueError('process session already exists; recover then use a fresh session identity')
            _publish_bytes(self.directory / 'session.json', canonical(self.descriptor))
            self._last_now = self._now()
            self._opened_ns = self._last_now
            self.active = Journal(self.directory / '00000000.wal', venue=venue, environment=environment, limits=limits, create=True)
            _publish_opened(self.directory,self.descriptor,0)
            self.checkpoint('after_wal_open')
        except BaseException:
            self.abort()
            raise

    @property
    def path(self):
        return self.active.path

    def _now(self):
        value = self.now_ns()
        integer(value, 'monotonic observation')
        if hasattr(self, '_last_now') and value < self._last_now:
            self.poisoned = True
            raise ValueError('writer monotonic clock regressed; new session required')
        self._last_now = value
        return value

    def _ready(self):
        if self.closed or self.poisoned:
            raise JournalDamaged('writer closed or poisoned; recover in a fresh process session')

    def append(self, record):
        return self._append(record,sync=True)

    def append_batch(self, records):
        """One bounded FIFO group; acknowledge only after all rotations and fsync."""
        if type(records) is not tuple or not 1<=len(records)<=512:
            raise ValueError('bounded nonempty raw batch required')
        size=0
        for record in records:
            size+=len(record.to_bytes())
            if size>8*1024*1024:raise ValueError('raw batch byte bound')
        try:
            result=tuple(self._append(record,sync=False) for record in records)
            self.active.sync()
            return result
        except BaseException:
            self.poisoned=True
            raise

    def _append(self, record, *, sync):
        self._ready()
        if not isinstance(record, RawRecord):
            raise ValueError('typed raw record required')
        identity = self.descriptor['identity']
        if (record.venue, record.environment, record.receipt.capture_id, record.receipt.process_session_id,
            record.provenance.source_sha256, record.provenance.config_sha256) != (
                *self.identity, identity['capture_id'], identity['process_session_id'],
                self.descriptor['provenance']['source_sha256'], self.descriptor['provenance']['config_sha256']):
            raise ValueError('raw record differs from writer session')
        stream = record.receipt.stream_key
        if record.receipt.receive_seq != self.state.positions.get(stream, -1) + 1:
            raise ValueError('global collector receipt sequence must start at zero and remain consecutive')
        if stream not in self.state.positions and len(self.state.positions) >= self.limits.max_streams:
            raise JournalFull('session stream count bound')
        encoded = record.to_bytes()
        if len(encoded) > self.limits.max_record_bytes:
            raise ValueError('record exceeds writer byte bound')
        versions = {v for v in (record.provenance.universe_sha256, record.provenance.metadata_sha256) if v is not None}
        if len(versions) > self.segment_limits.max_versions:
            raise ValueError('single record exceeds segment provenance-version bound')
        self.tick()
        projected = self._projection(record, versions)
        if not projected[0] and self.active.state.records:
            self.rotate()
            projected = self._projection(record, versions)
        if not projected[0]:
            raise ValueError('single record exceeds segment manifest/resource bound')
        try:
            try:
                result = self.active.append(record) if sync else self.active._append(record,sync=False)
            except JournalFull:
                self.rotate()
                projected = self._projection(record, versions)
                result = self.active.append(record) if sync else self.active._append(record,sync=False)
        except JournalFull:
            raise
        except BaseException:
            self.poisoned = True
            raise
        self.state.positions[stream] = record.receipt.receive_seq
        self.state.records += 1
        self.state.valid_bytes += FRAME.size + len(encoded)
        _, row, metadata_bytes, all_versions = projected
        self._segment_streams[stream] = row
        self._stream_metadata_bytes, self._segment_versions = metadata_bytes, all_versions
        return result

    def _projection(self, record, versions):
        stream, seq = record.receipt.stream_key, record.receipt.receive_seq
        previous = self._segment_streams.get(stream)
        row = dict(stream=asdict(stream), first=seq if previous is None else previous['first'],
                   last=seq, records=1 if previous is None else previous['records'] + 1)
        metadata_bytes = self._stream_metadata_bytes + len(canonical(row)) - (len(canonical(previous)) if previous else 0)
        all_versions = self._segment_versions | versions
        stream_count = len(self._segment_streams) + (0 if previous is not None else 1)
        stream_array_bytes = metadata_bytes + max(0, stream_count - 1) + 2
        # Cover fixed hashes, WAL seal and bounded recovery-marker metadata; stream
        # identities and version hashes are accounted before durable admission.
        budget = 16384 + self.segment_limits.max_recoveries * 1024 + stream_array_bytes + 67 * len(all_versions) + 2
        fits = (len(all_versions) <= self.segment_limits.max_versions and
                budget <= self.segment_limits.max_manifest_bytes)
        return fits, row, metadata_bytes, all_versions

    def tick(self):
        self._ready()
        if self._now() - self._opened_ns >= self.descriptor['rotate_ns'] and self.active.state.records:
            return self.rotate()
        return None

    def rotate(self):
        return self._rotate(final=False)

    def _rotate(self, *, final):
        self._ready()
        if not self.active.state.records:
            return None
        ordinal = len(self.committed)
        if ordinal >= self.descriptor['max_segments'] or (not final and ordinal + 1 >= self.descriptor['max_segments']):
            raise JournalFull('session segment index bound; stop and finish this session')
        self.poisoned = True
        self.active.sync()
        destination = seal_segment(self.active, self.archive_root, _context(self.descriptor, ordinal),
                                   limits=self.segment_limits, checkpoint=self.checkpoint)
        reference = _reference(destination, ordinal)
        _mkdir(self.directory / 'committed')
        _publish_bytes(self.directory / 'committed' / (f'{ordinal:08d}.json'), canonical(reference))
        self.checkpoint('after_index')
        self.committed.append(destination)
        self.references.append(reference)
        self.active.close()
        if not final:
            self.active = Journal(self.directory / f'{ordinal + 1:08d}.wal', venue=self.identity[0],
                                  environment=self.identity[1], limits=self.limits, create=True)
            _publish_opened(self.directory,self.descriptor,ordinal+1)
            self.checkpoint('after_wal_open')
            self._opened_ns = self._now()
            self._segment_streams, self._segment_versions = {}, set()
            self._stream_metadata_bytes = 0
        self.poisoned = False
        return destination

    def encoded_records(self, *, allow_unsealed=False):
        self._ready()
        if not allow_unsealed:
            raise JournalDamaged('active rotating session is not sealed')
        for path in self.committed:
            for record in iter_segment_records(path, limits=self.segment_limits):
                yield record.to_bytes()
        yield from self.active.encoded_records(allow_unsealed=True)

    def raw_records(self, *, allow_unsealed=False):
        """Stream typed records without re-encoding already decoded segments.

        Segment verification and active WAL frame checks run on every call.
        The same serialized-owner restriction as encoded_records applies.
        """
        self._ready()
        if not allow_unsealed:
            raise JournalDamaged('active rotating session is not sealed')
        for path in self.committed:
            yield from iter_segment_records(path, limits=self.segment_limits)
        yield from self.active.raw_records(allow_unsealed=True)

    def finish(self):
        self._ready()
        try:
            self._rotate(final=True)
            _publish_bytes(self.directory / 'complete.json', _terminal_bytes(dict(schema='dc.writer-complete/2',
                session_sha256=digest(canonical(self.descriptor)), segments=self.references,
                records=self.state.records)))
        except BaseException:
            self.poisoned = True
            raise
        finally:
            self.abort()
        return tuple(self.committed)

    def abort(self):
        if self.active is not None and not self.active.closed:
            self.active.close()
        if getattr(self, '_owner', None) is not None:
            os.close(self._owner)
            self._owner = None
        self.closed = True

    def __enter__(self):
        return self

    def __exit__(self, kind, value, traceback):
        if self.closed:
            return
        if kind is None:
            self.finish()
        else:
            self.abort()


def _check_reference(row, ordinal, descriptor):
    if not isinstance(row, dict) or set(row) != {'ordinal', 'context_sha256', 'manifest_sha256'}:
        raise ValueError('unknown writer commit reference')
    integer(row['ordinal'], 'segment ordinal')
    if row['ordinal'] != ordinal or row['context_sha256'] != _context(descriptor, ordinal).context_id:
        raise ValueError('writer commit reference identity mismatch')
    sha256(row['manifest_sha256'], 'manifest_sha256')


def _recovery_evidence(directory, descriptor, paths):
    indexes = []
    for path in (directory / 'committed').glob('*.json'):
        if len(indexes) >= descriptor['max_segments']:
            raise ValueError('writer commit index count bound')
        indexes.append(path)
    indexes.sort()
    if [p.name for p in indexes] != [f'{i:08d}.json' for i in range(len(indexes))]:
        raise ValueError('writer commit index prefix missing')
    references = []
    for ordinal, path in enumerate(indexes):
        row = _read_json(path, 2048)
        _check_reference(row, ordinal, descriptor)
        references.append(row)
    terminals = []
    for name, schema in [('complete.json', 'dc.writer-complete/2'), ('recovered.json', 'dc.writer-recovered/2')]:
        path = directory / name
        if not path.exists():
            continue
        value = _read_json(path, MAX_TERMINAL_BYTES)
        keys = {'schema', 'session_sha256', 'segments', 'records'} | ({'continuity'} if name == 'recovered.json' else set())
        if not isinstance(value, dict) or set(value) != keys or value['schema'] != schema or (
                value['session_sha256'] != digest(canonical(descriptor))):
            raise ValueError('writer terminal identity mismatch')
        if name == 'recovered.json' and value['continuity'] != 'requires_new_session_and_interruption_controls':
            raise ValueError('unknown recovered continuity policy')
        integer(value['records'], 'terminal records')
        if not isinstance(value['segments'], list) or len(value['segments']) > descriptor['max_segments']:
            raise ValueError('writer terminal segment bound')
        for ordinal, row in enumerate(value['segments']):
            _check_reference(row, ordinal, descriptor)
            if ordinal < len(references) and references[ordinal] != row:
                raise ValueError('writer index/terminal reference conflict')
        if len(references) > len(value['segments']):
            raise ValueError('writer terminal omits committed segment')
        terminals.append(value)
    required = max([len(references)] + [len(v['segments']) for v in terminals])
    if len(paths) < required:
        raise ValueError('committed WAL dependency missing; retain and verify the immutable archive separately')
    return references, terminals


def recover_session(directory, archive_root, *, checkpoint=lambda stage: None,require_opened=False):
    """Seal preserved WAL prefixes, never reopen the previous ingress session.

    Original corrupt tails and seal/commit retries use the journal/segment evidence
    rules. Returned recovery is an archive fact, not proof of market continuity.
    """
    directory = Path(directory)
    owner = _lock(directory)
    try:
        with (directory / 'session.json').open('rb') as source:
            encoded = source.read(65537)
        if len(encoded) > 65536:
            raise ValueError('writer session metadata bound')
        descriptor = strict_json(encoded)
        identity = descriptor['identity']
        expected = _descriptor(identity['venue'], identity['environment'], identity['capture_id'], identity['process_session_id'],
            provenance=Provenance(**descriptor['provenance']), started_utc_ns=descriptor['started_utc_ns'],
            limits=JournalLimits(**descriptor['journal_limits']), segment_limits=SegmentLimits(**descriptor['segment_limits']),
            rotate_ns=descriptor['rotate_ns'], max_segments=descriptor['max_segments'])
        if descriptor != expected or canonical(descriptor) != encoded or directory.name != descriptor['session_id']:
            raise ValueError('writer session descriptor identity mismatch')
        paths = []
        for path in directory.glob('*.wal'):
            if len(paths) >= descriptor['max_segments']:
                raise ValueError('recovery segment count bound')
            paths.append(path)
        paths.sort()
        if [p.name for p in paths] != [f'{i:08d}.wal' for i in range(len(paths))]:
            raise ValueError('missing or foreign WAL in session')
        indexed, terminals = _recovery_evidence(directory, descriptor, paths)
        proofs=[]
        for path in (directory/'opened').glob('*.json'):
            if len(proofs)>=descriptor['max_segments']:raise ValueError('opened WAL proof count bound')
            proofs.append(path)
        proofs.sort()
        if [p.name for p in proofs]!=[f'{i:08d}.json' for i in range(len(proofs))]:
            raise ValueError('opened WAL proof prefix missing')
        for ordinal,path in enumerate(proofs):
            if _read_json(path,2048)!=strict_json(_opened_bytes(descriptor,ordinal)):raise ValueError('opened WAL proof differs')
            if ordinal>=len(paths):raise ValueError('opened WAL dependency missing')
        if require_opened and not proofs:raise ValueError('opened WAL proof required')
        # Seals and preserved loss evidence must never silently lose their WAL.
        for path in directory.iterdir():
            name=path.name
            if len(name)>13 and name[:8].isdigit() and name[8:13]=='.wal.' and not (directory/name[:12]).exists():
                raise ValueError('orphan WAL seal/recovery evidence')
        if require_opened and len(proofs)<len(paths):
            # A creator may crash after the empty WAL fsync but before its proof.
            # No writer exposed this ordinal, so only a clean empty suffix is safe.
            if len(paths)!=len(proofs)+1:raise ValueError('unproved WAL suffix')
            with Journal(paths[-1],venue=identity['venue'],environment=identity['environment'],
                         limits=JournalLimits(**descriptor['journal_limits'])) as journal:
                if journal.state.records or journal.state.reason or journal.seal_binding is not None:
                    raise ValueError('unproved WAL contains data or seal evidence')
            _publish_opened(directory,descriptor,len(proofs))
        complete = next((v for v in terminals if v['schema'] == 'dc.writer-complete/2'), None)
        references, destinations, total, positions = [], [], 0, {}
        for ordinal, path in enumerate(paths):
            with Journal(path, venue=identity['venue'], environment=identity['environment'],
                         limits=JournalLimits(**descriptor['journal_limits'])) as journal:
                if (complete is not None and ordinal == len(complete['segments']) and ordinal == len(paths) - 1
                        and ordinal >= len(indexed) and not journal.state.records and not journal.state.reason):
                    continue # clean unused WAL after the last graceful rotation
                journal.recover_tail(checkpoint=checkpoint)
                for encoded_record in journal.encoded_records(allow_unsealed=True):
                    record = RawRecord.from_bytes(encoded_record)
                    stream = record.receipt.stream_key
                    if record.receipt.receive_seq != positions.get(stream, -1) + 1:
                        raise ValueError('recovered session receipt prefix missing or conflicting')
                    if stream not in positions and len(positions) >= descriptor['journal_limits']['max_streams']:
                        raise ValueError('recovered session stream bound')
                    positions[stream] = record.receipt.receive_seq
                destination = seal_segment(journal, _archive_root(archive_root, descriptor), _context(descriptor, ordinal),
                                           limits=SegmentLimits(**descriptor['segment_limits']), checkpoint=checkpoint)
                reference = _reference(destination, ordinal)
                expected = [rows[ordinal] for rows in [indexed] + [v['segments'] for v in terminals] if ordinal < len(rows)]
                if any(row != reference for row in expected):
                    raise ValueError('recovered segment differs from committed evidence')
                _mkdir(directory / 'committed')
                _publish_bytes(directory / 'committed' / (f'{ordinal:08d}.json'), canonical(reference))
                checkpoint('after_index')
                references.append(reference)
                destinations.append(destination)
                total += journal.state.records
        for terminal in terminals:
            if terminal['segments'] != references or terminal['records'] != total:
                raise ValueError('recovered prefix differs from durable terminal evidence')
        if require_opened:
            expected={path.name for path in destinations}
            archive_segments=_archive_root(archive_root,descriptor)/'segments'
            for path in archive_segments.iterdir() if archive_segments.exists() else ():
                if path.name not in expected:raise ValueError('unindexed immutable archive evidence')
        result = dict(schema='dc.writer-recovered/2', session_sha256=digest(encoded), segments=references,
                      records=total, continuity='requires_new_session_and_interruption_controls')
        _publish_bytes(directory / 'recovered.json', _terminal_bytes(result))
        return tuple(destinations)
    finally:
        os.close(owner)
