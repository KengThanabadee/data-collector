"""Measure real managed-ledger pin costs before adding incremental state.

Synthetic one-instrument Deribit history in a real fsynced SessionWriter. No
network, cutoff publication, throughput, full-universe or deployment claim.
"""
import argparse
import cProfile
from datetime import datetime, timezone
from pathlib import Path
import platform
import pstats
import statistics
import sys
import tempfile
from time import perf_counter

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from data_collector.contracts import InstrumentKey, Provenance, RawRecord, Receipt, StreamKey, canonical, digest
from data_collector.cutoffs import IngressWriter
from data_collector.ledger import DurableLedger
from data_collector.metadata import MetadataVersion
from data_collector.owners import OwnerPlans
from data_collector.replay import BookBinding, SnapshotPlan
from data_collector.validity import FeedTarget
from data_collector.writer import SessionWriter


def profile_rows(profile):
    rows = []
    for (filename, line, name), (primitive, calls, own, cumulative, _) in pstats.Stats(profile).stats.items():
        path = Path(filename)
        if path.parent.name != 'data_collector':
            continue
        rows.append(dict(function=f'{path.name}:{name}', line=line, calls=calls,
                         own_seconds=own, cumulative_seconds=cumulative))
    return sorted(rows, key=lambda row: row['cumulative_seconds'], reverse=True)


def probe(count, repeats):
    with tempfile.TemporaryDirectory(prefix='dc-ledger-refresh-') as directory:
        root = Path(directory)
        instrument = InstrumentKey('deribit', 'production', 'option', 'BTC-SYNTHETIC-C', 'listing-1')
        ws = StreamKey('capture', 'process', 'ws')
        meta = StreamKey('capture', 'process', 'meta')
        attrs = dict(base_currency='BTC', amount_unit='native', quote_currency='native')
        receipt = Receipt('capture', 'process', 'meta', 0, 1000, 1000, 'epoch')
        metadata = MetadataVersion(instrument, receipt, 1000, canonical(attrs))
        binding = BookBinding(FeedTarget(instrument, 'deribit_full', 'dc-owner/1:'+'1'*32),
                              ws, meta, metadata.version_id)
        plan = SnapshotPlan((binding,), 'b'*64)
        provenance = Provenance('a'*64, 'b'*64, plan.dependencies['universe'])
        writer = SessionWriter(root/'wal', root/'archive', venue='deribit', environment='production',
            capture_id='capture', process_session_id='process', provenance=provenance,
            started_utc_ns=900, now_ns=lambda: 0)
        try:
            args = dict(capture_scope='scope', capture_id='capture', process_session_id='process',
                        provenance=provenance)
            ingress = IngressWriter(writer, 'capture', 'process')
            ledger = DurableLedger(writer, ledger_id='ledger', **args)
            ledger.initialize(utc_ns=900, monotonic_ns=900, clock_epoch='epoch')
            ingress.append(RawRecord(receipt, 'deribit', 'production', 'option', 'metadata',
                'metadata_version', canonical(dict(schema='dc.metadata/2', effective_at_ns=1000,
                attributes=attrs, supersedes=None)), provenance, instrument))
            OwnerPlans(writer, **args).declare(plan, utc_ns=1001, monotonic_ns=1001, clock_epoch='epoch')
            for sequence in range(count):
                data = dict(type='snapshot' if sequence == 0 else 'change', timestamp=sequence,
                    instrument_name=instrument.native_symbol, change_id=sequence+1,
                    bids=[['new' if sequence == 0 else 'change', '100', str(sequence % 9+1)]],
                    asks=[['new', '101', '3']] if sequence == 0 else [])
                if sequence:
                    data['prev_change_id'] = sequence
                body = canonical(dict(jsonrpc='2.0', method='subscription',
                    params=dict(channel=binding.native_channel, data=data)))
                ingress.append(RawRecord(Receipt('capture', 'process', 'ws', sequence,
                    1100+sequence, 1100+sequence, 'epoch'), 'deribit', 'production', 'option',
                    binding.native_channel, 'ws_message', body, provenance, instrument))
            # Include an immutable segment, not just the active WAL. Setup/fsync
            # costs are excluded from pin timing; filesystem caches are warm.
            writer.rotate()
            expected = ledger.pin()
            samples = []
            for _ in range(repeats):
                started = perf_counter()
                actual = ledger.pin()
                samples.append(perf_counter()-started)
                if actual != expected:
                    raise RuntimeError('unchanged raw produced a different ledger pin')
            profiler = cProfile.Profile()
            profiler.enable()
            actual = ledger.pin()
            profiler.disable()
            if actual != expected:
                raise RuntimeError('profiled pin differs')
            rows = profile_rows(profiler)
            calls = sum(row['calls'] for row in rows if row['function'] == 'replay.py:_book_message')
            if calls != count:
                raise RuntimeError(f'expected {count} native dispatches, observed {calls}')
            return dict(native_events=count, physical_records=ledger._scan_records,
                raw_encoded_bytes=ledger._scan_bytes, unchanged_pin_seconds=samples,
                unchanged_pin_median_seconds=statistics.median(samples),
                profiled_native_dispatches=calls, profiled_functions=rows[:20],
                profiled_native_functions=[row for row in rows if row['function'] == 'replay.py:_book_message'
                    or row['function'].startswith(('books.py:', 'decoders.py:'))], pin_unchanged=True)
        finally:
            writer.abort()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--counts', type=int, nargs='+', default=[100, 1000, 10000])
    parser.add_argument('--repeats', type=int, default=3)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    if not 1 <= len(args.counts) <= 4 or any(not 1 <= n <= 20000 for n in args.counts) or not 1 <= args.repeats <= 5:
        parser.error('use 1-4 counts in 1-20000 and 1-5 repeats')
    rows = []
    for count in args.counts:
        row = probe(count, args.repeats)
        rows.append(row)
        print(canonical({k: v for k, v in row.items() if not k.startswith('profiled_') or k == 'profiled_native_dispatches'}).decode(), flush=True)
    root = Path(__file__).resolve().parents[1]
    sources = ['experiments/ledger_refresh_probe.py', 'data_collector/ledger.py',
        'data_collector/discoveries.py', 'data_collector/owners.py', 'data_collector/writer.py',
        'data_collector/segments.py', 'data_collector/journal.py', 'data_collector/contracts.py',
        'data_collector/books.py', 'data_collector/decoders.py', 'data_collector/replay.py']
    result = dict(schema='dc.ledger-refresh-probe/1', run_utc=datetime.now(timezone.utc).isoformat(),
        scope='synthetic single-instrument warm local immutable segment; unchanged managed ledger pins only',
        limitations='profiled times include profiler overhead and overlap; not comparable to unprofiled samples; '
            'no capture throughput, queue latency, publication, full-universe or deployment measurement',
        python=sys.version, platform=platform.platform(), machine=platform.machine(),
        source_sha256={name: digest((root/name).read_bytes()) for name in sources}, rows=rows)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_bytes(canonical(result)+b'\n')


if __name__ == '__main__':
    main()
