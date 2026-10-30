from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import os

from data_collector.contracts import canonical,digest
from data_collector.rawquota import take_slot
from data_collector.sharded import partition,verify


class ShardedTests(unittest.TestCase):
    def test_frozen_assignment_is_disjoint_and_exhaustive(self):
        symbols=[f'BTC-X-{n}-C' for n in range(2796)]
        groups=partition(symbols,4)
        self.assertEqual(len(groups),4)
        self.assertEqual(len(set(s for g in groups for s in g)),len(symbols))
        self.assertEqual(sum(map(len,groups)),len(symbols))
        self.assertLessEqual(max(map(len,groups))-min(map(len,groups)),1)
        self.assertEqual(len(partition(['BTC-X-C'],4)),1)

    def test_shared_subscription_budget_reserves_only_one_slot(self):
        with tempfile.TemporaryDirectory() as tmp:
            path=Path(tmp)/'budget';path.write_bytes(b'0')
            with patch('data_collector.rawquota.time.monotonic_ns',return_value=100):
                with ThreadPoolExecutor(max_workers=4) as pool:
                    outcomes=list(pool.map(lambda _:take_slot(path),range(4)))
                self.assertEqual(outcomes.count(True),1)
            with patch('data_collector.rawquota.time.monotonic_ns',return_value=1_000_000_099):
                self.assertFalse(take_slot(path))
            with patch('data_collector.rawquota.time.monotonic_ns',return_value=1_000_000_100):
                self.assertTrue(take_slot(path))
            with self.assertRaises(RuntimeError):take_slot(Path(tmp)/'missing')

    def test_verifier_rejects_missing_or_overlapping_worker_coverage(self):
        with tempfile.TemporaryDirectory() as tmp:
            metadata=Path(tmp);root=metadata/'all-options';root.mkdir()
            symbols=['BTC-X-C','ETH-X-P']
            body=canonical(dict(selected=[dict(instrument=dict(native_product='option',native_symbol=s)) for s in symbols]))
            (metadata/'universe.json').write_bytes(body)
            rows=[]
            for i,symbol in enumerate(symbols):
                child=root/'workers'/str(i)/'continuous';child.mkdir(parents=True)
                channels=[f'{feed}.{symbol}.100ms' for feed in ('book','ticker','trades')]
                (child/'result.json').write_bytes(canonical(dict(symbols=[symbol],channels=channels,
                    metadata_ref=os.path.relpath(metadata,child),status='raw_capture_complete',bytes=0)))
                rows.append(dict(root=str(child.relative_to(root)),symbols=[symbol]))
            report=dict(schema='dc.raw-sharded-result/1',status='raw_capture_complete',metadata_ref='..',
                metadata_sha256=digest(body),symbols=symbols,shards=rows,frames=0,bytes=0)
            checked=dict(records=0,ws_messages=0,initial_feed_coverage_verified=True)
            with patch('data_collector.continuous.verify',return_value=checked):
                self.assertEqual(verify(root,report)['channels_planned'],6)
                with self.assertRaisesRegex(ValueError,'omits'):
                    verify(root,dict(report,shards=rows[:1]))
                with self.assertRaisesRegex(ValueError,'duplicate'):
                    verify(root,dict(report,shards=[rows[0],rows[0]]))
