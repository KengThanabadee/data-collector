import asyncio
from datetime import datetime, timezone
import json
from pathlib import Path
import shutil
import tempfile
import unittest
from unittest.mock import patch

from aiohttp import web

from data_collector.minute import capture_minutes, read_minute
from data_collector.minute_storage import (
    HourlyJournal, _compress, confirm_offsite_copy, prune_minute_day, seal_minute_day, verify_minute_day,
)
from data_collector.minute_reference import ReferenceAttempts
from data_collector.minute_tickers import (
    Instrument, TickerSampler, _Control, read_deribit_metadata_failures, read_deribit_retirements,
)
from data_collector.minute_files import body_fields, dumps
from data_collector.minute_config import Config
from tests.helpers import DERIBIT_MINUTE_REPLIES as REPLIES


DAY = "2026-11-08"
FIRST = int(datetime(2026, 11, 8, 23, 58, tzinfo=timezone.utc).timestamp())
TODAY = FIRST + 120


class MinuteStorageTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name)
        self.config = Config(data_root=self.root, attempts=1)
        self.wall = FIRST - 10
        self.clock = 0.
        self.calls = 0
        app = web.Application()
        app.router.add_get("/public/{method}", self.reply)
        runner = web.AppRunner(app)
        await runner.setup()
        self.addAsyncCleanup(runner.cleanup)
        site = web.TCPSite(runner, "127.0.0.1", 0)
        await site.start()
        url = f"http://127.0.0.1:{runner.addresses[0][1]}/"
        self.enterContext(patch.object(Config, "base_url", property(lambda _: url)))
        self.hourly = self.root / "hourly" / DAY / "23.jsonl"
        self.wal = self.root / "work" / str(FIRST) / "deribit" / "journal.jsonl"
        self.sealed = self.root / "daily" / f"{DAY}.jsonl.zst"
        self.manifest = self.root / "daily" / f"{DAY}.manifest.json"

    async def reply(self, request):
        self.calls += 1
        key = request.match_info["method"], tuple(sorted(request.query.items()))
        body = REPLIES.get(key)
        return web.Response(body=body, status=200 if body is not None else 404)

    async def sleep(self, seconds):
        self.wall += seconds
        self.clock += seconds
        await asyncio.sleep(0)

    async def capture(self, minutes):
        await capture_minutes(self.config, minutes=minutes, wall=lambda: self.wall,
                              clock=lambda: self.clock, sleep=self.sleep)

    async def test_seals_verified_day_and_keeps_recent_journals_until_confirmation(self):
        await self.capture(2)
        before = read_minute(self.root, FIRST)
        self.assertEqual(len(self.hourly.read_bytes().splitlines()), 2)
        self.assertTrue(self.wal.is_file())

        manifest = seal_minute_day(self.root, DAY)
        self.assertEqual(manifest["minutes"], 2)
        self.assertEqual(manifest["first_slot"], FIRST)
        self.assertEqual(manifest["last_slot"], FIRST + 60)
        self.assertEqual(verify_minute_day(self.root, DAY), manifest)
        self.assertTrue(self.sealed.is_file())
        self.assertTrue(self.hourly.is_file())
        self.assertTrue(self.wal.is_file())

        with self.assertRaisesRegex(ValueError, "offsite"):
            prune_minute_day(self.root, DAY, now=TODAY)
        with self.assertRaises(ValueError):
            confirm_offsite_copy(self.root, DAY, "0" * 64)
        self.assertTrue(self.hourly.is_file())
        self.assertTrue(self.wal.is_file())

        confirm_offsite_copy(self.root, DAY, manifest["archive_sha256"])
        self.assertGreater(prune_minute_day(self.root, DAY, now=TODAY), 0)
        self.assertFalse(self.hourly.exists())
        self.assertFalse(self.wal.exists())
        self.assertEqual(read_minute(self.root, FIRST), before)
        self.assertEqual(prune_minute_day(self.root, DAY, now=TODAY), 0)

    async def test_minute_read_uses_verified_seal_when_prune_removes_staged_files(self):
        await self.capture(2)
        before = read_minute(self.root, FIRST)
        seal_minute_day(self.root, DAY)
        staged_index = next((self.root / "minutes").glob("**/2358/index.jsonl"))
        is_file = Path.is_file
        pruned = False

        def interleave_prune(path):
            nonlocal pruned
            present = is_file(path)
            if path == staged_index and present and not pruned:
                pruned = True
                self.assertGreater(prune_minute_day(self.root, DAY, now=TODAY + 3 * 86400), 0)
            return present

        with patch.object(Path, "is_file", interleave_prune):
            self.assertEqual(read_minute(self.root, FIRST), before)
        self.assertTrue(pruned)
        self.assertFalse(staged_index.exists())

    async def test_interrupted_publication_and_manifest_mismatch_preserve_local_evidence(self):
        await self.capture(2)
        hourly = self.hourly.read_bytes()
        wal = self.wal.read_bytes()

        def interrupt(phase):
            if phase == "after_archive":
                raise InterruptedError("simulated termination after archive publication")

        with self.assertRaises(InterruptedError):
            seal_minute_day(self.root, DAY, checkpoint=interrupt)
        self.assertTrue(self.sealed.is_file())
        self.assertFalse(self.manifest.exists())
        self.assertEqual(self.hourly.read_bytes(), hourly)
        self.assertEqual(self.wal.read_bytes(), wal)
        manifest = seal_minute_day(self.root, DAY)
        self.assertEqual(verify_minute_day(self.root, DAY), manifest)

        self.manifest.write_bytes(dumps(dict(manifest, minutes=1)))
        with self.assertRaises(ValueError):
            prune_minute_day(self.root, DAY, now=TODAY + 3 * 86400)
        self.assertEqual(self.hourly.read_bytes(), hourly)
        self.assertEqual(self.wal.read_bytes(), wal)
        self.manifest.write_bytes(dumps(manifest))
        self.assertGreater(prune_minute_day(self.root, DAY, now=TODAY + 3 * 86400), 0)
        self.assertFalse(self.hourly.exists())

    async def test_rollover_seals_yesterday_and_leaves_today_open(self):
        await self.capture(3)
        self.assertEqual(verify_minute_day(self.root, DAY)["minutes"], 2)
        self.assertFalse((self.root / "daily" / "2026-11-09.jsonl.zst").exists())
        with self.assertRaisesRegex(ValueError, "open"):
            seal_minute_day(self.root, "2026-11-09")
        self.assertEqual(self.calls, 33)

    async def test_seal_preserves_deribit_evidence_written_after_the_last_journal_entry(self):
        await self.capture(1)
        slot = FIRST + 60
        sampler = TickerSampler(self.root, "ws://unused", "production", wall=lambda: self.wall)
        sampler.connection = "fixture-connection"
        listing = Instrument(1, FIRST * 1000, slot * 1000)
        control = _Control(10, "public/unsubscribe", {"BTC-9NOV26-76000-C": listing}, slot + 1, 0)
        sampler._record_retirement(control, "requested", slot + 1)
        await self.capture(1)
        hourly = self.hourly.read_bytes()

        # Rollover can begin while the prior-day unsubscribe is still in flight.
        async with HourlyJournal(self.root) as journal:
            sampler.retirements_idle.clear()
            try:
                await journal.seal_due(TODAY, sampler.retirements_idle)
                await asyncio.sleep(0)
                self.assertFalse(journal.seal_task.done())
                self.assertFalse(self.manifest.exists())
                sampler._record_retirement(control, "acknowledged", slot + 8, b'{"id":10}')
                attempt = ReferenceAttempts("production", "deribit", slot)
                reply = dict(http_status=429, **body_fields(b'{"error":{"code":10028}}'))
                sampler._write_metadata_failure(attempt, slot, "error", [reply])
            finally:
                sampler.retirements_idle.set()

        self.assertEqual(self.hourly.read_bytes(), hourly)
        manifest = verify_minute_day(self.root, DAY)
        self.assertEqual(manifest["schema_version"], 2)
        self.assertIn("late_references_sha256", manifest)
        restored = self.root / "restored"
        shutil.copytree(self.root / "daily", restored / "daily")
        controls = read_deribit_retirements(restored, slot)
        self.assertEqual([control["status"] for control in controls], ["requested", "acknowledged"])
        self.assertEqual(read_deribit_metadata_failures(restored, slot)[0]["responses"], [reply])
        self.assertEqual(read_minute(restored, slot), read_minute(self.root, slot))
        self.assertGreater(prune_minute_day(self.root, DAY, now=TODAY + 3 * 86400), 0)

    async def test_recovers_pre_upgrade_archive_without_replacing_its_bytes(self):
        await self.capture(2)
        self.sealed.parent.mkdir()
        _compress([self.hourly], self.sealed, b"")
        legacy_archive = self.sealed.read_bytes()
        slot = FIRST + 60
        sampler = TickerSampler(self.root, "ws://unused", "production", wall=lambda: self.wall)
        attempt = ReferenceAttempts("production", "deribit", slot)
        reply = dict(http_status=429, **body_fields(b'{"error":{"code":10028}}'))
        sampler._write_metadata_failure(attempt, slot, "error", [reply])

        manifest = seal_minute_day(self.root, DAY)
        self.assertEqual(self.sealed.read_bytes(), legacy_archive)
        self.assertIn("recovered_late_references", manifest)
        restored = self.root / "restored"
        shutil.copytree(self.root / "daily", restored / "daily")
        self.assertEqual(read_deribit_metadata_failures(restored, slot)[0]["responses"], [reply])
        self.assertEqual(verify_minute_day(restored, DAY), manifest)

    async def test_failed_background_seal_preserves_evidence_and_continues_across_restarts(self):
        await self.capture(2)
        broken_journal = self.hourly.read_bytes() + b'{"torn":'
        self.hourly.write_bytes(broken_journal)
        wal = self.wal.read_bytes()

        with self.assertLogs("data_collector.minute_storage", level="ERROR") as errors:
            await self.capture(2)
            await self.capture(1)

        self.assertEqual(len(errors.records), 2)
        self.assertTrue(all(DAY in entry.message for entry in errors.records))
        for slot in (TODAY, TODAY + 60, TODAY + 120):
            self.assertEqual(len(read_minute(self.root, slot)[1]), 11)
        self.assertEqual(self.hourly.read_bytes(), broken_journal)
        self.assertEqual(self.wal.read_bytes(), wal)
        self.assertFalse(self.manifest.exists())
        with self.assertRaisesRegex(ValueError, "incomplete hourly journal"):
            seal_minute_day(self.root, DAY)

    async def test_replays_cursor_without_resending_and_keeps_torn_journal_bytes(self):
        await self.capture(1)
        original = self.hourly.read_bytes()
        state_path = self.root / "minute-grid.json"
        state = json.loads(state_path.read_bytes())
        state["next_slot"] = FIRST
        state_path.write_bytes(dumps(state))
        torn = b'{"schema_version":1,"files":'
        with self.hourly.open("ab") as file:
            file.write(torn)

        await self.capture(1)
        self.assertEqual(self.calls, 11)
        self.assertEqual(self.hourly.read_bytes(), original)
        partials = list(self.hourly.parent.glob("*.partial"))
        self.assertEqual(len(partials), 1)
        self.assertEqual(partials[0].read_bytes(), torn)
        self.assertTrue(self.wal.is_file())

    async def test_missing_initial_hour_cannot_seal_a_shorter_day(self):
        self.wall = FIRST - 59 * 60 - 10
        await self.capture(61)
        first_hour = self.hourly.with_name("22.jsonl")
        self.assertTrue(first_hour.exists())
        first_hour.rename(first_hour.with_suffix(".held"))
        with self.assertRaisesRegex(ValueError, "journal"):
            seal_minute_day(self.root, DAY)
        self.assertFalse(self.manifest.exists())
        self.assertTrue(first_hour.with_suffix(".held").is_file())
