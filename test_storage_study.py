from collections import Counter
import multiprocessing as mp
from pathlib import Path
import tempfile
import unittest

from storage_study import (
    analyze, collect_children, connect, ingest, initialize, lease, save, snapshot,
)


def send_large_report(channel):
    channel.put(list(range(100_000)))
    channel.close()
    channel.join_thread()


class StorageTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.path = Path(self.directory.name) / "test.sqlite"
        initialize(self.path, 2)
        self.db = connect(self.path)

    def tearDown(self):
        self.db.close()
        self.directory.cleanup()

    def test_retries_and_counter_isolation(self):
        ingest(self.db, [(1, 0), (2, 1), (3, 0)])
        ingest(self.db, [(1, 0), (3, 0)])
        total, checkpoint, identities = snapshot(self.db, 0)
        self.assertEqual((total, checkpoint, identities), (2, 3, [1, 3]))
        self.assertEqual(snapshot(self.db, 1), (1, 2, [2]))

    def test_replaced_owner_is_fenced_by_real_transaction(self):
        metrics = Counter()
        ingest(self.db, [(1, 0)])
        old = lease(self.db, 0, 0, 10**9, metrics)
        self.db.execute("UPDATE leases SET expires=0")
        new = lease(self.db, 0, 1, 10**9, metrics)
        self.assertGreater(new, old)
        self.assertFalse(save(self.db, 0, 1, 1, 0, old, metrics))
        self.assertTrue(save(self.db, 0, 1, 1, 1, new, metrics))
        self.assertEqual(self.db.execute("SELECT count(*) FROM commits").fetchone()[0], 1)

    def test_expiry_alone_rejects_write(self):
        metrics = Counter()
        epoch = lease(self.db, 0, 0, 10**9, metrics)
        self.db.execute("UPDATE leases SET expires=0")
        self.assertFalse(save(self.db, 0, 0, 0, 0, epoch, metrics))

    def test_live_owner_prevents_other_grant(self):
        metrics = Counter()
        first = lease(self.db, 0, 0, 10**9, metrics)
        self.assertIsNone(lease(self.db, 0, 1, 10**9, metrics))
        self.assertEqual(lease(self.db, 0, 0, 10**9, metrics), first)

    def test_overlapping_writes_replace_instead_of_increment(self):
        metrics = Counter()
        ingest(self.db, [(1, 0), (2, 0)])
        total, checkpoint, _ = snapshot(self.db, 0)
        save(self.db, 0, total, checkpoint, 0, None, metrics)
        save(self.db, 0, total, checkpoint, 1, None, metrics)
        self.assertEqual(snapshot(self.db, 0), (2, 2, []))

    def test_regression_is_detected_and_repaired(self):
        metrics = Counter()
        ingest(self.db, [(1, 0)])
        old_total, old_checkpoint, _ = snapshot(self.db, 0)
        ingest(self.db, [(2, 0)])
        save(self.db, 0, 2, 2, 1, None, metrics)
        save(self.db, 0, old_total, old_checkpoint, 0, None, metrics)
        self.assertEqual(metrics["regressions"], 1)
        self.assertEqual(snapshot(self.db, 0), (2, 2, [2]))

    def test_progress_guard_rejects_older_without_ownership(self):
        metrics = Counter()
        ingest(self.db, [(1, 0), (2, 0)])
        self.assertTrue(save(self.db, 0, 2, 2, 1, None, metrics, True))
        self.assertFalse(save(self.db, 0, 1, 1, 0, None, metrics, True))
        self.assertEqual(snapshot(self.db, 0), (2, 2, []))
        self.assertEqual(metrics["superseded_writes"], 1)
        self.assertEqual(self.db.execute("SELECT count(*) FROM leases").fetchone()[0], 0)

    def test_progress_guard_allows_equal_immutable_checkpoint(self):
        metrics = Counter()
        ingest(self.db, [(1, 0)])
        self.assertTrue(save(self.db, 0, 1, 1, 0, None, metrics, True))
        self.assertTrue(save(self.db, 0, 1, 1, 1, None, metrics, True))
        self.assertEqual(snapshot(self.db, 0), (1, 1, []))

    def test_stable_visibility_includes_later_regression(self):
        samples = [
            {"at_ns": 10_000_000, "checkpoints": [0]},
            {"at_ns": 20_000_000, "checkpoints": [1]},
            {"at_ns": 30_000_000, "checkpoints": [0]},
            {"at_ns": 40_000_000, "checkpoints": [1]},
        ]
        metrics = analyze(samples, [(1, 0, 5_000_000)], 1)
        self.assertEqual(metrics["stable_visibility_p95_ms"], 35)
        self.assertEqual(metrics["oldest_missing_max_ms"], 25)

    def test_never_visible_event_is_failure_not_missing_sample(self):
        with self.assertRaises(AssertionError):
            analyze([{"at_ns": 20, "checkpoints": [0]}], [(1, 0, 10)], 1)

    def test_large_child_report_drained_before_join(self):
        ctx = mp.get_context("spawn")
        channel = ctx.Queue()
        process = ctx.Process(target=send_large_report, args=(channel,))
        process.start()
        try:
            reports = collect_children([process], channel, timeout=5)
            self.assertEqual(reports, [list(range(100_000))])
            self.assertEqual(process.exitcode, 0)
        finally:
            if process.is_alive():
                process.terminate()
            process.join()
            channel.close()


if __name__ == "__main__":
    unittest.main()
