import unittest

from pilot import Config, Job, Pilot, run_matrix


class PilotTests(unittest.TestCase):
    def test_all_scenarios_preserve_prefix_and_converge(self):
        for run in run_matrix()["runs"]:
            with self.subTest(
                strategy=run["strategy"], routing=run["routing"],
                scenario=run["scenario"]
            ):
                self.assertTrue(run["converged"])
                self.assertEqual(run["final_count"], 110)
                self.assertIsNotNone(run["metrics"]["final_catchup_ms"])
                for step in run["trace"]:
                    if step["action"] == "save":
                        self.assertEqual(step["count"], 100 + step["checkpoint"])

    def test_sticky_routing_does_not_force_duplicate_scans(self):
        overlap = Pilot("overlap", "sticky", "normal").run()
        owned = Pilot("owned", "sticky", "normal").run()
        self.assertEqual(overlap["metrics"]["repeated_event_reads"], 0)
        self.assertEqual(
            overlap["metrics"]["event_reads"], owned["metrics"]["event_reads"]
        )
        self.assertEqual(overlap["metrics"]["max_active_jobs"], 1)
        self.assertEqual(overlap["metrics"]["lease_renewals"], 0)
        self.assertGreater(owned["metrics"]["lease_renewals"], 0)

    def test_round_robin_can_exercise_overlap(self):
        run = Pilot("overlap", "round_robin", "normal").run()
        self.assertEqual(run["metrics"]["max_active_jobs"], 2)
        self.assertGreater(run["metrics"]["repeated_event_reads"], 0)

    def test_pause_exercises_fencing_and_unfenced_regression(self):
        owned = Pilot("owned", "round_robin", "pause").run()
        overlap = Pilot("overlap", "round_robin", "pause").run()
        self.assertGreater(owned["metrics"]["rejected_writes"], 0)
        self.assertEqual(owned["metrics"]["checkpoint_regressions"], 0)
        self.assertGreater(overlap["metrics"]["checkpoint_regressions"], 0)

    def test_old_epoch_rejected_even_if_same_worker_owns_again(self):
        pilot = Pilot("owned", "sticky", "normal")
        pilot.owner, pilot.epoch, pilot.expires = 0, 3, 100
        old = Job(101, 1, 1)
        pilot.workers[0].job = old
        pilot.finish(0, old)
        self.assertEqual(pilot.count, 100)
        self.assertEqual(pilot.metrics["rejected_writes"], 1)

    def test_expired_owner_cannot_write_before_new_grant(self):
        pilot = Pilot("owned", "sticky", "normal")
        pilot.owner, pilot.epoch, pilot.expires = 0, 1, 100
        pilot.now = 100
        job = Job(101, 1, 1)
        pilot.workers[0].job = job
        pilot.finish(0, job)
        self.assertEqual(pilot.count, 100)
        self.assertEqual(pilot.metrics["rejected_writes"], 1)

    def test_duplicate_input_does_not_increment_twice(self):
        pilot = Pilot("overlap", "sticky", "normal")
        pilot.schedule(21, lambda: pilot.accept("E1"))
        result = pilot.run()
        self.assertEqual(result["final_count"], 110)
        self.assertEqual(result["metrics"]["duplicate_inputs"], 1)

    def test_crashed_job_cannot_commit_after_restart(self):
        pilot = Pilot("overlap", "sticky", "normal")
        old = Job(101, 1, 0)
        pilot.workers[0].job = None
        pilot.finish(0, old)
        self.assertEqual(pilot.metrics["rollup_write_attempts"], 0)

    def test_incomplete_run_reports_no_final_catchup(self):
        config = Config(scan_base_ms=1000)
        result = Pilot("overlap", "sticky", "normal", config).run()
        self.assertFalse(result["converged"])
        self.assertIsNone(result["metrics"]["final_catchup_ms"])

    def test_stale_duration_integrates_until_catchup(self):
        config = Config(
            events=1, arrival_ms=20, scan_base_ms=30, scan_per_event_ms=5
        )
        result = Pilot("overlap", "sticky", "normal", config).run()
        self.assertEqual(result["metrics"]["stale_counter_ms"], 35)
        self.assertEqual(result["metrics"]["final_catchup_ms"], 35)

    def test_model_is_reproducible(self):
        self.assertEqual(run_matrix(), run_matrix())


if __name__ == "__main__":
    unittest.main()
