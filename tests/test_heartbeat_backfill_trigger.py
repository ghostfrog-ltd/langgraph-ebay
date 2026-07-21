import unittest
from unittest.mock import patch

from pipelines.listing import main


class HeartbeatBackfillTriggerTests(unittest.TestCase):
    """Heartbeat backfill should move faster while retaining budget gates."""

    def test_backfill_spawn_uses_larger_default_batch_inside_daily_budget(self):
        plan = {
            "api_budget": {"recommended_backfill_calls": 1000},
            "recommended_stale_candidate_preview": [{"id": 1}],
        }

        with patch("pipelines.listing.main.is_backfill_running", return_value=False):
            with patch("pipelines.listing.main.backfill_checks_today", return_value=0):
                with patch("pipelines.listing.main.build_backfill_plan", return_value=plan) as planner:
                    with patch("pipelines.listing.main._spawn_backfill_subprocess", return_value=1234) as spawn:
                        state = main.backfill_trigger({"blocked_services": []})

        self.assertEqual(main.BACKFILL_LIMIT_PER_TICK, 50)
        planner.assert_called_once()
        spawn.assert_called_once_with(limit=50, daily_budget=main.BACKFILL_DAILY_BUDGET)
        self.assertEqual(state["backfill_trigger_out"]["status"], "spawned")
        self.assertEqual(state["backfill_trigger_out"]["limit"], 50)
        self.assertEqual(state["backfill_trigger_out"]["daily_budget"], 1000)


if __name__ == "__main__":
    unittest.main()
