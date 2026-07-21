import os
import unittest
from datetime import datetime, timezone
from unittest.mock import patch

from pipelines.listing.main import _build_heartbeat_slack_message, _heartbeat_scheduler_status


class HeartbeatSlackMarketDiscoveryTests(unittest.TestCase):
    """Slack heartbeat should include safe buy-signal lane guidance."""

    def _base_metrics(self):
        return {
            "listing_counts": {"live": 42},
            "comps": {"comp_keys": 12, "comp_samples": 100},
            "live_comp_coverage": {
                "live_total": 42,
                "live_with_3plus_comps": 10,
            },
            "paper_trading": {
                "watching": 3,
                "closed": 2,
                "sold": 1,
                "profitable_on_paper": 0,
            },
            "api_total": 123,
            "market_discovery": {
                "status_counts": {
                    "data_gap": 5,
                    "parked": 97,
                    "rejected": 45,
                    "buy_candidate": 0,
                },
                "top_lanes": [
                    {
                        "source": "ebay-apple",
                        "model_key": "apple-macbook-pro-13in-intel-16gb-512gb_b",
                        "status": "data_gap",
                        "score": 61.92,
                        "estimated_net_spread": 47.64,
                    }
                ],
            },
        }

    def test_market_discovery_summary_is_observer_only(self):
        metrics = self._base_metrics()

        text, blocks = _build_heartbeat_slack_message(
            {},
            metrics,
            datetime(2026, 5, 28, 12, 0, tzinfo=timezone.utc),
        )
        rendered = f"{text}\n{blocks}"

        self.assertIn("Buy signals", rendered)
        self.assertIn("0 buy candidates, 5 data gaps, 97 parked, 45 rejected", rendered)
        self.assertIn("apple-macbook-pro-13in-intel-16gb-512gb_b", rendered)
        self.assertIn("improve model keys and confirmed final-price coverage", rendered)
        self.assertNotIn("BUY", rendered)
        self.assertNotIn("BID", rendered)
        self.assertNotIn("ACTIONABLE", rendered)

    def test_scheduler_status_is_in_hourly_slack_message(self):
        metrics = self._base_metrics()
        state = {
            "scheduler_status_out": {
                "status": "running",
                "expected_label": "com.ghostfrog.langgraph-ebay.python-heartbeat",
                "observed_label": "com.ghostfrog.langgraph-ebay.python-heartbeat",
                "schedule_minutes": 5,
                "checked_at": "2026-06-03T13:00:00+00:00",
                "message": "running via launchd every 5 min",
            }
        }

        text, blocks = _build_heartbeat_slack_message(
            state,
            metrics,
            datetime(2026, 6, 3, 13, 0, tzinfo=timezone.utc),
        )
        rendered = f"{text}\n{blocks}"

        self.assertIn("Scheduler running: running via launchd every 5 min", rendered)
        self.assertIn("Scheduler", rendered)
        self.assertIn("Running: running via launchd every 5 min", rendered)
        self.assertNotIn("BUY", rendered)
        self.assertNotIn("BID", rendered)
        self.assertNotIn("ACTIONABLE", rendered)

    def test_scheduler_status_checks_launchd_wrapper_marker(self):
        with patch.dict(
            os.environ,
            {"GF_HEARTBEAT_LAUNCHED_BY": "launchd", "XPC_SERVICE_NAME": "0"},
            clear=False,
        ):
            status = _heartbeat_scheduler_status(
                datetime(2026, 6, 3, 13, 0, tzinfo=timezone.utc)
            )

        self.assertEqual(status["status"], "running")
        self.assertEqual(
            status["observed_label"],
            "com.ghostfrog.langgraph-ebay.python-heartbeat",
        )
        self.assertEqual(status["launched_by"], "launchd")
        self.assertIn("running via launchd every 5 min", status["message"])

    def test_scheduler_status_can_fallback_to_expected_xpc_label(self):
        with patch.dict(
            os.environ,
            {
                "GF_HEARTBEAT_LAUNCHED_BY": "",
                "XPC_SERVICE_NAME": "com.ghostfrog.langgraph-ebay.python-heartbeat",
            },
            clear=False,
        ):
            status = _heartbeat_scheduler_status(
                datetime(2026, 6, 3, 13, 0, tzinfo=timezone.utc)
            )

        self.assertEqual(status["status"], "running")
        self.assertEqual(
            status["observed_label"],
            "com.ghostfrog.langgraph-ebay.python-heartbeat",
        )
        self.assertIsNone(status["launched_by"])
        self.assertIn("running via launchd every 5 min", status["message"])


if __name__ == "__main__":
    unittest.main()
