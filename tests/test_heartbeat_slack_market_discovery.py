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
            "api_total": 123,
            "market_discovery": {
                "summary": {
                    "potentials": 1,
                    "checked": 12,
                    "live_rows_with_comps": 20,
                },
                "potentials": [
                    {
                        "source": "ebay-apple",
                        "external_id": "v1|206446095377|0",
                        "title": "Apple MacBook Air M2 16GB 512GB",
                        "url": "https://example.test/macbook-air",
                        "sale_type": "auction",
                        "end_time": "2026-05-28T18:30:00+00:00",
                        "model_key": "apple-macbook-pro-13in-intel-16gb-512gb_b",
                        "current_price": 120.0,
                        "max_safe_buy": 190.0,
                        "expected_profit": 70.0,
                        "sold_median": 250.0,
                        "comp_samples": 15,
                        "condition_grade": "B",
                    }
                ],
                "closest": [
                    {
                        "source": "ebay-tools",
                        "external_id": "v1|111111111111|0",
                        "title": "Mafell A 12 Drill",
                        "url": "https://example.test/mafell",
                        "current_price": 160.0,
                        "max_safe_buy": 170.0,
                        "expected_profit": 45.0,
                        "sold_median": 230.0,
                        "comp_samples": 15,
                    }
                ],
            },
            "market_discovery_punt": {
                "potentials": [
                    {
                        "source": "ebay-consoles",
                        "external_id": "v1|222222222222|0",
                        "title": "Nintendo Switch Console",
                        "url": "https://example.test/switch",
                        "current_price": 90.0,
                        "max_safe_buy": 120.0,
                        "expected_profit": 30.0,
                        "sold_median": 165.0,
                        "comp_samples": 15,
                    }
                ],
                "closest": [],
            },
            "listing_favourites": [
                {
                    "source": "ebay-apple",
                    "external_id": "v1|333333333333|0",
                    "title": "Favourite iPad Pro",
                    "url": "https://example.test/ipad",
                    "status": "live",
                    "price_current": 199.0,
                    "model_key": "apple-ipad-pro-favourite",
                }
            ],
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
        self.assertIn("1 potentials, 12 checked, 20 live rows with comps", rendered)
        self.assertIn("GBP 50 Profit Candidates", rendered)
        self.assertIn("Apple MacBook Air M2", rendered)
        self.assertIn("Current £120.00 | max £190.00 | profit £70.00", rendered)
        self.assertIn("review: `up v1|206446095377|0` / `down v1|206446095377|0`", rendered)
        self.assertIn("GBP 25 Profit Candidates", rendered)
        self.assertIn("Nintendo Switch Console", rendered)
        self.assertIn("GBP 50 Closest Misses", rendered)
        self.assertIn("Mafell A 12 Drill", rendered)
        self.assertIn("Favourites", rendered)
        self.assertIn("Favourite iPad Pro", rendered)
        self.assertIn("review: `up v1|333333333333|0` / `down v1|333333333333|0`", rendered)
        self.assertNotIn("apple-macbook-pro-13in-intel-16gb-512gb_b", rendered)
        self.assertNotIn("apple-ipad-pro-favourite", rendered)
        self.assertNotIn("model_key", rendered)
        self.assertIn("profit", rendered)
        self.assertNotIn("BUY", rendered)
        self.assertNotIn("BID", rendered)
        self.assertNotIn("ACTIONABLE", rendered)
        self.assertNotIn("Paper", rendered)

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
