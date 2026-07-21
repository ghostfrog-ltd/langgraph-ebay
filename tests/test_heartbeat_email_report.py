import unittest
from datetime import datetime, timezone

from pipelines.listing.main import _build_heartbeat_email_html


class HeartbeatEmailReportTests(unittest.TestCase):
    """Heartbeat email should expose fresh live evidence and backfill status."""

    def test_email_includes_fresh_live_rows_and_backfill_activity(self):
        metrics = {
            "listing_counts": {
                "total": 100,
                "live": 12,
                "sold": 4,
                "ended": 6,
                "stale": 70,
                "stale_unfinalized": 65,
                "final_prices": 4,
                "missing_model_key": 9,
            },
            "comps": {
                "comp_keys": 10,
                "comp_samples": 55,
                "comp_keys_3plus": 7,
                "computed_at": datetime(2026, 6, 3, 8, 30, tzinfo=timezone.utc),
            },
            "live_comp_coverage": {
                "live_total": 12,
                "live_with_3plus_comps": 5,
                "live_unknown_model_key": 3,
            },
            "api_today": [{"service": "ebay_ended_v1", "call_count": 17}],
            "api_total": 17,
            "source_freshness": [],
            "high_risk_roi": {
                "live_roi_gt_15": 2,
                "live_roi_gt_100": 0,
                "live_roi_gt_1000": 0,
            },
            "paper_trading": {
                "total_predictions": 8,
                "watching": 3,
                "watching_past_end": 1,
                "checked_today": 0,
                "closed": 5,
                "closed_today": 0,
                "sold": 1,
                "would_have_won": 1,
                "outbid_above_ceiling": 0,
                "profitable_on_paper": 1,
                "avg_price_error": None,
                "avg_observed_roi": None,
            },
            "paper_by_source": [],
            "paper_lessons": {"lesson_counts": {}, "lessons": []},
            "backfill_plan": {
                "api_budget": {
                    "recommended_backfill_calls": 5,
                    "planned_normal_calls": 17,
                    "theoretical_spare": 4483,
                    "daily_cap": 5000,
                    "safety_reserve": 500,
                    "today_calls": 17,
                    "avg_total_7d": 20,
                    "max_total_7d": 25,
                },
                "excluded_backfill_sources": ["ebay-motors"],
                "stale_backfill_source_summary": [
                    {
                        "source": "ebay-apple",
                        "model_keys_needing_comps": 2,
                        "live_listings_needing_comps": 4,
                        "stale_backfill_pool": 9,
                        "newest_stale": datetime(2026, 6, 2, 9, 0, tzinfo=timezone.utc),
                    }
                ],
                "unknown_model_key_work": [],
                "recommended_stale_candidate_preview": [],
            },
            "backfill_activity": {
                "checked_today": 5,
                "checked_24h": 9,
                "finalized_today": 2,
                "sold_today": 1,
                "latest_checked_at": datetime(2026, 6, 3, 9, 15, tzinfo=timezone.utc),
            },
            "backfill_reason_counts": [{"reason": "ended_with_bids", "checks": 1}],
            "backfill_worker_running": True,
            "market_discovery": {},
            "review_auctions": {
                "items": [
                    {
                        "source": "ebay-apple",
                        "title": "Apple MacBook Air 13in M1 8GB 256GB",
                        "url": "https://example.test/item/1",
                        "current_price": 105,
                        "estimated_profit": 42,
                        "comp_samples": 8,
                        "ends": "5h",
                        "risk_notes": ["review photos/condition"],
                    }
                ],
                "scout_items": [
                    {
                        "source": "ebay-tools",
                        "title": "Mafell A 12 Drill",
                        "url": "https://example.test/item/2",
                        "current_price": 80,
                        "comp_samples": None,
                        "ends": "2h",
                        "reason": "needs sold comps",
                    }
                ],
            },
        }

        html = _build_heartbeat_email_html(
            {
                "backfill_trigger_out": {
                    "status": "spawned",
                    "remaining_budget": 995,
                }
            },
            metrics,
            datetime(2026, 6, 3, 10, 0, tzinfo=timezone.utc),
        )

        self.assertIn("Pipeline is running from live PostgreSQL data", html)
        self.assertIn("Fresh Live Review Data", html)
        self.assertIn("Apple MacBook Air", html)
        self.assertIn("Mafell A 12", html)
        self.assertIn("Heartbeat trigger status", html)
        self.assertIn("spawned", html)
        self.assertIn("Worker lock status", html)
        self.assertIn("running", html)
        self.assertIn("Rows checked today", html)
        self.assertIn("ended_with_bids", html)


if __name__ == "__main__":
    unittest.main()
