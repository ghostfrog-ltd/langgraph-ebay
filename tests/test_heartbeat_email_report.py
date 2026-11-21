import unittest
from datetime import datetime, timezone
from unittest.mock import patch

from pipelines.listing.main import (
    _build_heartbeat_email_html,
    _candidate_rows_for_refresh,
    _email_link,
    _next_action_for_email,
    _potential_rows_for_email,
    hourly_status_email,
    send_heartbeat_email_now,
)


class HeartbeatEmailReportTests(unittest.TestCase):
    """Heartbeat email should expose fresh live evidence and backfill status."""

    def test_email_listing_link_uses_compact_visible_label(self):
        html = _email_link(
            "Apple MacBook Air 13in M1 8GB 256GB",
            "https://www.ebay.co.uk/itm/168181595699?amdata=very-long",
        )

        self.assertIn("Apple MacBook Air 13in M1 8GB 256GB", html)
        self.assertIn('href="https://www.ebay.co.uk/itm/168181595699?amdata=very-long"', html)
        self.assertIn(">View listing</a>", html)
        self.assertNotIn(">https://www.ebay.co.uk/itm/168181595699", html)

    def test_candidate_price_refresh_shortlist_is_unique_and_capped(self):
        report = {
            "thresholds": {"min_profit_gbp": 50},
            "potentials": [
                {"source": "ebay-apple", "external_id": "v1|1|0"},
                {"source": "ebay-apple", "external_id": "v1|1|0"},
            ],
            "closest": [
                {
                    "source": "ebay-consoles",
                    "external_id": "v1|2|0",
                    "expected_profit": 45,
                    "max_safe_buy": 80,
                },
                {
                    "source": "ebay-lego",
                    "external_id": "v1|3|0",
                    "expected_profit": 5,
                    "max_safe_buy": 40,
                },
            ],
        }

        rows = _candidate_rows_for_refresh(report, limit=2)

        self.assertEqual(
            [(row["source"], row["external_id"]) for row in rows],
            [("ebay-apple", "v1|1|0"), ("ebay-consoles", "v1|2|0")],
        )

    def test_candidate_price_refresh_shortlist_zero_limit_disables_refresh(self):
        report = {
            "thresholds": {"min_profit_gbp": 25},
            "potentials": [
                {"source": "ebay-apple", "external_id": "v1|1|0"},
                {"source": "ebay-consoles", "external_id": "v1|2|0"},
            ],
            "closest": [
                {
                    "source": "ebay-lego",
                    "external_id": "v1|3|0",
                    "expected_profit": 20,
                    "max_safe_buy": 40,
                },
                {
                    "source": "ebay-samsung",
                    "external_id": "v1|4|0",
                    "expected_profit": 5,
                    "max_safe_buy": 40,
                },
            ],
        }

        rows = _candidate_rows_for_refresh(report, limit=0)

        self.assertEqual(rows, [])

    def test_heartbeat_email_disabled_skips_metrics_and_send(self):
        with patch("pipelines.listing.main.HEARTBEAT_EMAIL_ENABLED", False):
            with patch("pipelines.listing.main._metrics_for_heartbeat") as metrics:
                with patch("pipelines.listing.main.send_email") as send:
                    result = send_heartbeat_email_now()

        self.assertEqual(result, {"status": "skipped", "reason": "disabled"})
        metrics.assert_not_called()
        send.assert_not_called()

    def test_hourly_heartbeat_email_disabled_skips_metrics_and_send(self):
        with patch("pipelines.listing.main.HEARTBEAT_EMAIL_ENABLED", False):
            with patch("pipelines.listing.main._metrics_for_heartbeat") as metrics:
                with patch("pipelines.listing.main.send_email") as send:
                    state = hourly_status_email({})

        self.assertEqual(
            state["heartbeat_email_out"],
            {"status": "skipped", "reason": "disabled"},
        )
        metrics.assert_not_called()
        send.assert_not_called()

    def test_email_potential_rows_zero_limit_means_all(self):
        report = {
            "potentials": [
                {
                    "source": "ebay-apple",
                    "external_id": f"v1|{index}|0",
                    "title": f"Candidate {index}",
                    "url": f"https://example.test/{index}",
                    "sale_type": "auction",
                    "current_price": index,
                    "max_safe_buy": index + 10,
                    "expected_profit": index + 20,
                    "sold_median": index + 30,
                    "comp_samples": 100,
                    "condition_grade": "B",
                }
                for index in range(12)
            ]
        }

        rows, used_ids = _potential_rows_for_email(report, limit=0)

        self.assertEqual(len(rows), 12)
        self.assertEqual(len(used_ids), 12)

    def test_email_potential_rows_show_listing_type(self):
        report = {
            "potentials": [
                {
                    "source": "ebay-apple",
                    "external_id": "v1|auction|0",
                    "title": "Auction candidate",
                    "url": "https://example.test/auction",
                    "sale_type": "auction",
                    "current_price": 100,
                    "max_safe_buy": 150,
                    "expected_profit": 50,
                    "sold_median": 200,
                    "comp_samples": 100,
                    "condition_grade": "B",
                },
                {
                    "source": "ebay-samsung",
                    "external_id": "v1|bin|0",
                    "title": "Buy now candidate",
                    "url": "https://example.test/bin",
                    "sale_type": "bin",
                    "current_price": 100,
                    "max_safe_buy": 150,
                    "expected_profit": 50,
                    "sold_median": 200,
                    "comp_samples": 100,
                    "condition_grade": "B",
                },
                {
                    "source": "ebay-consoles",
                    "external_id": "v1|both|0",
                    "title": "Auction plus buy now candidate",
                    "url": "https://example.test/both",
                    "sale_type": "both",
                    "current_price": 100,
                    "max_safe_buy": 150,
                    "expected_profit": 50,
                    "sold_median": 200,
                    "comp_samples": 100,
                    "condition_grade": "B",
                },
            ]
        }

        rows, _used_ids = _potential_rows_for_email(report, limit=0)

        rendered = "\n".join(" ".join(row) for row in rows)
        self.assertIn("Auction", rendered)
        self.assertIn("Buy Now", rendered)
        self.assertIn("Auction + BIN", rendered)

    def test_email_potential_rows_show_next_action(self):
        report = {
            "potentials": [
                {
                    "source": "ebay-apple",
                    "external_id": "v1|auction|0",
                    "title": "Auction candidate",
                    "url": "https://example.test/auction",
                    "sale_type": "auction",
                    "current_price": 100,
                    "max_safe_buy": 150,
                    "expected_profit": 50,
                    "sold_median": 200,
                    "comp_samples": 100,
                    "condition_grade": "B",
                },
                {
                    "source": "ebay-samsung",
                    "external_id": "v1|bin|0",
                    "title": "Buy now candidate",
                    "url": "https://example.test/bin",
                    "sale_type": "bin",
                    "current_price": 100,
                    "max_safe_buy": 150,
                    "expected_profit": 50,
                    "sold_median": 200,
                    "comp_samples": 100,
                    "condition_grade": "B",
                },
            ]
        }

        rows, _used_ids = _potential_rows_for_email(report, limit=0)
        rendered = "\n".join(" ".join(row) for row in rows)

        self.assertIn("Bid to &pound;150.00; relist BIN &pound;190", rendered)
        self.assertIn("Buy now if <= &pound;150.00; relist BIN &pound;190", rendered)

    def test_email_next_action_avoids_rows_above_max_buy(self):
        action = _next_action_for_email(
            {
                "sale_type": "auction",
                "current_price": 160,
                "max_safe_buy": 150,
                "sold_median": 200,
            }
        )

        self.assertEqual(action, "Avoid")

    def test_email_potential_rows_enforce_buy_signal_comp_floor(self):
        report = {
            "potentials": [
                {
                    "source": "ebay-graphics-cards",
                    "external_id": "v1|low|0",
                    "title": "Thin evidence row",
                    "url": "https://example.test/low",
                    "current_price": 100,
                    "max_safe_buy": 150,
                    "expected_profit": 50,
                    "sold_median": 200,
                    "comp_samples": 49,
                    "effective_min_comps": 5,
                    "condition_grade": "B",
                },
                {
                    "source": "ebay-apple",
                    "external_id": "v1|ok|0",
                    "title": "Enough evidence row",
                    "url": "https://example.test/ok",
                    "current_price": 100,
                    "max_safe_buy": 150,
                    "expected_profit": 50,
                    "sold_median": 200,
                    "comp_samples": 50,
                    "condition_grade": "B",
                },
            ]
        }

        rows, used_ids = _potential_rows_for_email(report, limit=0)

        rendered = "\n".join(" ".join(row) for row in rows)
        self.assertNotIn("Thin evidence row", rendered)
        self.assertIn("Enough evidence row", rendered)
        self.assertEqual(used_ids, {"v1|ok|0"})

    def test_email_potential_rows_allow_explicit_lower_analysis_floor(self):
        report = {
            "potentials": [
                {
                    "source": "ebay-graphics-cards",
                    "external_id": "v1|low|0",
                    "title": "Thin evidence row",
                    "url": "https://example.test/low",
                    "current_price": 100,
                    "max_safe_buy": 150,
                    "expected_profit": 50,
                    "sold_median": 200,
                    "comp_samples": 5,
                    "effective_min_comps": 5,
                    "condition_grade": "B",
                },
            ]
        }

        rows, used_ids = _potential_rows_for_email(report, limit=0, min_comps=5)

        rendered = "\n".join(" ".join(row) for row in rows)
        self.assertIn("Thin evidence row", rendered)
        self.assertEqual(used_ids, {"v1|low|0"})

    def test_email_potential_rows_sort_by_ending_soonest(self):
        report = {
            "potentials": [
                {
                    "source": "ebay-apple",
                    "external_id": "v1|later|0",
                    "title": "Ending later",
                    "url": "https://example.test/later",
                    "sale_type": "auction",
                    "end_time": "2026-06-07T12:00:00+00:00",
                    "current_price": 100,
                    "max_safe_buy": 150,
                    "expected_profit": 50,
                    "sold_median": 200,
                    "comp_samples": 100,
                    "condition_grade": "B",
                },
                {
                    "source": "ebay-apple",
                    "external_id": "v1|soon|0",
                    "title": "Ending soon",
                    "url": "https://example.test/soon",
                    "sale_type": "auction",
                    "end_time": "2026-06-04T12:00:00+00:00",
                    "current_price": 100,
                    "max_safe_buy": 150,
                    "expected_profit": 50,
                    "sold_median": 200,
                    "comp_samples": 100,
                    "condition_grade": "B",
                },
            ]
        }

        rows, _used_ids = _potential_rows_for_email(report, limit=0)

        self.assertIn("Ending soon", rows[0][1])
        self.assertIn("04 Jun", rows[0][3])
        self.assertIn("Ending later", rows[1][1])

    def test_email_includes_fresh_live_rows_and_backfill_activity(self):
        metrics = {
            "listing_counts": {
                "total": 100,
                "live": 12,
                "sold": 4,
                "ended": 6,
                "unknown": 70,
                "unknown_unfinalized": 65,
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
                "unknown_backfill_source_summary": [
                    {
                        "source": "ebay-apple",
                        "model_keys_needing_comps": 2,
                        "live_listings_needing_comps": 4,
                        "unknown_backfill_pool": 9,
                        "newest_unknown": datetime(2026, 6, 2, 9, 0, tzinfo=timezone.utc),
                    }
                ],
                "unknown_model_key_work": [],
                "recommended_unknown_candidate_preview": [],
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
            "market_discovery": {
                "potentials": [
                    {
                        "source": "ebay-apple",
                        "title": "Apple MacBook Air 13in M1 8GB 256GB",
                        "url": "https://example.test/item/1",
                        "external_id": "v1|1|0",
                        "sale_type": "auction",
                        "current_price": 105,
                        "max_safe_buy": 180,
                        "expected_profit": 75,
                        "sold_median": 240,
                        "comp_samples": 100,
                        "condition_grade": "B",
                    }
                ],
                "closest": [
                    {
                        "source": "ebay-consoles",
                        "title": "Nintendo Switch HAC-001 Neon Blue Red",
                        "url": "https://example.test/item/3",
                        "external_id": "v1|3|0",
                        "sale_type": "auction",
                        "current_price": 85,
                        "max_safe_buy": 80.00,
                        "expected_profit": 45.00,
                        "sold_median": 167.00,
                        "comp_samples": 100,
                        "condition_grade": "B",
                    },
                    {
                        "source": "ebay-samsung",
                        "title": "Samsung Galaxy A12 Cracked",
                        "url": "https://example.test/item/5",
                        "external_id": "v1|5|0",
                        "sale_type": "bin",
                        "current_price": 33,
                        "max_safe_buy": -7.20,
                        "expected_profit": -15.20,
                        "sold_median": 40,
                        "comp_samples": 6,
                        "condition_grade": "B",
                    }
                ],
            },
            "market_discovery_punt": {
                "potentials": [
                    {
                        "source": "ebay-tools",
                        "title": "Mafell A 12 Drill",
                        "url": "https://example.test/item/2",
                        "external_id": "v1|2|0",
                        "sale_type": "both",
                        "current_price": 80,
                        "max_safe_buy": 112,
                        "expected_profit": 32,
                        "sold_median": 145,
                        "comp_samples": 100,
                        "condition_grade": "B",
                    }
                ],
                "closest": [
                    {
                        "source": "ebay-apple",
                        "title": "Apple iPhone 11 128GB White",
                        "url": "https://example.test/item/4",
                        "external_id": "v1|4|0",
                        "sale_type": "bin",
                        "current_price": 53,
                        "max_safe_buy": 48.00,
                        "expected_profit": 20.00,
                        "sold_median": 96.55,
                        "comp_samples": 18,
                        "condition_grade": "B",
                    }
                ],
            },
            "market_discovery_pokemon": {
                "potentials": [
                    {
                        "source": "ebay-pokemon",
                        "title": "Pokemon TCG Test Card PSA 10",
                        "url": "https://example.test/item/pokemon",
                        "external_id": "v1|pokemon|0",
                        "sale_type": "bin",
                        "current_price": 40,
                        "max_safe_buy": 46,
                        "expected_profit": 22,
                        "sold_median": 88,
                        "comp_samples": 3,
                        "condition_grade": "B",
                    },
                    {
                        "source": "ebay-pokemon",
                        "title": "Pokemon TCG Thin Comp",
                        "url": "https://example.test/item/pokemon-thin",
                        "external_id": "v1|pokemon-thin|0",
                        "sale_type": "bin",
                        "current_price": 40,
                        "max_safe_buy": 45,
                        "expected_profit": 22,
                        "sold_median": 88,
                        "comp_samples": 2,
                        "condition_grade": "B",
                    },
                ],
                "closest": [],
            },
            "model_key_inventory": [
                {
                    "source": "ebay-apple",
                    "model_key": "apple-macbook-air-13in-m1-8gb-256gb",
                    "listing_count": 12,
                    "live_count": 2,
                    "final_price_count": 8,
                    "source_key_count": 2,
                    "source_listing_count": 20,
                    "source_live_count": 3,
                    "source_final_price_count": 14,
                    "source_total_rows": 31,
                    "source_without_final_price_count": 17,
                    "source_rank": 1,
                },
                {
                    "source": "ebay-apple",
                    "model_key": "apple-iphone-13-128gb",
                    "listing_count": 8,
                    "live_count": 1,
                    "final_price_count": 6,
                    "source_key_count": 2,
                    "source_listing_count": 20,
                    "source_live_count": 3,
                    "source_final_price_count": 14,
                    "source_total_rows": 31,
                    "source_without_final_price_count": 17,
                    "source_rank": 2,
                },
                {
                    "source": "ebay-tools",
                    "model_key": "mafell-a12-drill",
                    "listing_count": 4,
                    "live_count": 1,
                    "final_price_count": 3,
                    "source_key_count": 1,
                    "source_listing_count": 4,
                    "source_live_count": 1,
                    "source_final_price_count": 3,
                    "source_total_rows": 9,
                    "source_without_final_price_count": 6,
                    "source_rank": 1,
                },
            ],
            "listing_favourites": [
                {
                    "source": "ebay-apple",
                    "external_id": "v1|9|0",
                    "title": "Favourite MacBook",
                    "url": "https://example.test/item/9",
                    "status": "live",
                    "price_current": 199,
                    "model_key": "apple-macbook-favourite",
                    "condition_grade": "B",
                }
            ],
            "review_auctions": {},
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
        self.assertIn("GBP 50 Profit Candidates", html)
        self.assertIn("GBP 25 Profit Candidates", html)
        self.assertIn("Review", html)
        self.assertIn("Type", html)
        self.assertIn("action=up", html)
        self.assertIn("action=down", html)
        self.assertIn("Apple MacBook Air", html)
        self.assertIn("Mafell A 12", html)
        self.assertNotIn("Pokemon TCG Test Card PSA 10", html)
        self.assertNotIn("Pokemon TCG Thin Comp", html)
        self.assertIn("Auction", html)
        self.assertIn("Auction + BIN", html)
        self.assertIn("&pound;75.00", html)
        self.assertIn("&pound;32.00", html)
        self.assertNotIn("&pound;22.00", html)
        self.assertNotIn("How To Read Listing Rows", html)
        self.assertNotIn("Expected profit if bought at the current price", html)
        self.assertNotIn("Highest safe buy price after resale costs and target profit", html)
        self.assertNotIn("Pokemon GBP 20 Profit Candidates", html)
        self.assertNotIn("GBP 50 Closest Misses", html)
        self.assertNotIn("GBP 25 Closest Misses", html)
        self.assertNotIn("No actionable near misses at or below max buy", html)
        self.assertNotIn("Nintendo Switch HAC-001", html)
        self.assertNotIn("Apple iPhone 11 128GB", html)
        self.assertNotIn("&pound;45.00", html)
        self.assertNotIn("&pound;20.00", html)
        self.assertNotIn("Samsung Galaxy A12 Cracked", html)
        self.assertNotIn("&pound;-15.20", html)
        self.assertNotIn("Favourites", html)
        self.assertNotIn("Favourite MacBook", html)
        self.assertNotIn("Model Keys By Market", html)
        self.assertNotIn("usable model-key evidence only", html)
        self.assertNotIn("20 usable rows", html)
        self.assertNotIn("apple-macbook-air-13in-m1-8gb-256gb", html)
        self.assertNotIn("apple-iphone-13-128gb", html)
        self.assertNotIn("mafell-a12-drill", html)
        self.assertLess(
            html.index("GBP 50 Profit Candidates"),
            html.index("GBP 25 Profit Candidates"),
        )
        self.assertNotIn("Fresh Live Review Data", html)
        self.assertNotIn("Backfill Position", html)
        self.assertNotIn("Listing Store", html)
        self.assertNotIn("Pipeline Run", html)
        self.assertNotIn("API Usage Today", html)
        self.assertNotIn("Backfill Preview", html)
        self.assertNotIn("What Matters Right Now", html)
        self.assertNotIn("Paper trades", html)
        self.assertNotIn("Paper sold", html)
        self.assertNotIn("Candidate Check Loop", html)
        self.assertNotIn("paper", html.lower())


if __name__ == "__main__":
    unittest.main()
