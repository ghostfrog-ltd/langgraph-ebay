import unittest
from io import StringIO
from unittest.mock import patch

from pipelines.listing import operator_console


class OperatorConsoleTests(unittest.TestCase):
    """The local ebay console should expose the useful operator commands."""

    def test_menu_lists_core_commands_and_safety(self):
        menu = operator_console.command_menu()

        self.assertIn("Mission: buy cheap from eBay", menu)
        self.assertIn("serious", menu)
        self.assertIn("punt", menu)
        self.assertIn("stats", menu)
        self.assertIn("api-health", menu)
        self.assertIn("backfill-plan", menu)
        self.assertIn("model-keys-apply", menu)
        self.assertIn("actioncam-keys-apply", menu)
        self.assertIn("lego-keys-apply", menu)
        self.assertIn("review-server", menu)
        self.assertIn("review-down", menu)
        self.assertIn("favourites", menu)
        self.assertIn("spends eBay API calls", menu)

    def test_serious_runs_market_discovery_with_50_profit_gate(self):
        report = {
            "thresholds": {"min_profit_gbp": 50.0, "min_comps": 4},
            "summary": {"checked": 2, "potentials": 1, "live_rows_with_comps": 3},
            "potentials": [
                {
                    "title": "Apple MacBook Pro 16 M3 Pro",
                    "url": "https://example.test/mac",
                    "model_key": "apple-macbook-pro-16in-m3pro-18gb-512gb",
                    "condition_grade": "B",
                    "current_price": 648,
                    "sold_median": 1034.15,
                    "comp_samples": 4,
                    "max_safe_buy": 832.71,
                    "expected_profit": 234.71,
                    "expected_roi": 0.362,
                }
            ],
            "closest": [],
        }

        with patch("pipelines.listing.market_discovery.graph.run", return_value=report) as run:
            with patch("sys.stdout", new_callable=StringIO) as out:
                code = operator_console.main(["serious", "--limit", "7", "--min-comps", "4"])

        self.assertEqual(code, 0)
        run.assert_called_once()
        text = out.getvalue()
        self.assertIn("Live eBay potentials", text)
        self.assertIn("Apple MacBook Pro 16", text)
        self.assertIn("profit GBP 234.71", text)

    def test_near_misses_hide_rows_already_above_max_buy(self):
        report = {
            "thresholds": {"min_profit_gbp": 50.0, "min_comps": 3},
            "summary": {"checked": 2, "potentials": 0, "live_rows_with_comps": 2},
            "potentials": [],
            "closest": [
                {
                    "title": "Almost useful console",
                    "url": "https://example.test/almost",
                    "current_price": 100.0,
                    "max_safe_buy": 95.0,
                    "expected_profit": 45.0,
                    "price_gap_to_max_buy": -5.0,
                },
                {
                    "title": "Low value game bundle",
                    "url": "https://example.test/game",
                    "current_price": 15.0,
                    "max_safe_buy": -23.51,
                    "expected_profit": 11.49,
                    "price_gap_to_max_buy": -38.51,
                }
            ],
        }

        with patch("sys.stdout", new_callable=StringIO) as out:
            operator_console.print_potentials_report(report)

        text = out.getvalue()
        self.assertIn("No actionable near misses", text)
        self.assertNotIn("Almost useful console", text)
        self.assertNotIn("short GBP 5.00", text)
        self.assertNotIn("Low value game bundle", text)
        self.assertNotIn("max no positive buy", text)

    def test_stats_prints_compact_health_block(self):
        stats = {
            "total_listings": 126541,
            "known_model_keys": 81854,
            "unknown_model_keys": 44687,
            "confirmed_final_prices": 13126,
            "final_prices_with_known_model_key": 9270,
            "comp_buckets": 2136,
            "comp_samples": 7081,
            "comp_buckets_3plus": 408,
            "live_listings": 1504,
            "unknown_listings": 90475,
            "unknown_queue": 89818,
            "live_with_known_key": 849,
            "live_with_same_grade_comps": 161,
            "live_with_3plus_same_grade_comps": 93,
            "today_calls": 4337,
            "planned_normal_calls": 4337,
            "spare_calls": 163,
            "backfill_checked_today": 1501,
            "backfill_sold_today": 577,
            "current_roi_rows": 7,
            "current_roi_rows_above_100": 1,
            "api_accounting": {
                "buckets": {
                    "trading": {
                        "label": "Trading API",
                        "used": 2100,
                        "limit": 5000,
                        "remaining": 2900,
                        "blocked": False,
                    },
                    "browse": {
                        "label": "Browse API",
                        "used": 2237,
                        "limit": 5000,
                        "remaining": 2763,
                        "blocked": False,
                    },
                    "identity": {
                        "label": "Identity API",
                        "used": 0,
                        "limit": 5000,
                        "remaining": 5000,
                        "blocked": False,
                    },
                    "developer_analytics": {
                        "label": "Developer Analytics API",
                        "used": 0,
                        "limit": 5000,
                        "remaining": 5000,
                        "blocked": False,
                    },
                },
                "unknown_services": [],
            },
        }

        with patch("pipelines.listing.operator_console.build_stats", return_value=stats):
            with patch("sys.stdout", new_callable=StringIO) as out:
                code = operator_console.main(["stats"])

        self.assertEqual(code, 0)
        text = out.getvalue()
        self.assertIn("GhostFrog eBay stats", text)
        self.assertIn("LIVE LISTINGS", text)
        self.assertIn("CONFIRMED COMPS", text)
        self.assertIn("LIVE COMP COVERAGE", text)
        self.assertIn("API TODAY", text)
        self.assertIn("13,126", text)
        self.assertIn("7,081 final-price samples", text)
        self.assertIn("93 of 1,504 have 3+ comps", text)
        self.assertIn("T 2,100/5,000 | B 2,237/5,000", text)
        self.assertIn("577 sold confirmed", text)
        self.assertIn("== Data Quality ==", text)
        self.assertIn("Total listings", text)
        self.assertIn("126,541", text)
        self.assertIn("== Comps And Live Coverage ==", text)
        self.assertIn("== API Buckets ==", text)
        self.assertIn("Trading API", text)

    def test_stats_are_mapped_from_shared_heartbeat_metrics(self):
        metrics = {
            "listing_counts": {
                "total": 100,
                "unknown": 20,
                "unknown_unfinalized": 18,
                "missing_model_key": 30,
                "final_prices": 12,
                "final_prices_with_known_model_key": 9,
            },
            "comps": {
                "comp_keys": 7,
                "comp_samples": 40,
                "comp_keys_3plus": 5,
            },
            "live_comp_coverage": {
                "live_total": 10,
                "live_unknown_model_key": 3,
                "live_with_comps": 6,
                "live_with_3plus_comps": 4,
            },
            "api_total": 22,
            "api_accounting": {"buckets": {}},
            "backfill_plan": {
                "api_budget": {
                    "planned_normal_calls": 30,
                    "theoretical_spare": 4470,
                },
            },
            "backfill_activity": {
                "checked_today": 11,
                "sold_today": 2,
            },
            "high_risk_roi": {
                "live_roi_gt_15": 8,
                "live_roi_gt_100": 1,
            },
        }

        with patch("pipelines.listing.operator_metrics.build_operator_metrics", return_value=metrics):
            stats = operator_console.build_stats()

        self.assertEqual(stats["total_listings"], 100)
        self.assertEqual(stats["known_model_keys"], 70)
        self.assertEqual(stats["unknown_model_keys"], 30)
        self.assertEqual(stats["confirmed_final_prices"], 12)
        self.assertEqual(stats["final_prices_with_known_model_key"], 9)
        self.assertEqual(stats["live_with_known_key"], 7)
        self.assertEqual(stats["live_with_3plus_same_grade_comps"], 4)
        self.assertEqual(stats["today_calls"], 22)
        self.assertEqual(stats["planned_normal_calls"], 30)
        self.assertEqual(stats["spare_calls"], 4470)

    def test_backfill_apply_defaults_to_useful_batch_size(self):
        with patch("pipelines.listing.operator_console._python_module", return_value=0) as run:
            code = operator_console.main(["backfill-apply"])

        self.assertEqual(code, 0)
        run.assert_called_once_with(
            "pipelines.listing.point_zero.backfill_apply",
            "--limit",
            "100",
            "--daily-limit",
            "1000",
            "--apply",
        )

    def test_backfill_apply_forwards_daily_limit_override(self):
        with patch("pipelines.listing.operator_console._python_module", return_value=0) as run:
            code = operator_console.main(["backfill-apply", "--limit", "250", "--daily-limit", "5000"])

        self.assertEqual(code, 0)
        run.assert_called_once_with(
            "pipelines.listing.point_zero.backfill_apply",
            "--limit",
            "250",
            "--daily-limit",
            "5000",
            "--apply",
        )

    def test_heartbeat_email_sends_now_with_optional_recipient(self):
        result = {"status": "sent", "sent_to": ["test@example.com"]}
        with patch("pipelines.listing.main.send_heartbeat_email_now", return_value=result) as send:
            with patch("sys.stdout", new_callable=StringIO) as out:
                code = operator_console.main(["heartbeat-email", "--to", "test@example.com"])

        self.assertEqual(code, 0)
        send.assert_called_once_with(recipients=["test@example.com"], record_sent=True)
        self.assertIn('"status": "sent"', out.getvalue())

    def test_review_down_records_exclusion(self):
        row = {"external_id": "v1|168181595699|0", "title": "MacBook"}
        with patch("pipelines.listing.review_actions.record_review_action", return_value=row) as record:
            with patch("sys.stdout", new_callable=StringIO) as out:
                code = operator_console.main(["review-down", "168181595699"])

        self.assertEqual(code, 0)
        record.assert_called_once_with(external_id="168181595699", action="down")
        self.assertIn("excluded: v1|168181595699|0", out.getvalue())

    def test_favourites_prints_local_rows(self):
        rows = [
            {
                "source": "ebay-apple",
                "external_id": "v1|1|0",
                "title": "Favourite Mac",
                "url": "https://example.test/1",
                "status": "live",
                "price_current": 199,
            }
        ]
        with patch("pipelines.listing.review_actions.list_favourites", return_value=rows):
            with patch("sys.stdout", new_callable=StringIO) as out:
                code = operator_console.main(["favourites"])

        self.assertEqual(code, 0)
        self.assertIn("Local favourites", out.getvalue())
        self.assertIn("Favourite Mac", out.getvalue())

    def test_actioncam_keys_apply_rebuilds_actioncam_source(self):
        with patch("pipelines.listing.operator_console._python_module", return_value=0) as run:
            code = operator_console.main(["actioncam-keys-apply", "--limit", "1234"])

        self.assertEqual(code, 0)
        run.assert_called_once_with(
            "pipelines.listing.attributes.rebuild_model_keys",
            "--all",
            "--source",
            "ebay-actioncams",
            "--limit",
            "1234",
        )

    def test_lego_keys_apply_rebuilds_lego_source(self):
        with patch("pipelines.listing.operator_console._python_module", return_value=0) as run:
            code = operator_console.main(["lego-keys-apply", "--limit", "4321"])

        self.assertEqual(code, 0)
        run.assert_called_once_with(
            "pipelines.listing.attributes.rebuild_model_keys",
            "--all",
            "--source",
            "ebay-lego",
            "--limit",
            "4321",
        )

    def test_model_keys_apply_rebuilds_all_sources_by_default(self):
        with patch("pipelines.listing.operator_console._python_module", return_value=0) as run:
            code = operator_console.main(["model-keys-apply"])

        self.assertEqual(code, 0)
        run.assert_called_once_with(
            "pipelines.listing.attributes.rebuild_model_keys",
            "--all",
            "--limit",
            "200000",
        )

    def test_model_keys_dry_run_can_filter_source(self):
        with patch("pipelines.listing.operator_console._python_module", return_value=0) as run:
            code = operator_console.main(["model-keys-dry-run", "--source", "ebay-lego", "--limit", "123"])

        self.assertEqual(code, 0)
        run.assert_called_once_with(
            "pipelines.listing.attributes.rebuild_model_keys",
            "--all",
            "--limit",
            "123",
            "--source",
            "ebay-lego",
            "--dry-run",
        )


if __name__ == "__main__":
    unittest.main()
