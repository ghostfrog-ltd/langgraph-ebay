import unittest
from datetime import datetime, timedelta, timezone

from pipelines.listing.market_discovery.graph import (
    _fetch_live_listing_comp_rows,
    build_potentials,
    effective_min_comps_for_source,
    potential_from_row,
    useful_near_misses,
)


class _QueryCursor:
    def __init__(self):
        self.sql = ""
        self.params = None

    def execute(self, sql, params=None):
        self.sql = sql
        self.params = params

    def fetchall(self):
        return []


class ListingPotentialTests(unittest.TestCase):
    """Pure tests for live listing potential maths."""

    def test_potential_from_row_calculates_safe_buy_math(self):
        potential = potential_from_row(
            {
                "source": "ebay-apple",
                "external_id": "v1|1|0",
                "title": "Apple MacBook Air M1 8GB 256GB",
                "url": "https://example.test/1",
                "model_key": "apple-macbook-air-13in-m1-8gb-256gb",
                "sale_type": "both",
                "end_time": "2026-06-04T12:00:00+00:00",
                "price_current": 120.0,
                "median_final_price": 304.37,
                "samples": 42,
            }
        )

        self.assertEqual(potential.current_price, 120.0)
        self.assertEqual(potential.sold_median, 304.37)
        self.assertEqual(potential.sale_type, "both")
        self.assertEqual(potential.end_time, "2026-06-04T12:00:00+00:00")
        self.assertEqual(potential.comp_samples, 42)
        self.assertEqual(potential.expected_profit, 127.8)
        self.assertEqual(potential.max_safe_buy, 197.8)
        self.assertEqual(potential.price_gap_to_max_buy, 77.8)
        self.assertIn("current £120.0 vs max buy £197.8", potential.signal)

    def test_potential_from_row_uses_effective_visual_downgrade(self):
        potential = potential_from_row(
            {
                "source": "ebay-apple",
                "external_id": "v1|iphone|0",
                "title": "Apple iPhone 12 64GB",
                "url": "https://example.test/iphone",
                "model_key": "apple-iphone-12-64gb",
                "condition_grade": "B",
                "text_condition_grade": "B",
                "visual_condition_grade": "C",
                "visual_condition_checked": True,
                "visual_review_required": True,
                "visual_damage_flags": ["chipped_corner"],
                "visual_model_name": "minicpm-v:8b",
                "visual_checked_at": "2026-08-26T20:00:00+00:00",
                "price_current": 24.0,
                "median_final_price": 89.0,
                "samples": 20,
            }
        )

        self.assertEqual(potential.condition_grade, "C")
        self.assertEqual(potential.text_condition_grade, "B")
        self.assertEqual(potential.visual_condition_grade, "C")
        self.assertTrue(potential.visual_condition_checked)
        self.assertEqual(potential.visual_damage_flags, ["chipped_corner"])

    def test_potential_from_row_uses_effective_llm_text_downgrade(self):
        potential = potential_from_row(
            {
                "source": "ebay-apple",
                "external_id": "v1|iphone|1",
                "title": "Apple iPhone 12 64GB fully working",
                "url": "https://example.test/iphone-2",
                "model_key": "apple-iphone-12-64gb",
                "condition_grade": "B",
                "text_condition_grade": "B",
                "text_scan_grade": "D",
                "visual_condition_grade": "B",
                "price_current": 24.0,
                "median_final_price": 89.0,
                "samples": 20,
            }
        )

        self.assertEqual(potential.condition_grade, "D")
        self.assertEqual(potential.text_condition_grade, "B")
        self.assertEqual(potential.text_scan_grade, "D")

    def test_default_candidate_query_only_uses_live_rows(self):
        cur = _QueryCursor()

        _fetch_live_listing_comp_rows(cur)

        self.assertIn("a.status = 'live'", cur.sql)
        self.assertNotIn("a.status = 'unknown'", cur.sql)
        self.assertIn("COALESCE(a.last_seen_at, a.first_seen)", cur.sql)
        self.assertEqual(cur.params, ["24"])
        self.assertIn("listing_review_actions", cur.sql)
        self.assertIn("review.action = 'down'", cur.sql)
        self.assertIn("latest_listing_visual_conditions", cur.sql)
        self.assertIn("latest_listing_text_conditions", cur.sql)
        self.assertIn("grade.effective_condition_grade", cur.sql)

    def test_email_prefetch_can_include_future_unknown_rows_for_refresh(self):
        cur = _QueryCursor()

        _fetch_live_listing_comp_rows(cur, include_future_unknown=True)

        self.assertIn("a.status = 'live'", cur.sql)
        self.assertIn("a.status = 'unknown'", cur.sql)
        self.assertIn("a.end_time >", cur.sql)

    def test_candidate_query_can_filter_to_one_source(self):
        cur = _QueryCursor()

        _fetch_live_listing_comp_rows(cur, source="ebay-pokemon")

        self.assertIn("AND a.source = %s", cur.sql)
        self.assertEqual(cur.params, ["24", "ebay-pokemon"])

    def test_build_potentials_returns_rows_that_clear_thresholds(self):
        rows = [
            {
                "source": "ebay-apple",
                "external_id": "v1|1|0",
                "title": "Apple MacBook Air M1 8GB 256GB",
                "url": "https://example.test/1",
                "model_key": "apple-macbook-air-13in-m1-8gb-256gb",
                "price_current": 120.0,
                "median_final_price": 304.37,
                "samples": 42,
            },
            {
                "source": "ebay-apple",
                "external_id": "v1|2|0",
                "title": "Apple iPhone 11 128GB",
                "url": "https://example.test/2",
                "model_key": "apple-iphone-11-128gb",
                "price_current": 110.0,
                "median_final_price": 123.0,
                "samples": 16,
            },
        ]

        potentials, closest, stats = build_potentials(rows)

        self.assertEqual(stats["checked"], 2)
        self.assertEqual(len(potentials), 1)
        self.assertEqual(potentials[0].external_id, "v1|1|0")
        self.assertEqual(closest[0].external_id, "v1|1|0")

    def test_build_potentials_blocks_dirty_identity(self):
        potentials, closest, stats = build_potentials(
            [
                {
                    "source": "ebay-apple",
                    "external_id": "v1|screen|0",
                    "title": "Original SCREEN for Apple MacBook Pro 14 2024 M4",
                    "url": "https://example.test/screen",
                    "model_key": "apple-macbook-pro-142in-m4_b",
                    "price_current": 40.0,
                    "median_final_price": 500.0,
                    "samples": 5,
                }
            ]
        )

        self.assertEqual(potentials, [])
        self.assertEqual(closest, [])
        self.assertEqual(stats["blocked_identity"], 1)

    def test_build_potentials_blocks_macbook_parts_against_whole_mac_comps(self):
        potentials, closest, stats = build_potentials(
            [
                {
                    "source": "ebay-apple",
                    "external_id": "v1|parts|0",
                    "title": "Apple MacBook Pro A2141 UK Laptop Keyboard Palmrest Touchpad Battery Assembly",
                    "url": "https://example.test/parts",
                    "model_key": "apple-macbook-pro-a2141-16in-core-i7-16gb-512gb",
                    "end_time": "2026-07-28T23:02:00+00:00",
                    "price_current": 38.0,
                    "median_final_price": 329.99,
                    "samples": 83,
                }
            ]
        )

        self.assertEqual(potentials, [])
        self.assertEqual(closest, [])
        self.assertEqual(stats["blocked_identity"], 1)

    def test_build_potentials_blocks_faulty_scrap_attrs_against_whole_mac_comps(self):
        potentials, closest, stats = build_potentials(
            [
                {
                    "source": "ebay-apple",
                    "external_id": "v1|faulty|0",
                    "title": "Apple MacBook Pro 13 A2338 2020 M1 Space Grey",
                    "url": "https://example.test/faulty",
                    "model_key": "apple-macbook-pro-a2338-13in-m1-8gb-256gb",
                    "raw_attrs": {
                        "Seller notes": (
                            "The macbook is faulty and I am selling for scraps. "
                            "The screen works but it's badly damaged. "
                            "The Logic board and touch ID is missing. "
                            "No charger or accessories included. "
                            "This is listed for faulty and not working."
                        )
                    },
                    "end_time": "2026-07-28T23:02:00+00:00",
                    "price_current": 67.0,
                    "median_final_price": 345.0,
                    "samples": 19,
                }
            ]
        )

        self.assertEqual(potentials, [])
        self.assertEqual(closest, [])
        self.assertEqual(stats["blocked_identity"], 1)

    def test_dirty_false_positives_do_not_hide_valid_future_auction(self):
        now = datetime(2026, 8, 11, 12, 0, tzinfo=timezone.utc)
        dirty_rows = [
            {
                "source": "ebay-consoles",
                "external_id": "v1|307061337039|0",
                "title": "Nintendo Switch 2 USB-C Wall Charger And Cable",
                "url": "https://example.test/charger",
                "model_key": "nintendo-switch-2",
                "raw_attrs": {"Type": "USB Cable", "Compatible Product": "Nintendo Switch 2 Console"},
                "sale_type": "bin",
                "price_current": 8.0,
                "median_final_price": 390.0,
                "samples": 20,
            },
            {
                "source": "ebay-consoles",
                "external_id": "v1|375144747987|0",
                "title": "Wireless Controller for Nintendo Switch 2",
                "url": "https://example.test/controller",
                "model_key": "nintendo-switch-2",
                "raw_attrs": {"Type": "Controller", "Platform": "Nintendo Switch 2"},
                "sale_type": "bin",
                "price_current": 20.0,
                "median_final_price": 390.0,
                "samples": 20,
            },
            {
                "source": "ebay-consoles",
                "external_id": "v1|236802209004|0",
                "title": "Nintendo Switch 2 Messenger Bag Carry Case",
                "url": "https://example.test/bag",
                "model_key": "nintendo-switch-2",
                "raw_attrs": {"Type": "Carry Case", "Compatible Product": "Nintendo Switch 2 Console"},
                "sale_type": "bin",
                "price_current": 12.0,
                "median_final_price": 390.0,
                "samples": 20,
            },
            {
                "source": "ebay-consoles",
                "external_id": "v1|168601275794|0",
                "title": "Nintendo Switch Console Game - Let's Build a Zoo -",
                "url": "https://example.test/game",
                "model_key": "nintendo-switch-2",
                "sale_type": "bin",
                "price_current": 45.0,
                "median_final_price": 390.0,
                "samples": 20,
            },
            {
                "source": "ebay-graphics-cards",
                "external_id": "v1|227471586676|0",
                "title": "*DAMAGED* NVIDIA GeForce RTX 5090 32GB Graphics Card",
                "url": "https://example.test/gpu",
                "model_key": "nvidia-rtx-5090-32gb",
                "sale_type": "bin",
                "price_current": 600.0,
                "median_final_price": 1800.0,
                "samples": 20,
            },
            {
                "source": "ebay-apple",
                "external_id": "v1|198553558510-mac|0",
                "title": "*DAMAGED* Apple MacBook Pro 14 inch M3 Pro 18GB 512GB",
                "url": "https://example.test/macbook",
                "model_key": "apple-macbook-pro-14in-m3pro-18gb-512gb",
                "sale_type": "bin",
                "price_current": 200.0,
                "median_final_price": 1200.0,
                "samples": 20,
            },
            {
                "source": "ebay-lego",
                "external_id": "v1|177824021246|0",
                "title": "LEGO Castle Byers Exclusive Promo Set 75810",
                "url": "https://example.test/lego",
                "model_key": "lego-set-75810",
                "sale_type": "bin",
                "price_current": 5.0,
                "median_final_price": 170.0,
                "samples": 20,
            },
            {
                "source": "ebay-apple",
                "external_id": "v1|198553558510-phone|0",
                "title": "Apple iPhone 15 Pro 512GB",
                "url": "https://example.test/iphone",
                "model_key": "apple-iphone-15-pro-512gb",
                "sale_type": "auction",
                "end_time": now + timedelta(hours=86),
                "price_current": 1.0,
                "median_final_price": 700.0,
                "samples": 20,
            },
        ]

        potentials, closest, stats = build_potentials(dirty_rows, now=now)

        self.assertEqual([p.external_id for p in potentials], ["v1|198553558510-phone|0"])
        self.assertEqual(closest[0].external_id, "v1|198553558510-phone|0")
        self.assertEqual(stats["blocked_identity"], 7)
        self.assertEqual(stats["checked"], 1)

    def test_scratch_typo_switch_screen_damage_does_not_reach_punt_list(self):
        potentials, closest, stats = build_potentials(
            [
                {
                    "source": "ebay-consoles",
                    "external_id": "v1|267569109985|0",
                    "title": "Nintendo Switch 32GB Home Console - Black (HAC-001(01 )*SCRATHED SCREEN*",
                    "url": "https://example.test/scrathed",
                    "model_key": "nintendo-switch-base",
                    "sale_type": "bin",
                    "price_current": 66.0,
                    "median_final_price": 114.70,
                    "samples": 203,
                }
            ],
            min_profit=25,
        )

        self.assertEqual(potentials, [])
        self.assertEqual(closest, [])
        self.assertEqual(stats["blocked_identity"], 1)

    def test_future_auction_can_still_be_scored(self):
        now = datetime(2026, 8, 11, 12, 0, tzinfo=timezone.utc)
        potentials, closest, stats = build_potentials(
            [
                {
                    "source": "ebay-apple",
                    "external_id": "v1|mature|0",
                    "title": "Apple iPhone 15 Pro 512GB",
                    "url": "https://example.test/future",
                    "model_key": "apple-iphone-15-pro-512gb",
                    "sale_type": "auction",
                    "end_time": now + timedelta(hours=86),
                    "price_current": 100.0,
                    "median_final_price": 700.0,
                    "samples": 20,
                }
            ],
            now=now,
        )

        self.assertEqual(len(potentials), 1)
        self.assertEqual(closest[0].external_id, "v1|mature|0")

    def test_build_potentials_counts_low_comp_rows_without_returning_them(self):
        potentials, closest, stats = build_potentials(
            [
                {
                    "source": "ebay-tools",
                    "external_id": "v1|thin|0",
                    "title": "Paslode IM65A Nail Gun",
                    "url": "https://example.test/thin",
                    "model_key": "paslode-im65a-74v-kit",
                    "price_current": 100.0,
                    "median_final_price": 280.0,
                    "samples": 1,
                }
            ],
            min_comps=3,
        )

        self.assertEqual(potentials, [])
        self.assertEqual(closest, [])
        self.assertEqual(stats["below_min_comps"], 1)

    def test_category_specific_comp_floor_allows_samsung_at_five(self):
        potentials, _closest, stats = build_potentials(
            [
                {
                    "source": "ebay-samsung",
                    "external_id": "v1|fold|0",
                    "title": "Samsung Galaxy Z Fold 7 512GB",
                    "url": "https://example.test/fold",
                    "model_key": "samsung-galaxy-z-fold7-512gb",
                    "condition_grade": "B",
                    "price_current": 100.0,
                    "median_final_price": 800.0,
                    "samples": 5,
                }
            ]
        )

        self.assertEqual(len(potentials), 1)
        self.assertEqual(potentials[0].effective_min_comps, 5)
        self.assertEqual(stats["below_min_comps"], 0)

    def test_category_specific_comp_floor_keeps_lego_at_fifteen(self):
        potentials, _closest, stats = build_potentials(
            [
                {
                    "source": "ebay-lego",
                    "external_id": "v1|lego|0",
                    "title": "LEGO Set 75810 Complete",
                    "url": "https://example.test/lego",
                    "model_key": "lego-set-75810",
                    "condition_grade": "B",
                    "price_current": 100.0,
                    "median_final_price": 800.0,
                    "samples": 14,
                }
            ]
        )

        self.assertEqual(potentials, [])
        self.assertEqual(stats["below_min_comps"], 1)

    def test_explicit_min_comps_override_applies_globally(self):
        potentials, _closest, _stats = build_potentials(
            [
                {
                    "source": "ebay-lego",
                    "external_id": "v1|lego|0",
                    "title": "LEGO Set 75810 Complete",
                    "url": "https://example.test/lego",
                    "model_key": "lego-set-75810",
                    "condition_grade": "B",
                    "price_current": 100.0,
                    "median_final_price": 800.0,
                    "samples": 3,
                }
            ],
            min_comps=3,
        )

        self.assertEqual(len(potentials), 1)
        self.assertEqual(potentials[0].effective_min_comps, 3)

    def test_effective_min_comps_for_source_uses_fallback_and_override(self):
        self.assertEqual(effective_min_comps_for_source("ebay-apple"), 10)
        self.assertEqual(effective_min_comps_for_source("ebay-samsung"), 5)
        self.assertEqual(effective_min_comps_for_source("ebay-graphics-cards"), 5)
        self.assertEqual(effective_min_comps_for_source("ebay-consoles"), 15)
        self.assertEqual(effective_min_comps_for_source("unknown-source"), 15)
        self.assertEqual(effective_min_comps_for_source("ebay-apple", override=3), 3)

    def test_useful_near_misses_zero_limit_means_all(self):
        rows = [
            {"expected_profit": 45, "max_safe_buy": 10, "current_price": 10},
            {"expected_profit": 44, "max_safe_buy": 9, "current_price": 8},
            {"expected_profit": 1, "max_safe_buy": 1, "current_price": 1},
        ]

        useful = useful_near_misses(rows, min_profit=50, limit=0)

        self.assertEqual(useful, rows[:2])

    def test_useful_near_misses_exclude_rows_already_above_max_buy(self):
        rows = [
            {"expected_profit": 20, "max_safe_buy": 45, "current_price": 53},
            {"expected_profit": 20, "max_safe_buy": 45, "current_price": 45},
        ]

        useful = useful_near_misses(rows, min_profit=25, limit=0)

        self.assertEqual(useful, rows[1:])


if __name__ == "__main__":
    unittest.main()
