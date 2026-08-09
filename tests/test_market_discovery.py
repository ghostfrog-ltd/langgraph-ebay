import unittest

from pipelines.listing.market_discovery.graph import (
    _fetch_live_listing_comp_rows,
    build_potentials,
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

    def test_default_candidate_query_only_uses_live_rows(self):
        cur = _QueryCursor()

        _fetch_live_listing_comp_rows(cur)

        self.assertIn("a.status = 'live'", cur.sql)
        self.assertNotIn("a.status = 'unknown'", cur.sql)
        self.assertIn("listing_review_actions", cur.sql)
        self.assertIn("review.action = 'down'", cur.sql)

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
        self.assertEqual(cur.params, ["ebay-pokemon"])

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
