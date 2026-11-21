import unittest
from unittest.mock import patch

from pipelines.listing.point_zero.backfill_plan import (
    _fetch_unknown_backfill_candidate_page,
    unknown_backfill_candidates,
)


class PointZeroBackfillPlanTests(unittest.TestCase):
    def test_candidates_prioritize_sale_likelihood_before_comp_tier(self):
        class CaptureCursor:
            def execute(self, sql, params):
                self.sql = sql
                self.params = params

            def fetchall(self):
                return []

        cur = CaptureCursor()

        rows = _fetch_unknown_backfill_candidate_page(
            cur,
            limit=50,
            offset=0,
            excluded_sources=("ebay-motors",),
        )

        self.assertEqual(rows, [])
        order_by = cur.sql.split("FROM eligible", 1)[1].split("LIMIT %s", 1)[0]
        self.assertLess(order_by.index("stale_end_rank"), order_by.index("sale_likelihood_rank"))
        self.assertLess(order_by.index("sale_likelihood_rank"), order_by.index("tier_rank"))
        self.assertIn("WHEN COALESCE(l.bids_count, 0) > 0 THEN 0", cur.sql)
        self.assertIn("WHEN l.sale_type IN ('bin', 'both') THEN 1", cur.sql)

    def test_backfill_uses_known_key_queue_not_live_buy_guard(self):
        rows = [
            {
                "id": 1,
                "external_id": "v1|111111111111|0",
                "source": "ebay-tools",
                "model_key": "hilti-scw22a-22v-bare_d",
                "title": "Hilti SCW 22-A 22V Cordless Circular Saw Bare Unit",
            },
            {
                "id": 2,
                "external_id": "v1|222222222222|0",
                "source": "ebay-watches",
                "model_key": "seiko-sur527p1new-quartz-fullset_b",
                "title": "Seiko SUR527P1 New Quartz Full Set",
            },
        ]

        cur = object()

        with patch(
            "pipelines.listing.point_zero.backfill_plan._fetch_unknown_backfill_candidate_page",
            return_value=rows,
        ) as fetch:
            candidates = unknown_backfill_candidates(
                cur,
                limit=2,
                excluded_sources=("ebay-motors",),
            )

        self.assertEqual(candidates, rows)
        fetch.assert_called_once_with(
            cur,
            limit=2,
            offset=0,
            excluded_sources=("ebay-motors",),
        )


if __name__ == "__main__":
    unittest.main()
