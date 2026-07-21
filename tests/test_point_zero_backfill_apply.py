import unittest

from pipelines.listing.point_zero.backfill_apply import _classify_trading_snapshot


class PointZeroBackfillApplyTests(unittest.TestCase):
    """Classifier tests for safe stale-listing finalization backfills."""

    def _classify(self, parsed):
        return _classify_trading_snapshot(
            listing_id=123,
            external_id="v1|999999999999|0",
            source="ebay-apple",
            model_key="apple-iphone-11-64gb_b",
            title="Apple iPhone 11 64GB",
            parsed=parsed,
        )

    def test_ended_with_bids_and_price_becomes_sold(self):
        outcome = self._classify(
            {
                "ended": True,
                "sold_flag": True,
                "final_price": 121.5,
                "bid_count": 7,
            }
        )

        self.assertEqual(outcome.action, "finalize")
        self.assertEqual(outcome.new_status, "sold")
        self.assertEqual(outcome.final_price, 121.5)
        self.assertEqual(outcome.bid_count, 7)

    def test_ended_without_confirmed_sale_becomes_ended(self):
        outcome = self._classify(
            {
                "ended": True,
                "sold_flag": False,
                "final_price": 99.0,
                "bid_count": 0,
            }
        )

        self.assertEqual(outcome.action, "finalize")
        self.assertEqual(outcome.new_status, "ended")
        self.assertIsNone(outcome.final_price)

    def test_item_not_found_is_finalized_as_ended_not_sold(self):
        outcome = self._classify({"code": "1505"})

        self.assertEqual(outcome.action, "finalize")
        self.assertEqual(outcome.new_status, "ended")
        self.assertIsNone(outcome.final_price)

    def test_live_trading_response_is_left_unchanged(self):
        outcome = self._classify(
            {
                "ended": False,
                "sold_flag": False,
                "final_price": 52.0,
                "bid_count": 0,
            }
        )

        self.assertEqual(outcome.action, "skip")
        self.assertEqual(outcome.new_status, "skipped")
        self.assertEqual(outcome.reason, "trading_says_not_ended")

    def test_unparseable_response_is_error_without_update(self):
        outcome = self._classify(None)

        self.assertEqual(outcome.action, "skip")
        self.assertEqual(outcome.new_status, "error")
        self.assertEqual(outcome.reason, "unparseable_trading_response")


if __name__ == "__main__":
    unittest.main()
