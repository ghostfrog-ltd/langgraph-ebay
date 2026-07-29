import unittest
from types import SimpleNamespace
from unittest.mock import patch

import requests
from pipelines.listing.ended.graph import _parse_trading_get_item
from pipelines.listing.point_zero.backfill_apply import (
    _classify_trading_snapshot,
    run_backfill,
)


class PointZeroBackfillApplyTests(unittest.TestCase):
    """Classifier tests for safe unknown-listing finalization backfills."""

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

    def test_fixed_price_quantity_sold_becomes_sold_with_clear_reason(self):
        outcome = self._classify(
            {
                "ended": True,
                "sold_flag": True,
                "final_price": 149.99,
                "bid_count": 0,
                "quantity_sold": 1,
            }
        )

        self.assertEqual(outcome.action, "finalize")
        self.assertEqual(outcome.new_status, "sold")
        self.assertEqual(outcome.final_price, 149.99)
        self.assertEqual(outcome.bid_count, 0)
        self.assertEqual(outcome.reason, "fixed_price_quantity_sold")

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

    def test_item_not_found_is_deleted_not_sold(self):
        outcome = self._classify({"code": "1505"})

        self.assertEqual(outcome.action, "delete")
        self.assertEqual(outcome.new_status, "deleted")
        self.assertIsNone(outcome.final_price)
        self.assertEqual(outcome.reason, "trading_item_not_found_1505")

    def test_inaccessible_deleted_item_is_deleted_locally(self):
        outcome = self._classify({"code": "17"})

        self.assertEqual(outcome.action, "delete")
        self.assertEqual(outcome.new_status, "deleted")
        self.assertIsNone(outcome.final_price)
        self.assertEqual(outcome.reason, "trading_item_not_found_17")

    def test_trading_parser_preserves_inaccessible_error_code(self):
        parsed = _parse_trading_get_item(
            """<?xml version="1.0" encoding="UTF-8"?>
            <GetItemResponse xmlns="urn:ebay:apis:eBLBaseComponents">
              <Ack>Failure</Ack>
              <Errors>
                <ShortMessage>Item can't be accessed.</ShortMessage>
                <LongMessage>This item cannot be accessed because the listing has been deleted or you are not the seller.</LongMessage>
                <ErrorCode>17</ErrorCode>
              </Errors>
            </GetItemResponse>
            """
        )

        self.assertEqual(parsed, {"code": "17"})

    def test_trading_parser_treats_quantity_sold_as_fixed_price_sale(self):
        parsed = _parse_trading_get_item(
            """<?xml version="1.0" encoding="UTF-8"?>
            <GetItemResponse xmlns="urn:ebay:apis:eBLBaseComponents">
              <Ack>Success</Ack>
              <Item>
                <SellingStatus>
                  <ListingStatus>Completed</ListingStatus>
                  <BidCount>0</BidCount>
                  <QuantitySold>1</QuantitySold>
                  <CurrentPrice currencyID="GBP">149.99</CurrentPrice>
                </SellingStatus>
                <ListingDetails>
                  <EndTime>2026-07-20T12:00:00.000Z</EndTime>
                </ListingDetails>
              </Item>
            </GetItemResponse>
            """
        )

        self.assertTrue(parsed["ended"])
        self.assertTrue(parsed["sold_flag"])
        self.assertEqual(parsed["final_price"], 149.99)
        self.assertEqual(parsed["bid_count"], 0)
        self.assertEqual(parsed["quantity_sold"], 1)

    def test_live_trading_response_restores_unknown_listing_to_live(self):
        outcome = self._classify(
            {
                "ended": False,
                "sold_flag": False,
                "final_price": 52.0,
                "bid_count": 0,
                "ebay_end_time": "2026-07-22T12:00:00.000Z",
            }
        )

        self.assertEqual(outcome.action, "restore")
        self.assertEqual(outcome.new_status, "live")
        self.assertEqual(outcome.reason, "trading_says_not_ended")
        self.assertEqual(outcome.ebay_end_time, "2026-07-22T12:00:00.000Z")

    def test_unparseable_response_is_error_without_update(self):
        outcome = self._classify(None)

        self.assertEqual(outcome.action, "skip")
        self.assertEqual(outcome.new_status, "error")
        self.assertEqual(outcome.reason, "unparseable_trading_response")

    def test_backfill_timeout_is_recorded_and_batch_continues(self):
        candidates = [
            {
                "id": 1,
                "external_id": "v1|111111111111|0",
                "source": "ebay-apple",
                "model_key": "apple-iphone-15-128gb",
                "title": "Timeout row",
            },
            {
                "id": 2,
                "external_id": "v1|222222222222|0",
                "source": "ebay-apple",
                "model_key": "apple-iphone-15-128gb",
                "title": "Sold row",
            },
        ]
        captured_outcomes = []

        def fake_call(item_id):
            if item_id == "111111111111":
                raise requests.ConnectTimeout("timed out")
            return SimpleNamespace(text="<xml/>")

        def fake_apply(outcomes):
            captured_outcomes.extend(outcomes)
            return 1

        with patch(
            "pipelines.listing.point_zero.backfill_apply.backfill_checks_today",
            return_value=0,
        ), patch(
            "pipelines.listing.point_zero.backfill_apply._load_candidates_from_plan",
            return_value=(candidates, {"api_budget": {}}),
        ), patch(
            "pipelines.listing.point_zero.backfill_apply.increment_api_usage",
        ) as usage, patch(
            "pipelines.listing.point_zero.backfill_apply._call_trading_get_item",
            side_effect=fake_call,
        ), patch(
            "pipelines.listing.point_zero.backfill_apply._parse_trading_get_item",
            return_value={
                "ended": True,
                "sold_flag": True,
                "final_price": 199.99,
                "bid_count": 3,
            },
        ), patch(
            "pipelines.listing.point_zero.backfill_apply._apply_outcomes",
            side_effect=fake_apply,
        ):
            summary = run_backfill(limit=2, apply=True, daily_backfill_budget=10)

        self.assertEqual(summary["outcome_count"], 2)
        self.assertEqual(summary["applied"], 1)
        self.assertEqual(usage.call_count, 2)
        self.assertEqual(captured_outcomes[0].action, "skip")
        self.assertEqual(captured_outcomes[0].new_status, "error")
        self.assertEqual(captured_outcomes[0].reason, "trading_api_timeout")
        self.assertEqual(captured_outcomes[1].new_status, "sold")
        self.assertIn("skip:error:trading_api_timeout", summary["counts"])
        self.assertIn("finalize:sold:ended_with_bids", summary["counts"])


if __name__ == "__main__":
    unittest.main()
