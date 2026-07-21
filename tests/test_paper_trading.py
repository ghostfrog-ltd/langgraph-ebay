import unittest
from datetime import datetime, timezone

from pipelines.listing.paper_trading.graph import (
    classify_paper_trade_lesson,
    classify_observed_outcome,
    classify_trading_resolution,
    snapshot_from_opportunity,
)
from pipelines.listing.roi.graph import Opportunity


class PaperTradingTests(unittest.TestCase):
    """Pure logic tests for review-only paper-trading predictions."""

    def test_snapshot_records_review_only_prediction_fields(self):
        opportunity = Opportunity(
            source="ebay-apple",
            external_id="v1|123|0",
            title="Apple iPad 9th Gen",
            url="https://example.test/item",
            model_key="apple-ipad-9th-64gb_b",
            comps_samples=12,
            comps_median=230.0,
            purchase_cost=115.0,
            outbound_ship=7.0,
            fees=29.9,
            profit=78.1,
            roi=0.679,
            end_time=datetime(2026, 5, 28, tzinfo=timezone.utc),
            time_left_s=3600,
        )

        snapshot = snapshot_from_opportunity(opportunity)

        self.assertEqual(snapshot.external_id, "v1|123|0")
        self.assertEqual(snapshot.predicted_resale_price, 230.0)
        self.assertEqual(snapshot.predicted_inbound_shipping, 5.0)
        self.assertEqual(snapshot.predicted_packaging, 2.0)
        self.assertEqual(snapshot.predicted_selling_reserve, 3.0)
        self.assertEqual(snapshot.predicted_profit, 68.1)
        self.assertEqual(snapshot.recommended_max_bid, 133.1)

    def test_sold_listing_inside_max_bid_gets_purchase_side_profit(self):
        outcome = classify_observed_outcome(
            {
                "id": 10,
                "listing_status": "sold",
                "final_price": 120.0,
                "current_price": 115.0,
                "predicted_resale_price": 230.0,
                "predicted_fees": 29.9,
                "predicted_outbound_shipping": 7.0,
                "predicted_inbound_shipping": 5.0,
                "predicted_packaging": 2.0,
                "predicted_selling_reserve": 3.0,
                "recommended_max_bid": 133.1,
            }
        )

        self.assertEqual(outcome.decision_status, "closed")
        self.assertEqual(outcome.observed_status, "sold")
        self.assertEqual(outcome.observed_purchase_price, 120.0)
        self.assertTrue(outcome.would_have_bid)
        self.assertTrue(outcome.would_have_won)
        self.assertEqual(outcome.observed_price_error, -13.1)
        self.assertEqual(outcome.observed_paper_profit, 63.1)
        self.assertEqual(outcome.outcome_reason, "confirmed_purchase_price_within_max_bid")

    def test_sold_listing_above_max_bid_closes_as_disciplined_no_buy(self):
        outcome = classify_observed_outcome(
            {
                "id": 13,
                "listing_status": "sold",
                "final_price": 150.0,
                "current_price": 115.0,
                "predicted_resale_price": 230.0,
                "predicted_fees": 29.9,
                "predicted_outbound_shipping": 7.0,
                "predicted_inbound_shipping": 5.0,
                "predicted_packaging": 2.0,
                "predicted_selling_reserve": 3.0,
                "recommended_max_bid": 133.1,
            }
        )

        self.assertEqual(outcome.decision_status, "closed")
        self.assertEqual(outcome.observed_status, "sold")
        self.assertFalse(outcome.would_have_won)
        self.assertIsNone(outcome.observed_paper_profit)
        self.assertEqual(outcome.outcome_reason, "final_price_above_max_bid")

    def test_ended_without_final_price_closes_without_fake_sale(self):
        outcome = classify_observed_outcome(
            {
                "id": 11,
                "listing_status": "ended",
                "final_price": None,
                "current_price": 115.0,
                "predicted_resale_price": 230.0,
                "predicted_fees": 29.9,
                "predicted_outbound_shipping": 7.0,
                "recommended_max_bid": 133.1,
            }
        )

        self.assertEqual(outcome.decision_status, "closed")
        self.assertEqual(outcome.observed_status, "ended")
        self.assertIsNone(outcome.observed_final_price)
        self.assertEqual(outcome.outcome_reason, "terminal_without_confirmed_sale")

    def test_live_listing_stays_watching(self):
        outcome = classify_observed_outcome(
            {
                "id": 12,
                "listing_status": "live",
                "final_price": None,
                "current_price": 115.0,
                "predicted_resale_price": 230.0,
                "predicted_fees": 29.9,
                "predicted_outbound_shipping": 7.0,
                "recommended_max_bid": 133.1,
            }
        )

        self.assertEqual(outcome.decision_status, "watching")
        self.assertEqual(outcome.outcome_reason, "listing_still_open")

    def test_guardrail_invalidated_prediction_closes_without_api_outcome(self):
        outcome = classify_observed_outcome(
            {
                "id": 14,
                "source": "ebay-consoles",
                "title": "Capcom Arcade Stadium 1+2 (Nintendo Switch Code In A Box)",
                "model_key": "switch_b",
                "listing_status": "live",
                "final_price": None,
                "current_price": 14.0,
                "predicted_resale_price": 119.34,
                "predicted_fees": 15.51,
                "predicted_outbound_shipping": 7.0,
                "recommended_max_bid": 36.83,
            }
        )

        self.assertEqual(outcome.decision_status, "closed")
        self.assertEqual(outcome.observed_status, "guardrail_blocked")
        self.assertFalse(outcome.would_have_bid)
        self.assertEqual(outcome.outcome_reason, "guardrail_invalidated_before_outcome")

    def test_lesson_flags_paper_loss(self):
        lesson = classify_paper_trade_lesson(
            {
                "external_id": "v1|loss|0",
                "source": "ebay-apple",
                "title": "Apple iPhone",
                "model_key": "apple-iphone-11-64gb_b",
                "decision_status": "closed",
                "observed_status": "sold",
                "predicted_resale_price": 120.0,
                "predicted_profit": 50.0,
                "observed_paper_profit": -12.0,
                "observed_price_error": -55.0,
                "outcome_reason": "confirmed_final_price",
            }
        )

        self.assertEqual(lesson.lesson_category, "paper_loss")
        self.assertIn("paper loss", lesson.learning_hint)

    def test_lesson_flags_outbid_above_ceiling(self):
        lesson = classify_paper_trade_lesson(
            {
                "external_id": "v1|outbid|0",
                "source": "ebay-apple",
                "title": "Apple iPad",
                "model_key": "apple-ipad-9th-64gb_b",
                "decision_status": "closed",
                "observed_status": "sold",
                "predicted_resale_price": 200.0,
                "predicted_profit": 75.0,
                "observed_paper_profit": None,
                "observed_price_error": 16.9,
                "would_have_won": False,
                "outcome_reason": "final_price_above_max_bid",
            }
        )

        self.assertEqual(lesson.lesson_category, "outbid_above_ceiling")

    def test_lesson_flags_no_confirmed_sale(self):
        lesson = classify_paper_trade_lesson(
            {
                "external_id": "v1|ended|0",
                "source": "ebay-apple",
                "title": "Apple MacBook",
                "model_key": "apple-macbook_b",
                "decision_status": "closed",
                "observed_status": "ended",
                "predicted_resale_price": 300.0,
                "predicted_profit": 80.0,
                "observed_paper_profit": None,
                "observed_price_error": None,
                "outcome_reason": "terminal_without_confirmed_sale",
            }
        )

        self.assertEqual(lesson.lesson_category, "no_confirmed_sale")

    def test_lesson_flags_guardrail_invalidated(self):
        lesson = classify_paper_trade_lesson(
            {
                "external_id": "v1|guardrail|0",
                "source": "ebay-consoles",
                "title": "Nintendo Switch Code In A Box",
                "model_key": "switch_b",
                "decision_status": "closed",
                "observed_status": "guardrail_blocked",
                "predicted_resale_price": 120.0,
                "predicted_profit": 72.0,
                "observed_paper_profit": None,
                "observed_price_error": None,
                "outcome_reason": "guardrail_invalidated_before_outcome",
            }
        )

        self.assertEqual(lesson.lesson_category, "guardrail_invalidated")

    def test_trading_resolution_closes_sold_with_final_price(self):
        resolution = classify_trading_resolution(
            prediction_id=1,
            external_id="v1|123|0",
            listing_id=99,
            parsed={
                "ended": True,
                "sold_flag": True,
                "final_price": 220.0,
                "bid_count": 8,
            },
        )

        self.assertEqual(resolution.action, "close")
        self.assertEqual(resolution.listing_status, "sold")
        self.assertEqual(resolution.final_price, 220.0)

    def test_trading_resolution_closes_ended_without_final_price(self):
        resolution = classify_trading_resolution(
            prediction_id=1,
            external_id="v1|123|0",
            listing_id=99,
            parsed={
                "ended": True,
                "sold_flag": False,
                "final_price": 220.0,
                "bid_count": 0,
            },
        )

        self.assertEqual(resolution.action, "close")
        self.assertEqual(resolution.listing_status, "ended")
        self.assertIsNone(resolution.final_price)

    def test_trading_resolution_keeps_still_live_listing_watching(self):
        resolution = classify_trading_resolution(
            prediction_id=1,
            external_id="v1|123|0",
            listing_id=99,
            parsed={
                "ended": False,
                "sold_flag": False,
                "final_price": 150.0,
                "bid_count": 0,
            },
        )

        self.assertEqual(resolution.action, "keep_watching")
        self.assertEqual(resolution.reason, "trading_says_not_ended")


if __name__ == "__main__":
    unittest.main()
