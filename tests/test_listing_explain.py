import unittest

from pipelines.listing.explain import ListingExplanation, format_listing_explanation


class ListingExplainTests(unittest.TestCase):
    def test_format_staggers_target_and_comp_rows(self):
        explanation = ListingExplanation(
            target={
                "source": "ebay-apple",
                "external_id": "v1|206446095377|0",
                "title": "Apple iPhone 15 Pro 128GB Black Unlocked",
                "url": "https://example.test/live",
                "status": "live",
                "model_key": "apple-iphone-15-pro-128gb",
                "condition_grade": "B",
            },
            potential={
                "current_price": 199.0,
                "sold_median": 315.0,
                "comp_samples": 2,
                "max_safe_buy": 207.05,
                "expected_profit": 58.05,
                "expected_roi": 0.2917,
            },
            comps=[
                {
                    "source": "ebay-apple",
                    "external_id": "v1|1|0",
                    "title": "Apple iPhone 15 Pro Black 128GB",
                    "url": "https://example.test/sold-1",
                    "sale_type": "auction",
                    "bids_count": 9,
                    "price_current": 305.0,
                    "final_price": 310.0,
                    "end_time": "2026-07-20 12:00:00+00:00",
                },
                {
                    "source": "ebay-apple",
                    "external_id": "v1|2|0",
                    "title": "iPhone 15 Pro 128GB Unlocked",
                    "url": "https://example.test/sold-2",
                    "sale_type": "bin",
                    "bids_count": 0,
                    "price_current": 330.0,
                    "final_price": 320.0,
                    "end_time": "2026-07-22 12:00:00+00:00",
                },
            ],
            comp_count=2,
            comp_window_days=365,
        )

        text = format_listing_explanation(explanation)

        self.assertIn("Listing profit explanation", text)
        self.assertIn("Apple iPhone 15 Pro 128GB Black Unlocked", text)
        self.assertIn("bucket: apple-iphone-15-pro-128gb | grade B", text)
        self.assertIn("current GBP 199.00 | estimated resale median GBP 315.00 | comps 2", text)
        self.assertIn("max safe buy GBP 207.05 | expected profit GBP 58.05 | ROI 29%", text)
        self.assertIn("Exact sold comps nearest to the estimate median", text)
        self.assertIn("  |- Apple iPhone 15 Pro Black 128GB", text)
        self.assertIn(
            "ebay-apple | v1|1|0 | type auction | bids 9 | last GBP 305.00 | exact sold GBP 310.00",
            text,
        )
        self.assertIn("  `- iPhone 15 Pro 128GB Unlocked", text)
        self.assertIn("type bin | bids 0 | last GBP 330.00 | exact sold GBP 320.00", text)

    def test_format_error_keeps_target_context(self):
        explanation = ListingExplanation(
            target={
                "external_id": "v1|206446095377|0",
                "title": "Unknown listing",
                "model_key": "unknown",
                "condition_grade": "B",
            },
            potential=None,
            comps=[],
            comp_count=0,
            comp_window_days=365,
            error="listing has no clean model_key, so no same-product comps can explain it",
        )

        text = format_listing_explanation(explanation)

        self.assertIn("Cannot explain", text)
        self.assertIn("Unknown listing", text)
        self.assertIn("listing has no clean model_key", text)


if __name__ == "__main__":
    unittest.main()
