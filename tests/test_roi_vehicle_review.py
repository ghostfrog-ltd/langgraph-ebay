import unittest

from pipelines.listing.roi.graph import _requires_vehicle_roi_review


class RoiVehicleReviewGateTests(unittest.TestCase):
    """Guardrail tests for vehicle-like ROI candidates."""

    def test_motomine_is_review_only_by_default(self):
        self.assertTrue(
            _requires_vehicle_roi_review(
                source="motomine",
                title="Lot#66848 2015 YAMAHA YZF-R125 ABS -SPARES/REPAIR",
                model_key="yamaha-yzfr125-125cc_b",
            )
        )

    def test_mini_source_is_review_only_by_default(self):
        self.assertTrue(
            _requires_vehicle_roi_review(
                source="ebay-mini-cooper",
                title="Mini Cooper project",
                model_key="mini-cooper_b",
            )
        )

    def test_apple_listing_is_not_vehicle_review_only(self):
        self.assertFalse(
            _requires_vehicle_roi_review(
                source="ebay-apple",
                title="Apple MacBook Air 13 inch M1",
                model_key="apple-macbook-air-13in-m1-8gb-256gb_b",
            )
        )


if __name__ == "__main__":
    unittest.main()
