import unittest

from pipelines.listing.market_discovery.graph import (
    MarketLane,
    build_lanes,
    score_lane,
)


class MarketDiscoveryTests(unittest.TestCase):
    """Pure tests for read-only market discovery lane scoring."""

    def test_strong_lane_is_paper_trade_candidate(self):
        score = score_lane(
            MarketLane(
                source="ebay-apple",
                model_key="apple-macbook-air-13in-m1-8gb-256gb_b",
                active_count=4,
                active_median_price=120.0,
                active_min_price=95.0,
                comp_samples=42,
                comp_median_price=304.37,
                guardrail_blocked_count=0,
            )
        )

        self.assertEqual(score.status, "paper_trade")
        self.assertGreater(score.estimated_net_spread, 100)
        self.assertEqual(score.risk_flags, [])

    def test_thin_comps_are_rejected(self):
        score = score_lane(
            MarketLane(
                source="ebay-cameras",
                model_key="gopro-hero-12_b",
                active_count=5,
                active_median_price=180.0,
                active_min_price=150.0,
                comp_samples=2,
                comp_median_price=310.0,
                guardrail_blocked_count=0,
            )
        )

        self.assertEqual(score.status, "rejected")
        self.assertIn("thin_confirmed_comps", score.risk_flags)

    def test_high_guardrail_share_parks_lane(self):
        score = score_lane(
            MarketLane(
                source="ebay-consoles",
                model_key="switch_b",
                active_count=4,
                active_median_price=25.0,
                active_min_price=14.0,
                comp_samples=20,
                comp_median_price=160.0,
                guardrail_blocked_count=2,
            )
        )

        self.assertEqual(score.status, "parked")
        self.assertIn("high_false_positive_risk", score.risk_flags)

    def test_vehicle_like_sources_are_parked_until_costs_are_modelled(self):
        score = score_lane(
            MarketLane(
                source="motomine",
                model_key="yamaha-yzfr125-125cc_b",
                active_count=3,
                active_median_price=100.0,
                active_min_price=50.0,
                comp_samples=12,
                comp_median_price=1400.0,
                guardrail_blocked_count=0,
            )
        )

        self.assertEqual(score.status, "parked")
        self.assertIn("vehicle_logistics_not_modelled", score.risk_flags)

    def test_build_lanes_keeps_guardrail_risk_as_signal(self):
        lanes = build_lanes(
            [
                {
                    "source": "ebay-consoles",
                    "model_key": "switch_b",
                    "title": "Nintendo Switch Console",
                    "price_current": 80.0,
                },
                {
                    "source": "ebay-consoles",
                    "model_key": "switch_b",
                    "title": "Capcom Arcade Stadium Nintendo Switch Code In A Box",
                    "price_current": 14.0,
                },
            ],
            {"switch_b": {"samples": 12, "median_final_price": 185.0}},
        )

        self.assertEqual(len(lanes), 1)
        self.assertEqual(lanes[0].active_count, 2)
        self.assertEqual(lanes[0].guardrail_blocked_count, 1)


if __name__ == "__main__":
    unittest.main()
