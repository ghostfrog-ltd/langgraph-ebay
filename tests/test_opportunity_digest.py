import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import Mock, patch

from pipelines.listing.opportunity_digest import (
    build_review_auction_digest,
    format_review_auction_digest_for_slack,
    _is_fresh_live_listing_for_slack,
    _is_low_interest_for_slack,
    _build_scout_items,
    _select_diverse_opportunities,
)


class OpportunityDigestTests(unittest.TestCase):
    """Review auction digest must stay useful without becoming an action surface."""

    def test_slack_format_is_review_only(self):
        text = format_review_auction_digest_for_slack(
            {
                "status": "ok",
                "count": 2,
                "items": [
                    {
                        "title": "Apple MacBook Air 13in M1 8GB 256GB",
                        "url": "https://example.test/item/1",
                        "current_price": 105,
                        "resale_estimate": 304.37,
                        "estimated_profit": 142.8,
                        "roi_percent": 128.4,
                        "review_ceiling": 229.83,
                        "comp_samples": 8,
                        "ends": "5h 10m",
                        "risk_notes": ["review photos/condition"],
                    },
                    {
                        "title": "Sony PlayStation 4 PS4 Console Only",
                        "url": "https://example.test/item/2",
                        "current_price": 55,
                        "resale_estimate": 95,
                        "estimated_profit": 11.07,
                        "roi_percent": 17.9,
                        "review_ceiling": 16.0,
                        "comp_samples": 62,
                        "ends": "23h 20m",
                        "risk_notes": ["review photos/condition"],
                    },
                ],
            }
        )

        self.assertIn("Review-only auction shortlist", text)
        self.assertIn("Apple MacBook Air", text)
        self.assertIn("Current GBP 105", text)
        self.assertIn("Review cap GBP 230", text)
        self.assertIn("notes: review photos/condition\n\n2. ", text)
        self.assertIn("notes: review photos/condition\n\n_Observer only", text)
        self.assertIn("Observer only", text)
        self.assertNotIn("BUY", text)
        self.assertNotIn("BID", text)
        self.assertNotIn("ACTIONABLE", text)

    def test_build_digest_uses_current_roi_shortlist_and_paper_costs(self):
        now = datetime(2026, 5, 28, 10, 0, tzinfo=timezone.utc)
        opportunity = Mock(
            source="ebay-apple",
            external_id="a1",
            title="Apple MacBook Air 13in M1 8GB 256GB",
            url="https://example.test/item/1",
            model_key="apple-macbook-air-13in-m1-8gb-256gb_b",
            profit=142.8,
            roi=1.284,
            end_time=now + timedelta(hours=6),
        )
        snapshot = Mock(
            current_price=105.0,
            predicted_resale_price=304.37,
            predicted_profit=142.8,
            predicted_roi=1.284,
            recommended_max_bid=229.83,
            comp_samples=8,
        )
        fresh_listing = {
            "status": "live",
            "end_time": now + timedelta(hours=6),
            "last_seen_at": now - timedelta(minutes=5),
        }

        with patch("pipelines.listing.opportunity_digest._fetch_active_listings", return_value=[fresh_listing]):
            with patch("pipelines.listing.opportunity_digest._comps_lookup", return_value={}):
                with patch(
                    "pipelines.listing.opportunity_digest._build_all_opps_for_roi",
                    return_value=[opportunity],
                ):
                    with patch(
                        "pipelines.listing.opportunity_digest.snapshot_from_opportunity",
                        return_value=snapshot,
                    ):
                        digest = build_review_auction_digest(limit=4, now=now)

        self.assertEqual(digest["status"], "ok")
        self.assertEqual(digest["count"], 1)
        self.assertEqual(digest["items"][0]["estimated_profit"], 142.8)
        self.assertEqual(digest["items"][0]["review_ceiling"], 229.83)
        self.assertEqual(digest["items"][0]["ends"], "6h")

    def test_digest_suppresses_low_interest_legacy_laptops(self):
        now = datetime(2026, 5, 28, 10, 0, tzinfo=timezone.utc)
        m1_air = Mock(
            source="ebay-apple",
            external_id="a1",
            title="Apple MacBook Air 13in M1 8GB 256GB",
            url="https://example.test/m1",
            model_key="apple-macbook-air-13in-m1-8gb-256gb_b",
            profit=142.8,
            roi=1.284,
            end_time=now + timedelta(hours=6),
        )
        legacy_intel = Mock(
            source="ebay-apple",
            external_id="a2",
            title='Apple MacBook Pro A1398 Mid-2015 15" Intel i7 16GB',
            url="https://example.test/intel",
            model_key="apple-macbook-pro-154in-intel-16gb-512gb_b",
            profit=100.0,
            roi=0.9,
            end_time=now + timedelta(hours=5),
        )
        pentium_laptop = Mock(
            source="ebay-computers",
            external_id="p1",
            title="HP Laptop Pentium Silver 4GB RAM",
            url="https://example.test/pentium",
            model_key="hp-laptop-pentium_b",
            profit=90.0,
            roi=0.8,
            end_time=now + timedelta(hours=5),
        )
        snapshot = Mock(
            current_price=105.0,
            predicted_resale_price=304.37,
            predicted_profit=142.8,
            predicted_roi=1.284,
            recommended_max_bid=229.83,
            comp_samples=8,
        )
        fresh_listing = {
            "status": "live",
            "end_time": now + timedelta(hours=6),
            "last_seen_at": now - timedelta(minutes=5),
        }

        self.assertFalse(_is_low_interest_for_slack(m1_air))
        self.assertTrue(_is_low_interest_for_slack(legacy_intel))
        self.assertTrue(_is_low_interest_for_slack(pentium_laptop))
        self.assertTrue(
            _is_low_interest_for_slack(
                Mock(
                    title="FAULTY Sony PlayStation 4 PS4 500GB SPARES OR REPAIR",
                    model_key="ps4_b",
                )
            )
        )

        with patch("pipelines.listing.opportunity_digest._fetch_active_listings", return_value=[fresh_listing]):
            with patch("pipelines.listing.opportunity_digest._comps_lookup", return_value={}):
                with patch(
                    "pipelines.listing.opportunity_digest._build_all_opps_for_roi",
                    return_value=[legacy_intel, pentium_laptop, m1_air],
                ):
                    with patch(
                        "pipelines.listing.opportunity_digest.snapshot_from_opportunity",
                        return_value=snapshot,
                    ):
                        digest = build_review_auction_digest(limit=4, now=now)

        self.assertEqual(digest["count"], 1)
        self.assertEqual(digest["filtered_low_interest"], 2)
        self.assertIn("M1", digest["items"][0]["title"])

    def test_digest_requires_recent_live_evidence(self):
        now = datetime(2026, 5, 28, 10, 0, tzinfo=timezone.utc)
        self.assertTrue(
            _is_fresh_live_listing_for_slack(
                {
                    "status": "live",
                    "end_time": now + timedelta(hours=1),
                    "last_seen_at": now - timedelta(minutes=30),
                },
                now,
            )
        )
        self.assertFalse(
            _is_fresh_live_listing_for_slack(
                {
                    "status": "live",
                    "end_time": now + timedelta(hours=1),
                    "last_seen_at": now - timedelta(minutes=61),
                },
                now,
            )
        )
        self.assertFalse(
            _is_fresh_live_listing_for_slack(
                {
                    "status": "live",
                    "end_time": now - timedelta(minutes=1),
                    "last_seen_at": now,
                },
                now,
            )
        )

    def test_diverse_selection_keeps_non_apple_rows_visible(self):
        opportunities = [
            Mock(source="ebay-apple", external_id="a1", profit=150.0, roi=1.4),
            Mock(source="ebay-apple", external_id="a2", profit=80.0, roi=0.7),
            Mock(source="ebay-apple", external_id="a3", profit=60.0, roi=0.5),
            Mock(source="ebay-consoles", external_id="c1", profit=22.0, roi=0.38),
            Mock(source="ebay-consoles", external_id="c2", profit=17.0, roi=0.39),
            Mock(source="ebay-samsung", external_id="s1", profit=9.0, roi=0.12),
        ]

        selected = _select_diverse_opportunities(opportunities, limit=4)
        sources = [op.source for op in selected]

        self.assertEqual(sources.count("ebay-apple"), 2)
        self.assertIn("ebay-consoles", sources)
        self.assertLessEqual(max(sources.count(source) for source in set(sources)), 2)

    def test_main_digest_excludes_non_positive_profit_rows(self):
        now = datetime(2026, 5, 28, 10, 0, tzinfo=timezone.utc)
        positive = Mock(
            source="ebay-consoles",
            external_id="c1",
            title="Sony PlayStation 4 PS4 Console",
            url="https://example.test/ps4",
            model_key="ps4_b",
            profit=11.0,
            roi=0.18,
            end_time=now + timedelta(hours=6),
        )
        negative = Mock(
            source="ebay-apple",
            external_id="a1",
            title="Apple MacBook Air 13in M1 8GB 512GB",
            url="https://example.test/mac",
            model_key="apple-macbook-air-13in-m1-8gb-512gb_b",
            profit=-3.0,
            roi=-0.01,
            end_time=now + timedelta(hours=6),
        )
        snapshot = Mock(
            current_price=55.0,
            predicted_resale_price=95.0,
            predicted_profit=11.0,
            predicted_roi=0.18,
            recommended_max_bid=16.0,
            comp_samples=62,
        )
        fresh_listing = {
            "status": "live",
            "end_time": now + timedelta(hours=6),
            "last_seen_at": now - timedelta(minutes=5),
        }

        with patch("pipelines.listing.opportunity_digest._fetch_active_listings", return_value=[fresh_listing]):
            with patch("pipelines.listing.opportunity_digest._comps_lookup", return_value={}):
                with patch(
                    "pipelines.listing.opportunity_digest._build_all_opps_for_roi",
                    return_value=[negative, positive],
                ):
                    with patch(
                        "pipelines.listing.opportunity_digest.snapshot_from_opportunity",
                        return_value=snapshot,
                    ):
                        digest = build_review_auction_digest(limit=4, now=now)

        self.assertEqual(digest["count"], 1)
        self.assertIn("PlayStation", digest["items"][0]["title"])

    def test_main_digest_excludes_non_positive_paper_profit_rows(self):
        now = datetime(2026, 5, 28, 10, 0, tzinfo=timezone.utc)
        opportunity = Mock(
            source="ebay-consoles",
            external_id="c1",
            title="PlayStation 3 Slim Console",
            url="https://example.test/ps3",
            model_key="ps3_b",
            profit=8.0,
            roi=0.15,
            end_time=now + timedelta(hours=6),
        )
        snapshot = Mock(
            current_price=55.0,
            predicted_resale_price=80.0,
            predicted_profit=-2.0,
            predicted_roi=-0.04,
            recommended_max_bid=3.0,
            comp_samples=47,
        )
        fresh_listing = {
            "status": "live",
            "end_time": now + timedelta(hours=6),
            "last_seen_at": now - timedelta(minutes=5),
        }

        with patch("pipelines.listing.opportunity_digest._fetch_active_listings", return_value=[fresh_listing]):
            with patch("pipelines.listing.opportunity_digest._comps_lookup", return_value={}):
                with patch(
                    "pipelines.listing.opportunity_digest._build_all_opps_for_roi",
                    return_value=[opportunity],
                ):
                    with patch(
                        "pipelines.listing.opportunity_digest.snapshot_from_opportunity",
                        return_value=snapshot,
                    ):
                        digest = build_review_auction_digest(limit=4, now=now)

        self.assertEqual(digest["count"], 0)

    def test_scout_items_show_fresh_non_roi_sources(self):
        now = datetime(2026, 5, 28, 10, 0, tzinfo=timezone.utc)
        listings = [
            {
                "source": "ebay-watches",
                "external_id": "w1",
                "title": "Seiko 5 Sports Automatic Watch",
                "url": "https://example.test/watch",
                "price_current": 167,
                "model_key": "seiko-seiko5-auto-papers_b",
                "end_time": now + timedelta(hours=8),
            },
            {
                "source": "ebay-lego",
                "external_id": "l1",
                "title": "LEGO Ideas A-Frame Cabin 21338",
                "url": "https://example.test/lego",
                "price_current": 169,
                "model_key": "lego-ideas-21338_b-c",
                "end_time": now + timedelta(hours=7),
            },
        ]
        scouts = _build_scout_items(
            listings=listings,
            comps_by_model={
                "seiko-seiko5-auto-papers_b": {
                    "samples": 12,
                    "median_final_price": 65,
                },
                "lego-ideas-21338_b-c": {
                    "samples": 1,
                    "median_final_price": 146,
                },
            },
            selected_opportunities=[],
            now=now,
            limit=4,
        )

        self.assertEqual({scout.source for scout in scouts}, {"ebay-watches", "ebay-lego"})
        text = format_review_auction_digest_for_slack(
            {
                "items": [],
                "scout_items": [
                    {
                        "source": scout.source,
                        "title": scout.title,
                        "url": scout.url,
                        "current_price": scout.current_price,
                        "comp_samples": scout.comp_samples,
                        "reason": scout.reason,
                        "ends": scout.ends,
                    }
                    for scout in scouts
                ],
            }
        )

        self.assertIn("Source scouts", text)
        self.assertIn("ebay-watches", text)
        self.assertIn("ebay-lego", text)
        self.assertIn("not profit-scored", text)


if __name__ == "__main__":
    unittest.main()
