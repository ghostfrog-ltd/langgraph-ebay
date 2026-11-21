from collections import defaultdict
from datetime import datetime, timezone
import os
import unittest
from unittest.mock import MagicMock

from mcp.server.fastmcp.exceptions import ToolError
from psycopg2.extras import Json

from ops import ghostfrog_diagnostics as diagnostics
from ops import ghostfrog_listing_detail as detail


class ListingDetailTests(unittest.TestCase):
    def test_continuation_uses_evidence_sequence_and_reports_byte_limit(self):
        rows = [
            {"evidence_seq": 9, "value": "MacBook Pro"},
            {"evidence_seq": 10, "value": "Battery and display details " * 20},
        ]
        result = detail._page(rows, limit=2, budget=100, after=8, evidence=True)
        self.assertEqual(result["rows"], rows[:1])
        self.assertTrue(result["truncated"])
        self.assertEqual(result["next_after"], 9)
        self.assertEqual(detail._page([], limit=5, budget=100, after=9)["status"], "end_of_section")
        self.assertEqual(detail._page([], limit=5, budget=100, after=0)["status"], "not_collected")

    def test_pricing_uses_stored_same_grade_snapshot_and_respects_flags(self):
        now = datetime.now(timezone.utc)
        listing = {
            "source": "ebay-apple", "title": "Apple iPhone 16 Pro 128GB Unlocked",
            "model_key": "apple-iphone-16-pro-128gb", "effective_condition_grade": "B",
            "identity_risk": False, "text_review_required": False, "visual_review_required": False,
            "status": "live", "end_time": None, "observation_state": "recent",
            "text_condition_checked": False, "visual_condition_checked": False, "price_current": 300,
        }
        cursor = MagicMock()
        cursor.fetchone.return_value = {"samples": 12, "median_final_price": 500, "computed_at": now}
        result = detail._pricing(cursor, listing, {}, now)
        self.assertEqual(cursor.execute.call_args.args[1], (listing["model_key"], "B"))
        self.assertEqual(result["indicative_max_purchase_gbp"], 368)
        self.assertFalse(result["condition_checks_complete"])
        for reason, changed in (
            ("unresolved_identity_or_product_risk", {"identity_risk": True}),
            ("condition_or_operator_review", {"text_review_required": True}),
            ("observation_stale_or_unavailable", {"observation_state": "stale"}),
        ):
            result = detail._pricing(cursor, listing | changed, {}, now)
            self.assertEqual(result["reason"], reason)
            self.assertIsNone(result["indicative_max_purchase_gbp"])

    def test_rejects_non_public_item_and_invalid_pages(self):
        for args in (
            ("1 OR 1 = 1", "overview", 0, 20),
            ("123456789012", "UPDATE", 0, 20),
            ("123456789012", "description", -1, 20),
            ("123456789012", "photos", 0, 200),
            ("123456789012", "overview", 1, 20),
        ):
            with self.assertRaises(ToolError):
                detail.listing_detail(*args)

    def test_comparable_page_preserves_drill_down_identity(self):
        cursor = MagicMock()
        first = {"listing_id": 100, "item_id": "123456789012", "condition_grade": "B", "final_price": 450}
        cursor.fetchall.return_value = [first, {"listing_id": 101}]
        result = detail._comparables(cursor, {"listing_id": 42}, 10, 1, 1000, {"snapshot": None})
        self.assertEqual(cursor.execute.call_args.args[1], (42, 10, 2))
        self.assertEqual(result["rows"], [first])
        self.assertEqual(result["next_after"], 11)
        self.assertIn("item_id and listing_id", result["drill_down"])


@unittest.skipUnless(os.getenv("GF_TEST_DIAGNOSTIC_LOGIN") == "1", "Requires the deployed diagnostics login")
class ListingInspectionIntegrationTests(unittest.TestCase):
    def test_sql_evidence_preserves_long_text_and_public_terms(self):
        title = "Apple MacBook Air M1 A2337 8GB RAM 256GB SSD 13.3 inch"
        long_description = ("MacBook Air 8GB memory and 256GB SSD with a small lid scratch. " * 90).strip()
        photos = ["https://i.ebayimg.com/images/g/example/s-l1600.jpg",
                  "https://i.ebayimg.com/images/g/next/s-l500.webp"]
        attrs = {
            "Model": ["MacBook Air", {"name": "Model", "value": "A2337"},
                      {"name": "Seller Username", "value": "PrivateAlias"}],
            "Features": {"GPU": {"value": "8-core"}, "Contact Email": "hidden@example.test"},
            "Browse description": long_description + " | seller address: 221B Baker Street",
            "Browse subtitle": "M1 8GB",
            "Browse conditionDescription": "A scuff on the lid | IMEI: 123456789123456",
            "Browse imageUrl": photos[0],
            "Browse imageUrls": [photos[0], {"imageUrl": photos[1] + "?access_token=hidden"},
                                 "https://photos.invalid/private.jpg?token=private"],
            "Browse seller username": "public-shop_1",
            "Browse seller feedbackScore": 975,
            "Browse seller feedbackPercentage": "99.3",
            "Browse itemLocation postalCode": "NW1 6XE",
            "Browse itemLocation city": "London",
            "Browse shippingOptions": [
                {"shippingServiceCode": "UK_RoyalMailTracked",
                 "shippingCost": {"value": "2.99", "currency": "GBP", "private": "shipping_private"},
                 "address": "shipping_private"},
            ],
            "Browse returnTerms returnsAccepted": True,
            "Browse returnTerms returnPeriod": "30 DAY",
            "Browse buyer email": "buyer@example.test",
        }
        checks = {"text assessment": {"notes": "M1 identified | phone: +44 7700 900 123",
                                      "api_key": "private_assessment"}, "text risk flags": ["screen_damage"]}
        with diagnostics._connection() as conn, conn.cursor() as cursor:
            cursor.execute(
                "SELECT section,field,value_path,part_index,part_count,value,withheld "
                "FROM public.ghostfrog_listing_evidence(%s,%s,%s,%s) ORDER BY section,evidence_seq",
                (title, "Check hinge | authorization: Bearer fake_private_token", Json(attrs), Json(checks)),
            )
            rows = cursor.fetchall()
            cursor.execute(
                "SELECT public.ghostfrog_marketplace_url(%s,false), public.ghostfrog_marketplace_url(%s,false)",
                ("https://www.ebay.co.uk/itm/item-abc/123456789012?token=secret&var=42",
                 "https://www.ebay.co.uk@private.invalid/itm/123456789012"),
            )
            self.assertEqual(cursor.fetchone(), ("https://www.ebay.co.uk/itm/item-abc/123456789012?var=42", None))
        by_field = defaultdict(list)
        for section, field, path, part, count, value, withheld in rows:
            by_field[field].append((path, part, count, value, withheld))
        self.assertEqual(by_field["title"][0][3], title)
        self.assertEqual([x[3] for x in by_field["Model"]], ["MacBook Air", "A2337"])
        self.assertEqual(by_field["Features"][0][0], ["Features", "GPU", "value"])
        self.assertEqual(by_field["Features"][0][3], "8-core")
        self.assertEqual(by_field["Browse seller username"][0][3], "public-shop_1")
        self.assertEqual(by_field["Browse seller feedbackScore"][0][3], 975)
        self.assertEqual(by_field["Browse returnTerms returnsAccepted"][0][3], True)
        self.assertEqual([x[3] for x in by_field["Browse imageUrls"]], photos + [None])
        self.assertTrue(by_field["Browse imageUrls"][-1][4])
        self.assertEqual(by_field["Browse shippingOptions"][0][0][1], 0)
        self.assertEqual({x[3] for x in by_field["Browse shippingOptions"]}, {"2.99", "GBP", "UK_RoyalMailTracked"})
        chunks = by_field["Browse description"]
        self.assertEqual([x[1] for x in chunks], list(range(1, len(chunks) + 1)))
        self.assertTrue(all(x[2] == len(chunks) and len(x[3]) <= 480 for x in chunks))
        self.assertEqual("".join(x[3] for x in chunks), long_description + " | [redacted]")
        self.assertIn("screen_damage", [x[3] for x in by_field["text risk flags"]])
        output = repr(rows)
        for private in ("221B", "NW1", "hidden@example", "123456789123456", "PrivateAlias",
                        "shipping_private", "fake_private_token", "7700", "private_assessment", "buyer@example"):
            self.assertNotIn(private, output)

    def test_diagnostics_role_still_has_no_raw_reads_or_writes(self):
        with diagnostics._connection() as conn, conn.cursor() as cursor:
            cursor.execute("""
                SELECT has_column_privilege('public.auction_listings','raw_attrs','SELECT'),
                       has_column_privilege('public.auction_listings','url','SELECT'),
                       has_table_privilege('public.listing_inspection_v','SELECT'),
                       has_table_privilege('public.listing_inspection_v','UPDATE'),
                       has_table_privilege('public.listing_inspection_evidence_v','UPDATE')
            """)
            self.assertEqual(cursor.fetchone(), (False, False, True, False, False))
        with self.assertRaises(ToolError):
            diagnostics.query("SELECT * FROM ghostfrog_listing_comparables(42, 0, 50)", 1)

    def test_missing_uncollected_listing_is_explicit(self):
        item_id = "000000000000"
        result = detail.listing_detail(item_id, "overview", 0, 20)
        self.assertEqual(result["status"], "not_collected")
        self.assertNotIn("listing", result)
