import re
import unittest
from collections import deque
from unittest.mock import patch

from pipelines.listing.comps.graph import (
    APPLE_MAC_ALLOWED_CPU_PATTERN,
    CONSOLE_COMP_ACCESSORY_ATTR_PATTERN,
    CONSOLE_COMP_ACCESSORY_FOR_PATTERN,
    CONSOLE_COMP_ACCESSORY_PATTERNS,
    CONSOLE_COMP_HARDWARE_BLOB_PATTERN,
    CONSOLE_COMP_HARDWARE_TITLE_PATTERN,
    CONSOLE_COMP_NON_HARDWARE_PATTERNS,
    ELECTRONICS_DAMAGE_NEGATION_PATTERN,
    ELECTRONICS_DAMAGE_WORD_PATTERN,
    _compute_daily_comps,
)
from pipelines.listing.retrieve.adapters.apple import Adapter as AppleAdapter
from pipelines.listing.retrieve.adapters.ebay_base import (
    EbayAdapterBase,
    _browse_description_attrs,
    _browse_image_urls,
    _browse_item_unavailable_reason,
    _sale_type_from_buying_options,
    _search_sort_for_sale_type,
    bulk_upsert_auction_listings,
)
from utils.condition import APPLE_IPHONE_KEY_PATTERN, APPLE_MACBOOK_NEO_KEY_PATTERN


class _FakeCursor:
    rowcount = 0

    def __init__(self):
        self.calls = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, sql, params=None):
        self.calls.append((sql, params))

    def fetchone(self):
        return (0,)


class _FakeConnection:
    def __init__(self):
        self.cursor_obj = _FakeCursor()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def cursor(self):
        return self.cursor_obj


class _FakeResponse:
    status_code = 200
    text = "{}"

    def json(self):
        return {"itemSummaries": []}


class CompsGraphTests(unittest.TestCase):
    def _pattern_matches(self, text, patterns):
        lowered = text.lower()
        return any(pattern.strip("%") in lowered for pattern in patterns)

    def _console_comp_blocked_by_sql_constants(self, title, attrs_text=""):
        title_text = title.lower()
        blob = f"{title_text} {attrs_text.lower()}".strip()
        hardware_title_pattern = CONSOLE_COMP_HARDWARE_TITLE_PATTERN.replace("\\m", "\\b").replace("\\M", "\\b")
        hardware_blob_pattern = CONSOLE_COMP_HARDWARE_BLOB_PATTERN.replace("\\m", "\\b").replace("\\M", "\\b")
        accessory_attr_pattern = CONSOLE_COMP_ACCESSORY_ATTR_PATTERN.replace("\\m", "\\b").replace("\\M", "\\b")
        accessory_for_pattern = CONSOLE_COMP_ACCESSORY_FOR_PATTERN.replace("\\m", "\\b").replace("\\M", "\\b")
        has_hardware = bool(
            re.search(hardware_title_pattern, title_text)
            or re.search(hardware_blob_pattern, blob)
        )
        return (
            self._pattern_matches(blob, CONSOLE_COMP_NON_HARDWARE_PATTERNS)
            or (
                self._pattern_matches(blob, CONSOLE_COMP_ACCESSORY_PATTERNS)
                and not has_hardware
            )
            or (
                bool(re.search(accessory_attr_pattern, attrs_text.lower()))
                and not has_hardware
            )
            or (
                bool(re.search(accessory_for_pattern, title_text))
                and not has_hardware
            )
        )

    def _electronics_damage_blocked_by_sql_constants(self, text):
        lowered = text.lower()
        damage_pattern = ELECTRONICS_DAMAGE_WORD_PATTERN.replace("\\m", "\\b").replace("\\M", "\\b")
        negation_pattern = ELECTRONICS_DAMAGE_NEGATION_PATTERN.replace("\\m", "\\b").replace("\\M", "\\b")
        return bool(re.search(damage_pattern, lowered)) and not bool(
            re.search(negation_pattern, lowered)
        )

    def test_console_comp_sql_fixture_blocks_switch_accessory_rows(self):
        blocked = [
            ("Nintendo Switch Carry Case Messenger Bag Power A.", "Type Carry Case"),
            ("Nintendo Switch Console Game - Turquoise", ""),
            ("Wireless Controller for Nintendo Switch 2", "Type Controller"),
        ]

        for title, attrs in blocked:
            with self.subTest(title=title):
                self.assertTrue(self._console_comp_blocked_by_sql_constants(title, attrs))

    def test_console_comp_sql_fixture_allows_console_with_controller(self):
        self.assertFalse(
            self._console_comp_blocked_by_sql_constants(
                "Nintendo Switch 2 Console With Wireless Controller",
                "Type Video Game Console",
            )
        )

    def test_electronics_damage_sql_fixture_respects_negation(self):
        self.assertTrue(
            self._electronics_damage_blocked_by_sql_constants(
                "Samsung Galaxy Z Fold 7 256GB Unlocked Blue damaged"
            )
        )
        self.assertFalse(
            self._electronics_damage_blocked_by_sql_constants(
                "Samsung Galaxy S24 Excellent Condition no damage"
            )
        )

    def test_apple_mac_comps_allow_intel_core_keys(self):
        valid_keys = [
            "apple-macbook-pro-a2141-16in-core-i7-16gb-512gb",
            "apple-macbook-air-a2179-133in-core-i5-16gb-1tb",
            "apple-macbook-a1534-12in-core-m3-8gb-256gb",
            "apple-macbook-pro-a2442-14in-m1pro-16gb-1tb",
            "apple-macbook-neo-a3404-13in-a18pro-8gb-512gb",
        ]

        for key in valid_keys:
            with self.subTest(key=key):
                self.assertRegex(key, APPLE_MAC_ALLOWED_CPU_PATTERN)

    def test_apple_mac_comps_still_block_under_specified_mac_keys(self):
        invalid_keys = [
            "apple-macbook-pro-16gb-512gb",
            "apple-macbook-air-13in-8gb-256gb",
            "apple-macbook-neo-a3404-13in-a18pro-16gb-512gb",
        ]

        pattern = re.compile(APPLE_MAC_ALLOWED_CPU_PATTERN)
        for key in invalid_keys:
            with self.subTest(key=key):
                self.assertIsNone(pattern.search(key))

    def test_comps_filter_dirty_raw_attrs_as_well_as_title(self):
        fake_conn = _FakeConnection()

        with patch("pipelines.listing.comps.graph.connection", fake_conn):
            _compute_daily_comps(days=365)

        insert_call = next(
            call for call in fake_conn.cursor_obj.calls if "INSERT INTO comps" in call[0]
        )
        sql, params = insert_call
        self.assertIn("latest_listing_text_conditions", sql)
        self.assertIn("tc.text_grade", sql)
        self.assertIn("raw_attrs::text", sql)
        self.assertIn("\\mlocked\\M", sql)
        self.assertEqual(params[1], APPLE_IPHONE_KEY_PATTERN)
        self.assertRegex('apple-iphone-duo-256gb', params[1])
        self.assertEqual(params[4], APPLE_MACBOOK_NEO_KEY_PATTERN)
        patterns = params[3]
        self.assertNotIn('%macbook neo%', patterns)
        self.assertIn("%selling for scraps%", patterns)
        self.assertIn("%logic board missing%", patterns)
        self.assertIn("%screen is damaged%", patterns)
        self.assertIn("%joy-con%", patterns)
        self.assertIn("%instructions and stickers only%", patterns)
        self.assertIn("%stickers only%", patterns)
        self.assertIn("%%castle byers%%", sql)
        self.assertIn("NOT LIKE ALL", sql)
        self.assertIn("source = 'ebay-consoles'", sql)
        self.assertIn("source IN", sql)
        self.assertIn("!~", sql)
        console_non_hardware_patterns = params[5]
        console_accessory_patterns = params[6]
        damage_pattern = params[15]
        damage_negation_pattern = params[16]
        lego_whole_set_patterns = params[17]
        self.assertIn("%console game%", console_non_hardware_patterns)
        self.assertIn("%case game holder%", console_non_hardware_patterns)
        self.assertIn("%carry case%", console_accessory_patterns)
        self.assertIn("%controller for%", console_accessory_patterns)
        self.assertIn("damaged", damage_pattern)
        self.assertIn("no|not|without|free from", damage_negation_pattern)
        self.assertIn("%complete set%", lego_whole_set_patterns)

    def test_comps_compute_upserts_current_snapshot(self):
        fake_conn = _FakeConnection()

        with patch("pipelines.listing.comps.graph.connection", fake_conn):
            _compute_daily_comps(days=365)

        sql, _params = next(
            call for call in fake_conn.cursor_obj.calls if "INSERT INTO comps" in call[0]
        )
        self.assertIn("ON CONFLICT (model_key, condition_grade) DO UPDATE", sql)
        self.assertIn("median_final_price = EXCLUDED.median_final_price", sql)
        self.assertIn("mean_final_price = EXCLUDED.mean_final_price", sql)
        self.assertIn("samples = EXCLUDED.samples", sql)
        self.assertIn("computed_at = EXCLUDED.computed_at", sql)

    def test_console_comp_sql_blocks_accessories_without_global_controller_ban(self):
        fake_conn = _FakeConnection()

        with patch("pipelines.listing.comps.graph.connection", fake_conn):
            _compute_daily_comps(days=365)

        sql, params = next(
            call for call in fake_conn.cursor_obj.calls if "INSERT INTO comps" in call[0]
        )
        console_accessory_patterns = params[6]

        self.assertIn("%controller for%", console_accessory_patterns)
        self.assertIn("CONSOLE", sql.upper())
        self.assertIn("NOT (", sql)
        self.assertNotIn("%with controller%", console_accessory_patterns)

    def test_electronics_damage_sql_has_negation_guard(self):
        fake_conn = _FakeConnection()

        with patch("pipelines.listing.comps.graph.connection", fake_conn):
            _compute_daily_comps(days=365)

        sql, params = next(
            call for call in fake_conn.cursor_obj.calls if "INSERT INTO comps" in call[0]
        )
        damage_pattern = params[15]
        damage_negation_pattern = params[16]

        self.assertIn("damaged", damage_pattern)
        self.assertIn("damage", damage_pattern)
        self.assertIn("no|not|without|free from", damage_negation_pattern)
        self.assertIn("!~", sql)

    def test_apple_collection_defaults_to_bin_and_auction_feeds(self):
        self.assertEqual(AppleAdapter.SALE_TYPE, ["bin", "auction"])

    def test_base_adapter_uses_source_specific_model_keys(self):
        adapter = object.__new__(EbayAdapterBase)
        adapter.DOMAIN = "ebay-apple"

        self.assertEqual(
            adapter._model_key_for("Apple iPhone 14 128GB Purple Unlocked"),
            "apple-iphone-14-128gb",
        )

    def test_auction_only_source_clears_old_live_bin_rows(self):
        adapter = object.__new__(EbayAdapterBase)
        adapter.DOMAIN = "ebay-apple"
        adapter._source_name = "ebay-apple"
        fake_conn = _FakeConnection()

        with patch(
            "pipelines.listing.retrieve.adapters.ebay_base.get_connection",
            return_value=fake_conn,
        ):
            adapter._mark_disallowed_live_sale_types(["auction"])

        update_calls = [
            call for call in fake_conn.cursor_obj.calls if "UPDATE auction_listings" in call[0]
        ]
        self.assertEqual(len(update_calls), 1)
        self.assertEqual(update_calls[0][1], ("ebay-apple", ["auction", "both"]))
        self.assertIn("sale_type <> ALL", update_calls[0][0])

    def test_buying_options_can_mark_listing_as_auction_plus_buy_now(self):
        self.assertEqual(
            _sale_type_from_buying_options(["AUCTION", "FIXED_PRICE"]),
            "both",
        )
        self.assertEqual(_sale_type_from_buying_options(["AUCTION"]), "auction")
        self.assertEqual(_sale_type_from_buying_options(["FIXED_PRICE"]), "bin")

    def test_search_sort_matches_sale_type(self):
        self.assertEqual(_search_sort_for_sale_type("bin"), "newlyListed")
        self.assertEqual(_search_sort_for_sale_type("auction"), "endingSoon")
        self.assertEqual(_search_sort_for_sale_type("both"), "endingSoon")

    def test_normalize_item_preserves_auction_plus_buy_now_type(self):
        adapter = object.__new__(EbayAdapterBase)
        adapter.DOMAIN = "ebay-test"
        adapter._source_name = "ebay-test"
        adapter._source_id = 1
        adapter._model_key_for = lambda title: "apple-iphone-15-128gb"

        norm = adapter._normalize_item(
            {
                "itemId": "v1|both|0",
                "title": "Apple iPhone 15 128GB",
                "itemWebUrl": "https://example.test/both",
                "buyingOptions": ["AUCTION", "FIXED_PRICE"],
                "currentBidPrice": {"value": "101.00"},
                "price": {"value": "299.00"},
                "itemEndDate": "2026-12-30T12:00:00Z",
            },
            "auction",
        )

        self.assertIsNotNone(norm)
        row, _history = norm
        self.assertEqual(row["sale_type"], "both")
        self.assertEqual(row["price_current"], 101)
        self.assertEqual(row["price_bid_current"], 101)

    def test_category_fetch_uses_buying_options_filter_for_auction(self):
        adapter = object.__new__(EbayAdapterBase)
        adapter.DOMAIN = "ebay-test"
        adapter.SEARCH_QUERY = "test"
        adapter._hist_api = deque(maxlen=10)
        seen_urls = []

        def fake_get(url, **kwargs):
            seen_urls.append(url)
            return _FakeResponse()

        with patch.dict("os.environ", {"EBAY_API_BASE": "https://api.example.test"}), patch(
            "pipelines.listing.retrieve.adapters.ebay_base.requests.get",
            side_effect=fake_get,
        ), patch("pipelines.listing.retrieve.adapters.ebay_base.increment_api_usage"):
            adapter._fetch_category_items("token", 123, "auction")

        self.assertEqual(len(seen_urls), 1)
        self.assertIn("filter=buyingOptions:{AUCTION}", seen_urls[0])
        self.assertIn("sort=endingSoon", seen_urls[0])
        self.assertNotIn("listingType", seen_urls[0])

    def test_category_fetch_uses_newly_listed_sort_for_bin(self):
        adapter = object.__new__(EbayAdapterBase)
        adapter.DOMAIN = "ebay-test"
        adapter.SEARCH_QUERY = "test"
        adapter._hist_api = deque(maxlen=10)
        seen_urls = []

        def fake_get(url, **kwargs):
            seen_urls.append(url)
            return _FakeResponse()

        with patch.dict("os.environ", {"EBAY_API_BASE": "https://api.example.test"}), patch(
            "pipelines.listing.retrieve.adapters.ebay_base.requests.get",
            side_effect=fake_get,
        ), patch("pipelines.listing.retrieve.adapters.ebay_base.increment_api_usage"):
            adapter._fetch_category_items("token", 123, "bin")

        self.assertEqual(len(seen_urls), 1)
        self.assertIn("filter=buyingOptions:{FIXED_PRICE}", seen_urls[0])
        self.assertIn("sort=newlyListed", seen_urls[0])
        self.assertNotIn("listingType", seen_urls[0])

    def test_unseen_live_cleanup_marks_not_seen_rows_unknown(self):
        fake_conn = _FakeConnection()
        row = {
            "source": "ebay-apple",
            "external_id": "v1|future|0",
            "title": "Apple MacBook Pro",
            "price_current": 100,
            "bids_count": 1,
            "end_time": None,
            "url": "https://example.test/item/future",
            "sale_type": "auction",
            "source_id": 1,
            "model_key": "apple-macbook-pro-13in-m1-8gb-256gb",
            "status": "live",
        }

        with patch(
            "pipelines.listing.retrieve.adapters.ebay_base.get_connection",
            return_value=fake_conn,
        ), patch("pipelines.listing.retrieve.adapters.ebay_base.execute_values"):
            bulk_upsert_auction_listings([row])

        update_calls = [
            call for call in fake_conn.cursor_obj.calls if "UPDATE auction_listings" in call[0]
        ]
        self.assertEqual(len(update_calls), 1)
        cleanup_sql = update_calls[0][0]
        cleanup_params = update_calls[0][1]
        self.assertIn("source = ANY(%s)", cleanup_sql)
        self.assertIn("last_seen_at IS NULL", cleanup_sql)
        self.assertIn("last_seen_at < %s", cleanup_sql)
        self.assertIn("end_time IS NULL", cleanup_sql)
        self.assertIn("end_time <=", cleanup_sql)
        self.assertEqual(cleanup_params[0], ["ebay-apple"])

    def test_getitem_404_unknown_sets_end_time_to_now(self):
        adapter = object.__new__(EbayAdapterBase)
        adapter.DOMAIN = "ebay-test"
        fake_conn = _FakeConnection()

        with patch("utils.db_schema.get_connection", return_value=fake_conn):
            adapter._mark_404_listings_unknown({"v1|gone|0"})

        update_calls = [
            call for call in fake_conn.cursor_obj.calls if "UPDATE auction_listings" in call[0]
        ]
        self.assertEqual(len(update_calls), 1)
        self.assertIn("SET status   = 'unknown'", update_calls[0][0])
        self.assertIn("end_time = (now() AT TIME ZONE 'utc')", update_calls[0][0])
        self.assertNotIn("COALESCE(end_time", update_calls[0][0])

    def test_browse_unavailable_detects_out_of_stock(self):
        reason = _browse_item_unavailable_reason(
            {
                "itemId": "v1|soldout|0",
                "estimatedAvailabilities": [
                    {
                        "estimatedAvailabilityStatus": "OUT_OF_STOCK",
                        "estimatedAvailableQuantity": 0,
                    }
                ],
            }
        )

        self.assertEqual(reason, "browse_out_of_stock")

    def test_browse_description_attrs_keep_condition_description(self):
        attrs = _browse_description_attrs(
            {
                "condition": "Used",
                "conditionDescription": (
                    "Working 2020 M1 Macbook Air, left side of the screen is damaged "
                    "but the laptop is still useable."
                ),
                "localizedAspects": [{"name": "RAM Size", "value": "8 GB"}],
            }
        )

        self.assertEqual(attrs["Browse condition"], "Used")
        self.assertIn("screen is damaged", attrs["Browse conditionDescription"])
        self.assertEqual(attrs["Browse aspect RAM Size"], "8 GB")

    def test_browse_description_attrs_keep_low_cost_review_evidence(self):
        attrs = _browse_description_attrs(
            {
                "condition": "Used",
                "description": "Apple iPhone 14 with clean display and Find My removed.",
                "shortDescription": "iPhone 14 128GB on EE.",
                "categoryId": "9355",
                "itemCreationDate": "2026-08-05T12:30:00Z",
                "seller": {
                    "username": "phone-seller",
                    "feedbackScore": 443,
                    "feedbackPercentage": "99.4",
                },
                "itemLocation": {
                    "country": "GB",
                    "city": "London",
                    "postalCode": "SW1A",
                },
                "returnTerms": {
                    "returnsAccepted": True,
                    "returnPeriod": {"value": 30, "unit": "DAY"},
                },
                "shippingOptions": [
                    {
                        "shippingServiceCode": "UK_RoyalMail48",
                        "shippingCost": {"value": "3.29", "currency": "GBP"},
                    }
                ],
                "localizedAspects": [
                    {"name": "Network", "value": "EE"},
                    {"name": "Lock Status", "value": "Unlocked"},
                    {"name": "Storage Capacity", "value": "128 GB"},
                    {"name": "Model Number", "value": "MPUA3LL/A"},
                ],
            }
        )

        self.assertEqual(attrs["Browse description"], "Apple iPhone 14 with clean display and Find My removed.")
        self.assertEqual(attrs["Browse seller feedbackScore"], 443)
        self.assertEqual(attrs["Browse seller feedbackPercentage"], "99.4")
        self.assertEqual(attrs["Browse itemLocation country"], "GB")
        self.assertEqual(attrs["Browse returnTerms returnsAccepted"], True)
        self.assertEqual(attrs["Browse returnTerms returnPeriod"], "30 DAY")
        self.assertEqual(attrs["Browse shippingOptions"][0]["shippingCost"]["currency"], "GBP")
        self.assertEqual(attrs["Browse aspect Network"], "EE")
        self.assertEqual(attrs["Browse aspect Lock Status"], "Unlocked")
        self.assertEqual(attrs["Browse aspect Storage Capacity"], "128 GB")
        self.assertEqual(attrs["Browse aspect Model Number"], "MPUA3LL/A")

    def test_browse_description_attrs_do_not_coerce_missing_feedback_percentage_to_zero(self):
        attrs = _browse_description_attrs(
            {
                "condition": "Used",
                "seller": {
                    "username": "phone-seller",
                    "feedbackScore": 35,
                },
            }
        )

        self.assertEqual(attrs["Browse seller feedbackScore"], 35)
        self.assertNotIn("Browse seller feedbackPercentage", attrs)

    def test_browse_image_urls_collect_primary_and_additional_urls(self):
        urls = _browse_image_urls(
            {
                "image": {"imageUrl": "https://i.ebayimg.test/primary.jpg"},
                "thumbnailImages": [{"imageUrl": "https://i.ebayimg.test/thumb.jpg"}],
                "additionalImages": [
                    {"imageUrl": "https://i.ebayimg.test/side.jpg"},
                    {"imageUrl": "https://i.ebayimg.test/primary.jpg"},
                    {"imageUrl": "not-a-url"},
                ],
            }
        )

        self.assertEqual(
            urls,
            [
                "https://i.ebayimg.test/primary.jpg",
                "https://i.ebayimg.test/side.jpg",
                "https://i.ebayimg.test/thumb.jpg",
            ],
        )

    def test_browse_description_attrs_keep_image_urls(self):
        attrs = _browse_description_attrs(
            {
                "condition": "Used",
                "image": {"imageUrl": "https://i.ebayimg.test/primary.jpg"},
                "additionalImages": [{"imageUrl": "https://i.ebayimg.test/back.jpg"}],
            }
        )

        self.assertEqual(attrs["Browse imageUrl"], "https://i.ebayimg.test/primary.jpg")
        self.assertEqual(
            attrs["Browse imageUrls"],
            ["https://i.ebayimg.test/primary.jpg", "https://i.ebayimg.test/back.jpg"],
        )

    def test_browse_unavailable_detects_past_end_time(self):
        reason = _browse_item_unavailable_reason(
            {
                "itemId": "v1|ended|0",
                "itemEndDate": "2020-01-01T00:00:00Z",
            }
        )

        self.assertEqual(reason, "browse_item_ended")

    def test_browse_unavailable_keeps_available_items(self):
        reason = _browse_item_unavailable_reason(
            {
                "itemId": "v1|live|0",
                "estimatedAvailabilities": [
                    {
                        "estimatedAvailabilityStatus": "IN_STOCK",
                        "estimatedAvailableQuantity": 1,
                    }
                ],
            }
        )

        self.assertIsNone(reason)

    def test_refresh_items_price_marks_unavailable_rows_unknown(self):
        adapter = object.__new__(EbayAdapterBase)
        adapter.DOMAIN = "ebay-test"
        adapter._batch_buffer = []
        adapter._ph_buffer = []
        adapter._fetch_items_by_ids = lambda token, ids: [
            {
                "itemId": "v1|soldout|0",
                "title": "Sold out item",
                "estimatedAvailabilities": [
                    {
                        "estimatedAvailabilityStatus": "OUT_OF_STOCK",
                        "estimatedAvailableQuantity": 0,
                    }
                ],
            }
        ]
        marked = []
        adapter._mark_unavailable_listings_unknown = lambda ids: marked.append(set(ids))

        adapter.refresh_items_price("token", ["v1|soldout|0"])

        self.assertEqual(marked, [{"v1|soldout|0"}])
        self.assertEqual(adapter._batch_buffer, [])

    def test_refresh_items_price_preserves_description_condition_grade(self):
        adapter = object.__new__(EbayAdapterBase)
        adapter.DOMAIN = "ebay-test"
        adapter._source_name = "ebay-apple"
        adapter._source_id = 1
        adapter.SALE_TYPE = ["auction"]
        adapter._batch_buffer = []
        adapter._ph_buffer = []
        adapter._last_flush = 0
        adapter._model_key_for = lambda title: "apple-macbook-air-13in-m1-8gb-256gb"
        adapter._is_relevant = lambda row: True
        adapter._fetch_items_by_ids = lambda token, ids: [
            {
                "itemId": "v1|damaged|0",
                "title": "Apple MacBook Air 13in M1 8GB 256GB",
                "itemWebUrl": "https://example.test/damaged",
                "buyingOptions": ["AUCTION"],
                "currentBidPrice": {"value": "157.00"},
                "itemEndDate": "2026-12-30T22:16:00Z",
                "condition": "Used",
                "conditionDescription": (
                    "Working 2020 M1 Macbook Air, left side of the screen is damaged "
                    "but the laptop is still useable."
                ),
            }
        ]
        captured_rows = []

        with patch(
            "pipelines.listing.retrieve.adapters.ebay_base.bulk_upsert_auction_listings",
            side_effect=lambda rows: captured_rows.extend(rows),
        ), patch(
            "pipelines.listing.retrieve.adapters.ebay_base.bulk_append_price_history"
        ):
            adapter.refresh_items_price("token", ["v1|damaged|0"])

        self.assertEqual(len(captured_rows), 1)
        row = captured_rows[0]
        self.assertEqual(row["condition_grade"], "C")
        self.assertIn("screen is damaged", row["raw_attrs"]["Browse conditionDescription"])

    def test_category_fetch_marks_unavailable_rows_unknown(self):
        adapter = object.__new__(EbayAdapterBase)
        adapter.DOMAIN = "ebay-test"
        adapter.FETCH_MODE = "category"
        adapter.SALE_TYPE = ["bin"]
        adapter.CATEGORY_IDS = [123]
        adapter.CATEGORY_PAUSE_SECONDS = 0
        adapter._batch_buffer = []
        adapter._ph_buffer = []
        adapter._fetch_category_items = lambda token, cat_id, sale_type: [
            {
                "itemId": "v1|soldout|0",
                "title": "Sold out item",
                "buyingOptions": ["FIXED_PRICE"],
                "estimatedAvailabilities": [
                    {
                        "estimatedAvailabilityStatus": "OUT_OF_STOCK",
                        "estimatedAvailableQuantity": 0,
                    }
                ],
            }
        ]
        adapter._is_relevant = lambda row: True
        adapter._mark_disallowed_live_sale_types = lambda sale_types: None
        marked = []
        adapter._mark_unavailable_listings_unknown = lambda ids: marked.append(set(ids))

        adapter.fetch_listings_api("token")

        self.assertEqual(marked, [{"v1|soldout|0"}])
        self.assertEqual(adapter._batch_buffer, [])


if __name__ == "__main__":
    unittest.main()
