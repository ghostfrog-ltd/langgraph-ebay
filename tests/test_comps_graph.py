import re
import unittest
from collections import deque
from unittest.mock import patch

from pipelines.listing.comps.graph import APPLE_MAC_ALLOWED_CPU_PATTERN, _compute_daily_comps
from pipelines.listing.retrieve.adapters.apple import Adapter as AppleAdapter
from pipelines.listing.retrieve.adapters.ebay_base import (
    EbayAdapterBase,
    _browse_description_attrs,
    _browse_item_unavailable_reason,
    _sale_type_from_buying_options,
    bulk_upsert_auction_listings,
)


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
    def test_apple_mac_comps_allow_intel_core_keys(self):
        valid_keys = [
            "apple-macbook-pro-a2141-16in-core-i7-16gb-512gb",
            "apple-macbook-air-a2179-133in-core-i5-16gb-1tb",
            "apple-macbook-a1534-12in-core-m3-8gb-256gb",
            "apple-macbook-pro-a2442-14in-m1pro-16gb-1tb",
        ]

        for key in valid_keys:
            with self.subTest(key=key):
                self.assertRegex(key, APPLE_MAC_ALLOWED_CPU_PATTERN)

    def test_apple_mac_comps_still_block_under_specified_mac_keys(self):
        invalid_keys = [
            "apple-macbook-pro-16gb-512gb",
            "apple-macbook-air-13in-8gb-256gb",
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
        self.assertIn("raw_attrs::text", sql)
        self.assertIn("\\mlocked\\M", sql)
        patterns = params[2]
        self.assertIn("%selling for scraps%", patterns)
        self.assertIn("%logic board missing%", patterns)
        self.assertIn("%screen is damaged%", patterns)
        self.assertIn("%joy-con%", patterns)

    def test_apple_collection_defaults_to_auction_feed(self):
        self.assertEqual(AppleAdapter.SALE_TYPE, ["auction"])

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
                "itemEndDate": "2026-08-30T12:00:00Z",
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
        self.assertNotIn("RAM Size", attrs)

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
                "itemEndDate": "2026-08-28T22:16:00Z",
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
