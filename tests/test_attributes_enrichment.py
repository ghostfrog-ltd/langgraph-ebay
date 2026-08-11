import json
import unittest
from unittest.mock import patch

from pipelines.listing.attributes.graph import (
    TRADING_ENRICHMENT_STATUS_KEY,
    _apply_attributes,
    _extract_from_trading,
    _load_candidates,
    _load_candidates_for_ids,
    _merge_raw_attrs,
    _node_mark_false,
    _with_trading_marker,
)


class _FakeCursor:
    rowcount = 0

    def __init__(self, fetchone_rows=None, fetchall_rows=None):
        self.calls = []
        self.fetchone_rows = list(fetchone_rows or [])
        self.fetchall_rows = list(fetchall_rows or [])

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, sql, params=None):
        self.calls.append((sql, params))

    def fetchone(self):
        if self.fetchone_rows:
            return self.fetchone_rows.pop(0)
        return None

    def fetchall(self):
        return self.fetchall_rows


class _FakeConnection:
    def __init__(self, cursor):
        self.cursor_obj = cursor
        self.commits = 0
        self.closed = False

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def cursor(self):
        return self.cursor_obj

    def commit(self):
        self.commits += 1

    def close(self):
        self.closed = True


class AttributesEnrichmentTests(unittest.TestCase):
    def test_load_candidates_schedules_browse_only_rows(self):
        cursor = _FakeCursor(fetchall_rows=[])
        fake_conn = _FakeConnection(cursor)

        with patch("pipelines.listing.attributes.graph.get_connection", return_value=fake_conn):
            rows = _load_candidates(25)

        self.assertEqual(rows, [])
        sql, params = cursor.calls[0]
        self.assertIn("raw_attrs IS NULL", sql)
        self.assertIn("jsonb_object_keys(raw_attrs)", sql)
        self.assertIn("attr_key.key NOT LIKE 'Browse %%'", sql)
        self.assertIn("brand IS NULL", sql)
        self.assertIn("NOT (raw_attrs ? %s)", sql)
        self.assertEqual(params, (TRADING_ENRICHMENT_STATUS_KEY, 25))

    def test_load_candidates_for_ids_only_schedules_requested_items(self):
        cursor = _FakeCursor(fetchall_rows=[])
        fake_conn = _FakeConnection(cursor)

        with patch("pipelines.listing.attributes.graph.get_connection", return_value=fake_conn):
            rows = _load_candidates_for_ids(["147485216891", "v1|366595448811|0"])

        self.assertEqual(rows, [])
        sql, params = cursor.calls[0]
        self.assertIn("external_id = ANY(%s)", sql)
        self.assertIn("v1|147485216891|0", params[0])
        self.assertIn("v1|366595448811|0", params[0])
        self.assertEqual(params[1], False)
        self.assertEqual(params[2], TRADING_ENRICHMENT_STATUS_KEY)
        self.assertIn("raw_attrs IS NULL", sql)

    def test_load_candidates_for_ids_can_force_refresh(self):
        cursor = _FakeCursor(fetchall_rows=[])
        fake_conn = _FakeConnection(cursor)

        with patch("pipelines.listing.attributes.graph.get_connection", return_value=fake_conn):
            _load_candidates_for_ids(["147485216891"], force=True)

        _sql, params = cursor.calls[0]
        self.assertEqual(params[1], True)

    def test_trading_extract_keeps_apple_phone_specifics(self):
        xml = """<?xml version="1.0" encoding="utf-8"?>
        <GetItemResponse xmlns="urn:ebay:apis:eBLBaseComponents">
          <Item>
            <ItemSpecifics>
              <NameValueList><Name>Brand</Name><Value>Apple</Value></NameValueList>
              <NameValueList><Name>Network</Name><Value>Unlocked</Value></NameValueList>
              <NameValueList><Name>Lock Status</Name><Value>Network Unlocked</Value></NameValueList>
              <NameValueList><Name>Storage Capacity</Name><Value>128 GB</Value></NameValueList>
              <NameValueList><Name>Model Number</Name><Value>MPUA3LL/A</Value></NameValueList>
            </ItemSpecifics>
          </Item>
        </GetItemResponse>
        """

        attrs, raw_map = _extract_from_trading(xml)

        self.assertEqual(attrs["brand"], "Apple")
        self.assertEqual(attrs["storage_gb"], 128)
        self.assertEqual(raw_map["Network"], "Unlocked")
        self.assertEqual(raw_map["Lock Status"], "Network Unlocked")
        self.assertEqual(raw_map["Storage Capacity"], "128 GB")
        self.assertEqual(raw_map["Model Number"], "MPUA3LL/A")

    def test_merge_preserves_browse_evidence_and_adds_trading_specifics(self):
        browse_attrs = {
            "Browse conditionDescription": "Used phone, no cracks.",
            "Browse seller feedbackScore": 443,
            "Browse returnTerms returnsAccepted": True,
            "Browse itemLocation country": "GB",
            "Browse aspect Network": "EE",
        }
        trading_attrs = _with_trading_marker(
            {
                "Network": "Unlocked",
                "Lock Status": "Network Unlocked",
                "Storage Capacity": "128 GB",
                "Model Number": "MPUA3LL/A",
            },
            "complete",
        )

        merged = _merge_raw_attrs(browse_attrs, trading_attrs)

        self.assertEqual(merged["Browse seller feedbackScore"], 443)
        self.assertEqual(merged["Browse returnTerms returnsAccepted"], True)
        self.assertEqual(merged["Browse aspect Network"], "EE")
        self.assertEqual(merged["Network"], "Unlocked")
        self.assertEqual(merged["Lock Status"], "Network Unlocked")
        self.assertEqual(merged["Storage Capacity"], "128 GB")
        self.assertEqual(merged["Model Number"], "MPUA3LL/A")
        self.assertEqual(merged[TRADING_ENRICHMENT_STATUS_KEY], "complete")

    def test_apply_attributes_merges_raw_attrs_and_keeps_riskier_grade(self):
        cursor = _FakeCursor(
            fetchone_rows=[
                (
                    {
                        "Browse conditionDescription": "Screen is damaged.",
                        "Browse seller feedbackScore": 443,
                    },
                    "C",
                )
            ]
        )
        fake_conn = _FakeConnection(cursor)

        with patch("pipelines.listing.attributes.graph.get_connection", return_value=fake_conn):
            _apply_attributes(
                auction_id=10,
                raw_attrs=_with_trading_marker(
                    {
                        "Network": "Unlocked",
                        "Storage Capacity": "128 GB",
                    },
                    "complete",
                ),
                brand="Apple",
                model_name="iPhone 14",
                storage_gb=128,
                colour=None,
                epid=None,
                model_key="apple-iphone-14-128gb",
                condition_grade="B",
            )

        update_sql, update_params = cursor.calls[-1]
        self.assertIn("UPDATE auction_listings", update_sql)
        stored_raw_attrs = json.loads(update_params[0])
        self.assertEqual(stored_raw_attrs["Browse conditionDescription"], "Screen is damaged.")
        self.assertEqual(stored_raw_attrs["Browse seller feedbackScore"], 443)
        self.assertEqual(stored_raw_attrs["Network"], "Unlocked")
        self.assertEqual(stored_raw_attrs["Storage Capacity"], "128 GB")
        self.assertEqual(stored_raw_attrs[TRADING_ENRICHMENT_STATUS_KEY], "complete")
        self.assertEqual(update_params[-2], "C")
        self.assertEqual(update_params[-1], 10)

    def test_no_response_is_retryable_and_does_not_mark_row_complete(self):
        state = {
            "auction_id": 10,
            "external_id": "v1|147485216891|0",
            "item_id": "147485216891",
            "xml_text": None,
            "processed": 0,
            "marked_false": 0,
            "retryable_failures": 0,
            "outcomes": [],
        }

        with patch("pipelines.listing.attributes.graph._apply_attributes") as apply_attrs:
            updated = _node_mark_false(state)

        apply_attrs.assert_not_called()
        self.assertEqual(updated["processed"], 1)
        self.assertEqual(updated["marked_false"], 0)
        self.assertEqual(updated["retryable_failures"], 1)
        self.assertEqual(updated["outcomes"][0]["status"], "retryable_no_response")

    def test_invalid_item_id_is_terminally_marked(self):
        state = {
            "auction_id": 10,
            "external_id": "not-a-trading-id",
            "item_id": None,
            "xml_text": None,
            "processed": 0,
            "marked_false": 0,
            "retryable_failures": 0,
            "outcomes": [],
        }

        with patch("pipelines.listing.attributes.graph._apply_attributes") as apply_attrs:
            updated = _node_mark_false(state)

        apply_attrs.assert_called_once()
        self.assertEqual(apply_attrs.call_args.kwargs["raw_attrs"][TRADING_ENRICHMENT_STATUS_KEY], "invalid_item_id")
        self.assertEqual(updated["marked_false"], 1)
        self.assertEqual(updated["retryable_failures"], 0)
        self.assertEqual(updated["outcomes"][0]["status"], "invalid_item_id")


if __name__ == "__main__":
    unittest.main()
