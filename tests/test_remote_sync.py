from datetime import UTC, datetime
import unittest
from unittest.mock import patch

from pipelines.listing.explain import ListingExplanation
from pipelines.listing.remote_sync import (
    build_delta_payload,
    build_listing_sync_state,
    build_model_key_comp_payload,
    build_sold_evidence_payload,
    build_sync_payload,
    fetch_live_listing_rows,
    send_delta_chunked,
)


class _FakeCursor:
    def __init__(self):
        self.sql = ""
        self.params = None
        self.description = []

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def execute(self, sql, params=None):
        self.sql = sql
        self.params = params

    def fetchall(self):
        return []


class _FakeConnection:
    def __init__(self):
        self.cursor_obj = _FakeCursor()

    def cursor(self, *_, **__):
        return self.cursor_obj

    def close(self):
        pass


class RemoteSyncPayloadTests(unittest.TestCase):
    def test_live_listing_fetch_requires_recent_seen_timestamp(self):
        conn = _FakeConnection()

        with patch("pipelines.listing.remote_sync.get_fresh_connection", return_value=conn), patch.dict(
            "os.environ",
            {"GHOSTFROG_REMOTE_SYNC_LIVE_MAX_AGE_HOURS": "24"},
        ):
            rows = fetch_live_listing_rows()

        self.assertEqual(rows, [])
        self.assertIn("COALESCE(a.last_seen_at, a.first_seen)", conn.cursor_obj.sql)
        self.assertIn("hours", conn.cursor_obj.sql)
        self.assertEqual(conn.cursor_obj.params, ("24",))

    def test_only_publishes_allowlisted_fields_and_evidence(self):
        report = {
        "summary": {"checked": 1, "potentials": 1, "secret_metric": 99},
        "closest": [
            {
                "source": "ebay-apple",
                "external_id": "123",
                "title": "MacBook Air M2",
                "url": "https://example.test/123",
                "model_key": "apple-macbook-air-m2-16gb-512gb",
                "condition_grade": "C",
                "text_condition_grade": "B",
                "text_scan_grade": "D",
                "text_scan_checked": True,
                "text_scan_review_required": True,
                "text_scan_risk_flags": ["locked_device"],
                "text_scan_model_name": "gemma3:12b",
                "text_scan_checked_at": datetime(2026, 8, 26, tzinfo=UTC),
                "visual_condition_grade": "C",
                "visual_condition_checked": True,
                "visual_review_required": True,
                "visual_damage_flags": ["chipped_corner"],
                "visual_model_name": "minicpm-v:8b",
                "visual_checked_at": datetime(2026, 8, 26, tzinfo=UTC),
                "sale_type": "auction",
                "current_price": 350,
                "sold_median": 500,
                "comp_samples": 12,
                "max_safe_buy": 365,
                "expected_profit": 65,
                "expected_roi": 0.1857,
                "price_gap_to_max_buy": 15,
                "effective_min_comps": 10,
                "signal": "evidence",
                "raw_attrs": {"must": "not leave local"},
                "seller": "must-not-publish",
            }
        ],
    }

        def explain(_external_id: str) -> ListingExplanation:
            return ListingExplanation(
            target={},
            potential={},
            comps=[
                {
                    "external_id": "999",
                    "source": "ebay-apple",
                    "title": "Sold MacBook",
                    "url": "https://example.test/999",
                    "final_price": 499,
                    "text_scan_grade": "B",
                    "text_scan_checked": True,
                    "text_scan_review_required": False,
                    "text_scan_risk_flags": [],
                    "text_scan_model_name": "gemma3:12b",
                    "text_scan_checked_at": datetime(2026, 8, 25, tzinfo=UTC),
                    "visual_condition_grade": "B",
                    "visual_condition_checked": True,
                    "visual_review_required": False,
                    "visual_damage_flags": [],
                    "visual_model_name": "minicpm-v:8b",
                    "visual_checked_at": datetime(2026, 8, 25, tzinfo=UTC),
                    "raw_attrs": {"private": True},
                }
            ],
            comp_count=1,
            comp_window_days=90,
        )

        payload = build_sync_payload(
            report,
            explain=explain,
            generated_at=datetime(2026, 8, 17, tzinfo=UTC),
        )

        listing = payload["listings"][0]
        self.assertEqual(listing["external_id"], "123")
        self.assertEqual(listing["condition_grade"], "C")
        self.assertEqual(listing["text_condition_grade"], "B")
        self.assertEqual(listing["text_scan_grade"], "D")
        self.assertTrue(listing["text_scan_checked"])
        self.assertEqual(listing["text_scan_risk_flags"], ["locked_device"])
        self.assertEqual(listing["visual_condition_grade"], "C")
        self.assertTrue(listing["visual_condition_checked"])
        self.assertEqual(listing["visual_damage_flags"], ["chipped_corner"])
        self.assertNotIn("raw_attrs", listing)
        self.assertNotIn("seller", listing)
        self.assertEqual(listing["comps"][0]["visual_condition_grade"], "B")
        self.assertEqual(listing["comps"][0]["text_scan_grade"], "B")
        self.assertTrue(listing["comps"][0]["text_scan_checked"])
        self.assertTrue(listing["comps"][0]["visual_condition_checked"])
        self.assertNotIn("raw_attrs", listing["comps"][0])
        self.assertNotIn("secret_metric", payload["summary"])

    def test_skips_rows_without_explainable_evidence(self):
        report = {"summary": {}, "closest": [{"external_id": "123"}]}

        def explain(_external_id: str) -> ListingExplanation:
            return ListingExplanation(
            target={},
            potential=None,
            comps=[],
            comp_count=0,
            comp_window_days=90,
            error="no evidence",
        )

        payload = build_sync_payload(report, explain=explain)

        self.assertEqual(payload["listings"], [])

    def test_explicit_evidence_payload_keeps_browse_rows_without_comps(self):
        report = {
            "summary": {},
            "closest": [
                {
                    "source": "ebay-tools",
                    "external_id": "tool-123",
                    "title": "Cordless drill",
                    "url": "https://example.test/tool-123",
                    "model_key": "unknown",
                    "condition_grade": "B",
                    "sale_type": "auction",
                    "current_price": 20,
                    "sold_median": 0,
                    "comp_samples": 0,
                    "max_safe_buy": 0,
                    "expected_profit": 0,
                    "expected_roi": 0,
                    "price_gap_to_max_buy": 0,
                    "effective_min_comps": 1,
                    "signal": "no same-grade sold evidence yet",
                }
            ],
        }

        payload = build_sync_payload(report, evidence={})

        self.assertEqual(len(payload["listings"]), 1)
        self.assertEqual(payload["listings"][0]["external_id"], "tool-123")
        self.assertEqual(payload["listings"][0]["comps"], [])

    def test_delta_payload_only_sends_changed_listings_and_expired_ids(self):
        current_payload = {
            "schema_version": 1,
            "batch_id": "ignored-source-batch",
            "generated_at": "2026-09-06T00:30:00+00:00",
            "summary": {"published": 2},
            "listings": [
                {
                    "external_id": "same",
                    "title": "Same listing",
                    "current_price": 100,
                    "comps": [],
                },
                {
                    "external_id": "changed",
                    "title": "Changed listing",
                    "current_price": 90,
                    "comps": [],
                },
            ],
        }
        previous_payload = {
            **current_payload,
            "listings": [
                {
                    "external_id": "same",
                    "title": "Same listing",
                    "current_price": 100,
                    "comps": [],
                },
                {
                    "external_id": "changed",
                    "title": "Changed listing",
                    "current_price": 95,
                    "comps": [],
                },
                {
                    "external_id": "gone",
                    "title": "Gone listing",
                    "current_price": 80,
                    "comps": [],
                },
            ],
        }
        previous_state = build_listing_sync_state(previous_payload, endpoint="https://example.test")

        delta = build_delta_payload(current_payload, previous_state=previous_state)

        self.assertEqual(delta["mode"], "delta")
        self.assertEqual([row["external_id"] for row in delta["listings"]], ["changed"])
        self.assertEqual(delta["expired_external_ids"], ["gone"])
        self.assertEqual(delta["current_external_ids"], ["changed", "same"])
        self.assertEqual(delta["summary"]["delta_upserts"], 1)
        self.assertEqual(delta["summary"]["delta_expired"], 1)

    def test_delta_bootstrap_can_prune_without_resending_every_listing(self):
        payload = {
            "schema_version": 1,
            "generated_at": "2026-09-06T00:30:00+00:00",
            "summary": {"published": 2},
            "listings": [
                {"external_id": "one", "title": "One", "current_price": 100, "comps": []},
                {"external_id": "two", "title": "Two", "current_price": 90, "comps": []},
            ],
        }

        delta = build_delta_payload(payload, previous_state={}, bootstrap_without_upserts=True)

        self.assertEqual(delta["listings"], [])
        self.assertEqual(delta["expired_external_ids"], [])
        self.assertEqual(delta["current_external_ids"], ["one", "two"])
        self.assertTrue(delta["summary"]["delta_bootstrap"])
        self.assertTrue(delta["summary"]["delta_bootstrap_without_upserts"])

    def test_delta_chunking_only_prunes_on_final_chunk(self):
        calls = []

        def fake_send(payload, *, endpoint, secret, timeout=120):
            calls.append(payload)
            return {
                "batch_id": payload["batch_id"],
                "upserts": len(payload["listings"]),
                "expired": len(payload.get("expired_external_ids", [])),
            }

        payload = {
            "schema_version": 1,
            "batch_id": "source-batch",
            "generated_at": "2026-09-06T00:30:00+00:00",
            "mode": "delta",
            "summary": {},
            "listings": [
                {"external_id": "one", "comps": []},
                {"external_id": "two", "comps": []},
                {"external_id": "three", "comps": []},
            ],
            "expired_external_ids": ["gone"],
            "current_external_ids": ["one", "two", "three"],
        }

        with patch("pipelines.listing.remote_sync.send_snapshot", side_effect=fake_send):
            result = send_delta_chunked(payload, endpoint="https://example.test", secret="secret", chunk_size=2)

        self.assertEqual(result["upserts"], 3)
        self.assertEqual(len(calls), 2)
        self.assertNotIn("current_external_ids", calls[0])
        self.assertEqual(calls[0]["expired_external_ids"], [])
        self.assertEqual(calls[1]["current_external_ids"], ["one", "two", "three"])
        self.assertEqual(calls[1]["expired_external_ids"], ["gone"])

    def test_model_key_comp_payload_is_narrow(self):
        import pipelines.listing.remote_sync as remote_sync

        original_summaries = remote_sync.fetch_model_key_comp_summaries
        original_evidence = remote_sync.fetch_comp_evidence
        remote_sync.fetch_model_key_comp_summaries = lambda _keys: [
            {
                "source": "ebay-apple",
                "model_key": "apple-macbook-pro-a2442-14in-m1pro-16gb-512gb",
                "condition_grade": "B",
                "samples": 89,
                "median_final_price": 639.99,
                "computed_at": datetime(2026, 8, 18, tzinfo=UTC),
            }
        ]
        remote_sync.fetch_comp_evidence = lambda _rows: {
            ("apple-macbook-pro-a2442-14in-m1pro-16gb-512gb", "B"): [
                {
                    "source": "ebay-apple",
                    "external_id": "157855519619",
                    "title": "Apple MacBook Pro 14",
                    "url": "https://example.test/sold",
                    "sale_type": "bin",
                    "end_time": datetime(2026, 8, 17, tzinfo=UTC),
                    "final_price": 639.99,
                    "bids_count": 0,
                    "text_scan_grade": "B",
                    "text_scan_checked": True,
                    "text_scan_review_required": False,
                    "text_scan_risk_flags": [],
                    "text_scan_model_name": "gemma3:12b",
                    "text_scan_checked_at": datetime(2026, 8, 18, tzinfo=UTC),
                    "visual_condition_grade": "B",
                    "visual_condition_checked": True,
                    "visual_review_required": False,
                    "visual_damage_flags": [],
                    "visual_model_name": "minicpm-v:8b",
                    "visual_checked_at": datetime(2026, 8, 18, tzinfo=UTC),
                }
            ]
        }
        try:
            payload = build_model_key_comp_payload(
                ["apple-macbook-pro-a2442-14in-m1pro-16gb-512gb"],
                generated_at=datetime(2026, 8, 19, tzinfo=UTC),
            )
        finally:
            remote_sync.fetch_model_key_comp_summaries = original_summaries
            remote_sync.fetch_comp_evidence = original_evidence

        self.assertEqual(payload["schema_version"], 1)
        self.assertNotIn("listings", payload)
        self.assertEqual(len(payload["model_key_comps"]), 1)
        self.assertEqual(payload["model_key_comps"][0]["samples"], 89)
        self.assertEqual(len(payload["model_key_comps"][0]["comps"]), 1)
        self.assertEqual(payload["model_key_comps"][0]["comps"][0]["external_id"], "157855519619")
        self.assertTrue(payload["model_key_comps"][0]["comps"][0]["text_scan_checked"])
        self.assertTrue(payload["model_key_comps"][0]["comps"][0]["visual_condition_checked"])

    def test_sold_evidence_payload_is_narrow_and_broad(self):
        import pipelines.listing.remote_sync as remote_sync

        original_rows = remote_sync.fetch_sold_evidence_rows
        remote_sync.fetch_sold_evidence_rows = lambda *, since_days=None: [
            {
                "source": "ebay-lego",
                "external_id": "lego-sold-1",
                "title": "LEGO set sold",
                "url": "https://example.test/lego-sold-1",
                "sale_type": "auction",
                "end_time": datetime(2026, 8, 17, tzinfo=UTC),
                "final_price": 42.5,
                "bids_count": 5,
                "condition_grade": "B",
                "model_key": "lego-1234",
                "text_scan_grade": "C",
                "text_scan_checked": True,
                "text_scan_review_required": True,
                "text_scan_risk_flags": ["casing_damage"],
                "text_scan_model_name": "gemma3:12b",
                "text_scan_checked_at": datetime(2026, 8, 18, tzinfo=UTC),
                "visual_condition_grade": "C",
                "visual_condition_checked": True,
                "visual_review_required": True,
                "visual_damage_flags": ["corner_wear"],
                "visual_model_name": "minicpm-v:8b",
                "visual_checked_at": datetime(2026, 8, 18, tzinfo=UTC),
                "raw_attrs": {"must": "stay local"},
            }
        ]
        try:
            payload = build_sold_evidence_payload(generated_at=datetime(2026, 8, 19, tzinfo=UTC))
        finally:
            remote_sync.fetch_sold_evidence_rows = original_rows

        self.assertEqual(payload["schema_version"], 1)
        self.assertNotIn("listings", payload)
        self.assertEqual(len(payload["sold_evidence"]), 1)
        self.assertEqual(payload["sold_evidence"][0]["external_id"], "lego-sold-1")
        self.assertEqual(payload["sold_evidence"][0]["text_scan_grade"], "C")
        self.assertTrue(payload["sold_evidence"][0]["text_scan_checked"])
        self.assertEqual(payload["sold_evidence"][0]["text_scan_risk_flags"], ["casing_damage"])
        self.assertEqual(payload["sold_evidence"][0]["visual_condition_grade"], "C")
        self.assertTrue(payload["sold_evidence"][0]["visual_condition_checked"])
        self.assertEqual(payload["sold_evidence"][0]["visual_damage_flags"], ["corner_wear"])
        self.assertNotIn("raw_attrs", payload["sold_evidence"][0])


if __name__ == "__main__":
    unittest.main()
