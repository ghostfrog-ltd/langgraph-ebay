import unittest
from unittest.mock import patch

from pipelines.listing.text_condition.ollama_client import (
    assess_text,
    captured_text_from_attrs,
    normalise_text_assessment,
)
from pipelines.listing.text_condition.__main__ import (
    TextCandidate,
    enqueue_text_candidates,
    load_candidates,
    save_text_condition,
)


class _FakeOllamaResponse:
    def __init__(self, content):
        self._content = content

    def raise_for_status(self):
        return None

    def json(self):
        return {"message": {"content": self._content}}


class _FakeCursor:
    def __init__(self, rows):
        self.rows = rows
        self.sql = ""
        self.sqls = []
        self.params = None

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def execute(self, sql, params=None):
        self.sql = sql
        self.sqls.append(sql)
        self.params = params

    def fetchall(self):
        return self.rows


class _FakeConnection:
    def __init__(self, rows):
        self.cursor_obj = _FakeCursor(rows)

    def cursor(self, cursor_factory=None):
        return self.cursor_obj

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False


class TextConditionTests(unittest.TestCase):
    def test_captured_text_from_attrs_keeps_listing_evidence(self):
        captured = captured_text_from_attrs(
            {
                "Browse conditionDescription": "Screen has scratches",
                "Browse seller feedbackScore": 1234,
                "Browse imageUrls": ["https://i.ebayimg.test/one.jpg"],
                "Other": "ignored",
            }
        )

        self.assertEqual(captured["Browse conditionDescription"], "Screen has scratches")
        self.assertEqual(captured["Browse seller feedbackScore"], 1234)
        self.assertNotIn("Browse imageUrls", captured)
        self.assertNotIn("Other", captured)

    def test_normalise_text_assessment_caps_serious_risk_to_d(self):
        result = normalise_text_assessment(
            {
                "text_grade": "B",
                "risk_flags": ["iCloud locked", "cracked screen"],
                "confidence": 0.88,
                "review_required": False,
            }
        )

        self.assertEqual(result["text_grade"], "D")
        self.assertEqual(result["risk_flags"], ["locked_device", "screen_damage"])
        self.assertTrue(result["review_required"])

    def test_assess_text_posts_captured_text_to_ollama(self):
        with patch(
            "pipelines.listing.text_condition.ollama_client.requests.post",
            return_value=_FakeOllamaResponse(
                '{"text_grade":"C","risk_flags":["screen_damage"],"confidence":0.91,'
                '"review_required":true,"notes":"screen scratched"}'
            ),
        ) as post:
            result = assess_text(
                title="Apple iPhone 12 64GB",
                raw_attrs={"Browse conditionDescription": "Screen scratched"},
                model_key="apple-iphone-12-64gb",
                source="ebay-apple",
                model="local-text:test",
            )

        self.assertEqual(result["text_grade"], "C")
        self.assertEqual(result["risk_flags"], ["screen_damage"])
        posted = post.call_args.kwargs
        self.assertEqual(posted["json"]["model"], "local-text:test")
        self.assertIn("captured_text", posted["json"]["messages"][1]["content"])

    def test_load_candidates_reads_unchecked_stored_text_rows(self):
        fake_conn = _FakeConnection(
            [
                {
                    "listing_id": 123,
                    "external_id": "v1|123|0",
                    "source": "ebay-apple",
                    "title": "Apple iPhone 12 64GB",
                    "url": "https://www.ebay.co.uk/itm/123",
                    "model_key": "apple-iphone-12-64gb",
                    "condition_grade": "B",
                    "status": "sold",
                    "raw_attrs": {"Browse conditionDescription": "Working"},
                }
            ]
        )

        with patch("pipelines.listing.text_condition.__main__.ensure_text_condition_table"), patch(
            "pipelines.listing.text_condition.__main__.schema.get_connection",
            return_value=fake_conn,
        ):
            candidates = load_candidates(
                limit=5,
                source="ebay-apple",
                product="iphone",
                status="sold",
            )

        self.assertEqual(len(candidates), 1)
        self.assertEqual(candidates[0].external_id, "v1|123|0")
        self.assertIn("latest_listing_text_conditions", fake_conn.cursor_obj.sql)
        self.assertIn("tc.id IS NULL", fake_conn.cursor_obj.sql)
        self.assertEqual(fake_conn.cursor_obj.params[-2:], ["sold", 5])

    def test_enqueue_text_candidates_queues_all_unchecked_rows_by_default(self):
        fake_conn = _FakeConnection([{"id": 44}])

        with patch("pipelines.listing.text_condition.__main__.ensure_text_condition_table"), patch(
            "pipelines.listing.text_condition.__main__.ensure_text_condition_queue_table"
        ), patch(
            "pipelines.listing.text_condition.__main__.schema.get_connection",
            return_value=fake_conn,
        ):
            inserted = enqueue_text_candidates(limit=1000, status="all")

        self.assertEqual(inserted, 1)
        enqueue_sql = fake_conn.cursor_obj.sql
        self.assertIn("listing_text_condition_queue", enqueue_sql)
        self.assertIn("tc.id IS NULL", enqueue_sql)
        self.assertIn("'new_listing'", enqueue_sql)
        self.assertIn("'sold_comp'", enqueue_sql)
        self.assertIn("'dashboard_potential'", enqueue_sql)
        self.assertIn("'potential_comp_evidence'", enqueue_sql)
        self.assertNotIn("AND a.source = %s", enqueue_sql)
        self.assertEqual(fake_conn.cursor_obj.params[:2], [360, 360])
        self.assertEqual(fake_conn.cursor_obj.params[-2:], [1000, 3])

    def test_save_text_condition_records_assessment_without_overwriting_coded_grade(self):
        fake_conn = _FakeConnection([])
        candidate = TextCandidate(
            listing_id=123,
            external_id="v1|123|0",
            source="ebay-apple",
            title="Apple iPhone 12",
            url="https://example.test/123",
            model_key="apple-iphone-12-64gb",
            condition_grade="B",
            status="live",
            raw_attrs={},
        )

        with patch("pipelines.listing.text_condition.__main__.ensure_auction_listing_text_flags"), patch(
            "pipelines.listing.text_condition.__main__.schema.get_connection",
            return_value=fake_conn,
        ):
            save_text_condition(
                candidate,
                {
                    "text_grade": "D",
                    "confidence": 0.9,
                    "risk_flags": ["locked_device"],
                    "review_required": True,
                },
                model="local-text:test",
            )

        executed_sql = "\n".join(fake_conn.cursor_obj.sqls)
        self.assertIn("INSERT INTO listing_text_conditions", executed_sql)
        self.assertIn("text_condition_checked = true", executed_sql)
        self.assertNotIn("condition_grade =", executed_sql)


if __name__ == "__main__":
    unittest.main()
