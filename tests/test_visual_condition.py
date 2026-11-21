import base64
import unittest
from unittest.mock import patch

from pipelines.listing.visual_condition.ollama_client import (
    assess_images,
    image_urls_from_attrs,
    normalise_visual_assessment,
)
from pipelines.listing.visual_condition.__main__ import (
    VisualCandidate,
    enqueue_visual_candidates,
    load_candidates,
    parse_targets,
    save_visual_condition,
)


class _FakeImageResponse:
    content = b"fake-image"

    def raise_for_status(self):
        return None


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

    def close(self):
        return None


class VisualConditionTests(unittest.TestCase):
    def test_parse_targets_defaults_to_all_image_backed_rows(self):
        self.assertEqual(
            parse_targets([]),
            [("", "")],
        )

    def test_parse_targets_rejects_invalid_values(self):
        with self.assertRaises(SystemExit):
            parse_targets(["ebay-apple"])

    def test_image_urls_from_attrs_uses_browse_urls(self):
        urls = image_urls_from_attrs(
            {
                "Browse imageUrls": [
                    "https://i.ebayimg.test/one.jpg",
                    "https://i.ebayimg.test/one.jpg",
                    "not-a-url",
                    "https://i.ebayimg.test/two.jpg",
                ]
            }
        )

        self.assertEqual(
            urls,
            [
                "https://i.ebayimg.test/one.jpg",
                "https://i.ebayimg.test/two.jpg",
            ],
        )

    def test_damage_flags_cap_clean_grade_to_c(self):
        result = normalise_visual_assessment(
            {
                "visual_grade": "B",
                "damage_flags": ["cracked rear glass", "corner chip"],
                "confidence": 0.88,
                "review_required": False,
            },
            image_count=3,
        )

        self.assertEqual(result["visual_grade"], "C")
        self.assertEqual(result["damage_flags"], ["cracked_back_glass", "chipped_corner"])
        self.assertTrue(result["review_required"])

    def test_lock_screen_caps_to_d(self):
        result = normalise_visual_assessment(
            {
                "visual_grade": "B",
                "damage_flags": ["activation lock"],
                "confidence": 0.9,
            },
            image_count=1,
        )

        self.assertEqual(result["visual_grade"], "D")
        self.assertTrue(result["review_required"])

    def test_no_images_is_unknown_review(self):
        result = assess_images(title="iPhone 15", image_urls=[])

        self.assertEqual(result["visual_grade"], "unknown")
        self.assertTrue(result["review_required"])
        self.assertEqual(result["notes"], "no images available")

    def test_assess_images_downloads_and_posts_to_ollama(self):
        posted = {}

        def fake_post(url, json, timeout):
            posted["url"] = url
            posted["json"] = json
            posted["timeout"] = timeout
            return _FakeOllamaResponse(
                '{"visual_grade":"B","damage_flags":[],"confidence":0.91,'
                '"review_required":false,"notes":"clean"}'
            )

        with patch(
            "pipelines.listing.visual_condition.ollama_client.requests.get",
            return_value=_FakeImageResponse(),
        ), patch(
            "pipelines.listing.visual_condition.ollama_client.requests.post",
            side_effect=fake_post,
        ):
            result = assess_images(
                title="Apple iPhone 15 Pro",
                image_urls=["https://i.ebayimg.test/phone.jpg"],
                model="local-vision:test",
            )

        self.assertEqual(result["visual_grade"], "B")
        self.assertFalse(result["review_required"])
        self.assertEqual(posted["json"]["model"], "local-vision:test")
        self.assertEqual(
            posted["json"]["messages"][1]["images"],
            [base64.b64encode(b"fake-image").decode("ascii")],
        )

    def test_all_live_candidate_selection_does_not_require_comps(self):
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
                    "price_current": 40,
                    "median_final_price": 0,
                    "comp_samples": 0,
                    "raw_attrs": {"Browse imageUrls": ["https://i.ebayimg.test/phone.jpg"]},
                }
            ]
        )

        with patch("pipelines.listing.visual_condition.__main__.ensure_review_table"), patch(
            "pipelines.listing.visual_condition.__main__.ensure_visual_condition_table"
        ), patch("pipelines.listing.visual_condition.__main__.schema.get_fresh_connection", return_value=fake_conn):
            candidates = load_candidates(
                limit=5,
                source="ebay-apple",
                product="iphone",
                min_profit=999,
                min_comps=999,
                include_checked=False,
                all_live=True,
            )

        self.assertEqual(len(candidates), 1)
        self.assertEqual(candidates[0].external_id, "v1|123|0")
        self.assertEqual(candidates[0].comp_samples, 0)
        self.assertIn("latest_listing_text_conditions", fake_conn.cursor_obj.sql)
        self.assertIn("LEFT JOIN latest_comps", fake_conn.cursor_obj.sql)
        self.assertIn("a.first_seen DESC", fake_conn.cursor_obj.sql)

    def test_queue_enqueue_marks_new_listing_priority_without_ebay_api_calls(self):
        fake_conn = _FakeConnection([{"id": 44}])

        with patch("pipelines.listing.visual_condition.__main__.ensure_visual_condition_table"), patch(
            "pipelines.listing.visual_condition.__main__.ensure_visual_condition_queue_table"
        ), patch("pipelines.listing.visual_condition.__main__.schema.get_connection", return_value=fake_conn):
            inserted = enqueue_visual_candidates(
                source="ebay-apple",
                product="iphone",
                limit=25,
                max_attempts=5,
                new_listing_minutes=120,
            )

        self.assertEqual(inserted, 1)
        enqueue_sql = fake_conn.cursor_obj.sql
        self.assertIn("priority_flag", enqueue_sql)
        self.assertIn("'new_listing'", enqueue_sql)
        self.assertIn("'backfill'", enqueue_sql)
        self.assertIn("a.raw_attrs ? 'Browse imageUrls'", enqueue_sql)
        self.assertIn("NOT EXISTS", enqueue_sql)
        self.assertEqual(fake_conn.cursor_obj.params[-2:], [25, 5])

    def test_save_visual_condition_marks_listing_visually_checked(self):
        fake_conn = _FakeConnection([])
        candidate = VisualCandidate(
            listing_id=123,
            external_id="v1|123|0",
            source="ebay-apple",
            title="Apple iPhone 12",
            url="https://www.ebay.co.uk/itm/123",
            model_key="apple-iphone-12-64gb",
            condition_grade="B",
            price_current=100,
            median_final_price=150,
            comp_samples=5,
            expected_profit=20,
            image_urls=["https://i.ebayimg.test/phone.jpg"],
        )

        with patch("pipelines.listing.visual_condition.__main__.ensure_auction_listing_visual_flags"), patch(
            "pipelines.listing.visual_condition.__main__.schema.get_connection",
            return_value=fake_conn,
        ):
            save_visual_condition(
                candidate,
                {
                    "visual_grade": "C",
                    "confidence": 0.9,
                    "damage_flags": ["chipped_corner"],
                    "review_required": True,
                },
                model="local-vision:test",
            )

        executed_sql = "\n".join(fake_conn.cursor_obj.sqls)
        self.assertIn("INSERT INTO listing_visual_conditions", executed_sql)
        self.assertIn("visual_condition_checked = true", executed_sql)
        self.assertIn("visual_condition_checked_at", executed_sql)


if __name__ == "__main__":
    unittest.main()
