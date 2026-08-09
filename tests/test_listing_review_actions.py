import unittest
from unittest.mock import patch

from pipelines.listing.review_actions import list_favourites, normalise_external_id, review_url


class _FakeCursor:
    def __init__(self):
        self.calls = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, sql, params=None):
        self.calls.append((sql, params))

    def fetchall(self):
        return []


class _FakeConnection:
    def __init__(self):
        self.cursor_obj = _FakeCursor()

    def close(self):
        return None

    def cursor(self, *args, **kwargs):
        return self.cursor_obj


class ListingReviewActionTests(unittest.TestCase):
    def test_normalise_external_id_accepts_bare_item_id(self):
        self.assertEqual(normalise_external_id("168181595699"), "v1|168181595699|0")

    def test_normalise_external_id_accepts_url(self):
        self.assertEqual(
            normalise_external_id("https://www.ebay.co.uk/itm/168181595699"),
            "v1|168181595699|0",
        )

    def test_review_url_keeps_query_short(self):
        url = review_url(
            base_url="http://127.0.0.1:8765",
            action="down",
            source="ebay-apple",
            external_id="v1|168181595699|0",
            title="Very long title",
            url="https://www.ebay.co.uk/itm/168181595699?amdata=very-long",
        )

        self.assertIn("action=down", url)
        self.assertIn("source=ebay-apple", url)
        self.assertIn("external_id=v1%7C168181595699%7C0", url)
        self.assertNotIn("amdata", url)

    def test_list_favourites_only_returns_still_buyable_live_rows(self):
        fake_conn = _FakeConnection()
        with patch("pipelines.listing.review_actions.ensure_review_table"), patch(
            "pipelines.listing.review_actions.get_fresh_connection",
            return_value=fake_conn,
        ):
            rows = list_favourites(limit=25)

        self.assertEqual(rows, [])
        sql, params = fake_conn.cursor_obj.calls[-1]
        self.assertEqual(params, (25,))
        self.assertIn("JOIN auction_listings AS a", sql)
        self.assertIn("latest.action = 'up'", sql)
        self.assertIn("a.status = 'live'", sql)
        self.assertIn("COALESCE(a.finalized, FALSE) = FALSE", sql)
        self.assertIn("a.end_time IS NULL", sql)
        self.assertIn("a.end_time > (now() AT TIME ZONE 'utc')", sql)


if __name__ == "__main__":
    unittest.main()
