import unittest

from pipelines.listing.review_actions import normalise_external_id, review_url


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


if __name__ == "__main__":
    unittest.main()
