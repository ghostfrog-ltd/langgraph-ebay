import unittest

from utils.ebay_api_accounting import build_api_accounting, flatten_rate_limits


class EbayApiAccountingTests(unittest.TestCase):
    def test_groups_local_services_into_real_ebay_buckets(self):
        accounting = build_api_accounting(
            {
                "ebay_ended_v1": 4872,
                "ebay_attributes_v1": 2292,
                "ebay_base_fetch_items_by_id_v1": 1353,
                "ebay_base_fetch_category_items_v1": 1078,
                "ebay_base_seller_items_v1": 7,
                "ebay_auth_v1": 11,
            }
        )

        self.assertEqual(accounting["buckets"]["trading"]["used"], 7164)
        self.assertTrue(accounting["buckets"]["trading"]["blocked"])
        self.assertIn("ebay_ended_v1", accounting["blocked_services"])
        self.assertIn("ebay_attributes_v1", accounting["blocked_services"])

        self.assertEqual(accounting["buckets"]["browse"]["used"], 2438)
        self.assertFalse(accounting["buckets"]["browse"]["blocked"])
        self.assertNotIn("ebay_base_fetch_items_by_id_v1", accounting["blocked_services"])

        self.assertEqual(accounting["buckets"]["identity"]["used"], 11)
        self.assertEqual(accounting["known_total"], 9613)

    def test_flattens_ebay_developer_analytics_response(self):
        rows = flatten_rate_limits(
            {
                "rateLimits": [
                    {
                        "apiContext": "buy",
                        "apiName": "browse",
                        "apiVersion": "v1",
                        "resources": [
                            {
                                "name": "item_summary/search",
                                "rates": [
                                    {
                                        "limit": 5000,
                                        "count": 123,
                                        "remaining": 4877,
                                        "reset": "2026-07-28T00:00:00.000Z",
                                        "timeWindow": 86400,
                                    }
                                ],
                            }
                        ],
                    }
                ]
            }
        )

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["api_context"], "buy")
        self.assertEqual(rows[0]["api_name"], "browse")
        self.assertEqual(rows[0]["resource"], "item_summary/search")
        self.assertEqual(rows[0]["remaining"], 4877)


if __name__ == "__main__":
    unittest.main()
