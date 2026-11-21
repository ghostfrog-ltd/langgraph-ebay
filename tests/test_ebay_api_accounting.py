import unittest
from unittest.mock import patch

from utils.ebay_api_accounting import (
    bucket_limit_overrides_from_rate_limits,
    build_api_accounting,
    flatten_rate_limits,
)


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
        self.assertEqual(accounting["buckets"]["trading"]["limit"], 50000)
        self.assertFalse(accounting["buckets"]["trading"]["blocked"])
        self.assertNotIn("ebay_ended_v1", accounting["blocked_services"])
        self.assertNotIn("ebay_attributes_v1", accounting["blocked_services"])

        self.assertEqual(accounting["buckets"]["browse"]["used"], 2438)
        self.assertEqual(accounting["buckets"]["browse"]["limit"], 5000)
        self.assertFalse(accounting["buckets"]["browse"]["blocked"])
        self.assertNotIn("ebay_base_fetch_items_by_id_v1", accounting["blocked_services"])

        self.assertEqual(accounting["buckets"]["identity"]["used"], 11)
        self.assertEqual(accounting["known_total"], 9613)

    def test_trading_limit_can_be_overridden_by_env(self):
        with patch.dict("os.environ", {"EBAY_BUCKET_LIMIT_TRADING": "60000"}):
            accounting = build_api_accounting({"ebay_ended_v1": 51000})

        self.assertEqual(accounting["buckets"]["trading"]["limit"], 60000)
        self.assertFalse(accounting["buckets"]["trading"]["blocked"])

    def test_can_apply_live_ebay_limit_overrides(self):
        overrides = bucket_limit_overrides_from_rate_limits(
            [
                {
                    "api_context": "sell",
                    "api_name": "TradingAPI",
                    "resource": "GetItem",
                    "limit": 75000,
                    "time_window": 86400,
                },
                {
                    "api_context": "buy",
                    "api_name": "browse",
                    "resource": "item_summary/search",
                    "limit": 5000,
                    "time_window": 86400,
                },
            ]
        )

        accounting = build_api_accounting(
            {"ebay_ended_v1": 51000},
            bucket_limit_overrides=overrides,
        )

        self.assertEqual(accounting["buckets"]["trading"]["limit"], 75000)
        self.assertFalse(accounting["buckets"]["trading"]["blocked"])
        self.assertEqual(accounting["buckets"]["browse"]["limit"], 5000)

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
