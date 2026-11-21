from contextlib import contextmanager
from pathlib import Path
import sys
import unittest
from unittest.mock import MagicMock, patch

from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

from ops import ghostfrog_mcp as server


MODEL = "apple-macbook-air-a2337-13in-m1-8gb-256gb"
TITLE = 'Apple MacBook Air A2337 13" M1 8GB RAM 256GB SSD'


def listing(item_id=123456789012, **fields):
    return {
        "external_id": f"v1|{item_id}|0",
        "title": TITLE,
        "raw_attrs": {"Private Field": "must not be returned"},
        "model_key": MODEL,
        "condition_grade": "B",
        "status": "live",
        "price_current": 300,
        "final_price": None,
        "bids_count": 3,
        "end_time": None,
        "last_seen_at": None,
        "review_required": False,
        "review_action": None,
        "text_grade": None,
        "visual_grade": None,
        **fields,
    }


class ValuationTests(unittest.TestCase):
    def test_strict_identity_and_condition_rejects(self):
        self.assertEqual(server._identity_and_reject(listing()), (MODEL, None))
        for row in (
            listing(title=TITLE + " for parts"),
            listing(title='Apple MacBook Air A2337 13" M1 8GB RAM'),
            listing(raw_attrs={"Processor": "Apple M2"}),
            listing(raw_attrs={"RAM Size": "16 GB"}),
            listing(raw_attrs={"SSD Capacity": "512 GB"}),
            listing(text_grade="D"),
            listing(review_action="down"),
        ):
            self.assertIsNotNone(server._identity_and_reject(row)[1])

    def test_valuation_filters_and_deduplicates_before_calculating(self):
        sales = [
            listing(123456789100 + index, status="sold", final_price=500)
            for index in range(10)
        ]
        sales.extend([
            dict(sales[0], external_id="123456789100"),
            listing(123456789200, status="sold", final_price=1500, text_grade="C"),
            listing(123456789201, status="sold", final_price=10, title=TITLE + " spares or repairs"),
        ])
        cursor = MagicMock()
        cursor.fetchone.return_value = listing()
        cursor.fetchall.return_value = sales

        @contextmanager
        def read_cursor():
            yield cursor

        with patch.object(server, "_read_cursor", read_cursor):
            result = server.ghostfrog_macbook_valuation("123456789012", limit=1)
            self.assertEqual(result["comp_count"], 10)
            self.assertEqual(len(result["sold_comps"]), 1)
            self.assertEqual(result["sold_median_gbp"], 500)
            self.assertEqual(result["max_safe_buy_gbp"], 368)
            self.assertIsNone(result["reject_reason"])
            self.assertNotIn(TITLE, str(result))
            self.assertNotIn("Private Field", str(result))

            cursor.fetchall.return_value = sales[:9]
            thin = server.ghostfrog_macbook_valuation("123456789012")
            self.assertEqual(thin["reject_reason"], "insufficient_same_grade_comps")
            self.assertIsNone(thin["max_safe_buy_gbp"])


class ProtocolTests(unittest.IsolatedAsyncioTestCase):
    async def test_stdio_allowlist_validation_and_backend_unavailable(self):
        parameters = StdioServerParameters(
            command=sys.executable,
            args=["-m", "ops.ghostfrog_mcp"],
            cwd=Path(__file__).resolve().parents[1],
            env={"PGSERVICEFILE": "/dev/null"},
        )
        async with stdio_client(parameters) as (read, write):
            async with ClientSession(read, write) as session:
                await session.initialize()
                tools = (await session.list_tools()).tools
                self.assertEqual({tool.name for tool in tools}, {
                    "ghostfrog_source_coverage", "ghostfrog_macbook_listings",
                    "ghostfrog_macbook_bid_history", "ghostfrog_macbook_valuation",
                })
                self.assertTrue(all(tool.annotations.readOnlyHint for tool in tools))
                coverage = "ghostfrog_source_coverage"
                for invalid in (0, 26, "1"):
                    result = await session.call_tool(coverage, {"limit": invalid})
                    self.assertTrue(result.isError)
                bad_item = await session.call_tool("ghostfrog_macbook_bid_history", {"item_id": "invalid"})
                self.assertTrue(bad_item.isError)
                result = await session.call_tool(coverage, {"limit": 1})
                self.assertTrue(result.isError)
                self.assertIn("PostgreSQL is unavailable", result.content[0].text)
                self.assertNotIn("PGSERVICEFILE", result.content[0].text)


class DatabaseConnectionTests(unittest.TestCase):
    @patch.object(server.psycopg2, "connect")
    def test_uses_dedicated_loopback_login_and_read_only_transaction(self, connect):
        with server._read_cursor():
            pass
        self.assertEqual(connect.call_args.kwargs["user"], "ghostfrog_mcp")
        self.assertEqual(connect.call_args.kwargs["hostaddr"], "127.0.0.1")
        connect.return_value.set_session.assert_called_once_with(readonly=True)
        connect.return_value.close.assert_called_once()
