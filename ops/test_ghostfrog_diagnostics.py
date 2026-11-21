import os
from pathlib import Path
import re
import unittest
from unittest.mock import patch

from mcp.server.fastmcp.exceptions import ToolError
import psycopg2
from psycopg2.extras import Json

from ops import ghostfrog_diagnostics as diagnostics
from ops.ghostfrog_diagnostic_catalog import RELATIONS


class DiagnosticSQLTests(unittest.TestCase):
    def test_supports_catalog_joins_nested_selects_and_aggregates(self):
        statement = """
            WITH overdue AS (
                SELECT source_id, status FROM auction_listings
                WHERE end_time < CURRENT_TIMESTAMP AND NOT finalized
            ) SELECT s.name, count(*) FILTER (WHERE o.status = 'live')
              FROM overdue o JOIN sources s ON s.id = o.source_id GROUP BY s.name;
        """
        self.assertEqual(diagnostics.validate_query(statement)[1], ["auction_listings", "sources"])
        self.assertEqual(diagnostics.validate_query(
            "SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY median_final_price) FROM latest_comps"
        )[1], ["latest_comps"])
        self.assertEqual(diagnostics.validate_query(
            "SELECT s.name FROM sources s WHERE EXISTS (SELECT 1 FROM auction_listings WHERE source_id = s.id)"
        )[1], ["auction_listings", "sources"])

    def test_rejects_statements_outside_read_only_allowlist_before_connect(self):
        rejected = (
            "INSERT INTO auction_listings (id) VALUES (1)",
            "SELECT 1; SELECT 2",
            "SELECT * FROM pg_catalog.pg_roles",
            "SELECT * FROM pg_roles",
            "SELECT * FROM public.auction_listings",
            "SELECT * FROM ebay_app_token",
            "SELECT * INTO copied FROM sources",
            "SELECT * FROM sources FOR SHARE",
            "SELECT pg_sleep(1)",
            "SELECT current_user",
            "SELECT current_setting('server_version')",
            "SELECT 'auction_listings'::regclass",
            "SELECT n FROM generate_series(1, 10) n",
            "SELECT 1 OPERATOR(public.+) 2",
            "WITH deleted AS (DELETE FROM sources RETURNING id) SELECT * FROM deleted",
            "WITH RECURSIVE r AS (SELECT 1) SELECT * FROM r",
            "WITH sample AS (WITH pg_roles AS (SELECT 1) SELECT * FROM pg_roles) SELECT * FROM pg_roles",
        )
        with patch.object(diagnostics, "_connection") as connect:
            for statement in rejected:
                with self.subTest(statement=statement), self.assertRaises(ToolError):
                    diagnostics.query(statement, 5)
            connect.assert_not_called()

    def test_input_limits_and_grant_catalog_agree(self):
        for limit in (0, 101, True, "3"):
            with self.subTest(limit=limit), self.assertRaises(ToolError):
                diagnostics.query("SELECT 1", limit)
        with self.assertRaises(diagnostics.RejectedQuery):
            diagnostics.validate_query("SELECT '" + "a" * diagnostics.MAX_QUERY_BYTES + "'")
        grant_file = (
            Path(__file__).resolve().parents[1] / "migrations" / "20261121_002_add_ghostfrog_diagnostics_reader.sql"
        )
        grants = dict(re.findall(r"\('([a-z0-9_]+)', '([a-z_ ]+)'\)", grant_file.read_text()))
        self.assertEqual(grants, {
            relation: " ".join(columns.split()) for relation, columns in RELATIONS.items()
            if relation != "apple_listing_evidence_v"
        })
        self.assertNotIn("ebay_app_token", grants)
        self.assertNotIn("raw_attrs", grants["auction_listings"])
        self.assertNotIn("last_error", grants["listing_visual_condition_queue"])

    def test_product_evidence_is_queryable_without_expanding_sql_operations(self):
        statement = """
            SELECT evidence_seq, field, product_value, part_index, part_count
            FROM apple_listing_evidence_v
            WHERE listing_id = 42 AND evidence_seq > 5
            ORDER BY evidence_seq
        """
        self.assertEqual(diagnostics.validate_query(statement)[1], ["apple_listing_evidence_v"])
        for statement in (
            "SELECT title FROM public.auction_listings",
            "SELECT ghostfrog_apple_evidence('title', NULL)",
            "SELECT product_value::jsonb FROM apple_listing_evidence_v",
            "UPDATE apple_listing_evidence_v SET product_value = 'changed'",
        ):
            with self.subTest(statement=statement), self.assertRaises(diagnostics.RejectedQuery):
                diagnostics.validate_query(statement)

    def test_cell_clipping_is_detectable(self):
        long_value = "M1 8GB RAM and 256GB SSD " * 40
        self.assertEqual(diagnostics._json_cell(long_value), (long_value[:512], True))
        self.assertEqual(diagnostics._json_cell("M1 8GB"), ("M1 8GB", False))


@unittest.skipUnless(os.getenv("GF_TEST_DIAGNOSTIC_LOGIN") == "1", "Requires the deployed diagnostics login")
class DiagnosticEvidenceIntegrationTests(unittest.TestCase):
    def test_sanitizes_original_titles_and_structured_values_before_chunking(self):
        title = "Apple MacBook Air A2337 13.3-inch M1 8GB RAM 256GB SSD"
        description = ("Screen line visible on boot; the USB-C port works. " * 24).strip()
        attributes = {
            "Model": ["MacBook Air", {"values": ["A2337", "M1"], "seller": "private-alias"}],
            "Storage Capacity": "256GB",
            "RAM Size": "8 GB",
            "Browse aspect Network": ["Unlocked", "5G"],
            "Browse aspect Seller Username": "private-alias",
            "Features": [
                "Activation Lock off; buyer [at] example [dot] invalid",
                "Small dent; phone +44 7700 900 123 | 221B Baker Street NW1 6XE",
                "USB-C charges | Authorization: Bearer fakeToken | Cookie: foo=letters; sid=secretValue",
            ],
            "Condition Description": description + " | seller name: Private Person",
            "pack quantity": 4,
            "Activation Lock": False,
            "Browse seller username": "private-alias",
            "Shipping Address": "private-street",
            "Access Token": "hidden-figures",
            "Other": {"Model": "should-not-be-promoted"},
        }
        with diagnostics._connection() as conn, conn.cursor() as cursor:
            cursor.execute(
                "SELECT field,value_index,part_index,part_count,product_value "
                "FROM public.ghostfrog_apple_evidence(%s,%s) ORDER BY evidence_seq",
                (title, Json(attributes)),
            )
            rows = cursor.fetchall()
        text = " ".join(row[-1] for row in rows)
        self.assertEqual(rows[0], ("title", 1, 1, 1, title))
        self.assertEqual([row[-1] for row in rows if row[0] == "Model"], ["MacBook Air", "A2337", "M1"])
        self.assertIn(("pack quantity", 1, 1, 1, "4"), rows)
        self.assertEqual([row[-1] for row in rows if row[0] == "Browse aspect Network"], ["Unlocked", "5G"])
        self.assertIn(("Activation Lock", 1, 1, 1, "false"), rows)
        self.assertIn("Activation Lock off", text)
        self.assertIn("Small dent", text)
        self.assertIn("USB-C charges", text)
        self.assertIn("[redacted]", text)
        for private in (
            "private-alias", "private-street", "hidden-figures", "should-not-be-promoted",
            "example", "7700", "Baker Street", "NW1", "fakeToken", "sid=", "secretValue", "Private Person",
        ):
            self.assertNotIn(private, text)
        chunks = [row for row in rows if row[0] == "Condition Description"]
        self.assertGreater(len(chunks), 1)
        self.assertEqual([row[2] for row in chunks], list(range(1, len(chunks) + 1)))
        self.assertTrue(all(row[3] == len(chunks) and len(row[-1]) <= 480 for row in chunks))
        self.assertEqual("".join(row[-1] for row in chunks), description + " | [redacted]")

    def test_current_login_can_read_view_and_cannot_read_raw_columns_or_write(self):
        with diagnostics._connection() as conn, conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT current_user, current_setting('transaction_read_only'),
                    has_column_privilege('public.auction_listings', 'raw_attrs', 'SELECT'),
                    has_column_privilege('public.auction_listings', 'title', 'SELECT'),
                    has_table_privilege('public.apple_listing_evidence_v', 'SELECT'),
                    has_table_privilege('public.auction_listings', 'UPDATE'),
                    has_table_privilege('public.apple_listing_evidence_v', 'UPDATE')
                """
            )
            self.assertEqual(cursor.fetchone(), (diagnostics.ROLE, "on", False, False, True, False, False))
            cursor.execute(
                "SELECT name FROM public.schema_migrations WHERE name = %s",
                ("20261121_003_add_apple_listing_evidence.sql",),
            )
            self.assertIsNotNone(cursor.fetchone())
        for column in ("raw_attrs", "title"):
            with self.assertRaises(ToolError):
                diagnostics.query(f"SELECT {column} FROM auction_listings LIMIT 0", 1)
        # A zero-row UPDATE verifies the grant independently of transaction mode.
        with diagnostics._connection() as conn, conn.cursor() as cursor:
            conn.set_session(readonly=False)
            with self.assertRaises(psycopg2.errors.InsufficientPrivilege):
                cursor.execute("UPDATE public.auction_listings SET model_key = model_key WHERE false")
