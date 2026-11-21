from pathlib import Path
import re
import unittest
from unittest.mock import patch

from mcp.server.fastmcp.exceptions import ToolError

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
        self.assertEqual(grants, {relation: " ".join(columns.split()) for relation, columns in RELATIONS.items()})
        self.assertNotIn("ebay_app_token", grants)
        self.assertNotIn("raw_attrs", grants["auction_listings"])
        self.assertNotIn("last_error", grants["listing_visual_condition_queue"])
