from datetime import date
from decimal import Decimal
import json
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

from ops import ghostfrog_spend as spend


DAY_START = 1795219200  # 2026-11-21 00:00 UTC
NOW = DAY_START + 75
ORG = "org-Test"
GBP_PER_USD = Decimal("0.8")


def bucket(start: int, entries: list) -> dict:
    return {"object": "bucket", "start_time": start, "end_time": start + spend.DAY, "results": entries}


def page(buckets: list, cursor=None) -> bytes:
    return json.dumps({"object": "page", "data": buckets, "has_more": bool(cursor), "next_page": cursor}).encode()


class CostsTests(unittest.TestCase):
    @patch.object(spend, "_request")
    def test_complete_empty_bucket_and_missing_response_differ(self, request):
        request.return_value = page([bucket(DAY_START, [])])
        self.assertEqual(spend._costs("test", ORG, DAY_START, NOW), ("USD", Decimal(0)))
        request.return_value = page([])
        with self.assertRaises(spend.Unavailable):
            spend._costs("test", ORG, DAY_START, NOW)

    @patch.object(spend, "_request")
    def test_cost_reads_paginate_and_do_not_assume_tunnel_attribution(self, request):
        first = {"object": "organization.costs.result", "amount": {"currency": "usd", "value": 0.1}}
        second = {"object": "organization.costs.result", "amount": {"currency": "usd", "value": 0.2}}
        request.side_effect = [
            page([bucket(DAY_START, [first])], cursor="next"),
            page([bucket(DAY_START + spend.DAY, [second])]),
        ]
        self.assertEqual(spend._costs("test", ORG, DAY_START, NOW + spend.DAY), ("USD", Decimal("0.3")))
        self.assertIn("page=next", request.call_args.args[0])
        self.assertNotIn("api_key_ids", request.call_args.args[0])

    @patch.object(spend, "_request")
    def test_fx_uses_rates_from_the_same_day_and_rejects_stale_quotes(self, request):
        request.return_value = b'<root><Cube time="2026-11-20"><Cube currency="USD" rate="1.25"/><Cube currency="GBP" rate="1"/></Cube></root>'
        self.assertEqual(spend._exchange_rate("USD", date(2026, 11, 21)), (GBP_PER_USD, "2026-11-20"))
        with self.assertRaises(spend.Unavailable):
            spend._exchange_rate("USD", date(2026, 12, 1))


class SpendStateTests(unittest.TestCase):
    def setUp(self):
        temp = TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name)

    def _reading(self, total, offset):
        try:
            previous = spend._load(self.root)
        except FileNotFoundError:
            previous = {}
        spend._write(self.root, spend._update(
            previous, ORG, NOW + offset, Decimal(total), "USD", GBP_PER_USD, "2026-11-20",
        ))
        return spend.current_summary(self.root, now=NOW + offset)

    def test_baseline_first_charge_repeat_ack_and_new_charge(self):
        baseline = self._reading("10", 0)
        self.assertEqual(baseline["increase"], "0")
        self.assertIsNone(baseline["first_charge_event"])
        initial = self._reading("10.25", 60)
        self.assertEqual(initial["increase"], "0.25")
        self.assertIsNotNone(initial["first_charge_event"])
        self.assertIsNone(initial["repeat_alert_id"])
        self.assertIsNone(self._reading("16.25", 90)["repeat_alert_id"])  # Exactly £5.
        exceeded = self._reading("17", 100)
        self.assertEqual(exceeded["first_charge_event"], initial["first_charge_event"])
        self.assertEqual(exceeded["increase_gbp_estimate"], "5.6000")
        alert = exceeded["repeat_alert_id"]
        self.assertIsNotNone(alert)
        self.assertEqual(self._reading("17", 160)["repeat_alert_id"], alert)

        spend.acknowledge(self.root, alert, NOW + 175)
        self.assertIsNone(self._reading("17", 180)["repeat_alert_id"])
        self.assertIsNone(self._reading("16.8", 190)["repeat_alert_id"])
        new_alert = self._reading("17.01", 200)["repeat_alert_id"]
        self.assertIsNotNone(new_alert)
        self.assertNotEqual(new_alert, alert)
        with self.assertRaises(spend.Unavailable):
            spend.acknowledge(self.root, alert, NOW + 205)

    def test_outage_and_stale_source_never_report_zero(self):
        self.assertEqual(spend.current_summary(self.root, now=NOW)["status"], "monitoring unavailable")
        self._reading("10", 0)
        with self.assertRaises(spend.Unavailable):
            spend.refresh(self.root, NOW + 10)
        summary = spend.current_summary(self.root, now=NOW + 10)
        self.assertEqual(summary["status"], "monitoring unavailable")
        self.assertNotIn("total", summary)
        self.assertEqual(self._reading("10.25", 60)["increase"], "0.25")
        stale = spend.current_summary(self.root, now=NOW + 61 + spend.MAX_AGE_SECONDS)
        self.assertEqual(stale["status"], "monitoring unavailable")
        self.assertNotIn("total", stale)
