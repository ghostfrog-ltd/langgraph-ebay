import os
import unittest
from unittest.mock import Mock, patch

from pipelines.listing.slack_review_commands import (
    SlackOperatorCommand,
    format_slack_operator_command_menu,
    parse_slack_operator_command,
    parse_slack_help_command,
    parse_slack_review_command,
    process_slack_review_commands_once,
    run_slack_operator_command,
)


class SlackReviewCommandTests(unittest.TestCase):
    def test_parse_accepts_plain_item_id(self):
        command = parse_slack_review_command("up 206446095377")

        self.assertIsNotNone(command)
        self.assertEqual(command.action, "up")
        self.assertEqual(command.external_id, "206446095377")

    def test_parse_accepts_optional_prefix_and_browse_id(self):
        command = parse_slack_review_command("gf down v1|206446095377|0")

        self.assertIsNotNone(command)
        self.assertEqual(command.action, "down")
        self.assertEqual(command.external_id, "v1|206446095377|0")

    def test_parse_accepts_slack_url_markup(self):
        command = parse_slack_review_command(
            "up <https://www.ebay.co.uk/itm/206446095377|listing>"
        )

        self.assertIsNotNone(command)
        self.assertEqual(command.external_id, "https://www.ebay.co.uk/itm/206446095377")

    def test_parse_ignores_unknown_commands(self):
        self.assertIsNone(parse_slack_review_command("buy 206446095377"))
        self.assertIsNone(parse_slack_review_command("hello"))

    def test_parse_accepts_help_commands(self):
        self.assertTrue(parse_slack_help_command("ebay"))
        self.assertTrue(parse_slack_help_command("ebay commands"))
        self.assertTrue(parse_slack_help_command("`ebay`"))
        self.assertTrue(parse_slack_help_command("```ebay```"))
        self.assertTrue(parse_slack_help_command("gf commands"))
        self.assertTrue(parse_slack_help_command("<@U123> ebay"))
        self.assertFalse(parse_slack_help_command("up 206446095377"))

    def test_command_menu_lists_console_without_execution_claim(self):
        menu = format_slack_operator_command_menu()

        self.assertIn("Slack runs only this small safe subset", menu)
        self.assertIn("`ebay heartbeat`", menu)
        self.assertIn("`ebay serious`", menu)
        self.assertIn("`ebay explain 206446095377`", menu)
        self.assertIn("`ebay backfill-apply --limit 4000 --daily-limit 10000`", menu)
        self.assertIn("`ebay favourites`", menu)
        self.assertIn("`up 206446095377`", menu)
        self.assertIn("Local console commands are intentionally omitted", menu)
        self.assertNotIn("`ebay comps`", menu)
        self.assertNotIn("`ebay heartbeat-email`", menu)
        self.assertIn("no bids", menu)

    def test_parse_operator_accepts_explain_identifier(self):
        command = parse_slack_operator_command("ebay explain 206446095377")

        self.assertEqual(command, SlackOperatorCommand(name="explain", item="206446095377"))

    def test_parse_operator_accepts_explain_all_after_identifier(self):
        command = parse_slack_operator_command("ebay explain 206446095377 --all")

        self.assertEqual(
            command,
            SlackOperatorCommand(name="explain", item="206446095377", show_all=True),
        )

    def test_parse_operator_accepts_explain_all_before_identifier(self):
        command = parse_slack_operator_command("ebay explain --all 206446095377")

        self.assertEqual(
            command,
            SlackOperatorCommand(name="explain", item="206446095377", show_all=True),
        )

    def test_parse_operator_accepts_explain_in_code_block(self):
        command = parse_slack_operator_command("```ebay explain 206446095377```")

        self.assertEqual(command, SlackOperatorCommand(name="explain", item="206446095377"))

    def test_parse_operator_accepts_explain_in_language_code_block(self):
        command = parse_slack_operator_command("```text\nebay explain 206446095377\n```")

        self.assertEqual(command, SlackOperatorCommand(name="explain", item="206446095377"))

    def test_parse_operator_accepts_explain_slack_url_markup(self):
        command = parse_slack_operator_command(
            "ebay explain <https://www.ebay.co.uk/itm/206446095377|listing>"
        )

        self.assertEqual(
            command,
            SlackOperatorCommand(
                name="explain",
                item="https://www.ebay.co.uk/itm/206446095377",
            ),
        )

    def test_parse_operator_accepts_backfill_apply_limits(self):
        command = parse_slack_operator_command(
            "ebay backfill-apply --limit 4000 --daily-limit 10000"
        )

        self.assertEqual(
            command,
            SlackOperatorCommand(name="backfill-apply", limit=4000, daily_limit=10000),
        )

    def test_parse_operator_accepts_backfill_apply_equals_options(self):
        command = parse_slack_operator_command(
            "ebay backfill-apply --limit=4000 --daily-backfill-budget=10000 --include-vehicle-sources"
        )

        self.assertEqual(
            command,
            SlackOperatorCommand(
                name="backfill-apply",
                limit=4000,
                daily_limit=10000,
                include_vehicle_sources=True,
            ),
        )

    def test_parse_operator_reports_backfill_apply_option_errors(self):
        command = parse_slack_operator_command("ebay backfill-apply --limit nope")

        self.assertEqual(command.name, "backfill-apply")
        self.assertIn("--limit", command.error)

    def test_disabled_sync_does_not_call_slack(self):
        with patch.dict(os.environ, {"SLACK_REVIEW_COMMANDS_ENABLED": "0"}, clear=False):
            with patch("pipelines.listing.slack_review_commands.requests.get") as get:
                result = process_slack_review_commands_once()

        self.assertEqual(result.status, "skipped")
        self.assertFalse(result.enabled)
        get.assert_not_called()

    def test_sync_records_allowed_commands_and_replies(self):
        history = Mock()
        history.json.return_value = {
            "ok": True,
            "messages": [
                {"ts": "200.1", "text": "noise"},
                {"ts": "201.1", "text": "up 206446095377"},
            ],
        }
        reply = Mock()
        reply.json.return_value = {"ok": True}

        with patch.dict(
            os.environ,
            {
                "SLACK_REVIEW_COMMANDS_ENABLED": "1",
                "SLACK_REVIEW_CHANNEL_ID": "C123",
                "SLACK_BOT_TOKEN": "xoxb-secret",
            },
            clear=False,
        ):
            with patch("pipelines.listing.slack_review_commands.get_flag", return_value=None):
                with patch("pipelines.listing.slack_review_commands.set_flag") as set_flag:
                    with patch(
                        "pipelines.listing.slack_review_commands.requests.get",
                        return_value=history,
                    ) as get:
                        with patch(
                            "pipelines.listing.slack_review_commands.requests.post",
                            return_value=reply,
                        ) as post:
                            with patch(
                                "pipelines.listing.slack_review_commands.record_review_action",
                                return_value={
                                    "action": "up",
                                    "external_id": "v1|206446095377|0",
                                    "title": "MacBook Air",
                                },
                            ) as record:
                                result = process_slack_review_commands_once()

        self.assertEqual(result.status, "ok")
        self.assertEqual(result.fetched, 2)
        self.assertEqual(result.processed, 1)
        self.assertEqual(result.ignored, 1)
        self.assertEqual(result.replied, 1)
        get.assert_called_once()
        record.assert_called_once_with(external_id="206446095377", action="up")
        post.assert_called_once()
        self.assertTrue(post.call_args.kwargs["json"]["reply_broadcast"])
        set_flag.assert_called_once_with(
            "listing_slack_review_command_cursor",
            True,
            {"last_ts": "201.1"},
        )

    def test_sync_replies_to_ebay_help_without_recording_action(self):
        history = Mock()
        history.json.return_value = {
            "ok": True,
            "messages": [
                {"ts": "202.1", "text": "ebay"},
            ],
        }
        reply = Mock()
        reply.json.return_value = {"ok": True}

        with patch.dict(
            os.environ,
            {
                "SLACK_REVIEW_COMMANDS_ENABLED": "1",
                "SLACK_REVIEW_CHANNEL_ID": "C123",
                "SLACK_BOT_TOKEN": "xoxb-secret",
            },
            clear=False,
        ):
            with patch("pipelines.listing.slack_review_commands.get_flag", return_value=None):
                with patch("pipelines.listing.slack_review_commands.set_flag"):
                    with patch(
                        "pipelines.listing.slack_review_commands.requests.get",
                        return_value=history,
                    ):
                        with patch(
                            "pipelines.listing.slack_review_commands.requests.post",
                            return_value=reply,
                        ) as post:
                            with patch(
                                "pipelines.listing.slack_review_commands.record_review_action",
                            ) as record:
                                result = process_slack_review_commands_once()

        self.assertEqual(result.status, "ok")
        self.assertEqual(result.processed, 1)
        self.assertEqual(result.ignored, 0)
        self.assertEqual(result.replied, 1)
        record.assert_not_called()
        _url, kwargs = post.call_args
        self.assertIn("GhostFrog eBay commands", kwargs["json"]["text"])
        self.assertIn("`ebay serious`", kwargs["json"]["text"])

    def test_sync_acknowledges_operator_command_before_running(self):
        history = Mock()
        history.json.return_value = {
            "ok": True,
            "messages": [
                {"ts": "203.1", "text": "ebay stats"},
            ],
        }
        reply = Mock()
        reply.json.return_value = {"ok": True}

        with patch.dict(
            os.environ,
            {
                "SLACK_REVIEW_COMMANDS_ENABLED": "1",
                "SLACK_REVIEW_CHANNEL_ID": "C123",
                "SLACK_BOT_TOKEN": "xoxb-secret",
            },
            clear=False,
        ):
            with patch("pipelines.listing.slack_review_commands.get_flag", return_value=None):
                with patch("pipelines.listing.slack_review_commands.set_flag"):
                    with patch(
                        "pipelines.listing.slack_review_commands.requests.get",
                        return_value=history,
                    ):
                        with patch(
                            "pipelines.listing.slack_review_commands.requests.post",
                            return_value=reply,
                        ) as post:
                            with patch(
                                "pipelines.listing.slack_review_commands.run_slack_operator_command",
                                return_value=True,
                            ) as run:
                                result = process_slack_review_commands_once()

        self.assertEqual(result.status, "ok")
        self.assertEqual(result.processed, 1)
        self.assertEqual(result.replied, 1)
        run.assert_called_once()
        self.assertGreaterEqual(post.call_count, 1)
        self.assertIn("Running `ebay stats`", post.call_args_list[0].kwargs["json"]["text"])

    def test_sync_dispatches_explain_operator_command(self):
        history = Mock()
        history.json.return_value = {
            "ok": True,
            "messages": [
                {"ts": "204.1", "text": "ebay explain 206446095377"},
            ],
        }
        reply = Mock()
        reply.json.return_value = {"ok": True}

        with patch.dict(
            os.environ,
            {
                "SLACK_REVIEW_COMMANDS_ENABLED": "1",
                "SLACK_REVIEW_CHANNEL_ID": "C123",
                "SLACK_BOT_TOKEN": "xoxb-secret",
            },
            clear=False,
        ):
            with patch("pipelines.listing.slack_review_commands.get_flag", return_value=None):
                with patch("pipelines.listing.slack_review_commands.set_flag"):
                    with patch(
                        "pipelines.listing.slack_review_commands.requests.get",
                        return_value=history,
                    ):
                        with patch(
                            "pipelines.listing.slack_review_commands.requests.post",
                            return_value=reply,
                        ) as post:
                            with patch(
                                "pipelines.listing.slack_review_commands.run_slack_operator_command",
                                return_value=True,
                            ) as run:
                                result = process_slack_review_commands_once()

        self.assertEqual(result.status, "ok")
        self.assertEqual(result.processed, 1)
        self.assertEqual(result.replied, 1)
        run.assert_called_once_with(
            SlackOperatorCommand(name="explain", item="206446095377"),
            channel_id="C123",
        )
        self.assertIn(
            "Running `ebay explain 206446095377`",
            post.call_args_list[0].kwargs["json"]["text"],
        )

    def test_sync_dispatches_explain_all_operator_command(self):
        history = Mock()
        history.json.return_value = {
            "ok": True,
            "messages": [
                {"ts": "205.1", "text": "ebay explain 206446095377 --all"},
            ],
        }
        reply = Mock()
        reply.json.return_value = {"ok": True}

        with patch.dict(
            os.environ,
            {
                "SLACK_REVIEW_COMMANDS_ENABLED": "1",
                "SLACK_REVIEW_CHANNEL_ID": "C123",
                "SLACK_BOT_TOKEN": "xoxb-secret",
            },
            clear=False,
        ):
            with patch("pipelines.listing.slack_review_commands.get_flag", return_value=None):
                with patch("pipelines.listing.slack_review_commands.set_flag"):
                    with patch(
                        "pipelines.listing.slack_review_commands.requests.get",
                        return_value=history,
                    ):
                        with patch(
                            "pipelines.listing.slack_review_commands.requests.post",
                            return_value=reply,
                        ) as post:
                            with patch(
                                "pipelines.listing.slack_review_commands.run_slack_operator_command",
                                return_value=True,
                            ) as run:
                                result = process_slack_review_commands_once()

        self.assertEqual(result.status, "ok")
        self.assertEqual(result.processed, 1)
        self.assertEqual(result.replied, 1)
        run.assert_called_once_with(
            SlackOperatorCommand(name="explain", item="206446095377", show_all=True),
            channel_id="C123",
        )
        self.assertIn(
            "Running `ebay explain 206446095377 --all`",
            post.call_args_list[0].kwargs["json"]["text"],
        )

    def test_sync_dispatches_backfill_apply_and_saves_cursor_before_run(self):
        history = Mock()
        history.json.return_value = {
            "ok": True,
            "messages": [
                {"ts": "206.1", "text": "ebay backfill-apply --limit 4000 --daily-limit 10000"},
            ],
        }
        reply = Mock()
        reply.json.return_value = {"ok": True}

        with patch.dict(
            os.environ,
            {
                "SLACK_REVIEW_COMMANDS_ENABLED": "1",
                "SLACK_REVIEW_CHANNEL_ID": "C123",
                "SLACK_BOT_TOKEN": "xoxb-secret",
            },
            clear=False,
        ):
            with patch("pipelines.listing.slack_review_commands.get_flag", return_value=None):
                with patch("pipelines.listing.slack_review_commands.set_flag") as set_flag:
                    with patch(
                        "pipelines.listing.slack_review_commands.requests.get",
                        return_value=history,
                    ):
                        with patch(
                            "pipelines.listing.slack_review_commands.requests.post",
                            return_value=reply,
                        ) as post:
                            with patch(
                                "pipelines.listing.slack_review_commands.run_slack_operator_command",
                                return_value=True,
                            ) as run:
                                result = process_slack_review_commands_once()

        self.assertEqual(result.status, "ok")
        self.assertEqual(result.processed, 1)
        self.assertEqual(result.replied, 1)
        run.assert_called_once_with(
            SlackOperatorCommand(name="backfill-apply", limit=4000, daily_limit=10000),
            channel_id="C123",
        )
        self.assertGreaterEqual(set_flag.call_count, 2)
        set_flag.assert_any_call(
            "listing_slack_review_command_cursor",
            True,
            {"last_ts": "206.1"},
        )
        self.assertIn(
            "Running `ebay backfill-apply --limit 4000 --daily-limit 10000`",
            post.call_args_list[0].kwargs["json"]["text"],
        )

    def test_run_backfill_apply_posts_summary(self):
        conn = Mock()
        summary = {
            "status": "completed",
            "limit": 4000,
            "effective_limit": 12,
            "daily_backfill_budget": 10000,
            "backfill_checked_today": 4,
            "remaining_backfill_budget": 9996,
            "candidate_count": 12,
            "outcome_count": 12,
            "applied": 7,
            "counts": {
                "finalize:sold:ended_with_bids": 3,
                "finalize:ended:ended_without_confirmed_sale": 4,
                "skip:error:trading_api_timeout": 5,
            },
            "outcomes": [
                {
                    "external_id": "v1|1|0",
                    "title": "Sold listing",
                    "new_status": "sold",
                    "reason": "ended_with_bids",
                    "final_price": 121.5,
                }
            ],
        }

        with patch("pipelines.listing.slack_review_commands._post_channel_message", return_value=True) as post:
            with patch("pipelines.listing.point_zero.backfill_apply.get_fresh_connection", return_value=conn):
                with patch("pipelines.listing.point_zero.backfill_apply._try_acquire_lock", return_value=True):
                    with patch("pipelines.listing.point_zero.backfill_apply._release_lock") as release:
                        with patch(
                            "pipelines.listing.point_zero.backfill_apply.run_backfill",
                            return_value=summary,
                        ) as run:
                            ok = run_slack_operator_command(
                                SlackOperatorCommand(
                                    name="backfill-apply",
                                    limit=4000,
                                    daily_limit=10000,
                                ),
                                channel_id="C123",
                            )

        self.assertTrue(ok)
        run.assert_called_once_with(
            limit=4000,
            apply=True,
            daily_backfill_budget=10000,
            include_vehicle_sources=False,
        )
        release.assert_called_once_with(conn)
        conn.close.assert_called_once()
        text = post.call_args.kwargs["text"]
        self.assertIn("Backfill apply finished", text)
        self.assertIn("sold `3`", text)
        self.assertIn("ended `4`", text)
        self.assertIn("skipped/errors `5`", text)


if __name__ == "__main__":
    unittest.main()
