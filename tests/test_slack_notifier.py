import os
import unittest
from unittest.mock import Mock, patch

from utils.slack_notifier import send_slack_message


class SlackNotifierTests(unittest.TestCase):
    """Pure unit tests for secret-safe Slack notification behavior."""

    def test_disabled_slack_does_not_post(self):
        with patch.dict(os.environ, {"SLACK_NOTIFICATIONS_ENABLED": "0"}, clear=False):
            with patch("utils.slack_notifier.requests.post") as post:
                result = send_slack_message(text="pipeline heartbeat")

        self.assertEqual(result.status, "skipped")
        self.assertFalse(result.enabled)
        self.assertEqual(result.reason, "disabled")
        post.assert_not_called()

    def test_enabled_without_webhook_does_not_post(self):
        with patch.dict(
            os.environ,
            {
                "SLACK_NOTIFICATIONS_ENABLED": "1",
                "SLACK_WEBHOOK_URL": "",
            },
            clear=False,
        ):
            with patch("utils.slack_notifier.requests.post") as post:
                result = send_slack_message(text="pipeline heartbeat")

        self.assertEqual(result.status, "skipped")
        self.assertTrue(result.enabled)
        self.assertEqual(result.reason, "missing_webhook_url")
        post.assert_not_called()

    def test_enabled_slack_posts_payload(self):
        response = Mock(status_code=200)
        with patch.dict(
            os.environ,
            {
                "SLACK_NOTIFICATIONS_ENABLED": "1",
                "SLACK_WEBHOOK_URL": "https://hooks.slack.com/services/T000/B000/SECRET",
                "SLACK_TIMEOUT_SECONDS": "3",
            },
            clear=False,
        ):
            with patch("utils.slack_notifier.requests.post", return_value=response) as post:
                result = send_slack_message(
                    text="pipeline heartbeat",
                    blocks=[{"type": "section", "text": {"type": "mrkdwn", "text": "ok"}}],
                )

        self.assertEqual(result.status, "sent")
        self.assertEqual(result.status_code, 200)
        post.assert_called_once()
        _url, kwargs = post.call_args
        self.assertEqual(kwargs["json"]["text"], "pipeline heartbeat")
        self.assertFalse(kwargs["json"]["unfurl_links"])
        self.assertEqual(kwargs["timeout"], 3)

    def test_request_errors_do_not_expose_webhook_secret(self):
        webhook = "https://hooks.slack.com/services/T000/B000/SECRET"
        with patch.dict(
            os.environ,
            {
                "SLACK_NOTIFICATIONS_ENABLED": "1",
                "SLACK_WEBHOOK_URL": webhook,
            },
            clear=False,
        ):
            with patch(
                "utils.slack_notifier.requests.post",
                side_effect=RuntimeError(f"failed to post to {webhook}"),
            ):
                result = send_slack_message(text="pipeline heartbeat")

        self.assertEqual(result.status, "failed")
        self.assertNotIn("hooks.slack.com", result.error or "")
        self.assertNotIn("SECRET", result.error or "")


if __name__ == "__main__":
    unittest.main()
