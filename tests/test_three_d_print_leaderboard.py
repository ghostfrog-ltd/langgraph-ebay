import unittest
from io import StringIO
from unittest.mock import patch

from pipelines.listing import three_d_print_leaderboard


class ThreeDPrintLeaderboardTests(unittest.TestCase):
    def test_disabled_main_does_not_build_payload(self):
        with patch.object(three_d_print_leaderboard, "LEADERBOARD_ENABLED", False):
            with patch.object(three_d_print_leaderboard, "build_payload") as build_payload:
                with patch("sys.argv", ["three_d_print_leaderboard", "--send"]):
                    with patch("sys.stdout", new_callable=StringIO) as output:
                        code = three_d_print_leaderboard.main()

        self.assertEqual(code, 0)
        self.assertIn("disabled", output.getvalue())
        build_payload.assert_not_called()


if __name__ == "__main__":
    unittest.main()
