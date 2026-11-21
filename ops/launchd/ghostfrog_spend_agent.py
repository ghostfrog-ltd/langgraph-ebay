"""Render the local cost snapshot refresher. This does not install or start it."""

from pathlib import Path
import plistlib
import sys


def main() -> None:
    root = Path(__file__).resolve().parents[2]
    logs = Path.home() / "Library/Logs/GhostFrog/spend"
    plistlib.dump(
        {
            "Label": "com.ghostfrog.langgraph-ebay.spend-snapshot",
            "ProgramArguments": [sys.executable, "-m", "ops.ghostfrog_spend", "refresh"],
            "WorkingDirectory": str(root),
            "LimitLoadToSessionType": "Aqua",
            "StartInterval": 900,
            "RunAtLoad": True,
            "ProcessType": "Background",
            "Umask": "077",
            "StandardOutPath": str(logs / "stdout.log"),
            "StandardErrorPath": str(logs / "stderr.log"),
        },
        sys.stdout.buffer,
    )


if __name__ == "__main__":
    main()
