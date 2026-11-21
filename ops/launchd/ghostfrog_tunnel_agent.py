"""Render or run the user LaunchAgent for the existing GhostFrog tunnel."""

import argparse
import os
from pathlib import Path
import plistlib
import stat
import subprocess
import sys


LABEL = "com.ghostfrog.langgraph-ebay.mcp-tunnel"
FOREGROUND_PATTERN = (
    r"(^|/)tunnel-client[[:space:]]+run([[:space:]]+[^[:space:]]+)*"
    r"[[:space:]]+--profile([[:space:]]+|=)ghostfrog([[:space:]]|$)"
)


def log_dir(home: Path) -> Path:
    return home / "Library/Logs/GhostFrog/mcp-tunnel"


def agent_definition(home: Path) -> dict:
    logs = log_dir(home)
    return {
        "Label": LABEL,
        "ProgramArguments": [sys.executable, str(Path(__file__).resolve()), "run"],
        "LimitLoadToSessionType": "Aqua",
        "RunAtLoad": True,
        "KeepAlive": True,
        "ThrottleInterval": 60,
        "ProcessType": "Background",
        "Umask": "077",
        "StandardOutPath": str(logs / "stdout.log"),
        "StandardErrorPath": str(logs / "stderr.log"),
    }


def run_tunnel(home: Path) -> None:
    logs = log_dir(home)
    profiles = home / ".config/tunnel-client"
    credentials = home / ".config/ghostfrog-mcp"
    client = home / ".local/bin/tunnel-client"

    for path in (profiles / "ghostfrog.yaml", credentials / "platform-runtime-key", home / ".pgpass", logs):
        try:
            metadata = path.lstat()
        except FileNotFoundError:
            raise SystemExit(f"Missing local GhostFrog setup path: {path}") from None
        expected_type = stat.S_ISDIR if path == logs else stat.S_ISREG
        if not expected_type(metadata.st_mode) or metadata.st_uid != os.getuid() or metadata.st_mode & 0o077:
            raise SystemExit(f"GhostFrog requires a private, user-owned path: {path}")

    existing = subprocess.run(
        ["/usr/bin/pgrep", "-u", str(os.getuid()), "-f", FOREGROUND_PATTERN],
        stdout=subprocess.DEVNULL, check=False,
    )
    if existing.returncode == 0:
        raise SystemExit("A GhostFrog tunnel is already running. Stop it before starting this LaunchAgent.")
    if existing.returncode != 1:
        raise SystemExit("Could not check for an existing GhostFrog tunnel.")

    arguments = [
        str(client), "run", "--profile", "ghostfrog", "--profile-dir", str(profiles),
        "--health.listen-addr", "127.0.0.1:0", "--health.url-file", str(logs / "health.url"),
        "--allow-remote-ui=false", "--open-web-ui=false",
        "--log.level=warn", "--log.file=stdout", "--log.http-raw-unsafe=false",
        "--tracing.enabled=false", "--harpoon.capture-payloads=false",
    ]
    # The existing profile resolves its key from a local file.
    os.execve(client, arguments, {"HOME": str(home), "PATH": "/usr/bin:/bin:/usr/sbin:/sbin"})


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("render", "run"))
    args = parser.parse_args()
    home = Path.home()
    if args.action == "render":
        plistlib.dump(agent_definition(home), sys.stdout.buffer)
    else:
        run_tunnel(home)


if __name__ == "__main__":
    main()
