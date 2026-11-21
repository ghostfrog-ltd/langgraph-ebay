#!/bin/zsh

set -euo pipefail

ROOT="${GHOSTFROG_PROJECT_ROOT:-$(cd "$(dirname "$0")/.." && pwd)}"
LABEL="com.ghostfrog.langgraph-ebay.text-condition"
PLIST="$HOME/Library/LaunchAgents/$LABEL.plist"
RUNNER_DIR="$HOME/Library/Application Support/GhostFrog"
LOG_DIR="${GHOSTFROG_TEXT_CONDITION_LOG_DIR:-$HOME/Library/Logs/GhostFrog}"
RUNNER_PATH="$RUNNER_DIR/text-condition-worker.zsh"
PYTHON_BIN="$ROOT/.venv-mac/bin/python"

if [[ ! -x "$PYTHON_BIN" ]]; then
    PYTHON_BIN="$ROOT/.venv/bin/python"
fi

if [[ ! -x "$PYTHON_BIN" ]]; then
    PYTHON_BIN="$(command -v python3)"
fi

mkdir -p "$HOME/Library/LaunchAgents" "$RUNNER_DIR" "$LOG_DIR"
cp "$ROOT/scripts/run_text_condition_once.sh" "$RUNNER_PATH"
chmod +x "$RUNNER_PATH"

cat > "$PLIST" <<PLIST
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>$LABEL</string>
    <key>ProgramArguments</key>
    <array>
        <string>$RUNNER_PATH</string>
    </array>
    <key>EnvironmentVariables</key>
    <dict>
        <key>GHOSTFROG_PROJECT_ROOT</key>
        <string>$ROOT</string>
        <key>GHOSTFROG_TEXT_CONDITION_ENABLED</key>
        <string>${GHOSTFROG_TEXT_CONDITION_ENABLED:-1}</string>
        <key>GHOSTFROG_TEXT_CONDITION_MODEL</key>
        <string>${GHOSTFROG_TEXT_CONDITION_MODEL:-gemma3:12b}</string>
        <key>GHOSTFROG_TEXT_CONDITION_BATCH_SIZE</key>
        <string>${GHOSTFROG_TEXT_CONDITION_BATCH_SIZE:-4}</string>
        <key>GHOSTFROG_TEXT_CONDITION_ENQUEUE_LIMIT</key>
        <string>${GHOSTFROG_TEXT_CONDITION_ENQUEUE_LIMIT:-1000}</string>
        <key>GHOSTFROG_TEXT_CONDITION_STATUS</key>
        <string>${GHOSTFROG_TEXT_CONDITION_STATUS:-all}</string>
        <key>GHOSTFROG_TEXT_CONDITION_POLL_SECONDS</key>
        <string>${GHOSTFROG_TEXT_CONDITION_POLL_SECONDS:-10}</string>
        <key>GHOSTFROG_TEXT_CONDITION_MAX_ATTEMPTS</key>
        <string>${GHOSTFROG_TEXT_CONDITION_MAX_ATTEMPTS:-3}</string>
        <key>GHOSTFROG_TEXT_CONDITION_NEW_LISTING_MINUTES</key>
        <string>${GHOSTFROG_TEXT_CONDITION_NEW_LISTING_MINUTES:-360}</string>
    </dict>
    <key>RunAtLoad</key>
    <true/>
    <key>KeepAlive</key>
    <true/>
    <key>StandardOutPath</key>
    <string>$LOG_DIR/text-condition.launchd.log</string>
    <key>StandardErrorPath</key>
    <string>$LOG_DIR/text-condition.launchd.err.log</string>
</dict>
</plist>
PLIST

plutil -lint "$PLIST"

launchctl bootout "gui/$(id -u)" "$PLIST" 2>/dev/null || true
launchctl bootstrap "gui/$(id -u)" "$PLIST"
launchctl enable "gui/$(id -u)/$LABEL"

echo "Installed $LABEL"
