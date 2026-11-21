#!/bin/zsh

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
LABEL="com.ghostfrog.langgraph-ebay.visual-condition"
PLIST_DIR="${GHOSTFROG_LAUNCH_AGENTS_DIR:-$HOME/Library/LaunchAgents}"
PLIST_PATH="$PLIST_DIR/$LABEL.plist"
LOG_DIR="${GHOSTFROG_VISUAL_CONDITION_LOG_DIR:-$HOME/Library/Logs/GhostFrog}"
RUNNER_DIR="$HOME/Library/Application Support/GhostFrog"
RUNNER_PATH="$RUNNER_DIR/visual-condition-worker.zsh"
PYTHON_BIN="$ROOT/.venv-mac/bin/python"

if [[ ! -x "$PYTHON_BIN" ]]; then
    PYTHON_BIN="$ROOT/.venv/bin/python"
fi

if [[ ! -x "$PYTHON_BIN" ]]; then
    PYTHON_BIN="$(command -v python3)"
fi

mkdir -p "$PLIST_DIR" "$LOG_DIR" "$RUNNER_DIR"

cp "$ROOT/scripts/run_visual_condition_once.sh" "$RUNNER_PATH"
chmod +x "$RUNNER_PATH"

cat > "$PLIST_PATH" <<PLIST
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>$LABEL</string>
    <key>ProgramArguments</key>
    <array>
        <string>/bin/zsh</string>
        <string>$RUNNER_PATH</string>
    </array>
    <key>EnvironmentVariables</key>
    <dict>
        <key>GHOSTFROG_PROJECT_ROOT</key>
        <string>$ROOT</string>
        <key>GF_LOG_DIR</key>
        <string>$LOG_DIR</string>
        <key>GHOSTFROG_VISUAL_CONDITION_ENABLED</key>
        <string>1</string>
        <key>GHOSTFROG_VISUAL_CONDITION_MODEL</key>
        <string>minicpm-v:8b</string>
        <key>GHOSTFROG_VISUAL_CONDITION_BATCH_SIZE</key>
        <string>4</string>
        <key>GHOSTFROG_VISUAL_CONDITION_ENQUEUE_LIMIT</key>
        <string>250</string>
        <key>GHOSTFROG_VISUAL_CONDITION_MAX_IMAGES</key>
        <string>3</string>
        <key>GHOSTFROG_VISUAL_CONDITION_POLL_SECONDS</key>
        <string>5</string>
        <key>GHOSTFROG_VISUAL_CONDITION_MAX_ATTEMPTS</key>
        <string>3</string>
        <key>GHOSTFROG_VISUAL_CONDITION_NEW_LISTING_MINUTES</key>
        <string>360</string>
        <key>GHOSTFROG_VISUAL_CONDITION_TARGETS</key>
        <string>ebay-apple:iphone ebay-samsung:galaxy</string>
    </dict>
    <key>RunAtLoad</key>
    <true/>
    <key>KeepAlive</key>
    <true/>
    <key>StandardOutPath</key>
    <string>$LOG_DIR/visual-condition.launchd.log</string>
    <key>StandardErrorPath</key>
    <string>$LOG_DIR/visual-condition.launchd.err.log</string>
</dict>
</plist>
PLIST

plutil -lint "$PLIST_PATH"
launchctl bootout "gui/$(id -u)/$LABEL" >/dev/null 2>&1 || true
"$PYTHON_BIN" -m pipelines.listing.visual_condition --release-processing-locks --sync-listing-flags --max-attempts 3 || {
    echo "Could not release visual condition queue locks before restart; continuing install." >&2
}
launchctl bootstrap "gui/$(id -u)" "$PLIST_PATH"
launchctl kickstart -k "gui/$(id -u)/$LABEL"

echo "Installed $LABEL. It runs continuously and polls the visual grading queue."
