#!/bin/zsh

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
LABEL="com.ghostfrog.langgraph-ebay.web-sync"
PLIST_DIR="${GHOSTFROG_LAUNCH_AGENTS_DIR:-$HOME/Library/LaunchAgents}"
PLIST_PATH="$PLIST_DIR/$LABEL.plist"
LOG_DIR="${GHOSTFROG_WEB_SYNC_LOG_DIR:-$HOME/Library/Logs/GhostFrog}"
RUNNER_DIR="$HOME/Library/Application Support/GhostFrog"
RUNNER_PATH="$RUNNER_DIR/web-sync-once.zsh"
INTERVAL_SECONDS="${GHOSTFROG_WEB_SYNC_INTERVAL_SECONDS:-300}"

mkdir -p "$PLIST_DIR" "$LOG_DIR" "$RUNNER_DIR"

cat > "$RUNNER_PATH" <<RUNNER
#!/bin/zsh
set -euo pipefail

ROOT="$ROOT"
PYTHON_BIN="\$ROOT/.venv-mac/bin/python"

if [[ ! -x "\$PYTHON_BIN" ]]; then
    PYTHON_BIN="\$ROOT/.venv/bin/python"
fi

if [[ ! -x "\$PYTHON_BIN" ]]; then
    PYTHON_BIN="\$(command -v python3)"
fi

export PATH="/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin"
export GF_LOG_DIR="\${GF_LOG_DIR:-$LOG_DIR}"
export GHOSTFROG_WEB_SYNC_CHUNK_SIZE="\${GHOSTFROG_WEB_SYNC_CHUNK_SIZE:-50}"
export GHOSTFROG_WEB_SYNC_LOCK_DIR="\${GHOSTFROG_WEB_SYNC_LOCK_DIR:-\$GF_LOG_DIR/web-sync.lock}"
export GHOSTFROG_REMOTE_SYNC_STATE_DIR="\${GHOSTFROG_REMOTE_SYNC_STATE_DIR:-\$GF_LOG_DIR/remote-sync-state}"
export GHOSTFROG_SOLD_EVIDENCE_SYNC_ENABLED="\${GHOSTFROG_SOLD_EVIDENCE_SYNC_ENABLED:-0}"
export GHOSTFROG_SOLD_EVIDENCE_CHUNK_SIZE="\${GHOSTFROG_SOLD_EVIDENCE_CHUNK_SIZE:-1000}"
export GHOSTFROG_SOLD_EVIDENCE_INTERVAL_MINUTES="\${GHOSTFROG_SOLD_EVIDENCE_INTERVAL_MINUTES:-60}"
export GHOSTFROG_3D_PRINT_LEADERBOARD_ENABLED="\${GHOSTFROG_3D_PRINT_LEADERBOARD_ENABLED:-0}"
export GHOSTFROG_3D_PRINT_LEADERBOARD_TOP_LIMIT="\${GHOSTFROG_3D_PRINT_LEADERBOARD_TOP_LIMIT:-200}"
export GHOSTFROG_3D_PRINT_LEADERBOARD_INTERVAL_HOURS="\${GHOSTFROG_3D_PRINT_LEADERBOARD_INTERVAL_HOURS:-24}"

local_endpoint="\${GHOSTFROG_PUSH_LOCAL_URL:-http://127.0.0.1:8080}"
local_secret="\${GHOSTFROG_PUSH_LOCAL_SYNC_SECRET:-local-development-sync-secret-change-me}"
prod_endpoint="\${GHOSTFROG_PUSH_PROD_URL:-https://ebay.ghostfrog.co.uk}"
prod_secret=""
prod_ssh_key="\${GHOSTFROG_PROD_SSH_KEY:-\$HOME/.ssh/id_ghostfrog}"
prod_ssh_host="\${GHOSTFROG_PROD_SSH_HOST:-root@91.99.113.93}"
prod_app_dir="\${GHOSTFROG_PROD_APP_DIR:-/opt/ghostfrog-evidence}"

if [[ -n "\${GHOSTFROG_PUSH_PROD_SYNC_SECRET:-}" ]]; then
    prod_secret="\$GHOSTFROG_PUSH_PROD_SYNC_SECRET"
elif [[ -n "\${GHOSTFROG_PROD_SYNC_SECRET:-}" ]]; then
    prod_secret="\$GHOSTFROG_PROD_SYNC_SECRET"
elif [[ -n "\${GHOSTFROG_WEB_URL:-}" && "\${GHOSTFROG_WEB_URL:-}" != "\$local_endpoint" && -n "\${GHOSTFROG_SYNC_SECRET:-}" ]]; then
    prod_endpoint="\$GHOSTFROG_WEB_URL"
    prod_secret="\$GHOSTFROG_SYNC_SECRET"
fi

if [[ -z "\$prod_secret" && "\${GHOSTFROG_PUSH_PROD_SECRET_FROM_SSH:-1}" != "0" && -r "\$prod_ssh_key" ]]; then
    prod_secret="\$(ssh -i "\$prod_ssh_key" "\$prod_ssh_host" "cd '\$prod_app_dir' && if [ -f .env ]; then sed -n 's/^GHOSTFROG_SYNC_SECRET=//p' .env | head -n 1; fi" 2>/dev/null || true)"
    if [[ -z "\$prod_secret" ]]; then
        prod_secret="\$(ssh -i "\$prod_ssh_key" "\$prod_ssh_host" "cd '\$prod_app_dir' && docker compose exec -T app php -r 'echo getenv(\"GHOSTFROG_SYNC_SECRET\") ?: \"\";'" 2>/dev/null || true)"
    fi
    prod_secret="\${prod_secret%\\"}"
    prod_secret="\${prod_secret#\\"}"
    prod_secret="\${prod_secret%\\'}"
    prod_secret="\${prod_secret#\\'}"
fi

cd "$ROOT"

mkdir -p "\$GF_LOG_DIR"
lock_dir="\$GHOSTFROG_WEB_SYNC_LOCK_DIR"
if ! mkdir "\$lock_dir" 2>/dev/null; then
    lock_pid_file="\$lock_dir/pid"
    lock_pid=""
    if [[ -f "\$lock_pid_file" ]]; then
        lock_pid="\$(<"\$lock_pid_file")"
    fi
    if [[ "\$lock_pid" == <-> ]] && kill -0 "\$lock_pid" 2>/dev/null; then
        echo "Web sync is already running; skipping this tick."
        exit 0
    fi
    echo "Removing stale web sync lock: \$lock_dir" >&2
    rm -f "\$lock_pid_file" 2>/dev/null || true
    rmdir "\$lock_dir" 2>/dev/null || true
    if ! mkdir "\$lock_dir" 2>/dev/null; then
        echo "Web sync lock is still held; skipping this tick."
        exit 0
    fi
fi
print -r -- "\$\$" > "\$lock_dir/pid"
trap 'rm -f "\$lock_dir/pid" 2>/dev/null || true; rmdir "\$lock_dir" 2>/dev/null || true' EXIT

echo "Pushing live web delta to local Laravel (\$local_endpoint)..."
GHOSTFROG_WEB_URL="\$local_endpoint" GHOSTFROG_SYNC_SECRET="\$local_secret" "\$PYTHON_BIN" -m pipelines.listing.remote_sync --send --chunk-size "\$GHOSTFROG_WEB_SYNC_CHUNK_SIZE"

if [[ -n "\$prod_secret" ]]; then
    echo "Pushing live web delta to prod Laravel (\$prod_endpoint)..."
    GHOSTFROG_WEB_URL="\$prod_endpoint" GHOSTFROG_SYNC_SECRET="\$prod_secret" "\$PYTHON_BIN" -m pipelines.listing.remote_sync --send --chunk-size "\$GHOSTFROG_WEB_SYNC_CHUNK_SIZE"
    echo "Live web delta completed for local and prod."
else
    echo "Local web delta completed, but prod push was skipped: no prod sync secret." >&2
fi

sold_evidence_marker="\$GF_LOG_DIR/sold_evidence.last"
sold_evidence_interval_seconds=\$(( GHOSTFROG_SOLD_EVIDENCE_INTERVAL_MINUTES * 60 ))
sold_evidence_last=0

if [[ "\$GHOSTFROG_SOLD_EVIDENCE_SYNC_ENABLED" != "1" ]]; then
    echo "Sold evidence sync disabled for scheduled web sync."
elif [[ -f "\$sold_evidence_marker" ]]; then
    sold_evidence_last="\$(stat -f %m "\$sold_evidence_marker" 2>/dev/null || stat -c %Y "\$sold_evidence_marker" 2>/dev/null || echo 0)"
fi

sync_now="\$(date +%s)"
if [[ "\$GHOSTFROG_SOLD_EVIDENCE_SYNC_ENABLED" == "1" ]] && (( sync_now - sold_evidence_last >= sold_evidence_interval_seconds )); then
    echo "Pushing all sold evidence to local Laravel (\$local_endpoint)..."
    if GHOSTFROG_WEB_URL="\$local_endpoint" GHOSTFROG_SYNC_SECRET="\$local_secret" "\$PYTHON_BIN" -m pipelines.listing.remote_sync --sold-evidence-only --send --chunk-size "\$GHOSTFROG_SOLD_EVIDENCE_CHUNK_SIZE"; then
        if [[ -n "\$prod_secret" ]]; then
            echo "Pushing all sold evidence to prod Laravel (\$prod_endpoint)..."
            if ! GHOSTFROG_WEB_URL="\$prod_endpoint" GHOSTFROG_SYNC_SECRET="\$prod_secret" "\$PYTHON_BIN" -m pipelines.listing.remote_sync --sold-evidence-only --send --chunk-size "\$GHOSTFROG_SOLD_EVIDENCE_CHUNK_SIZE"; then
                echo "Prod sold evidence sync failed; continuing live web sync." >&2
            fi
        else
            echo "Local evidence sync completed, but prod push was skipped: no prod sync secret." >&2
        fi
        mkdir -p "\$GF_LOG_DIR"
        touch "\$sold_evidence_marker"
    else
        echo "Sold evidence sync failed; continuing normal web sync." >&2
    fi
fi

if [[ "\$GHOSTFROG_3D_PRINT_LEADERBOARD_ENABLED" == "1" ]]; then
    leaderboard_marker="\$GF_LOG_DIR/3d_print_leaderboard.last"
    leaderboard_interval_seconds=\$(( GHOSTFROG_3D_PRINT_LEADERBOARD_INTERVAL_HOURS * 3600 ))
    leaderboard_last=0

    if [[ -f "\$leaderboard_marker" ]]; then
        leaderboard_last="\$(stat -f %m "\$leaderboard_marker" 2>/dev/null || stat -c %Y "\$leaderboard_marker" 2>/dev/null || echo 0)"
    fi

    if (( sync_now - leaderboard_last >= leaderboard_interval_seconds )); then
        echo "Pushing 3D print seller leaderboard to local Laravel (\$local_endpoint)..."
        if GHOSTFROG_WEB_URL="\$local_endpoint" GHOSTFROG_SYNC_SECRET="\$local_secret" "\$PYTHON_BIN" -m pipelines.listing.three_d_print_leaderboard --send --endpoint "\$local_endpoint" --secret "\$local_secret" --top-limit "\$GHOSTFROG_3D_PRINT_LEADERBOARD_TOP_LIMIT"; then
            if [[ -n "\$prod_secret" ]]; then
                echo "Pushing 3D print seller leaderboard to prod Laravel (\$prod_endpoint)..."
                if ! "\$PYTHON_BIN" -m pipelines.listing.three_d_print_leaderboard --send --endpoint "\$prod_endpoint" --secret "\$prod_secret" --top-limit "\$GHOSTFROG_3D_PRINT_LEADERBOARD_TOP_LIMIT"; then
                    echo "Prod 3D print seller leaderboard sync failed; continuing live web sync." >&2
                fi
            else
                echo "Local 3D print seller leaderboard sync completed, but prod push was skipped: no prod sync secret." >&2
            fi
            mkdir -p "\$GF_LOG_DIR"
            touch "\$leaderboard_marker"
        else
            echo "3D print seller leaderboard sync failed; continuing normal web sync." >&2
        fi
    fi
fi

echo "Web sync tick completed."
RUNNER
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
    <key>RunAtLoad</key>
    <true/>
    <key>StartInterval</key>
    <integer>$INTERVAL_SECONDS</integer>
    <key>StandardOutPath</key>
    <string>$LOG_DIR/web-sync.launchd.log</string>
    <key>StandardErrorPath</key>
    <string>$LOG_DIR/web-sync.launchd.err.log</string>
</dict>
</plist>
PLIST

plutil -lint "$PLIST_PATH"
launchctl bootout "gui/$(id -u)/$LABEL" >/dev/null 2>&1 || true
launchctl bootstrap "gui/$(id -u)" "$PLIST_PATH"
launchctl kickstart -k "gui/$(id -u)/$LABEL"

echo "Installed $LABEL. It runs at load and every $INTERVAL_SECONDS seconds."
