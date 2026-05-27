#!/bin/zsh

set -euo pipefail

PROJECT_DIR="/Volumes/Bob/www/ghostfrog-langgraph-ebay/laravel"
PROJECT_ROOT="/Volumes/Bob/www/ghostfrog-langgraph-ebay"
LOCAL_LOG_DIR="$HOME/Library/Logs/ghostfrog-langgraph-ebay"
SCHEDULER_LOG_FILE="$LOCAL_LOG_DIR/scheduler.log"

mkdir -p "$LOCAL_LOG_DIR"

if [[ ! -d "$PROJECT_DIR" ]]; then
    echo "$(date -u '+%Y-%m-%dT%H:%M:%SZ') Project directory unavailable; external volume may not be mounted: $PROJECT_ROOT" >> "$LOCAL_LOG_DIR/launchd.err.log"
    exit 0
fi

export PATH="$HOME/Library/Application Support/Herd/bin:/opt/homebrew/bin:/opt/homebrew/sbin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin:$PATH"

PHP_BIN=""
for candidate in \
    "$HOME/Library/Application Support/Herd/bin/php" \
    "/opt/homebrew/bin/php" \
    "/usr/local/bin/php" \
    "/usr/bin/php"
do
    if [[ -x "$candidate" ]]; then
        PHP_BIN="$candidate"
        break
    fi
done

if [[ -z "$PHP_BIN" ]]; then
    echo "$(date -u '+%Y-%m-%dT%H:%M:%SZ') PHP executable not found." >> "$LOCAL_LOG_DIR/launchd.err.log"
    exit 1
fi

cd "$PROJECT_DIR"

"$PHP_BIN" artisan schedule:run --no-interaction >> "$SCHEDULER_LOG_FILE" 2>&1
