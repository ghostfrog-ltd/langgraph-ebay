#!/bin/zsh

set -euo pipefail

PROJECT_DIR="/Volumes/Bob/www/ghostfrog-langgraph-ebay/laravel"
LOG_DIR="$PROJECT_DIR/storage/logs"
LOG_FILE="$LOG_DIR/scheduler.log"

if [[ ! -d "$PROJECT_DIR" ]]; then
    echo "$(date -u '+%Y-%m-%dT%H:%M:%SZ') Project directory is not mounted: $PROJECT_DIR" >&2
    exit 0
fi

mkdir -p "$LOG_DIR"

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
    echo "$(date -u '+%Y-%m-%dT%H:%M:%SZ') PHP executable not found." >> "$LOG_FILE"
    exit 1
fi

cd "$PROJECT_DIR"

"$PHP_BIN" artisan schedule:run --no-interaction >> "$LOG_FILE" 2>&1
