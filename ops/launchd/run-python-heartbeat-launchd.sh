#!/bin/zsh

set -euo pipefail

PROJECT_ROOT="/Volumes/Bob/www/ghostfrog-langgraph-ebay"
PYTHON_BIN="$PROJECT_ROOT/.venv-mac/bin/python"
LOCAL_LOG_DIR="$HOME/Library/Logs/ghostfrog-langgraph-ebay"
HEARTBEAT_LOG_FILE="$LOCAL_LOG_DIR/python-heartbeat.log"

mkdir -p "$LOCAL_LOG_DIR"

if [[ ! -d "$PROJECT_ROOT" ]]; then
    echo "$(date -u '+%Y-%m-%dT%H:%M:%SZ') Project directory unavailable; external volume may not be mounted: $PROJECT_ROOT" >> "$LOCAL_LOG_DIR/python-heartbeat.err.log"
    exit 0
fi

if [[ ! -x "$PYTHON_BIN" ]]; then
    echo "$(date -u '+%Y-%m-%dT%H:%M:%SZ') Python executable unavailable: $PYTHON_BIN" >> "$LOCAL_LOG_DIR/python-heartbeat.err.log"
    exit 1
fi

export GF_LOG_DIR="$LOCAL_LOG_DIR"
export GF_HEARTBEAT_LAUNCHED_BY="launchd"
export GF_HEARTBEAT_LAUNCHD_LABEL="com.ghostfrog.langgraph-ebay.python-heartbeat"

cd "$PROJECT_ROOT"

"$PYTHON_BIN" -m pipelines.listing.heartbeat >> "$HEARTBEAT_LOG_FILE" 2>&1
