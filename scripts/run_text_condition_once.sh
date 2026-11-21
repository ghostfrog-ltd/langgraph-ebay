#!/bin/zsh

set -euo pipefail

ROOT="${GHOSTFROG_PROJECT_ROOT:-$(cd "$(dirname "$0")/.." && pwd)}"
PYTHON_BIN="$ROOT/.venv-mac/bin/python"

if [[ ! -x "$PYTHON_BIN" ]]; then
    PYTHON_BIN="$ROOT/.venv/bin/python"
fi

if [[ ! -x "$PYTHON_BIN" ]]; then
    PYTHON_BIN="$(command -v python3)"
fi

export PATH="/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin"
export GF_LOG_DIR="${GF_LOG_DIR:-$ROOT/logs}"

mkdir -p "$GF_LOG_DIR"

text_enabled="${GHOSTFROG_TEXT_CONDITION_ENABLED:-1}"
if [[ "$text_enabled" == "0" ]]; then
    echo "Text condition worker disabled."
    exit 0
fi

text_model="${GHOSTFROG_TEXT_CONDITION_MODEL:-gemma3:12b}"
text_batch_size="${GHOSTFROG_TEXT_CONDITION_BATCH_SIZE:-4}"
text_enqueue_limit="${GHOSTFROG_TEXT_CONDITION_ENQUEUE_LIMIT:-1000}"
text_source="${GHOSTFROG_TEXT_CONDITION_SOURCE:-}"
text_product="${GHOSTFROG_TEXT_CONDITION_PRODUCT:-}"
text_status="${GHOSTFROG_TEXT_CONDITION_STATUS:-all}"
text_poll_seconds="${GHOSTFROG_TEXT_CONDITION_POLL_SECONDS:-10}"
text_max_attempts="${GHOSTFROG_TEXT_CONDITION_MAX_ATTEMPTS:-3}"
text_new_listing_minutes="${GHOSTFROG_TEXT_CONDITION_NEW_LISTING_MINUTES:-360}"

cd "$ROOT"

cmd=(
    "$PYTHON_BIN" -m pipelines.listing.text_condition
    --queue-worker
    --model "$text_model"
    --queue-batch-size "$text_batch_size"
    --enqueue-limit "$text_enqueue_limit"
    --status "$text_status"
    --poll-seconds "$text_poll_seconds"
    --max-attempts "$text_max_attempts"
    --new-listing-minutes "$text_new_listing_minutes"
)

if [[ -n "$text_source" ]]; then
    cmd+=(--source "$text_source")
fi

if [[ -n "$text_product" ]]; then
    cmd+=(--product "$text_product")
fi

exec "${cmd[@]}"
