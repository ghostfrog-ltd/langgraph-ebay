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

visual_enabled="${GHOSTFROG_VISUAL_CONDITION_ENABLED:-1}"
if [[ "$visual_enabled" == "0" ]]; then
    echo "Visual condition worker disabled."
    exit 0
fi

visual_model="${GHOSTFROG_VISUAL_CONDITION_MODEL:-minicpm-v:8b}"
visual_batch_size="${GHOSTFROG_VISUAL_CONDITION_BATCH_SIZE:-4}"
visual_enqueue_limit="${GHOSTFROG_VISUAL_CONDITION_ENQUEUE_LIMIT:-250}"
visual_max_images="${GHOSTFROG_VISUAL_CONDITION_MAX_IMAGES:-4}"
visual_targets="${GHOSTFROG_VISUAL_CONDITION_TARGETS:-all:all}"
visual_poll_seconds="${GHOSTFROG_VISUAL_CONDITION_POLL_SECONDS:-10}"
visual_max_attempts="${GHOSTFROG_VISUAL_CONDITION_MAX_ATTEMPTS:-3}"
visual_new_listing_minutes="${GHOSTFROG_VISUAL_CONDITION_NEW_LISTING_MINUTES:-360}"

cd "$ROOT"

cmd=(
    "$PYTHON_BIN" -m pipelines.listing.visual_condition
    --queue-worker
    --model "$visual_model"
    --queue-batch-size "$visual_batch_size"
    --enqueue-limit "$visual_enqueue_limit"
    --max-images "$visual_max_images"
    --poll-seconds "$visual_poll_seconds"
    --max-attempts "$visual_max_attempts"
    --new-listing-minutes "$visual_new_listing_minutes"
)

for target in ${(z)visual_targets}; do
    if [[ -z "$target" || "$target" != *:* ]]; then
        echo "Skipping invalid visual grading target: $target" >&2
        continue
    fi
    cmd+=(--target "$target")
done

exec "${cmd[@]}"
