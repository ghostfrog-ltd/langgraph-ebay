#!/bin/zsh

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
PYTHON_BIN="$ROOT/.venv-mac/bin/python"

if [[ ! -x "$PYTHON_BIN" ]]; then
    PYTHON_BIN="$ROOT/.venv/bin/python"
fi

if [[ ! -x "$PYTHON_BIN" ]]; then
    PYTHON_BIN="$(command -v python3)"
fi

export PATH="/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin"
export GF_LOG_DIR="${GF_LOG_DIR:-$ROOT/logs}"

cd "$ROOT"

mkdir -p "$GF_LOG_DIR"
lock_dir="${GHOSTFROG_WEB_SYNC_LOCK_DIR:-$ROOT/logs/web-sync.lock}"
if ! mkdir "$lock_dir" 2>/dev/null; then
    lock_pid_file="$lock_dir/pid"
    lock_pid=""
    if [[ -f "$lock_pid_file" ]]; then
        lock_pid="$(<"$lock_pid_file")"
    fi
    if [[ "$lock_pid" == <-> ]] && kill -0 "$lock_pid" 2>/dev/null; then
        echo "Web sync is already running; skipping this tick."
        exit 0
    fi
    echo "Removing stale web sync lock: $lock_dir" >&2
    rm -f "$lock_pid_file" 2>/dev/null || true
    rmdir "$lock_dir" 2>/dev/null || true
    if ! mkdir "$lock_dir" 2>/dev/null; then
        echo "Web sync lock is still held; skipping this tick."
        exit 0
    fi
fi
print -r -- "$$" > "$lock_dir/pid"
trap 'rm -f "$lock_dir/pid" 2>/dev/null || true; rmdir "$lock_dir" 2>/dev/null || true' EXIT

inner_lock_dir="${GHOSTFROG_WEB_SYNC_INNER_LOCK_DIR:-$GF_LOG_DIR/web-sync-all.lock}"
if ! GHOSTFROG_WEB_SYNC_LOCK_DIR="$inner_lock_dir" "$ROOT/ebay" web-sync-all --chunk-size "${GHOSTFROG_WEB_SYNC_CHUNK_SIZE:-50}"; then
    echo "Live web sync failed." >&2
    exit 1
fi

sold_evidence_marker="$GF_LOG_DIR/sold_evidence.last"
sold_evidence_enabled="${GHOSTFROG_SOLD_EVIDENCE_SYNC_ENABLED:-0}"
sold_evidence_interval_minutes="${GHOSTFROG_SOLD_EVIDENCE_INTERVAL_MINUTES:-60}"
sold_evidence_interval_seconds=$(( sold_evidence_interval_minutes * 60 ))
sold_evidence_last=0

if [[ "$sold_evidence_enabled" != "1" ]]; then
    echo "Sold evidence sync disabled for scheduled web sync."
elif [[ -f "$sold_evidence_marker" ]]; then
    sold_evidence_last="$(stat -f %m "$sold_evidence_marker" 2>/dev/null || stat -c %Y "$sold_evidence_marker" 2>/dev/null || echo 0)"
fi

sync_now="$(date +%s)"
if [[ "$sold_evidence_enabled" == "1" ]] && (( sync_now - sold_evidence_last >= sold_evidence_interval_seconds )); then
    if "$ROOT/ebay" evidence-sync --chunk-size "${GHOSTFROG_SOLD_EVIDENCE_CHUNK_SIZE:-1000}"; then
        mkdir -p "$GF_LOG_DIR"
        touch "$sold_evidence_marker"
    else
        echo "Sold evidence sync failed; continuing normal web sync." >&2
        mkdir -p "$GF_LOG_DIR"
        touch "$sold_evidence_marker"
    fi
fi

leaderboard_enabled="${GHOSTFROG_3D_PRINT_LEADERBOARD_ENABLED:-0}"

if [[ "$leaderboard_enabled" == "1" ]]; then
    leaderboard_marker="$GF_LOG_DIR/3d_print_leaderboard.last"
    leaderboard_interval_hours="${GHOSTFROG_3D_PRINT_LEADERBOARD_INTERVAL_HOURS:-24}"
    leaderboard_interval_seconds=$(( leaderboard_interval_hours * 3600 ))
    leaderboard_last=0

    if [[ -f "$leaderboard_marker" ]]; then
        leaderboard_last="$(stat -f %m "$leaderboard_marker" 2>/dev/null || stat -c %Y "$leaderboard_marker" 2>/dev/null || echo 0)"
    fi

    leaderboard_now="$sync_now"
    if (( leaderboard_now - leaderboard_last >= leaderboard_interval_seconds )); then
        if "$ROOT/ebay" 3d-print-leaderboard-sync \
            --top-limit "${GHOSTFROG_3D_PRINT_LEADERBOARD_TOP_LIMIT:-200}"; then
            mkdir -p "$GF_LOG_DIR"
            touch "$leaderboard_marker"
        else
            echo "3D print seller leaderboard sync failed; continuing normal web sync." >&2
            mkdir -p "$GF_LOG_DIR"
            touch "$leaderboard_marker"
        fi
    fi
fi

echo "Web sync tick completed."
