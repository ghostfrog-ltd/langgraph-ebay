#!/usr/bin/env bash
set -euo pipefail

PROJECT_DIR="/Volumes/Bob/www/ghostfrog-langgraph-ebay"
ENV_FILE="${PROJECT_DIR}/.env"
BACKUP_DIR="${GHOSTFROG_BACKUP_DIR:-${PROJECT_DIR}/backups/postgres}"
RETENTION_DAYS="${GHOSTFROG_BACKUP_RETENTION_DAYS:-180}"

if [[ -f "${ENV_FILE}" ]]; then
    while IFS= read -r line || [[ -n "${line}" ]]; do
        if [[ "${line}" =~ ^[[:space:]]*([A-Za-z_][A-Za-z0-9_]*)[[:space:]]*=[[:space:]]*(.*)[[:space:]]*$ ]]; then
            key="${BASH_REMATCH[1]}"
            value="${BASH_REMATCH[2]}"
            value="${value%\"}"
            value="${value#\"}"
            value="${value%\'}"
            value="${value#\'}"

            if [[ -z "${!key:-}" ]]; then
                printf -v "${key}" "%s" "${value}"
                export "${key}"
            fi
        fi
    done < "${ENV_FILE}"
fi

DB_NAME="${DB_NAME:-gf_aab_prod}"
DB_HOST="${DB_HOST:-localhost}"
DB_PORT="${DB_PORT:-5432}"
DB_USER="${DB_USER:-garypixie}"
DB_PASS="${DB_PASS:-}"

PG_DUMP_BIN="${PG_DUMP_BIN:-/opt/homebrew/opt/postgresql@16/bin/pg_dump}"
if [[ ! -x "${PG_DUMP_BIN}" ]]; then
    PG_DUMP_BIN="$(command -v pg_dump)"
fi

if [[ -z "${PG_DUMP_BIN}" ]]; then
    echo "pg_dump not found" >&2
    exit 1
fi

if [[ -z "${DB_PASS}" ]]; then
    echo "DB_PASS is not set; refusing to run backup without explicit credentials" >&2
    exit 1
fi

umask 077
mkdir -p "${BACKUP_DIR}"

timestamp="$(date -u +"%Y%m%dT%H%M%SZ")"
backup_file="${BACKUP_DIR}/${DB_NAME}_${timestamp}.dump"
partial_file="${backup_file}.in_progress"

cleanup_partial() {
    rm -f "${partial_file}"
}
trap cleanup_partial EXIT

echo "Starting PostgreSQL backup for ${DB_NAME} at ${timestamp}"

PGPASSWORD="${DB_PASS}" "${PG_DUMP_BIN}" \
    --host="${DB_HOST}" \
    --port="${DB_PORT}" \
    --username="${DB_USER}" \
    --dbname="${DB_NAME}" \
    --format=custom \
    --compress=9 \
    --file="${partial_file}"

mv "${partial_file}" "${backup_file}"
trap - EXIT

echo "Backup complete: ${backup_file}"

find "${BACKUP_DIR}" \
    -type f \
    -name "${DB_NAME}_*.dump" \
    -mtime "+${RETENTION_DAYS}" \
    -print \
    -delete

echo "Retention cleanup complete for backups older than ${RETENTION_DAYS} days"
