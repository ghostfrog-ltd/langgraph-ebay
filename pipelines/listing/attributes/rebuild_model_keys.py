from __future__ import annotations

import argparse
import json  # needed for raw_attrs parsing
from psycopg2.extras import RealDictCursor
from utils.db_schema import get_connection
from pipelines.listing.attributes.mk import normalise_model
from utils.condition import _derive_condition_grade
from utils.logger import get_logger

logger = get_logger(__name__)


LIMIT_ROWS = 9000
UNKNOWN_KEY = "unknown"


def _parse_raw_attrs(raw):
    """
    Accept dict (JSON/JSONB) or JSON string; return dict or None.
    """
    if raw is None:
        return None
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, str):
        s = raw.strip()
        if not s:
            return None
        try:
            obj = json.loads(s)
            return obj if isinstance(obj, dict) else None
        except Exception:
            return None
    return None


def _normalise_attrs(attrs: dict | None) -> dict | None:
    """
    Make attrs safe for key builders that expect strings:
      - If value is a list/tuple, use its first element.
      - Coerce non-None values to str.
    """
    if not attrs:
        return attrs
    out: dict = {}
    for k, v in attrs.items():
        if isinstance(v, (list, tuple)):
            v = v[0] if v else None
        if v is None:
            out[k] = None
        else:
            out[k] = v if isinstance(v, str) else str(v)
    return out


def rebuild_model_keys(
    limit: int = LIMIT_ROWS,
    *,
    only_missing: bool = True,
    source: str | None = None,
    dry_run: bool = False,
) -> dict[str, int]:
    """
    Recompute listing model keys from persisted titles and raw attributes.

    By default this preserves the legacy behaviour of filling only NULL/unknown
    keys. Passing `only_missing=False` intentionally revisits existing keys too,
    which is required after tightening a source-specific key builder; otherwise
    old broad keys can survive forever and contaminate comps.
    """

    updated_total = 0
    changed_total = 0
    condition_changed_total = 0
    to_unknown_total = 0
    from_unknown_total = 0
    source_filter = source.strip().lower() if source else None

    connection = get_connection()

    with connection.cursor(cursor_factory=RealDictCursor) as cur:
        where_clauses = []
        params: list[object] = []

        if only_missing:
            where_clauses.append("(model_key IS NULL OR LOWER(model_key) = %s)")
            params.append(UNKNOWN_KEY)
        if source_filter:
            where_clauses.append("LOWER(source) = %s")
            params.append(source_filter)

        where_sql = ""
        if where_clauses:
            where_sql = "WHERE " + " AND ".join(where_clauses)

        params.append(limit)
        cur.execute(
            f"""
            SELECT id, title, raw_attrs, source, model_key, condition_grade
              FROM auction_listings
              {where_sql}
             ORDER BY id
             LIMIT %s
            """,
            params,
        )
        rows = cur.fetchall()

        for row in rows:
            listing_source = row["source"] or ""
            title = row["title"] or ""
            attrs_raw = _parse_raw_attrs(row.get("raw_attrs"))
            attrs = _normalise_attrs(attrs_raw)
            key = normalise_model(title=title, attrs=attrs, source=listing_source) or UNKNOWN_KEY
            condition_grade = _derive_condition_grade(attrs or {}, title)

            old_key = str(row.get("model_key") or UNKNOWN_KEY).lower()
            if key != old_key:
                changed_total += 1
                if key == UNKNOWN_KEY and old_key != UNKNOWN_KEY:
                    to_unknown_total += 1
                if old_key == UNKNOWN_KEY and key != UNKNOWN_KEY:
                    from_unknown_total += 1
            old_condition = str(row.get("condition_grade") or "").upper()
            if condition_grade != old_condition:
                condition_changed_total += 1

            if not dry_run:
                cur.execute(
                    "UPDATE auction_listings SET model_key = %s, condition_grade = %s WHERE id = %s",
                    (key, condition_grade, row["id"]),
                )
            updated_total += 1

        if dry_run:
            connection.rollback()
        else:
            connection.commit()

    logger.info(
        "[rebuild_model_keys] batch complete — scanned %d rows, changed %d "
        "(condition_changed=%d, to_unknown=%d, from_unknown=%d, limit=%d, only_missing=%s, source=%s, dry_run=%s)",
        updated_total,
        changed_total,
        condition_changed_total,
        to_unknown_total,
        from_unknown_total,
        limit,
        only_missing,
        source_filter or "*",
        dry_run,
    )
    return {
        "scanned": updated_total,
        "changed": changed_total,
        "condition_changed": condition_changed_total,
        "to_unknown": to_unknown_total,
        "from_unknown": from_unknown_total,
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Rebuild auction listing model keys.")
    parser.add_argument("--limit", type=int, default=LIMIT_ROWS)
    parser.add_argument("--all", action="store_true", help="recompute existing keys as well as missing keys")
    parser.add_argument("--source", default=None, help="optional source filter, e.g. ebay-consoles")
    parser.add_argument("--dry-run", action="store_true", help="scan and count changes without writing")
    args = parser.parse_args()

    rebuild_model_keys(
        limit=args.limit,
        only_missing=not args.all,
        source=args.source,
        dry_run=args.dry_run,
    )
