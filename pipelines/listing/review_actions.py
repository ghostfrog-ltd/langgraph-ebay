from __future__ import annotations

from html import escape
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import parse_qs, quote, unquote, urlparse

from psycopg2.extras import RealDictCursor

from utils.db_schema import ensure_utc_session, get_connection, get_fresh_connection

VALID_ACTIONS = {"up", "down"}
DEFAULT_REVIEW_SERVER_HOST = "127.0.0.1"
DEFAULT_REVIEW_SERVER_PORT = 8765


def ensure_review_table() -> None:
    """Create the local-only review action table if migrations have not run yet."""

    conn = get_connection()
    with conn, conn.cursor() as cur:
        ensure_utc_session(cur)
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS listing_review_actions (
                id BIGSERIAL PRIMARY KEY,
                source TEXT,
                external_id TEXT NOT NULL,
                title TEXT,
                url TEXT,
                action TEXT NOT NULL CHECK (action IN ('up', 'down')),
                created_at TIMESTAMPTZ NOT NULL DEFAULT (now() AT TIME ZONE 'utc')
            )
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS listing_review_actions_latest_idx
                ON listing_review_actions (external_id, created_at DESC, id DESC)
            """
        )
        cur.execute(
            """
            CREATE INDEX IF NOT EXISTS listing_review_actions_action_idx
                ON listing_review_actions (action, created_at DESC)
            """
        )


def normalise_external_id(value: str) -> str:
    """Accept a Browse REST ID, an item URL, or a bare numeric eBay item ID."""

    raw = unquote(str(value or "").strip())
    if not raw:
        raise ValueError("external_id is required")
    if "ebay." in raw and "/itm/" in raw:
        parsed = urlparse(raw)
        raw = parsed.path.rstrip("/").split("/")[-1]
    if raw.startswith("v1|"):
        return raw
    if raw.isdigit():
        return f"v1|{raw}|0"
    raise ValueError(f"cannot understand eBay item id: {value}")


def _lookup_listing(external_id: str) -> dict[str, Any]:
    conn = get_fresh_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            ensure_utc_session(cur)
            cur.execute(
                """
                SELECT source, external_id, title, url
                FROM auction_listings
                WHERE external_id = %s
                LIMIT 1
                """,
                (external_id,),
            )
            row = cur.fetchone()
            return dict(row) if row else {}
    finally:
        conn.close()


def record_review_action(
    *,
    external_id: str,
    action: str,
    source: str | None = None,
    title: str | None = None,
    url: str | None = None,
) -> dict[str, Any]:
    """Record a local thumbs-up/down action. Latest action wins downstream."""

    normalised_id = normalise_external_id(external_id)
    normalised_action = str(action or "").strip().lower()
    if normalised_action not in VALID_ACTIONS:
        raise ValueError("action must be 'up' or 'down'")

    existing = _lookup_listing(normalised_id)
    source = source or existing.get("source")
    title = title or existing.get("title")
    url = url or existing.get("url")

    ensure_review_table()
    conn = get_connection()
    with conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        ensure_utc_session(cur)
        cur.execute(
            """
            INSERT INTO listing_review_actions (source, external_id, title, url, action)
            VALUES (%s, %s, %s, %s, %s)
            RETURNING id, source, external_id, title, url, action, created_at
            """,
            (source, normalised_id, title, url, normalised_action),
        )
        row = cur.fetchone()
    return dict(row)


def latest_review_actions(limit: int = 50) -> list[dict[str, Any]]:
    """Return recent latest actions, one row per listing."""

    ensure_review_table()
    conn = get_fresh_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            ensure_utc_session(cur)
            cur.execute(
                """
                WITH latest AS (
                    SELECT DISTINCT ON (external_id)
                        id, source, external_id, title, url, action, created_at
                    FROM listing_review_actions
                    ORDER BY external_id, created_at DESC, id DESC
                )
                SELECT *
                FROM latest
                ORDER BY created_at DESC, id DESC
                LIMIT %s
                """,
                (limit,),
            )
            return [dict(row) for row in cur.fetchall()]
    finally:
        conn.close()


def list_favourites(limit: int = 100) -> list[dict[str, Any]]:
    """Return latest thumbs-up listings, enriched from the live listing table."""

    ensure_review_table()
    conn = get_fresh_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            ensure_utc_session(cur)
            cur.execute(
                """
                WITH latest AS (
                    SELECT DISTINCT ON (external_id)
                        id, source, external_id, title, url, action, created_at
                    FROM listing_review_actions
                    ORDER BY external_id, created_at DESC, id DESC
                )
                SELECT
                    COALESCE(a.source, latest.source) AS source,
                    latest.external_id,
                    COALESCE(a.title, latest.title) AS title,
                    COALESCE(a.url, latest.url) AS url,
                    a.status,
                    a.price_current,
                    a.model_key,
                    COALESCE(a.condition_grade, 'B') AS condition_grade,
                    latest.created_at
                FROM latest
                LEFT JOIN auction_listings AS a
                  ON a.external_id = latest.external_id
                WHERE latest.action = 'up'
                ORDER BY latest.created_at DESC, latest.id DESC
                LIMIT %s
                """,
                (limit,),
            )
            return [dict(row) for row in cur.fetchall()]
    finally:
        conn.close()


def review_url(
    *,
    base_url: str,
    action: str,
    source: Any,
    external_id: Any,
    title: Any,
    url: Any,
) -> str:
    clean_base = str(base_url or "").rstrip("/")
    params = {
        "action": str(action),
        "source": str(source or ""),
        "external_id": str(external_id or ""),
    }
    query = "&".join(f"{key}={quote(value, safe='')}" for key, value in params.items())
    return f"{clean_base}/review?{query}"


def start_review_server(
    *,
    host: str = DEFAULT_REVIEW_SERVER_HOST,
    port: int = DEFAULT_REVIEW_SERVER_PORT,
) -> None:
    """Run the local thumbs-up/down click handler."""

    ensure_review_table()

    class ReviewHandler(BaseHTTPRequestHandler):
        def log_message(self, format: str, *args: Any) -> None:
            return

        def _send_html(self, status: int, body: str) -> None:
            payload = body.encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def do_GET(self) -> None:
            parsed = urlparse(self.path)
            if parsed.path in {"", "/"}:
                self._send_html(
                    200,
                    "<h1>GhostFrog review server</h1>"
                    "<p>Use the Up/Down links in the heartbeat email.</p>",
                )
                return
            if parsed.path != "/review":
                self._send_html(404, "<h1>Not found</h1>")
                return

            params = parse_qs(parsed.query)
            try:
                row = record_review_action(
                    external_id=(params.get("external_id") or [""])[0],
                    action=(params.get("action") or [""])[0],
                    source=(params.get("source") or [""])[0] or None,
                    title=(params.get("title") or [""])[0] or None,
                    url=(params.get("url") or [""])[0] or None,
                )
            except Exception as exc:
                self._send_html(400, f"<h1>Review failed</h1><p>{escape(str(exc))}</p>")
                return

            action_label = "Favourite" if row["action"] == "up" else "Excluded"
            title = escape(str(row.get("title") or row.get("external_id") or "listing"))
            ebay_link = ""
            if row.get("url"):
                ebay_link = (
                    f'<p><a href="{escape(str(row["url"]))}">'
                    "Open listing on eBay</a></p>"
                )
            self._send_html(
                200,
                f"<h1>{action_label}</h1>"
                f"<p>{title}</p>"
                f"<p>Latest local action: <strong>{escape(row['action'])}</strong>.</p>"
                f"{ebay_link}",
            )

    server = ThreadingHTTPServer((host, int(port)), ReviewHandler)
    print(f"Review server listening on http://{host}:{int(port)}")
    print("Leave this running while clicking Up/Down links in email.")
    server.serve_forever()
