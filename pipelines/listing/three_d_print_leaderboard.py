from __future__ import annotations

import argparse
import base64
import hashlib
import hmac
import json
import os
import time
import uuid
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any
from urllib.parse import urlencode

import requests
from dotenv import load_dotenv


load_dotenv()

MARKETPLACE_ID = "EBAY_GB"
DEFAULT_QUERY_LIMIT = 100
DEFAULT_MAX_PAGES_PER_QUERY = 2
DEFAULT_TOP_LIMIT = 200
LEADERBOARD_ENABLED = (
    os.getenv("GF_3D_PRINT_LEADERBOARD_ENABLED", "0") == "1"
    or os.getenv("GHOSTFROG_3D_PRINT_LEADERBOARD_ENABLED", "0") == "1"
)

QUERIES = [
    "3d printed",
    "3d printed custom",
    "3d printed holder",
    "3d printed mount",
    "3d printed bracket",
    "3d printed stand",
    "3d printed parts",
    "3d printed cosplay",
    "3d printed terrain",
    "3d printed warhammer",
    "3d printed tabletop",
    "3d printed fidget",
    "3d printed dragon",
    "3d printed planter",
    "3d printed keycap",
    "3d printed car",
    "3d printed rc",
    "3d printed organizer",
]


@dataclass
class SellerSnapshot:
    username: str
    feedback_score: int = 0
    feedback_percentage: float = 0.0
    item_ids: set[str] = field(default_factory=set)
    titles: list[str] = field(default_factory=list)
    urls: list[str] = field(default_factory=list)
    total_value: float = 0.0
    max_price: float = 0.0
    queries: set[str] = field(default_factory=set)

    def add_item(self, item: dict[str, Any], query: str) -> None:
        item_id = str(item.get("itemId") or "")
        if item_id:
            self.item_ids.add(item_id)

        seller_info = item.get("seller") or {}
        if isinstance(seller_info, dict):
            self.feedback_score = max(self.feedback_score, _int_value(seller_info.get("feedbackScore")))
            self.feedback_percentage = max(
                self.feedback_percentage,
                _float_value(seller_info.get("feedbackPercentage")),
            )

        value = _money_value(item)
        self.total_value += value
        self.max_price = max(self.max_price, value)
        self.queries.add(query)

        title = str(item.get("title") or "").strip()
        if title and title not in self.titles and len(self.titles) < 4:
            self.titles.append(title[:1000])

        item_url = str(item.get("itemWebUrl") or "").strip()
        if item_url and item_url not in self.urls and len(self.urls) < 2:
            self.urls.append(item_url[:2000])

    def payload_row(self, rank: int) -> dict[str, Any]:
        return {
            "rank": rank,
            "seller_username": self.username,
            "item_count": len(self.item_ids),
            "active_value": round(self.total_value, 2),
            "max_price": round(self.max_price, 2),
            "feedback_score": self.feedback_score,
            "feedback_percentage": round(self.feedback_percentage, 2),
            "matched_queries": sorted(self.queries),
            "example_titles": self.titles,
            "example_url": self.urls[0] if self.urls else None,
        }


def _int_value(value: Any) -> int:
    try:
        return int(float(str(value or 0)))
    except ValueError:
        return 0


def _float_value(value: Any) -> float:
    try:
        return float(str(value or 0))
    except ValueError:
        return 0.0


def _money_value(raw: dict[str, Any]) -> float:
    for key in ("price", "currentBidPrice"):
        price = raw.get(key)
        if isinstance(price, dict) and price.get("value") is not None:
            return _float_value(price.get("value"))
    return 0.0


def get_app_token() -> str:
    app_id = os.getenv("EBAY_APP_ID")
    cert_id = os.getenv("EBAY_CERT_ID")
    api_base = os.getenv("EBAY_API_BASE", "https://api.ebay.com").rstrip("/")
    if not app_id or not cert_id:
        raise RuntimeError("Missing EBAY_APP_ID / EBAY_CERT_ID")

    auth = base64.b64encode(f"{app_id}:{cert_id}".encode("utf-8")).decode("utf-8")
    response = requests.post(
        f"{api_base}/identity/v1/oauth2/token",
        headers={
            "Authorization": f"Basic {auth}",
            "Content-Type": "application/x-www-form-urlencoded",
        },
        data={
            "grant_type": "client_credentials",
            "scope": "https://api.ebay.com/oauth/api_scope",
        },
        timeout=20,
    )
    response.raise_for_status()
    return str(response.json()["access_token"])


def collect_sellers(
    *,
    token: str,
    query_limit: int = DEFAULT_QUERY_LIMIT,
    max_pages_per_query: int = DEFAULT_MAX_PAGES_PER_QUERY,
) -> tuple[list[SellerSnapshot], int, int]:
    api_base = os.getenv("EBAY_API_BASE", "https://api.ebay.com").rstrip("/")
    headers = {
        "Authorization": f"Bearer {token}",
        "X-EBAY-C-MARKETPLACE-ID": MARKETPLACE_ID,
    }
    sellers: dict[str, SellerSnapshot] = {}
    seen_items: set[str] = set()
    api_calls = 0

    for query in QUERIES:
        for page in range(max_pages_per_query):
            params = {
                "q": query,
                "filter": "sellerAccountTypes:{BUSINESS},itemLocationCountry:GB",
                "limit": str(query_limit),
                "offset": str(page * query_limit),
                "sort": "bestMatch",
            }
            url = f"{api_base}/buy/browse/v1/item_summary/search?{urlencode(params)}"
            response = requests.get(url, headers=headers, timeout=20)
            api_calls += 1
            if response.status_code != 200:
                raise RuntimeError(
                    f"eBay Browse search failed query={query!r} page={page} "
                    f"status={response.status_code}: {response.text[:240]}"
                )

            payload = response.json()
            items = payload.get("itemSummaries") or []
            if not isinstance(items, list) or not items:
                break

            for item in items:
                if not isinstance(item, dict):
                    continue
                item_id = str(item.get("itemId") or "")
                if not item_id or item_id in seen_items:
                    continue
                seen_items.add(item_id)

                seller_info = item.get("seller") or {}
                if not isinstance(seller_info, dict):
                    continue
                username = str(seller_info.get("username") or "").strip()
                if not username:
                    continue

                key = username.lower()
                seller = sellers.setdefault(key, SellerSnapshot(username=username))
                seller.add_item(item, query)

            total = _int_value(payload.get("total"))
            if (page + 1) * query_limit >= total:
                break
            time.sleep(0.15)

    ranked = sorted(
        sellers.values(),
        key=lambda seller: (len(seller.item_ids), seller.total_value, seller.feedback_score),
        reverse=True,
    )
    return ranked, len(seen_items), api_calls


def build_payload(*, top_limit: int = DEFAULT_TOP_LIMIT) -> dict[str, Any]:
    ranked, item_count, api_calls = collect_sellers(token=get_app_token())
    generated_at = datetime.now(UTC).isoformat(timespec="seconds")
    selected = ranked[: max(top_limit, 1)]

    return {
        "schema_version": 1,
        "batch_id": str(uuid.uuid4()),
        "generated_at": generated_at,
        "marketplace": MARKETPLACE_ID,
        "summary": {
            "queries": len(QUERIES),
            "api_calls": api_calls,
            "unique_items": item_count,
            "business_sellers": len(ranked),
        },
        "sellers": [
            seller.payload_row(rank)
            for rank, seller in enumerate(selected, start=1)
        ],
    }


def send_snapshot(
    payload: dict[str, Any],
    *,
    endpoint: str,
    secret: str,
    timeout: int = 120,
) -> dict[str, Any]:
    body = json.dumps(payload, default=str, separators=(",", ":"))
    timestamp = str(int(datetime.now(UTC).timestamp()))
    signature = hmac.new(
        secret.encode("utf-8"),
        f"{timestamp}.{body}".encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()
    response = requests.post(
        f"{endpoint.rstrip('/')}/api/v1/sync/3d-print-sellers",
        data=body.encode("utf-8"),
        headers={
            "Content-Type": "application/json",
            "Accept": "application/json",
            "X-GhostFrog-Timestamp": timestamp,
            "X-GhostFrog-Signature": signature,
        },
        timeout=timeout,
    )
    response.raise_for_status()
    result = response.json()
    return result if isinstance(result, dict) else {"result": result}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Publish the 3D print seller leaderboard to Laravel.")
    parser.add_argument("--send", action="store_true", help="Send the snapshot. Without this, print a dry-run summary.")
    parser.add_argument("--endpoint", default=os.getenv("GHOSTFROG_WEB_URL", ""))
    parser.add_argument("--secret", default=os.getenv("GHOSTFROG_SYNC_SECRET", ""))
    parser.add_argument("--top-limit", type=int, default=DEFAULT_TOP_LIMIT)
    parser.add_argument("--json", action="store_true", help="Print the safe payload.")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    if not LEADERBOARD_ENABLED:
        print(
            "3D print seller leaderboard disabled; set "
            "GF_3D_PRINT_LEADERBOARD_ENABLED=1 to opt back in."
        )
        return 0

    payload = build_payload(top_limit=args.top_limit)

    if args.json:
        print(json.dumps(payload, indent=2, default=str))
    else:
        print(
            "3D print seller leaderboard: "
            f"{len(payload['sellers'])} published sellers, "
            f"{payload['summary']['business_sellers']} matched business sellers, "
            f"{payload['summary']['unique_items']} unique active items."
        )

    if not args.send:
        print("Dry run only; nothing was sent.")
        return 0
    if not args.endpoint or not args.secret:
        print("Send requires GHOSTFROG_WEB_URL and GHOSTFROG_SYNC_SECRET.")
        return 2

    result = send_snapshot(payload, endpoint=args.endpoint, secret=args.secret)
    print(
        f"Published 3D print seller batch {result.get('batch_id', payload['batch_id'])}: "
        f"{result.get('sellers', 0)} sellers."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
