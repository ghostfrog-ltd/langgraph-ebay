from __future__ import annotations

import os
from typing import Any, Mapping

import requests

from utils.usage_tracker import increment_api_usage


BUCKET_LIMIT_DEFAULTS: dict[str, int] = {
    "trading": 50000,
    "browse": 5000,
    "identity": 5000,
    "developer_analytics": 5000,
}

BUCKET_LABELS: dict[str, str] = {
    "trading": "Trading API",
    "browse": "Browse API",
    "identity": "Identity API",
    "developer_analytics": "Developer Analytics API",
    "unknown": "Unknown/local",
}

SERVICE_BUCKETS: dict[str, str] = {
    "ebay_ended_v1": "trading",
    "ebay_attributes_v1": "trading",
    "ebay_base_fetch_category_items_v1": "browse",
    "ebay_base_fetch_items_by_id_v1": "browse",
    "ebay_base_seller_items_v1": "browse",
    "ebay_auth_v1": "identity",
    "ebay_developer_analytics_v1": "developer_analytics",
}

def _bucket_limit_env_key(bucket: str) -> str:
    cleaned = "".join(ch if ch.isalnum() else "_" for ch in bucket.upper())
    return f"EBAY_BUCKET_LIMIT_{cleaned}"


def get_bucket_limit(bucket: str) -> int:
    raw = os.getenv(_bucket_limit_env_key(bucket))
    if raw:
        try:
            value = int(raw)
            if value > 0:
                return value
        except ValueError:
            pass
    return BUCKET_LIMIT_DEFAULTS.get(bucket, 0)


SERVICE_LIMIT_DEFAULTS: dict[str, int] = {
    service: get_bucket_limit(bucket)
    for service, bucket in SERVICE_BUCKETS.items()
    if bucket != "unknown"
}


def service_bucket(service: str) -> str:
    return SERVICE_BUCKETS.get(service, "unknown")


def _bucket_limit(bucket: str, overrides: Mapping[str, int] | None = None) -> int:
    if overrides and bucket in overrides:
        try:
            value = int(overrides[bucket])
            if value > 0:
                return value
        except (TypeError, ValueError):
            pass
    return get_bucket_limit(bucket)


def build_api_accounting(
    raw_usage: Mapping[str, int] | None,
    *,
    bucket_limit_overrides: Mapping[str, int] | None = None,
) -> dict[str, Any]:
    """Group local service counters into eBay's real daily quota buckets."""

    usage = {str(k): int(v or 0) for k, v in (raw_usage or {}).items()}
    buckets: dict[str, dict[str, Any]] = {}
    services: dict[str, dict[str, Any]] = {}

    for bucket in BUCKET_LIMIT_DEFAULTS:
        buckets[bucket] = {
            "bucket": bucket,
            "label": BUCKET_LABELS.get(bucket, bucket),
            "used": 0,
            "limit": _bucket_limit(bucket, bucket_limit_overrides),
            "remaining": _bucket_limit(bucket, bucket_limit_overrides),
            "blocked": False,
            "services": [],
        }

    unknown_services: list[str] = []

    for service, used in usage.items():
        bucket = service_bucket(service)
        if bucket == "unknown":
            unknown_services.append(service)
            services[service] = {
                "service": service,
                "bucket": bucket,
                "bucket_label": BUCKET_LABELS["unknown"],
                "used": used,
                "limit": 0,
                "remaining": 0,
                "blocked": False,
            }
            continue

        bucket_info = buckets.setdefault(
            bucket,
            {
                "bucket": bucket,
                "label": BUCKET_LABELS.get(bucket, bucket),
                "used": 0,
                "limit": _bucket_limit(bucket, bucket_limit_overrides),
                "remaining": _bucket_limit(bucket, bucket_limit_overrides),
                "blocked": False,
                "services": [],
            },
        )
        bucket_info["used"] = int(bucket_info["used"]) + used
        bucket_info["services"].append(service)

    blocked_buckets: list[str] = []
    for bucket, info in buckets.items():
        limit = int(info.get("limit") or 0)
        used = int(info.get("used") or 0)
        remaining = max(limit - used, 0) if limit else 0
        blocked = bool(limit and used >= limit)
        info["remaining"] = remaining
        info["blocked"] = blocked
        if blocked:
            blocked_buckets.append(bucket)

    blocked_services: list[str] = []
    known_services = set(SERVICE_BUCKETS).union(usage)
    for service in sorted(known_services):
        bucket = service_bucket(service)
        used = usage.get(service, 0)
        bucket_info = buckets.get(bucket) or {}
        blocked = bool(bucket_info.get("blocked"))
        if blocked:
            blocked_services.append(service)
        services.setdefault(
            service,
            {
                "service": service,
                "bucket": bucket,
                "bucket_label": BUCKET_LABELS.get(bucket, bucket),
                "used": used,
                "limit": int(bucket_info.get("limit") or 0),
                "remaining": int(bucket_info.get("remaining") or 0),
                "blocked": blocked,
            },
        )
        services[service]["blocked"] = blocked
        services[service]["limit"] = int(bucket_info.get("limit") or 0)
        services[service]["remaining"] = int(bucket_info.get("remaining") or 0)

    known_total = sum(int(info.get("used") or 0) for info in buckets.values())
    raw_total = sum(usage.values())

    return {
        "raw_total": raw_total,
        "known_total": known_total,
        "unknown_total": raw_total - known_total,
        "buckets": buckets,
        "services": services,
        "blocked_buckets": sorted(blocked_buckets),
        "blocked_services": sorted(set(blocked_services)),
        "unknown_services": sorted(unknown_services),
    }


def bucket_limit_overrides_from_rate_limits(rows: list[dict[str, Any]]) -> dict[str, int]:
    """Map eBay Developer Analytics rows onto local daily quota buckets."""

    overrides: dict[str, int] = {}
    for row in rows:
        try:
            limit = int(row.get("limit") or 0)
        except (TypeError, ValueError):
            continue
        if limit <= 0:
            continue

        try:
            time_window = int(row.get("time_window") or 86400)
        except (TypeError, ValueError):
            time_window = 86400
        if time_window != 86400:
            continue

        context = str(row.get("api_context") or "").lower()
        api_name = str(row.get("api_name") or "").lower()
        resource = str(row.get("resource") or "").lower()
        bucket = ""
        if api_name == "tradingapi" and resource == "getitem":
            bucket = "trading"
        elif context == "buy" and api_name == "browse":
            bucket = "browse"
        elif context == "commerce" and "identity" in api_name:
            bucket = "identity"
        elif context == "developer" and "analytics" in api_name:
            bucket = "developer_analytics"

        if bucket:
            overrides[bucket] = max(int(overrides.get(bucket) or 0), limit)
    return overrides


def fetch_ebay_rate_limits(*, token: str, api_base: str) -> dict[str, Any]:
    """Fetch eBay's own quota view via Developer Analytics API."""

    base = api_base.rstrip("/")
    url = f"{base}/developer/analytics/v1_beta/rate_limit/"
    response = requests.get(
        url,
        headers={"Authorization": f"Bearer {token}"},
        timeout=10,
    )
    response.raise_for_status()
    increment_api_usage("ebay_developer_analytics_v1")
    return response.json()


def flatten_rate_limits(payload: Mapping[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for api in payload.get("rateLimits") or []:
        if not isinstance(api, Mapping):
            continue
        for resource in api.get("resources") or []:
            if not isinstance(resource, Mapping):
                continue
            for rate in resource.get("rates") or []:
                if not isinstance(rate, Mapping):
                    continue
                rows.append(
                    {
                        "api_context": api.get("apiContext"),
                        "api_name": api.get("apiName"),
                        "api_version": api.get("apiVersion"),
                        "resource": resource.get("name"),
                        "limit": rate.get("limit"),
                        "count": rate.get("count"),
                        "remaining": rate.get("remaining"),
                        "reset": rate.get("reset"),
                        "time_window": rate.get("timeWindow"),
                    }
                )
    return rows
