from __future__ import annotations

import os
import time
from time import perf_counter
from datetime import datetime, timezone, timedelta
from typing import Optional, List, Dict, Any, Set, Tuple
from collections import deque
from urllib.parse import quote_plus
from psycopg2.extras import Json, execute_values
import requests
from utils.logger import get_logger
from pipelines.listing.attributes.mk import normalise_model
from utils.condition import _derive_condition_grade
from utils.db_schema import (
    resolve_source_id,
    resolve_source_field,
    get_connection,
    to_aware_utc,
    ensure_utc_session
)
from utils.usage_tracker import increment_api_usage

logger = get_logger(__name__)

# -----------------
# Constants / defaults
# -----------------
DEFAULT_Q = "a"  # minimal harmless primary query to satisfy Browse's rule


# ----------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------
def _parse_iso_utc(ts: Optional[str]) -> Optional[datetime]:
    if not ts:
        return None
    try:
        ts_fixed = ts.replace("Z", "+00:00")
        dt = datetime.fromisoformat(ts_fixed)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        else:
            dt = dt.astimezone(timezone.utc)
        return dt
    except Exception:
        return None


def _secs_left(end_time: Optional[datetime]) -> Optional[int]:
    if not end_time:
        return None
    if end_time.tzinfo is None:
        end_aware = end_time.replace(tzinfo=timezone.utc)
    else:
        end_aware = end_time.astimezone(timezone.utc)
    now_aware = datetime.now(timezone.utc)
    delta = (end_aware - now_aware).total_seconds()
    return int(delta) if delta > 0 else 0


def _iso_z(dt: datetime) -> str:
    # ISO8601 with Z suffix
    return dt.replace(microsecond=0, tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")


def _browse_attr_value(value: Any, *, max_chars: int = 4000) -> Any:
    if value is None:
        return None
    if isinstance(value, str):
        value = value.strip()
        if not value:
            return None
        return value[:max_chars]
    if isinstance(value, (int, float, bool)):
        return value
    return value


def _add_browse_attr(
    attrs: dict[str, Any],
    label: str,
    value: Any,
    *,
    max_chars: int = 4000,
) -> None:
    value = _browse_attr_value(value, max_chars=max_chars)
    if value is not None:
        attrs[label] = value


def _image_url_from_value(value: Any) -> str | None:
    if isinstance(value, str):
        url = value.strip()
    elif isinstance(value, dict):
        url = str(value.get("imageUrl") or value.get("url") or "").strip()
    else:
        url = ""

    if not url:
        return None
    if not url.lower().startswith(("http://", "https://")):
        return None
    return url


def _browse_image_urls(raw: dict[str, Any]) -> list[str]:
    urls: list[str] = []
    seen: set[str] = set()

    def add(value: Any) -> None:
        url = _image_url_from_value(value)
        if url and url not in seen:
            seen.add(url)
            urls.append(url)

    for key in ("image", "thumbnailImage"):
        add(raw.get(key))

    for key in ("additionalImages", "thumbnailImages"):
        values = raw.get(key)
        if isinstance(values, list):
            for value in values:
                add(value)

    return urls


def _browse_description_attrs(raw: dict[str, Any]) -> dict[str, Any]:
    """
    Extract useful low-cost evidence from Browse search/getItem responses.

    The attributes pipeline still owns normalized specifics. This preserves
    already-returned Browse evidence in raw_attrs so human review and condition
    guardrails can see risk text that may be absent from the title.
    """
    attrs: dict[str, Any] = {}
    for key in (
        "condition",
        "conditionDescription",
        "description",
        "shortDescription",
        "subtitle",
        "categoryId",
        "categoryPath",
        "itemCreationDate",
        "itemEndDate",
    ):
        _add_browse_attr(attrs, f"Browse {key}", raw.get(key))

    seller = raw.get("seller") or {}
    if isinstance(seller, dict):
        for key in ("username", "feedbackScore", "feedbackPercentage"):
            _add_browse_attr(attrs, f"Browse seller {key}", seller.get(key), max_chars=255)

    item_location = raw.get("itemLocation") or {}
    if isinstance(item_location, dict):
        for key in ("country", "city", "stateOrProvince", "postalCode"):
            _add_browse_attr(attrs, f"Browse itemLocation {key}", item_location.get(key), max_chars=255)

    return_terms = raw.get("returnTerms") or {}
    if isinstance(return_terms, dict):
        for key in ("returnsAccepted", "refundMethod", "returnMethod"):
            _add_browse_attr(attrs, f"Browse returnTerms {key}", return_terms.get(key), max_chars=255)
        return_period = return_terms.get("returnPeriod")
        if isinstance(return_period, dict):
            _add_browse_attr(
                attrs,
                "Browse returnTerms returnPeriod",
                " ".join(
                    str(part)
                    for part in (
                        return_period.get("value"),
                        return_period.get("unit"),
                    )
                    if part is not None
                ),
                max_chars=255,
            )

    shipping_options = raw.get("shippingOptions") or []
    if isinstance(shipping_options, list) and shipping_options:
        compact_shipping: list[dict[str, Any]] = []
        for option in shipping_options[:3]:
            if not isinstance(option, dict):
                continue
            compact: dict[str, Any] = {}
            for key in ("shippingServiceCode", "shippingCarrierCode", "type"):
                if option.get(key) is not None:
                    compact[key] = option.get(key)
            cost = option.get("shippingCost")
            if isinstance(cost, dict):
                compact["shippingCost"] = {
                    key: cost.get(key)
                    for key in ("value", "currency")
                    if cost.get(key) is not None
                }
            if compact:
                compact_shipping.append(compact)
        if compact_shipping:
            attrs["Browse shippingOptions"] = compact_shipping

    aspects = raw.get("localizedAspects") or []
    if isinstance(aspects, list):
        for aspect in aspects:
            if not isinstance(aspect, dict):
                continue
            name = str(aspect.get("name") or "").strip()
            if not name:
                continue
            _add_browse_attr(attrs, f"Browse aspect {name}", aspect.get("value"), max_chars=1000)

    image_urls = _browse_image_urls(raw)
    if image_urls:
        attrs["Browse imageUrls"] = image_urls[:12]
        attrs["Browse imageUrl"] = image_urls[0]

    return attrs


def _sale_type_from_buying_options(
    buying_options: list[Any],
    *,
    fallback: str | None = None,
) -> str:
    options = {str(opt or "").strip().upper() for opt in buying_options}
    has_auction = "AUCTION" in options
    has_bin = "FIXED_PRICE" in options
    if has_auction and has_bin:
        return "both"
    if has_auction:
        return "auction"
    if has_bin:
        return "bin"
    return str(fallback or "")


def _search_sort_for_sale_type(sale_type: str | None) -> str:
    return "newlyListed" if sale_type == "bin" else "endingSoon"


def is_configurable_item(raw: dict[str, Any]) -> bool:
    """
    Detect multi-variation / configurable-style listings so we can skip them entirely.
    """
    val = raw.get("isMultiVariationListing")
    if val is not None:
        if isinstance(val, (list, tuple)):
            if val:
                val = val[0]
        if isinstance(val, dict) and "__value__" in val:
            val = val.get("__value__")
        if isinstance(val, bool):
            if val:
                return True
        elif isinstance(val, str) and val.lower() == "true":
            return True

    group_type = raw.get("itemGroupType")
    if isinstance(group_type, str):
        gt = group_type.strip().upper()
        if gt in {"SELLER_DEFINED_VARIATIONS", "GROUP", "MULTI_SKU"}:
            return True

    if "variations" in raw or "variation" in raw:
        return True

    return False


def get_item_id(raw: dict[str, Any]) -> str:
    iid = raw.get("itemId") or raw.get("item_id") or raw.get("legacyItemId")
    return str(iid) if iid is not None else "unknown"


def _browse_item_unavailable_reason(raw: dict[str, Any]) -> str | None:
    """Return a reason when Browse says the item is no longer buyable."""

    end_time = _parse_iso_utc(raw.get("itemEndDate"))
    if end_time and end_time <= datetime.now(timezone.utc):
        return "browse_item_ended"

    def _status(value: Any) -> str:
        return str(value or "").strip().upper()

    def _qty(value: Any) -> int | None:
        if value is None:
            return None
        try:
            return int(float(str(value)))
        except (TypeError, ValueError):
            return None

    def _first_present(mapping: dict[str, Any], keys: tuple[str, ...]) -> Any:
        for key in keys:
            if key in mapping and mapping[key] is not None:
                return mapping[key]
        return None

    unavailable_statuses = {"OUT_OF_STOCK", "SOLD_OUT", "ENDED", "UNAVAILABLE"}
    available_statuses = {"IN_STOCK", "LIMITED_QUANTITY", "AVAILABLE"}

    top_level_status = _status(
        raw.get("availabilityStatus")
        or raw.get("availability")
        or raw.get("estimatedAvailabilityStatus")
    )
    if top_level_status in unavailable_statuses:
        return "browse_out_of_stock"
    if top_level_status in available_statuses:
        return None

    availabilities = raw.get("estimatedAvailabilities") or []
    if isinstance(availabilities, dict):
        availabilities = [availabilities]
    if not isinstance(availabilities, list):
        return None

    unavailable_signals = 0
    meaningful_signals = 0
    for availability in availabilities:
        if not isinstance(availability, dict):
            continue
        status = _status(
            availability.get("estimatedAvailabilityStatus")
            or availability.get("availabilityStatus")
        )
        qty = _qty(
            _first_present(
                availability,
                ("estimatedAvailableQuantity", "availableQuantity", "quantity"),
            )
        )
        if status in available_statuses or (qty is not None and qty > 0):
            return None
        if status in unavailable_statuses or qty == 0:
            unavailable_signals += 1
            meaningful_signals += 1
        elif status:
            meaningful_signals += 1

    if meaningful_signals and unavailable_signals == meaningful_signals:
        return "browse_out_of_stock"
    return None


# ----------------------------------------------------------------------
# Base Adapter
# ----------------------------------------------------------------------
class EbayAdapterBase:
    DOMAIN: str = "ebay-base"

    CATEGORY_IDS: List[int] = []
    SELLER_USERNAME: Optional[str] = None
    FETCH_MODE: str = "category"
    SALE_TYPE: str | List[str] = ["bin", "auction"]
    SEARCH_QUERY: Optional[str] = None
    SEARCH_QUERIES: List[str] = []
    SEARCH_QUERIES_BY_CATEGORY: Dict[int | None, List[str]] = {}
    RETRO_KEYWORDS: List[str] = []
    MODERN_KEYWORDS: List[str] = []

    # Tunables
    FLUSH_EVERY = 50
    FLUSH_SECONDS = 3
    MIN_TIME_BATCH = 10
    CATEGORY_PAUSE_SECONDS = 1.5

    # Politeness
    MAX_PAGES = 5
    MAX_LISTINGS = 200

    def __init__(self):
        self._source_name, self._source_id = self._resolve_source()
        self._batch_buffer: list[dict[str, Any]] = []
        self._ph_buffer: list[tuple[str, int, int]] = []
        self._last_flush = time.time()
        self._hist_api: deque[float] = deque(maxlen=500)
        self._hist_norm: deque[float] = deque(maxlen=500)
        self._hist_db: deque[float] = deque(maxlen=500)
        self._bench_n: int = 0

    # ------------------------------------------------------------------
    # Source resolution
    # ------------------------------------------------------------------
    def _resolve_source(self) -> tuple[str, Optional[int]]:
        try:
            sid = resolve_source_id(self.DOMAIN, use_domain=True)
            if sid is not None:
                sname = resolve_source_field(self.DOMAIN, "name", use_domain=True)
                logger.info(f"[{self.DOMAIN}] sources resolved by domain -> name='{sname}', id={sid}")
                return (str(sname) if sname else self.DOMAIN, int(sid))
        except Exception as e:
            logger.warning(f"[{self.DOMAIN}] domain lookup failed: {e}")

        try:
            sname = resolve_source_field(self.DOMAIN, "name", use_domain=False)
            if sname:
                sid = resolve_source_id(self.DOMAIN, use_domain=False)
                logger.info(
                    f"[{self.DOMAIN}] sources resolved by name -> name='{sname}', id={sid}"
                )
                return (str(sname), int(sid) if sid is not None else None)
        except Exception:
            pass

        for legacy_key in ("ebay-uk", "ebay"):
            try:
                sname = resolve_source_field(legacy_key, "name", use_domain=False)
                if sname:
                    sid = resolve_source_id(legacy_key, use_domain=False)
                    logger.info(
                        f"[{self.DOMAIN}] sources resolved via legacy key '{legacy_key}' -> name='{sname}', id={sid}"
                    )
                    return (str(sname), int(sid) if sid is not None else None)
            except Exception:
                continue

        logger.warning(f"[{self.DOMAIN}] sources row not found; using fallback '{self.DOMAIN}'")
        return self.DOMAIN, None

    # ------------------------------------------------------------------
    # Classification helpers
    # ------------------------------------------------------------------
    def categorize_title(self, title_lower: str) -> str:
        if any(k in title_lower for k in self.RETRO_KEYWORDS):
            return "retro"
        if any(k in title_lower for k in self.MODERN_KEYWORDS):
            return "modern"
        return "unknown"

    '''
    def _model_key_for(self, title: str) -> Optional[str]:
        try:
            mk = normalise_model(title)
        except Exception:
            return None
        if isinstance(mk, tuple):
            mk = mk[0]
        if not mk or (isinstance(mk, str) and not mk.strip()):
            return None
        return mk
    '''

    def _model_key_for(self, title: str) -> Optional[str]:
        try:
            mk = normalise_model(title=title, attrs={}, source=self.DOMAIN)
        except Exception as exc:
            logger.warning("[%s] model-key classification failed for %r: %s", self.DOMAIN, title, exc)
            return None
        if not mk or mk == "unknown":
            return None
        return mk

    def _is_relevant(self, row: dict[str, Any]) -> bool:
        return True

    # ------------------------------------------------------------------
    # DB flushing
    # ------------------------------------------------------------------
    def _maybe_flush(self):
        n_list = len(self._batch_buffer)
        n_hist = len(self._ph_buffer)
        n_total = n_list + n_hist
        due_by_size = n_list >= self.FLUSH_EVERY or n_hist >= (self.FLUSH_EVERY * 2)
        due_by_time = (time.time() - self._last_flush) >= self.FLUSH_SECONDS
        if (due_by_size or (due_by_time and n_total >= self.MIN_TIME_BATCH)) and n_total:
            self.flush_batch()

    def _filter_to_this_seller(self, collected: list[dict]) -> list[dict]:
        expected = getattr(self, "SELLER_USERNAME", None)
        if not expected:
            return collected
        expected_norm = expected.lower().strip()
        filtered: list[dict] = []
        for item in collected:
            seller_norm = (item.get("seller_username") or "").lower().strip()
            if seller_norm == expected_norm:
                filtered.append(item)
            else:
                logger.debug(
                    "[%s] dropped foreign seller '%s' (kept only '%s') for title=%r",
                    getattr(self, "DOMAIN", "?"),
                    seller_norm,
                    expected_norm,
                    item.get("title"),
                )
        return filtered

    def flush_batch(self):
        if not self._batch_buffer and not self._ph_buffer:
            return

        t0 = perf_counter()
        n_list = len(self._batch_buffer)
        n_hist = len(self._ph_buffer)

        try:
            if n_list:
                deduped: Dict[str, Dict[str, Any]] = {}
                for row in self._batch_buffer:
                    ext_id = row.get("external_id")
                    deduped[ext_id] = row
                safe_rows = list(deduped.values())

                bulk_upsert_auction_listings(safe_rows)
                self._batch_buffer.clear()

            if n_hist:
                try:
                    bulk_append_price_history(self._ph_buffer)
                except Exception as e:
                    logger.warning(f"[{self.DOMAIN}] bulk price_history failed: {e}")
                finally:
                    self._ph_buffer.clear()

        finally:
            dt = perf_counter() - t0
            self._last_flush = time.time()
            logger.info(
                f"[{self.DOMAIN}] bulk flush in {dt:.3f}s "
                f"(listings={n_list}, price_history={n_hist})"
            )

    # ------------------------------------------------------------------
    # eBay API helpers
    # ------------------------------------------------------------------
    def _build_headers(self, token: str) -> dict[str, str]:
        return {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "Accept": "application/json",
            "X-EBAY-C-MARKETPLACE-ID": "EBAY_GB",
        }

    def _fetch_category_items(
            self,
            token: str,
            category_id: int,
            sale_type: str | None = None,
            limit: int = 50,
    ) -> list[dict[str, Any]]:
        base = os.getenv("EBAY_API_BASE", "").rstrip("/")
        if not base:
            logger.error(f"[{self.DOMAIN}] EBAY_API_BASE missing in env")
            return []

        buying_option = None
        if sale_type == "bin":
            buying_option = "FIXED_PRICE"
        elif sale_type == "auction":
            buying_option = "AUCTION"

        queries_by_category = getattr(self, "SEARCH_QUERIES_BY_CATEGORY", {}) or {}
        queries = list(queries_by_category.get(category_id) or getattr(self, "SEARCH_QUERIES", []) or [])
        if not queries:
            queries = [self.SEARCH_QUERY or DEFAULT_Q]

        all_items: list[dict[str, Any]] = []
        seen_ids: set[str] = set()

        for query in queries:
            qs = [
                f"q={quote_plus(query or DEFAULT_Q)}",
                f"limit={limit}",
                f"sort={_search_sort_for_sale_type(sale_type)}",
            ]
            if category_id:
                qs.append(f"category_ids={category_id}")
            if buying_option:
                qs.append(f"filter=buyingOptions:{{{buying_option}}}")

            url = f"{base}/buy/browse/v1/item_summary/search?" + "&".join(qs)

            t_api_start = perf_counter()
            try:
                r = requests.get(url, headers=self._build_headers(token), timeout=10)
            except Exception as e:
                logger.warning(
                    f"[{self.DOMAIN}] API request failed cat={category_id} query={query!r} ({sale_type}): {e}"
                )
                continue

            self._hist_api.append(perf_counter() - t_api_start)
            if r.status_code != 200:
                logger.warning(
                    f"[{self.DOMAIN}] API cat={category_id} query={query!r} ({sale_type}) status {r.status_code}: {r.text[:200]}"
                )
                continue

            increment_api_usage("ebay_base_fetch_category_items_v1")

            try:
                payload = r.json()
            except Exception as e:
                logger.warning(
                    f"[{self.DOMAIN}] bad JSON cat={category_id} query={query!r} ({sale_type}): {e}"
                )
                continue

            items = payload.get("itemSummaries") or payload.get("item_summary") or []
            if not isinstance(items, list):
                logger.warning(
                    f"[{self.DOMAIN}] unexpected payload for cat={category_id} query={query!r} ({sale_type})"
                )
                continue

            for item in items:
                item_id = get_item_id(item)
                if not item_id or item_id in seen_ids:
                    continue
                seen_ids.add(item_id)
                all_items.append(item)

        return all_items

    def _fetch_seller_items(
            self,
            token: str,
            seller_username: str,
            sale_type: str | None = None,
            limit: int = 50,
    ) -> list[dict[str, Any]]:
        base = os.getenv("EBAY_API_BASE", "").rstrip("/")
        if not base:
            logger.error(f"[{self.DOMAIN}] EBAY_API_BASE missing in env")
            return []

        buying_opt = None
        if sale_type == "bin":
            buying_opt = "FIXED_PRICE"
        elif sale_type == "auction":
            buying_opt = "AUCTION"

        window_days_options = [60, 14, 7]

        all_items: list[dict[str, Any]] = []
        seen_ids: set[str] = set()

        for days in window_days_options:
            start_iso = _iso_z(datetime.now(timezone.utc))
            end_iso = _iso_z(datetime.now(timezone.utc) + timedelta(days=days))

            filter_bits = [
                f"sellers:{{{seller_username}}}",
                f"itemEndDate:[{start_iso}..{end_iso}]",
                "itemLocationCountry:GB",
            ]
            if buying_opt:
                filter_bits.append(f"buyingOptions:{{{buying_opt}}}")
            filter_param = ",".join(filter_bits)

            offset = 0
            page_count = 0

            while True:
                qs = [
                    f"q={quote_plus(self.SEARCH_QUERY or DEFAULT_Q)}",
                    f"filter={filter_param}",
                    f"limit={limit}",
                    f"offset={offset}",
                    f"sort={_search_sort_for_sale_type(sale_type)}",
                ]
                url = f"{base}/buy/browse/v1/item_summary/search?" + "&".join(qs)

                t_api_start = perf_counter()
                try:
                    r = requests.get(url, headers=self._build_headers(token), timeout=10)
                except Exception as e:
                    logger.warning(
                        f"[{self.DOMAIN}] API request failed seller={seller_username} ({sale_type}): {e}"
                    )
                    break

                self._hist_api.append(perf_counter() - t_api_start)

                if r.status_code != 200:
                    txt = (r.text or "").lower()
                    logger.warning(
                        f"[{self.DOMAIN}] API seller={seller_username} status {r.status_code}: {r.text[:200]}"
                    )
                    if r.status_code == 400 and ("too large" in txt or "too many" in txt):
                        break
                    else:
                        return all_items

                increment_api_usage("ebay_base_seller_items_v1")

                try:
                    payload = r.json()
                except Exception as e:
                    logger.warning(f"[{self.DOMAIN}] bad JSON seller={seller_username}: {e}")
                    break

                items = payload.get("itemSummaries") or payload.get("item_summary") or []
                if not isinstance(items, list) or not items:
                    break

                new_batch = []
                for it in items:
                    iid = it.get("itemId")
                    if not iid or iid in seen_ids:
                        continue
                    seen_ids.add(iid)
                    new_batch.append(it)

                if not new_batch:
                    break

                all_items.extend(new_batch)

                page_count += 1
                if page_count >= getattr(self, "MAX_PAGES", 10):
                    logger.info(
                        f"[{self.DOMAIN}] seller={seller_username} reached page cap ({page_count})"
                    )
                    break
                if len(all_items) >= getattr(self, "MAX_LISTINGS", 1000):
                    logger.info(
                        f"[{self.DOMAIN}] seller={seller_username} reached MAX_LISTINGS"
                    )
                    break

                offset += limit
                time.sleep(0.25)

            if all_items:
                break

        return all_items

    def _mark_404_listings_unknown(self, external_ids: Set[str]) -> None:
        """
        For listings that returned 404 from getItem, mark them as unknown in DB
        so Trading can decide whether they are live, sold, ended, or deleted.
        """
        if not external_ids:
            return

        from utils.db_schema import get_connection  # if not already imported at top

        conn = get_connection()
        ids_list = list(external_ids)

        try:
            with conn, conn.cursor() as cur:
                ensure_utc_session(cur)
                cur.execute(
                    """
                    UPDATE auction_listings
                    SET status   = 'unknown',
                        end_time = (now() AT TIME ZONE 'utc')
                    WHERE external_id = ANY(%s)
                    """,
                    (ids_list,),
                )
            logger.info(
                "[%s] marked %d listings as unknown due to 404 from getItem",
                self.DOMAIN,
                len(ids_list),
            )
        except Exception as e:
            logger.warning(
                "[%s] failed to mark 404 listings unknown (ids=%s): %s",
                self.DOMAIN,
                ids_list,
                e,
            )

    def _mark_unavailable_listings_unknown(
            self,
            external_ids: Set[str],
            *,
            reason: str = "browse_unavailable",
    ) -> None:
        """
        Remove Browse-confirmed ended/out-of-stock rows from live candidate flow.

        We do not mark them sold here. Trading/final-price backfill owns the
        final outcome; this only prevents dead rows from reaching the email.
        """
        if not external_ids:
            return

        conn = get_connection()
        ids_list = list(external_ids)

        try:
            with conn, conn.cursor() as cur:
                ensure_utc_session(cur)
                cur.execute(
                    """
                    UPDATE auction_listings
                    SET status   = 'unknown',
                        end_time = (now() AT TIME ZONE 'utc')
                    WHERE external_id = ANY(%s)
                    """,
                    (ids_list,),
                )
            logger.info(
                "[%s] marked %d listings unknown after getItem %s",
                self.DOMAIN,
                len(ids_list),
                reason,
            )
        except Exception as e:
            logger.warning(
                "[%s] failed to mark unavailable listings unknown (ids=%s): %s",
                self.DOMAIN,
                ids_list,
                e,
            )

    def _fetch_items_by_ids(
            self,
            token: str,
            external_ids: List[str],
            chunk_size: int = 20,
    ) -> List[Dict[str, Any]]:
        """
        Fetch full item details for a list of REST item IDs using the Browse getItem API.

        Used by the pph (price-per-hour) pipeline to refresh prices/bids for
        already-known listings without doing full discovery.
        """
        base = os.getenv("EBAY_API_BASE", "").rstrip("/")
        if not base:
            logger.error(f"[{self.DOMAIN}] EBAY_API_BASE missing in env")
            return []

        ids = [str(i) for i in external_ids if i]
        if not ids:
            return []

        all_items: List[Dict[str, Any]] = []
        seen_ids: Set[str] = set()
        not_found_ids: Set[str] = set()

        for eid in ids:
            if eid in seen_ids:
                continue
            seen_ids.add(eid)

            url = f"{base}/buy/browse/v1/item/{eid}"

            t_api_start = perf_counter()
            try:
                r = requests.get(url, headers=self._build_headers(token), timeout=10)
            except Exception as e:
                logger.warning(
                    "[%s] getItem request failed for ID %s: %s",
                    self.DOMAIN,
                    eid,
                    e,
                )
                continue

            self._hist_api.append(perf_counter() - t_api_start)

            # 404 == listing gone (ended/cancelled)
            if r.status_code == 404:
                logger.info("[%s] getItem 404 (ended?) for ID %s", self.DOMAIN, eid)
                not_found_ids.add(eid)
                continue

            if r.status_code != 200:
                logger.warning(
                    "[%s] getItem status %s for ID %s: %s",
                    self.DOMAIN,
                    r.status_code,
                    eid,
                    (r.text or "")[:200],
                )
                continue

            increment_api_usage("ebay_base_fetch_items_by_id_v1")

            try:
                item = r.json()
            except Exception as e:
                logger.warning("[%s] bad JSON from getItem for ID %s: %s", self.DOMAIN, eid, e)
                continue

            all_items.append(item)
            time.sleep(0.05)

        # After we are done, mark any 404'd listings as unknown in DB.
        if not_found_ids:
            self._mark_404_listings_unknown(not_found_ids)

        return all_items

    def _normalize_item(self, raw: dict[str, Any], sale_type: str):
        unavailable_reason = _browse_item_unavailable_reason(raw)
        if unavailable_reason:
            logger.info(
                "[%s] skipping unavailable listing itemId=%s reason=%s title=%r",
                self.DOMAIN,
                get_item_id(raw),
                unavailable_reason,
                raw.get("title"),
            )
            return None

        if is_configurable_item(raw):
            logger.info(
                "[%s] skipping configurable/multi-variation listing itemId=%s title=%r",
                self.DOMAIN,
                get_item_id(raw),
                raw.get("title"),
            )
            return None

        item_id = raw.get("itemId")

        title = raw.get("title") or ""
        requested_sale_type = str(sale_type or "")
        buying_opts = raw.get("buyingOptions") or []
        sale_type = _sale_type_from_buying_options(buying_opts, fallback=requested_sale_type)
        seller_info = raw.get("seller") or {}
        seller_username = seller_info.get("username")

        price_info = raw.get("price") or {}
        bid_info = raw.get("currentBidPrice") or {}

        price_value = price_info.get("value")
        bid_value = bid_info.get("value")

        web_url = raw.get("itemWebUrl") or raw.get("itemUrl") or ""
        end_time = _parse_iso_utc(raw.get("itemEndDate"))
        time_left_s = _secs_left(end_time)

        raw_bids = raw.get("bidCount")
        try:
            bids_count = int(raw_bids) if raw_bids is not None else 0
        except Exception:
            bids_count = 0

        if requested_sale_type == "bin" and sale_type == "auction":
            return None
        if requested_sale_type == "auction" and sale_type == "bin":
            return None

        title_lower = title.lower()
        model_key = self._model_key_for(title)

        def _to_int(val) -> Optional[int]:
            if val is None:
                return None
            try:
                return int(round(float(str(val))))
            except Exception:
                return None

        price_bid_current_int: Optional[int] = None
        price_current_int: Optional[int] = None

        if sale_type in {"auction", "both"}:
            price_bid_current_int = _to_int(bid_value)
            if price_bid_current_int is not None:
                price_current_int = price_bid_current_int
            else:
                price_current_int = _to_int(price_value)
        else:
            price_current_int = _to_int(price_value)

        row = {
            "source": self._source_name,
            "external_id": item_id,
            "title": title[:255],
            "price_current": price_current_int or 0,
            "price_bid_current": price_bid_current_int,
            "bids_count": bids_count,
            "end_time": end_time,
            "url": web_url[:1024],
            "sale_type": sale_type,
            "roi_estimate": None,
            "max_bid": None,
            "notes": '',
            "source_id": self._source_id,
            "model_key": model_key,
            "time_left_s": time_left_s,
            "status": "live",
            "seller_username": (seller_username or "").strip()[:255],
        }
        browse_attrs = _browse_description_attrs(raw)
        if browse_attrs:
            row["raw_attrs"] = browse_attrs
            row["condition_grade"] = _derive_condition_grade(browse_attrs, title)

        ph = (item_id, price_current_int or 0, bids_count)
        return row, ph

    # ------------------------------------------------------------------
    # Public entry: full scrape
    # ------------------------------------------------------------------
    def fetch_listings_api(self, ebay_token: str) -> None:
        sale_types = (
            self.SALE_TYPE if isinstance(self.SALE_TYPE, (list, tuple)) else [self.SALE_TYPE]
        )
        sale_types = [str(s) for s in sale_types if s]
        unavailable_ids: Set[str] = set()

        # SELLER MODE
        if self.FETCH_MODE == "seller":
            seller = self.SELLER_USERNAME
            if not seller:
                logger.warning(f"[{self.DOMAIN}] seller mode but no SELLER_USERNAME set")
                return

            for sale_type in sale_types:
                items = self._fetch_seller_items(ebay_token, seller, sale_type)
                if not items:
                    logger.info(f"[{self.DOMAIN}] seller {seller} {sale_type}: 0 items")
                    continue

                added = 0
                for raw in items:
                    unavailable_reason = _browse_item_unavailable_reason(raw)
                    if unavailable_reason:
                        item_id = get_item_id(raw)
                        if item_id != "unknown":
                            unavailable_ids.add(item_id)
                        logger.info(
                            "[%s] seller fetch: marking ID %s non-live (%s)",
                            self.DOMAIN,
                            item_id,
                            unavailable_reason,
                        )
                        continue

                    norm = self._normalize_item(raw, sale_type)
                    if not norm:
                        continue
                    row, ph = norm

                    if row["seller_username"].lower().strip() != seller.lower().strip():
                        logger.debug(
                            "[%s] skipping foreign seller '%s' (wanted '%s') itemId=%s title=%r",
                            self.DOMAIN,
                            row["seller_username"],
                            seller,
                            row.get("external_id"),
                            row.get("title"),
                        )
                        continue

                    if not self._is_relevant(row):
                        continue

                    self._batch_buffer.append(row)
                    if row["price_current"]:
                        self._ph_buffer.append(ph)
                    added += 1
                    self._maybe_flush()

                logger.info(
                    f"[{self.DOMAIN}] seller {seller} {sale_type}: {added} listings"
                )

            self.flush_batch()
            self._mark_disallowed_live_sale_types(sale_types)
            if unavailable_ids:
                self._mark_unavailable_listings_unknown(unavailable_ids)
            return

        # CATEGORY MODE
        for cat_id in self.CATEGORY_IDS:
            for sale_type in sale_types:
                items = self._fetch_category_items(ebay_token, cat_id, sale_type)
                if not items:
                    logger.info(f"[{self.DOMAIN}] cat {cat_id} {sale_type}: 0 items")
                    time.sleep(self.CATEGORY_PAUSE_SECONDS)
                    continue

                added = 0
                for raw in items:
                    unavailable_reason = _browse_item_unavailable_reason(raw)
                    if unavailable_reason:
                        item_id = get_item_id(raw)
                        if item_id != "unknown":
                            unavailable_ids.add(item_id)
                        logger.info(
                            "[%s] category fetch: marking ID %s non-live (%s)",
                            self.DOMAIN,
                            item_id,
                            unavailable_reason,
                        )
                        continue

                    norm = self._normalize_item(raw, sale_type)
                    if not norm:
                        continue
                    row, ph = norm

                    if not self._is_relevant(row):
                        continue

                    self._batch_buffer.append(row)
                    if row["price_current"]:
                        self._ph_buffer.append(ph)
                    added += 1
                    self._maybe_flush()

                logger.info(f"[{self.DOMAIN}] cat {cat_id} {sale_type}: {added} listings")
                time.sleep(self.CATEGORY_PAUSE_SECONDS)

        self.flush_batch()
        self._mark_disallowed_live_sale_types(sale_types)
        if unavailable_ids:
            self._mark_unavailable_listings_unknown(unavailable_ids)

    def _mark_disallowed_live_sale_types(self, allowed_sale_types: list[str]) -> None:
        """
        Keep full source scrapes aligned with their configured live market.

        If an adapter is switched from BIN+auction to auction-only, previously
        seen BIN rows should stop appearing as live candidates immediately after
        the next successful scrape. They are not ended; they are simply outside
        the source's current live collection contract.
        """
        allowed_set = {s for s in allowed_sale_types if s}
        if {"auction", "bin"} & allowed_set:
            allowed_set.add("both")
        allowed = tuple(sorted(allowed_set))
        if not allowed:
            return

        conn = get_connection()
        with conn, conn.cursor() as cur:
            ensure_utc_session(cur)
            cur.execute(
                """
                UPDATE auction_listings
                SET status = 'unknown'
                WHERE source = %s
                  AND status = 'live'
                  AND (
                        sale_type IS NULL
                        OR sale_type <> ALL(%s)
                  )
                """,
                (self._source_name, list(allowed)),
            )
            changed = cur.rowcount

        if changed:
            logger.info(
                "[%s] marked %d live rows unknown because sale_type is outside %s",
                self.DOMAIN,
                changed,
                ", ".join(allowed),
            )

    # ------------------------------------------------------------------
    # Public entry: targeted price refresh (PPH)
    # ------------------------------------------------------------------
    def refresh_items_price(self, ebay_token: str, external_ids: List[str]) -> None:
        """
        Targeted price/bid refresh for already-known listings.

        Used by the pph (price-per-hour) pipeline:
          - ONLY hits listings we already know about (by external_id).
          - DOES NOT do discovery searches.
          - Reuses normalisation + bulk upsert + price history.
        """
        if not external_ids:
            logger.info("[%s] refresh_items_price called with empty ID list", self.DOMAIN)
            return

        items = self._fetch_items_by_ids(ebay_token, external_ids)
        if not items:
            logger.info(
                "[%s] refresh_items_price: 0 items returned for %d requested IDs",
                self.DOMAIN,
                len(external_ids),
            )
            return

        allowed_ids: Set[str] = {str(i) for i in external_ids if i}
        unavailable_ids: Set[str] = set()
        added = 0

        for raw in items:
            item_id = get_item_id(raw)
            if item_id not in allowed_ids:
                continue

            unavailable_reason = _browse_item_unavailable_reason(raw)
            if unavailable_reason:
                unavailable_ids.add(item_id)
                logger.info(
                    "[%s] refresh_items_price: marking ID %s non-live (%s)",
                    self.DOMAIN,
                    item_id,
                    unavailable_reason,
                )
                continue

            buying_opts = raw.get("buyingOptions") or []
            st = self.SALE_TYPE
            fallback_sale_type = (
                "auction"
                if isinstance(st, (list, tuple)) and "auction" in st
                else "bin"
                if isinstance(st, (list, tuple)) and "bin" in st
                else str(st[0])
                if isinstance(st, (list, tuple)) and st
                else str(st)
            )
            sale_type = _sale_type_from_buying_options(
                buying_opts,
                fallback=fallback_sale_type,
            )

            norm = self._normalize_item(raw, sale_type)
            if not norm:
                continue
            row, ph = norm

            if not self._is_relevant(row):
                continue

            self._batch_buffer.append(row)
            if row["price_current"]:
                self._ph_buffer.append(ph)
            added += 1
            self._maybe_flush()

        logger.info(
            "[%s] refresh_items_price: refreshed %d listings (requested=%d)",
            self.DOMAIN,
            added,
            len(external_ids),
        )

        self.flush_batch()
        if unavailable_ids:
            self._mark_unavailable_listings_unknown(unavailable_ids)


def ensure_utc_session(cur):
    try:
        cur.execute("SET TIME ZONE 'UTC'")
    except Exception:
        pass


def bulk_append_price_history(rows: list[tuple[str, int, int]]):
    """
    rows: (external_id, price, bids_count)
    """
    if not rows:
        return
    sql = """
        INSERT INTO auction_price_history (external_id, price, bids_count, recorded_at)
        VALUES %s
        ON CONFLICT DO NOTHING
    """
    conn = get_connection()  # Always get a live connection
    with conn, conn.cursor() as cur:
        ensure_utc_session(cur)
        cur.execute("SET LOCAL synchronous_commit TO OFF;")
        execute_values(
            cur,
            sql,
            rows,
            template="(%s, %s, %s, (now() AT TIME ZONE 'utc'))",
            page_size=500,
        )


def bulk_upsert_auction_listings(rows: list[dict]):
    """
    Bulk upsert of listings data from scrapers.
    Scraper semantics:
      - Every call represents "we have just seen these listings as ACTIVE on eBay".
      - This is the ONLY place that should bump last_seen_at.
    """
    if not rows:
        return

    # Consistent "seen alive" timestamp for this batch
    now = datetime.now(timezone.utc)

    # How long before a live listing becomes unknown if we stop seeing it.
    # Default: 180 minutes (3 hours), override with GF_LISTING_UNKNOWN_MINUTES if needed.
    try:
        unknown_minutes = int(os.getenv("GF_LISTING_UNKNOWN_MINUTES", "180"))
    except Exception:
        unknown_minutes = 180
    unknown_cutoff = now - timedelta(minutes=unknown_minutes)

    cols = [
        "source",
        "external_id",
        "title",
        "price_current",
        "bids_count",
        "end_time",
        "url",
        "sale_type",
        "roi_estimate",
        "max_bid",
        "notes",
        "source_id",
        "model_key",
        "raw_attrs",
        "condition_grade",
        "time_left_s",
        "status",
        "last_seen_at",
    ]

    values: list[tuple] = []
    batch_sources = sorted({str(r.get("source") or "") for r in rows if r.get("source")})
    for r in rows:
        row_vals: list[Any] = []
        for c in cols:
            if c == "last_seen_at":
                # Always set "seen alive" to this batch timestamp
                row_vals.append(now)
            elif c == "raw_attrs":
                raw_attrs = r.get(c)
                if isinstance(raw_attrs, (dict, list)):
                    row_vals.append(Json(raw_attrs))
                else:
                    row_vals.append(raw_attrs)
            else:
                row_vals.append(r.get(c))
        values.append(tuple(row_vals))

    sql = f"""
        INSERT INTO auction_listings ({", ".join(cols)})
        VALUES %s
        ON CONFLICT (external_id) DO UPDATE
        SET source        = EXCLUDED.source,
            title         = EXCLUDED.title,
            price_current = COALESCE(EXCLUDED.price_current, auction_listings.price_current),
            bids_count    = COALESCE(EXCLUDED.bids_count,    auction_listings.bids_count),
            end_time      = COALESCE(EXCLUDED.end_time,      auction_listings.end_time),
            url           = EXCLUDED.url,
            sale_type     = EXCLUDED.sale_type,
            roi_estimate  = EXCLUDED.roi_estimate,
            max_bid       = EXCLUDED.max_bid,
            notes         = EXCLUDED.notes,
            source_id     = EXCLUDED.source_id,
            model_key     = CASE
                              WHEN EXCLUDED.source = 'ebay-apple' AND auction_listings.source = EXCLUDED.source
                                AND auction_listings.title IS NOT DISTINCT FROM EXCLUDED.title
                                AND (auction_listings.model_key = 'unknown'
                                  OR jsonb_typeof(auction_listings.raw_attrs) = 'object')
                                THEN COALESCE(auction_listings.model_key, EXCLUDED.model_key)
                              WHEN EXCLUDED.source = 'ebay-apple'
                                AND auction_listings.title IS DISTINCT FROM EXCLUDED.title
                                THEN EXCLUDED.model_key
                              ELSE COALESCE(EXCLUDED.model_key, auction_listings.model_key)
                            END,
            raw_attrs     = CASE
                              WHEN EXCLUDED.raw_attrs IS NULL THEN auction_listings.raw_attrs
                              WHEN auction_listings.raw_attrs IS NULL
                                OR auction_listings.raw_attrs = 'false'::jsonb
                                THEN EXCLUDED.raw_attrs
                              ELSE auction_listings.raw_attrs || EXCLUDED.raw_attrs
                            END,
            condition_grade = COALESCE(EXCLUDED.condition_grade, auction_listings.condition_grade),
            time_left_s   = COALESCE(EXCLUDED.time_left_s,   auction_listings.time_left_s),
            status        = COALESCE(EXCLUDED.status,        auction_listings.status),
            last_seen_at  = EXCLUDED.last_seen_at
    """
    conn = get_connection()  # Always get a live connection
    with conn, conn.cursor() as cur:
        ensure_utc_session(cur)
        cur.execute("SET LOCAL synchronous_commit TO OFF;")

        # 1) Upsert this batch – everything in here is "seen alive" right now
        execute_values(cur, sql, values, page_size=250)

        # 2) If a source stops seeing an old live listing whose end time has
        #    passed, move it to unknown so final-price backfill can resolve it.
        #    Do not demote future-dated rows; they may simply be missed by a
        #    narrow seeded search and can still be active on eBay.
        if batch_sources:
            cur.execute(
                """
                UPDATE auction_listings
                SET status = 'unknown'
                WHERE status = 'live'
                  AND source = ANY(%s)
                  AND (
                        last_seen_at IS NULL
                        OR last_seen_at < %s
                  )
                  AND (
                        end_time IS NULL
                        OR end_time <= (now() AT TIME ZONE 'utc')
                  )
                """,
                (batch_sources, unknown_cutoff),
            )
