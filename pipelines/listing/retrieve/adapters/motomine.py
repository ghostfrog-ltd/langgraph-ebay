from __future__ import annotations

from typing import Any, Optional

from .ebay_base import EbayAdapterBase


class Adapter(EbayAdapterBase):
    DOMAIN = "motomine"
    FETCH_MODE = "seller"
    SELLER_USERNAME = "motomine"
    SALE_TYPE = "auction"
    SEARCH_QUERY = "spares repair"
    MAX_PAGES = 4
    MAX_LISTINGS = 250

    def __init__(self, max_pages: int = 4, delay: float = 2.0):
        super().__init__()
        self.max_pages = max_pages
        self.delay = delay
        self.MAX_PAGES = max_pages

    def _resolve_source(self) -> tuple[str, Optional[int]]:
        _source_name, source_id = super()._resolve_source()
        return self.DOMAIN, source_id

    def _normalize_item(self, raw: dict[str, Any], sale_type: str):
        norm = super()._normalize_item(raw, sale_type)
        if not norm:
            return None

        row, ph = norm
        legacy_id = raw.get("legacyItemId")
        if legacy_id:
            external_id = str(legacy_id)
            row["external_id"] = external_id
            ph = (
                external_id,
                ph[1],
                ph[2],
            )

        row["source"] = self.DOMAIN
        return row, ph

    def refresh_items_price(self, ebay_token: str, external_ids: list[str]) -> None:
        browse_ids = [
            str(eid) if "|" in str(eid) else f"v1|{eid}|0"
            for eid in external_ids
            if eid
        ]
        super().refresh_items_price(ebay_token, browse_ids)
