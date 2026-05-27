from __future__ import annotations

from typing import Any

from .ebay_base import EbayAdapterBase


class Adapter(EbayAdapterBase):
    DOMAIN = "ebay-honda-msx125"

    # Motorcycles & Scooters. Keep this narrow so the search does not drown
    # in parts/accessories.
    CATEGORY_IDS = [179753]
    SALE_TYPE = ["bin", "auction"]
    SEARCH_QUERY = "honda msx 125 grom"

    def _is_relevant(self, row: dict[str, Any]) -> bool:
        title = (row.get("title") or "").lower()

        if "honda" not in title:
            return False

        markers = (
            "msx125",
            "msx 125",
            "msx-125",
            "grom",
        )
        return any(marker in title for marker in markers)
