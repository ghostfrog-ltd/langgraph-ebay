from __future__ import annotations

from typing import Any, Optional

from pipelines.listing.attributes.mk import normalise_model

from .ebay_base import EbayAdapterBase


class Adapter(EbayAdapterBase):
    DOMAIN = "ebay-motherboards"

    CATEGORY_IDS = [
        None,  # exact search seeds outperform stale marketplace category IDs
    ]

    SALE_TYPE = ["bin", "auction"]
    SEARCH_QUERY = "motherboard"
    SEARCH_QUERIES = [
        "b650 motherboard",
        "b650e motherboard",
        "x670e motherboard",
        "z790 motherboard",
        "z890 motherboard",
        "b760 motherboard",
        "asus rog strix b650e",
        "msi b650 tomahawk",
        "gigabyte b650 aorus",
        "asrock b650 motherboard",
    ]

    def _model_key_for(self, title: str) -> Optional[str]:
        key = normalise_model(title=title, attrs={}, source=self.DOMAIN)
        return None if key == "unknown" else key

    def _is_relevant(self, row: dict[str, Any]) -> bool:
        title = (row.get("title") or "").lower()
        if any(
            term in title
            for term in (
                "box only",
                "empty box",
                "i/o shield only",
                "io shield only",
                "wifi antenna only",
                "antenna only",
                "bios chip",
                "repair service",
                "laptop motherboard",
                "server motherboard",
                "oem motherboard",
                "cpu included",
                "processor included",
                "ram included",
                "cpu bundle",
                "processor bundle",
                "ram bundle",
                "faulty",
                "damaged",
                "untested",
                "spares",
                "for parts",
                "for repair",
                "bent pins",
                "damaged socket",
                "no post",
                "liquid damage",
                "job lot",
                "bundle",
            )
        ):
            return False
        return bool(row.get("model_key"))
