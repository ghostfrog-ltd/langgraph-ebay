from __future__ import annotations

from typing import Any, Optional

from pipelines.listing.attributes.mk import normalise_model

from .ebay_base import EbayAdapterBase


class Adapter(EbayAdapterBase):
    DOMAIN = "ebay-cpus"

    CATEGORY_IDS = [
        164,  # CPUs / Processors
    ]

    SALE_TYPE = ["bin", "auction"]
    SEARCH_QUERY = "cpu"
    SEARCH_QUERIES = [
        "ryzen 5 7600x",
        "ryzen 7 7700x",
        "ryzen 7 7800x3d",
        "ryzen 9 7900x",
        "ryzen 9 7950x",
        "intel i5 14600k",
        "intel i7 14700k",
        "intel i7 14700kf",
        "intel i9 14900k",
        "intel i9 14900kf",
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
                "cooler only",
                "stock cooler only",
                "heatsink only",
                "keyring",
                "engineering sample",
                "qualification sample",
                "xeon",
                "epyc",
                "server",
                "laptop",
                "notebook",
                "mobile",
                "compatible with",
                "processor not included",
                "faulty",
                "untested",
                "spares",
                "for parts",
                "for repair",
                "bent pins",
                "missing pins",
                "damaged pins",
                "job lot",
                "bundle",
            )
        ):
            return False
        return bool(row.get("model_key"))
