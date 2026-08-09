from __future__ import annotations

from typing import Any, Optional

from pipelines.listing.attributes.mk import normalise_model

from .ebay_base import EbayAdapterBase


class Adapter(EbayAdapterBase):
    DOMAIN = "ebay-graphics-cards"

    CATEGORY_IDS = [
        27386,  # Graphics / Video Cards
    ]

    SALE_TYPE = ["bin", "auction"]
    SEARCH_QUERY = "graphics card"
    SEARCH_QUERIES = [
        "rtx 4070",
        "rtx 4070 ti",
        "rtx 4070 ti super",
        "rtx 4080",
        "rtx 4090",
        "rtx 3080",
        "rx 7800 xt",
        "rx 7900 xt",
        "rx 7900 xtx",
        "intel arc b580",
        "quadro rtx 4000",
        "quadro rtx 5000",
        "quadro rtx 6000",
        "quadro rtx 8000",
        "rtx a2000",
        "rtx a4000",
        "rtx a4500",
        "rtx a5000",
        "rtx a6000",
        "rtx 4000 ada",
        "rtx 4500 ada",
        "rtx 5000 ada",
        "rtx 6000 ada",
        "radeon pro w5700",
        "radeon pro w6800",
        "radeon pro w7500",
        "radeon pro w7600",
        "radeon pro w7800",
        "radeon pro w7900",
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
                "heatsink only",
                "fan only",
                "replacement fan",
                "backplate",
                "bracket",
                "riser",
                "water block",
                "waterblock",
                "mining rig",
                "laptop gpu",
                "mobile gpu",
                "mxm",
                "sxm",
                "faulty",
                "untested",
                "spares",
                "for parts",
                "for repair",
                "repair service",
                "no display",
                "no signal",
                "artifact",
                "artifacts",
                "job lot",
                "bundle",
            )
        ):
            return False
        return bool(row.get("model_key"))
