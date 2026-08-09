from __future__ import annotations

from typing import Any, Optional

from pipelines.listing.attributes.mk import normalise_model

from .ebay_base import EbayAdapterBase


class Adapter(EbayAdapterBase):
    DOMAIN = "ebay-camera-lenses"

    CATEGORY_IDS = [
        3323,  # Camera Lenses & Filters
    ]

    SALE_TYPE = ["bin", "auction"]
    SEARCH_QUERY = "camera lens"
    SEARCH_QUERIES = [
        "Sony 70-200 GM II",
        "Sony FE 85mm 1.4 GM",
        "Canon RF 85mm F1.2 L",
        "Canon RF 70-200 F2.8 L",
        "Nikon Z 70-200 F2.8 S",
        "Fujifilm GF 110mm F2",
        "Fujifilm XF 50-140 F2.8",
        "Leica Summicron M 35mm",
        "Hasselblad XCD 80mm",
        "Sigma 85mm F1.4 DG DN Art",
        "Tamron 35-150 F2-2.8 Sony",
        "Tamron 70-180 F2.8 Sony",
    ]

    def _model_key_for(self, title: str) -> Optional[str]:
        key = normalise_model(title=title, attrs={}, source=self.DOMAIN)
        return None if key == "unknown" else key

    def _is_relevant(self, row: dict[str, Any]) -> bool:
        title = (row.get("title") or "").lower()
        if any(
            term in title
            for term in (
                "cap only",
                "lens cap",
                "rear cap",
                "body cap",
                "hood only",
                "lens hood",
                "filter",
                "adapter",
                "mount adapter",
                "case only",
                "pouch only",
                "box only",
                "empty box",
                "tripod collar",
                "collar only",
                "repair service",
                "for parts",
                "for repair",
                "spares",
                "faulty",
                "fungus",
                "haze",
                "scratched glass",
                "job lot",
                "bundle",
            )
        ):
            return False
        return bool(row.get("model_key"))
