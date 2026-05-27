from __future__ import annotations

import re
from typing import Any

from .ebay_base import EbayAdapterBase


class Adapter(EbayAdapterBase):
    DOMAIN = "ebay-mini-cooper"

    # Cars + generic vehicle parts/accessories.
    CATEGORY_IDS = [
        9801,  # Cars, Motorcycles & Vehicles
        6030,  # Vehicle Parts & Accessories
    ]

    SALE_TYPE = ["bin", "auction"]
    SEARCH_QUERY = "mini cooper"

    def _is_relevant(self, row: dict[str, Any]) -> bool:
        title = (row.get("title") or "").lower().strip()
        model_key = (row.get("model_key") or "").lower().strip()

        negative_terms = (
            "diecast",
            "die-cast",
            "1:18",
            "1/18",
            "1:24",
            "1/24",
            "1:43",
            "slot car",
            "hot wheels",
            "matchbox",
            "scalextric",
            "corgi",
            "rc car",
            "r/c car",
            "remote control",
            "model car",
            "toy car",
            "poster",
            "brochure",
            "owners manual",
            "owner's manual",
            "workshop manual",
            "haynes manual",
            "key ring",
            "keyring",
            "sticker",
            "decal",
            "lego",
        )
        if any(term in title for term in negative_terms):
            return False

        if model_key.startswith("mini-"):
            return True

        if "mini" not in title:
            return False

        strong_family_terms = (
            "cooper",
            "john cooper works",
            "jcw",
            "countryman",
            "clubman",
            "paceman",
            "roadster",
            "convertible",
            "hatch",
            "mini one",
        )
        generation_terms = (
            "r50", "r52", "r53", "r55", "r56", "r57", "r58", "r59", "r60", "r61",
            "f54", "f55", "f56", "f57", "f60",
        )
        part_terms = (
            "bumper",
            "headlight",
            "headlamp",
            "tail light",
            "taillight",
            "mirror",
            "wing mirror",
            "door card",
            "bonnet",
            "bootlid",
            "tailgate",
            "gearbox",
            "engine",
            "ecu",
            "alloy wheel",
            "alloy wheels",
            "wheel",
            "wheels",
            "seat",
            "steering wheel",
            "breaking",
            "for parts",
            "spares or repair",
        )

        if any(term in title for term in strong_family_terms):
            return True
        if any(term in title for term in generation_terms):
            return True
        if any(term in title for term in part_terms) and "cooper" in title:
            return True

        return bool(re.search(r"\bmini\b.*\b(1\.4|1\.5|1\.6|2\.0|one|cooper)\b", title))
