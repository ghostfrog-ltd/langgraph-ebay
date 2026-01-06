from __future__ import annotations
import re
from typing import Mapping, Any, Optional
from utils.condition import _derive_condition_grade


def _as_text(val: Any) -> str:
    if val is None: return ""
    return str(val).strip().lower()


def _extract_storage(attrs: Mapping[str, Any], title: str) -> str:
    """
    Extracts capacity to differentiate models like Xbox Series S 512GB vs 1TB.
    """
    # Scan Storage Capacity and Size attributes
    attr_storage = _as_text(attrs.get("Storage Capacity")) + _as_text(attrs.get("Size"))
    search_text = f"{attr_storage} {title.lower()}"

    if re.search(r'2\s?tb', search_text): return "2tb"
    if re.search(r'1\s?tb', search_text): return "1tb"
    if re.search(r'825\s?gb', search_text): return "825gb"  # Base PS5 Spec
    if re.search(r'512\s?gb', search_text): return "512gb"
    if re.search(r'500\s?gb', search_text): return "500gb"
    if re.search(r'128\s?gb', search_text): return "128gb"
    return ""


def _is_console_type(attrs: Mapping[str, Any]) -> bool:
    t = _as_text(attrs.get("Type"))
    model = _as_text(attrs.get("Model"))

    # Negative signals from the dataset
    exclude = {"controller", "remote player", "portal", "disc drive", "adapter", "pedals", "wheel"}
    if any(x in t for x in exclude) or any(x in model for x in exclude):
        return False

    include = {"console", "handheld system", "gaming system"}
    return any(x in t for x in include)


def console_or_game_model_key(attrs: Mapping[str, Any], title: str) -> Optional[str]:
    if not attrs or not _is_console_type(attrs):
        return "unknown"

    brand = _as_text(attrs.get("Brand"))
    model = _as_text(attrs.get("Model"))
    blob = f"{brand} {model} {title.lower()}"
    base_key = "unknown"

    # Mapping Logic
    if "playstation" in blob or "ps5" in blob or "ps4" in blob:
        if "ps5" in blob:
            base_key = "ps5"
        elif "ps4" in blob:
            base_key = "ps4"
        elif "ps3" in blob:
            base_key = "ps3"
        elif "vita" in blob:
            base_key = "ps_vita"
    elif "xbox" in blob:
        if "series" in blob:
            base_key = "xbox_series"
        elif "one" in blob:
            base_key = "xbox_one"
        elif "360" in blob:
            base_key = "xbox_360"
    elif "switch" in blob:
        base_key = "switch_2" if "switch 2" in blob else "switch"

    if base_key == "unknown": return "unknown"

    # Append storage for modern units
    if base_key in {"ps5", "xbox_series", "xbox_one", "switch"}:
        storage = _extract_storage(attrs, title)
        if storage: base_key += f"_{storage}"

    grade = _derive_condition_grade(attrs, title)
    return f"{base_key}_{grade}"