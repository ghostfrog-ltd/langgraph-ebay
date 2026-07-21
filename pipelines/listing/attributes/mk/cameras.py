from __future__ import annotations
import re
from typing import Mapping, Any, Optional
from utils.condition import _derive_condition_grade


def _clean(s: Any) -> str:
    return str(s).strip().lower() if s else ""


def _is_action_cam(attrs: Mapping[str, Any], title: str = "") -> bool:
    t = _clean(attrs.get("Type"))
    model = _clean(attrs.get("Model"))
    title_low = title.lower()
    # Exclude drones and accessories found in data
    exclude = {"drone", "mavic", "mount", "strap", "case", "charger"}
    title_exclude = (
        "lens mod",
        "max lens",
        "media mod",
        "charger only",
        "battery only",
        "case only",
        "mount only",
    )
    if (
        any(x in t for x in exclude)
        or any(x in model for x in exclude)
        or any(x in title_low for x in title_exclude)
    ):
        return False
    if re.search(r"\b(gopro|hero\s*\d{1,2}|osmo\s+action|insta\s*360|insta360)\b", title_low):
        return True
    return "camera" in t or "camera" in model or "cam" in t


def _brand_from_title(title: str) -> str:
    title_low = title.lower()
    if re.search(r"\bgopro\b", title_low):
        return "gopro"
    if re.search(r"\bdji\b|\bosmo\s+action\b", title_low):
        return "dji"
    if re.search(r"\binsta\s*360\b|\binsta360\b", title_low):
        return "insta360"
    return ""


def _model_from_title(title: str, brand: str) -> str:
    title_low = title.lower()
    if brand == "gopro":
        match = re.search(r"\bhero\s*([0-9]{1,2})\b", title_low)
        if match:
            return f"hero{match.group(1)}"
    if brand == "dji":
        match = re.search(r"\b(?:dji\s+)?(?:osmo\s+)?action\s*([0-9]{1,2})\b", title_low)
        if match:
            return f"osmoaction{match.group(1)}"
    if brand == "insta360":
        match = re.search(r"\b(?:insta\s*360|insta360)\s*([a-z0-9]+)\b", title_low)
        if match:
            return match.group(1)
    return ""


def _extract_cam_storage(attrs: Mapping[str, Any], title: str) -> str:
    # Use 'Flash Memory Installed' for internal units
    search = (_clean(attrs.get("Flash Memory Installed")) + title.lower())
    if "128" in search: return "128gb"
    if "64" in search: return "64gb"
    if "32" in search: return "32gb"
    return ""


def camera_drone_model_key(attrs: Mapping[str, Any], title: str = "") -> Optional[str]:
    if not _is_action_cam(attrs or {}, title):
        return "unknown"

    brand = _clean(attrs.get("Brand")).replace(" ", "") or _brand_from_title(title)
    model_raw = _clean(attrs.get("Model"))

    # Model Repair Logic
    if brand == "gopro":
        if "hero" not in model_raw and any(char.isdigit() for char in model_raw):
            model_raw = "hero" + "".join(filter(str.isdigit, model_raw))
    elif brand == "dji" and "action" in model_raw and "osmo" not in model_raw:
        model_raw = "osmo" + model_raw

    title_model = _model_from_title(title, brand)

    if brand and model_raw.startswith(brand):
        model_raw = model_raw[len(brand):].strip()

    # Keep only alphanumeric for the key. A clear title signal beats item
    # specifics because eBay attributes are often copied from a different model.
    model_core = title_model or "".join(ch for ch in model_raw if ch.isalnum())

    # Specific storage for internal-memory units
    if "go" in model_core or "insta360" in brand:
        storage = _extract_cam_storage(attrs, title)
        if storage: model_core += f"_{storage}"

    if not brand or not model_core: return "unknown"

    grade = _derive_condition_grade(attrs, title)
    return f"{brand}-{model_core}_{grade}"
