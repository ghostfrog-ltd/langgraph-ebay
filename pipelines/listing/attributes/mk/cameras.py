from __future__ import annotations
from typing import Mapping, Any, Optional
from utils.condition import _derive_condition_grade


def _clean(s: Any) -> str:
    return str(s).strip().lower() if s else ""


def _is_action_cam(attrs: Mapping[str, Any]) -> bool:
    t = _clean(attrs.get("Type"))
    model = _clean(attrs.get("Model"))
    # Exclude drones and accessories found in data
    exclude = {"drone", "mavic", "mount", "strap", "case", "charger"}
    if any(x in t for x in exclude) or any(x in model for x in exclude):
        return False
    return "camera" in t or "camera" in model or "cam" in t


def _extract_cam_storage(attrs: Mapping[str, Any], title: str) -> str:
    # Use 'Flash Memory Installed' for internal units
    search = (_clean(attrs.get("Flash Memory Installed")) + title.lower())
    if "128" in search: return "128gb"
    if "64" in search: return "64gb"
    if "32" in search: return "32gb"
    return ""


def camera_drone_model_key(attrs: Mapping[str, Any], title: str = "") -> Optional[str]:
    if not attrs or not _is_action_cam(attrs):
        return "unknown"

    brand = _clean(attrs.get("Brand")).replace(" ", "")
    model_raw = _clean(attrs.get("Model"))

    # Model Repair Logic
    if brand == "gopro":
        if "hero" not in model_raw and any(char.isdigit() for char in model_raw):
            model_raw = "hero" + "".join(filter(str.isdigit, model_raw))
    elif brand == "dji" and "action" in model_raw and "osmo" not in model_raw:
        model_raw = "osmo" + model_raw

    # Keep only alphanumeric for the key
    model_core = "".join(ch for ch in model_raw if ch.isalnum())

    # Specific storage for internal-memory units
    if "go" in model_core or "insta360" in brand:
        storage = _extract_cam_storage(attrs, title)
        if storage: model_core += f"_{storage}"

    if not brand or not model_core: return "unknown"

    grade = _derive_condition_grade(attrs, title)
    return f"{brand}-{model_core}_{grade}"