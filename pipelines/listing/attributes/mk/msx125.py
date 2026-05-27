from __future__ import annotations

import re
from typing import Any, Mapping, Optional

from utils.condition import _derive_condition_grade

UNKNOWN_KEY = "unknown"


def _clean(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _blob(attrs: Mapping[str, Any], title: str) -> str:
    return " ".join(
        _clean(value)
        for value in (
            title,
            attrs.get("Manufacturer"),
            attrs.get("Make"),
            attrs.get("Model"),
            attrs.get("Type"),
            attrs.get("Engine Size"),
            attrs.get("Capacity (cc)"),
        )
        if _clean(value)
    ).lower()


def _extract_capacity(attrs: Mapping[str, Any], title: str) -> str:
    for key in ("Capacity (cc)", "Engine Size"):
        raw = _clean(attrs.get(key))
        if not raw:
            continue
        match = re.search(r"\b(12[0-9]|124|125)\b", raw)
        if match:
            return "125cc"

    if re.search(r"\b(msx\s*125|msx-125|msx125|125cc|124cc)\b", title.lower()):
        return "125cc"

    return ""


def honda_msx125_model_key(
    attrs: Mapping[str, Any],
    title: str = "",
) -> Optional[str]:
    blob = _blob(attrs, title)

    if "honda" not in blob:
        return UNKNOWN_KEY
    if not any(marker in blob for marker in ("msx125", "msx 125", "msx-125", "grom")):
        return UNKNOWN_KEY

    capacity = _extract_capacity(attrs, title)
    grade = _derive_condition_grade(attrs, title)

    parts = ["honda-msx125"]
    if capacity:
        parts.append(capacity)

    return f"{'-'.join(parts)}_{grade}"
