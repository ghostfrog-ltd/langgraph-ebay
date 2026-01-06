# agent/model_keys/tools.py
from __future__ import annotations

import re
from typing import Mapping, Any, Optional
from utils.condition import _derive_condition_grade

UNKNOWN_KEY = "unknown"


def _clean(s: Any) -> str:
    if s is None: return ""
    if isinstance(s, list):
        return " ".join(str(v).lower() for v in s)
    return str(s).strip()


def _extract_voltage(attrs: Mapping[str, Any], title: str) -> str:
    """Standardizes Voltage (e.g., '18v', '54v')."""
    raw_v = _clean(attrs.get("Voltage"))
    search_text = f"{raw_v} {title.lower()}"

    match = re.search(r'\b(\d{2,3})\s?v\b', search_text)
    if match: return f"{match.group(1)}v"

    decimal_match = re.search(r'\b(\d\.\d)\s?v\b', search_text)
    if decimal_match: return f"{decimal_match.group(1).replace('.', '')}v"
    return ""


def _is_bare_tool(attrs: Mapping[str, Any], title: str) -> bool:
    """Detects 'Body Only' vs 'Kit' status."""
    t = _clean(attrs.get("Type")).lower()
    title_low = title.lower()
    bare_signals = {"body only", "bare tool", "no battery", "unit only", "no charger"}
    return any(sig in title_low for sig in bare_signals) or any(sig in t for sig in bare_signals)


def _is_brushless(attrs: Mapping[str, Any], title: str) -> str:
    """
    Detects Brushless motor technology.
    A critical price anchor: Brushless tools are worth ~30% more.
    """
    # Check attributes and title for the 'brushless' keyword
    search_text = f"{_clean(attrs.get('Type'))} {_clean(attrs.get('Features'))} {title.lower()}"

    if "brushless" in search_text:
        return "brushless"
    # Optional: We could mark as 'brushed' but usually better to leave empty
    # if unknown to avoid mislabeling legacy tools.
    return ""


def _normalise_brand(raw: Any) -> str:
    s = _clean(raw).lower()
    if not s or s in {"unbranded", "does not apply"}: return ""
    return "".join(ch for ch in s if ch.isalnum())


def _normalise_model(raw_model: Any, brand: str) -> str:
    """Extracts stable model family."""
    s = _clean(raw_model)
    if not s or "does not apply" in s or len(s) < 2: return ""

    # Strip parentheses and join first two tokens
    s = re.sub(r'\(.*?\)', '', s)
    s = s.replace("/", " ").replace("-", " ").replace("\\", " ")
    tokens = [t for t in s.split() if "".join(ch for ch in t.lower() if ch.isalnum())]

    if tokens and tokens[0].lower() == brand: tokens = tokens[1:]
    return "".join("".join(ch for ch in t.lower() if ch.isalnum()) for t in tokens[:2])


def tools_model_key(
        attrs: Mapping[str, Any],
        title: str = "",
) -> Optional[str]:
    """
    Builds canonical tools key: {brand}-{model}-{voltage}-{motor?}-{bare/kit}_{grade}

    Example: "dewalt-dcd996-18v-brushless-bare_B".
    """
    brand = _normalise_brand(attrs.get("Brand"))

    # Cascade: Model -> MPN -> Type
    model_core = _normalise_model(attrs.get("Model"), brand) or \
                 _normalise_model(attrs.get("MPN"), brand) or \
                 _normalise_model(attrs.get("Type"), brand)

    if not brand or not model_core:
        return UNKNOWN_KEY

    # Specs Extraction
    voltage = _extract_voltage(attrs, title)
    motor = _is_brushless(attrs, title)
    bare_status = "bare" if _is_bare_tool(attrs, title) else "kit"

    # Construction
    parts = [brand, model_core]
    if voltage: parts.append(voltage)
    if motor: parts.append(motor)
    parts.append(bare_status)

    base_key = "-".join(parts)
    grade = _derive_condition_grade(attrs, title)

    return f"{base_key}_{grade}"