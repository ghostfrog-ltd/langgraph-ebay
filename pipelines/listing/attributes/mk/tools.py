# agent/model_keys/tools.py
from __future__ import annotations

import re
from typing import Mapping, Any, Optional
from utils.condition import _derive_condition_grade

UNKNOWN_KEY = "unknown"
TITLE_BRANDS = (
    "mafell",
    "dewalt",
    "makita",
    "milwaukee",
    "bosch",
    "festool",
    "hilti",
    "ryobi",
    "metabo",
    "einhell",
    "hitachi",
    "hikoki",
)


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
    search_text = (
        f"{_clean(attrs.get('Type'))} "
        f"{_clean(attrs.get('Features'))} "
        f"{_clean(attrs.get('Motor Type'))} "
        f"{title.lower()}"
    ).lower()

    if "brushless" in search_text:
        return "brushless"
    # Optional: We could mark as 'brushed' but usually better to leave empty
    # if unknown to avoid mislabeling legacy tools.
    return ""


def _normalise_brand(raw: Any) -> str:
    s = _clean(raw).lower()
    if not s or s in {"unbranded", "does not apply"}: return ""
    return "".join(ch for ch in s if ch.isalnum())


def _brand_from_title(title: str) -> str:
    """Infer a known power-tool brand from title-only Browse rows."""

    title_low = title.lower()
    for brand in TITLE_BRANDS:
        if re.search(rf"\b{re.escape(brand)}\b", title_low):
            return "".join(ch for ch in brand if ch.isalnum())
    return ""


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


def _model_from_title(title: str, brand: str) -> str:
    """
    Extract a compact model family from title-only rows.

    Browse results often lack structured Brand/Model attributes. For Slack
    scout rows, title patterns like `Mafell A 12` or `DeWalt DCD796` are strong
    enough to group sold comps without waiting for richer attributes.
    """

    if not brand:
        return ""

    title_low = title.lower()
    brand_pattern = r"\s+".join(re.escape(part) for part in brand.split())
    after_brand = re.search(rf"\b{brand_pattern}\b\s+([a-z]{{1,4}}\s*[-]?\s*\d{{1,5}}[a-z0-9-]*)", title_low)
    if after_brand:
        return "".join(ch for ch in after_brand.group(1) if ch.isalnum())

    generic_model = re.search(r"\b([a-z]{2,5}\d{2,5}[a-z0-9-]*)\b", title_low)
    if generic_model:
        return "".join(ch for ch in generic_model.group(1) if ch.isalnum())

    return ""


def _model_attr_looks_like_catalogue_code(model_core: str) -> bool:
    """
    Detect attribute codes that are less useful than explicit title models.

    Some tool listings expose MPN/catalogue codes such as `91D021` while the
    title says the trade model family, e.g. `Mafell A 12`. For comp grouping the
    title family is often the better stable key.
    """

    return bool(re.fullmatch(r"\d+[a-z]\d+", model_core or ""))


def tools_model_key(
        attrs: Mapping[str, Any],
        title: str = "",
) -> Optional[str]:
    """
    Builds canonical tools key: {brand}-{model}-{voltage}-{motor?}-{bare/kit}_{grade}

    Example: "dewalt-dcd996-18v-brushless-bare_B".
    """
    brand = _normalise_brand(attrs.get("Brand")) or _brand_from_title(title)

    title_model = _model_from_title(title, brand)

    # Cascade: Model -> MPN -> Type, but let strong title families beat noisy
    # catalogue-code attributes.
    model_core = _normalise_model(attrs.get("Model"), brand) or \
                 _normalise_model(attrs.get("MPN"), brand) or \
                 _normalise_model(attrs.get("Type"), brand)

    if title_model and (
        not model_core or _model_attr_looks_like_catalogue_code(model_core)
    ):
        model_core = title_model

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
