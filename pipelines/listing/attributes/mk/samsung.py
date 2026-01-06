# agent/model_keys/samsung.py
from __future__ import annotations

import re
from typing import Any, Mapping, Optional
from utils.condition import _derive_condition_grade

UNKNOWN_KEY = "unknown"


def _clean(s: Any) -> str:
    """Basic string cleaner: None -> "", strip whitespace."""
    if s is None: return ""
    if isinstance(s, list):
        return " ".join(str(v).lower() for v in s)
    return str(s).strip()


def _extract_storage(attrs: Mapping[str, Any], title: str) -> str:
    """
    Extracts canonical storage (e.g., '128gb', '1tb').
    Scans 'Storage Capacity' and 'Size' attributes.
    """
    raw_storage = _clean(attrs.get("Storage Capacity")) + _clean(attrs.get("Size"))
    search_text = f"{raw_storage} {title.lower()}"

    # 1. Check for TB
    if re.search(r'1\s?tb', search_text): return "1tb"
    # 2. Check for GB (Standardizing 128, 256, 512)
    match = re.search(r'\b(128|256|512|64|32)\s?gb\b', search_text)
    if match:
        return f"{match.group(1)}gb"

    return ""


def _is_5g(attrs: Mapping[str, Any], title: str) -> str:
    """Detects 5G capability, a major price driver for A-series."""
    conn = _clean(attrs.get("Connectivity"))
    search_text = f"{conn} {title.lower()}"
    return "5g" if "5g" in search_text else ""


def _normalise_brand(raw: Any) -> str:
    """Collapses Samsung and competitors found in the data."""
    s = _clean(raw).lower()
    if not s or s in {"unbranded", "does not apply"}: return ""

    # List derived from ebay-samsung.md
    if "samsung" in s: return "samsung"
    if "google" in s: return "google"
    if "motorola" in s: return "motorola"
    if "oneplus" in s: return "oneplus"

    return "".join(ch for ch in s if ch.isalnum())


def _compress_model_tokens(tokens: list[str]) -> str:
    """Collapses phone models into stable families (e.g., 's23ultra')."""
    if not tokens: return ""

    STOP = {
        "galaxy", "samsung", "phone", "smartphone", "5g", "lte", "edition",
        "dual", "sim", "unlocked", "android", "mobile", "sm"
    }

    out: list[str] = []
    for tok in tokens:
        if tok in STOP: continue

        # Handle 'z' + 'fold/flip' logic
        if not out:
            out.append(tok)
            continue
        if out == ["z"] and tok in {"fold", "flip"}:
            out[-1] = f"z{tok}"
            continue

        # Keep major suffixes
        if re.search(r"\d", tok) or tok in {"ultra", "fe", "plus", "pro", "max"}:
            out.append(tok)
            # Once we have something like 's23' + 'ultra', we stop
            if len(out) >= 2: break
            continue

        if len(out) >= 2: break
        out.append(tok)

    return "".join(out)


def _normalise_model(attrs: Mapping[str, Any], brand: str) -> str:
    """Extracts model from Model, Model Number, or MPN."""
    # Cascade: Model is usually best, Model Number is technical
    raw = _clean(attrs.get("Model") or attrs.get("Model Number") or attrs.get("MPN"))
    if not raw or raw in {"does not apply", "n/a", "android phone"}:
        return ""

    # Clean separators
    s = raw.replace("/", " ").replace("-", " ")
    raw_tokens = s.split()

    tokens: list[str] = []
    for tok in raw_tokens:
        alnum = "".join(ch for ch in tok.lower() if ch.isalnum())
        if alnum and alnum != brand:
            tokens.append(alnum)

    return _compress_model_tokens(tokens)


def samsung_model_key(
        attrs: Mapping[str, Any],
        title: str = "",
) -> Optional[str]:
    """
    Builds canonical key: {brand}-{model}-{storage}-{5g?}_{grade}

    Examples:
    - Brand="Samsung", Model="Galaxy S23 Ultra", Storage="256 GB" -> "samsung-s23ultra-256gb_B"
    - Brand="Samsung", Model="Galaxy A54 5G", Storage="128 GB" -> "samsung-a54-128gb-5g_B"
    """
    brand = _normalise_brand(attrs.get("Brand"))
    model_core = _normalise_model(attrs, brand)

    if not brand or not model_core:
        return UNKNOWN_KEY

    # Specs Extraction
    storage = _extract_storage(attrs, title)
    connectivity = _is_5g(attrs, title)

    # Construct base key
    parts = [brand, model_core]
    if storage: parts.append(storage)
    if connectivity: parts.append(connectivity)

    base_key = "-".join(parts)
    grade = _derive_condition_grade(attrs, title)

    return f"{base_key}_{grade}"