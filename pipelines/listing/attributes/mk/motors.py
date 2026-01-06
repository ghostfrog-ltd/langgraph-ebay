# agent/model_keys/motors.py
from __future__ import annotations

import re
from typing import Mapping, Any, Optional

from utils.condition import _derive_condition_grade

UNKNOWN_KEY = "unknown"


def _clean(s: Any) -> str:
    """Basic string cleaner: None -> "", strip whitespace."""
    if s is None: return ""
    if isinstance(s, list):
        return " ".join(str(v).lower() for v in s)
    return str(s).strip()


def _extract_engine_size(attrs: Mapping[str, Any], title: str) -> str:
    """
    Extracts canonical engine size (e.g., '1.6l', '1200cc').
    Scans Engine Size, Engine, Engine Number, and Drivetrain attributes.
    """
    # Cascade: Check structured attributes first
    candidates = [
        attrs.get("Engine Size"),
        attrs.get("Engine"),
        attrs.get("Engine Number"),
        attrs.get("Drivetrain")
    ]

    search_text = " ".join(_clean(c) for c in candidates) + " " + title.lower()

    # 1. Look for 'cc' patterns (e.g., 1197cc, 1200 cc)
    cc_match = re.search(r'(\d{3,4})\s?cc', search_text)
    if cc_match:
        return f"{cc_match.group(1)}cc"

    # 2. Look for Litre patterns (e.g., 1.6, 1.6L, 2.0 Litre)
    litre_match = re.search(r'(\d\.\d)\s?(?:l|ltr|litre)?', search_text)
    if litre_match:
        return f"{litre_match.group(1)}l"

    # 3. Look for raw 3-4 digit numbers that look like displacement (e.g., 1300)
    raw_cc_match = re.search(r'\b(8\d{2}|[1-5]\d{3})\b', search_text)
    if raw_cc_match:
        return f"{raw_cc_match.group(1)}cc"

    return ""


def _extract_year(attrs: Mapping[str, Any], title: str) -> str:
    """
    Extracts manufacture year using registration dates and manufacture attributes.
    """
    # Grab all year-related candidates
    candidates = [
        str(attrs.get("Year", "")),
        str(attrs.get("Date of 1st Registration", "")),
        str(attrs.get("Year Manufactured", ""))
    ]

    blob = " ".join(candidates) + " " + title

    # Search for plausible years (1950-2030)
    match = re.search(r'\b(19\d{2}|20[0-2]\d)\b', blob)
    return match.group(1) if match else ""


def _extract_mileage_bucket(attrs: Mapping[str, Any], title: str) -> str:
    """
    Groups mileage into 10,000 mile buckets to facilitate clustering.
    Example: 34,500 miles -> '30k'.
    """
    raw_mileage = _clean(attrs.get("Mileage"))
    search_text = f"{raw_mileage} {title.lower()}".replace(",", "")

    match = re.search(r'\b(\d{1,6})\b', search_text)
    if match:
        try:
            val = int(match.group(1))
            # Create 10k buckets (e.g., 34000 -> 30k)
            bucket = (val // 10000) * 10
            return f"{bucket}k"
        except ValueError:
            pass
    return ""


def _normalise_brand(raw: Any) -> str:
    """Normalises Brand/Make to compact alphanumeric."""
    s = _clean(raw)
    if not s or s.lower() in {"other", "unbranded", "does not apply"}:
        return ""
    return "".join(ch for ch in s.lower() if ch.isalnum())


def _compress_model_tokens(tokens: list[str]) -> str:
    """Collapses noisy trim strings into a stable family token."""
    if not tokens: return ""

    STOP_TOKENS = {
        "sport", "msport", "amg", "line", "sline", "edition", "luxury",
        "premium", "quattro", "xdrive", "awd", "fwd", "hatchback", "saloon",
        "estate", "auto", "manual", "diesel", "petrol"
    }

    out: list[str] = []
    have_digit_anchor = False

    for tok in tokens:
        if not out:
            out.append(tok)
            if any(ch.isdigit() for ch in tok): have_digit_anchor = True
            continue

        if tok in STOP_TOKENS: break

        # Keep identity anchors like '320d' or 'tt'
        if any(ch.isdigit() for ch in tok):
            out.append(tok)
            have_digit_anchor = True
            if len(out) >= 2: break
            continue

        if have_digit_anchor or len(out) >= 2: break
        out.append(tok)

    return "".join(out)


def _normalise_model(raw_model: Any, raw_brand: Any) -> str:
    """Standardizes Model or Variant attributes."""
    s = _clean(raw_model)
    if not s or s.lower() in {"not supplied", "does not apply", "n/a"}:
        return ""

    s = s.replace("/", " ").replace("-", " ")
    raw_tokens = s.split()

    brand_first = _clean(raw_brand).split()[0].lower() if raw_brand else ""
    tokens: list[str] = []
    for tok in raw_tokens:
        alnum = "".join(ch for ch in tok if ch.isalnum()).lower()
        if alnum and alnum != brand_first:
            tokens.append(alnum)

    return _compress_model_tokens(tokens)


def motors_model_key(
        attrs: Mapping[str, Any],
        title: str = "",
) -> Optional[str]:
    """
    Builds canonical motors key: {brand}-{model}-{engine}-{year}-{mileage}_{grade}

    Supported Logic:
    - Identity: Brand + (Model or Variant cascade)
    - Engine: Standardized Litre/CC extractor
    - Age: Year extraction from registration dates
    - Usage: 10k Mileage buckets
    """
    brand = _normalise_brand(attrs.get("Brand"))
    # Fallback cascade for identification
    model_core = _normalise_model(attrs.get("Model"), attrs.get("Brand")) or \
                 _normalise_model(attrs.get("Variant"), attrs.get("Brand"))

    if not brand or not model_core:
        return UNKNOWN_KEY

    # Extraction Layer
    engine = _extract_engine_size(attrs, title)
    year = _extract_year(attrs, title)
    mileage = _extract_mileage_bucket(attrs, title)

    # Construct base key with identity + specs
    base_parts = [brand, model_core]
    if engine: base_parts.append(engine)
    if year: base_parts.append(year)
    if mileage: base_parts.append(mileage)

    base_key = "-".join(base_parts)
    grade = _derive_condition_grade(attrs, title)

    return f"{base_key}_{grade}"