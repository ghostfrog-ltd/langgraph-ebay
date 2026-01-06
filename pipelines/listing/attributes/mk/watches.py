# agent/model_keys/watches.py
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


def _extract_provenance(attrs: Mapping[str, Any], title: str) -> str:
    """
    Detects Box and Papers. In watches, 'Full Set' is the ultimate price anchor.
    Scans 'With Original Box/Packaging' and 'With Papers'.
    """
    box = _clean(attrs.get("With Original Box/Packaging")).lower()
    papers = _clean(attrs.get("With Papers")).lower()
    title_low = title.lower()

    has_box = "yes" in box or "with box" in title_low
    has_papers = "yes" in papers or "with papers" in title_low or "v5c" not in title_low  # Watch specific papers

    if has_box and has_papers: return "fullset"
    if has_box: return "box"
    if has_papers: return "papers"
    return "watchonly"


def _extract_movement(attrs: Mapping[str, Any], title: str) -> str:
    """
    Differentiates between Quartz and Mechanical (Automatic/Manual).
    Crucial for brands like Seiko or Omega where movement dictates value tiers.
    """
    m = _clean(attrs.get("Movement")).lower()
    search_text = f"{m} {title.lower()}"

    if "automatic" in search_text: return "auto"
    if "manual" in search_text or "hand-winding" in search_text: return "manual"
    if "quartz" in search_text or "solar" in search_text: return "quartz"
    return ""


def _normalise_brand(raw: Any) -> str:
    """Normalises Brand/Make to compact alphanumeric."""
    s = _clean(raw).lower()
    if not s or s in {"other", "unbranded", "does not apply"}:
        return ""
    # Standardize common watch brands
    if "seiko" in s: return "seiko"
    if "omega" in s: return "omega"
    if "rolex" in s: return "rolex"
    if "casio" in s: return "casio"
    if "g-shock" in s or "gshock" in s: return "gshock"

    return "".join(ch for ch in s if ch.isalnum())


def _extract_reference(attrs: Mapping[str, Any]) -> str:
    """Extracts the high-confidence Reference Number."""
    ref = attrs.get("Reference Number") or attrs.get("ReferenceNumber")
    if not ref: return ""

    if isinstance(ref, list):
        # Specificity wins: pick the longest alphanumeric string
        candidates = ["".join(ch for ch in str(r).lower() if ch.isalnum()) for r in ref]
        return max(candidates, key=len) if candidates else ""

    clean_ref = "".join(ch for ch in str(ref).lower() if ch.isalnum())
    if clean_ref in {"none", "na", "doesnotapply"}: return ""
    return clean_ref


def _normalise_model_core(attrs: Mapping[str, Any], brand: str) -> str:
    """Fallback: Extracts model family if reference is missing."""
    m = _clean(attrs.get("Model") or attrs.get("Watch Model"))
    if not m or m in {"watch", "mens", "womens"}: return ""

    # Strip brand repetition (e.g., 'Seiko 5' -> '5')
    tokens = m.replace("-", " ").split()
    if tokens and tokens[0] == brand:
        tokens = tokens[1:]

    # Keep up to 2 identifying tokens
    res = "".join("".join(ch for ch in t if ch.isalnum()) for t in tokens[:2])
    return res


def watch_model_key(
        attrs: Mapping[str, Any],
        title: str = "",
) -> Optional[str]:
    """
    Builds canonical watch key: {brand}-{ref_or_model}-{movement?}-{provenance}_{grade}

    Example: Brand="Seiko", Ref="SKX007", Movement="Automatic", Provenance="Full Set"
             -> "seiko-skx007-auto-fullset_B".
    """
    brand = _normalise_brand(attrs.get("Brand"))

    # Priority: Reference Number -> Model Core
    ref = _extract_reference(attrs)
    model = _normalise_model_core(attrs, brand)

    identity = ref or model
    if not brand or not identity:
        return UNKNOWN_KEY

    # Specs Extraction
    movement = _extract_movement(attrs, title)
    provenance = _extract_provenance(attrs, title)

    # Construction
    parts = [brand, identity]
    if movement: parts.append(movement)
    parts.append(provenance)

    base_key = "-".join(parts)
    grade = _derive_condition_grade(attrs, title)

    return f"{base_key}_{grade}"