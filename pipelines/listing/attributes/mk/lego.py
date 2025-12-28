from __future__ import annotations

import re
from typing import Any, Mapping, Optional

from utils.condition import _derive_condition_grade

UNKNOWN_KEY = "unknown"


def _clean(s: Any) -> str:
    """Basic string cleaner: None -> "", strip whitespace."""
    if s is None:
        return ""
    return str(s).strip()


def _normalise_brand(raw: Any) -> str:
    """Normalise Brand into a compact token for the key.

    For ebay-lego we mostly collapse to:
      - "lego" for genuine LEGO
      - "moclego" for MOC / compatible builds

    Rules:
    - Lowercase
    - If the brand contains "moc" anywhere -> moclego
    - Else -> lego (even if user typed "Lego", "LEGO®", etc)
    """
    s = _clean(raw)
    if not s:
        return ""

    low = s.lower()
    if "moc" in low:
        return "moclego"

    # If it's any flavour of LEGO, normalise to lego
    if "lego" in low:
        return "lego"

    # Fallback: compact alnum brand (rare in this feed)
    out = []
    for ch in low:
        if ch.isalnum():
            out.append(ch)
    return "".join(out) or "lego"


def _parse_int_like(raw: Any) -> str:
    """Extract a compact numeric token from an attribute value.

    Accepts values like:
      - "10214"
      - "S_76294-1___GB"  -> "76294"
      - "4002025" (corporate / special sets)
      - "Nicht zutreffend", "Does not apply" -> ""
    """
    s = _clean(raw)
    if not s:
        return ""

    low = s.lower()

    BAD = {
        "n/a", "na", "none", "not applicable", "does not apply", "doesnotapply",
        "does not apply.", "doesn't apply", "doesntapply", "nicht zutreffend",
        "see description", "see photo", "random",
    }
    if low in BAD or "does not apply" in low or "nicht zutreffend" in low:
        return ""

    # Pull the first run of 3..7 digits (covers old 3-digit sets + 4-5 digit modern + 7-digit corporate)
    m = re.search(r"\b(\d{3,7})\b", s)
    if not m:
        return ""

    num = m.group(1)

    # Avoid obvious years (these appear a lot in titles / attrs)
    try:
        year = int(num)
        if 1950 <= year <= 2035:
            return ""
    except Exception:
        pass

    return num


def _set_number_from_attrs(attrs: Mapping[str, Any]) -> str:
    """Best-effort extraction of a LEGO set/model number from attrs."""
    # Order matters: prefer explicit "MPN-ish" fields, then "Model", then translated variants.
    CANDIDATE_KEYS = [
        # Common eBay keys
        "MPN",
        "Manufacturer Part Number",
        "Model Number",
        "Model",
        "Part Number",
        "Artikelnummer",
        "Herstellernummer",

        # Variants seen in the source export (different languages/encodings)
        "Numéro de l'assortiment LEGO",
        "NumÃ©ro de l'assortiment LEGO",
        "Number of l'assortment LEGO",
        "NumÃ©ro de l'assortiment LEGO",
        "Item model number",
    ]

    for k in CANDIDATE_KEYS:
        if k in attrs:
            val = _parse_int_like(attrs.get(k))
            if val:
                return val

    return ""


def _set_number_from_title(title: str) -> str:
    """Fallback: try to find a plausible set number in the title."""
    t = _clean(title)
    if not t:
        return ""

    # Collect all numeric candidates, then pick the first plausible one.
    candidates = re.findall(r"\b(\d{3,7})\b", t)
    for num in candidates:
        # Skip years
        try:
            year = int(num)
            if 1950 <= year <= 2035:
                continue
        except Exception:
            pass

        # Most LEGO set numbers are 3-5 digits, but keep 6-7 too (e.g. corporate / special)
        return num

    return ""


def lego_model_key(
    attrs: Mapping[str, Any],
    title: str = "",
) -> Optional[str]:
    """Build a canonical model key for LEGO listings (source='ebay-lego').

    Output format (console-style):

        {brand}-{setnum}_{grade}

    Examples:
        Brand="LEGO", Herstellernummer="10214"
            -> "lego-10214_B"

        Brand="MOC LEGO", Model="S_76294-1___GB"
            -> "moclego-76294_B"

    Rules:
    - Uses attrs["Brand"] and (preferably) a set/model number from attrs
      (MPN/Herstellernummer/Model/Model Number/etc)
    - Falls back to extracting a plausible number from the title
    - Passes attrs + title into _derive_condition_grade for grade
    - If no usable Brand or no usable set number -> returns UNKNOWN_KEY ("unknown")
    """
    raw_brand = attrs.get("Brand")
    brand = _normalise_brand(raw_brand)

    setnum = _set_number_from_attrs(attrs) or _set_number_from_title(title)

    if not brand or not setnum:
        return UNKNOWN_KEY

    base_key = f"{brand}-{setnum}"
    grade = _derive_condition_grade(attrs, title)
    return f"{base_key}_{grade}"
