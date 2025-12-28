from __future__ import annotations

import re
from typing import Any, Mapping, Optional

from utils.condition import _derive_condition_grade

UNKNOWN_KEY = "unknown"


def _clean(s: Any) -> str:
    if s is None:
        return ""
    return str(s).strip()


def _normalise_brand(raw: Any) -> str:
    """Normalise brand to a compact token.

    NOTE: For ebay-honda-nc750 listings, Brand is often the SELLER (e.g. MOTOPART4U),
    not Honda. We still include it to distinguish aftermarket suppliers.
    """
    s = _clean(raw).lower()
    if not s:
        return ""
    return "".join(ch for ch in s if ch.isalnum())


def _extract_bike_family(attrs: Mapping[str, Any]) -> str:
    """Extract the bike family from compatibility fields.

    We deliberately collapse everything to:
      - nc700x
      - nc750x

    This avoids exploding keys by year ranges.
    """
    candidates = [
        attrs.get("Compatible Make"),
        attrs.get("Fitment 1"),
        attrs.get("Fitment 2"),
    ]

    joined = " ".join(_clean(c) for c in candidates).lower()

    if "nc750" in joined:
        return "nc750x"
    if "nc700" in joined:
        return "nc700x"

    return ""


def _extract_part_token(attrs: Mapping[str, Any]) -> str:
    """Extract a stable part identifier.

    Prefer Manufacturer Part Number / OEM reference.
    """
    for key in (
        "Manufacturer Part Number",
        "Reference OE/OEM Number",
    ):
        val = _clean(attrs.get(key))
        if not val:
            continue

        low = val.lower()
        if low in {"n/a", "na", "does not apply"}:
            continue

        return "".join(ch for ch in val.lower() if ch.isalnum())

    return ""


def honda_nc750_model_key(
    attrs: Mapping[str, Any],
    title: str = "",
) -> Optional[str]:
    """Canonical model key for ebay-honda-nc750 listings.

    Output format:

        {brand}-{bike}-{part}_{grade}

    Examples:
        Brand="MOTOPART4U",
        Compatible Make="For Honda NC750X",
        Manufacturer Part Number="MOT-083"

        -> motopart4u-nc750x-mot083_B

    Notes:
    - Brand is aftermarket supplier, not Honda.
    - Bike family is collapsed to nc700x / nc750x.
    - Year range is intentionally ignored.
    - Condition grade derived via _derive_condition_grade.
    """
    brand = _normalise_brand(attrs.get("Brand"))
    bike = _extract_bike_family(attrs)
    part = _extract_part_token(attrs)

    if not brand or not bike or not part:
        return UNKNOWN_KEY

    grade = _derive_condition_grade(attrs, title)
    return f"{brand}-{bike}-{part}_{grade}"
