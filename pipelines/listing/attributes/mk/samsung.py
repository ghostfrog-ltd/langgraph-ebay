from __future__ import annotations

import re
from typing import Any, Mapping, Optional

UNKNOWN_KEY = "unknown"


def _text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, (list, tuple, set)):
        return " ".join(_text(item) for item in value)
    return str(value).strip().lower()


def _clean_token(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-")


def _blob(attrs: Mapping[str, Any], title: str) -> str:
    fields = [
        title,
        _text(attrs.get("Brand")),
        _text(attrs.get("Model")),
        _text(attrs.get("Model Number")),
        _text(attrs.get("MPN")),
        _text(attrs.get("Product Line")),
        _text(attrs.get("Series")),
        _text(attrs.get("Network")),
        _text(attrs.get("Connectivity")),
        _text(attrs.get("Cellular Band")),
        _text(attrs.get("Cellular Technology")),
        _text(attrs.get("Storage Capacity")),
        _text(attrs.get("Size")),
    ]
    return " ".join(part for part in fields if part).lower()


def _is_samsung(attrs: Mapping[str, Any], title: str) -> bool:
    brand = _text(attrs.get("Brand") or attrs.get("brand"))
    blob = _blob(attrs, title)
    return "samsung" in brand or "samsung" in blob or "galaxy" in blob


def _is_junk(blob: str) -> bool:
    junk_terms = (
        "case for",
        "phone case",
        "flip cover",
        "screen protector",
        "tempered glass",
        "replacement screen",
        "screen assembly",
        "lcd assembly",
        "digitizer",
        "charging port",
        "flex cable",
        "back cover",
        "back glass",
        "housing",
        "frame",
        "battery door",
        "charger only",
        "cable only",
        "box only",
        "empty box",
        "dummy phone",
        "display phone",
    )
    return any(term in blob for term in junk_terms)


def _capacity_token(amount: str, unit: str) -> str:
    number = int(amount)
    unit = unit.lower()
    if unit == "tb":
        if number == 1:
            return "1tb"
        return f"{number}tb"
    if number in {250, 251, 255, 256}:
        return "256gb"
    if number in {500, 512}:
        return "512gb"
    if number in {1000, 1024}:
        return "1tb"
    return f"{number}gb"


def _storage(attrs: Mapping[str, Any], title: str) -> str:
    storage_text = " ".join(
        _text(attrs.get(key))
        for key in ("Storage Capacity", "Size", "Memory Card Type", "Capacity", "Model")
        if attrs.get(key)
    )
    blob = f"{title.lower()} {storage_text}"
    candidates: list[str] = []
    for match in re.finditer(r"\b(32|64|128|256|512|1000|1024|1)\s*(gb|tb)\b", blob):
        before = blob[max(0, match.start() - 18) : match.start()]
        after = blob[match.end() : match.end() + 18]
        if re.search(r"(ram|memory)\s*$", before) or re.search(r"^\s*(ram|memory)", after):
            continue
        token = _capacity_token(match.group(1), match.group(2))
        if token not in candidates:
            candidates.append(token)
    return candidates[-1] if candidates else ""


def _has_5g(attrs: Mapping[str, Any], title: str) -> bool:
    return bool(re.search(r"\b5g\b", _blob(attrs, title)))


def _foldable_from_model_number(blob: str) -> str:
    if re.search(r"\bsm-f9(?:16|26|36|46|56|66)[a-z]*\b", blob):
        mapping = {
            "916": "z-fold2",
            "926": "z-fold3",
            "936": "z-fold4",
            "946": "z-fold5",
            "956": "z-fold6",
            "966": "z-fold7",
        }
        code = re.search(r"\bsm-f9(16|26|36|46|56|66)[a-z]*\b", blob)
        return mapping.get(code.group(1), "") if code else ""
    if re.search(r"\bsm-f7(?:11|21|31|41|61)[a-z]*\b", blob):
        mapping = {
            "11": "z-flip3",
            "21": "z-flip4",
            "31": "z-flip5",
            "41": "z-flip6",
            "61": "z-flip7",
        }
        code = re.search(r"\bsm-f7(11|21|31|41|61)[a-z]*\b", blob)
        return mapping.get(code.group(1), "") if code else ""
    return ""


def _extract_foldable(blob: str) -> str:
    model_number = _foldable_from_model_number(blob)
    if model_number:
        return model_number

    match = re.search(r"\b(?:galaxy\s*)?z\s*fold\s*(\d)\b", blob)
    if match:
        return f"z-fold{match.group(1)}"

    match = re.search(r"\b(?:galaxy\s*)?z\s*flip\s*(\d)\b", blob)
    if match:
        return f"z-flip{match.group(1)}"

    if re.search(r"\bz\s*fold\b|\bfold\b", blob):
        return ""
    if re.search(r"\bz\s*flip\b|\bflip\b", blob):
        return ""
    return ""


def _extract_s_or_note(blob: str) -> str:
    blob = re.sub(r"\bs(\d{1,2})\+", r"s\1 plus", blob)
    blob = re.sub(r"\bnote(\d{1,2})\+", r"note\1 plus", blob)
    match = re.search(r"\b(?:galaxy\s*)?s\s*(\d{1,2})\s*(ultra|fe|plus|\+)?\b", blob)
    if not match:
        match = re.search(r"\bs(\d{1,2})(ultra|fe|plus|\+)?\b", blob)
    if match:
        generation = match.group(1)
        tier = match.group(2) or ""
        if not tier:
            tail = blob[match.end() : match.end() + 36]
            tier_match = re.search(r"\b(ultra|fe|plus|\+)\b", tail)
            tier = tier_match.group(1) if tier_match else ""
        tier = "plus" if tier == "+" else tier
        return "-".join(part for part in [f"s{generation}", tier] if part)

    match = re.search(r"\b(?:galaxy\s*)?note\s*(\d{1,2})\s*(ultra|plus|\+)?\b", blob)
    if not match:
        match = re.search(r"\bnote(\d{1,2})(ultra|plus|\+)?\b", blob)
    if match:
        tier = "plus" if match.group(2) == "+" else (match.group(2) or "")
        return "-".join(part for part in [f"note{match.group(1)}", tier] if part)

    return ""


def _extract_budget_phone(blob: str) -> str:
    match = re.search(r"\b(?:galaxy\s*)?([am])\s*(\d{2})(s)?\b", blob)
    if match:
        suffix = "s" if match.group(3) else ""
        return f"{match.group(1)}{match.group(2)}{suffix}"
    match = re.search(r"\b([am]\d{2}s?)\b", blob)
    return match.group(1) if match else ""


def _extract_tab(blob: str) -> str:
    if not re.search(r"\b(tab|tablet)\b", blob):
        return ""

    line = ""
    match = re.search(r"\btab\s*([as])\s*(\d{1,2})\s*(ultra|plus|\+|fe)?\b", blob)
    if match:
        tier = "plus" if match.group(3) == "+" else (match.group(3) or "")
        line = "-".join(part for part in [f"tab-{match.group(1)}{match.group(2)}", tier] if part)
    else:
        match = re.search(r"\bgalaxy\s*tab\s*([as])(\d{1,2})(ultra|plus|\+|fe)?\b", blob)
        if match:
            tier = "plus" if match.group(3) == "+" else (match.group(3) or "")
            line = "-".join(part for part in [f"tab-{match.group(1)}{match.group(2)}", tier] if part)

    return line


def _connectivity_for_model(model: str, attrs: Mapping[str, Any], title: str) -> str:
    if not _has_5g(attrs, title):
        return ""
    if re.match(r"^[am]\d{2}s?$", model):
        return "5g"
    return ""


def _wifi_or_cellular(attrs: Mapping[str, Any], title: str) -> str:
    blob = _blob(attrs, title)
    if re.search(r"\b(cellular|5g|lte|4g|sim)\b", blob):
        return "cellular"
    if re.search(r"\b(wifi|wi-fi)\b", blob):
        return "wifi"
    return ""


def samsung_model_key(
    attrs: Mapping[str, Any],
    title: str = "",
) -> Optional[str]:
    """
    Build strict Samsung product identity only.

    Condition is stored separately in auction_listings.condition_grade.
    Unclear Samsung rows return `unknown` rather than broad comp buckets.
    """

    if not _is_samsung(attrs, title):
        return UNKNOWN_KEY

    blob = _blob(attrs, title)
    if _is_junk(blob):
        return UNKNOWN_KEY

    storage = _storage(attrs, title)

    tab = _extract_tab(blob)
    if tab:
        connectivity = _wifi_or_cellular(attrs, title)
        if not storage:
            return UNKNOWN_KEY
        return "-".join(part for part in ["samsung", "galaxy", tab, storage, connectivity] if part)

    model = _extract_foldable(blob) or _extract_s_or_note(blob) or _extract_budget_phone(blob)
    if not model or not storage:
        return UNKNOWN_KEY

    connectivity = _connectivity_for_model(model, attrs, title)
    return "-".join(part for part in ["samsung", "galaxy", model, storage, connectivity] if part)
