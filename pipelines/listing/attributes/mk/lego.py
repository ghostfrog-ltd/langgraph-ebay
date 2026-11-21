from __future__ import annotations

import re
from typing import Any, Mapping, Optional


UNKNOWN_KEY = "unknown"

MINIFIG_THEME_MAP = {
    "sw": "star-wars",
    "sh": "super-heroes",
    "hp": "harry-potter",
    "col": "collectible-minifigures",
    "loc": "legends-of-chima",
    "mk": "monkie-kid",
    "mo": "monkie-kid",
    "njo": "ninjago",
    "njoa": "ninjago",
    "pj": "ninjago",
    "poc": "pirates-of-the-caribbean",
    "pop": "prince-of-persia",
    "spd": "spiderman",
    "toy": "toy-story",
    "u": "ultra-agents",
    "vid": "vidiyo",
    "v": "vidiyo",
    "ww": "western",
    "w": "western",
    "sp": "space",
    "cas": "castle",
}


def _clean(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, (list, tuple, set)):
        return " ".join(_clean(item) for item in value)
    return str(value).strip().lower()


def _attr_blob(attrs: Mapping[str, Any]) -> str:
    return " ".join(_clean(value) for value in attrs.values() if _clean(value))


def _blob(attrs: Mapping[str, Any], title: str) -> str:
    return f"{_clean(title)} {_attr_blob(attrs)}".strip()


def _is_not_real_lego(text: str) -> bool:
    return any(
        term in text
        for term in (
            "non lego",
            "non-lego",
            "not lego",
            "lego compatible",
            "compatible with lego",
            "lego style",
            "lego type",
            "building block set",
            "building blocks",
            "mega bloks",
            "megabloks",
            "lepin",
            "decool",
            "sluban",
            "mould king",
            "cada ",
            "qing yi",
        )
    )


def _is_bulk_or_joblot(text: str) -> bool:
    return any(
        term in text
        for term in (
            "job lot",
            "joblot",
            "bulk lego",
            "bulk bricks",
            "mixed lego",
            "random lego",
            "mystery box",
            "uncounted",
        )
    ) or bool(re.search(r"\b\d+(?:\.\d+)?\s?kg\b", text))


def _is_partial_or_accessory(text: str) -> bool:
    return any(
        term in text
        for term in (
            "promo",
            "promotional",
            "incomplete",
            "partial",
            "missing pieces",
            "missing parts",
            "no minifigure",
            "no minifigures",
            "no mini figures",
            "without minifigures",
            "without minifigs",
            "build only",
            "builds only",
            "instructions only",
            "instructions and stickers only",
            "instruction book only",
            "manual only",
            "stickers only",
            "box only",
            "empty box",
            "custom",
            "replacement sticker",
            "sticker sheet only",
            "spare parts",
            "parts only",
            "minifigure only",
            "minifig only",
        )
    )


def _is_sub_build_without_whole_set_signal(text: str) -> bool:
    sub_build_terms = (
        "castle byers",
    )
    whole_set_terms = (
        "complete set",
        "complete lego set",
        "full set",
        "sealed set",
        "new sealed",
        "new and sealed",
        "brand new sealed",
    )
    return any(term in text for term in sub_build_terms) and not any(
        term in text for term in whole_set_terms
    )


def _is_minifigure_listing(attrs: Mapping[str, Any], title: str) -> bool:
    text = _blob(attrs, title)
    return any(
        term in text
        for term in (
            "minifigure",
            "minifig",
            "mini figure",
            "item type minifigure",
            "part type minifigure",
        )
    )


def _parse_minifig_id(text: str) -> str:
    match = re.search(r"\b([a-z]{1,5})(\d{3,5}[a-z]?)\b", text)
    if not match:
        return ""
    prefix, ident = match.groups()
    theme = MINIFIG_THEME_MAP.get(prefix, prefix)
    return f"lego-minifig-{theme}-{ident}"


def _valid_set_number(candidate: str) -> bool:
    if not candidate or not candidate.isdigit():
        return False
    if len(candidate) < 4 or len(candidate) > 7:
        return False
    as_int = int(candidate)
    return not (1950 <= as_int <= 2035)


def _candidate_numbers(text: str) -> list[str]:
    found: list[str] = []
    for raw in re.findall(r"\b(\d{4,7})\b", text):
        if _valid_set_number(raw) and raw not in found:
            found.append(raw)
    return found


def _parse_set_number_from_attrs(attrs: Mapping[str, Any]) -> str:
    candidate_keys = (
        "LEGO Set Number",
        "MPN",
        "Manufacturer Part Number",
        "Model Number",
        "Model",
        "LEGO Set Number:",
        "Numéro de l'assortiment LEGO",
        "NumÃ©ro de l'assortiment LEGO",
        "Herstellernummer",
    )
    for key in candidate_keys:
        raw = _clean(attrs.get(key))
        if not raw or "does not apply" in raw or "not apply" in raw:
            continue

        numbers = _candidate_numbers(raw)
        if len(numbers) == 1:
            return numbers[0]
    return ""


def _parse_set_number_from_title(title: str) -> str:
    title_text = _clean(title)

    explicit: list[str] = []
    for pattern in (
        r"\b(?:lego|set|model|item)\s*(?:set)?\s*[:#-]?\s*(\d{4,7})\b",
        r"\((\d{4,7})\)",
    ):
        for match in re.finditer(pattern, title_text):
            number = match.group(1)
            if _valid_set_number(number) and number not in explicit:
                explicit.append(number)

    if len(explicit) == 1:
        all_numbers = _candidate_numbers(title_text)
        extra = [number for number in all_numbers if number != explicit[0]]
        if extra and any(token in title_text for token in ("+", " and ", "&", "bundle", "x2")):
            return ""
        return explicit[0]

    all_numbers = _candidate_numbers(title_text)
    if len(all_numbers) == 1:
        return all_numbers[0]
    return ""


def lego_model_key(attrs: Mapping[str, Any], title: str = "") -> Optional[str]:
    safe_attrs = attrs or {}
    text = _blob(safe_attrs, title)

    if (
        _is_not_real_lego(text)
        or _is_bulk_or_joblot(text)
        or _is_partial_or_accessory(text)
        or _is_sub_build_without_whole_set_signal(_clean(title))
    ):
        return UNKNOWN_KEY

    if _is_minifigure_listing(safe_attrs, title):
        return _parse_minifig_id(text) or UNKNOWN_KEY

    set_number = _parse_set_number_from_attrs(safe_attrs) or _parse_set_number_from_title(title)
    if not set_number:
        return UNKNOWN_KEY

    return f"lego-set-{set_number}"
