from __future__ import annotations

import re
from typing import Any, Mapping, Optional

from utils.condition import _derive_condition_grade

UNKNOWN_KEY = "unknown"

GENERATION_RE = re.compile(r"\b(r5[0-9]|r6[0-1]|f5[4-7]|f60)\b", re.I)
YEAR_RE = re.compile(r"\b(19\d{2}|20[0-2]\d)\b")
ENGINE_CC_RE = re.compile(r"\b(\d{3,4})\s?cc\b", re.I)
ENGINE_L_RE = re.compile(r"\b(\d\.\d)\s?(?:l|ltr|litre)?\b", re.I)
PART_NUMBER_KEYS = (
    "Manufacturer Part Number",
    "Reference OE/OEM Number",
    "OE/OEM Part Number",
    "Classic Part/Note",
)
PART_MAP: tuple[tuple[str, str], ...] = (
    ("headlight", "headlight"),
    ("headlamp", "headlight"),
    ("tail light", "taillight"),
    ("tail lamp", "taillight"),
    ("taillight", "taillight"),
    ("rear light", "rearlight"),
    ("wing mirror", "wingmirror"),
    ("door mirror", "wingmirror"),
    ("mirror", "mirror"),
    ("front bumper", "frontbumper"),
    ("rear bumper", "rearbumper"),
    ("bumper", "bumper"),
    ("bonnet", "bonnet"),
    ("bootlid", "bootlid"),
    ("tailgate", "tailgate"),
    ("door card", "doorcard"),
    ("engine", "engine"),
    ("gearbox", "gearbox"),
    ("ecu", "ecu"),
    ("steering wheel", "steeringwheel"),
    ("alloy wheel", "alloywheel"),
    ("wheel", "wheel"),
    ("seat", "seat"),
)


def _clean(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, list):
        return " ".join(str(v) for v in value if v is not None).strip()
    return str(value).strip()


def _slug(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", value.lower())


def _joined(attrs: Mapping[str, Any], title: str) -> str:
    candidates = [
        title,
        attrs.get("Manufacturer"),
        attrs.get("Brand"),
        attrs.get("Model"),
        attrs.get("Body Type"),
        attrs.get("Engine"),
        attrs.get("Engine Size"),
    ]
    return " ".join(_clean(value) for value in candidates if _clean(value)).lower()


def _is_mini(attrs: Mapping[str, Any], title: str) -> bool:
    blob = _joined(attrs, title)
    make = _clean(attrs.get("Manufacturer") or attrs.get("Brand")).lower()

    if make == "mini":
        return True

    return " mini " in f" {blob} " and (
        "cooper" in blob
        or "countryman" in blob
        or "clubman" in blob
        or "paceman" in blob
        or "john cooper works" in blob
        or "jcw" in blob
        or "mini one" in blob
    )


def _extract_generation(attrs: Mapping[str, Any], title: str) -> str:
    blob = _joined(attrs, title)
    match = GENERATION_RE.search(blob)
    return match.group(1).lower() if match else ""


def _extract_year(attrs: Mapping[str, Any], title: str) -> str:
    blob = " ".join(
        [
            _clean(attrs.get("Year")),
            _clean(attrs.get("Date of 1st Registration")),
            _clean(attrs.get("Year Manufactured")),
            title,
        ]
    )
    match = YEAR_RE.search(blob)
    return match.group(1) if match else ""


def _extract_engine(attrs: Mapping[str, Any], title: str) -> str:
    blob = " ".join(
        [
            _clean(attrs.get("Engine Size")),
            _clean(attrs.get("Engine")),
            _clean(attrs.get("Engine Number")),
            _clean(attrs.get("Drivetrain")),
            title,
        ]
    ).lower()

    cc_match = ENGINE_CC_RE.search(blob)
    if cc_match:
        return f"{cc_match.group(1)}cc"

    litre_match = ENGINE_L_RE.search(blob)
    if litre_match:
        return f"{litre_match.group(1)}l"

    raw_cc = re.search(r"\b(1199|1364|1397|1499|1598|1599|1995|1998)\b", blob)
    if raw_cc:
        value = raw_cc.group(1)
        return f"{value}cc"

    return ""


def _extract_family(attrs: Mapping[str, Any], title: str) -> str:
    blob = _joined(attrs, title)

    family_map = (
        ("countryman", "countryman"),
        ("clubman", "clubman"),
        ("paceman", "paceman"),
        ("roadster", "roadster"),
        ("convertible", "convertible"),
        ("cabrio", "convertible"),
        ("coupe", "coupe"),
        ("hatch", "hatch"),
        ("hatchback", "hatch"),
    )
    for needle, family in family_map:
        if needle in blob:
            return family

    model = _clean(attrs.get("Model")).lower()
    if model == "one":
        return "hatch"

    return ""


def _extract_trim(attrs: Mapping[str, Any], title: str) -> str:
    blob = _joined(attrs, title)
    trim_map = (
        ("john cooper works", "jcw"),
        ("jcw", "jcw"),
        ("cooper sd", "coopersd"),
        ("cooper s", "coopers"),
        ("cooper d", "cooperd"),
        ("cooper", "cooper"),
        ("mini one", "one"),
        (" one ", "one"),
    )
    for needle, trim in trim_map:
        if needle in f" {blob} ":
            return trim
    return ""


def _extract_part_number(attrs: Mapping[str, Any]) -> str:
    for key in PART_NUMBER_KEYS:
        raw = _clean(attrs.get(key))
        if not raw:
            continue
        low = raw.lower()
        if low in {"n/a", "na", "does not apply", "unknown"}:
            continue
        return _slug(raw)[:24]
    return ""


def _extract_part(attrs: Mapping[str, Any], title: str) -> str:
    blob = _joined(attrs, title)
    for needle, token in PART_MAP:
        if needle in blob:
            return token
    return ""


def mini_cooper_model_key(
    attrs: Mapping[str, Any],
    title: str = "",
) -> Optional[str]:
    if not _is_mini(attrs, title):
        return UNKNOWN_KEY

    family = _extract_family(attrs, title)
    trim = _extract_trim(attrs, title)
    generation = _extract_generation(attrs, title)
    engine = _extract_engine(attrs, title)
    year = _extract_year(attrs, title)
    part = _extract_part(attrs, title)
    part_number = _extract_part_number(attrs)
    grade = _derive_condition_grade(attrs, title)

    base_parts = ["mini"]

    if family:
        base_parts.append(family)
    if trim and trim not in base_parts:
        base_parts.append(trim)
    if generation:
        base_parts.append(generation)

    if part:
        base_parts.append(part)
        if part_number:
            base_parts.append(part_number)
    else:
        if engine:
            base_parts.append(engine)
        if year:
            base_parts.append(year)

    if len(base_parts) == 1:
        return UNKNOWN_KEY

    return f"{'-'.join(base_parts)}_{grade}"
