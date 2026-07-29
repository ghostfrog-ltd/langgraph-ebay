from __future__ import annotations

import re
from typing import Any, Mapping, Optional


UNKNOWN_KEY = "unknown"


def _as_text(val: Any) -> str:
    if val is None:
        return ""
    return str(val).strip().lower()


def _attr_blob(attrs: Mapping[str, Any]) -> str:
    parts: list[str] = []
    for value in attrs.values():
        if isinstance(value, (list, tuple, set)):
            parts.extend(_as_text(item) for item in value)
        else:
            parts.append(_as_text(value))
    return " ".join(part for part in parts if part)


def _blob(attrs: Mapping[str, Any], title: str) -> str:
    return f"{_as_text(title)} {_attr_blob(attrs)}".strip()


def _extract_storage(attrs: Mapping[str, Any], title: str) -> str:
    attr_storage = " ".join(
        _as_text(attrs.get(key))
        for key in (
            "Storage Capacity",
            "Storage",
            "Size",
            "Hard Drive Capacity",
            "storage_gb",
        )
    )
    text = f"{attr_storage} {_as_text(title)}"

    if re.search(r"\b2\s?tb\b", text):
        return "2tb"
    if re.search(r"\b1\s?tb\b", text):
        return "1tb"
    if re.search(r"\b825\s?gb\b", text):
        return "825gb"
    if re.search(r"\b512\s?gb\b", text):
        return "512gb"
    if re.search(r"\b500\s?gb\b", text):
        return "500gb"
    legacy_match = re.search(r"\b(320|250|120|60|40|20|12)\s?gb\b", text)
    if legacy_match:
        return f"{legacy_match.group(1)}gb"
    return ""


def _has_game_attrs(attrs: Mapping[str, Any]) -> bool:
    return any(key in attrs for key in ("Game Name", "Video Game Name", "Genre", "Rating"))


def _is_accessory_or_part(text: str) -> bool:
    return any(
        term in text
        for term in (
            "box only",
            "case only",
            "no console",
            "empty box",
            "packaging only",
            "controller only",
            "controller pair",
            "controller pairs",
            "wireless controller",
            "power a controller",
            "dock only",
            "charging dock",
            "accessory kit",
            "accessories kit",
            "sports accessories",
            "replacement shell",
            "replacement case",
            "housing shell",
            "faceplate",
            "cover plate",
            "thumbstick",
            "joystick",
            "hdmi port",
            "power supply",
            "charging cable",
            "skin sticker",
            "guide button",
            "led sticker",
            "playstation portal",
            "remote player",
            "cronus zen",
            "joycons bundle",
            "joy-con bundle",
            "joycon only",
            "joy-cons only",
            "joycon pair",
            "joy-con pair",
            "joy-con controller",
            "disc drive attachment",
            "disc drive blu ray attachment",
            "game only",
            "code in a box",
            "download code",
            "digital code",
        )
    )


def _looks_like_game_only(text: str, attrs: Mapping[str, Any]) -> bool:
    if _has_game_attrs(attrs):
        return True

    if any(
        term in text
        for term in (
            "cartridge",
            "steelbook",
            "artwork",
            "cover is",
            "pal english",
            "game for",
            "ps5 game",
            "ps4 game",
            "ps3 game",
            "xbox game",
            "switch game",
            "nintendo switch game",
        )
    ):
        return True

    hardware_signal = any(
        term in text
        for term in (
            "console",
            "system",
            "handheld",
            "disc edition",
            "disk edition",
            "digital edition",
            "blu ray",
            "blu-ray",
            "with controller",
            "console only",
            "tablet only",
            "oled model",
        )
    ) or bool(re.search(r"\b(500|512|825)\s?gb\b|\b(1|2)\s?tb\b", text))
    platform_only = any(
        term in text
        for term in (
            "playstation 5",
            "playstation 4",
            "playstation 3",
            "playstation vita",
            "ps5",
            "ps4",
            "ps3",
            "ps vita",
            "xbox one",
            "xbox series",
            "nintendo switch",
        )
    )
    return platform_only and not hardware_signal


def _playstation_key(title_text: str, text: str, storage: str) -> str:
    if not any(term in text for term in ("playstation", "ps5", "ps4", "ps3", "vita")):
        return UNKNOWN_KEY

    if re.search(r"\b(playstation\s*1|ps1|ps\s*one)\b", title_text):
        return UNKNOWN_KEY
    if re.search(r"\b(playstation\s*2|ps2)\b", title_text):
        return UNKNOWN_KEY

    if "vita" in text or "ps vita" in text or "playstation vita" in text:
        return "sony-playstation-vita"

    if "ps3" in text or "playstation 3" in text:
        return "sony-playstation-3"

    if "ps5" in text or "playstation 5" in text:
        if "pro" in text:
            return "sony-playstation-5-pro-2tb"

        slim = "slim" in text
        digital = "digital" in text
        disc = any(term in text for term in ("disc edition", "disk edition", "blu ray", "blu-ray"))
        if not storage or not (digital or disc):
            return UNKNOWN_KEY

        if slim and storage != "1tb":
            return UNKNOWN_KEY
        if not slim and storage != "825gb":
            return UNKNOWN_KEY

        edition = "digital" if digital else "disc"
        if slim:
            return f"sony-playstation-5-slim-{edition}-{storage}"
        return f"sony-playstation-5-{edition}-{storage}"

    if "ps4" in text or "playstation 4" in text:
        if not storage:
            return UNKNOWN_KEY
        if "pro" in text:
            variant = "pro"
        elif "slim" in text:
            variant = "slim"
        else:
            variant = "base"
        if storage not in {"500gb", "1tb", "2tb"}:
            return UNKNOWN_KEY
        return f"sony-playstation-4-{variant}-{storage}"

    return UNKNOWN_KEY


def _xbox_series_variant(title_text: str, text: str) -> str:
    for candidate in (title_text, text):
        series_x = bool(re.search(r"\bseries\s*x\b|\bxbox\s*x\b", candidate))
        series_s = bool(re.search(r"\bseries\s*s\b|\bxbox\s*s\b", candidate))
        if series_x and not series_s:
            return "x"
        if series_s and not series_x:
            return "s"
    return ""


def _xbox_one_variant(title_text: str, text: str) -> str:
    for candidate in (title_text, text):
        one_x = bool(re.search(r"\bone\s*x\b", candidate))
        one_s = bool(re.search(r"\bone\s*s\b", candidate))
        if one_x and not one_s:
            return "x"
        if one_s and not one_x:
            return "s"
    return "base"


def _xbox_key(title_text: str, text: str, storage: str) -> str:
    if "xbox" not in text:
        return UNKNOWN_KEY

    if "360" in text:
        if storage:
            return f"microsoft-xbox-360-{storage}"
        return "microsoft-xbox-360"

    if "series" in text:
        if not storage:
            return UNKNOWN_KEY
        variant = _xbox_series_variant(title_text, text)
        if not variant:
            return UNKNOWN_KEY
        if variant == "x" and storage not in {"1tb", "2tb"}:
            return UNKNOWN_KEY
        if variant == "s" and storage not in {"512gb", "1tb"}:
            return UNKNOWN_KEY
        return f"microsoft-xbox-series-{variant}-{storage}"

    if "one" in text:
        if not storage:
            return UNKNOWN_KEY
        variant = _xbox_one_variant(title_text, text)
        if storage not in {"500gb", "1tb", "2tb"}:
            return UNKNOWN_KEY
        return f"microsoft-xbox-one-{variant}-{storage}"

    return UNKNOWN_KEY


def _switch_key(text: str) -> str:
    if "switch" not in text:
        return UNKNOWN_KEY

    if "switch 2" in text:
        return "nintendo-switch-2"
    if "tablet only" in text or "console/tablet only" in text or "screen only" in text:
        return "nintendo-switch-tablet-only"
    if "oled" in text:
        return "nintendo-switch-oled"
    if "lite" in text:
        return "nintendo-switch-lite"
    return "nintendo-switch-base"


def console_or_game_model_key(attrs: Mapping[str, Any], title: str) -> Optional[str]:
    if not attrs:
        return UNKNOWN_KEY

    title_text = _as_text(title)
    text = _blob(attrs, title)
    if _is_accessory_or_part(text) or _looks_like_game_only(text, attrs):
        return UNKNOWN_KEY

    storage = _extract_storage(attrs, title)
    key = _playstation_key(title_text, text, storage)
    if key != UNKNOWN_KEY:
        return key

    key = _xbox_key(title_text, text, storage)
    if key != UNKNOWN_KEY:
        return key

    return _switch_key(text)
