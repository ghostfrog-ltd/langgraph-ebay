from __future__ import annotations

import re
from typing import Any, Mapping, Optional


UNKNOWN_KEY = "unknown"


def _clean(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip().lower()


def _attr_blob(attrs: Mapping[str, Any]) -> str:
    parts: list[str] = []
    for value in attrs.values():
        if isinstance(value, (list, tuple, set)):
            parts.extend(_clean(item) for item in value)
        else:
            parts.append(_clean(value))
    return " ".join(part for part in parts if part)


def _text(attrs: Mapping[str, Any], title: str) -> str:
    return f"{_clean(title)} {_attr_blob(attrs)}".strip()


def _is_accessory_or_non_camera(text: str) -> bool:
    return any(
        term in text
        for term in (
            "lens mod",
            "max lens",
            "media mod",
            "display mod",
            "light mod",
            "charger only",
            "battery only",
            "case only",
            "mount only",
            "housing only",
            "frame only",
            "floaty",
            "strap",
            "harness",
            "chest mount",
            "head strap",
            "waterproof case",
            "protective case only",
            "screen protector",
            "nd filter",
            "filter set",
            "mic adapter",
            "drone only",
            "gimbal only",
            "karma drone",
            "mavic",
            "phantom",
            "mini 4 pro",
            "mini 3 pro",
        )
    )


def _storage(title: str, attrs: Mapping[str, Any]) -> str:
    text = " ".join(
        [
            _clean(title),
            _clean(attrs.get("Flash Memory Installed")),
            _clean(attrs.get("Storage Capacity")),
            _clean(attrs.get("storage_gb")),
        ]
    )
    if re.search(r"\b128\s?gb\b", text):
        return "128gb"
    if re.search(r"\b64\s?gb\b", text):
        return "64gb"
    if re.search(r"\b32\s?gb\b", text):
        return "32gb"
    return ""


def _gopro_key(title_text: str, text: str) -> str:
    if "gopro" not in text and "hero" not in text:
        return UNKNOWN_KEY

    if re.search(r"\bgopro\s+max\b|\bmax\s+360\b", text):
        return "gopro-max"
    if "fusion" in text:
        return "gopro-fusion"

    if "hero 2018" in text or re.search(r"\bhero\s+4k\b", title_text):
        return "gopro-hero-2018"

    match = re.search(r"\bhero\s*([0-9]{1,2})(?:\s*(?:\+|plus))?(?=\b|\s|$)", text)
    if not match:
        return UNKNOWN_KEY

    generation = int(match.group(1))
    if generation in {8, 9, 10, 11, 12, 13}:
        if "mini" in title_text and generation in {11, 13}:
            return f"gopro-hero{generation}-mini"
        return f"gopro-hero{generation}"

    if generation == 7:
        if "silver" in title_text:
            return "gopro-hero7-silver"
        if "white" in title_text:
            return "gopro-hero7-white"
        if "black" in title_text:
            return "gopro-hero7-black"
        return UNKNOWN_KEY

    if generation == 6:
        return "gopro-hero6-black"

    if generation == 5:
        if "session" in title_text:
            return "gopro-hero5-session"
        return "gopro-hero5-black"

    if generation == 4:
        if "session" in title_text:
            return "gopro-hero4-session"
        if "silver" in title_text:
            return "gopro-hero4-silver"
        if "black" in title_text:
            return "gopro-hero4-black"
        return UNKNOWN_KEY

    if generation == 3:
        if "+" in text or "plus" in text:
            return "gopro-hero3plus"
        return "gopro-hero3"

    return UNKNOWN_KEY


def _dji_key(title_text: str, text: str) -> str:
    if not any(term in text for term in ("dji", "osmo", "action 2", "action 3", "action 4", "action 5", "action 6")):
        return UNKNOWN_KEY

    if re.search(r"\bosmo\s+pocket\s*3\b|\bpocket\s*3\b", text):
        return "dji-osmo-pocket-3"
    if re.search(r"\bosmo\s*360\b", text):
        return "dji-osmo-360"
    if re.search(r"\baction\s*2\b", text):
        return "dji-action-2"

    match = re.search(r"\b(?:osmo\s+)?action\s*([3-6])\b", text)
    if not match:
        return UNKNOWN_KEY

    generation = match.group(1)
    if generation == "5":
        return "dji-osmo-action-5-pro" if "pro" in title_text else UNKNOWN_KEY
    return f"dji-osmo-action-{generation}"


def _insta360_go_key(title_text: str, attrs: Mapping[str, Any]) -> str:
    storage = _storage(title_text, attrs)
    if re.search(r"\bgo\s*ultra\b", title_text):
        return "insta360-go-ultra"
    if re.search(r"\bgo\s*3s\b", title_text):
        return f"insta360-go-3s-{storage}" if storage else UNKNOWN_KEY
    if re.search(r"\bgo\s*3\b", title_text):
        return f"insta360-go-3-{storage}" if storage else UNKNOWN_KEY
    if re.search(r"\bgo\s*2\b", title_text):
        return "insta360-go-2"
    return UNKNOWN_KEY


def _insta360_key(title_text: str, text: str, attrs: Mapping[str, Any]) -> str:
    if "insta360" not in text and "insta 360" not in text:
        return UNKNOWN_KEY

    if re.search(r"\bone\s*x2\b", title_text):
        return "insta360-one-x2"
    if re.search(r"\bone\s*x\b", title_text):
        return "insta360-one-x"
    if re.search(r"\bone\s*rs\b", title_text):
        if "twin" in title_text:
            return "insta360-one-rs-twin"
        return "insta360-one-rs"
    if re.search(r"\bone\s*r\b", title_text):
        if "twin" in title_text:
            return "insta360-one-r-twin"
        return "insta360-one-r"

    for generation in ("5", "4", "3", "2"):
        if re.search(rf"\bx\s*{generation}\b", title_text):
            return f"insta360-x{generation}"

    if re.search(r"\bace\s*pro\s*2\b", title_text):
        return "insta360-ace-pro-2"
    if re.search(r"\bace\s*pro\b", title_text):
        return "insta360-ace-pro"
    if re.search(r"\bace\b", title_text):
        return "insta360-ace"

    go_key = _insta360_go_key(title_text, attrs)
    if go_key != UNKNOWN_KEY:
        return go_key

    return UNKNOWN_KEY


def _secondary_key(title_text: str) -> str:
    if "akaso" in title_text:
        if re.search(r"\bek7000\s+pro\b", title_text):
            return "akaso-ek7000-pro"
        if "ek7000" in title_text:
            return "akaso-ek7000"
        if "brave 7 le" in title_text:
            return "akaso-brave-7-le"
        if "brave 7" in title_text:
            return "akaso-brave-7"
        if re.search(r"\bv50\b", title_text):
            return "akaso-v50"
    if "drift ghost xl" in title_text:
        return "drift-ghost-xl"
    if "sjcam c300" in title_text:
        return "sjcam-c300"
    return UNKNOWN_KEY


def camera_drone_model_key(attrs: Mapping[str, Any], title: str = "") -> Optional[str]:
    safe_attrs = attrs or {}
    title_text = _clean(title)
    text = _text(safe_attrs, title)

    if _is_accessory_or_non_camera(text):
        return UNKNOWN_KEY

    for key in (
        _gopro_key(title_text, text),
        _dji_key(title_text, text),
        _insta360_key(title_text, text, safe_attrs),
        _secondary_key(title_text),
    ):
        if key != UNKNOWN_KEY:
            return key

    return UNKNOWN_KEY
