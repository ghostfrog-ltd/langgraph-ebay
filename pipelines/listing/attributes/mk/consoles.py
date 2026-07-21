from __future__ import annotations
import re
from typing import Mapping, Any, Optional
from utils.condition import _derive_condition_grade


def _as_text(val: Any) -> str:
    if val is None: return ""
    return str(val).strip().lower()


def _extract_storage(attrs: Mapping[str, Any], title: str) -> str:
    """
    Extracts capacity to differentiate models like Xbox Series S 512GB vs 1TB.
    """
    # Scan Storage Capacity and Size attributes
    attr_storage = _as_text(attrs.get("Storage Capacity")) + _as_text(attrs.get("Size"))
    search_text = f"{attr_storage} {title.lower()}"

    if re.search(r'2\s?tb', search_text): return "2tb"
    if re.search(r'1\s?tb', search_text): return "1tb"
    if re.search(r'825\s?gb', search_text): return "825gb"  # Base PS5 Spec
    if re.search(r'512\s?gb', search_text): return "512gb"
    if re.search(r'500\s?gb', search_text): return "500gb"
    if re.search(r'128\s?gb', search_text): return "128gb"
    return ""


def _is_console_type(attrs: Mapping[str, Any], title: str = "") -> bool:
    t = _as_text(attrs.get("Type"))
    model = _as_text(attrs.get("Model"))
    platform = _as_text(attrs.get("Platform"))
    title_low = _as_text(title)
    blob = f"{t} {model} {platform} {title_low}"

    # Negative signals from the dataset
    exclude = {"controller", "remote player", "portal", "disc drive", "adapter", "pedals", "wheel"}
    if any(x in t for x in exclude) or any(x in model for x in exclude):
        return False
    if any(
        sig in blob
        for sig in (
            "box only",
            "case only",
            "no console",
            "empty box",
            "packaging only",
            "controller only",
            "dock only",
            "charging dock",
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
            "disc drive attachment",
            "disc drive blu ray attachment",
            "disc only",
            "game only",
            "code in a box",
            "download code",
        )
    ):
        return False

    include = {"console", "handheld system", "gaming system"}
    if any(x in t for x in include):
        return True
    if any(k in attrs for k in ("Game Name", "Video Game Name", "Genre", "Rating")):
        return False

    # Normalised live attributes do not currently keep the eBay Type field.
    # Fall back to title/platform signals, but only when the listing looks like
    # hardware rather than a standalone game or accessory.
    game_only = bool(
        re.search(
            r"\b(game|cartridge|disc|artwork|steelbook|cover\s+is|pal\s+english)\b",
            blob,
        )
    )
    title_has_console = any(sig in title_low for sig in ("console", "system", "handheld"))
    known_hardware_model = any(
        sig in f"{model} {title_low}"
        for sig in (
            "ps5",
            "playstation 5",
            "ps4",
            "playstation 4",
            "xbox series",
            "xbox one",
            "nintendo switch",
        )
    )
    hardware_signal = title_has_console or any(
        sig in title_low
        for sig in (
            "disc edition",
            "disk edition",
            "digital edition",
            "blu ray edition",
            "blu-ray edition",
            "with controller",
            "console only",
            "oled model",
        )
    )
    hardware_signal = hardware_signal or bool(
        re.search(r"\b(128|256|500|512|825)\s?gb\b|\b(1|2)\s?tb\b", blob)
    )
    return known_hardware_model and hardware_signal and not game_only


def console_or_game_model_key(attrs: Mapping[str, Any], title: str) -> Optional[str]:
    if not attrs or not _is_console_type(attrs, title):
        return "unknown"

    brand = _as_text(attrs.get("Brand"))
    model = _as_text(attrs.get("Model"))
    blob = f"{brand} {model} {title.lower()}"
    base_key = "unknown"

    # Mapping Logic
    if "playstation" in blob or "ps5" in blob or "ps4" in blob:
        if "ps5" in blob:
            base_key = "ps5"
        elif "ps4" in blob:
            base_key = "ps4"
        elif "ps3" in blob:
            base_key = "ps3"
        elif "vita" in blob:
            base_key = "ps_vita"
    elif "xbox" in blob:
        if "series" in blob:
            base_key = "xbox_series"
        elif "one" in blob:
            base_key = "xbox_one"
        elif "360" in blob:
            base_key = "xbox_360"
    elif "switch" in blob:
        base_key = "switch_2" if "switch 2" in blob else "switch"

    if base_key == "unknown": return "unknown"

    if base_key == "switch":
        if "tablet only" in blob or "console/tablet only" in blob or "screen only" in blob:
            base_key = "switch_tablet"
        elif "oled" in blob:
            base_key = "switch_oled"
        elif "lite" in blob:
            base_key = "switch_lite"

    if base_key == "ps4":
        if "ps4 pro" in blob or "playstation 4 pro" in blob:
            base_key = "ps4_pro"
        elif "ps4 slim" in blob or "playstation 4 slim" in blob:
            base_key = "ps4_slim"

    # Append storage for modern units
    if base_key.startswith(("ps4", "ps5", "xbox_series", "xbox_one", "switch")):
        storage = _extract_storage(attrs, title)
        if storage: base_key += f"_{storage}"

    grade = _derive_condition_grade(attrs, title)
    return f"{base_key}_{grade}"
