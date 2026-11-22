from __future__ import annotations

from typing import Optional, Mapping, Any

from .bikes import bike_model_key
from .apple import apple_model_key
from .watches import watch_model_key
from .consoles import console_or_game_model_key
from .cameras import camera_drone_model_key
from .tools import tools_model_key
from .motors import motors_model_key
from .lego import lego_model_key
from .pokemon import pokemon_model_key
from .samsung import samsung_model_key
from .headphones import headphones_model_key
from .nc750 import honda_nc750_model_key
from .msx125 import honda_msx125_model_key
from .mini_cooper import mini_cooper_model_key
from .components import (
    camera_lens_model_key,
    cpu_model_key,
    graphics_card_model_key,
    industrial_automation_model_key,
    motherboard_model_key,
)


def _with_normalised_aliases(attrs: Mapping[str, Any], *, source: str) -> Mapping[str, Any]:
    """
    The live attributes graph stores a small normalised dict
    (`brand`, `model_name`, `storage_gb`, etc.), while the source-specific
    key builders historically read raw eBay item-specific names such as
    `Brand`, `Model`, and `Storage Capacity`.

    Add raw-style aliases without overwriting richer raw attributes.
    """
    if not attrs:
        return attrs

    merged = dict(attrs)

    if merged.get("brand") and not merged.get("Brand"):
        merged["Brand"] = merged.get("brand")
    if merged.get("model_name") and not merged.get("Model"):
        merged["Model"] = merged.get("model_name")
    if merged.get("colour") and not merged.get("Colour"):
        merged["Colour"] = merged.get("colour")
    storage_fields = ("Storage Capacity",)
    if source == "ebay-apple":
        storage_fields += ("SSD Capacity", "Hard Drive Capacity", "Capacity")
    if merged.get("storage_gb") and not any(merged.get(field) for field in storage_fields):
        merged["Storage Capacity"] = f"{merged.get('storage_gb')} GB"
    if merged.get("epid") and not merged.get("ePID"):
        merged["ePID"] = merged.get("epid")

    return merged


def _canonicalise_key(key: Optional[str]) -> Optional[str]:
    """
    Standardise model_key formatting:
      - strip whitespace
      - lowercase
      - collapse blank → None
    """
    if not key:
        return None

    key = key.strip().lower()
    if not key:
        return None

    return key


def normalise_model(
        title: str,
        attrs: Optional[Mapping[str, Any]] = None,
        source: str = "",
) -> Optional[str]:
    """
    High-level classifier used everywhere.

    We route by `source` so each domain uses its own specialist:

      - ebay-consoles      -> consoles / games
      - motomine           -> bikes
      - ebay-apple         -> Apple
      - ebay-watches       -> watches
      - ebay-actioncams    -> cameras / drones
      - ebay-tools         -> tools
      - ebay-graphics-cards -> desktop GPUs
      - ebay-cpus          -> desktop CPUs
      - ebay-motherboards  -> desktop motherboards
      - ebay-camera-lenses -> camera lenses
      - ebay-industrial-automation -> industrial automation part numbers

    `attrs`:
      - For structured sources (MotoMine, Trading, etc.) pass the raw attributes dict.
      - For title-only cases (plain Browse listings), you can omit it and we'll
        treat it as an empty dict.
    """
    if not title:
        return None

    source = (source or "").strip().lower()
    safe_attrs: Mapping[str, Any] = _with_normalised_aliases(attrs or {}, source=source)

    if source == "ebay-consoles":
        return _canonicalise_key(
            console_or_game_model_key(attrs=safe_attrs, title=title)
        )

    if source == "motomine":
        return _canonicalise_key(
            bike_model_key(attrs=safe_attrs, title=title)
        )

    if source == "ebay-apple":
        return _canonicalise_key(
            apple_model_key(attrs=safe_attrs, title=title)
        )

    if source == "ebay-watches":
        return _canonicalise_key(
            watch_model_key(attrs=safe_attrs, title=title)
        )

    if source == "ebay-actioncams":
        return _canonicalise_key(
            camera_drone_model_key(attrs=safe_attrs, title=title)
        )

    if source == "ebay-tools":
        return _canonicalise_key(
            tools_model_key(attrs=safe_attrs, title=title)
        )

    if source == "ebay-motors":
        return _canonicalise_key(
            motors_model_key(attrs=safe_attrs, title=title)
        )

    if source == "ebay-lego":
        return _canonicalise_key(
            lego_model_key(attrs=safe_attrs, title=title)
        )

    if source == "ebay-pokemon":
        return _canonicalise_key(
            pokemon_model_key(attrs=safe_attrs, title=title)
        )

    if source == "ebay-samsung":
        return _canonicalise_key(
            samsung_model_key(attrs=safe_attrs, title=title)
        )

    if source == "ebay-headphones":
        return _canonicalise_key(
            headphones_model_key(attrs=safe_attrs, title=title)
        )

    if source == "ebay-honda-nc750":
        return _canonicalise_key(
            honda_nc750_model_key(attrs=safe_attrs, title=title)
        )

    if source == "ebay-honda-msx125":
        return _canonicalise_key(
            honda_msx125_model_key(attrs=safe_attrs, title=title)
        )

    if source == "ebay-mini-cooper":
        return _canonicalise_key(
            mini_cooper_model_key(attrs=safe_attrs, title=title)
        )

    if source == "ebay-graphics-cards":
        return _canonicalise_key(
            graphics_card_model_key(attrs=safe_attrs, title=title)
        )

    if source == "ebay-cpus":
        return _canonicalise_key(
            cpu_model_key(attrs=safe_attrs, title=title)
        )

    if source == "ebay-motherboards":
        return _canonicalise_key(
            motherboard_model_key(attrs=safe_attrs, title=title)
        )

    if source == "ebay-camera-lenses":
        return _canonicalise_key(
            camera_lens_model_key(attrs=safe_attrs, title=title)
        )

    if source == "ebay-industrial-automation":
        return _canonicalise_key(
            industrial_automation_model_key(attrs=safe_attrs, title=title)
        )

    # Unknown source → no classification
    return None
