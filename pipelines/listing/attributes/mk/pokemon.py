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


def _alnum_token(s: str) -> str:
    """Lowercase, keep alphanumerics only."""
    return "".join(ch for ch in s.lower() if ch.isalnum())


def _normalise_franchise(attrs: Mapping[str, Any]) -> str:
    """Pick a stable franchise token.

    This feed sometimes includes non-Pokémon CCG items (Yu-Gi-Oh, MTG, Gundam, etc).
    We attempt to detect Pokémon and otherwise fall back to a compact token.

    Priority:
      1) Franchise
      2) Game
      3) Manufacturer

    If any field clearly indicates Pokémon -> "pokemon"
    """
    candidates = [
        _clean(attrs.get("Franchise")),
        _clean(attrs.get("Game")),
        _clean(attrs.get("Manufacturer")),
    ]

    joined = " | ".join(candidates).lower()
    if "pokémon" in joined or "pokemon" in joined or "the pokemon company" in joined:
        return "pokemon"

    # Otherwise: compact the first non-empty candidate into a token
    for c in candidates:
        if c:
            t = _alnum_token(c)
            return t or "unknown"

    return "unknown"


def _normalise_set(raw_set: Any) -> str:
    """Normalise Set into a compact token (lower alnum, drop separators)."""
    s = _clean(raw_set)
    if not s:
        return ""

    low = s.lower()

    # Remove filler / junk buckets
    if low in {"random", "mix", "mixed"}:
        return ""

    # Clean up separators (keep words glued together)
    return _alnum_token(s)


def _normalise_language(raw_lang: Any) -> str:
    """Optional language token. Empty string means "don't include language"."""
    s = _clean(raw_lang)
    if not s:
        return ""
    t = _alnum_token(s)
    # keep this short + stable
    if t in {"english", "japanese", "korean", "chinese"}:
        return t[:2]  # en/ja/ko/zh
    return ""


def _extract_card_number(raw: Any) -> str:
    """Extract a stable 'card number' token.

    Examples handled:
      - "011/094" -> "011-094"
      - "SV-P-113/SV-P" -> "SVP113" (compact)
      - "SWSH241" -> "SWSH241"
      - "001 to 159" -> "001-159"
      - "60 Cards" -> "" (ignore)
    """
    s = _clean(raw)
    if not s:
        return ""

    low = s.lower()

    # Ignore non-card-number-ish values
    if "card" in low and not re.search(r"\d", s):
        return ""
    if "cards" in low:
        # often bundle counts like "60 Cards"
        if re.search(r"\b\d+\b", s) and not re.search(r"/", s):
            return ""
    if low in {"random", "hit"}:
        return ""

    # Pattern 1: 3/3 or 011/094 style
    m = re.search(r"\b(\d{1,4})\s*/\s*(\d{1,4})\b", s)
    if m:
        a, b = m.group(1).zfill(3), m.group(2).zfill(3)
        return f"{a}-{b}"

    # Pattern 2: ranges "001 to 159", "194/193 - 213/193"
    m = re.search(r"\b(\d{1,4})\s*(?:to|\-|–)\s*(\d{1,4})\b", low)
    if m:
        a, b = m.group(1).zfill(3), m.group(2).zfill(3)
        return f"{a}-{b}"

    # Pattern 3: alnum IDs like "SWSH241", "BLMM-EN014"
    # Keep only A-Z0-9, but strip common noise
    compact = "".join(ch for ch in s.upper() if ch.isalnum())
    # too short / too generic -> ignore
    if len(compact) >= 4 and re.search(r"\d", compact):
        return compact

    return ""


def _compress_card_name(raw: Any) -> str:
    """Compress Card Name into a short family token.

    We do NOT aim for uniqueness here, just bucketing:
      - "Charizard ex" -> "charizardex"
      - "Team Rocket's Giovanni" -> "teamrocketsgiovanni" (compact)
      - "Ash Blossom & Joyous Spring" -> "ashblossom" (first two tokens)

    Strategy:
      - Lowercase
      - Strip punctuation
      - Keep 1-2 meaningful tokens (skip connectors)
    """
    s = _clean(raw)
    if not s:
        return ""

    low = s.lower()

    # Some listings put generic words in Card Name
    if low in {"pokemon", "pokémon", "energy", "foil", "bundle", "hit"}:
        return ""

    # Normalise possessives + punctuation into spaces
    low = re.sub(r"[^a-z0-9]+", " ", low).strip()
    if not low:
        return ""

    tokens = [t for t in low.split() if t]
    if not tokens:
        return ""

    STOP = {"and", "or", "the", "a", "an", "of", "to", "with"}
    tokens = [t for t in tokens if t not in STOP]
    if not tokens:
        return ""

    # Keep at most 2 tokens, but preserve suffixes like ex/v/vmax if present
    keep = tokens[:2]

    suffixes = {"ex", "v", "vmax", "vstar", "gx", "tagteam", "promo"}
    if tokens:
        for tok in tokens[2:]:
            if tok in suffixes:
                keep.append(tok)
                break

    return "".join(keep)


def pokemon_model_key(
    attrs: Mapping[str, Any],
    title: str = "",
) -> Optional[str]:
    """Build a canonical model key for trading card listings (source='ebay-pokemon').

    Output format (console-style):

        {franchise}-{set}-{cardcore}{_lang?}_{grade}

    Where:
      - franchise: usually "pokemon" (but we degrade gracefully if feed includes other games)
      - set: compact token from attrs["Set"]
      - cardcore: prefer Card Number token; else compressed Card Name token
      - lang: optional 2-letter language (en/ja/ko/zh) when present
      - grade: from _derive_condition_grade(attrs, title)

    Examples:
        Franchise="Pokemon", Set="SV: Scarlet & Violet 151", Card Number="199/165"
            -> "pokemon-svscarletviolet151-199-165_B"

        Manufacturer="The Pokemon Company", Set="Evolving Skies", Card Name="Umbreon"
            -> "pokemon-evolvingskies-umbreon_B"

        Game="Yu-Gi-Oh! TCG", Set="PHANTASMAL FLAMES", Card Number="PHRE-EN024"
            -> "yugiohtcg-phantasmalflames-PHREEN024_B"

    Rules:
    - Uses attrs["Set"] + (Card Number OR Card Name) and optional Language
    - If set is missing, still tries to key by franchise + cardcore
    - If we can't extract any meaningful identity -> returns UNKNOWN_KEY
    """
    franchise = _normalise_franchise(attrs)
    set_token = _normalise_set(attrs.get("Set"))
    lang = _normalise_language(attrs.get("Language"))

    card_num = _extract_card_number(attrs.get("Card Number"))
    card_name = _compress_card_name(attrs.get("Card Name"))

    card_core = card_num or card_name
    if not card_core:
        # try a last-ditch parse from title (common patterns like "199/165" or "SWSH241")
        card_core = _extract_card_number(title) or _compress_card_name(title)

    if not franchise or franchise == "unknown" and not card_core:
        return UNKNOWN_KEY
    if not card_core:
        return UNKNOWN_KEY

    base = franchise
    if set_token:
        base = f"{base}-{set_token}"
    base = f"{base}-{card_core}"

    if lang:
        base = f"{base}_{lang}"

    grade = _derive_condition_grade(attrs, title)
    return f"{base}_{grade}"
