# agent/model_keys/pokemon.py
from __future__ import annotations

import re
from typing import Any, Mapping, Optional

from utils.condition import _derive_condition_grade

UNKNOWN_KEY = "unknown"


def _clean(s: Any) -> str:
    """Basic string cleaner: None -> "", strip whitespace."""
    if s is None: return ""
    if isinstance(s, list):
        return " ".join(str(v).lower() for v in s)
    return str(s).strip()


def _alnum_token(s: str) -> str:
    """Lowercase, keep alphanumerics only."""
    return "".join(ch for ch in s.lower() if ch.isalnum())


def _extract_professional_grade(attrs: Mapping[str, Any], title: str) -> str:
    """
    Identifies professional grading (PSA, BGS, CGC) and numeric scores.
    Scans Autograph Authentication, Professional Grader, and Title.
    """
    # Cascade check for professional grading signals
    auth = _clean(attrs.get("Autograph Authentication"))
    grader = _clean(attrs.get("Professional Grader"))
    search_text = f"{auth} {grader} {title.lower()}"

    # 1. Identify the company
    company = ""
    if "psa" in search_text or "professional sports authenticator" in search_text:
        company = "psa"
    elif "beckett" in search_text or "bgs" in search_text or "bas" in search_text:
        company = "bgs"
    elif "cgc" in search_text:
        company = "cgc"

    if not company:
        return ""

    # 2. Extract the numeric grade (1-10)
    # Looks for patterns like 'PSA 10', 'Grade 9', or 'BGS 9.5'
    grade_match = re.search(rf'{company}\s?(\d{{1,2}}(?:\.\d)?)', search_text)
    if grade_match:
        return f"{company}{grade_match.group(1).replace('.', '')}"

    return company


def _extract_finish(attrs: Mapping[str, Any], title: str) -> str:
    """Extracts card finish/rarity (e.g., 'holo', 'rev', 'vmax')."""
    candidates = [
        _clean(attrs.get("Speciality")),
        _clean(attrs.get("Spezialkarte")),
        _clean(attrs.get("Features")),
        title.lower()
    ]
    blob = " ".join(candidates)

    if "reverse holo" in blob or "rev holo" in blob: return "rev"
    if "holo" in blob: return "holo"
    if "vmax" in blob: return "vmax"
    if "vstar" in blob: return "vstar"
    if "gx" in blob: return "gx"
    if " ex" in blob or "ex " in blob: return "ex"
    if "promo" in blob: return "promo"
    return ""


def _normalise_franchise(attrs: Mapping[str, Any]) -> str:
    """Detects franchise, defaulting to 'pokemon' if signals match."""
    candidates = [
        _clean(attrs.get("Franchise")),
        _clean(attrs.get("Game")),
        _clean(attrs.get("Manufacturer")),
    ]
    joined = " | ".join(candidates).lower()
    if any(x in joined for x in ["pokémon", "pokemon", "the pokemon company"]):
        return "pokemon"

    for c in candidates:
        if c: return _alnum_token(c) or "unknown"
    return "unknown"


def _normalise_set(raw_set: Any) -> str:
    """Normalises Set names (e.g., 'Evolving Skies' -> 'evolvingskies')."""
    s = _clean(raw_set)
    if not s or s in {"random", "mix", "mixed"}: return ""
    return _alnum_token(s)


def _extract_card_number(raw: Any) -> str:
    """Extracts stable card number tokens like '151-165'."""
    s = _clean(raw)
    if not s or "cards" in s: return ""

    m = re.search(r"(\d{1,4})\s*/\s*(\d{1,4})", s)
    if m:
        return f"{m.group(1).zfill(3)}-{m.group(2).zfill(3)}"

    compact = "".join(ch for ch in s.upper() if ch.isalnum())
    if len(compact) >= 3 and any(ch.isdigit() for ch in compact):
        return compact
    return ""


def _compress_card_name(raw: Any) -> str:
    """Compresses card names (e.g., 'Charizard ex' -> 'charizardex')."""
    s = _clean(raw)
    if not s or s in {"pokemon", "energy", "foil", "hit"}: return ""

    low = re.sub(r"[^a-z0-9]+", " ", s.lower()).strip()
    tokens = [t for t in low.split() if t and t not in {"the", "and", "of", "to"}]

    if not tokens: return ""
    res = tokens[:2]
    suffixes = {"ex", "v", "vmax", "vstar", "gx", "promo"}
    for t in tokens[2:]:
        if t in suffixes:
            res.append(t)
            break
    return "".join(res)


def pokemon_model_key(attrs: Mapping[str, Any], title: str = "") -> Optional[str]:
    """
    Builds canonical card key: {franchise}-{set}-{cardcore}-{finish}{_lang?}_{grade}
    """
    franchise = _normalise_brand = _normalise_franchise(attrs)
    set_token = _normalise_set(attrs.get("Set"))

    card_num = _extract_card_number(attrs.get("Card Number"))
    card_name = _compress_card_name(attrs.get("Card Name"))
    card_core = card_num or card_name

    if not card_core:
        card_core = _extract_card_number(title) or _compress_card_name(title)

    if not card_core: return UNKNOWN_KEY

    # Specs Layer
    finish = _extract_finish(attrs, title)
    pro_grade = _extract_professional_grade(attrs, title)
    lang = _clean(attrs.get("Language"))[:2] if attrs.get("Language") else ""

    base_parts = [franchise]
    if set_token: base_parts.append(set_token)
    base_parts.append(card_core)
    if finish: base_parts.append(finish)

    base_key = "-".join(base_parts)
    if lang: base_key += f"_{lang}"

    # Condition & Professional Grade
    condition_grade = _derive_condition_grade(attrs, title)
    final_grade = f"{condition_grade}-{pro_grade}" if pro_grade else condition_grade

    return f"{base_key}_{final_grade}"