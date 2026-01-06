# agent/model_keys/headphones.py
from __future__ import annotations

import re
from typing import Any, Mapping, Optional

from utils.condition import _derive_condition_grade

UNKNOWN_KEY = "unknown"


def _clean(s: Any) -> str:
    """Basic string cleaner: None -> "", strip whitespace."""
    if s is None:
        return ""
    if isinstance(s, list):
        return " ".join(str(v).lower() for v in s)
    return str(s).strip()


def _strip_parentheses(s: str) -> str:
    """Remove anything inside parentheses, including parentheses."""
    out = []
    depth = 0
    for ch in s:
        if ch == "(":
            depth += 1
            continue
        if ch == ")":
            depth = max(depth - 1, 0)
            continue
        if depth == 0:
            out.append(ch)
    return "".join(out)


def _normalise_brand(raw: Any) -> str:
    """Normalise Brand into a compact token (lowercase alnum only)."""
    s = _clean(raw)
    if not s or s.lower() in {"branded", "unbranded", "does not apply"}:
        return ""

    low = s.lower()
    # Handle brand variations seen in data
    if "beats" in low: return "beats"
    if "soundcore" in low: return "soundcore"
    if "skullcandy" in low: return "skullcandy"

    out = []
    for ch in low:
        if ch.isalnum():
            out.append(ch)
    return "".join(out)


def _tokenise_model(s: str) -> list[str]:
    s = _strip_parentheses(s)
    # Replace slashes and hyphens with spaces to preserve tokens
    s = s.replace("/", " ").replace("\\", " ").replace("-", " ")
    s = " ".join(s.split())

    toks: list[str] = []
    for t in s.split():
        alnum = "".join(ch for ch in t.lower() if ch.isalnum())
        if alnum:
            toks.append(alnum)
    return toks


def _compress_model_tokens(tokens: list[str]) -> str:
    """Collapse model tokens into a short, bucketable 'family' token."""
    if not tokens:
        return ""

    STOP = {
        "headphones", "headphone", "earphones", "earphone", "earbuds", "earbud",
        "inear", "over", "on", "wireless", "bluetooth", "true", "tws", "anc",
        "noise", "cancelling", "canceling", "stereo", "audio", "sound", "calls",
        "genuine", "original", "boxed", "edition", "limited", "special", "gen",
        "black", "white", "silver", "grey", "gray", "blue", "red", "platinum",
        "na", "doesnotapply", "lookintitle", "other", "bud", "buds"
    }

    # Prefixes that indicate we need the subsequent token
    PREFIX_JOIN = {
        "wh", "wf", "wi", "mdr", "hd", "dt", "ie", "se", "ath", "qc", "rs",
        "quietcomfort", "momentum", "crusher", "major", "tour", "tune", "live",
        "liberty", "space", "powerbeats", "studio", "solo", "beoplay", "anker"
    }

    out: list[str] = []
    have_digit = False

    for tok in tokens:
        if tok in STOP:
            continue

        if not out:
            out.append(tok)
            have_digit = bool(re.search(r"\d", tok))
            continue

        # Keep joining for known prefixes (e.g., quietcomfort + ultra, wh + 1000xm5)
        if len(out) == 1 and out[0] in PREFIX_JOIN:
            out.append(tok)
            have_digit = have_digit or bool(re.search(r"\d", tok))
            # Keep going for specific model qualifiers like 'pro', 'max', or numbers
            if have_digit or tok in {"ultra", "max", "v", "iv", "ii", "iii", "pro"}:
                break
            continue

        # If the next token is a generation or pro/max, keep it
        if len(out) == 1 and (re.search(r"\d", tok) or tok in {"ultra", "max", "pro"}):
            out.append(tok)
            break

        break

    return "".join(out) if out else ""


def _normalise_model(raw_model: Any, raw_brand: Any) -> str:
    s = _clean(raw_model)
    if not s:
        return ""

    low = s.lower()
    if low in {"n/a", "na", "does not apply", "doesnotapply", "look in title", "other"}:
        return ""

    tokens = _tokenise_model(s)
    if not tokens:
        return ""

    # Remove brand repetition in the model string
    brand_raw = _clean(raw_brand)
    brand_tokens = _tokenise_model(brand_raw)
    brand_first = brand_tokens[0] if brand_tokens else ""

    if brand_first and tokens[0] == brand_first:
        tokens = tokens[1:]

    return _compress_model_tokens(tokens)


def headphones_model_key(
        attrs: Mapping[str, Any],
        title: str = "",
) -> Optional[str]:
    """Build a canonical model key for headphone listings (source='ebay-headphones').

    Output format: {brand}-{family}_{grade}

    Examples:
        Brand="Bose", Model="Bose QuietComfort Ultra" -> "bose-quietcomfortultra_B"
        Brand="Sony", Model="WH-1000XM4" -> "sony-wh1000xm4_B"
        Brand="Marshall", Model="Major V" -> "marshall-majorv_B"
    """
    raw_brand = attrs.get("Brand")
    brand = _normalise_brand(raw_brand)

    # Fallback cascade for model identification
    raw_model = (
            attrs.get("Model") or
            attrs.get("MPN") or
            attrs.get("Manufacturer Part Number") or
            attrs.get("Item Code")
    )
    model_core = _normalise_model(raw_model, raw_brand)

    if not brand or not model_core:
        return UNKNOWN_KEY

    base_key = f"{brand}-{model_core}"
    grade = _derive_condition_grade(attrs, title)
    return f"{base_key}_{grade}"