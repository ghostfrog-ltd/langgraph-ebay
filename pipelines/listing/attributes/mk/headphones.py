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
    """Normalise Brand into a compact token (lowercase alnum only).

    Examples:
      "Beats by Dr. Dre" -> "beatsbydrdre"
      "Audio-Technica"   -> "audiotechnica"
      "beyerdynamic"     -> "beyerdynamic"
    """
    s = _clean(raw)
    if not s:
        return ""

    low = s.lower()
    out = []
    for ch in low:
        if ch.isalnum():
            out.append(ch)
    return "".join(out)


def _tokenise_model(s: str) -> list[str]:
    s = _strip_parentheses(s)
    s = s.replace("/", " ").replace("\", " ").replace("-", " ")
    s = " ".join(s.split())

    toks: list[str] = []
    for t in s.split():
        alnum = "".join(ch for ch in t.lower() if ch.isalnum())
        if alnum:
            toks.append(alnum)
    return toks


def _compress_model_tokens(tokens: list[str]) -> str:
    """Collapse model tokens into a short, bucketable 'family' token.

    Goal: keep enough to identify the family, drop fluff (wireless/colour/etc).

    Examples:
      "Sony WH-1000XM5"                -> "wh1000xm5"
      "Bose QuietComfort Ultra"        -> "quietcomfortultra"
      "Sennheiser HD 560S"             -> "hd560s"
      "Skullcandy Crusher ANC 2"       -> "crusheranc2"
      "Audio-Technica ATH-M50X"        -> "athm50x"
      "Marshall Major V"               -> "majorv"
    """
    if not tokens:
        return ""

    STOP = {
        # generic product words
        "headphones", "headphone", "earphones", "earphone", "earbuds", "earbud",
        "inear", "in", "ear", "over", "on", "earcup", "earpad",
        "wireless", "bluetooth", "true", "tws", "anc", "noise", "cancelling",
        "canceling", "cancel", "nc",
        "stereo", "audio", "sound",
        # bundles / fluff
        "new", "genuine", "original", "boxed", "box", "packaging",
        "edition", "limited", "special", "gen", "generation", "2nd", "3rd",
        "pro",  # NOTE: we *keep* pro sometimes; handled below
        # colours (coarse list)
        "black", "white", "silver", "grey", "gray", "blue", "red", "green",
        "beige", "pink", "orange", "yellow", "ivory", "navy", "platinum",
        "sandstone", "chestnut",
        # placeholders
        "na", "n", "a", "doesnotapply", "lookintitle", "other",
    }

    # If first token already looks like a model id (contains digits),
    # it's often enough (e.g. wh1000xm5, wf1000xm5, athm50x, hd560s).
    first = tokens[0]
    if re.search(r"\d", first):
        # But sometimes first is a family word and digits come next (e.g. hd + 560s).
        pass

    PREFIX_JOIN = {
        # common 2-part ids
        "wh", "wf", "wi", "mdr", "hd", "dt", "ie", "se", "ath", "qc", "rs",
        # marketing families where next token matters
        "quietcomfort", "momentum", "crusher", "major", "tour", "tune", "live",
        "liberty", "space", "opus", "powerbeats", "studio", "solo",
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

        # Allow joining for known prefixes (hd + 560s, wh + 1000xm5, z + fold5 etc)
        if len(out) == 1 and out[0] in PREFIX_JOIN:
            out.append(tok)
            have_digit = have_digit or bool(re.search(r"\d", tok))
            # if we now have digits, we likely have enough
            if have_digit:
                break
            continue

        # Keep a second token if it adds digits or important suffix
        if len(out) == 1 and (re.search(r"\d", tok) or tok in {"ultra", "max", "v", "iv", "ii", "iii", "pro"}):
            out.append(tok)
            have_digit = have_digit or bool(re.search(r"\d", tok))
            break

        # Otherwise don't overfit: stop after 2 tokens
        break

    if not out:
        return ""

    return "".join(out)


def _normalise_model(raw_model: Any, raw_brand: Any) -> str:
    """Normalise the Model into a compact, bucketable token.

    Steps:
      - Strip worthless values ("does not apply", "look in title", etc)
      - Strip parentheses
      - Tokenise to alnum tokens
      - Strip leading brand token if repeated
      - Compress tokens into a short family
    """
    s = _clean(raw_model)
    if not s:
        return ""

    low = s.lower()
    if low in {"n/a", "na", "does not apply", "doesnotapply", "look in title", "other"}:
        return ""

    tokens = _tokenise_model(s)
    if not tokens:
        return ""

    # Remove leading brand token if repeated (e.g. "Sony WH-1000XM5")
    brand_first = _clean(raw_brand).split()
    brand_first = brand_first[0].lower() if brand_first else ""
    if brand_first and tokens and tokens[0] == "".join(ch for ch in brand_first if ch.isalnum()):
        tokens = tokens[1:]

    return _compress_model_tokens(tokens)


def headphones_model_key(
    attrs: Mapping[str, Any],
    title: str = "",
) -> Optional[str]:
    """Build a canonical model key for headphone listings (source='ebay-headphones').

    Output format (console-style):

        {brand}-{family}_{grade}

    Examples:
        Brand="Sony", Model="Sony WH-1000XM5" -> "sony-wh1000xm5_B"
        Brand="Bose", Model="QuietComfort Ultra" -> "bose-quietcomfortultra_B"
        Brand="Sennheiser", Model="HD 560S" -> "sennheiser-hd560s_B"

    Rules:
    - Uses attrs["Brand"] and prefers attrs["Model"] for family.
    - Falls back to MPN / Manufacturer Part Number / Item Code when Model is missing.
    - Passes attrs + title into _derive_condition_grade for grade.
    - If no usable Brand or Model -> returns UNKNOWN_KEY ("unknown").
    """
    raw_brand = attrs.get("Brand")
    brand = _normalise_brand(raw_brand)

    raw_model = attrs.get("Model") or attrs.get("MPN") or attrs.get("Manufacturer Part Number") or attrs.get("Item Code")
    model_core = _normalise_model(raw_model, raw_brand)

    if not brand or not model_core:
        return UNKNOWN_KEY

    base_key = f"{brand}-{model_core}"
    grade = _derive_condition_grade(attrs, title)
    return f"{base_key}_{grade}"
