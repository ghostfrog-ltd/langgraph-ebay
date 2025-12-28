
from __future__ import annotations

from typing import Mapping, Any, Optional

from utils.condition import _derive_condition_grade

UNKNOWN_KEY = "unknown"

def _clean(s: Any) -> str:
    if s is None:
        return ""
    return str(s).strip()


def _strip_parentheses(s: str) -> str:
    cleaned: list[str] = []
    depth = 0
    for ch in s:
        if ch == "(":
            depth += 1
            continue
        if ch == ")":
            if depth > 0:
                depth -= 1
            continue
        if depth == 0:
            cleaned.append(ch)
    return "".join(cleaned)


def _normalise_brand(raw: Any) -> str:
    """
    Motors uses attrs["Brand"] as the closest thing to "Make".
    Normalise to a compact token.
    """
    s = _clean(raw)
    if not s:
        return ""

    out: list[str] = []
    for ch in s.lower():
        if ch.isalnum():
            out.append(ch)
    return "".join(out)


def _compress_model_tokens(tokens: list[str]) -> str:
    """
    Collapse noisy car model/trim strings into a stable "family-ish" token.

    Examples (rough intent):
      "3 Series 320d M Sport"         -> "3series320d"
      "Golf 1.6 TDI Match"            -> "golf16tdi"   (keeps engine token if present early)
      "A4 Avant 2.0 TFSI S Line"      -> "a4avant20tfsi" or "a4avant20" (depending tokens)
      "C-Class C220d AMG Line"        -> "cclassc220d"
      "Model 3 Long Range AWD"        -> "model3"

    Strategy:
      - Keep name tokens at the front (usually 1–2 tokens)
      - Keep one “anchor” token that has digits (e.g. 320d, 2.0, 16, c220d)
      - Stop when we hit obvious trim / fluff words
      - Join chosen tokens together
    """
    if not tokens:
        return ""

    STOP_TOKENS = {
        # trim / marketing
        "sport", "msport", "m", "amg", "line", "sline", "rline", "gtline", "stline",
        "edition", "special", "limited", "exclusive", "signature",
        "lux", "luxury", "executive", "premium", "prestige",
        "se", "sel", "sx", "sxi", "sr", "sri", "vrs", "rs", "st", "xline",
        "titanium", "ghia", "zetec", "trendline", "highline",
        "dynamic", "dynamique", "elegance", "avantgarde", "laureate", "acenta",
        "nconnecta", "tekna", "active", "style", "design", "sportback",
        "shadow", "blackedition", "whiteedition",

        # drivetrain / misc that tends to explode variants
        "quattro", "xdrive", "4matic", "all4", "awd", "fwd", "rwd", "4x4", "4wd",

        # body-ish / generic noise (often redundant)
        "hatchback", "saloon", "sedan", "estate", "coupe", "convertible", "cab",
        "pickup", "van", "mpv", "suv", "touring", "sportstourer",

        # transmissions / fuel (too variant-y for base family)
        "auto", "automatic", "manual", "dsg", "cvt",
        "diesel", "petrol", "hybrid", "phev", "ev", "electric",
    }

    out: list[str] = []
    have_digit_anchor = False

    for tok in tokens:
        if not out:
            out.append(tok)
            if any(ch.isdigit() for ch in tok):
                have_digit_anchor = True
            continue

        # stop on fluff/trim tokens
        if tok in STOP_TOKENS:
            break

        # keep one token containing digits as an anchor (320d, 20tdi, c220d, etc)
        if any(ch.isdigit() for ch in tok):
            out.append(tok)
            have_digit_anchor = True
            # once we have name + anchor, usually enough
            if len(out) >= 2:
                break
            continue

        # allow up to 2 name tokens max (e.g. "3series", "grandcherokee")
        if have_digit_anchor:
            break

        if len(out) >= 2:
            break

        out.append(tok)

    return "".join(out)


def _normalise_model(raw_model: Any, raw_brand: Any) -> str:
    """
    Normalise attrs["Model"] into a compact, bucketable token.
    Falls in line with cameras.py approach: aggressively reduce key explosion.
    """
    s = _clean(raw_model)
    if not s:
        return ""

    low = s.lower()

    # common rubbish values
    if low in {"not supplied", "does not apply", "n/a", "na", "."}:
        return ""

    s = _strip_parentheses(s)

    # normalise separators
    s = s.replace("/", " ")
    s = s.replace("\\", " ")
    s = s.replace("-", " ")
    s = " ".join(s.split())

    if not s:
        return ""

    raw_tokens = s.split()

    # remove leading brand token if repeated (e.g. "BMW 3 Series ...")
    brand_clean = _clean(raw_brand)
    brand_tokens = brand_clean.split()
    brand_first = brand_tokens[0].lower() if brand_tokens else ""

    if brand_first and raw_tokens and raw_tokens[0].lower() == brand_first:
        raw_tokens = raw_tokens[1:]

    # token clean: keep alnum only
    tokens: list[str] = []
    for tok in raw_tokens:
        alnum = "".join(ch for ch in tok if ch.isalnum())
        if not alnum:
            continue
        tokens.append(alnum.lower())

    if not tokens:
        return ""

    return _compress_model_tokens(tokens)


def motors_model_key(
    attrs: Mapping[str, Any],
    title: str = "",
) -> Optional[str]:
    """
    Build a canonical model key for motors listings (source='ebay-motors').

    Output format:
        {brand}-{family}_{grade}

    Uses:
      - attrs["Brand"] as make
      - attrs["Model"] as primary model string
      - fallback: attrs["Variant"] if Model is missing/garbage
      - condition grade from _derive_condition_grade(attrs, title)

    If no usable Brand or Model/Variant -> returns UNKNOWN_KEY ("unknown")
    """
    raw_brand = attrs.get("Brand")
    raw_model = attrs.get("Model")

    brand = _normalise_brand(raw_brand)
    model_core = _normalise_model(raw_model, raw_brand)

    # fallback to Variant if Model is missing or junk
    if not model_core:
        raw_variant = attrs.get("Variant")
        model_core = _normalise_model(raw_variant, raw_brand)

    if not brand or not model_core:
        return UNKNOWN_KEY

    base_key = f"{brand}-{model_core}"
    grade = _derive_condition_grade(attrs, title)

    return f"{base_key}_{grade}"
