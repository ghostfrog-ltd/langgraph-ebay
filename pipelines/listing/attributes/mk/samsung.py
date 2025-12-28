from __future__ import annotations

import re
from typing import Any, Mapping, Optional

from utils.condition import _derive_condition_grade

UNKNOWN_KEY = "unknown"


def _clean(s: Any) -> str:
    if s is None:
        return ""
    return str(s).strip()


def _normalise_brand(raw: Any) -> str:
    """Collapse brand to 'samsung' when appropriate."""
    s = _clean(raw).lower()
    if not s:
        return ""
    if "samsung" in s:
        return "samsung"
    # fallback: compact alnum
    return "".join(ch for ch in s if ch.isalnum())


def _strip_parentheses(s: str) -> str:
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


def _compress_model_tokens(tokens: list[str]) -> str:
    """Collapse Samsung phone models into a stable family token.

    Examples:
      Galaxy S23 Ultra -> s23ultra
      Galaxy S23 FE    -> s23fe
      Galaxy A54 5G    -> a54
      Galaxy Z Fold5   -> zfold5
      Galaxy Z Flip6  -> zflip6
    """
    if not tokens:
        return ""

    STOP = {
        "galaxy", "samsung", "phone", "smartphone",
        "5g", "lte", "enterprise", "edition",
        "dual", "sim", "unlocked",
        "ultra5g", "plus5g",
        "gb", "tb",
    }

    out: list[str] = []
    for tok in tokens:
        if tok in STOP:
            continue

        # first meaningful token (s23, a54, z, fold, flip)
        if not out:
            out.append(tok)
            continue

        # join z + fold/flip
        if out == ["z"] and tok in {"fold", "flip"}:
            out[-1] = f"z{tok}"
            continue

        # numeric or suffix token (23, 5, ultra, fe, plus)
        if re.search(r"\d", tok) or tok in {"ultra", "fe", "plus"}:
            out.append(tok)
            break

        if len(out) >= 2:
            break

        out.append(tok)

    return "".join(out)


def _normalise_model(attrs: Mapping[str, Any]) -> str:
    """Extract model family from Model / Model Number / MPN."""
    raw = (
        attrs.get("Model")
        or attrs.get("Model Number")
        or attrs.get("MPN")
    )
    s = _clean(raw)
    if not s:
        return ""

    low = s.lower()
    if low in {"does not apply", "n/a", "android phone", "mobile phone"}:
        return ""

    s = _strip_parentheses(s)
    s = s.replace("/", " ").replace("-", " ")
    s = " ".join(s.split())

    tokens = []
    for tok in s.split():
        alnum = "".join(ch for ch in tok.lower() if ch.isalnum())
        if alnum:
            tokens.append(alnum)

    return _compress_model_tokens(tokens)


def samsung_model_key(
    attrs: Mapping[str, Any],
    title: str = "",
) -> Optional[str]:
    """Canonical model key for ebay-samsung listings.

    Output:
        samsung-{family}_{grade}

    Examples:
        Samsung Galaxy S23 Ultra -> samsung-s23ultra_B
        Samsung Galaxy A54 5G    -> samsung-a54_B
        Samsung Galaxy Z Fold5  -> samsung-zfold5_B
    """
    brand = _normalise_brand(attrs.get("Brand"))
    model_core = _normalise_model(attrs)

    if not brand or not model_core:
        return UNKNOWN_KEY

    grade = _derive_condition_grade(attrs, title)
    return f"{brand}-{model_core}_{grade}"
