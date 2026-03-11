
from __future__ import annotations

from typing import Mapping, Any, Optional
from typing import Dict, Set


def _as_text(val: Any) -> str:
    if val is None:
        return ""
    return str(val).strip().lower()


GENERIC_PART_ONLY_TERMS = (
    "for parts",
    "parts only",
    "for spares",
    "spares or repairs",
    "spares & repairs",
    "repair only",
    "needs repair",
    "box only",
    "empty box",
    "manual only",
    "instructions only",
    "charger only",
    "battery only",
    "case only",
    "housing only",
    "shell only",
    "cover only",
    "cable only",
    "adapter only",
    "dock only",
    "stand only",
    "mount only",
    "replacement part",
)

WATCH_PART_TERMS = (
    "dial",
    "movement only",
    "watch movement",
    "bezel",
    "crystal",
    "caseback",
    "case back",
    "crown only",
    "watch hands",
    "strap",
    "bracelet",
    "buckle",
    "clasp",
    "links",
    "papers only",
)

LEGO_PART_TERMS = (
    "minifigure only",
    "minifig only",
    "no minifigure",
    "part only",
    "spare parts",
    "bulk bricks",
    "job lot bricks",
    "joblot bricks",
    "instruction book only",
)

DEVICE_ACCESSORY_TERMS = (
    "screen protector",
    "phone case",
    "tablet case",
    "keyboard case",
    "ear pads",
    "headband",
    "controller only",
    "remote only",
)


def looks_like_partial_or_accessory_listing(
    title: str,
    attrs: Optional[Mapping[str, Any]] = None,
    *,
    source: Optional[str] = None,
    model_key: Optional[str] = None,
) -> bool:
    """
    Conservative guard for listings that should not be valued as complete items.

    This is intentionally stricter for watches/LEGO because those categories
    create extremely misleading comps when a part-only listing inherits the
    whole-product model key.
    """
    title_text = _as_text(title)
    source_text = _as_text(source)
    model_key_text = _as_text(model_key)

    all_attr_parts = []
    for _, value in (attrs or {}).items():
        if isinstance(value, (list, tuple, set)):
            all_attr_parts.extend(_as_text(x) for x in value)
        else:
            all_attr_parts.append(_as_text(value))

    attr_text = " ".join(part for part in all_attr_parts if part)
    blob = f"{title_text} {attr_text} {model_key_text}".strip()

    if any(term in blob for term in GENERIC_PART_ONLY_TERMS):
        return True

    is_watch = "watch" in source_text or "omega" in blob or "rolex" in blob or "seamaster" in blob
    if is_watch and any(term in blob for term in WATCH_PART_TERMS):
        return True

    is_lego = "lego" in source_text or "lego" in blob
    if is_lego and any(term in blob for term in LEGO_PART_TERMS):
        return True

    if any(term in blob for term in DEVICE_ACCESSORY_TERMS):
        return True

    return False

def _derive_condition_grade(attrs: Mapping[str, Any], title: str) -> str:
    """
    Map raw attributes + title into a coarse condition grade:

        A = new / sealed / like new
        B = default used / unknown (safe mid-grade)
        C = poorer refurb / rough
        D = clearly faulty / for parts / box-only style listings

    Heuristics are intentionally simple and easy to tweak.
    """

    # 1) Normalised text from title + attributes
    title_text = _as_text(title)

    all_attr_parts = []
    for _, v in attrs.items():
        if isinstance(v, (list, tuple, set)):
            all_attr_parts.extend(_as_text(x) for x in v)
        else:
            all_attr_parts.append(_as_text(v))

    attr_text = " ".join(p for p in all_attr_parts if p)
    blob = f"{title_text} {attr_text}".strip()

    # ----------------
    # Grade D: faulty / spares / box-only
    # ----------------
    faulty_terms = [
        # Core faults
        "faulty", "not working", "no power", "won't power", "wont power",
        "won't turn on", "wont turn on", "does not turn on", "doesn't turn on",
        "no display", "no video", "no sound", "dead", "powers on then off",
        "boot loop", "does not charge", "won't charge", "wont charge",
        "overheating", "water damaged", "liquid damaged", "liquid damage",
        "screen damaged", "screen cracked", "cracked screen", "screen issues",
        "screen fault", "display fault", "display issue", "faulty battery",
        "bad battery", "battery issue", "battery fault", "charging port issue",
        "charging fault", "screen flickers", "read description", "empty",
        "packaging only", "damaged", "broken screen", "spares and repairs",

        # Spares/repairs flags
        "for spares", "for parts", "spares or repairs", "spares & repairs",
        "repair only", "needs repair", "for repair", "parts only",
        "non working", "non-working", "untested", "sold as seen", "as is", "as-is",
        "not tested", "unable to test",

        # Box/accessory-only
        "box only", "boxes only", "package only",
        "case only", "charger only", "battery only", "shell only",
        "housing only", "empty box", "empty packaging",
        "no console included", "no phone included", "no tablet included",

        # Warnings / scam flags
        "see description", "read full listing", "please read",

        # Tools-specific
        "no blade", "no battery", "bare unit", "body only", "body-only",
        "no charger", "no accessories", "missing parts", "missing screws",

        # Camera/drone
        "lens error", "lens fault", "gimbal error", "gimbal fault",
        "motor overload", "crash damage", "crashed",

        # Console-specific
        "drift issue", "stick drift", "joystick drift", "controller drift",
        "no hdmi output", "av output fault",
    ]

    if any(term in blob for term in faulty_terms):
        return "D"

    # ----------------
    # Explicit grade / condition fields (if present)
    # ----------------
    grade_val = _as_text(
        attrs.get("Grade")
        or attrs.get("Product Grade")
        or attrs.get("Condition")
    )
    if grade_val:
        if "grade c" in grade_val or "c+" in grade_val or "c " in grade_val:
            return "C"
        if "grade b" in grade_val or "b+" in grade_val or "b " in grade_val:
            return "B"
        if "grade a" in grade_val or "a+" in grade_val or "a " in grade_val:
            return "A"

        # eBay-style condition text
        if "for parts" in grade_val or "not working" in grade_val:
            return "D"
        if "used" in grade_val or "seller refurbished" in grade_val or "refurbished" in grade_val:
            return "B"
        if "new" in grade_val or "brand new" in grade_val or "sealed" in grade_val:
            return "A"

    # Some sellers encode USED in the model code itself
    model_val = _as_text(attrs.get("Model"))
    if "used" in model_val:
        return "B"

    # ----------------
    # Grade A: clearly new-ish
    # ----------------
    new_terms = [
        "brand new", "new & sealed", "new and sealed",
        "factory sealed", "sealed", "unopened",
        "unused", "like new", "open box",
    ]
    if any(term in blob for term in new_terms):
        return "A"

    # ----------------
    # Grade C: rough but working
    # ----------------
    rough_terms = [
        "heavy wear", "heavily used", "scratches",
        "cosmetic damage", "cosmetically poor","scraped screen"
    ]
    if any(term in blob for term in rough_terms):
        return "C"

    # Default: B (generic used / unknown)
    return "B"
