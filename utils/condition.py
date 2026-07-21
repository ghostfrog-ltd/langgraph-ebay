
from __future__ import annotations

import re
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
    "spares / repair",
    "spares/repair",
    "spares repair",
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
    "no figures",
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
    "wall mount",
    "storage bracket",
    "earphone holder",
    "controller only",
    "controller cable",
    "controller battery",
    "thumb grip",
    "thumb grips",
    "thumbstick",
    "thumb stick",
    "kontrolfreek",
    "fps freek",
    "charging stand",
    "cooling fan",
    "remote only",
    "headset",
    "headphones",
    "microphone",
    "power supply",
    "psu",
    "official brick",
)

DEVICE_REPAIR_REVIEW_TERMS = (
    "activation locked",
    "icloud locked",
    "i cloud locked",
    "locked apple iphone",
    "locked apple ipad",
    "locked iphone",
    "locked ipad",
    "passcode locked",
    "password locked",
    "needs new battery",
    "needs a new battery",
    "needs battery",
    "battery service",
    "battery replacement needed",
    "works with issues",
)

CONSOLE_NON_HARDWARE_TERMS = (
    "code in a box",
    "code-in-a-box",
    "download code",
    "digital code",
    "game code",
    "games bundle",
    "game bundle",
    "software bundle",
    "stickers",
    "sticker",
    "button led",
    "guide button",
    "home guide button",
    "mod stickers",
    "led mod",
)

CONSOLE_HARDWARE_TERMS = (
    "console",
    "system",
    "handheld",
    "disc edition",
    "disk edition",
    "digital edition",
    "blu ray edition",
    "blu-ray edition",
    "with controller",
    "console only",
    "oled model",
)

VEHICLE_PART_TERMS = (
    "aerial",
    "antenna",
    "badge",
    "bearing",
    "bolt",
    "brake disc",
    "brake master cylinder",
    "bumper",
    "clutch lever",
    "cover",
    "drain bolt",
    "engine cover",
    "fender eliminator",
    "fork",
    "front fork",
    "front hanger",
    "front master cylinder",
    "grille",
    "grab handle",
    "handlebar",
    "headlight",
    "indicator",
    "mirror",
    "number plate",
    "numberplate",
    "oil spinner",
    "parcel shelf",
    "quick release",
    "rear disc",
    "seal",
    "surround",
    "switchgear",
    "boot release",
    "boot lid",
    "tail tidy",
    "tool kit",
    "washer",
    "wheel bearing",
    "window regulator",
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

    if any(term in blob for term in DEVICE_REPAIR_REVIEW_TERMS):
        return True

    is_console = any(
        marker in f"{source_text} {model_key_text} {blob}"
        for marker in ("console", "ps4", "ps5", "xbox", "switch", "playstation", "nintendo")
    )
    if is_console and any(term in blob for term in CONSOLE_NON_HARDWARE_TERMS):
        return True

    complete_console_signal = any(term in blob for term in CONSOLE_HARDWARE_TERMS) or bool(
        re.search(r"\b(128|256|500|512|825)\s?gb\b|\b(1|2)\s?tb\b", blob)
    )
    platform_only_signal = any(
        term in blob
        for term in (
            "playstation 5",
            "playstation 4",
            "ps5",
            "ps4",
            "nintendo switch",
            "xbox one",
            "xbox series",
        )
    )
    if is_console and platform_only_signal and not complete_console_signal:
        return True

    if is_console and not complete_console_signal and any(
        term in blob
        for term in (
            "controller",
            "gamepad",
            "pad",
            "stand",
            "dock",
            "charger",
            "battery",
            "cable",
            "mount",
            "bracket",
            "holder",
            "headset",
            "headphones",
            "microphone",
            "power supply",
            "psu",
            "brick",
        )
    ):
        return True

    is_vehicle = any(
        marker in f"{source_text} {model_key_text} {blob}"
        for marker in ("mini", "cooper", "honda", "msx", "grom", "motor", "motors", "car")
    )
    if is_vehicle and any(term in blob for term in VEHICLE_PART_TERMS):
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
        "blue screen", "blue screened", "disc drive fault",

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
