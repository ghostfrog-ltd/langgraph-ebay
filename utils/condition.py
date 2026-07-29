
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
    "parts missing",
    "for spares",
    "spares only",
    "spares or repairs",
    "spares or repair",
    "spares & repairs",
    "spares repairs",
    "spares / repair",
    "spares/repair",
    "spares repair",
    "spare repair",
    "repair only",
    "for repair",
    "needs repair",
    "faulty",
    "not working",
    "not starting",
    "dead",
    "scrap",
    "scraps",
    "selling for scraps",
    "badly damaged",
    "screen damage",
    "damaged screen",
    "screen is damaged",
    "screen badly damaged",
    "badly damaged screen",
    "broken screen",
    "cracked screen",
    "broken lcd",
    "broken display",
    "untested",
    "as is",
    "as-is",
    "read description",
    "please read",
    "grade d",
    "box only",
    "empty box",
    "empty original box",
    "box & inserts",
    "box and inserts",
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
    "job lot",
    "joblot",
    "bundle job lot",
    "unit only",
    "no accessories",
)

SUSPICIOUS_PRODUCT_IDENTITY_TERMS = (
    "macbook neo",
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
    "no minifigures",
    "no mini figures",
    "without minifigures",
    "without minifigs",
    "missing pieces",
    "missing parts",
    "incomplete",
    "build only",
    "builds only",
    "part only",
    "spare parts",
    "bulk bricks",
    "job lot bricks",
    "joblot bricks",
    "instruction book only",
    "instructions only",
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
    "usb-c cable",
    "usb cable",
    "usb-a to usb-c",
    "joy-con",
    "joy con",
    "joycon",
    "thumb grip",
    "thumb grips",
    "thumbstick",
    "thumb stick",
    "kontrolfreek",
    "fps freek",
    "charging stand",
    "cooling fan",
    "stealth case",
    "carrying case",
    "carrying cases",
    "protective case",
    "travel case",
    "remote only",
    "headset",
    "headphones",
    "microphone",
    "power supply",
    "psu",
    "official brick",
)

DEVICE_PART_TERMS = (
    "motherboard",
    "logic board",
    "logic board missing",
    "missing logic board",
    "logicboard",
    "main board",
    "mainboard",
    "circuit board",
    "pcb board",
    "daughterboard",
    "screen assembly",
    "screen for",
    "display assembly",
    "lcd assembly",
    "battery assembly",
    "keyboard palmrest",
    "palmrest",
    "palm rest",
    "touchpad",
    "trackpad",
    "topcase",
    "top case",
    "bottom case",
    "digitizer",
    "touch screen digitizer",
    "touchscreen digitizer",
    "replacement screen",
    "screen replacement",
    "front glass",
    "rear glass",
    "back glass",
    "rear housing",
    "back housing",
    "housing frame",
    "chassis frame",
    "mid frame",
    "camera module",
    "speaker module",
    "earpiece speaker",
    "charging port",
    "dock connector",
    "flex cable",
    "ribbon cable",
    "sim tray",
    "taptic engine",
)

DEVICE_REPAIR_REVIEW_TERMS = (
    "activation locked",
    "activation lock",
    "icloud locked",
    "icloud lock",
    "i cloud locked",
    "i cloud lock",
    "locked apple iphone",
    "locked apple ipad",
    "locked iphone",
    "locked ipad",
    "mdm locked",
    "remote management locked",
    "blacklisted",
    "blocked imei",
    "bad imei",
    "no face id",
    "face id not working",
    "touch id not working",
    "touch id missing",
    "touch id issue",
    "touchid issue",
    "truedepth issue",
    "true depth issue",
    "screen issue",
    "screen is damaged",
    "screen & mic issue",
    "mic issue",
    "microphone issue",
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
    Conservative guard for listings that should not be valued as clean complete
    items.

    This is intentionally stricter for watches/LEGO because those categories
    create extremely misleading comps when a part-only listing inherits the
    whole-product model key. It also catches obvious product-identity pollution
    such as fabricated Apple family/chip combinations.
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

    if any(term in blob for term in SUSPICIOUS_PRODUCT_IDENTITY_TERMS):
        return True

    is_apple = "ebay-apple" in source_text or "apple-" in model_key_text or " apple " in f" {blob} "
    is_macbook = "macbook" in blob or "apple-macbook" in model_key_text
    if is_apple and re.search(r"\blocked\b", blob):
        return True

    if is_apple and is_macbook and re.search(r"\ba\d{1,2}\s*(pro|max|ultra)?\b", blob):
        return True

    is_watch = "watch" in source_text or "omega" in blob or "rolex" in blob or "seamaster" in blob
    if is_watch and any(term in blob for term in WATCH_PART_TERMS):
        return True

    is_lego = "lego" in source_text or "lego" in blob
    if is_lego and any(term in blob for term in LEGO_PART_TERMS):
        return True

    if any(term in blob for term in DEVICE_ACCESSORY_TERMS):
        return True

    if any(term in blob for term in DEVICE_PART_TERMS):
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
            "playstation 3",
            "playstation vita",
            "ps5",
            "ps4",
            "ps3",
            "ps vita",
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


def has_investible_product_identity(
    title: str,
    attrs: Optional[Mapping[str, Any]] = None,
    *,
    source: Optional[str] = None,
    model_key: Optional[str] = None,
) -> bool:
    """
    True only when a listing is clean enough to compare against whole-product comps.

    Raw collection can keep broad or suspicious rows, but buy/resale math needs
    like-for-like identity. This keeps broad Apple buckets such as unknown iPad
    generation or iPhone-with-storage-only out of comps, ROI, and research lanes
    until model-key extraction repairs them.
    """
    if looks_like_partial_or_accessory_listing(
        title,
        attrs,
        source=source,
        model_key=model_key,
    ):
        return False

    mk = _as_text(model_key)
    if not mk or mk == "unknown" or mk.startswith("unknown") or "generic" in mk:
        return False
    if re.search(r"_[a-d](?:-[a-d])?$", mk):
        return False

    source_text = _as_text(source)
    is_apple = source_text == "ebay-apple" or mk.startswith("apple-")
    is_samsung = source_text == "ebay-samsung" or mk.startswith("samsung-")
    is_console = (
        source_text == "ebay-consoles"
        or mk.startswith("sony-playstation-")
        or mk.startswith("microsoft-xbox-")
        or mk.startswith("nintendo-switch-")
    )
    is_actioncam = (
        source_text == "ebay-actioncams"
        or mk.startswith("gopro-")
        or mk.startswith("dji-")
        or mk.startswith("insta360-")
        or mk.startswith("akaso-")
        or mk.startswith("drift-")
        or mk.startswith("sjcam-")
    )
    is_lego = source_text == "ebay-lego" or mk.startswith("lego-")
    is_motor_vehicle = source_text == "ebay-motors" or mk.startswith("vehicle-")
    if (
        not is_apple
        and not is_samsung
        and not is_console
        and not is_actioncam
        and not is_lego
        and not is_motor_vehicle
    ):
        return True

    if "gen-unk" in mk:
        return False

    if is_motor_vehicle:
        if "_" in mk:
            return False
        return bool(re.match(r"^vehicle-[a-z0-9-]+-[a-z0-9]+(?:-[a-z0-9.]+)*-\d{4}$", mk))

    if is_lego:
        if "_" in mk:
            return False
        lego_set = r"^lego-set-\d{4,7}$"
        lego_minifig = r"^lego-minifig-[a-z0-9-]+-\d{3,5}[a-z]?$"
        return bool(re.match(lego_set, mk) or re.match(lego_minifig, mk))

    if is_actioncam:
        if "_" in mk:
            return False
        gopro = (
            r"^gopro-"
            r"("
            r"hero(8|9|10|11|12|13)(-mini)?|"
            r"hero7-(black|silver|white)|"
            r"hero6-black|"
            r"hero5-(black|session)|"
            r"hero4-(black|silver|session)|"
            r"hero3plus|hero3|hero-2018|max|fusion"
            r")$"
        )
        dji = (
            r"^dji-"
            r"("
            r"osmo-action-(3|4|6)|"
            r"osmo-action-5-pro|"
            r"action-2|"
            r"osmo-pocket-3|"
            r"osmo-360"
            r")$"
        )
        insta360 = (
            r"^insta360-"
            r"("
            r"x(2|3|4|5)|"
            r"go-(2|ultra)|"
            r"go-(3|3s)-(32|64|128)gb|"
            r"ace|ace-pro|ace-pro-2|"
            r"one-x|one-x2|one-r|one-r-twin|one-rs|one-rs-twin"
            r")$"
        )
        secondary_actioncam = r"^(akaso-(ek7000|ek7000-pro|brave-7|brave-7-le|v50)|drift-ghost-xl|sjcam-c300)$"
        return bool(
            re.match(gopro, mk)
            or re.match(dji, mk)
            or re.match(insta360, mk)
            or re.match(secondary_actioncam, mk)
        )

    if is_console:
        if "_" in mk:
            return False
        playstation = (
            r"^sony-playstation-"
            r"("
            r"5-(disc|digital)-825gb|"
            r"5-slim-(disc|digital)-1tb|"
            r"5-pro-2tb|"
            r"4-(base|slim|pro)-(500gb|1tb|2tb)|"
            r"3|vita"
            r")$"
        )
        xbox = (
            r"^microsoft-xbox-"
            r"("
            r"series-x-(1tb|2tb)|"
            r"series-s-(512gb|1tb)|"
            r"one-(base|s|x)-(500gb|1tb|2tb)|"
            r"360(-(320|250|120|60|40|20|12)gb)?"
            r")$"
        )
        switch = r"^nintendo-switch-(base|oled|lite|2|tablet-only)$"
        return bool(
            re.match(playstation, mk)
            or re.match(xbox, mk)
            or re.match(switch, mk)
        )

    if is_samsung:
        if "_" in mk:
            return False
        samsung_phone = (
            r"^samsung-galaxy-"
            r"(s\d{1,2}(-(ultra|plus|fe))?|note\d{1,2}(-(ultra|plus))?|z-(fold|flip)\d|[am]\d{2}s?)"
            r"-(32|64|128|256|512)gb(-5g)?$"
        )
        samsung_phone_tb = (
            r"^samsung-galaxy-"
            r"(s\d{1,2}(-(ultra|plus|fe))?|note\d{1,2}(-(ultra|plus))?|z-(fold|flip)\d)"
            r"-1tb$"
        )
        samsung_tab = (
            r"^samsung-galaxy-tab-[as]\d{1,2}(-(ultra|plus|fe))?"
            r"-(32|64|128|256|512)gb(-(wifi|cellular))?$"
        )
        return bool(
            re.match(samsung_phone, mk)
            or re.match(samsung_phone_tb, mk)
            or re.match(samsung_tab, mk)
        )

    if mk.startswith("apple-iphone-") and not re.match(r"^apple-iphone-(se|x|xr|xs|\d+)\b", mk):
        return False

    if mk.startswith(("apple-macbook", "apple-imac", "apple-mac-mini", "apple-mac-studio")):
        if not re.search(r"-(m[1-9](pro|max|ultra)?|intel|core-i[3579]|core-m[357])(-|_)", mk):
            return False

    return True

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
        "water damage",
        "screen damage", "damaged screen", "screen is damaged", "screen badly damaged",
        "badly damaged screen", "screen damaged", "screen cracked", "cracked screen", "screen issues",
        "screen fault", "display fault", "display issue", "faulty battery",
        "bad battery", "battery issue", "battery fault", "charging port issue",
        "charging fault", "screen flickers", "read description", "empty",
        "packaging only", "damaged", "broken screen", "spares and repairs",
        "blue screen", "blue screened", "disc drive fault",

        # Spares/repairs flags
        "for spares", "for parts", "spares or repairs", "spares & repairs",
        "repair only", "needs repair", "for repair", "parts only",
        "parts or repair", "parts & repair",
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
        "no hdmi output", "av output fault", "white light error",
        "blue light error", "yellow light of death", "red ring of death",
        "wlod", "ylod", "rrod",
    ]

    if any(term in blob for term in faulty_terms):
        return "D"

    if re.search(r"\b(grade\s*c|used\s+fair|fair\s+condition|condition\s+fair)\b", blob):
        return "C"

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
        "cosmetic damage", "cosmetically poor", "scraped screen",
        "no box", "without box", "unboxed", "no original box",
        "no manual", "no instructions", "missing manual",
    ]
    if any(term in blob for term in rough_terms):
        return "C"

    # Default: B (generic used / unknown)
    return "B"
