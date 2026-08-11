
from __future__ import annotations

import re
from typing import Mapping, Any, Optional
from typing import Dict, Set


def _as_text(val: Any) -> str:
    if val is None:
        return ""
    return str(val).strip().lower()


def _listing_blob(title: str, attrs: Optional[Mapping[str, Any]] = None, model_key: Optional[str] = None) -> str:
    parts = [_as_text(title)]
    for _, value in (attrs or {}).items():
        if isinstance(value, (list, tuple, set)):
            parts.extend(_as_text(x) for x in value)
        else:
            parts.append(_as_text(value))
    parts.append(_as_text(model_key))
    return " ".join(part for part in parts if part).strip()


def _normalised_tokens(text: str) -> list[str]:
    return re.findall(r"[a-z0-9]+", _as_text(text))


def _has_token_phrase(tokens: list[str], phrase: tuple[str, ...]) -> bool:
    if not phrase or len(phrase) > len(tokens):
        return False
    width = len(phrase)
    return any(tuple(tokens[idx : idx + width]) == phrase for idx in range(len(tokens) - width + 1))


APPLE_LOCK_RISK_PHRASES = (
    ("activation", "locked"),
    ("activation", "lock"),
    ("icloud", "locked"),
    ("icloud", "lock"),
    ("i", "cloud", "locked"),
    ("i", "cloud", "lock"),
    ("network", "locked"),
    ("carrier", "locked"),
    ("locked", "apple", "iphone"),
    ("locked", "apple", "ipad"),
    ("locked", "iphone"),
    ("locked", "ipad"),
    ("mdm", "locked"),
    ("remote", "management", "locked"),
)


def _has_apple_lock_risk_language(text: str) -> bool:
    tokens = _normalised_tokens(text)
    if "locked" in tokens:
        return True
    return any(_has_token_phrase(tokens, phrase) for phrase in APPLE_LOCK_RISK_PHRASES)


def _has_vehicle_signal(*, source: str, model_key: str, text: str) -> bool:
    source_text = _as_text(source)
    model_key_text = _as_text(model_key)
    if source_text in {"ebay-motors", "ebay-mini-cooper"}:
        return True
    if model_key_text.startswith("vehicle-") or model_key_text.startswith("mini-cooper"):
        return True

    tokens = _normalised_tokens(text)
    return (
        _has_token_phrase(tokens, ("mini", "cooper"))
        or "honda" in tokens
        or "msx" in tokens
        or "grom" in tokens
        or "motor" in tokens
        or "motors" in tokens
        or "car" in tokens
    )


FUNCTIONAL_FAULT_TERMS = (
    "faulty",
    "not working",
    "non working",
    "non-working",
    "no power",
    "won't power",
    "wont power",
    "won't turn on",
    "wont turn on",
    "does not turn on",
    "doesn't turn on",
    "no display",
    "no video",
    "no sound",
    "dead",
    "powers on then off",
    "boot loop",
    "does not charge",
    "won't charge",
    "wont charge",
    "overheating",
    "water damaged",
    "liquid damaged",
    "liquid damage",
    "water damage",
    "screen issue",
    "screen issues",
    "screen fault",
    "faulty screen",
    "display fault",
    "display issue",
    "faulty battery",
    "bad battery",
    "battery issue",
    "battery fault",
    "charging port issue",
    "charging fault",
    "screen flickers",
    "broken lcd",
    "broken display",
    "broken screen",
    "spares and repairs",
    "blue screen",
    "blue screened",
    "disc drive fault",
    "for spares",
    "for parts",
    "spares or repairs",
    "spares or repair",
    "spares & repairs",
    "repair only",
    "needs repair",
    "for repair",
    "parts only",
    "parts or repair",
    "parts & repair",
    "untested",
    "not tested",
    "unable to test",
    "not fully tested",
    "sold as seen",
    "as is",
    "as-is",
    "lens error",
    "lens fault",
    "gimbal error",
    "gimbal fault",
    "motor overload",
    "crash damage",
    "crashed",
    "drift issue",
    "stick drift",
    "joystick drift",
    "controller drift",
    "no hdmi output",
    "av output fault",
    "white light error",
    "blue light error",
    "yellow light of death",
    "red ring of death",
    "wlod",
    "ylod",
    "rrod",
    "no face id",
    "face id not working",
    "touch id not working",
    "touch id missing",
    "touch id issue",
    "touchid issue",
    "truedepth issue",
    "true depth issue",
    "mic issue",
    "microphone issue",
    "works with issues",
)


PHYSICAL_DAMAGE_TERMS = (
    "damaged",
    "badly damaged",
    "screen damage",
    "damaged screen",
    "screen is damaged",
    "screen badly damaged",
    "badly damaged screen",
    "screen damaged",
    "screen cracked",
    "cracked screen",
    "scratch screen",
    "scratched screen",
    "scrathed screen",
    "screen scratch",
    "screen scratched",
    "screen scrathed",
    "screen smashed",
    "smashed screen",
    "screen broken",
    "cracked",
    "smashed",
    "broken",
    "scratched",
    "scrathed",
    "dent",
    "dented",
    "chip",
    "chipped",
    "scratches",
    "heavy wear",
    "heavily used",
    "cosmetic damage",
    "cosmetically poor",
    "scraped screen",
    "physical damage",
    "damaged casing",
    "casing damage",
    "damaged case",
    "case damage",
    "damaged housing",
    "housing damage",
    "damaged shell",
    "shell damage",
    "damaged chassis",
    "chassis damage",
    "damage to phone",
    "damage to the phone",
    "damage to device",
    "damage to the device",
    "damage to laptop",
    "damage to the laptop",
    "damage to console",
    "damage to the console",
    "damage to camera",
    "damage to the camera",
    "damage to watch",
    "damage to the watch",
    "damage to bike",
    "damage to the bike",
    "damage to car",
    "damage to the car",
    "phone has damage",
    "phone with damage",
    "rear glass damage",
    "back glass damage",
    "front glass damage",
    "cracked rear glass",
    "cracked back glass",
    "cracked front glass",
)


def _has_functional_fault_language(blob: str) -> bool:
    normalised = re.sub(r"[^a-z0-9]+", " ", blob)
    normalised = re.sub(r"\s+", " ", normalised).strip()
    if any(term in normalised for term in FUNCTIONAL_FAULT_TERMS):
        return True
    return bool(
        re.search(
            r"\b(face id|touch id|true\s*depth|hdmi|charging|charge port|battery|mic|microphone|"
            r"speaker|camera|screen|display|disc drive|gimbal|lens|joystick|stick)\b"
            r".{0,40}\b(issue|fault|faulty|not working|does not work|doesn't work|missing|broken)\b",
            normalised,
        )
        or re.search(
            r"\b(works|working|fully working|tested and working)\b.{0,80}"
            r"\b(except|apart from|but|however)\b.{0,80}"
            r"\b(issue|fault|not working|does not work|doesn't work|missing|drift|no hdmi|no face id)\b",
            normalised,
        )
    )


def _has_physical_damage_language(blob: str) -> bool:
    normalised = re.sub(r"[^a-z0-9]+", " ", blob)
    normalised = re.sub(r"\s+", " ", normalised).strip()
    detection_text = re.sub(
        r"\b(?:no|not|without|free from)\s+(?:visible\s+|significant\s+|major\s+)?"
        r"(?:damage|damaged|crack|cracked|scratch|scratched|scratches|broken|breaks)\b",
        "",
        normalised,
    )
    detection_text = re.sub(r"\s+", " ", detection_text).strip()
    if any(term in detection_text for term in PHYSICAL_DAMAGE_TERMS):
        return True
    return bool(
        re.search(
            r"\b(damage|crack|cracked|smashed|scratch|scratched|scrathed|dent|dented|chip|chipped)\b.{0,30}\b(casing|case|housing|body|shell|chassis|glass|corner|lid|panel|screen|display)\b",
            detection_text,
        )
        or re.search(
            r"\b(screen|display)\b.{0,30}\b(damage|damaged|crack|cracked|smashed|scratch|scratched|scrathed|broken|faulty)\b",
            detection_text,
        )
    )


def _requires_condition_manual_review(blob: str) -> bool:
    if re.search(r"\b(condition unknown|unknown condition|not sure if (it )?(works|working)|may be faulty|may need repair|untested but|not fully tested)\b", blob):
        return True
    return bool(
        re.search(r"\b(fully working|working|works|tested and working)\b.{0,80}\b(but|except|apart from|however)\b", blob)
        and _has_functional_fault_language(blob)
    )


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
    "no power",
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
    "promo",
    "promotional",
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
    "partial",
    "build only",
    "builds only",
    "part only",
    "spare parts",
    "bulk bricks",
    "job lot bricks",
    "joblot bricks",
    "instruction book only",
    "instructions only",
    "box only",
    "custom",
    "compatible with lego",
    "lego compatible",
    "non-lego",
    "non lego",
    "not lego",
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
    "battery for",
    "battery replacement",
    "replacement battery",
    "hac-003",
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
    "software only",
    "console game",
    "video game",
    "game only",
    "game cartridge",
    "game card",
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

CONSOLE_ACCESSORY_OR_SOFTWARE_TERMS = (
    "wall charger",
    "usb-c charger",
    "usb c charger",
    "charger cable",
    "charging cable",
    "charging lead",
    "usb-c cable",
    "usb c cable",
    "cable for",
    "wireless controller",
    "pro controller",
    "controller for",
    "gamepad for",
    "carrying case",
    "protective case",
    "messenger bag",
    "travel bag",
    "case for",
    "bag for",
    "dock only",
    "dock for",
    "screen protector",
    "sd card",
    "memory card",
    "stand for",
    "grip for",
    "replacement part",
    "replacement shell",
    "replacement case",
    "game cartridge",
    "game card",
    "console game",
    "video game",
    "software only",
)


def _has_console_hardware_signal(title: str, blob: str) -> bool:
    title_text = _as_text(title)
    return any(term in title_text for term in CONSOLE_HARDWARE_TERMS) or bool(
        re.search(r"\b(128|256|500|512|825)\s?gb\b|\b(1|2)\s?tb\b", title_text)
        or re.search(r"\b(?:with|including|incl\.?)\s+(?:a\s+)?(?:console|system)\b", blob)
    )


def _looks_like_console_accessory_or_software(
    title: str,
    attrs: Optional[Mapping[str, Any]],
    blob: str,
) -> bool:
    title_text = _as_text(title)
    attrs_text = _listing_blob("", attrs)
    attr_keys = " ".join(_as_text(key) for key in (attrs or {}).keys())

    if any(key in attr_keys for key in ("game name", "video game name", "genre", "rating")):
        return True

    accessory_type = re.search(
        r"\b(usb\s*-?\s*cable|cable|charger|charging\s+dock|controller|gamepad|case|bag|"
        r"dock|screen\s+protector|sd\s+card|memory\s+card|stand|grip|replacement\s+part|"
        r"game|software)\b",
        attrs_text,
    )
    if accessory_type and not _has_console_hardware_signal(title, blob):
        return True

    if any(term in title_text for term in CONSOLE_ACCESSORY_OR_SOFTWARE_TERMS):
        return not _has_console_hardware_signal(title, blob)

    return bool(
        re.search(
            r"\b(charger|cable|controller|gamepad|case|bag|dock|stand|grip|game|software)\b"
            r".{0,40}\b(for|compatible with)\b",
            title_text,
        )
        and not _has_console_hardware_signal(title, blob)
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

COMPONENT_BUNDLE_TERMS = (
    "job lot",
    "joblot",
    "bundle",
    "collection",
    "lot of",
    "multiple",
    "2x ",
    "3x ",
    "4x ",
    "x2 ",
    "x3 ",
    "x4 ",
    "pair of",
)

GPU_NON_PRODUCT_TERMS = (
    "gpu box only",
    "graphics card box only",
    "empty gpu box",
    "empty graphics card box",
    "cooler only",
    "heatsink only",
    "heat sink only",
    "fan only",
    "replacement fan",
    "fan shroud",
    "backplate",
    "bracket",
    "gpu support",
    "riser",
    "riser cable",
    "water block",
    "waterblock",
    "mining rig",
    "laptop gpu",
    "mobile gpu",
    "mxm",
    "sxm",
    "repair service",
)

CPU_NON_PRODUCT_TERMS = (
    "cpu box only",
    "processor box only",
    "empty cpu box",
    "empty processor box",
    "cooler only",
    "stock cooler only",
    "heatsink only",
    "heat sink only",
    "fan only",
    "cpu keyring",
    "processor keyring",
    "engineering sample",
    "qualification sample",
    "motherboard compatible",
    "compatible motherboard",
    "processor not included",
)

MOTHERBOARD_NON_PRODUCT_TERMS = (
    "motherboard box only",
    "empty motherboard box",
    "i/o shield only",
    "io shield only",
    "wifi antenna only",
    "antenna only",
    "bios chip",
    "socket replacement",
    "repair service",
    "laptop motherboard",
    "server motherboard",
    "oem motherboard",
    "cpu included",
    "processor included",
    "ram included",
    "cpu bundle",
    "processor bundle",
    "ram bundle",
)

CAMERA_LENS_NON_PRODUCT_TERMS = (
    "cap only",
    "lens cap",
    "rear cap",
    "body cap",
    "hood only",
    "lens hood",
    "filter",
    "adapter",
    "mount adapter",
    "case only",
    "pouch only",
    "tripod collar",
    "collar only",
    "repair service",
    "fungus",
    "haze",
    "scratched glass",
)

INDUSTRIAL_AUTOMATION_NON_PRODUCT_TERMS = (
    "manual",
    "book",
    "catalogue",
    "cable",
    "lead",
    "connector",
    "enclosure",
    "cabinet",
    "control cabinet",
    "repair service",
    "service only",
    "training",
    "software only",
    "license only",
    "licence only",
    "generic switch",
    "network switch",
    "complete machinery",
    "machine only",
    "freight",
    "collection only",
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
    source_text = _as_text(source)
    model_key_text = _as_text(model_key)
    blob = _listing_blob(title, attrs, model_key)

    if any(term in blob for term in GENERIC_PART_ONLY_TERMS):
        return True

    if _requires_condition_manual_review(blob):
        return True

    if any(term in blob for term in SUSPICIOUS_PRODUCT_IDENTITY_TERMS):
        return True

    is_apple = "ebay-apple" in source_text or "apple-" in model_key_text or " apple " in f" {blob} "
    is_macbook = "macbook" in blob or "apple-macbook" in model_key_text
    if is_apple and _has_apple_lock_risk_language(blob):
        return True

    if is_apple and is_macbook and re.search(r"\ba\d{1,2}\s*(pro|max|ultra)?\b", blob):
        return True

    is_watch = "watch" in source_text or "omega" in blob or "rolex" in blob or "seamaster" in blob
    if is_watch and any(term in blob for term in WATCH_PART_TERMS):
        return True

    is_lego = "lego" in source_text or "lego" in blob
    if is_lego and any(term in blob for term in LEGO_PART_TERMS):
        return True

    is_console = any(
        marker in f"{source_text} {model_key_text} {blob}"
        for marker in ("console", "ps4", "ps5", "xbox", "switch", "playstation", "nintendo")
    )
    if is_console and _looks_like_console_accessory_or_software(title, attrs, blob):
        return True

    if any(term in blob for term in DEVICE_ACCESSORY_TERMS):
        return True

    if any(term in blob for term in DEVICE_PART_TERMS):
        return True

    electronic_or_vehicle = any(
        marker in f"{source_text} {model_key_text} {blob}"
        for marker in (
            "apple",
            "iphone",
            "ipad",
            "macbook",
            "samsung",
            "galaxy",
            "console",
            "ps4",
            "ps5",
            "xbox",
            "switch",
            "playstation",
            "nintendo",
            "gopro",
            "dji",
            "insta360",
            "graphics",
            "gpu",
            "rtx",
            "gtx",
            "radeon",
            "camera",
            "lens",
            "vehicle",
            "motor",
        )
    )
    if electronic_or_vehicle and (_has_functional_fault_language(blob) or _has_physical_damage_language(blob)):
        return True

    if any(term in blob for term in DEVICE_REPAIR_REVIEW_TERMS):
        return True

    if is_console and any(term in blob for term in CONSOLE_NON_HARDWARE_TERMS):
        return True

    complete_console_signal = _has_console_hardware_signal(title, blob)
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

    is_vehicle = _has_vehicle_signal(source=source_text, model_key=model_key_text, text=blob)
    if is_vehicle and any(term in blob for term in VEHICLE_PART_TERMS):
        return True

    is_gpu = (
        "ebay-graphics-cards" in source_text
        or model_key_text.startswith(
            (
                "nvidia-rtx-",
                "nvidia-gtx-",
                "nvidia-quadro-rtx-",
                "amd-radeon-rx-",
                "amd-radeon-pro-",
                "intel-arc-",
            )
        )
    )
    if is_gpu and (
        any(term in blob for term in GPU_NON_PRODUCT_TERMS)
        or any(term in blob for term in COMPONENT_BUNDLE_TERMS)
    ):
        return True

    is_cpu = (
        "ebay-cpus" in source_text
        or model_key_text.startswith(("amd-ryzen-", "intel-core-i"))
    )
    if is_cpu and (
        any(term in blob for term in CPU_NON_PRODUCT_TERMS)
        or any(term in blob for term in COMPONENT_BUNDLE_TERMS)
        or re.search(r"\b(xeon|epyc|server|laptop|notebook|mobile)\b", blob)
        or re.search(r"\b(bent|missing|damaged)\s+(pin|pins|pad|pads)\b", blob)
    ):
        return True

    is_motherboard = (
        "ebay-motherboards" in source_text
        or (
            source_text == "ebay-motherboards"
            and model_key_text.startswith(("asus-", "msi-", "gigabyte-", "asrock-"))
        )
    )
    if is_motherboard and (
        any(term in blob for term in MOTHERBOARD_NON_PRODUCT_TERMS)
        or any(term in blob for term in COMPONENT_BUNDLE_TERMS)
        or re.search(r"\b(bent|damaged|missing)\s+(socket|pin|pins|component|components)\b", blob)
        or re.search(r"\b(no post|no display|dead ram slot|dead pcie|dead m\.?2|liquid damage)\b", blob)
    ):
        return True

    is_camera_lens = source_text == "ebay-camera-lenses"
    if is_camera_lens and (
        any(term in blob for term in CAMERA_LENS_NON_PRODUCT_TERMS)
        or any(term in blob for term in COMPONENT_BUNDLE_TERMS)
    ):
        return True

    is_industrial_automation = source_text == "ebay-industrial-automation"
    if is_industrial_automation and (
        any(term in blob for term in INDUSTRIAL_AUTOMATION_NON_PRODUCT_TERMS)
        or any(term in blob for term in COMPONENT_BUNDLE_TERMS)
    ):
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
    if source_text in {"ebay-camera-lenses", "ebay-industrial-automation"}:
        return False

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
    is_gpu = (
        source_text == "ebay-graphics-cards"
        or mk.startswith(
            (
                "nvidia-rtx-",
                "nvidia-gtx-",
                "nvidia-quadro-rtx-",
                "amd-radeon-rx-",
                "amd-radeon-pro-",
                "intel-arc-",
            )
        )
    )
    is_cpu = source_text == "ebay-cpus" or mk.startswith(("amd-ryzen-", "intel-core-i"))
    is_motherboard = source_text == "ebay-motherboards"
    if (
        not is_apple
        and not is_samsung
        and not is_console
        and not is_actioncam
        and not is_lego
        and not is_motor_vehicle
        and not is_gpu
        and not is_cpu
        and not is_motherboard
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

    if is_gpu:
        if "_" in mk:
            return False
        nvidia_gpu = r"^nvidia-(rtx|gtx)-[1-5]\d{3}(-(ti|super))*-(4|6|8|10|11|12|16|20|24|32|48)gb$"
        nvidia_quadro = r"^nvidia-quadro-rtx-(4000|5000|6000|8000)-(8|16|24|48)gb$"
        nvidia_rtx_a = r"^nvidia-rtx-a(2000|4000|4500|5000|6000)-(6|8|12|16|20|24|48)gb$"
        nvidia_rtx_ada = r"^nvidia-rtx-(4000|4500|5000|6000)-ada-(20|24|32|48)gb$"
        amd_gpu = r"^amd-radeon-rx-[5-9]\d{3}(-(gre|xt|xtx))?-(4|6|8|10|12|16|20|24|32)gb$"
        amd_pro = r"^amd-radeon-pro-w(5700|6800|7500|7600|7800|7900)-(8|32|48)gb$"
        intel_gpu = r"^intel-arc-[ab]\d{3}-(4|6|8|10|12|16)gb$"
        return bool(
            re.match(nvidia_gpu, mk)
            or re.match(nvidia_quadro, mk)
            or re.match(nvidia_rtx_a, mk)
            or re.match(nvidia_rtx_ada, mk)
            or re.match(amd_gpu, mk)
            or re.match(amd_pro, mk)
            or re.match(intel_gpu, mk)
        )

    if is_cpu:
        if "_" in mk:
            return False
        amd_cpu = r"^amd-ryzen-[3579]-\d{4,5}(x3d|xt|x|g|ge)?$"
        intel_cpu = r"^intel-core-i[3579]-\d{4,5}(ks|kf|k|f|t)?$"
        return bool(re.match(amd_cpu, mk) or re.match(intel_cpu, mk))

    if is_motherboard:
        return False

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
    Map raw attributes + title into the coarse condition grade used for comps.

    Policy:
        A = clearly new-ish: brand new, sealed, unused, open box, like new.
        B = normal resale used: fully working with ordinary cosmetic marks or
            unclear-but-not-risky used condition.
        C = physical/cosmetic damage cap: cracked, smashed, dented, damaged
            casing/glass/body, heavy wear, or disclosed visible damage while
            still described as working.
        D = functional risk/fault: faulty, not working, spares/repair,
            untested, water/liquid damage, charging/display/HDMI/Face ID/stick
            drift faults, or incomplete/accessory-only listings.

    Contradictory or unclear wording is handled by
    looks_like_partial_or_accessory_listing(), which keeps those rows out of
    investible product identity/comps until a human reviews them.
    """

    # 1) Normalised text from title + attributes
    blob = _listing_blob(title, attrs)

    # ----------------
    # Grade D: faulty / spares / box-only
    # ----------------
    if _has_functional_fault_language(blob):
        return "D"

    accessory_only_terms = [
        "empty",
        "packaging only",
        "box only",
        "boxes only",
        "package only",
        "case only",
        "charger only",
        "battery only",
        "shell only",
        "housing only",
        "empty box",
        "empty packaging",
        "no console included",
        "no phone included",
        "no tablet included",
        "no blade",
        "no battery",
        "bare unit",
        "body only",
        "body-only",
        "no charger",
        "no accessories",
        "missing parts",
        "missing screws",
    ]
    if any(term in blob for term in accessory_only_terms):
        return "D"

    if "grade d" in blob:
        return "D"

    if re.search(r"\b(grade\s*c|used\s+fair|fair\s+condition|condition\s+fair)\b", blob):
        return "C"

    # Physical damage is a condition cap, not always a functional fault.
    if _has_physical_damage_language(blob):
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
        if "grade d" in grade_val or " d" in grade_val or "d " in grade_val:
            return "D"
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
        "no box", "without box", "unboxed", "no original box",
        "no manual", "no instructions", "missing manual",
    ]
    if any(term in blob for term in rough_terms):
        return "C"

    # Default: B (generic used / unknown)
    return "B"
