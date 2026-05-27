# agent/model_keys/lego.py
from __future__ import annotations

import re
from typing import Any, Mapping, Optional

from utils.condition import _derive_condition_grade

UNKNOWN_KEY = "unknown"

# Theme prefixes for minifigures
MINIFIG_THEME_MAP = {
    "sw": "star-wars",
    "sh": "super-heroes",
    "hp": "harry-potter",
    "col": "collectible-minifigures",
    "loc": "legends-of-chima",
    "mo": "monkie-kid",
    "pj": "ninjago",
    "poc": "pirates-of-the-caribbean",
    "pop": "prince-of-persia",
    "spd": "spiderman",
    "toy": "toy-story",
    "u": "ultra-agents",
    "v": "vidiyo",
    "w": "western",
}

# High-level LEGO themes found in the dataset
THEME_KEYWORDS = {
    "star-wars": ["star wars", "falcon", "vader", "jedi", "sith"],
    "technic": ["technic", "mclaren", "bugatti", "tractor", "motor"],
    "ninjago": ["ninjago", "kai", "lloyd", "zane", "spinjitzu"],
    "city": ["city", "police", "fire station", "tractor"],
    "super-heroes": ["super heroes", "marvel", "dc", "batman", "avengers"],
    "icons": ["icons", "tudor", "time machine", "creator expert"],
    "harry-potter": ["harry potter", "hogwarts", "wizarding world"],
    "disney": ["disney", "princess", "mickey", "minnie"],
    "stranger-things": ["stranger things", "castle byers", "upside down"],
    "speed-champions": ["speed champions", "f1", "ferrari", "porsche", "lamborghini", "mclaren f1", "race car"],
    "ideas": ["lego ideas", "ideas:", "ship in a bottle", "grand piano", "medieval blacksmith", "the globe", "big bang theory"],
    "minecraft": ["minecraft", "iron golem", "creeper", "nether", "crafting table"],
    "animal-crossing": ["animal crossing", "tom nook", "bunnie", "isabelle"],
    "architecture": ["architecture", "brandenburg gate", "trevi fountain", "london architecture", "skyline"],
    "friends": ["lego friends", "heartlake", "paisley", "horse show trailer", "unicorn", "friendship"],
    "lord-of-the-rings": ["lord of the rings", "lotr", "orthanc", "moria", "urak-hai", "hobbit", "mirkwood"],
    "jurassic-world": ["jurassic world", "jurassic park", "dilophosaurus", "t-rex", "velociraptor"],
    "fortnite": ["fortnite", "peely", "battle bus", "supply llama"],
    "super-mario": ["super mario", "mario kart", "spiny shell", "bowser", "luigi"],
    "botanicals": ["botanicals", "orchid", "daisies", "field flowers", "floral", "happy plants", "bouquet"],
    "brickheadz": ["brickheadz", "brick headz"],
    "pokemon": ["pokémon", "pokemon", "pikachu", "eevee", "venusaur", "charizard", "blastoise", "pokemon center", "pokémon center"],
    "duplo": ["duplo", "polar zoo", "town steam train"],
    "creator": ["creator 3 in 1", "creator", "viking ship", "orange cat", "parrot"],
}


def _clean(s: Any) -> str:
    """Basic string cleaner: None -> "", strip whitespace."""
    if s is None: return ""
    if isinstance(s, list):
        return " ".join(str(v).lower() for v in s)
    return str(s).strip()


def _derive_theme(attrs: Mapping[str, Any], title: str) -> str:
    """Identifies the LEGO theme for categorization."""
    t = _clean(attrs.get("Type")).lower()
    lego_theme = _clean(attrs.get("LEGO Theme")).lower()
    title_low = title.lower()
    full_text = f"{t} {lego_theme} {title_low}"

    for theme, keywords in THEME_KEYWORDS.items():
        if any(kw in full_text for kw in keywords):
            return theme
    return "generic"


def _derive_variant_suffix(attrs: Mapping[str, Any], title: str) -> str:
    """
    Capture high-signal listing variants that materially change LEGO value.

    This keeps promo / exclusive / polybag-style items out of broad set buckets.
    """
    desc = _clean(attrs.get("Bundle Description")).lower()
    subtheme = _clean(attrs.get("LEGO Subtheme")).lower()
    title_low = title.lower()
    full_text = f"{desc} {subtheme} {title_low}"

    variant_flags: list[str] = []

    if any(sig in full_text for sig in ["promo", "promotional", "gift with purchase", "gwp"]):
        variant_flags.append("promo")

    if any(sig in full_text for sig in ["exclusive", "store exclusive", "vip"]):
        variant_flags.append("exclusive")

    if any(sig in full_text for sig in ["polybag", "foil pack", "foilpack"]):
        variant_flags.append("polybag")

    if any(sig in full_text for sig in ["retired", "discontinued"]):
        variant_flags.append("retired")

    if not variant_flags:
        return ""

    return "-".join(variant_flags)


def _derive_completeness_suffix(attrs: Mapping[str, Any], title: str) -> str:
    """Adjusts the grade suffix based on completeness signals."""
    t = _clean(attrs.get("Type")).lower()
    desc = _clean(attrs.get("Bundle Description")).lower()
    full_text = f"{t} {desc} {title.lower()}"

    if any(sig in full_text for sig in ["incomplete", "missing pieces", "no minifigures"]):
        return "INC"

    missing_box = any(sig in full_text for sig in ["no box", "loose", "no original box"])
    missing_manual = any(sig in full_text for sig in ["no manual", "no instructions", "missing manual"])

    if missing_box and missing_manual: return "L"
    if missing_box: return "NB"
    if missing_manual: return "NM"
    return "C"


def _is_joblot(attrs: Mapping[str, Any], title: str) -> bool:
    """Detects if the listing is a bulk lot or joblot."""
    t = _clean(attrs.get("Type")).lower()
    title_low = title.lower()
    if any(sig in t for sig in ["joblot", "bulk", "mystery box", "kg"]):
        return not ("minifig" in t or "minifigure" in t)
    return any(sig in title_low for sig in ["joblot", "mixed", "uncounted"]) or bool(re.search(r'\d+\s?kg', title_low))


def _is_minifigure(attrs: Mapping[str, Any], title: str) -> bool:
    """Detects standalone minifigures or minifigure bundles."""
    t = _clean(attrs.get("Type")).lower()
    title_low = title.lower()
    if any(sig in t for sig in ["minifigure", "figure", "figurine"]):
        return not ("no minifigure" in t or "no box" in t)
    return ("minifigure" in title_low or "minifig" in title_low) and not any(
        x in title_low for x in ["with", "no", "without"])


def _normalise_brand(raw: Any, is_minifig: bool = False, is_bulk: bool = False) -> str:
    """Normalise Brand into a compact token."""
    if is_bulk: return "legobulk"
    if is_minifig: return "legominifig"
    s = _clean(raw)
    if not s: return "lego"
    low = s.lower()
    if "moc" in low: return "moclego"
    return "lego"


def _parse_id(raw: Any, is_minifig: bool = False, is_bulk: bool = False, title: str = "") -> str:
    """Extracts a Set number, Minifigure ID, Weight, or Bundle Count."""
    s = _clean(raw)
    full_text = f"{s} {title.lower()}"
    if is_bulk:
        m = re.search(r'(\d+)\s?kg', full_text)
        return f"{m.group(1)}kg" if m else "mixed"
    if is_minifig:
        bm = re.search(r'(\d+)\s?(?:x|minifig|figure)', full_text)
        if bm and int(bm.group(1)) > 1: return f"bundle-{bm.group(1)}"
        fm = re.search(r"\b([a-z]{1,4})(\d{3,5}[a-z]?)\b", s.lower())
        if fm:
            theme = MINIFIG_THEME_MAP.get(fm.group(1), fm.group(1))
            return f"{theme}-{fm.group(2)}"
    # Prefer explicit LEGO-ish set references first.
    explicit_patterns = [
        r"(?:set|lego|item|model|#)\s*[:#-]?\s*(\d{4,7})\b",
        r"\((\d{4,7})\)",
    ]
    for pattern in explicit_patterns:
        sm = re.search(pattern, s, re.I)
        if sm:
            return sm.group(1)

    # Then prefer longer numeric candidates (4-7 digits) over short incidental
    # numbers like "911" in Speed Champions titles.
    candidates = re.findall(r"\b(\d{3,7})\b", s)
    if candidates:
        filtered = []
        for candidate in candidates:
            try:
                as_int = int(candidate)
            except ValueError:
                continue
            if 1950 <= as_int <= 2035:
                continue  # avoid year collisions
            filtered.append(candidate)

        if not filtered:
            return ""

        preferred = [candidate for candidate in filtered if len(candidate) >= 4]
        if preferred:
            return max(preferred, key=len)

        return filtered[-1]
    return ""


def lego_model_key(attrs: Mapping[str, Any], title: str = "") -> Optional[str]:
    """Canonical model key for LEGO with theme and completeness tracking."""
    is_bulk = _is_joblot(attrs, title)
    is_minifig = False if is_bulk else _is_minifigure(attrs, title)
    brand = _normalise_brand(attrs.get("Brand"), is_minifig, is_bulk)
    theme = _derive_theme(attrs, title)

    CANDIDATE_KEYS = ["MPN", "Manufacturer Part Number", "Model Number", "Model",
                      "Numéro de l'assortiment LEGO", "NumÃ©ro de l'assortiment LEGO", "Herstellernummer"]

    set_id = ""
    for k in CANDIDATE_KEYS:
        set_id = _parse_id(attrs.get(k), is_minifig, is_bulk, title)
        if set_id: break
    if not set_id: set_id = _parse_id(title, is_minifig, is_bulk, title)
    if not brand or not set_id: return UNKNOWN_KEY

    grade = _derive_condition_grade(attrs, title)
    completeness = _derive_completeness_suffix(attrs, title)
    variant = _derive_variant_suffix(attrs, title)

    # Final key structure:
    #   {brand}-{theme}-{id}-{variant}_{grade}-{completeness}
    # Variant is optional and only used when it materially changes comps.
    base_parts = [brand, theme, set_id]
    if variant:
        base_parts.append(variant)

    return f"{'-'.join(base_parts)}_{grade}-{completeness}"
