from __future__ import annotations

import re
from typing import Any, Mapping, Optional


UNKNOWN_KEY = "unknown"


MAKE_ALIASES = {
    "citroa n": "citroen",
    "citroen": "citroen",
    "landrover": "land-rover",
    "land rover": "land-rover",
    "range rover": "land-rover",
    "mercedes": "mercedes-benz",
    "mercedes benz": "mercedes-benz",
    "mercedesbenz": "mercedes-benz",
    "mini": "mini",
    "skoda": "skoda",
    "a koda": "skoda",
    "toyoya": "toyota",
    "volgswagen": "volkswagen",
    "vw": "volkswagen",
    "vw split screen": "volkswagen",
    "vauxhall opel": "vauxhall",
    "vauxhall": "vauxhall",
    "vauxhaul": "vauxhall",
}

KNOWN_MAKES = {
    "abarth",
    "alfa-romeo",
    "aston-martin",
    "audi",
    "austin",
    "bentley",
    "bmw",
    "chevrolet",
    "chrysler",
    "citroen",
    "dacia",
    "dodge",
    "ds",
    "fiat",
    "ford",
    "honda",
    "hummer",
    "hyundai",
    "jaguar",
    "jeep",
    "kia",
    "land-rover",
    "lexus",
    "lotus",
    "maserati",
    "mazda",
    "mercedes-benz",
    "mg",
    "mini",
    "mitsubishi",
    "nissan",
    "peugeot",
    "porsche",
    "reliant",
    "renault",
    "rover",
    "saab",
    "seat",
    "skoda",
    "smart",
    "ssangyong",
    "subaru",
    "suzuki",
    "tesla",
    "toyota",
    "trabant",
    "vauxhall",
    "volkswagen",
    "volvo",
}

PART_TERMS = (
    "alloy wheel",
    "wheels and tyres",
    "wheel & tyre",
    "tyre",
    "tyres",
    "headlight",
    "headlamp",
    "rear light",
    "tail light",
    "bumper",
    "bonnet",
    "bootlid",
    "tailgate",
    "door handle",
    "grille",
    "radiator",
    "intercooler",
    "turbo",
    "exhaust",
    "brake disc",
    "brake pad",
    "caliper",
    "shock absorber",
    "strut",
    "spring",
    "seat",
    "steering wheel",
    "airbag",
    "dashboard",
    "engine control unit",
    "ecu",
    "cover headlamp",
)

NON_VEHICLE_TERMS = (
    "diecast",
    "die-cast",
    "model car",
    "model bike",
    "hot wheels",
    "matchbox",
    "corgi",
    "lego",
    "technic",
    "poster",
    "brochure",
    "owners manual",
    "owner s manual",
    "workshop manual",
    "haynes manual",
    "number plate",
    "private plate",
    "keyring",
    "t-shirt",
    "t shirt",
    "fishing",
    "carp",
    "daiwa",
    "trakker",
    "prologic",
    "nash",
)

MODEL_STOP_WORDS = {
    "sport",
    "msport",
    "m-sport",
    "m",
    "amg",
    "line",
    "sline",
    "s-line",
    "edition",
    "luxury",
    "premium",
    "quattro",
    "xdrive",
    "hatchback",
    "saloon",
    "estate",
    "auto",
    "manual",
    "diesel",
    "petrol",
    "bluehdi",
    "puretech",
    "tdi",
    "tfsi",
}


def _clean(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, (list, tuple, set)):
        return " ".join(_clean(item) for item in value)
    return str(value).strip().lower()


def _spaced(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", value.lower()).strip()


def _slug(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", _spaced(value)).strip("-")


def _blob(attrs: Mapping[str, Any], title: str) -> str:
    parts = [title]
    parts.extend(str(value) for value in attrs.values() if value is not None)
    return _spaced(" ".join(parts))


def _normalise_make(raw: Any, title: str = "") -> str:
    cleaned = _spaced(_clean(raw))
    if cleaned in {"other", "unbranded", "does not apply", "not supplied", "n a"}:
        cleaned = ""

    if cleaned:
        cleaned = MAKE_ALIASES.get(cleaned, cleaned)
        cleaned = _slug(cleaned)
        if cleaned in KNOWN_MAKES:
            return cleaned

        for make in sorted(KNOWN_MAKES, key=len, reverse=True):
            make_text = make.replace("-", " ")
            if re.search(rf"\b{re.escape(make_text)}\b", cleaned.replace("-", " ")):
                return make

    title_text = _spaced(title)
    for make in sorted(KNOWN_MAKES, key=len, reverse=True):
        if re.search(rf"\b{re.escape(make.replace('-', ' '))}\b", title_text):
            return make
    return ""


def _extract_make(attrs: Mapping[str, Any], title: str) -> str:
    for key in ("Manufacturer", "Make", "Brand"):
        make = _normalise_make(attrs.get(key), title)
        if make:
            return make
    return _normalise_make("", title)


def _extract_year(attrs: Mapping[str, Any], title: str) -> str:
    for value in (
        _clean(attrs.get("Year")),
        _clean(attrs.get("Year Manufactured")),
        _clean(attrs.get("Date of 1st Registration")),
        _clean(title),
    ):
        match = re.search(r"\b(19\d{2}|20[0-2]\d)\b", value)
        if match:
            return match.group(1)
        date_match = re.search(r"\b(19\d{2}|20[0-2]\d)\d{4}\b", value)
        if date_match:
            return date_match.group(1)
        uk_date_match = re.search(r"\b\d{4}(19\d{2}|20[0-2]\d)\b", value)
        if uk_date_match:
            return uk_date_match.group(1)
    return ""


def _extract_engine(attrs: Mapping[str, Any], title: str) -> str:
    text = _spaced(
        " ".join(
            _clean(attrs.get(key))
            for key in ("Engine Size", "Engine", "Engine Number")
        )
        + " "
        + title
    )
    cc_match = re.search(r"\b(\d{3,4})\s?cc\b", text)
    if cc_match:
        return f"{cc_match.group(1)}cc"

    litre_match = re.search(r"\b([0-5])[\s.]([0-9])\s?(?:l|ltr|litre)?\b", text)
    if litre_match:
        return f"{litre_match.group(1)}.{litre_match.group(2)}l"

    return ""


def _extract_fuel(attrs: Mapping[str, Any], title: str) -> str:
    text = _spaced(" ".join([_clean(attrs.get("Fuel Type")), _clean(attrs.get("Fuel")), title]))
    fuel_map = (
        ("diesel electric", "diesel-hybrid"),
        ("petrol electric", "petrol-hybrid"),
        ("petrol mhev", "petrol-mhev"),
        ("diesel mhev", "diesel-mhev"),
        ("phev", "phev"),
        ("hybrid", "hybrid"),
        ("electricity", "electric"),
        ("electric", "electric"),
        ("diesel", "diesel"),
        ("petrol", "petrol"),
        ("lpg", "lpg"),
    )
    for needle, token in fuel_map:
        if needle in text:
            return token
    return ""


def _extract_transmission(attrs: Mapping[str, Any], title: str) -> str:
    text = _spaced(f"{_clean(attrs.get('Transmission'))} {title}")
    if "automatic" in text or "auto" in text or "dsg" in text:
        return "automatic"
    if "manual" in text:
        return "manual"
    return ""


def _extract_body(attrs: Mapping[str, Any], title: str) -> str:
    text = _spaced(f"{_clean(attrs.get('Body Type'))} {_clean(attrs.get('Car Type'))} {title}")
    body_map = (
        ("campervan", "campervan"),
        ("convertible", "convertible"),
        ("cabriolet", "convertible"),
        ("roadster", "roadster"),
        ("coupe", "coupe"),
        ("estate", "estate"),
        ("tourer", "estate"),
        ("hatchback", "hatchback"),
        ("saloon", "saloon"),
        ("suv", "suv"),
        ("4x4", "suv"),
        ("mpv", "mpv"),
        ("van", "van"),
        ("pickup", "pickup"),
        ("pick up", "pickup"),
    )
    for needle, token in body_map:
        if needle in text:
            return token
    return ""


def _extract_drive(attrs: Mapping[str, Any], title: str) -> str:
    text = _spaced(f"{_clean(attrs.get('Drivetrain'))} {title}")
    if "quattro" in text or "4wd" in text or "4x4" in text or "awd" in text or "xdrive" in text:
        return "awd"
    if "rwd" in text:
        return "rwd"
    if "fwd" in text:
        return "fwd"
    return ""


def _extract_model(attrs: Mapping[str, Any], make: str, title: str) -> str:
    raw = _clean(attrs.get("Model")) or _clean(attrs.get("Variant"))
    if not raw or raw in {"not supplied", "does not apply", "n a", "."}:
        raw = title

    text = _spaced(raw)
    if make:
        text = re.sub(rf"\b{re.escape(make.replace('-', ' '))}\b", " ", text)
    year = _extract_year(attrs, title)
    if year:
        text = re.sub(rf"\b{year}\b", " ", text)

    tokens = [token for token in text.split() if token and token not in MODEL_STOP_WORDS]
    if not tokens:
        return ""

    out: list[str] = []
    for token in tokens:
        if token in MODEL_STOP_WORDS:
            break
        out.append(token)
        if any(ch.isdigit() for ch in token) and len(out) >= 2:
            break
        if len(out) >= 3:
            break

    return "".join(out)


def _is_vehicle_listing(attrs: Mapping[str, Any], title: str, make: str) -> bool:
    text = _blob(attrs, title)
    if any(term in text for term in NON_VEHICLE_TERMS):
        return False
    if any(term in text for term in PART_TERMS):
        return False
    if not make:
        return False
    return True


def motors_model_key(attrs: Mapping[str, Any], title: str = "") -> Optional[str]:
    safe_attrs = attrs or {}
    make = _extract_make(safe_attrs, title)
    if not _is_vehicle_listing(safe_attrs, title, make):
        return UNKNOWN_KEY

    model = _extract_model(safe_attrs, make, title)
    if not model:
        return UNKNOWN_KEY

    parts = ["vehicle", make, model]
    for value in (
        _extract_engine(safe_attrs, title),
        _extract_fuel(safe_attrs, title),
        _extract_transmission(safe_attrs, title),
        _extract_drive(safe_attrs, title),
        _extract_body(safe_attrs, title),
        _extract_year(safe_attrs, title),
    ):
        if value and value not in parts:
            parts.append(value)

    return "-".join(parts)
