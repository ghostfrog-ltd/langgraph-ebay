from __future__ import annotations
from typing import Mapping, Any, Optional
import re


def _clean(v: Any) -> str:
    if not v: return ""
    return str(v).strip().lower().replace(" ", "-").replace("\"", "").replace("'", "")


def _text(v: Any) -> str:
    if v is None:
        return ""
    if isinstance(v, (list, tuple, set)):
        return " ".join(_text(x) for x in v)
    return str(v).strip().lower()


def _num(v: Any) -> str:
    """Simple digit extractor (used for things like mm sizes)."""
    if not v:
        return ""
    return "".join(ch for ch in str(v) if ch.isdigit())


def _canonical_capacity(value: str) -> str:
    m = re.fullmatch(r"(\d+)(gb|tb)", value)
    if not m:
        return value
    amount = int(m.group(1))
    unit = m.group(2)
    if unit == "gb":
        if amount in {240, 250, 251, 255, 256}:
            return "256gb"
        if amount in {480, 500, 512}:
            return "512gb"
        if amount in {960, 1000, 1024}:
            return "1tb"
    return value


def _capacity_token(amount: str, unit: str) -> str:
    return _canonical_capacity(f"{int(amount)}{unit.lower()}")


def _capacity_from_labelled_text(text: str, labels_after: tuple[str, ...], labels_before: tuple[str, ...]) -> str:
    lowered = text.lower()
    for label in labels_after:
        m = re.search(rf"\b(\d{{1,4}})\s*(gb|tb)\s*(?:{label})\b", lowered)
        if m:
            return _capacity_token(m.group(1), m.group(2))
    for label in labels_before:
        m = re.search(rf"\b(?:{label})\s*(\d{{1,4}})\s*(gb|tb)\b", lowered)
        if m:
            return _capacity_token(m.group(1), m.group(2))
    return ""


def _device_storage(attrs: Mapping[str, Any], title: str) -> str:
    """Extract phone/tablet storage without using RAM/unified-memory values."""
    storage = _extract_storage(attrs, title)
    if storage:
        return storage

    blob = " ".join(
        [title]
        + [
            str(attrs.get(k) or "")
            for k in [
                "Storage Capacity",
                "Capacity",
                "Model",
                "Product Line",
                "Series",
            ]
        ]
    ).lower()
    candidates = []
    for match in re.finditer(r"\b(\d{1,4})\s*(gb|tb)\b", blob):
        before = blob[max(0, match.start() - 24) : match.start()]
        after = blob[match.end() : match.end() + 24]
        amount = int(match.group(1))
        unit = match.group(2)
        if (
            re.search(r"^\s*(ram|memory|unified)", after)
            or (
                unit == "gb"
                and amount <= 128
                and re.search(r"(ram|memory|unified)\s*$", before)
            )
        ):
            continue
        token = _capacity_token(match.group(1), unit)
        if token not in candidates:
            candidates.append(token)
    return candidates[-1] if candidates else ""


def _extract_storage(attrs: Mapping[str, Any], title: str) -> str:
    """Extract storage from SSD/storage evidence; never read RAM fields as disk."""
    storage_attrs = " ".join(
        str(attrs.get(k) or "")
        for k in ["Storage Capacity", "SSD Capacity", "Hard Drive Capacity", "Capacity"]
    )

    title_low = title.lower()
    slash = re.search(
        r"\b(\d{2,4})\s*(gb|tb)\s*/\s*(4|6|8|12|16|18|24|32|36|48|64|96|128)\s*gb\b",
        title_low,
    )
    if slash:
        return _capacity_token(slash.group(1), slash.group(2))

    storage = _capacity_from_labelled_text(
        f"{title} {storage_attrs}",
        labels_after=("ssd|storage|hard\\s*drive|hdd|flash",),
        labels_before=("ssd|storage|hard\\s*drive|hdd|flash|capacity",),
    )
    if storage:
        return storage

    m = re.search(r"\b(\d{2,4})\s*(gb|tb)\b", storage_attrs.lower())
    if m:
        return _capacity_token(m.group(1), m.group(2))

    candidates: list[str] = []
    for match in re.finditer(r"\b(\d{1,4})\s*(gb|tb)\b", title_low):
        before = title_low[max(0, match.start() - 24) : match.start()]
        after = title_low[match.end() : match.end() + 24]
        amount = int(match.group(1))
        unit = match.group(2)
        if (
            re.search(r"^\s*(ram|memory|unified)", after)
            or (
                unit == "gb"
                and amount <= 128
                and re.search(r"(ram|memory|unified)\s*$", before)
            )
        ):
            continue
        token = _capacity_token(match.group(1), unit)
        if token not in candidates:
            candidates.append(token)
    return candidates[-1] if candidates else ""


def _extract_ram(attrs: Mapping[str, Any], title: str) -> str:
    """Extract RAM from RAM/memory evidence and ignore impossible disk-sized RAM attrs."""
    val = " ".join([title] + [str(attrs.get(k) or "") for k in ["RAM Size", "Memory", "RAM"]]).lower()
    labelled = re.search(
        r"\b(4|6|8|12|16|18|24|32|36|48|64|96|128)\s*gb\s*(ram|memory|unified\s*memory)\b",
        val,
    )
    if labelled:
        return f"{labelled.group(1)}gb"

    adjacent = re.search(
        r"\b(4|6|8|12|16|18|24|32|36|48|64|96|128)\s*gb\b.{0,24}\b\d{2,4}\s*(gb|tb)\s*(ssd|storage|hard\s*drive|hdd|flash)\b",
        val,
    )
    if adjacent:
        return f"{adjacent.group(1)}gb"

    attr_text = " ".join(str(attrs.get(k) or "") for k in ["RAM Size", "Memory", "RAM"]).lower()
    m = re.search(r"\b(\d{1,3})\s*gb\b", attr_text)
    if m:
        amount = int(m.group(1))
        if amount in {4, 6, 8, 12, 16, 18, 24, 32, 36, 48, 64, 96, 128}:
            return f"{amount}gb"

    slash = re.search(r"\b(4|6|8|12|16|18|24|32|36|48|64|96|128)\s*gb\s*/\s*\d{2,4}\s*(gb|tb)\b", val)
    if slash:
        return f"{slash.group(1)}gb"
    return ""


def _extract_size(attrs: Mapping[str, Any], title: str) -> str:
    """Extracts screen size (e.g., '13in', '142in', '16in') for Macs/iPads."""
    val = " ".join([str(attrs.get(k) or "") for k in ["Screen Size", "Display"]] + [title]).lower()
    m = re.search(r"(\d{2}(\.\d)?)\s*(inch|\"|in)", val)
    if not m:
        return ""
    size = m.group(1)
    if size.endswith(".0"):
        size = size[:-2]
    return f"{size.replace('.', '')}in"


def _extract_chassis(attrs: Mapping[str, Any], title: str) -> str:
    val = " ".join(str(attrs.get(k) or "") for k in ["MPN", "Model", "Processor Model"]) + " " + title
    m = re.search(r"\b(a\d{4})\b", val.lower())
    return m.group(1) if m else ""


def _chip_from_text(text: str) -> str:
    chip_src = text.lower()

    if re.search(r"\bcore\s*m3\b|\bm3-\d|\bm3\s*y\d{2}\b", chip_src):
        return "core-m3"

    intel = re.search(r"\b(?:intel\s*)?(?:core\s*)?i([3579])\b", chip_src)
    if intel:
        return f"core-i{intel.group(1)}"

    apple_silicon = re.search(r"\b(?:apple\s*)?m([1-5])\s*(pro|max|ultra)?\b", chip_src)
    if apple_silicon and not re.search(r"\b(?:intel|core)\s+m[1-5]\b", chip_src):
        return f"m{apple_silicon.group(1)}{(apple_silicon.group(2) or '').strip()}"

    if "intel" in chip_src:
        return "intel"

    return ""


def _extract_chip(attrs: Mapping[str, Any], title: str) -> str:
    attr_src = " ".join(str(attrs.get(k) or "") for k in ["Chipset Model", "Processor", "CPU"])
    title_chip = _chip_from_text(title)
    attr_chip = _chip_from_text(attr_src)
    if title_chip and attr_chip and title_chip != attr_chip:
        return ""
    return title_chip or attr_chip


def macbook_specs_conflict(attrs: Mapping[str, Any], title: str) -> bool:
    """Flag incompatible specs independently reported by a listing title and its attributes."""
    for extract in (_extract_chassis, _extract_size, _extract_chip, _extract_ram, _extract_storage):
        from_title = extract({}, title)
        from_attrs = extract(attrs, "")
        if from_title and from_attrs and from_title != from_attrs:
            return True
    return False


def _apple_blob(attrs: Mapping[str, Any], title: str) -> str:
    return " ".join(
        [title]
        + [
            _text(attrs.get(k))
            for k in [
                "Model",
                "Product Line",
                "Series",
                "Type",
                "Processor",
                "Chipset Model",
                "CPU",
                "MPN",
            ]
            if attrs.get(k)
        ]
    ).lower()


def _contains_junk_apple_listing(blob: str) -> bool:
    junk_terms = (
        "case for",
        "cover for",
        "screen protector",
        "tempered glass",
        "replacement screen",
        "screen assembly",
        "lcd assembly",
        "digitizer",
        "logic board",
        "logicboard",
        "motherboard",
        "housing",
        "back glass",
        "front glass",
        "sim tray",
        "flex cable",
        "charger only",
        "cable only",
        "case only",
        "box only",
        "empty box",
        "dummy phone",
        "display phone",
    )
    return any(term in blob for term in junk_terms)


def _extract_connectivity(attrs: Mapping[str, Any], title: str) -> str:
    blob = _apple_blob(attrs, title)
    if re.search(r"\b(cellular|cellular\+wifi|wifi\+cellular|wi-fi\+cellular|gps\+cellular|5g|lte|4g|cell|cel)\b", blob):
        return "cellular"
    if re.search(r"\b(gps only|gps-only|gps)\b", blob):
        return "gps"
    if re.search(r"\b(wifi|wi-fi)\b", blob):
        return "wifi"
    return ""


def _iphone_model_token(attrs: Mapping[str, Any], title: str) -> str:
    blob = _apple_blob(attrs, title)
    clean_blob = _clean(blob)

    if re.search(r"\biphone\s*se\b", blob) or "iphone-se" in clean_blob:
        if re.search(r"\b(2022|3rd|third)\b", blob):
            return "se-3"
        if re.search(r"\b(2020|2nd|second)\b", blob):
            return "se-2"
        if re.search(r"\b(2016|1st|first)\b", blob):
            return "se-1"
        return "se"

    for legacy in ("xs max", "xs", "xr", "x"):
        if re.search(rf"\biphone\s*{re.escape(legacy)}\b", blob):
            return legacy.replace(" ", "-")

    match = re.search(r"\biphone\s*(\d{1,2})\b", blob)
    if not match:
        return ""

    base = match.group(1)
    tail = blob[match.end() : match.end() + 48]
    if re.search(r"\bpro\s*max\b", tail):
        return f"{base}-pro-max"
    if re.search(r"\bair\b", tail):
        return f"{base}-air"
    if re.search(r"\bmini\b", tail):
        return f"{base}-mini"
    if re.search(r"\bplus\b", tail):
        return f"{base}-plus"
    if re.search(r"\bpro\b", tail):
        return f"{base}-pro"
    return base


def _ipad_generation_token(attrs: Mapping[str, Any], title: str) -> str:
    blob = _apple_blob(attrs, title)
    gen = re.search(r"\b(\d{1,2})(?:st|nd|rd|th)\s*(?:gen|generation)\b", blob)
    if gen:
        return f"{gen.group(1)}th-gen"
    gen = re.search(r"\b(?:gen|generation)\s*(\d{1,2})\b", blob)
    if gen:
        return f"{gen.group(1)}th-gen"
    year_to_gen = {
        "2025": "11th-gen",
        "2022": "10th-gen",
        "2021": "9th-gen",
        "2020": "8th-gen",
        "2019": "7th-gen",
        "2018": "6th-gen",
        "2017": "5th-gen",
    }
    if "ipad pro" not in blob and "ipad air" not in blob and "ipad mini" not in blob:
        for year, generation in year_to_gen.items():
            if re.search(rf"\b{year}\b", blob):
                return generation
    return ""


def _watch_model_token(attrs: Mapping[str, Any], title: str) -> str:
    blob = _apple_blob(attrs, title)
    if "ultra" in blob:
        if re.search(r"\bultra\s*2\b|\b2nd\b|\bsecond\b", blob):
            return "ultra-2"
        return "ultra"
    if re.search(r"\bwatch\s*se\b|\bse\b", blob):
        if re.search(r"\b(2nd|second|2022)\b", blob):
            return "se-2"
        return "se"
    series = re.search(r"\bseries\s*(\d{1,2})\b", blob)
    return f"series-{series.group(1)}" if series else ""


def _watch_size_token(attrs: Mapping[str, Any], title: str) -> str:
    blob = _apple_blob(attrs, title)
    m = re.search(r"\b(38|40|41|42|44|45|46|49)\s*mm\b", blob)
    if m:
        return f"{m.group(1)}mm"
    case_size = _num(attrs.get("Case Size") or "")
    return f"{case_size}mm" if case_size in {"38", "40", "41", "42", "44", "45", "46", "49"} else ""


def _airpods_key(attrs: Mapping[str, Any], title: str) -> str:
    blob = _apple_blob(attrs, title)
    if "airpods max" in blob:
        return "apple-airpods-max"
    if "airpods pro" in blob:
        gen = "2" if re.search(r"\b(2nd|second|gen\s*2|generation\s*2|pro\s*2)\b", blob) else "1"
        connector = "usbc" if re.search(r"\b(usb-c|usbc|type-c)\b", blob) else "lightning" if "lightning" in blob else ""
        return "-".join(p for p in ["apple", "airpods-pro", gen, connector] if p)
    if "airpods" in blob or "airpod" in blob:
        gen_match = re.search(r"\b(?:airpods?|gen|generation)\s*(\d)\b", blob)
        gen = gen_match.group(1) if gen_match else ""
        if not gen:
            return "unknown"
        suffix = "anc" if re.search(r"\b(active noise cancellation|anc)\b", blob) else ""
        return "-".join(p for p in ["apple", "airpods", gen, suffix] if p)
    return ""


def apple_model_key(attrs: Mapping[str, Any], title: str = "") -> Optional[str]:
    """
    Granular Apple identity key generator.

    Mac keys are strict product identity only:
    apple-<line>-<chassis?>-<screen>-<chip>-<ram>-<storage>

    Condition is deliberately stored separately in auction_listings.condition_grade.
    """
    def _brand_is_apple() -> bool:
        brand = _clean(attrs.get("Brand") or attrs.get("Marca") or attrs.get("brand"))
        if "apple" in brand:
            return True
        title_blob = title.lower()
        return bool(
            re.search(
                r"\b(apple|iphone|ipad|macbook|imac|airpods?|airtag|ipod)\b",
                title_blob,
            )
        )

    if not _brand_is_apple(): return None

    apple_blob = _apple_blob(attrs, title)
    if _contains_junk_apple_listing(apple_blob):
        return "unknown"

    # 1. Base Spec Extraction
    storage = _extract_storage(attrs, title)
    ram = _extract_ram(attrs, title)
    size = _extract_size(attrs, title)
    chassis = _extract_chassis(attrs, title)
    chip = _extract_chip(attrs, title)

    # 3. Family Logic
    family_text = " ".join(
        _text(attrs.get(k))
        for k in ["Product Line", "Series", "Model", "Type"]
        if attrs.get(k)
    )
    family_title = title.lower()
    family_title_clean = _clean(family_title)
    family_blob = _clean(f"{family_text} {family_title}")

    parts = ["apple"]

    # --- AIRPODS / SMALL AUDIO ---
    if "airpods" in family_blob or "airpod" in family_blob:
        return _airpods_key(attrs, title)

    # --- MACS ---
    if any(x in family_blob for x in ("macbook", "imac", "mac-mini", "mac-studio", "mac-pro")):
        if "macbook" in family_blob:
            line = (
                "macbook-air"
                if "macbook-air" in family_title_clean
                else "macbook-pro"
                if "macbook-pro" in family_title_clean
                else "macbook-air"
                if "air" in family_blob
                else "macbook-pro"
                if "pro" in family_blob
                else "macbook"
            )
        elif "mac-mini" in family_blob or "mini" in family_blob:
            line = "mac-mini"
        elif "studio" in family_blob:
            line = "mac-studio"
        elif "mac-pro" in family_blob:
            line = "mac-pro"
        else:
            line = "imac"
        if line == "macbook-air" and chip in {"m2", "m3", "m4"} and size == "13in":
            size = "136in"
        if line.startswith("macbook") and not (chip and ram and storage and (chassis or size)):
            return "unknown"
        if not line.startswith("macbook") and not (chip and storage and (chassis or size)):
            return "unknown"
        parts.extend([line, chassis, size, chip, ram, storage])

    # --- IPHONES ---
    elif "iphone" in family_blob:
        model = _iphone_model_token(attrs, title)
        phone_storage = _device_storage(attrs, title)
        if not model or not phone_storage:
            return "unknown"
        parts.extend(["iphone", model, phone_storage])

    # --- IPADS ---
    elif "ipad" in family_blob:
        tablet_storage = _device_storage(attrs, title)
        connectivity = _extract_connectivity(attrs, title)
        if "ipad-mini" in family_blob or re.search(r"\bipad\s*mini\b", apple_blob):
            line = "ipad-mini"
        elif "ipad-pro" in family_blob or re.search(r"\bipad\s*pro\b", apple_blob):
            line = "ipad-pro"
        elif "ipad-air" in family_blob or re.search(r"\bipad\s*air\b", apple_blob):
            line = "ipad-air"
        else:
            line = "ipad"

        if line in {"ipad-pro", "ipad-air"}:
            identity = chip or _ipad_generation_token(attrs, title)
            if not (size and identity and tablet_storage):
                return "unknown"
            parts.extend([line, size, identity, tablet_storage, connectivity])
        else:
            generation = _ipad_generation_token(attrs, title)
            if not (generation and tablet_storage):
                return "unknown"
            parts.extend([line, size, generation, tablet_storage, connectivity])

    # --- WATCHES ---
    elif "watch" in family_blob:
        model = _watch_model_token(attrs, title)
        watch_size = _watch_size_token(attrs, title)
        connectivity = _extract_connectivity(attrs, title)
        if not (model and watch_size):
            return "unknown"
        parts.extend(["watch", model, watch_size, connectivity])

    # --- APPLE TV / HOMEPOD / AIRTAG / IPOD ---
    elif "apple-tv" in family_blob or "apple-tv" in _clean(title):
        blob = apple_blob
        line = "tv-4k" if "4k" in blob else "tv-hd" if "hd" in blob else ""
        tv_storage = _device_storage(attrs, title)
        if not line:
            return "unknown"
        parts = ["apple", line, tv_storage]
    elif "homepod" in family_blob:
        model = "homepod-mini" if "mini" in family_blob else "homepod"
        generation = "2nd-gen" if re.search(r"\b(2nd|second|2023)\b", apple_blob) else ""
        parts = ["apple", model, generation]
    elif "airtag" in family_blob or "air-tag" in family_blob:
        pack = "4pack" if re.search(r"\b(4|four)\s*(pack|pk)\b", apple_blob) else "1pack"
        parts = ["apple", "airtag", pack]
    elif "ipod" in family_blob:
        blob = apple_blob
        line = "ipod-touch" if "touch" in blob else "ipod-nano" if "nano" in blob else "ipod-classic" if "classic" in blob else ""
        ipod_storage = _device_storage(attrs, title)
        if not (line and ipod_storage):
            return "unknown"
        parts = ["apple", line, ipod_storage]

    # 4. Final Cleanup
    base_key = "-".join([p for p in parts if p]).strip("-")
    if base_key == "apple": return "unknown"
    return base_key
