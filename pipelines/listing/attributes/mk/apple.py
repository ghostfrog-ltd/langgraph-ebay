from __future__ import annotations
from typing import Mapping, Any, Optional
import re

from .apple_catalog import (
    MACBOOK_AIRS, MACBOOK_M5_PRO_CONFIGS, MACBOOK_M5_PRO_PARTS, MACBOOK_PROS, MACBOOK_PRO_GPU_CORES,
)
from .apple_devices import (
    airpods_key, airtag_key, desktop_key, evidence, gpu_cores, ipad_key, iphone_key, matches,
    model_numbers, screen_sizes, watch_key,
)


def _clean(v: Any) -> str:
    if not v: return ""
    return str(v).strip().lower().replace(" ", "-").replace("\"", "").replace("'", "")


def _text(v: Any) -> str:
    if v is None:
        return ""
    if isinstance(v, (list, tuple, set)):
        return " ".join(_text(x) for x in v)
    return str(v).strip().lower()


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


def _mac_line(text: str) -> str:
    match = re.search(r'\b(macbook(?:[ -]+(?:air|pro|neo))?|imac(?:[ -]+pro)?|mac[ -]*(?:mini|studio|pro))\b', text)
    if not match:
        return ''
    return re.sub(r'^mac(?=mini|studio|pro)', 'mac-', match[1].replace(' ', '-'))


def _macbook_neo_key(attrs, title, chip, ram, storage, chassis_ids, sizes, gpu):
    parts = r'\b(?:replacement (?:keyboard|battery)|(?:screen|keyboard|battery) for)\b'
    if any(re.search(parts, value) for value in evidence(attrs, title, 'Type', 'Model')):
        return 'unknown'
    processor = evidence(attrs, title, 'Processor', 'CPU', 'Processor Model', 'Chipset Model')
    cores = matches(r'\b(\d{1,2})[ -]*c(?:ores?)?[ -]*cpu\b', processor)
    a_series = {re.sub(r'[ -]', '', value) for value in matches(r'\b(a\d{2}(?:[ -]*pro)?)\b', processor)}
    if (chip or a_series - {'a18pro'} or cores - {'6'} or chassis_ids - {'a3404'}
            or sizes - {'13in'} or gpu - {5} or ram != '8gb' or storage not in {'256gb', '512gb'}):
        return 'unknown'

    touch_id = r'\btouch[ -]*id\b'
    without_touch_id = r'\b(?:no|without|non)[ -]+touch[ -]*id\b'
    texts = evidence(attrs, title, 'Model', 'Features')
    if storage == '512gb' and any(re.search(without_touch_id, text) for text in texts):
        return 'unknown'
    if storage == '256gb' and any(re.search(touch_id, re.sub(without_touch_id, '', text)) for text in texts):
        return 'unknown'
    return f'apple-macbook-neo-a3404-13in-a18pro-8gb-{storage}'


def _macbook_m5_pro_key(attrs, title, chip, ram, storage, chassis_ids, sizes, gpu, parts):
    display = evidence(attrs, title, 'Display', 'Display Type', 'Screen Size', 'Model')
    panels = set()
    if any(re.search(r'\b(?:proled|oled)\b', value) for value in display):
        panels.add('proled')
    if any(re.search(r'\b(?:liquid retina xdr|mini[ -]?led)\b', value) for value in display):
        panels.add('liquid')
    cores = matches(r'\b(\d{1,2})[ -]*c(?:ores?)?[ -]*cpu\b', evidence(attrs, title, 'Processor', 'CPU'))
    candidates = []
    for chassis, size, cpu, panel, gpu_count, memory, capacities in MACBOOK_M5_PRO_CONFIGS:
        rounded = f'{size[:2]}in'
        cpu_count = '10' if cpu == 'm5' else '15' if gpu_count == 16 else '18'
        if ((chip and chip != cpu) or chassis_ids - {chassis} or any(chassis not in ids for ids in parts)
                or sizes - {size, rounded} or panels - {panel} or gpu - {gpu_count} or cores - {cpu_count}
                or ram not in memory.split() or storage not in capacities.split()):
            continue
        variant = '' if cpu == 'm5' else f'-{gpu_count}gpu'
        candidates.append(f'apple-macbook-pro-{chassis}-{size}-{cpu}{variant}-{ram}-{storage}')
    return candidates[0] if len(candidates) == 1 else 'unknown'


def _macbook_key(attrs, title, line, chip, ram, storage):
    model_text = evidence(attrs, title, 'MPN', 'Model', 'Processor Model', 'Model Number', 'Type')
    chassis_ids = model_numbers(model_text)
    gpu = gpu_cores(attrs, title)
    if len(gpu) > 1:
        return 'unknown'
    screens = evidence(attrs, title, 'Screen Size', 'Display')
    screens.extend(f'{value}in' for value in evidence(attrs, '', 'Screen Size') if re.fullmatch(r'\d{2}(\.\d)?', value))
    shorthand = matches(r'\bmacbook(?:[ -]+(?:air|pro|neo))?[ -]+(1[3-6](?:\.[236])?)\b(?![.\d]|\s*(?:gb|tb)\b)',
                        evidence({}, title))
    screens.extend(f'{value}in' for value in shorthand)
    sizes = screen_sizes(screens)
    if len(chassis_ids) > 1 or not (ram and storage) or storage in {'1gb', ram}:
        return 'unknown'

    if line == 'macbook-neo' or 'a3404' in chassis_ids:
        if line not in {'macbook', 'macbook-neo'}:
            return 'unknown'
        return _macbook_neo_key(attrs, title, chip, ram, storage, chassis_ids, sizes, gpu)

    air_ids = {row[0] for row in MACBOOK_AIRS}
    air_chips = {row[1] for row in MACBOOK_AIRS}
    if chassis_ids & air_ids or (line == 'macbook-air' and chip in air_chips):
        if line not in {'macbook-air', 'macbook'}:
            return 'unknown'
        candidates = []
        for chassis, cpu, size, aliases, memory in MACBOOK_AIRS:
            capacities = {'256gb', '512gb', '1tb', '2tb'} if cpu != 'm5' else {'512gb', '1tb', '2tb', '4tb'}
            if (chassis_ids <= {chassis} and (chip == cpu or (not chip and chassis_ids == {chassis}))
                    and sizes <= set(aliases.split()) and ram in memory.split()
                    and storage in capacities):
                valid_gpu = {7, 8} if cpu == 'm1' else {8, 10}
                if cpu == 'm5' and size == '153in':
                    valid_gpu = {10}
                if gpu <= valid_gpu:
                    if cpu == 'm5' and len(valid_gpu) > 1 and not gpu:
                        continue
                    variant = f'-{next(iter(gpu))}gpu' if gpu else ''
                    if cpu == 'm5' and len(valid_gpu) == 1:
                        variant = ''
                    candidates.append(f'apple-macbook-air-{chassis}-{size}-{cpu}{variant}-{ram}-{storage}')
        return candidates[0] if len(candidates) == 1 else 'unknown'

    m5_ids = {row[0] for row in MACBOOK_M5_PRO_CONFIGS}
    parts = [chassis for stems, chassis in MACBOOK_M5_PRO_PARTS.items()
             if matches(rf'\b({"|".join(stems.split())})(?:[a-z]{{1,3}}(?:/a)?)?\b', model_text)]
    if chassis_ids & m5_ids or parts or chip in {'m5', 'm5pro', 'm5max'}:
        models = {_mac_line(text) for text in evidence(attrs, '', 'Product Line', 'Series', 'Model', 'Type')}
        if line not in {'macbook-pro', 'macbook'} or models - {'', 'macbook-pro', 'macbook'}:
            return 'unknown'
        return _macbook_m5_pro_key(attrs, title, chip, ram, storage, chassis_ids, sizes, gpu, parts)

    pro_ids = {chassis for codes, *_ in MACBOOK_PROS for chassis in codes.split()}
    pro_chips = {cpu for _, cpu, *_ in MACBOOK_PROS}
    if chassis_ids & pro_ids or (line == 'macbook-pro' and chip in pro_chips):
        if line not in {'macbook-pro', 'macbook'}:
            return 'unknown'
        candidates = []
        for codes, cpu, diagonal, memory, capacities in MACBOOK_PROS:
            if (chip and chip != cpu) or ram not in memory.split() or storage not in capacities.split():
                continue
            diagonals = [diagonal] if diagonal else ['142in', '162in']
            for chassis, size in zip(codes.split(), diagonals):
                rounded = {'133in': '13in', '142in': '14in', '162in': '16in'}[size]
                valid_gpu = MACBOOK_PRO_GPU_CORES[cpu]
                if chassis_ids <= {chassis} and sizes <= {size, rounded} and gpu <= valid_gpu:
                    variant = f'-{next(iter(gpu))}gpu' if gpu and len(valid_gpu) > 1 else ''
                    candidates.append(f'apple-macbook-pro-{chassis}-{size}-{cpu}{variant}-{ram}-{storage}')
        if chip or chassis_ids:
            return candidates[0] if len(candidates) == 1 else 'unknown'
        return 'unknown'

    if not chip or not (chassis_ids or sizes):
        return 'unknown'
    # Accept rounded marketing sizes alongside the matching measured diagonal.
    for short, precise in (('13in', '133in'), ('14in', '142in'), ('15in', '154in'), ('16in', '162in')):
        if {short, precise} <= sizes:
            sizes.remove(short)
    if len(sizes) > 1:
        return 'unknown'
    return '-'.join(part for part in (
        'apple', line, next(iter(chassis_ids), ''), next(iter(sizes), ''), chip, ram, storage,
    ) if part)


def _mac_memory(attrs, title, *, is_macbook):
    structured = matches(r'\b(\d{1,4})\s*gb\b', evidence(attrs, '', 'RAM Size', 'Memory', 'RAM'))
    # A disk-sized value in a laptop RAM item specific is contradictory evidence.
    if is_macbook and any(int(amount) > 128 for amount in structured):
        return ''
    explicit = structured | matches(
        r'\b(\d{1,3})\s*gb\s*(?:ram(?=\b|\d)|unified\s+memory\b)', evidence({}, title)
    )
    # Desktop configurations may legitimately have 256 GB and more.
    plausible = {f'{amount}gb' for amount in explicit if int(amount) in {
        4, 6, 8, 12, 16, 18, 24, 32, 36, 48, 64, 96, 128, 192, 256, 512, 768,
    }}
    if is_macbook:
        plausible = {value for value in plausible if int(value[:-2]) <= 128}
    if plausible:
        return next(iter(plausible)) if len(plausible) == 1 else ''
    ram = _extract_ram(attrs, title)
    if not ram and re.search(r'\b\d{3,4}\s*gb\b|\b\d{1,2}\s*tb\b', title):
        smaller = matches(r'\b(4|8|16|18|24|32|36|48|64|96|128)\s*gb\b', [title])
        if len(smaller) == 1:
            return f'{smaller.pop()}gb'
    return ram


def _mac_storage(attrs, title):
    explicit = {
        _capacity_token(*match.groups())
        for text in evidence(attrs, '', 'Storage Capacity', 'SSD Capacity', 'Hard Drive Capacity', 'Capacity')
        for match in re.finditer(r'\b(\d{1,4})\s*(gb|tb)\b', text)
    }
    for text in evidence({}, title):
        labelled = _capacity_from_labelled_text(
            text, ('ssd|storage|hard\\s*drive|hdd|flash',), ('ssd|storage|hard\\s*drive|hdd|flash',),
        )
        if labelled:
            explicit.add(labelled)
        else:
            # Avoid using GPU VRAM as storage in comma/slash-delimited specs.
            sizes = re.findall(r'\b(\d{1,4})\s*(gb|tb)\b(?!\s*(?:ram|unified memory))', text)
            disks = {_capacity_token(amount, unit) for amount, unit in sizes if unit == 'tb' or int(amount) >= 240}
            if len(disks) > 1:
                return ''
            explicit |= disks
    if explicit:
        return next(iter(explicit)) if len(explicit) == 1 else ''
    return _extract_storage(attrs, title)


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
                r"\b(apple|iphone|ipad|macbook|imac|mac[ -]*(?:mini|studio|pro)|airpods?|air[ -]?tags?|ipod|homepod)\b",
                title_blob,
            )
        )

    if not _brand_is_apple(): return None

    apple_blob = _apple_blob(attrs, title)
    if _contains_junk_apple_listing(apple_blob):
        return "unknown"

    title_low = evidence({}, title)
    title_text = title_low[0] if title_low else ''
    family_fields = evidence(attrs, '', 'Product Line', 'Series', 'Model', 'Type')

    def family(text):
        match = re.search(
            r'\b(airpods?|iphone|ipad|(?:apple\s+)?watch|air[ -]?tags?|apple[ -]+tv|homepod|ipod|'
            r'macbook(?:[ -]+(?:air|pro|neo))?|imac(?:[ -]+pro)?|mac[ -]*(?:mini|studio|pro))\b',
            text,
        )
        if match:
            name = match[1]
            if name.startswith('airpod'):
                return 'airpods'
            if name.replace(' ', '').replace('-', '').startswith('airtag'):
                return 'airtag'
            if name.endswith('watch'):
                return 'watch'
            return _mac_line(name) or name.replace(' ', '-')
        return ''

    line = family(title_text)
    if not line:
        families = {family(text) for text in family_fields} - {''}
        if len(families) != 1:
            return 'unknown'
        line = families.pop()

    if line == 'airpods':
        return airpods_key(attrs, title)
    if line == 'iphone':
        return iphone_key(attrs, title)
    if line == 'ipad':
        return ipad_key(attrs, title)
    if line == 'watch':
        return watch_key(attrs, title)
    if line == 'airtag':
        return airtag_key(attrs, title)
    if line.startswith(('mac', 'imac')):
        reported_lines = {_mac_line(text) for text in family_fields} - {'', 'macbook'}
        # A few legacy item specifics use an incorrect series label; clear
        # laptop title evidence is needed before using those listings as Macs.
        if reported_lines - {line} and line != 'macbook-pro':
            return 'unknown'
        chips = {
            chip for value in evidence(attrs, title_text, 'Chipset Model', 'Processor', 'CPU', 'Processor Model')
            if (chip := _chip_from_text(value))
        }
        if chips & {'core-i3', 'core-i5', 'core-i7', 'core-i9', 'core-m3'}:
            chips.discard('intel')
        if len(chips) > 1:
            return 'unknown'
        chip = next(iter(chips), '') or _extract_chip(attrs, title_text)
        ram = _mac_memory(attrs, title_text, is_macbook=line.startswith('macbook'))
        storage = _mac_storage(attrs, title_text)
        if line.startswith('macbook'):
            return _macbook_key(attrs, title, line, chip, ram, storage)
        return desktop_key(attrs, title, line, chip, ram, storage)
    if line == 'apple-tv':
        model = 'tv-4k' if '4k' in apple_blob else 'tv-hd' if 'hd' in apple_blob else ''
        return '-'.join(filter(None, ('apple', model, _device_storage(attrs, title)))) if model else 'unknown'
    if line == 'homepod':
        family_blob = ' '.join(family_fields + [title_text])
        model = 'homepod-mini' if 'mini' in family_blob else 'homepod'
        generation = '2nd-gen' if re.search(r'\b(2nd|second|2023)\b', apple_blob) else ''
        return '-'.join(filter(None, ('apple', model, generation)))
    if line == 'ipod':
        model = next((kind for kind in ('touch', 'nano', 'classic') if kind in apple_blob), '')
        storage = _device_storage(attrs, title)
        return f'apple-ipod-{model}-{storage}' if model and storage else 'unknown'
    return 'unknown'
