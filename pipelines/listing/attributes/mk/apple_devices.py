from __future__ import annotations

import re

from .apple_catalog import (
    AIRPODS_CASES, AIRPODS_MODELS, IPADS, IPHONE_MODEL_NUMBERS, IPHONE_STORAGE,
    WATCH_CELLULAR_MATERIALS, WATCH_MODELS,
)


def evidence(attrs, title, *fields):
    """Keep each item specific separate so adjoining fields can't create a match."""
    values = [title]
    for field in fields:
        value = attrs.get(field)
        values.extend(value if isinstance(value, (list, tuple)) else [value])
    return [
        str(value).lower().translate(str.maketrans('‐‑–—“”″', '----"""')).replace("''", '"')
        for value in values if value
    ]


def matches(pattern, texts):
    return {match.group(1) for text in texts for match in re.finditer(pattern, text)}


def model_numbers(texts):
    return matches(r'\b(a\d{4})\b', texts)


def ordinal(number):
    suffix = {1: 'st', 2: 'nd', 3: 'rd'}.get(number % 10, 'th') if number % 100 not in {11, 12, 13} else 'th'
    return f'{number}{suffix}-gen'


def generations(texts):
    numbers = matches(r'\b(\d{1,2})(?:st|nd|rd|th)?[ -]+gen(?:eration)?\b', texts)
    numbers |= matches(r'\bgen(?:eration)?[ -]*(\d{1,2})\b(?!\s*(?:gb|tb|mm|inch|"))', texts)
    words = matches(r'\b(first|second|third|fourth|fifth|sixth|seventh|eighth)\s+gen', texts)
    word_numbers = dict(zip(('first', 'second', 'third', 'fourth', 'fifth', 'sixth', 'seventh', 'eighth'), range(1, 9)))
    return {int(number) for number in numbers} | {word_numbers[word] for word in words}


def screen_sizes(texts):
    sizes = matches(r'(?<![\d.])(\d{1,2}(?:\.\d{1,2})?)\s*[- ]?\s*(?:in(?:ch(?:es)?)?\b|")', texts)
    return {f"{size.removesuffix('.0').replace('.', '')}in" for size in sizes}


def device_capacity(attrs, title):
    """Retain contradictory storage values and discard clearly labelled RAM."""
    raw = evidence(attrs, '', 'Storage Capacity', 'SSD Capacity', 'Capacity')
    descriptions = evidence(attrs, title, 'Model')
    capacities = set()
    for text in raw + descriptions:
        alternatives = re.findall(r'\b(\d{2,4})\s*[/|]\s*\d{2,4}\s*(gb|tb)\b', text)
        capacities.update(f'{int(amount)}{unit}' for amount, unit in alternatives)
        for match in re.finditer(r'(?<![\d.])(\d{1,4})\s*(gb|tb)\b', text):
            amount, unit = match.groups()
            if text in descriptions and (
                re.match(r'\s*(?:ram|unified memory)\b', text[match.end():])
                or re.search(r'\b(?:ram|unified memory)\s*[:=]?\s*$', text[:match.start()])
            ):
                continue
            # Phone/tablet listings commonly place unlabelled RAM next to real storage.
            if text in descriptions and unit == 'gb' and int(amount) in {3, 4, 6, 8, 12}:
                continue
            capacities.add(f'{int(amount)}{unit}')
    return next(iter(capacities)) if len(capacities) == 1 else ''


def iphone_key(attrs, title):
    texts = evidence(attrs, title, 'Model', 'Product Line', 'Series')
    variants = set()
    for text in texts:
        for match in re.finditer(
            r'\biphone[ -]*(\d{1,2}[es]?|xs(?:[ -]+max)?|xr|x|se|duo)\b'
            r'(?:[ -]+(pro[ -]*max|pro|max|plus|mini|air)\b)?', text
        ):
            base, tier = match.groups()
            base = base.replace(' ', '-')
            if base == 'se':
                numbers = generations([text])
                numbers |= {number for year, number in [('2016', 1), ('2020', 2), ('2022', 3)] if year in text}
                direct = re.match(r'[ -]*(1|2|3)\b(?!\s*gb)', text[match.end():])
                if direct:
                    numbers.add(int(direct[1]))
                variants.update(f'se-{number}' for number in numbers)
            else:
                tier = 'pro-max' if tier and tier.startswith('pro') and tier.endswith('max') else tier
                variants.add(f'{base}-{tier}' if tier else base)
        if re.search(r'\biphone[ -]+air\b', text):
            variants.add('17-air')

    ids = model_numbers(evidence(attrs, title, 'Model', 'Model Number', 'MPN'))
    variants.update(
        model for model, numbers in IPHONE_MODEL_NUMBERS.items()
        if ids & set(numbers.split())
    )
    storage = device_capacity(attrs, title)
    if len(variants) != 1:
        return 'unknown'
    model = next(iter(variants))
    model_numbers_for_variant = IPHONE_MODEL_NUMBERS.get(model)
    if model_numbers_for_variant and not ids <= set(model_numbers_for_variant.split()):
        return 'unknown'
    if model == 'duo':
        chips = matches(r'\b(a\d{2}(?:[ -]*pro)?)\b', evidence(attrs, title, 'Chipset Model', 'Processor'))
        sizes = screen_sizes(evidence(attrs, title, 'Screen Size', 'Display'))
        if {chip.replace(' ', '').replace('-', '') for chip in chips} - {'a20pro'} or sizes - {
            '54in', '536in', '76in', '758in',
        }:
            return 'unknown'
    return f'apple-iphone-{model}-{storage}' if storage in IPHONE_STORAGE.get(model, '').split() else 'unknown'


def connectivity(texts, *, watch=False):
    kinds = set()
    for text in texts:
        cellular = re.search(r'\b(?:cellular|cel|lte|5g|4g|3g|mobile data)\b', text)
        if cellular:
            kinds.add('cellular')
        if watch and re.search(r'\bgps(?:[ -]+only)\b', text):
            kinds.add('gps')
        elif watch and not cellular and re.search(r'\bgps\b', text):
            kinds.add('gps-unspecified')
        if not watch and re.search(r'\bwi[ -]?fi\s+only\b', text):
            kinds.add('wifi')
        elif not watch and not cellular and re.search(r'\bwi[ -]?fi\b', text):
            kinds.add('wifi')
    return kinds


def ipad_key(attrs, title):
    texts = evidence(attrs, title, 'Model', 'Product Line', 'Series', 'MPN')
    suffixes = matches(r'\bipad[ -]+(pro|air|mini)\b', texts)
    line = f'ipad-{next(iter(suffixes))}' if len(suffixes) == 1 else 'ipad'
    if len(suffixes) > 1:
        return 'unknown'
    if suffixes and re.search(r'\bipad\s*[( -]*(?:a16|(?:10|11)th\s+gen)', title.lower()):
        return 'unknown'
    gen = generations(texts) | {
        int(number) for number in matches(r'\bipad(?:[ -]+(?:air|mini))?\s+(\d{1,2})\b(?!\s*(?:[."-]|inch|in\b|gb|tb))', texts)
    }
    chips = matches(r'\b(m[1-5]|a(?:17\s*pro|1[0-6][xz]?|[4-9]x?))\b', evidence(attrs, title, 'Model', 'Processor', 'Chipset Model', 'CPU'))
    chips = {chip.replace(' ', '') for chip in chips}
    ids = model_numbers(texts)
    sizes = screen_sizes(evidence(attrs, title, 'Screen Size', 'Display'))
    storage = device_capacity(attrs, title)
    network = connectivity(evidence({}, title))
    # eBay often lists Wi-Fi as a feature alongside Wi-Fi + Cellular.
    structured = [' '.join(evidence(attrs, '', field)) for field in ('Internet Connectivity', 'Connectivity')]
    structured_network = connectivity(structured)
    if len(structured_network) > 1 and 'cellular' in structured_network:
        structured_network = {'cellular'}
    network |= structured_network
    if not storage or not (gen or chips or ids):
        return 'unknown'

    candidates = []
    for family, number, chip, size, capacities, wifi_ids, cellular_ids in IPADS:
        aliases = {size}
        if size == '11in' and (family == 'ipad' or family == 'ipad-air'):
            aliases.add('1086in')
        if size == '109in':
            aliases.add('1086in')
        if ((line != family and not (line == 'ipad' and ids and not gen and not chips))
                or not gen <= {number} or not chips <= {chip}
                or not sizes <= aliases or storage not in capacities.split()):
            continue
        for kind, valid_ids in (('wifi', wifi_ids), ('cellular', cellular_ids)):
            if not (ids or network) or not ids <= set(valid_ids.split()) or not network <= {kind}:
                continue
            identity = chip if chip.startswith('m') else ordinal(number)
            candidates.append(f'apple-{family}-{size}-{identity}-{storage}-{kind}')
    return candidates[0] if len(candidates) == 1 else 'unknown'


def watch_key(attrs, title):
    texts = evidence(attrs, title, 'Model', 'Product Line', 'Series', 'MPN')
    ids = model_numbers(texts)
    models = set()
    for text in texts:
        for tier in ('ultra', 'se'):
            match = re.search(rf'\b{tier}(?:[ -]*(?:series[ -]*)?([1-9])(?!\d))?\b', text)
            if not match:
                continue
            numbers = generations([text])
            if match[1]:
                numbers.add(int(match[1]))
            if tier == 'ultra' and re.search(r'\b2022\b', text):
                numbers.add(1)
            if tier == 'se':
                numbers |= {number for year, number in [('2020', 1), ('2022', 2), ('2025', 3)] if year in text}
            models.update(tier if number == 1 else f'{tier}-{number}' for number in numbers)
            if not numbers:
                models.add(f'{tier}-unspecified')
        models.update(f'series-{number}' for number in matches(r'\bseries[ -]*(\d{1,2})\b', [text]) if 'ultra' not in text)

    if not ids and (not models or all(model.endswith('-unspecified') for model in models)):
        return 'unknown'
    size_texts = evidence(attrs, title, 'Case Size')
    sizes = {int(size) for size in matches(r'\b(\d{2})\s*mm\b', size_texts)}
    sizes |= {int(value) for value in evidence(attrs, '', 'Case Size') if value.isdigit()}
    networks = connectivity(evidence(attrs, title, 'Model', 'Connectivity', 'Network'), watch=True)
    if 'gps-unspecified' in networks:
        networks.remove('gps-unspecified')
        if not networks and not any(model.startswith('ultra') for model in models):
            networks.add('gps')
    materials = matches(
        r'\b(alumini?um|stainless[ -]+steel|titanium|ceramic)\b(?!\s*(?:band|strap|loop|bracelet|back))',
        evidence(attrs, title, 'Case Material'),
    )
    materials = {'aluminium' if value.startswith('alu') else 'steel' if value.startswith('stainless') else value
                 for value in materials}
    candidates = []
    for model, configurations in WATCH_MODELS.items():
        permitted_names = {model}
        if model.startswith('ultra'):
            permitted_names.add('ultra-unspecified')
        if model.startswith('se'):
            permitted_names.add('se-unspecified')
        if not models <= permitted_names:
            continue
        for size, (gps, cellular) in configurations.items():
            if not sizes <= {size}:
                continue
            for kind, valid_ids in (('gps', gps), ('cellular', cellular)):
                if not valid_ids or not ids <= set(valid_ids.split()) or not networks <= {kind}:
                    continue
                material = ''
                allowed = {'titanium'} if model.startswith('ultra') else {'aluminium'}
                if model.startswith('series-'):
                    number = int(model.split('-')[-1])
                    if kind == 'cellular' or number == 2:
                        allowed = set(WATCH_CELLULAR_MATERIALS.get(number, 'aluminium').split())
                if not materials <= allowed or (not materials and len(allowed) > 1) or len(materials) > 1:
                    continue
                if materials and len(allowed) > 1:
                    material = '-' + next(iter(materials))
                candidates.append(f'apple-watch-{model}-{size}mm-{kind}{material}')
    return candidates[0] if len(candidates) == 1 else 'unknown'


_AUDIO_COMPONENT = re.compile(
    r'\b(?:replica|clone|counterfeit|fake|single (?:earbud|bud|airpod)|'
    r'(?:left|right)(?:[ -]+(?:side|ear|earbud|bud|airpod))?\s*(?:and|&|\+|/)\s*(?:the\s+)?(?:charging\s+)?case|'
    r'(?:left|right)(?:[ -]+(?:side|ear|earbud|bud|airpod))?\b(?!\s*(?:and|&|\+|/))'
    r'|(?:no|without|missing)\s+(?:charging\s+case|case|buds?|earbuds?)|'
    r'(?:charging\s+case|earbuds?|buds?)\s+only|replacement\s+(?:charging\s+case|earbuds?|case)|'
    r'(?:case|earbud)\s+replacement|(?:no|without)\s+(?:airpods|headphones))\b'
)


def airpods_key(attrs, title):
    texts = evidence(attrs, title, 'Model', 'Product Line', 'Series', 'MPN', 'Type')
    if any(_AUDIO_COMPONENT.search(text) for text in texts):
        return 'unknown'
    ids = model_numbers(texts)
    families = set()
    gen = set()
    for text in texts:
        for match in re.finditer(r'\bairpods?[ -]*(?:(pro|max)\b)?[ (\-]*(\d)(?!\d|\s*(?:gb|pack|pk))', text):
            tier, number = match.groups()
            families.add(tier or 'base')
            gen.add(int(number))
        families.update(matches(r'\bairpods?[ -]+(pro|max)\b', [text]))
        gen |= generations([text])
    if any(re.search(r'\bairpods?\s+pro\s+max\b', text) for text in texts):
        return 'unknown'
    if len(families) > 1 or len(gen) > 1 or not (gen or ids or families):
        return 'unknown'

    has_usb = any(re.search(r'\b(?:usb[ -]?c|type[ -]?c)\b', text) for text in texts)
    has_lightning = any(re.search(r'\blightning\b', text) for text in texts)
    if has_usb and has_lightning:
        return 'unknown'
    ancillary_ids = ids & AIRPODS_CASES
    ids -= ancillary_ids
    if ancillary_ids and not ids and not any(re.search(r'\b(?:with|includes?)\b', text) for text in texts):
        return 'unknown'

    if not families and any(re.search(r'\bairpods?\b', text) for text in texts) and not ids:
        families.add('base')
    anc = any(re.search(r'\b(?:anc|active noise cancel)', text) for text in texts)
    no_anc = any(re.search(r'\b(?:no|without|non)[ -]+(?:anc|active noise cancel)', text) for text in texts)
    candidates = []
    for model, numbers in AIRPODS_MODELS.items():
        family = model.split('-')[0] if model.startswith(('pro-', 'max-')) else 'base'
        number = int(model.split('-')[0 if family == 'base' else 1])
        usb = model.endswith('-usbc') or model.startswith(('4', '5')) or model in {'max-2', 'pro-3'}
        if not families <= {family} or not gen <= {number} or not ids <= set(numbers.split()):
            continue
        if (has_usb and not usb) or (has_lightning and usb):
            continue
        if family == 'base' and number == 4:
            if no_anc and model.endswith('-anc'):
                continue
            if anc and not no_anc and not model.endswith('-anc'):
                continue
        candidates.append(f'apple-airpods-{model}')
    return candidates[0] if len(candidates) == 1 else 'unknown'


def airtag_key(attrs, title):
    texts = evidence(attrs, title, 'Model', 'MPN', 'Type', 'Product Line')
    if any(re.search(r'\b(?:bundle|mixed lot|assorted|replica|clone|holder|key[ -]?ring|'
                     r'keychain|protective case|cover|accessor(?:y|ies)|replacement|empty box)\b', text)
           for text in texts):
        return 'unknown'
    gen = generations(texts)
    ids = model_numbers(texts)
    for number, ident in ((1, 'a2187'), (2, 'a2937')):
        if ident in ids:
            gen.add(number)
    gen |= {int(value) for value in matches(r'\bair[ -]?tags?[ -]+([12])\b(?![ -]*(?:pack|pk|pcs|units))', texts)}
    if any(re.search(r'\b(?:second[ -]+generation ultra wideband|u2 chip)\b', text) for text in texts):
        gen.add(2)

    counts = set()
    # Regional retail part numbers share the same five-character SKU stem.
    # Apple education price lists identify the original and second-generation kits.
    sku_packs = {'mx532': (1, 1), 'mx542': (1, 4), 'mfe94': (2, 1), 'mfea2': (2, 4), 'mfea4': (2, 4)}
    for sku in matches(r'\b(mx532|mx542|mfe94|mfea2|mfea4)(?:[a-z]{1,3}(?:/a)?)?\b', texts):
        number, quantity = sku_packs[sku]
        gen.add(number)
        counts.add(quantity)
    words = dict(zip(('one', 'two', 'three', 'four', 'five', 'six', 'seven', 'eight', 'nine', 'ten'), range(1, 11)))
    quantity_pattern = r'\b(\d{1,2}|one|two|three|four|five|six|seven|eight|nine|ten)'
    for text in texts:
        quantities = matches(quantity_pattern + r'[ -]*(?:pack|pk|pcs|pieces|units|air[ -]?tags?)\b', [text])
        quantities |= matches(r'\b(?:pack of|lot of|x|×)\s*(\d{1,2})\b', [text])
        if re.search(r'\b(?:single|individual)(?:\s+(?:unit|air[ -]?tag|pack|tracker))?\b', text):
            quantities.add('one')
        counts.update(int(value) if value.isdigit() else words[value] for value in quantities)
    quantities = evidence(attrs, '', 'Number in Pack', 'Pack Quantity', 'Unit Quantity')
    counts |= {int(value) for value in quantities if value.isdigit()}

    if len(gen) != 1 or not gen <= {1, 2} or len(counts) != 1 or 0 in counts:
        return 'unknown'
    return f'apple-airtag-{ordinal(next(iter(gen)))}-{next(iter(counts))}pack'


def gpu_cores(attrs, title):
    explicit = matches(r'\b(\d{1,2})[ -]*(?:c(?:ores?)?[ -]*)?gpu\b',
                       evidence(attrs, title, 'GPU', 'Graphics Processing Type'))
    structured = matches(r'^(?:apple\s+)?(\d{1,2})[ -]*cores?$', evidence(attrs, '', 'GPU'))
    return {
        int(value) for value in explicit | structured
    }


def desktop_key(attrs, title, line, chip, ram, storage):
    from .apple_catalog import MAC_DESKTOPS

    model_text = evidence(attrs, title, 'Model', 'Model Identifier', 'MPN')
    sizes = screen_sizes(evidence(attrs, title, 'Screen Size', 'Display'))
    chassis = model_numbers(model_text)
    identifiers = {
        f'{family}{major}-{minor}'
        for text in model_text
        for family, major, minor in re.findall(r'\b(macmini|imac|macpro|mac)\s*(\d+)[,.-](\d+)\b', text)
    }
    gpu = gpu_cores(attrs, title)
    if not (chip and ram and storage):
        return 'unknown'

    if line == 'mac-mini' and chip == 'core-i5' and (identifiers == {'macmini7-1'} or (
        chassis == {'a1347'} and any(re.search(r'\blate[ -]*2014\b', text) for text in model_text)
    )):
        # Late 2014 is the only A1347 generation with this identifier. Exact
        # clock or CPU part number is still required to distinguish its i5 builds.
        clocks = matches(r'\b(1\.4|2\.6|2\.8)\s*ghz\b', evidence(attrs, title, 'Processor', 'Processor Speed', 'CPU'))
        if (len(clocks) == 1 and chassis <= {'a1347'} and identifiers <= {'macmini7-1'}
                and ram in {'4gb', '8gb', '16gb'} and storage in {'256gb', '512gb', '1tb', '2tb'}):
            clock = next(iter(clocks)).replace('.', '')
            return f'apple-mac-mini-macmini7-1-core-i5-{clock}ghz-{ram}-{storage}'
        return 'unknown'

    build = matches(r'\b(tower|rack)(?:\b|mount)', model_text) if line == 'mac-pro' else set()
    ports = matches(r'\b(2|4|two|four)[ -]+(?:thunderbolt[ -]+)?ports\b', model_text) if line == 'imac' else set()
    ports = {'2' if port in {'2', 'two'} else '4' for port in ports}
    candidates = []
    for family, identifier, codes, size, cpu, cores, memory, capacities in MAC_DESKTOPS:
        if (family != line or chip != cpu or not chassis <= set(codes.split())
                or not identifiers <= {identifier, identifier.rsplit('-', 1)[0]}
                or not gpu <= {cores} or ram not in memory.split() or storage not in capacities.split()):
            continue
        if size and not sizes <= {size, '235in'}:
            continue
        if build and not build <= {identifier.rsplit('-', 1)[-1]}:
            continue
        if ports and not ports <= {'2' if identifier in {'imac21-2', 'mac15-4', 'mac16-2'} else '4'}:
            continue
        variant = [identifier, size, cpu, f'{cores}gpu', ram, storage]
        candidates.append('-'.join(['apple', line] + [value for value in variant if value]))
    return candidates[0] if len(candidates) == 1 else 'unknown'
