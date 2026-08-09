from __future__ import annotations

import re
from typing import Any, Mapping, Optional

UNKNOWN_KEY = "unknown"


def _clean(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, (list, tuple, set)):
        return " ".join(str(v) for v in value if v is not None).strip()
    return str(value).strip()


def _text(attrs: Mapping[str, Any], title: str) -> str:
    bits = [
        title,
        _clean(attrs.get("Brand")),
        _clean(attrs.get("Model")),
        _clean(attrs.get("MPN")),
        _clean(attrs.get("Compatible Brand")),
        _clean(attrs.get("Compatible Model")),
        _clean(attrs.get("Socket Type")),
        _clean(attrs.get("Memory Type")),
        _clean(attrs.get("Form Factor")),
    ]
    txt = " ".join(bit for bit in bits if bit).lower()
    txt = txt.replace("ti-super", "ti super")
    txt = txt.replace("ti/super", "ti super")
    txt = txt.replace("super/ti", "super ti")
    txt = txt.replace("wi-fi", "wifi")
    return " ".join(txt.split())


def _slug(value: str) -> str:
    return "-".join(re.findall(r"[a-z0-9]+", value.lower()))


def _has_any(text: str, terms: tuple[str, ...]) -> bool:
    return any(term in text for term in terms)


BOX_ONLY_TERMS = (
    "box only",
    "empty box",
    "retail box only",
    "packaging only",
    "no card included",
    "no cpu included",
    "no motherboard included",
    "processor not included",
)

BUNDLE_TERMS = (
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

GPU_ACCESSORY_TERMS = (
    "cooler only",
    "heatsink only",
    "heat sink only",
    "fan only",
    "fans only",
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
    "sxm",
    "repair service",
)

CPU_ACCESSORY_TERMS = (
    "cooler only",
    "stock cooler only",
    "heatsink only",
    "heat sink only",
    "fan only",
    "keyring",
    "delidded",
    "engineering sample",
    "qualification sample",
)

MOTHERBOARD_ACCESSORY_TERMS = (
    "i/o shield only",
    "io shield only",
    "backplate only",
    "wifi antenna only",
    "antenna only",
    "bios chip",
    "socket replacement",
    "repair service",
)

LENS_ACCESSORY_TERMS = (
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
    "box only",
    "empty box",
    "tripod collar",
    "collar only",
    "repair service",
    "fungus",
    "haze",
    "scratched glass",
)

INDUSTRIAL_ACCESSORY_TERMS = (
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


def _extract_vram_gb(text: str) -> Optional[int]:
    matches = re.findall(r"\b(4|6|8|10|11|12|16|20|24|32|48)\s*gb\b", text)
    if not matches:
        return None
    return max(int(m) for m in matches)


NVIDIA_DEFAULT_VRAM = {
    "rtx-2060": 6,
    "rtx-2060-super": 8,
    "rtx-2070": 8,
    "rtx-2070-super": 8,
    "rtx-2080": 8,
    "rtx-2080-super": 8,
    "rtx-2080-ti": 11,
    "rtx-3050": 8,
    "rtx-3060": None,
    "rtx-3060-ti": 8,
    "rtx-3070": 8,
    "rtx-3070-ti": 8,
    "rtx-3080": None,
    "rtx-3080-ti": 12,
    "rtx-3090": 24,
    "rtx-3090-ti": 24,
    "rtx-4060": 8,
    "rtx-4060-ti": None,
    "rtx-4070": 12,
    "rtx-4070-super": 12,
    "rtx-4070-ti": 12,
    "rtx-4070-ti-super": 16,
    "rtx-4080": 16,
    "rtx-4080-super": 16,
    "rtx-4090": 24,
    "rtx-5060": 8,
    "rtx-5060-ti": None,
    "rtx-5070": 12,
    "rtx-5070-ti": 16,
    "rtx-5080": 16,
    "rtx-5090": 32,
    "gtx-1660": 6,
    "gtx-1660-super": 6,
    "gtx-1660-ti": 6,
}

AMD_DEFAULT_VRAM = {
    "rx-5600-xt": 6,
    "rx-5700": 8,
    "rx-5700-xt": 8,
    "rx-6600": 8,
    "rx-6600-xt": 8,
    "rx-6650-xt": 8,
    "rx-6700": 10,
    "rx-6700-xt": 12,
    "rx-6750-xt": 12,
    "rx-6800": 16,
    "rx-6800-xt": 16,
    "rx-6900-xt": 16,
    "rx-6950-xt": 16,
    "rx-7600": 8,
    "rx-7600-xt": 16,
    "rx-7700-xt": 12,
    "rx-7800-xt": 16,
    "rx-7900-gre": 16,
    "rx-7900-xt": 20,
    "rx-7900-xtx": 24,
    "rx-9060-xt": 16,
    "rx-9070": 16,
    "rx-9070-xt": 16,
}

INTEL_DEFAULT_VRAM = {
    "arc-a380": 6,
    "arc-a580": 8,
    "arc-a750": 8,
    "arc-a770": None,
    "arc-b580": 12,
    "arc-b570": 10,
}

NVIDIA_QUADRO_DEFAULT_VRAM = {
    "4000": 8,
    "5000": 16,
    "6000": 24,
    "8000": 48,
}

NVIDIA_RTX_A_DEFAULT_VRAM = {
    "a4000": 16,
    "a4500": 20,
    "a5000": 24,
    "a6000": 48,
}

NVIDIA_RTX_ADA_DEFAULT_VRAM = {
    "4000": 20,
    "4500": 24,
    "5000": 32,
    "6000": 48,
}

AMD_RADEON_PRO_DEFAULT_VRAM = {
    "w5700": 8,
    "w6800": 32,
    "w7500": 8,
    "w7600": 8,
    "w7800": 32,
    "w7900": 48,
}


def graphics_card_model_key(attrs: Mapping[str, Any], title: str = "") -> Optional[str]:
    text = _text(attrs, title)
    if not text:
        return UNKNOWN_KEY

    if _has_any(text, BOX_ONLY_TERMS) or _has_any(text, GPU_ACCESSORY_TERMS):
        return UNKNOWN_KEY
    if _has_any(text, BUNDLE_TERMS):
        return UNKNOWN_KEY
    if re.search(r"\b(laptop|notebook|mobile|mxm|sxm)\b", text):
        return UNKNOWN_KEY

    quadro = re.search(r"\bquadro\s+rtx\s*(4000|5000|6000|8000)\b", text)
    if quadro:
        number = quadro.group(1)
        vram = _extract_vram_gb(text) or NVIDIA_QUADRO_DEFAULT_VRAM.get(number)
        if not vram:
            return UNKNOWN_KEY
        return f"nvidia-quadro-rtx-{number}-{vram}gb"

    rtx_a = re.search(r"\brtx\s*-?\s*(a2000|a4000|a4500|a5000|a6000)\b", text)
    if rtx_a:
        model = rtx_a.group(1)
        vram = _extract_vram_gb(text) or NVIDIA_RTX_A_DEFAULT_VRAM.get(model)
        if not vram:
            return UNKNOWN_KEY
        return f"nvidia-rtx-{model}-{vram}gb"

    rtx_ada = re.search(r"\brtx\s*(4000|4500|5000|6000)\s+ada\b", text)
    if rtx_ada:
        number = rtx_ada.group(1)
        vram = _extract_vram_gb(text) or NVIDIA_RTX_ADA_DEFAULT_VRAM.get(number)
        if not vram:
            return UNKNOWN_KEY
        return f"nvidia-rtx-{number}-ada-{vram}gb"

    radeon_pro = re.search(r"\bradeon\s+pro\s+(w5700|w6800|w7500|w7600|w7800|w7900)\b", text)
    if radeon_pro:
        model = radeon_pro.group(1)
        vram = _extract_vram_gb(text) or AMD_RADEON_PRO_DEFAULT_VRAM.get(model)
        if not vram:
            return UNKNOWN_KEY
        return f"amd-radeon-pro-{model}-{vram}gb"

    nvidia = re.search(
        r"\b(rtx|gtx)\s*([1-5]\d{3})\s*(ti)?\s*(super)?\b",
        text,
    )
    if nvidia:
        series, number, ti, super_ = nvidia.groups()
        if series == "gtx" and not number.startswith(("10", "16")):
            return UNKNOWN_KEY
        parts = [series, number]
        if ti:
            parts.append("ti")
        if super_:
            parts.append("super")
        model = "-".join(parts)
        vram = _extract_vram_gb(text) or NVIDIA_DEFAULT_VRAM.get(model)
        if not vram:
            return UNKNOWN_KEY
        return f"nvidia-{model}-{vram}gb"

    amd = re.search(r"\brx\s*([5-9]\d{3})\s*(gre|xtx|xt)?\b", text)
    if amd:
        number, suffix = amd.groups()
        model = f"rx-{number}" + (f"-{suffix}" if suffix else "")
        vram = _extract_vram_gb(text) or AMD_DEFAULT_VRAM.get(model)
        if not vram:
            return UNKNOWN_KEY
        return f"amd-radeon-{model}-{vram}gb"

    intel = re.search(r"\barc\s*([ab])\s*-?\s*(\d{3})\b", text)
    if intel:
        letter, number = intel.groups()
        model = f"arc-{letter}{number}"
        vram = _extract_vram_gb(text) or INTEL_DEFAULT_VRAM.get(model)
        if not vram:
            return UNKNOWN_KEY
        return f"intel-{model}-{vram}gb"

    return UNKNOWN_KEY


def _normalise_aperture(value: str) -> str:
    raw = value.strip().lower().replace("/", "").replace(".", "")
    if raw.endswith("0") and len(raw) > 1:
        raw = raw[:-1]
    return f"f{raw}"


def _extract_lens_focal(text: str) -> Optional[str]:
    match = re.search(r"\b(\d{2,3})\s*-\s*(\d{2,3})\s*mm\b", text)
    if match:
        return f"{match.group(1)}-{match.group(2)}mm"
    match = re.search(r"\b(\d{2,3})\s*-\s*(\d{2,3})\b", text)
    if match:
        return f"{match.group(1)}-{match.group(2)}mm"
    match = re.search(r"\b(\d{2,3})\s*mm\b", text)
    if match:
        return f"{match.group(1)}mm"
    return None


def _extract_lens_aperture(text: str) -> Optional[str]:
    match = re.search(r"\bf\s*/?\s*(1\.2|1\.4|2\.8|2(?:\.0)?|4(?:\.0)?)\b", text)
    if match:
        return _normalise_aperture(match.group(1))
    match = re.search(r"\b1:(1\.2|1\.4|2\.8|2(?:\.0)?|4(?:\.0)?)\b", text)
    if match:
        return _normalise_aperture(match.group(1))
    return None


def camera_lens_model_key(attrs: Mapping[str, Any], title: str = "") -> Optional[str]:
    text = _text(attrs, title)
    if not text:
        return UNKNOWN_KEY

    if _has_any(text, BOX_ONLY_TERMS) or _has_any(text, LENS_ACCESSORY_TERMS):
        return UNKNOWN_KEY
    if _has_any(text, BUNDLE_TERMS) or re.search(r"\b(faulty|spares|for parts|for repair)\b", text):
        return UNKNOWN_KEY

    focal = _extract_lens_focal(text)
    if not focal:
        return UNKNOWN_KEY
    aperture = _extract_lens_aperture(text)

    if re.search(r"\bsony\b", text) and not re.search(r"\b(sigma|tamron)\b", text):
        if "fe" not in text or not re.search(r"\b(gm|g master|g lens)\b", text):
            return UNKNOWN_KEY
        tier = "gm" if re.search(r"\b(gm|g master)\b", text) else "g"
        parts = ["sony", "fe", focal]
        if aperture:
            parts.append(aperture)
        parts.append(tier)
        if re.search(r"\b(ii|mk2|mark 2)\b", text):
            parts.append("ii")
        return _slug("-".join(parts))

    if re.search(r"\bcanon\b", text):
        if "rf" not in text or not re.search(r"\bl\b", text):
            return UNKNOWN_KEY
        parts = ["canon", "rf", focal]
        if aperture:
            parts.append(aperture)
        parts.append("l")
        return _slug("-".join(parts))

    if re.search(r"\bnikon\b", text):
        if " z " not in f" {text} " or not re.search(r"\bs\b", text):
            return UNKNOWN_KEY
        parts = ["nikon", "z", focal]
        if aperture:
            parts.append(aperture)
        parts.append("s")
        return _slug("-".join(parts))

    if re.search(r"\b(fujifilm|fuji)\b", text):
        family = "gf" if re.search(r"\bgf\b", text) else "xf" if re.search(r"\bxf\b", text) else ""
        if not family:
            return UNKNOWN_KEY
        parts = ["fujifilm", family, focal]
        if aperture:
            parts.append(aperture)
        return _slug("-".join(parts))

    if re.search(r"\bleica\b", text):
        family_match = re.search(r"\b(summicron|summilux|noctilux|elmarit)\b", text)
        mount = "m" if re.search(r"\bm\b", text) else "sl" if re.search(r"\bsl\b", text) else ""
        if not family_match or not mount:
            return UNKNOWN_KEY
        return _slug(f"leica-{family_match.group(1)}-{mount}-{focal}")

    if re.search(r"\bhasselblad\b", text):
        if not re.search(r"\bxcd\b", text):
            return UNKNOWN_KEY
        parts = ["hasselblad", "xcd", focal]
        if aperture:
            parts.append(aperture)
        return _slug("-".join(parts))

    if re.search(r"\bsigma\b", text):
        if "art" not in text:
            return UNKNOWN_KEY
        parts = ["sigma", focal]
        if aperture:
            parts.append(aperture)
        if "dg dn" in text:
            parts.extend(["dg", "dn"])
        parts.append("art")
        return _slug("-".join(parts))

    if re.search(r"\btamron\b", text):
        if not aperture or not re.search(r"\b(sony|e mount|z mount|rf)\b", text):
            return UNKNOWN_KEY
        parts = ["tamron", focal, aperture]
        if "sony" in text or "e mount" in text:
            parts.append("sony-e")
        elif "z mount" in text:
            parts.append("nikon-z")
        elif "rf" in text:
            parts.append("canon-rf")
        return _slug("-".join(parts))

    return UNKNOWN_KEY


def _industrial_part_slug(value: str) -> str:
    return "-".join(re.findall(r"[a-z0-9]+", value.lower()))


def industrial_automation_model_key(attrs: Mapping[str, Any], title: str = "") -> Optional[str]:
    text = _text(attrs, title)
    if not text:
        return UNKNOWN_KEY

    if _has_any(text, BOX_ONLY_TERMS) or _has_any(text, INDUSTRIAL_ACCESSORY_TERMS):
        return UNKNOWN_KEY
    if _has_any(text, BUNDLE_TERMS) or re.search(r"\b(faulty|spares|for parts|for repair|untested)\b", text):
        return UNKNOWN_KEY

    industrial_patterns = (
        ("siemens", r"\b(6es7\s*\d{3}[-\s]?[0-9a-z]{4,5}[-\s]?[0-9a-z]{4}|6av\s*\d{4}[-\s]?[0-9a-z]{4,5}[-\s]?[0-9a-z]{4}|cpu\s*15\d{2}[a-z]?)\b"),
        ("allen-bradley", r"\b(1756|1769|1762|5069|2711p|2711r)[-\s]?[0-9a-z]+(?:[-\s][0-9a-z]+){0,3}\b"),
        ("omron", r"\b(cj\d+[a-z]?[-\s]?cpu\d+|cp1[hle][-\s]?[0-9a-z]+|nj\d+[-\s]?[0-9a-z]+)\b"),
        ("mitsubishi", r"\b(fx[35]u[-\s]?[0-9a-z]+|mr[-\s]?j\d[0-9a-z]?[-\s]?[0-9a-z]+)\b"),
        ("schneider", r"\b(bmx[0-9a-z]+|tm\d+[0-9a-z]+|atv\d+[0-9a-z]+)\b"),
        ("beckhoff", r"\b(cx\d{4}(?:-[0-9a-z]{3,5})?|el\d{4})\b"),
        ("pilz", r"\b(pnoz[-\s]?[0-9a-z]+(?:[-\s][0-9a-z]+)*)\b"),
        ("br", r"\b(x20[0-9a-z]+|4pp\d+[0-9a-z]*)\b"),
        ("control-techniques", r"\b(unidrive[-\s]?m\d{3,4}|sp\d{4}|mentor[-\s]?mp\d+)\b"),
    )

    brand_markers = {
        "siemens": ("siemens", "simatic", "s7-1500", "s7 1500"),
        "allen-bradley": ("allen bradley", "allen-bradley", "rockwell", "panelview"),
        "omron": ("omron",),
        "mitsubishi": ("mitsubishi",),
        "schneider": ("schneider",),
        "beckhoff": ("beckhoff",),
        "pilz": ("pilz",),
        "br": ("b&r", "b and r", "br automation"),
        "control-techniques": ("control techniques", "unidrive",),
    }

    for brand, pattern in industrial_patterns:
        if not any(marker in text for marker in brand_markers[brand]):
            continue
        match = re.search(pattern, text)
        if match:
            return f"{brand}-{_industrial_part_slug(match.group(0))}"

    return UNKNOWN_KEY


def cpu_model_key(attrs: Mapping[str, Any], title: str = "") -> Optional[str]:
    text = _text(attrs, title)
    if not text:
        return UNKNOWN_KEY

    if _has_any(text, BOX_ONLY_TERMS) or _has_any(text, CPU_ACCESSORY_TERMS):
        return UNKNOWN_KEY
    if _has_any(text, BUNDLE_TERMS):
        return UNKNOWN_KEY
    if re.search(r"\b(xeon|epyc|opteron|server|laptop|notebook|mobile)\b", text):
        return UNKNOWN_KEY
    if "compatible with" in text or "supports " in text or "support for" in text:
        return UNKNOWN_KEY
    if re.search(r"\b(bent|missing|damaged)\s+(pin|pins|pad|pads)\b", text):
        return UNKNOWN_KEY

    amd = re.search(
        r"\b(?:amd\s+)?ryzen\s*([3579])\s*([0-9]{4,5})\s*(x3d|xt|x|g|ge)?\b",
        text,
    )
    if amd:
        tier, number, suffix = amd.groups()
        if int(number[:1]) < 3:
            return UNKNOWN_KEY
        return f"amd-ryzen-{tier}-{number}{suffix or ''}"

    intel = re.search(
        r"\b(?:intel\s+)?(?:core\s*)?i([3579])[-\s]*([0-9]{4,5})(ks|kf|k|f|t)?\b",
        text,
    )
    if intel:
        tier, number, suffix = intel.groups()
        generation = int(number[:2]) if len(number) >= 5 else int(number[:1])
        if generation < 10:
            return UNKNOWN_KEY
        return f"intel-core-i{tier}-{number}{suffix or ''}"

    return UNKNOWN_KEY


MOTHERBOARD_STOP_TOKENS = {
    "motherboard",
    "mainboard",
    "board",
    "amd",
    "intel",
    "socket",
    "am4",
    "am5",
    "lga",
    "lga1700",
    "lga1200",
    "atx",
    "matx",
    "micro",
    "mini",
    "itx",
    "gaming",
    "pc",
    "new",
    "used",
    "boxed",
    "faulty",
    "damaged",
    "untested",
    "perfect",
    "condition",
    "box",
    "ryzen",
    "ready",
    "core",
    "i3",
    "i5",
    "i7",
    "i9",
    "13th",
    "12th",
    "14th",
    "15th",
    "1x",
    "sata",
    "sata3",
    "pcie",
    "5",
    "0",
    "6",
    "7",
}


def _normalise_motherboard_tokens(text: str) -> list[str]:
    text = text.replace("wi-fi", "wifi")
    text = re.sub(r"\b(?:ddr)\s*([45])\b", r"ddr\1", text)
    text = re.sub(r"\bd([45])\b", r"ddr\1", text)
    text = re.sub(r"\brev(?:ision)?\.?\s*([0-9])\.?([0-9])\b", r"rev \1 \2", text)
    raw_tokens = re.findall(r"[a-z0-9]+", text.lower())
    tokens: list[str] = []
    skip_next = False
    for idx, tok in enumerate(raw_tokens):
        if skip_next:
            skip_next = False
            continue
        if tok == "wi" and idx + 1 < len(raw_tokens) and raw_tokens[idx + 1] == "fi":
            tokens.append("wifi")
            skip_next = True
            continue
        tokens.append(tok)
    return tokens


def motherboard_model_key(attrs: Mapping[str, Any], title: str = "") -> Optional[str]:
    text = _text(attrs, title)
    if not text:
        return UNKNOWN_KEY

    if _has_any(text, BOX_ONLY_TERMS) or _has_any(text, MOTHERBOARD_ACCESSORY_TERMS):
        return UNKNOWN_KEY
    if _has_any(text, BUNDLE_TERMS):
        return UNKNOWN_KEY
    if re.search(r"\b(cpu|processor|ram|memory)\s+(included|bundle|combo)\b", text):
        return UNKNOWN_KEY
    if re.search(r"\b(laptop|server|proprietary|oem)\s+motherboard\b", text):
        return UNKNOWN_KEY

    brand_aliases = {
        "asus": "asus",
        "msi": "msi",
        "gigabyte": "gigabyte",
        "asrock": "asrock",
    }
    brand = ""
    attr_brand = _clean(attrs.get("Brand")).lower()
    for raw, canonical in brand_aliases.items():
        if raw in attr_brand or re.search(rf"\b{raw}\b", text):
            brand = canonical
            break
    if not brand:
        return UNKNOWN_KEY
    if brand != "asus" and re.search(r"\b(rog|strix|tuf)\b", text):
        return UNKNOWN_KEY

    tokens = _normalise_motherboard_tokens(text)
    tokens = [tok for tok in tokens if tok != brand]
    chipset_idx = None
    for idx, tok in enumerate(tokens):
        if re.fullmatch(r"(a|b|x|z|h|q)\d{3,4}[a-z]?", tok):
            chipset_idx = idx
            break
    if chipset_idx is None:
        return UNKNOWN_KEY

    start = max(0, chipset_idx - 3)
    end = min(len(tokens), chipset_idx + 7)
    product_tokens: list[str] = []
    seen_chipset = False
    for tok in tokens[start:end]:
        if tok in MOTHERBOARD_STOP_TOKENS or not tok or re.fullmatch(r"\d{3,4}", tok):
            continue
        if tok == "ddr5":
            continue
        if re.fullmatch(r"(a|b|x|z|h|q)\d{3,4}[a-z]?", tok):
            if seen_chipset:
                continue
            seen_chipset = True
        if tok not in product_tokens:
            product_tokens.append(tok)

    if "wifi" in tokens and "wifi" not in product_tokens:
        product_tokens.append("wifi")
    if "ddr4" in tokens and "ddr4" not in product_tokens:
        product_tokens.append("ddr4")
    if "v2" in tokens and "v2" not in product_tokens:
        product_tokens.append("v2")
    if "rev" in tokens:
        rev_idx = tokens.index("rev")
        if rev_idx + 2 < len(tokens):
            rev = f"rev-{tokens[rev_idx + 1]}-{tokens[rev_idx + 2]}"
            if rev not in product_tokens:
                product_tokens.append(rev)

    if not product_tokens or not any(tok.startswith(tokens[chipset_idx]) for tok in product_tokens):
        return UNKNOWN_KEY
    if len([tok for tok in product_tokens if tok != tokens[chipset_idx]]) < 1:
        return UNKNOWN_KEY

    return f"{brand}-{_slug('-'.join(product_tokens))}"
