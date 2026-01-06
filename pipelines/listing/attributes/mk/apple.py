from __future__ import annotations
from typing import Mapping, Any, Optional
import re
from utils.condition import _derive_condition_grade


def _clean(v: Any) -> str:
    if not v: return ""
    return str(v).strip().lower().replace(" ", "-").replace("\"", "").replace("'", "")


def _num(v: Any) -> str:
    """Simple digit extractor (used for things like mm sizes)."""
    if not v:
        return ""
    return "".join(ch for ch in str(v) if ch.isdigit())


def _extract_storage(attrs: Mapping[str, Any], title: str) -> str:
    """Extracts storage (e.g., '256gb', '1tb') using keys from your metadata."""
    val = " ".join([str(attrs.get(k) or "") for k in
                    ["Storage Capacity", "SSD Capacity", "Hard Drive Capacity", "Capacity"]] + [title]).lower()
    m = re.search(r"(\d+)\s*(gb|tb)", val)
    return f"{m.group(1)}{m.group(2)}" if m else ""


def _extract_ram(attrs: Mapping[str, Any], title: str) -> str:
    """Extracts RAM (e.g., '16gb', '24gb') using your 'RAM Size' metadata."""
    val = " ".join([str(attrs.get(k) or "") for k in ["RAM Size", "Memory", "RAM"]] + [title]).lower()
    m = re.search(r"(\d+)\s*gb", val)
    return f"{m.group(1)}gb" if m else ""


def _extract_size(attrs: Mapping[str, Any], title: str) -> str:
    """Extracts screen size (e.g., '13in', '142in', '16in') for Macs/iPads."""
    val = " ".join([str(attrs.get(k) or "") for k in ["Screen Size", "Display"]] + [title]).lower()
    m = re.search(r"(\d{2}(\.\d)?)\s*(inch|\"|in)", val)
    return f"{m.group(1).replace('.', '')}in" if m else ""


def apple_model_key(attrs: Mapping[str, Any], title: str = "") -> Optional[str]:
    """
    Granular Apple Key Generator (2026 Edition).
    Structure: apple-<family>-<size>-<chip>-<ram>-<storage>_<grade>
    """
    if not attrs: return "unknown"

    def _brand_is_apple() -> bool:
        brand = _clean(attrs.get("Brand") or attrs.get("Marca") or attrs.get("brand"))
        return "apple" in brand

    if not _brand_is_apple(): return None

    # 1. Base Spec Extraction
    storage = _extract_storage(attrs, title)
    ram = _extract_ram(attrs, title)
    size = _extract_size(attrs, title)

    # 2. Chip Extraction (M1-M5 / Intel)
    chip_src = " ".join([str(attrs.get(k) or "") for k in ["Chipset Model", "Processor", "CPU"]] + [title]).lower()
    chip = ""
    m_chip = re.search(r"\bm([1-5])\s*(pro|max|ultra)?\b", chip_src)
    if m_chip:
        chip = f"m{m_chip.group(1)}{(m_chip.group(2) or '').strip()}"
    elif "intel" in chip_src or "core-i" in chip_src:
        chip = "intel"

    # 3. Family Logic
    family_blob = "-".join([_clean(attrs.get(k)) for k in ["Product Line", "Series", "Model"] if attrs.get(k)])

    parts = ["apple"]

    # --- MACS ---
    if any(x in family_blob for x in ("macbook", "imac", "mini", "studio")):
        line = "macbook-air" if "air" in family_blob else "macbook-pro" if "pro" in family_blob else "imac"
        parts.extend([line, size, chip, ram, storage])

    # --- IPHONES ---
    elif "iphone" in family_blob:
        num = re.search(r"iphone\s*(\d+)", family_blob + " " + title.lower())
        model_num = num.group(1) if num else ""
        tier = "pro-max" if "max" in family_blob else "pro" if "pro" in family_blob else "plus" if "plus" in family_blob else ""
        parts.extend(["iphone", model_num, tier, storage])

    # --- IPADS ---
    elif "ipad" in family_blob:
        line = "ipad-pro" if "pro" in family_blob else "ipad-air" if "air" in family_blob else "ipad"
        parts.extend([line, size, chip or "gen-unk", storage])

    # --- WATCHES ---
    elif "watch" in family_blob:
        series = "ultra" if "ultra" in family_blob else "se" if "se" in family_blob else ""
        if not series:
            s_match = re.search(r"series\s*(\d+)", family_blob + " " + title.lower())
            series = f"series-{s_match.group(1)}" if s_match else ""

        # Fixed the Watch sizing logic with the _num helper
        mm_size = _num(attrs.get("Case Size") or title)
        parts.extend(["watch", series, (mm_size + "mm" if mm_size else "")])

    # 4. Final Cleanup & Grading
    base_key = "-".join([p for p in parts if p]).strip("-")
    if base_key == "apple": return "unknown"

    grade = _derive_condition_grade(attrs, title)
    return f"{base_key}_{grade}"