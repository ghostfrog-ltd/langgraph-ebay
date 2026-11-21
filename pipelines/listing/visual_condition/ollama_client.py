from __future__ import annotations

import base64
import json
import re
import time
from pathlib import Path
from typing import Any, Mapping, Sequence

import requests


DEFAULT_MODEL = "qwen2.5vl:7b"
DEFAULT_OLLAMA_URL = "http://127.0.0.1:11434/api/chat"
VALID_GRADES = {"A", "B", "C", "D", "unknown"}
VALID_DAMAGE_FLAGS = {
    "activation_lock_or_error_screen",
    "bent_frame",
    "camera_lens_damage",
    "chipped_corner",
    "cracked_back_glass",
    "cracked_screen",
    "deep_scratches",
    "heavy_wear",
    "missing_parts",
    "water_damage_signs",
}
GRADE_RANK = {"A": 0, "B": 1, "C": 2, "D": 3, "unknown": 4}

SYSTEM_PROMPT = """
You are a visual condition checker for used resale items.

Inspect only the supplied listing images. Do not infer defects from price.
Return JSON only, with exactly these keys:
visual_grade: "A", "B", "C", "D", or "unknown"
damage_flags: array of strings
confidence: number from 0 to 1
review_required: boolean
notes: short string

Use only these damage flag strings when visibly applicable:
cracked_screen, cracked_back_glass, chipped_corner, deep_scratches,
camera_lens_damage, bent_frame, missing_parts, activation_lock_or_error_screen,
water_damage_signs, heavy_wear.

Grade policy:
A = visually like-new/new.
B = normal used condition, no visible cracks, chips, deep scratches, missing parts, or lock/error screens.
C = visible physical/cosmetic damage but likely complete, such as cracked glass, chipped corners, deep scratches, dents, or heavy wear.
D = visible functional/blocking risk, such as activation lock/error screens, missing major parts, bent frame, or obvious water damage.
unknown = images are too unclear, not product photos, or do not show enough of the item.

Be conservative. If unsure whether the item is clean or complete, set review_required true.
""".strip()


class VisualConditionError(RuntimeError):
    """Raised when the local visual checker cannot complete."""


def image_urls_from_attrs(attrs: Mapping[str, Any] | None) -> list[str]:
    if not isinstance(attrs, Mapping):
        return []

    raw = attrs.get("Browse imageUrls") or attrs.get("image_urls") or attrs.get("imageUrls")
    if raw is None and attrs.get("Browse imageUrl"):
        raw = [attrs.get("Browse imageUrl")]
    if isinstance(raw, str):
        raw = [raw]
    if not isinstance(raw, Sequence):
        return []

    urls: list[str] = []
    seen: set[str] = set()
    for value in raw:
        url = str(value or "").strip()
        if not url.lower().startswith(("http://", "https://")):
            continue
        if url not in seen:
            seen.add(url)
            urls.append(url)
    return urls


def _extract_json_object(text: str) -> dict[str, Any]:
    cleaned = (text or "").strip()
    cleaned = re.sub(r"^```(?:json)?\s*", "", cleaned)
    cleaned = re.sub(r"\s*```$", "", cleaned)
    try:
        parsed = json.loads(cleaned)
    except json.JSONDecodeError:
        match = re.search(r"\{.*\}", cleaned, flags=re.S)
        if not match:
            raise
        parsed = json.loads(match.group(0))
    return parsed if isinstance(parsed, dict) else {}


def _normalise_flag(value: Any) -> str:
    flag = re.sub(r"[^a-z0-9]+", "_", str(value or "").strip().lower()).strip("_")
    aliases = {
        "cracked_rear_glass": "cracked_back_glass",
        "back_glass_cracked": "cracked_back_glass",
        "screen_crack": "cracked_screen",
        "corner_chip": "chipped_corner",
        "lens_damage": "camera_lens_damage",
        "locked_screen": "activation_lock_or_error_screen",
        "activation_lock": "activation_lock_or_error_screen",
    }
    return aliases.get(flag, flag)


def normalise_visual_assessment(raw: Mapping[str, Any] | None, *, image_count: int) -> dict[str, Any]:
    if not isinstance(raw, Mapping):
        raw = {}

    flags: list[str] = []
    seen: set[str] = set()
    raw_flags = raw.get("damage_flags") or raw.get("damageFlags") or []
    if isinstance(raw_flags, str):
        raw_flags = [raw_flags]
    if isinstance(raw_flags, Sequence):
        for value in raw_flags:
            flag = _normalise_flag(value)
            if flag in VALID_DAMAGE_FLAGS and flag not in seen:
                seen.add(flag)
                flags.append(flag)

    grade = str(raw.get("visual_grade") or raw.get("visualGrade") or raw.get("grade") or "").strip().upper()
    if grade not in VALID_GRADES:
        grade = "unknown"

    if not image_count:
        grade = "unknown"
        flags = []

    if any(flag in flags for flag in ("activation_lock_or_error_screen", "bent_frame", "missing_parts", "water_damage_signs")):
        grade = "D"
    elif flags and GRADE_RANK.get(grade, 4) < GRADE_RANK["C"]:
        grade = "C"

    try:
        confidence = float(raw.get("confidence"))
    except (TypeError, ValueError):
        confidence = 0.0
    confidence = max(0.0, min(confidence, 1.0))

    review_required = bool(raw.get("review_required", raw.get("reviewRequired", False)))
    if grade in {"C", "D", "unknown"} or confidence < 0.7:
        review_required = True

    notes = str(raw.get("notes") or raw.get("reason") or "").strip()
    if not image_count:
        notes = "no images available"

    return {
        "visual_grade": grade,
        "damage_flags": flags,
        "confidence": confidence,
        "review_required": review_required,
        "notes": notes[:500],
    }


def _image_url_to_base64(url: str, *, timeout: int = 20, max_bytes: int = 7_000_000) -> str:
    response = requests.get(url, timeout=timeout)
    response.raise_for_status()
    content = response.content
    if len(content) > max_bytes:
        raise VisualConditionError(f"image too large: {len(content)} bytes")
    return base64.b64encode(content).decode("ascii")


def _image_path_to_base64(path: str | Path) -> str:
    return base64.b64encode(Path(path).read_bytes()).decode("ascii")


def assess_images(
    *,
    title: str,
    image_urls: Sequence[str] = (),
    image_paths: Sequence[str | Path] = (),
    model: str = DEFAULT_MODEL,
    ollama_url: str = DEFAULT_OLLAMA_URL,
    max_images: int = 6,
    timeout: int = 240,
) -> dict[str, Any]:
    selected_urls = [str(url) for url in image_urls if url][:max_images]
    selected_paths = [path for path in image_paths if path][: max(max_images - len(selected_urls), 0)]
    images: list[str] = []
    errors: list[str] = []

    for url in selected_urls:
        try:
            images.append(_image_url_to_base64(url))
        except Exception as exc:
            errors.append(f"{url}: {exc}")

    for path in selected_paths:
        try:
            images.append(_image_path_to_base64(path))
        except Exception as exc:
            errors.append(f"{path}: {exc}")

    if not images:
        result = normalise_visual_assessment({}, image_count=0)
        if errors:
            result["image_errors"] = errors[:5]
        return result

    user_prompt = json.dumps(
        {
            "title": title,
            "task": "Inspect these eBay listing images for visible condition damage.",
        },
        ensure_ascii=True,
    )
    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": user_prompt, "images": images},
        ],
        "format": "json",
        "stream": False,
        "options": {
            "temperature": 0,
            "num_predict": 260,
        },
    }

    started = time.perf_counter()
    response = requests.post(ollama_url, json=payload, timeout=timeout)
    response.raise_for_status()
    elapsed_s = round(time.perf_counter() - started, 3)

    content = response.json().get("message", {}).get("content", "")
    parsed = _extract_json_object(content)
    result = normalise_visual_assessment(parsed, image_count=len(images))
    result.update(
        {
            "model_name": model,
            "image_count": len(images),
            "elapsed_s": elapsed_s,
        }
    )
    if errors:
        result["image_errors"] = errors[:5]
    return result
