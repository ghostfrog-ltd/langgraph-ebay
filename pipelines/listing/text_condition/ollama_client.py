from __future__ import annotations

import json
import re
import time
from typing import Any, Mapping

import requests


DEFAULT_MODEL = "gemma3:12b"
DEFAULT_OLLAMA_URL = "http://127.0.0.1:11434/api/chat"
VALID_GRADES = {"A", "B", "C", "D", "unknown"}
VALID_RISK_FLAGS = {
    "accessory_only",
    "battery_fault",
    "casing_damage",
    "contradiction",
    "functional_fault",
    "locked_device",
    "missing_major_part",
    "parts_or_repair",
    "screen_damage",
    "seller_risk",
    "vague_or_untested",
    "water_damage",
}
GRADE_RANK = {"A": 0, "B": 1, "C": 2, "D": 3, "unknown": 4}
D_FLAGS = {
    "accessory_only",
    "functional_fault",
    "locked_device",
    "missing_major_part",
    "parts_or_repair",
    "vague_or_untested",
    "water_damage",
}
C_FLAGS = {"battery_fault", "casing_damage", "screen_damage"}

SYSTEM_PROMPT = """
You are a text condition evidence extractor for used eBay resale.

Read only the supplied listing title and captured eBay text/attributes.
Do not infer condition from price. Do not decide whether to buy.
Return JSON only, with exactly these keys:
text_grade: "A", "B", "C", "D", or "unknown"
risk_flags: array of strings
confidence: number from 0 to 1
review_required: boolean
notes: short string

Use only these risk flag strings:
accessory_only, battery_fault, casing_damage, contradiction,
functional_fault, locked_device, missing_major_part, parts_or_repair,
screen_damage, seller_risk, vague_or_untested, water_damage.

Grade policy:
A = text clearly says new, sealed, unused, or like-new with no risks.
B = text says normal working used condition with no material risks.
C = text describes cosmetic damage, scratches, cracks, weak battery, or heavy wear, but likely complete and working.
D = text says faulty, untested, locked, parts/spares/repair, missing major parts, water damage, accessory-only, box-only, or functionally blocked.
unknown = text is too sparse or contradictory to classify.

Be conservative. If text mentions a serious risk, include the flag and choose
the worse grade. If unsure, set review_required true.
""".strip()


class TextConditionError(RuntimeError):
    """Raised when the local text checker cannot complete."""


def captured_text_from_attrs(attrs: Mapping[str, Any] | None) -> dict[str, Any]:
    if not isinstance(attrs, Mapping):
        return {}

    useful: dict[str, Any] = {}
    prefixes = (
        "Browse condition",
        "Browse conditionDescription",
        "Browse description",
        "Browse shortDescription",
        "Browse subtitle",
        "Browse aspect ",
        "Browse seller ",
        "Browse itemLocation ",
        "Browse returnTerms ",
        "Browse shippingOptions",
    )
    for key, value in attrs.items():
        if any(str(key).startswith(prefix) for prefix in prefixes):
            useful[str(key)] = value
    return useful


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
        "activation_lock": "locked_device",
        "icloud_locked": "locked_device",
        "network_locked": "locked_device",
        "carrier_locked": "locked_device",
        "for_parts": "parts_or_repair",
        "for_spares": "parts_or_repair",
        "spares_or_repair": "parts_or_repair",
        "not_working": "functional_fault",
        "no_power": "functional_fault",
        "does_not_turn_on": "functional_fault",
        "cracked_screen": "screen_damage",
        "cracked_back_glass": "casing_damage",
        "battery_issue": "battery_fault",
        "untested": "vague_or_untested",
        "not_tested": "vague_or_untested",
        "box_only": "accessory_only",
        "charger_only": "accessory_only",
    }
    return aliases.get(flag, flag)


def normalise_text_assessment(raw: Mapping[str, Any] | None) -> dict[str, Any]:
    if not isinstance(raw, Mapping):
        raw = {}

    flags: list[str] = []
    seen: set[str] = set()
    raw_flags = raw.get("risk_flags") or raw.get("riskFlags") or raw.get("flags") or []
    if isinstance(raw_flags, str):
        raw_flags = [raw_flags]
    if isinstance(raw_flags, (list, tuple, set)):
        for value in raw_flags:
            flag = _normalise_flag(value)
            if flag in VALID_RISK_FLAGS and flag not in seen:
                seen.add(flag)
                flags.append(flag)

    grade = str(raw.get("text_grade") or raw.get("textGrade") or raw.get("grade") or "").strip().upper()
    if grade not in VALID_GRADES:
        grade = "unknown"

    if any(flag in flags for flag in D_FLAGS):
        grade = "D"
    elif any(flag in flags for flag in C_FLAGS) and GRADE_RANK.get(grade, 4) < GRADE_RANK["C"]:
        grade = "C"

    try:
        confidence = float(raw.get("confidence"))
    except (TypeError, ValueError):
        confidence = 0.0
    confidence = max(0.0, min(confidence, 1.0))

    review_required = bool(raw.get("review_required", raw.get("reviewRequired", False)))
    if grade in {"D", "unknown"} or confidence < 0.7 or "contradiction" in flags:
        review_required = True

    notes = str(raw.get("notes") or raw.get("reason") or "").strip()

    return {
        "text_grade": grade,
        "risk_flags": flags,
        "confidence": confidence,
        "review_required": review_required,
        "notes": notes[:500],
    }


def assess_text(
    *,
    title: str,
    raw_attrs: Mapping[str, Any] | None = None,
    model_key: str = "",
    source: str = "",
    model: str = DEFAULT_MODEL,
    ollama_url: str = DEFAULT_OLLAMA_URL,
    timeout: int = 180,
) -> dict[str, Any]:
    captured = captured_text_from_attrs(raw_attrs)
    if not title and not captured:
        result = normalise_text_assessment({"text_grade": "unknown", "review_required": True})
        result["notes"] = "no captured listing text available"
        return result

    user_prompt = json.dumps(
        {
            "source": source,
            "model_key": model_key,
            "title": title,
            "captured_text": captured,
            "task": "Extract condition evidence and risk flags from this eBay listing text.",
        },
        ensure_ascii=True,
        default=str,
    )
    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": user_prompt},
        ],
        "format": "json",
        "stream": False,
        "options": {
            "temperature": 0,
            "num_predict": 240,
        },
    }

    started = time.perf_counter()
    response = requests.post(ollama_url, json=payload, timeout=timeout)
    response.raise_for_status()
    elapsed_s = round(time.perf_counter() - started, 3)

    content = response.json().get("message", {}).get("content", "")
    parsed = _extract_json_object(content)
    result = normalise_text_assessment(parsed)
    result.update(
        {
            "model_name": model,
            "elapsed_s": elapsed_s,
        }
    )
    return result
