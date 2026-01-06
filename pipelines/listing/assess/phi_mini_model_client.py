from __future__ import annotations

from typing import Any, Dict
import json
import requests

ASSESSMENT_SCHEMA: Dict[str, Any] = {
    "type": "object",
    "properties": {
        "verdict": {"type": "string", "enum": ["BUY", "SKIP", "REVIEW"]},
        "risk_reasons": {
            "type": "array",
            "items": {"type": "string"},
            "minItems": 0,
        },
        "condition_normalized": {
            "type": "string",
            "enum": ["new", "used_good", "used_fair", "parts_only", "unknown"],
        },
        "facts": {
            "type": "object",
            "properties": {
                "item": {"type": "string"},
                "brand": {"type": "string"},
                "model": {"type": "string"},
                "capacity": {"type": "string"},
                "included": {
                    "type": "array",
                    "items": {"type": "string"},
                },
                "missing": {
                    "type": "array",
                    "items": {"type": "string"},
                },
                "notes": {"type": "string"},
            },
            "required": [
                "item",
                "brand",
                "model",
                "capacity",
                "included",
                "missing",
                "notes",
            ],
            "additionalProperties": False,
        },
        "confidence": {"type": "number", "minimum": 0, "maximum": 1},
        "recommended_max_bid": {"type": "number", "minimum": 0},
    },
    "required": [
        "verdict",
        "risk_reasons",
        "condition_normalized",
        "facts",
        "confidence",
        "recommended_max_bid",
    ],
    "additionalProperties": False,
}

SYSTEM_PROMPT = f"""
You are an eBay listing risk assessor.

You will receive JSON input with a single key "listing".

Return ONLY a JSON object that matches this JSON Schema EXACTLY.
No markdown.
No commentary.
No extra keys.

JSON Schema:
{json.dumps(ASSESSMENT_SCHEMA, indent=2)}

Interpret VERDICT like this:

- BUY = "This looks like a good opportunity. The price seems fair or low for what is being offered, and I see no major red flags."
- SKIP = "This looks like a bad or risky opportunity. The price seems high for what is being offered, or there are serious concerns."
- REVIEW = "I genuinely cannot tell either way because important information is missing, contradictory, or too ambiguous."

Decision rules:

- Your goal is to MINIMISE REVIEW. Only use REVIEW if you truly cannot decide.
- If the information is mostly clear and normal-looking, you MUST choose BUY or SKIP.
- If you see clear negatives (damage, missing items, unclear condition, vague description, seller looks risky, etc.) and no strong positives, choose SKIP.
- If you see strong positives and the price does not obviously look too high for the described condition, choose BUY.
- Only use REVIEW if:
  - Key facts are missing (e.g. no condition, no photos, very vague description), OR
  - The description and data are contradictory in a way you cannot resolve.

IMPORTANT:
- Do NOT be overly conservative. If you can form a reasonable judgement, pick BUY or SKIP.
- confidence = how sure you are about your VERDICT (0 = guessing, 1 = very sure).
- If you choose REVIEW, confidence should usually be <= 0.6.
- If you choose BUY or SKIP and you're quite sure, confidence should be >= 0.7.
"""


def post_to_model(listing_row: dict) -> dict:
    payload = {
        "model": "phi3:mini",
        "messages": [
            {
                "role": "system",
                "content": SYSTEM_PROMPT,
            },
            {
                "role": "user",
                "content": json.dumps({"listing": listing_row}, ensure_ascii=False),
            },
        ],
        "format": "json",
        "stream": False,
        "options": {
            "temperature": 0,
            "num_ctx": 2048,
            "num_predict": 220,
        },
    }

    r = requests.post(
        "http://localhost:11434/api/chat",
        json=payload,
        timeout=600,
    )
    r.raise_for_status()

    content = r.json()["message"]["content"]

    # We expect JSON. If it's broken, we want to SEE it while learning.
    try:
        return json.loads(content)
    except json.JSONDecodeError as e:
        raise RuntimeError(
            "Model returned invalid JSON.\n"
            f"Error: {e}\n"
            f"Raw output:\n{content}"
        )
