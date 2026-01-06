from __future__ import annotations
import os
import json
import time
import google.generativeai as genai
from typing import Any, Dict
from dotenv import load_dotenv
from google.api_core import retry

# 1. Load Environment Variables
load_dotenv()
API_KEY = os.getenv("GEMINI_API_KEY")

if not API_KEY:
    raise ValueError("GEMINI_API_KEY not found. Please check your .env file.")

genai.configure(api_key=API_KEY)

# 2. Updated Schema with Price Intelligence
ASSESSMENT_SCHEMA: Dict[str, Any] = {
    "type": "object",
    "properties": {
        "verdict": {"type": "string", "enum": ["BUY", "SKIP", "REVIEW"]},
        "risk_reasons": {"type": "array", "items": {"type": "string"}},
        "condition_normalized": {
            "type": "string",
            "enum": ["new", "used_good", "used_fair", "parts_only", "unknown"]
        },
        "price_analysis": {
            "type": "object",
            "properties": {
                "perc_vs_median": {"type": "number", "description": "How much % below/above median"},
                "deal_quality": {"type": "string", "enum": ["great", "fair", "poor", "unknown"]},
                "market_reliability": {"type": "string", "description": "Based on comp_samples"}
            },
            "required": ["perc_vs_median", "deal_quality", "market_reliability"]
        },
        "facts": {
            "type": "object",
            "properties": {
                "item": {"type": "string"},
                "brand": {"type": "string"},
                "model": {"type": "string"},
                "capacity": {"type": "string"},
                "included": {"type": "array", "items": {"type": "string"}},
                "missing": {"type": "array", "items": {"type": "string"}},
                "notes": {"type": "string"},
            },
            "required": ["item", "brand", "model", "capacity", "included", "missing", "notes"],
        },
        "confidence": {"type": "number"},
        "recommended_max_bid": {"type": "number"},
    },
    "required": ["verdict", "risk_reasons", "condition_normalized", "price_analysis", "facts", "confidence",
                 "recommended_max_bid"],
}

# 3. Decision-Driven System Prompt
SYSTEM_PROMPT = """
You are an expert eBay Arbitrage Analyst. Your primary goal is to identify undervalued items.
MARKET DATA RULES:
1. Primary Anchor: Use 'market_median' as the 'Fair Market Value'.
2. BUY Signal: If 'price_current' is < 75% of 'market_median' AND condition is 'used_good' or 'new'.
3. SKIP Signal: If 'price_current' > 'market_median' or 'market_mean'.
4. RELIABILITY: If 'comp_samples' < 5, lower your 'confidence' score; the market data is thin.
5. CALCULATE: Set 'recommended_max_bid' to (market_median * 0.8) to ensure a 20% margin.
"""


def post_to_gemini(listing_row: dict) -> dict:
    model_name = "gemini-2.0-flash-lite"

    model = genai.GenerativeModel(
        model_name=model_name,
        system_instruction=SYSTEM_PROMPT,
        generation_config={
            "response_mime_type": "application/json",
            "response_schema": ASSESSMENT_SCHEMA,
            "temperature": 0.1,
        }
    )

    # 4. Explicitly highlight the new features in the prompt
    user_input = f"""
    ITEM TITLE: {listing_row.get('title')}
    CURRENT PRICE: £{listing_row.get('price_current')}

    MARKET CONTEXT (model_key: {listing_row.get('model_key')}):
    - Market Median Sold Price: £{listing_row.get('market_median')}
    - Market Mean Sold Price: £{listing_row.get('market_mean')}
    - Number of Samples: {listing_row.get('comp_samples')}

    LISTING DATA:
    {json.dumps(listing_row.get('raw_attrs'))}
    """

    try:
        response = model.generate_content(
            user_input,
            request_options={"retry": retry.Retry(initial=2.0, multiplier=2.0, maximum=60.0)}
        )
        return json.loads(response.text)
    except Exception as e:
        print(f"Failed to process listing: {e}")
        return {}