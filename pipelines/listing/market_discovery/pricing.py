"""Shared acquisition maths for live listing reports."""

FEE_RATE = 0.13
OUTBOUND_SHIP_GBP = 7.0
INBOUND_SHIP_GBP = 5.0
PACKAGING_GBP = 2.0
SELLING_RESERVE_GBP = 3.0
MIN_PROFIT_GBP = 50.0
MIN_CANDIDATE_COMPS = 15
CATEGORY_MIN_CANDIDATE_COMPS: dict[str, int] = {
    "ebay-apple": 10,
    "ebay-samsung": 5,
    "ebay-graphics-cards": 5,
    "ebay-consoles": 15,
    "ebay-actioncams": 15,
    "ebay-lego": 15,
}


def money(value: float) -> float:
    return round(float(value or 0.0) + 1e-9, 2)


def expected_profit(*, current_price: float, sold_median: float) -> float:
    return money(
        sold_median * (1.0 - FEE_RATE)
        - OUTBOUND_SHIP_GBP
        - INBOUND_SHIP_GBP
        - PACKAGING_GBP
        - SELLING_RESERVE_GBP
        - current_price
    )


def max_safe_buy(*, sold_median: float, min_profit: float) -> float:
    return expected_profit(current_price=min_profit, sold_median=sold_median)


def effective_min_comps_for_source(source: str | None, *, override: int | None = None) -> int:
    if override is not None:
        return max(int(override), 1)
    return CATEGORY_MIN_CANDIDATE_COMPS.get(str(source or "").strip().lower(), MIN_CANDIDATE_COMPS)
