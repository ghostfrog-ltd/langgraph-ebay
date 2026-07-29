# Purpose

## Number 1 Command

Buy cheap from eBay to resell on eBay for profit.

## Mechanism

Use historical eBay evidence to find live listings priced below safe resale
value.

The system should:

1. Collect listings in selected eBay markets.
2. Recover confirmed final sold prices.
3. Group listings by real product identity. For Macs, strict identity means
   line/chassis/screen/CPU/RAM/SSD; condition is separate.
4. Block junk: parts, spares, repairs, boxes, manuals, accessories, scams,
   locked devices, and incomplete products.
5. Build comps only from confirmed sold prices for the same product identity
   and condition grade.
6. For live listings, run a plain potentials query:
   `listing + model_key + current_price + sold_median + comp_count +
   max_safe_buy + expected_profit + signal`.
7. Use a simple default gate: expected profit >= £50 and at least 3 confirmed
   sold comps for the same clean `model_key` and condition grade. ROI is
   context, not the default blocker.
8. Show closest misses as well as potentials so the human can decide whether a
   near-miss is worth a punt.
9. Record manual buy/check/resell outcomes so the system learns which markets
   work.

## Listing States

Use this simple state machine:

1. Collector stops seeing row -> `unknown`, not ended.
2. Final-price checks only rows whose `end_time` has passed.
3. If eBay says not ended -> `live`.
4. If eBay says inaccessible/not found/deleted -> delete the local row.
5. If eBay says sold -> `sold` with confirmed `final_price`.
6. If eBay says ended with no confirmed sale -> `ended`.

## Current Focus

The immediate loop is:

1. Keep resolving unknown past-end rows into live/sold/ended/deleted outcomes.
2. Keep repairing strict product keys and condition grades.
3. Rebuild comps from confirmed final prices.
4. Run the live-listing potentials query.
5. Inspect potentials and closest misses.
6. Human decides whether any near-miss is worth a punt.

Avoid abstract labels such as research lanes, data gaps, parked lanes, or
LLM-style status buckets. The useful output is the direct price comparison and
why the listing is or is not above the profit gate.

## Boundary

The software may recommend, rank, calculate, explain, and notify.

It must not bid, buy, message, relist, refund, mark shipped, or otherwise change
third-party marketplace state without explicit human approval.
