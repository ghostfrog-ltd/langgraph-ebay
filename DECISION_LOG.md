# Decision Log

Only durable project decisions belong here.

## 2026-07-21 - Mission And Mechanism

Decision:

- Number 1 Command: buy cheap from eBay to resell on eBay for profit.
- The project exists to build a side-hustle evidence engine, not analytics for
  its own sake.
- Final-price recovery and product identity quality come before Buy Desk
  automation.
- Candidate validation replaces the vague "paper trading" framing.

Why:

- The historical listing data is valuable only if sold/unsold/unknown/dirty rows
  are separated.
- Profit comes from buying below safe resale value, not from dashboards.
- The system must stay focused on finding good buys, avoiding bad buys, and
  learning which markets work.

## 2026-07-21 - Plain Potentials Query

Decision:

- The core operator report is a direct live-listing potentials query, not an LLM
  scoring report.
- It must output listing, model key, current price, sold median, comp count, max
  safe buy, expected profit, ROI, and a short deterministic signal.
- Default gate is side-hustle aligned: expected profit >= £50 and at least 3
  confirmed sold comps for the same clean `model_key`.
- ROI is displayed for context but is not the default blocker.
- Closest misses are useful because the human may choose to punt when the maths
  is close.
- Avoid abstract operator labels such as research lanes, data gaps, parked
  lanes, or LLM-style buckets.

Why:

- The human needs to decide whether a concrete listing is worth buying, not
  interpret project-management labels.
- Profit comes from current price being below safe resale value for the same
  product identity.
- Near-misses can still be worth a human punt when condition, postage,
  negotiation, or lane-learning justifies it.

## 2026-07-21 - Strict Apple Identity And Separate Condition

Decision:

- Apple/Mac product identity must not include condition suffixes.
- Mac identity is strict: line, chassis when known, screen, CPU/chip, RAM, and
  SSD/storage.
- CPU/chip, RAM, and storage are hard walls for Mac comps. Missing or
  conflicting Mac specs become `unknown` rather than guessed keys.
- Condition is stored separately as `condition_grade`.
- Sold comps are grouped by `model_key` plus `condition_grade`.

Why:

- Broad or polluted keys can mix cheap junk with valuable Macs.
- RAM, SSD, and CPU are exactly what Mac buyers price against.
- Condition still matters, but it should not fragment product identity or hide
  dirty parsing bugs such as `512gb-512gb`.

## 2026-07-21 - Delete Inaccessible Final-Price Rows

Decision:

- If eBay Trading returns an item-not-found/inaccessible/deleted code for a
  listing with no confirmed final price, delete the local listing row.
- Known delete reasons include `trading_item_not_found_17`,
  `trading_item_not_found_1505`, and `trading_item_not_found_21920397`.
- Do not use these rows as ended listings, comps, history, or retry targets.

Why:

- The mission only needs clean model keys and confirmed final prices.
- Inaccessible rows cannot produce a final price and clog backfill work.
- Keeping them adds noise without improving buy decisions.

## 2026-07-21 - Unknown Replaces Stale

Decision:

- Do not use `stale` as a listing state.
- If the collector stops seeing a row, mark it `unknown`, not ended.
- Final-price checks only process unknown rows whose `end_time` has passed.
- If eBay says not ended, restore the row to `live`.
- If eBay says inaccessible/not found/deleted, delete the local row.
- If eBay says sold, store `sold` plus confirmed `final_price`.
- If eBay says ended with no confirmed sale, store `ended`.

Why:

- "Not seen recently" is not the same as "ended".
- Treating missing observations as ended clogged final-price backfill with rows
  that eBay still considered live.
- The mission needs clean final prices and model keys, not a vague stale bucket.

## 2026-05-31 - Core Stack

Decision:

- Python + PostgreSQL remain the active data engine.
- LangGraph is used for orchestration where it helps.

Why:

- The existing collectors, pipelines, tests, and operational data are already in
  Python/PostgreSQL.
- Rewriting the engine is not the side-hustle priority.

## 2026-05-27 - Safety Boundary

Decision:

- The system may recommend, rank, calculate, explain, and notify.
- It may not bid, buy, message, relist, refund, mark shipped, or otherwise
  change marketplace state without explicit human approval.

Why:

- Marketplace actions spend money and affect account reputation.
