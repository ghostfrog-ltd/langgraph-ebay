# GhostFrog listing states — plain-English guide

**Status: proposed standard for [Issue #19](https://github.com/ghostfrog-ltd/langgraph-ebay/issues/19); not yet a database migration.** This document explains the intended meanings and sets rules for future changes. It does **not** claim that all existing records already conform. PostgreSQL is the source of truth for current stored values.

## The three questions we must keep separate

1. **What happened to the eBay listing?** A *listing lifecycle* state.
2. **Did GhostFrog manage to check it?** A *collection/backfill processing* result, including retries and API errors.
3. **Can we safely value or buy it?** A *product identity / condition / review* decision.

One column named `status` should not attempt to answer all three.

## Intended listing lifecycle (four machine values)

| Machine value | Website label | What it means | What it does **not** mean |
| --- | --- | --- | --- |
| `live` | Live | eBay was last seen reporting the listing active. | Guaranteed still purchasable. Recheck end time, freshness, price and availability. |
| `unknown` | Outcome unknown | GhostFrog cannot currently verify whether the listing is active or how it finished. | Sold, unsold, faulty or permanently missing. |
| `sold` | Sold | A sale has been confirmed with a usable final sale price. | Automatically a clean comparable; exact identity/condition still matter. |
| `ended` | Ended · sale unconfirmed | Listing ended but GhostFrog has no confirmed sale price. | Proof that nothing sold. Do not use as a confirmed sold comp. |

Use **lowercase `snake_case`** for machine values. Website labels and report headings may be readable English, but must map back to the same machine values.

**Example:** A MacBook auction vanishes from search. Its listing is not automatically `sold` or `ended`; it may become `unknown` pending a conclusive check. A Trading API timeout is a *failed check*, not proof that the MacBook was not sold.

## What the legacy values mean

These values exist in the repository's historic `listing_statuses` migrations. Actual use and row counts in PostgreSQL **must be audited before changing anything**.

| Existing label | Intended interpretation / investigation |
| --- | --- |
| `active`, `open`, `ending`, `ending_soon` | Legacy live-like listing states. Proposed canonical equivalent: `live`, subject to actual eBay evidence and row audit. "Ending soon" should usually be **derived from `end_time`**, not stored as a separate lifecycle state. |
| `stale` | Historic name replaced by `unknown` in migration `20260721_004_replace_stale_with_unknown.sql`. Do not reintroduce it. |
| `parse_failed` | **Problem:** one ended-listing path stores this as a terminal listing state with `finalized=TRUE`. A parse failure tells us about a **check attempt**, not the eBay sale outcome. Investigate and plan a safe correction; do not silently rewrite these rows. |
| `closed`, `retry`, `deleted`, `error`, `skipped` | Can appear in operator prose or operation outcomes. **Not verified as persisted listing-status values.** Inspect usage before mapping. `deleted` may mean GhostFrog removed an inaccessible local row; that is different from an eBay sale state. |

Other fields are independent: `model_key='unknown'` means the *product identity* is unknown, not that `auction_listings.status` is unknown. `finalized`, `backfill_last_reason`, review flags and retry counts are not extra marketplace states.

## Separate processing and quality concepts (design proposal)

These are **examples for future schemas/interfaces**, not values to insert into `auction_listings.status` today.

| Question | Examples | Where they belong |
| --- | --- | --- |
| Check/worker result | `pending`, `running`, `retry_required`, `failed`, `completed` | Check-attempt / queue / backfill metadata |
| Product decision | `verified`, `needs_review`, `rejected` | Identity/condition/review evidence |
| eBay sale outcome | `live`, `unknown`, `sold`, `ended` | The canonical listing lifecycle |

Do not add a new state for every new feature; derive urgency, price or age labels from fields such as `end_time`, `last_seen_at`, `bids_count` and the review evidence.

## Transition rules for implementers

- **Observed active by eBay:** a new listing, or an `unknown` listing conclusively confirmed active, may be `live`.
- **No longer seen in our search:** do not declare `ended` or `sold`. Treat as uncertainty and follow the approved unknown/backfill policy.
- **Confirmed ended with a valid sale price:** `sold`. Include in comps only if identity, condition and quality checks also pass.
- **Confirmed ended, without a confirmed sale price:** `ended`; do not turn missing price into £0 or treat the row as a sold comp.
- **API error, timeout, missing fields or parse failure:** record failure/retry details separately. Do not call a marketplace outcome confirmed or mark it terminal solely because checking failed.
- **Inaccessible item:** current code sometimes deletes the local record. Retention and audit policy must be reviewed before changing this behaviour.
- **Ended/sold back to live:** require authoritative correction/relisting evidence; no arbitrary status reset.
- **Buying shortlist:** `live` is necessary but **not sufficient**; require a current, non-expired listing, clean product/condition evidence and valid profitability calculation. See [Issue #16](https://github.com/ghostfrog-ltd/langgraph-ebay/issues/16).

## Codex / developer instructions before implementing Issue #19

1. **Audit actual data (read-only).** Count distinct `auction_listings.status` values and `finalized` combinations by source and age; examine `listing_statuses`, retry/check history and contradictory rows. Do not assume historical labels still exist merely because a migration permits them.
2. **Inventory every producer and consumer.** Include collectors, ended/backfill pipelines, comps, Python opportunities, MCP, Laravel sync/UI, tests and operators. Distinguish persisted statuses from display labels and temporary job results.
3. **Produce a before/after mapping and transition matrix.** Resolve ambiguous records explicitly. Identify which states can cause missed listings, false opportunities or lost sold evidence. Do not merge unknown outcomes into sold or ended.
4. **Plan a reversible migration.** Preserve historic price and listing evidence; add compatibility reads/joins as needed; run a dry-run with affected row counts; specify rollback and a deployment order so old/new consumers do not disagree.
5. **Add tests first.** Cover unknown on disappearance, live restoration, sold only on confirmed evidence, ended without confirmed price, parse failure remaining retryable, and excluding expired/non-live rows from purchases.
6. **Ask for approval before any production write.** No schema migrations, mass updates, collector downtime, source disabling, deletion, marketplace actions or new subscriptions under this documentation change.

**Sources for current behaviour:** `PURPOSE.md`, `SCHEMA.md`, `migrations/20260531_004_add_listing_status_reference.sql`, `migrations/20260721_004_replace_stale_with_unknown.sql`, `pipelines/listing/ended/graph.py` and `pipelines/listing/point_zero/backfill_apply.py`. The desired contract is a proposal until Issue #19 is approved and implemented.
