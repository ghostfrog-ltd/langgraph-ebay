# AGENTS.md

## Mission

Buy cheap from eBay to resell on eBay for profit.

Read [PURPOSE.md](PURPOSE.md) first. Keep work tied to that mission.

## Role

Codex is the lead developer agent. Build and repair the data engine, pipelines,
tests, docs, and operator tooling.

## Non-Negotiables

- Do not bid, buy, message, relist, refund, mark shipped, or mutate marketplace
  state without explicit human approval.
- Do not expose credentials or secrets.
- Do not value parts/accessories/repair-only listings against whole-product
  comps.
- Use PostgreSQL as the live source of truth.
- Preserve user changes unless asked to change them.

## Listing state conventions

For any code touching `auction_listings.status`, sale finalization, retry logic,
comps or buy-listing visibility, read [docs/listing-statuses.md](docs/listing-statuses.md).
Keep listing lifecycle separate from processing/retry and identity/condition
review. The guide is a **proposed standard** under Issue #19; no schema or data
migration is authorised by this link.

## Engineering Defaults

- Prefer small, testable Python changes that improve collection, final-price
  recovery, product identity, comp quality, max-bid calculation, or potentials
  quality.
- Keep operator output boring and concrete. The core report is the live-listing
  potentials query: listing, `model_key`, `condition_grade`, current price,
  sold median, comp count, max safe buy, expected profit, ROI, and a simple
  deterministic signal. Do not reintroduce abstract LLM-style lane buckets.
- Keep product identity and condition separate. For Macs, strict identity means
  line/chassis/screen/CPU/RAM/SSD; conflicting or missing core specs should
  become `unknown`, not a guessed comp bucket.
- Use migrations for schema changes.
- Keep docs short. Update [PROGRESS.txt](PROGRESS.txt) only for meaningful state
  changes.
