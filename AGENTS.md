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

## Engineering Defaults

- Prefer small, testable Python changes that improve collection, final-price
  recovery, product identity, comp quality, max-bid calculation, or shortlist
  quality.
- Use migrations for schema changes.
- Keep docs short. Update [PROGRESS.txt](PROGRESS.txt) only for meaningful state
  changes.
