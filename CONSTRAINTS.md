# Constraints

## Hard Nos

- No autonomous bids, buys, offers, messages, relists, refunds, dispatch marks,
  or other third-party account writes.
- No secrets in code, docs, logs, summaries, commits, or chat.
- No fake comps: confirmed sold final prices only.
- No whole-product valuation for parts, spares, repairs, boxes, manuals,
  accessories, locked devices, or incomplete listings.
- No silent live fallback from PostgreSQL to local files or SQLite.
- No uncapped marketplace/API loops.

## Buy Candidate Gate

A review candidate needs:

- live/open listing and valid URL,
- clean product identity,
- confirmed sold comp depth,
- realistic fees/postage/packaging/repair/risk reserves,
- positive expected profit,
- safe max buy price,
- persisted red flags/evidence,
- human approval before real money moves.

## If Unsure

Downgrade to review/skip and record why.
