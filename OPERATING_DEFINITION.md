# Operating Definition

**Date**: 2026-07-21

## Number 1 Command

Buy cheap from eBay to resell on eBay for profit.

## What This Project Is

GhostFrog eBay exists to support that mission. It is a historical-market
evidence system first, and a buy-decision support system later.

Its immediate job is to turn the collected eBay listing history into reliable
market evidence:

- what actually sold,
- what failed to sell,
- what model/product identity each listing really represented,
- what complete items are worth after fees, postage, repair risk, and reserves,
- and which markets have enough repeatable evidence to deserve human attention.

The system must not place bids, buy items, message sellers, relist, or mutate
marketplace state without explicit human approval and a deterministic safety
gate.

## Current Priority

The next project priority is final-price recovery.

The repo appears to hold many months of listing observations across some
markets. That history is valuable even though it is incomplete. Its value
depends on recovering and separating:

- confirmed sold final prices,
- ended/no-sale outcomes,
- still-live/relisted rows,
- stale rows that need a targeted outcome check,
- dirty rows that should never become comps, such as parts, accessories, boxes,
  manuals, repair-only listings, or incomplete products.

Until final-price coverage and product-identity quality are strong enough in a
lane, ROI scores and paper/watchlist outcomes are research signals only.

## What "Paper Trading" Means Here

Paper trading is not a trading strategy. It is candidate validation.

Its only useful purpose is to test whether a proposed review candidate would
have been sensible under historical evidence:

- Did the listing represent the complete product the system thought it did?
- Would the stored max bid have won?
- Was the confirmed purchase-side final price below the review ceiling?
- Would the resale estimate have cleared fees, inbound postage, outbound
  postage, packaging, repair/refurb reserve, and selling/relist risk?
- What deterministic guardrail or model-key rule should change when it was
  wrong?

Paper/watchlist validation must not be used to imply permission to bid or buy.

## Work Order

1. Stabilize the heartbeat and database queries so evidence recovery can run
   predictably.
2. Prioritize targeted final-price recovery for source/model-key lanes that
   already have months of observations and plausible resale markets.
3. Rebuild comps only from confirmed final prices and exclude dirty product
   identities before they can affect resale estimates.
4. Use candidate validation to measure whether review candidates survive real
   final-price outcomes.
5. Define lane readiness before any Buy Desk labels: confirmed comp depth,
   product identity cleanliness, freshness, postage/cost assumptions, and enough
   candidate-validation outcomes to justify human review.

## What Is Not The Goal Yet

- A fully automated eBay trader.
- A dashboard that looks impressive while final-price evidence is thin.
- Broad generic ROI scoring across dirty categories.
- Buying or bidding from model output.
- Treating unconfirmed current prices as sold comps.
