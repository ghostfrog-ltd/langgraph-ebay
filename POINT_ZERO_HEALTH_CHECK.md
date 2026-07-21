# Point Zero Health Check

**Original audit date**: 2026-05-27  
**Latest refresh**: 2026-05-28, after model-key repair, comp fix, ROI guardrails, targeted backfill, paper-trading resolver work, and purchase-side paper P&L tightening.  
**Mode**: Local PostgreSQL/code health check. No marketplace writes.  
**Database**: Python/PostgreSQL operational database.

## Executive Summary

Point Zero first pass is complete. The system is no longer in the dangerous state where ended rows without confirmed sale prices can feed comps. We now have:

- confirmed-final-price-only comps,
- weekly PostgreSQL backups,
- targeted stale-finalization backfill tooling,
- model-key repair passes for Apple, consoles, Lego, and MotoMine,
- vehicle-like ROI sources held review-only by default,
- stronger accessory/parts guardrails,
- paper-trade prediction persistence,
- paper-trade lesson reporting,
- purchase-side paper P&L with inbound postage, packaging, and selling/relist reserves,
- and a capped resolver for overdue paper-trade outcomes.

The system is healthy enough to keep collecting, scoring, and paper trading. It is not yet ready to label anything `BUY`, `BID`, or `ACTIONABLE`.

The next gate is evidence: let paper trades close, resolve their outcomes, then use the lessons to tune condition/cost/model-key rules before building Buy Desk action labels.

Current handoff state for the next session:

- `paper_trade_predictions` is live and schema-managed by `pipelines.listing.paper_trading.ensure_tables`.
- The latest no-resolve paper run refreshed all `7` current Apple/device predictions using the stricter cost stack.
- All `7` predictions are still watching; none are closed yet.
- One iPhone candidate now falls below the `GBP 50` paper-profit target after reserves, but remains tracked for learning.
- No marketplace writes have been made.

## Current Snapshot

### Listing Store

- total listings: `114,647`
- live listings: `1,240`
- sold rows: `12,458`
- ended rows: `17,300`
- stale rows: `83,438`
- unfinalized stale rows: `83,437`
- parse failed rows: `211`
- rows with confirmed `final_price`: `12,458`
- rows with missing/unknown model key: `44,540`

Interpretation:

- Final-price evidence exists, but only for `sold` rows.
- The large stale backlog remains real, but it is now treated as incremental data hygiene rather than a launch blocker.
- The live scoring surface is much smaller and more important than the historical backlog.

### Comps

- comp keys: `3,195`
- confirmed final-price comp samples: `8,853`
- comp keys with 3+ samples: `543`
- latest recompute: `2026-05-27 23:28:39 +01:00`

What changed:

- Comps now use `final_price IS NOT NULL` only.
- `price_current` is no longer used as a fake realized sale price.
- This fixed the main Point Zero data integrity issue.

### Live Comp Coverage

- live listings: `1,240`
- live listings with any comps: `303`
- live listings with 3+ comp samples: `218`
- live listings with unknown model key: `422`

Coverage by active source:

| Source | Live | With comps | With 3+ comps | Unknown model key |
|---|---:|---:|---:|---:|
| ebay-apple | 254 | 184 | 137 | 7 |
| ebay-consoles | 186 | 55 | 54 | 129 |
| ebay-mini-cooper | 158 | 19 | 11 | 0 |
| ebay-pokemon | 103 | 0 | 0 | 0 |
| ebay-watches | 85 | 4 | 2 | 48 |
| ebay-retro-pc | 80 | 0 | 0 | 80 |
| ebay-lego | 76 | 10 | 0 | 9 |
| motomine | 72 | 31 | 14 | 0 |
| ebay-honda-msx125 | 63 | 0 | 0 | 1 |
| ebay-motors | 62 | 0 | 0 | 62 |
| ebay-tools | 39 | 0 | 0 | 27 |
| ebay-samsung | 36 | 0 | 0 | 35 |
| ebay-actioncams | 24 | 0 | 0 | 22 |
| ebay-headphones | 2 | 0 | 0 | 2 |

Interpretation:

- Apple is the first usable paper-trading lane.
- Consoles have comp coverage but still have high unknown model-key count.
- Mini, Honda, motors, and MotoMine are intentionally not trusted as normal parcel-post ROI lanes.
- Pokemon, retro PC, tools, Samsung, action cams, and headphones need model-key/comps work before meaningful scoring.

## 0.1 Ended/Sold Price Collection

**Status**: Improved but still incomplete.

Healthy:

- `sold` rows carry confirmed `final_price`.
- Comps are no longer polluted by `ended` rows without `final_price`.
- Paper-trade outcome resolver can spend targeted Trading API calls on watched predictions after end time.

Still weak:

- `ended` and `stale` rows generally do not carry confirmed final prices.
- The stale backlog remains large at `83,438` rows.
- Only `5` stale rows have been checked through the new targeted backfill audit fields so far:
  - `4` stayed stale because Trading said not ended.
  - `1` became ended/no sale.

Decision:

- Do not wait for the full stale backlog.
- Resolve outcomes in this order:
  1. overdue paper-trade predictions,
  2. current live model keys with weak comp coverage,
  3. broader historical cleanup only when spare API budget remains.

## 0.2 Backfill Needs

**Status**: Backfill exists as a controlled lane, not a blocker.

Backfill planner current budget:

- daily cap: `5,000`
- safety reserve: `500`
- calls today: `1,761`
- planned normal calls: `1,776`
- theoretical spare: `2,724`
- recommended capped backfill calls: `1,000/day`

Current safe preview lane:

- source: `ebay-apple`
- model key: `apple-macbook-pro-162in-m1pro-16gb-512gb_b`
- live count: `8`
- current comp samples: `2`
- preview candidates are stale Apple MacBook Pro rows from March 2026.

Backfill sources needing work:

| Source | Keys needing comps | Live listings needing comps | Stale pool |
|---|---:|---:|---:|
| ebay-mini-cooper | 103 | 147 | 551 |
| ebay-apple | 93 | 110 | 999 |
| ebay-honda-msx125 | 1 | 62 | 113 |
| motomine | 57 | 58 | 50 |
| ebay-watches | 29 | 35 | 45 |
| ebay-lego | 29 | 34 | 139 |
| ebay-tools | 11 | 12 | 10 |
| ebay-consoles | 3 | 3 | 13 |

Important exclusion:

- Vehicle/parts-heavy sources are still excluded from broad backfill by default:
  - `ebay-honda-msx125`
  - `ebay-honda-nc750`
  - `ebay-mini-cooper`
  - `ebay-motors`
  - `motomine`

Reason:

- They can mix whole vehicles, spares/repair vehicles, parts, and accessories.
- They need specific condition/cost modelling before ROI can be trusted.

## 0.3 Other Scoring Blockers

**Status**: Materially improved, still not Buy Desk ready.

Fixed or improved:

- Model-key repair applied for Apple, consoles, Lego, and MotoMine.
- MotoMine was audited and is effectively motorcycle-only in current live rows.
- MotoMine unknown model keys are now `0` for live rows.
- Broad vehicle-like ROI is review-only unless `GF_ENABLE_VEHICLE_ROI=1`.
- ROI persistence now clears stale active `roi_estimate` and `max_bid` before writing the current eligible set.
- Console headset and Xbox PSU false positives are filtered.
- Current ROI no longer has live rows over 1000% ROI.

Current ROI indicators:

- live rows with ROI above 15%: `20`
- live rows with ROI above 100%: `5`
- live rows with ROI above 1000%: `0`
- active rows with current ROI estimates: `176`
- latest shortlist: `7` Apple/device candidates

Remaining blockers:

- `422` live rows still have unknown model keys.
- Consoles have `129` live unknown model keys.
- Retro PC has `80` live unknown model keys.
- Motors has `62` live unknown model keys.
- Watches has `48` live unknown model keys.
- Samsung has `35` live unknown model keys.
- Tools has `27` live unknown model keys.
- Action cams has `22` live unknown model keys.
- Pokemon has no live comp coverage despite model keys being non-unknown.

Buy Desk implication:

- Apple/device candidates can be paper traded.
- Nothing should be labelled `BUY`, `BID`, or `ACTIONABLE` yet.

## 0.4 API Call Efficiency

**Status**: Healthy enough, but must remain budgeted.

API calls today:

| Service | Calls |
|---|---:|
| ebay_base_fetch_category_items_v1 | 1,312 |
| ebay_attributes_v1 | 387 |
| ebay_ended_v1 | 44 |
| ebay_auth_v1 | 12 |
| ebay_base_seller_items_v1 | 6 |

Total calls today: `1,761`

Interpretation:

- Current usage is well below the 5k/day cap.
- Category search is still the dominant cost.
- Ended/outcome calls are low and now better targeted.
- The highest-value API spend is overdue paper-trade outcomes first, then active comp gaps, not global stale cleanup.

## Paper Trading Status

**Status**: Live and waiting for outcomes.

- paper predictions: `7`
- watching: `7`
- watching past end time: `0`
- checked today: `0`
- closed today: `0`
- closed total: `0`
- sold with observed final price: `0`
- would have won under max bid: `0`
- closed above max bid: `0`
- profitable on paper: `0`
- current lesson category: `watching`

The paper-trade resolver is ready. It will only spend Trading API calls when watched predictions pass `end_time` plus the configured grace window.

Accounting model:

- confirmed auction `final_price` is treated as the hypothetical purchase price,
- confirmed sold comps remain the expected resale estimate,
- default inbound postage reserve: `GBP 5`,
- default packaging reserve: `GBP 2`,
- default selling/relist reserve: `GBP 3`,
- platform fee assumption remains inherited from ROI, currently `13%`,
- observed paper profit is only calculated when the auction closed within the stored max bid.

Paper formula:

```text
paper profit =
  predicted_resale_price
  - predicted_fees
  - predicted_outbound_shipping
  - predicted_inbound_shipping
  - predicted_packaging
  - predicted_selling_reserve
  - observed_purchase_price
```

If the auction closes above max bid, the outcome is `outbid_above_ceiling`: a disciplined no-buy, not a loss.

Defaults:

- `GF_PAPER_OUTCOME_RESOLVE_LIMIT=5`
- `GF_PAPER_OUTCOME_GRACE_MINUTES=60`
- `GF_PAPER_OUTCOME_RECHECK_HOURS=6`

## Current Health Classification

### Healthy

- PostgreSQL is live and has weekly backups configured.
- Scheduler/heartbeat is running.
- Heartbeat email now reports operational health, backfill position, API usage, paper trading, and lesson categories.
- Comps now use confirmed final prices only.
- API usage is under cap.
- Duplicate external IDs were healthy in the first audit and no regression has been observed.
- Apple has enough comp coverage to support paper trading.

### Needs Work

- Stale backlog is still large.
- Live unknown model keys are still high.
- Non-Apple sources need model-key and comp work.
- Vehicle-like sources need proper condition/transport/repair/reserve modelling.
- Paper trades need to close before we have evidence-based learning.

### Blocked Before Buy Desk Labels

Do not build `BUY`, `BID`, or `ACTIONABLE` labels until:

1. Paper trades produce closed outcomes and lesson categories.
2. Apple/device scoring has at least a small evidence sample from those outcomes.
3. Condition/cost reserves are tuned from misses, especially inbound postage, packaging, selling/relist reserve, repair/refurb reserve, and promoted/listing fee assumptions.
4. Minimum comp-sample and freshness thresholds are documented per active niche.
5. Buy Desk candidates persist confidence, red flags, cost assumptions, and human approval state.

## Recommended Next Step

Let the first paper-trade predictions pass `end_time`, allow the capped resolver to close them, then review the lesson categories:

- `paper_loss`: inspect condition language, fees, shipping, accessory terms, exact model match.
- `outbid_above_ceiling`: accept the no-buy discipline unless too many good candidates are being priced out.
- `overestimated_resale`: tighten comps or add condition reserves.
- `thin_margin`: raise reserves or minimum profit threshold.
- `no_confirmed_sale`: decide whether the model key deserves targeted backfill.
- `prediction_ok`: keep gathering evidence before loosening gates.

After the first closed batch, run a focused Apple/device scoring review before adding any Buy Desk persistence or UI.

Useful next-session commands:

```bash
.venv-mac/bin/python -m pipelines.listing.paper_trading --limit 25
.venv-mac/bin/python -m unittest discover tests
```

Use `--no-resolve` on the paper-trading command when you want a local-only snapshot refresh without spending Trading API calls.
