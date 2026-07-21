# SKILL.md

## Capability Playbook

This file defines verified workflows agents should follow when working in this repository. Add new skills here only when the workflow is repeatable, testable, and tied to the constraints in `CONSTRAINTS.md`.

## Core Implementation Standard

New core workflow code should be built around:

- **Pydantic models** for typed, validated contracts.
- **FastAPI** for service/API surfaces.
- **LangGraph** for pipeline orchestration.
- **PostgreSQL** for live operational state.

For every new or materially changed workflow, define the important input,
output, decision, and persistence payloads clearly. Prefer Pydantic models for
contracts that cross module, API, pipeline, database, or notification
boundaries. Use plain dictionaries only for narrow local glue or when migrating
legacy code incrementally.

Keep code readable for the Architect: public modules, pipeline nodes,
non-trivial helpers, scripts, Pydantic models, FastAPI routers, safety gates,
and scoring/filtering logic need docstrings or doc-blocks that explain purpose,
inputs, outputs, side effects, and safety assumptions. Inline comments should
explain domain intent or conservative choices, not obvious syntax.

## Skill Definition Template

Each skill must include:

- **Goal**: The specific outcome.
- **Inputs**: Required data, credentials, and tables.
- **Deterministic Logic**: Step-by-step flow.
- **Safety Checks**: Relevant rules from `CONSTRAINTS.md`.
- **Audit/Logging**: Structured records persisted to PostgreSQL.
- **Verification**: Python tests, SQL checks, pipeline dry-runs, or optional UI checks that prove the workflow worked.

## Verified Skill: Opportunity Assessment

**Goal**: Convert collected listings into reviewable buy candidates without authorizing a purchase.

**Inputs**:

- `auction_listings` rows from the Python PostgreSQL database.
- `latest_comps` materialized view or equivalent comp table.
- Latest `listing_assessments` row when present.
- Niche-specific exclusion rules for parts, accessories, repairs, boxes, manuals, and incomplete items.

**Workflow**:

1. Filter to active/live/open listings with valid URLs.
2. Normalize the listing title and known attributes into a product identity.
3. Reject obvious mismatch cases before LLM assessment.
4. Join to comps by the most specific safe key available.
5. Calculate conservative resale estimate, max bid, expected net profit, and ROI after costs.
6. Assign status:
   - `skip` for invalid, stale, or mismatched listings.
   - `review` for incomplete evidence or low confidence.
   - `candidate` for listings that pass deterministic gates.
7. Persist the structured decision summary and rule flags.
8. Expose candidates through the Python workflow output first; optional UI projections may consume the same PostgreSQL records later.

**Safety Checks**:

- Must comply with `CONSTRAINTS.md` sections "No autonomous buying" and "Buy Desk Safety Gate".
- Any candidate with product identity mismatch must be downgraded to `review` or `skip`.
- No external marketplace write is allowed in this skill.

**Audit/Logging**:

- Persist listing id, comp key, comp sample count, resale estimate, cost assumptions, expected profit, max bid, confidence, red flags, and decision status.
- Persist LLM model, usage, and estimated cost when an LLM is used.

**Verification**:

- Unit tests for ROI, max bid, and exclusion rules.
- SQL spot checks for top candidates to ensure accessories are not valued against whole-product comps.
- SQL or CLI check that candidates expose enough evidence for human approval.
- Optional UI check only if a UI projection is actively being used.

## Verified Skill: Point Zero System Health Check

**Goal**: Confirm the pipeline and database are healthy enough before building or trusting the Buy Desk.

**Inputs**:

- PostgreSQL connection used by the Python app.
- `auction_listings`, `latest_comps`, `listing_assessments`, `sources`, and any scrape/run metadata tables available in the project.
- Pipeline code under `pipelines/listing`.
- Scheduler/heartbeat configuration when present.

**Workflow**:

1. Check database connectivity and table availability.
2. Count listings by source, status, first seen date, last seen date, and finalization state.
3. Answer `0.1`: verify whether ended/sold/final prices are being collected, how many rows have `final_price`, and whether those rows are fresh enough for comps.
4. Answer `0.2`: identify backfill needs for ended prices, missing `model_key`, stale/missing comps, missing assessments, attributes, or source metadata.
5. Answer `0.3`: inspect other blockers, including stale sources, duplicate external ids, broken status transitions, missing timestamps, bad comp groups, unrealistic ROI, missing shipping/cost assumptions, and missing model usage/cost logs.
6. Answer `0.4`: inspect eBay API call efficiency under the daily cap, including call volume by source/pipeline, duplicate searches, refresh cadence, ended-price checks, comp refresh strategy, and whether calls are being spent on the highest-value niches.
7. Produce a short written health report with `healthy`, `needs_backfill`, `api_budget`, and `blocked` sections.
8. Do not change marketplace state.
9. Separate "targeted backfill needed for current live decisions" from "full historical backlog cleanup"; the latter must not block paper trading or Buy Desk design by default.

**Safety Checks**:

- Must comply with `CONSTRAINTS.md` sections "No autonomous buying", "No stealth external writes", and "No unbounded marketplace collection".
- Default mode is read-only.
- Any backfill job discovered by the health check must be proposed separately before it writes data.

**Audit/Logging**:

- Record the date of the health check, counts inspected, backfill recommendations, and blocking issues in PostgreSQL when an audit table exists; otherwise update `PROGRESS.txt` and the relevant planning docs.

**Verification**:

- SQL output or CLI report proves whether ended prices are collected.
- SQL output or CLI report lists concrete backfill gaps.
- API budget report estimates whether the current schedule can fit inside the daily call cap.
- Read-only backfill planning can be run with `python -m pipelines.listing.point_zero`.
- Buy Desk work does not wait for the full stale backlog. It proceeds only when the relevant source/model-key gates, confirmed-comp thresholds, and guardrails for the current niche are resolved or explicitly accepted in `DECISION_LOG.md`.

## Verified Skill: PostgreSQL Backup

**Goal**: Protect the operational price/listing database before health checks, backfills, or scoring changes.

**Inputs**:

- `.env` database settings for the Python/PostgreSQL app.
- `pg_dump` from PostgreSQL 16 or compatible.
- Backup script at `scripts/backup_postgres.sh`.

**Workflow**:

1. Load database connection settings without logging secrets.
2. Create a custom-format compressed `pg_dump`.
3. Write the dump to `backups/postgres/` with restrictive file permissions.
4. Keep partial dumps as `.in_progress` files only while running.
5. Delete dumps older than the configured retention window.
6. Run weekly from cron.

**Safety Checks**:

- Must comply with `CONSTRAINTS.md` section "No credential exposure".
- Backup files must be ignored by git.
- The script must fail if database credentials are missing.

**Audit/Logging**:

- Cron logs to `backups/postgres/backup.log`.
- The script prints backup start, completion path, and retention cleanup messages.

**Verification**:

- Run `pg_restore --list` against a dump to confirm it is readable.
- Periodically perform a restore test into a throwaway database before relying on backups for disaster recovery.

## Verified Skill: Slack Operational Notifications

**Goal**: Send low-risk operator notifications about LangGraph heartbeat status, failures, recoveries, and paper-trading learning state.

**Inputs**:

- `SLACK_NOTIFICATIONS_ENABLED=1`.
- `SLACK_WEBHOOK_URL` stored in `.env` or another secure local environment source.
- Heartbeat metrics from PostgreSQL, including `auction_listings`, `latest_comps`, `api_usage`, `sources`, and `paper_trade_predictions` when available.
- `alert_state` rows used to throttle heartbeat Slack notifications.

**Workflow**:

1. Run the normal Python/LangGraph heartbeat.
2. Build heartbeat metrics from PostgreSQL after pipeline stages complete.
3. Format a compact Slack message covering live listings, API usage, confirmed comp coverage, paper-trading state, and pipeline stage statuses.
4. Include Market Discovery review guidance when available: lane counts, the top research lane, and the next safe review step.
5. Include a review-only auction shortlist from guarded ROI evidence plus stricter paper-trade cost assumptions when available.
6. Suppress operator-declared low-interest or risky rows from the Slack auction digest, currently including Intel-era MacBooks, Pentium/Celeron/Core-era laptops, clearly ancient Apple tablets, and obvious faulty/spares/repair listings.
7. Require fresh live evidence before a row appears in the Slack auction digest: live-like status, future `end_time`, and recent `last_seen_at` within `GF_SLACK_AUCTION_FRESHNESS_MINUTES`.
8. Cap per-source dominance in the auction digest so one source, especially Apple, cannot occupy every slot when other guarded sources have reviewable rows.
9. Include a separate source-scout section for fresh rows from configured scout sources that are not profit-scored yet, with reasons such as no sold comps, thin comps, unknown model key, or above comps.
10. Treat scout rows with unknown model keys as extractor feedback, not as profit evidence. Prefer repairing deterministic title/attribute model-key logic and rebuilding confirmed-price comps before promoting a row into the profit-scored shortlist.
11. Send via `utils.slack_notifier.send_slack_message` only when Slack is explicitly enabled and a webhook URL is present.
12. Record the last successful heartbeat Slack send in `alert_state` using `listing_system_hourly_slack`.
13. Mirror heartbeat failure and recovery alerts to Slack when Slack is enabled, without dumping tracebacks or secrets into Slack.
14. Treat Slack send failures as notification failures only; they must not fail the main heartbeat or alter marketplace state.

**Safety Checks**:

- Must comply with `CONSTRAINTS.md` sections "No credential exposure", "No autonomous buying", and "No stealth external writes".
- Slack is outbound and observer-only. It must not accept commands, trigger bids, buy items, message sellers, relist items, refund buyers, mark shipped, or change any third-party account state.
- The webhook URL is a bearer secret. Do not print it, document it, commit it, or include it in errors.
- Slack messages must not label rows `BUY`, `BID`, or `ACTIONABLE`; paper-trading summaries remain review-only.
- Auction shortlist rows are inspection prompts only. They must use review-only wording and must not imply capital deployment approval.
- Market Discovery Slack guidance is advisory only and must not promote a lane into execution without paper-trade evidence and human approval.

**Audit/Logging**:

- Store heartbeat Slack throttle state in PostgreSQL `alert_state`.
- Return a structured send result with status, HTTP status code, and secret-safe error text.
- Keep detailed heartbeat failures in local logs and failure email; Slack receives a concise alert.

**Verification**:

- Focused unit tests in `tests/test_slack_notifier.py`.
- Slack Market Discovery formatting test in `tests/test_heartbeat_slack_market_discovery.py`.
- Slack review-auction digest tests in `tests/test_opportunity_digest.py`.
- Syntax check for `utils/slack_notifier.py`, `pipelines/listing/main/__init__.py`, and `pipelines/listing/heartbeat/__main__.py`.
- Optional manual test message after configuring the webhook.
- Full test suite with `.venv-mac/bin/python -m unittest discover tests`.

## Verified Skill: Paper Trading Loop

**Goal**: Validate opportunity scoring before committing capital.

**Inputs**:

- Candidate listings from the opportunity assessment skill.
- Final sale outcome or observed ended price.
- Cost assumptions used at decision time.
- PostgreSQL table `paper_trade_predictions`, created by `pipelines.listing.paper_trading`.

**Workflow**:

1. Run ROI scoring with deterministic guardrails.
2. Snapshot the current shortlist into `paper_trade_predictions`, at most once per `external_id` per day.
3. Record current price, resale estimate, selling fees, outbound postage, inbound postage reserve, packaging reserve, selling/relist reserve, expected profit, ROI, conservative review-only max bid, comp samples, and end time.
4. On each run, update open paper predictions from local `auction_listings` state.
5. For watched predictions past `end_time`, spend a small capped Trading API batch to resolve the listing outcome before broad stale backfill.
6. If a confirmed `final_price` exists, treat it as the hypothetical purchase price for the watched auction, not as resale income.
7. Mark whether the system would have bid and would have won by comparing the confirmed final price with the stored max bid.
8. Calculate observed paper profit only for would-have-won rows:
   `predicted_resale_price - predicted_fees - predicted_outbound_shipping - predicted_inbound_shipping - predicted_packaging - predicted_selling_reserve - observed_purchase_price`.
9. Close rows above max bid as disciplined no-buy outcomes rather than profitable or losing trades.
10. If a listing reaches a terminal status without `final_price`, close it as `terminal_without_confirmed_sale` rather than inventing a sale result.
11. Surface totals, watching/closed counts, watched-past-end count, checked-today count, sold count, would-have-won count, above-max-bid count, profitable-on-paper count, and average purchase-vs-max-bid error in the hourly heartbeat email.
12. Generate a deterministic lesson report from persisted predictions:
   - `watching` for rows with no terminal outcome yet.
   - `no_confirmed_sale` for terminal rows without final price.
   - `outbid_above_ceiling` for rows that closed above the stored max bid.
   - `paper_loss`, `overestimated_resale`, `thin_margin`, `underestimated_resale`, or `prediction_ok` for confirmed final-price outcomes.
13. Tune deterministic filters and niche rules based on misses before loosening any buy gate.

**Safety Checks**:

- Paper trading never places bids or purchases.
- Results must be persisted in PostgreSQL before being used to loosen any buy gate.
- No external marketplace writes are performed by this skill.
- Predictions are review-only; they must not be labelled `BUY`, `BID`, or `ACTIONABLE`.
- Outcome resolution uses read-only Trading API calls and local PostgreSQL updates only.
- Overdue outcome checks are capped and cooled down to protect the daily API budget.

**Audit/Logging**:

- Store prediction timestamp, observed outcome timestamp, predicted values, observed values, and error reason.
- Maintain daily uniqueness by `(external_id, prediction_date)` so hourly jobs do not flood the table.
- Lesson reports are derived from `paper_trade_predictions`; they are not execution signals.
- Store outcome check timestamp, count, and last reason for API-audited paper predictions.

**Verification**:

- Weekly report of hit rate, false positives, missed opportunities, and top recurring red flags.
- Focused unit tests for snapshot construction and outcome classification.
- Focused unit tests for lesson classification.
- Focused unit tests for Trading response resolution classification.
- CLI smoke run with `python -m pipelines.listing.paper_trading --limit 25`.

## Planned Skill: Market Discovery

**Goal**: Find and validate new profitable eBay markets before building source adapters or buy signals around them.

**Inputs**:

- eBay search/category ideas from the operator, observed listings, completed listings, and current database gaps.
- Confirmed sold-price evidence, active listing supply, listing velocity, postage/packaging assumptions, platform fee assumptions, and risk notes.
- Existing `auction_listings`, `latest_comps`, `paper_trade_predictions`, and future market-research tables or reports.

**Workflow**:

1. Generate candidate market hypotheses, such as product families, brands, repairable items, bundles, collectibles, or underpriced accessories that can be safely identified.
2. Run read-only research against sold and active listings where API policy and call budget allow.
3. Estimate spread between active buy prices and confirmed sold comps after fees, inbound postage, outbound postage, packaging, expected repair/refurb, return risk, and selling/relist reserve.
4. Score each market for comp depth, supply depth, model-key feasibility, postage complexity, fraud/return risk, condition ambiguity, capital required, and expected resale velocity.
5. Reject markets with weak identity matching, thin comps, high logistics risk, or high false-positive risk.
6. Promote only promising markets into a small monitored source or paper-trading lane.
7. Paper trade the market before any Buy Desk labels are allowed.
8. Document why a market was accepted, rejected, or parked.

**Safety Checks**:

- Market discovery is research-only until a later source/paper-trading lane is explicitly added.
- No autonomous buying, bidding, messaging, relisting, or seller contact is allowed.
- Use API budget deliberately; broad market scans must be capped and prioritized.
- Avoid niches that require unsafe scraping, account-risky behaviour, or marketplace-policy evasion.

**Audit/Logging**:

- Persist or document market hypothesis, search terms/category ids, sample size, comp depth, spread estimate, risk flags, and decision status.
- Record whether the market is `rejected`, `parked`, `research`, `paper_trade`, or `candidate_for_adapter`.

**Verification**:

- Produce a ranked market discovery report before implementing a new source adapter.
- Require at least one focused paper-trading batch before a market can feed Buy Desk candidates.
- First read-only implementation: `python -m pipelines.listing.market_discovery --limit 12 --min-score 20` ranks existing source/model-key lanes from local PostgreSQL evidence only. It does not call eBay or persist marketplace actions.

## Verified Skill: Model-Key Repair and Rebuild

**Goal**: Improve product identity keys and refresh confirmed-price comps without touching marketplace state.

**Inputs**:

- `auction_listings.title`, `source`, `raw_attrs`, existing `model_key`, `status`, `end_time`, and confirmed `final_price`.
- Source-specific key builders under `pipelines/listing/attributes/mk/`.
- `latest_comps`/`comps` built from confirmed `final_price` rows only.

**Workflow**:

1. Audit current comp coverage by total rows, confirmed final-price rows, keyed rows, comp-eligible rows, distinct comp keys, and source.
2. Identify two failure modes: overly broad keys with sold-price spread or accessory/fault contamination, and overly fragmented families where one product title/set/reference is split across many keys.
3. Patch deterministic source-specific key builders with focused regression tests.
4. Dry-run `python -m pipelines.listing.attributes.rebuild_model_keys --all --source <source> --dry-run` for affected sources.
5. When dry-run deltas are explainable, run `python -m pipelines.listing.attributes.rebuild_model_keys --all --limit <row_count>` to recompute existing keys as well as missing keys.
6. Rebuild comps from `auction_listings.final_price` only with `recompute_comps_from_auction_listings`.
7. Re-run coverage and spread audits, and record remaining risky buckets in `PROGRESS.txt`.

**Safety Checks**:

- No bids, buys, seller messages, relists, or marketplace writes are allowed.
- Do not use `price_current` as a sold comp.
- Demoting a previously broad key to `unknown` is safer than preserving contaminated ROI evidence.
- Rebuilds mutate local PostgreSQL state only and must be preceded by tests plus dry-run counts for high-impact changes.

**Audit/Logging**:

- Rebuild command logs scanned rows, changed rows, keys demoted to `unknown`, keys promoted from `unknown`, source filter, and dry-run status.
- Post-rebuild coverage counts and remaining known risks are recorded in `PROGRESS.txt`.

**Verification**:

- Focused tests in `tests/test_model_key_repair.py`.
- Full suite with `.venv-mac/bin/python -m unittest discover tests`.
- SQL checks for key depth and sold-price spread before and after comp rebuild.

## Verified Skill: Buy-Check-Ship Lifecycle

**Goal**: Track a human-approved purchase from candidate through resale outcome.

**Inputs**:

- Human-approved candidate.
- Actual purchase price, inbound postage, inspection result, repair/refurb cost, relist price, outbound postage, and final sale price.

**Workflow**:

1. Human marks a candidate as approved or bought.
2. Record actual purchase cost and source URL.
3. On arrival, record condition, photos checklist, missing parts, and faults.
4. Generate a draft relist description and price recommendation for human review.
5. Record sale outcome and actual profit.
6. Feed the result back into assessment metrics.

**Safety Checks**:

- Human approval is required before purchase and before any relist action.
- Draft listing text is review-only until explicitly approved.

**Audit/Logging**:

- Persist every lifecycle state transition with timestamp, actor, costs, and notes.

**Verification**:

- Reconcile predicted vs actual profit per niche and per model key.
