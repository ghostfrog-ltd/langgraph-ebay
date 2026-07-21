# DECISION_LOG.md

## System Memory and Architectural Audit Trail

This file records significant project decisions so the system does not depend on chat memory. New entries should explain the decision, the rationale, and the risk being managed.

## Decision Template

### Decision Title
**Date**: YYYY-MM-DD

**Decision**:
- State the explicit choice.

**Why**:
- Data-backed rationale.
- Risk being mitigated.
- Counter-perspective considered.

## Baseline Decisions

### Prioritize Final-Price Recovery and Market Evidence Before Buy Desk Work
**Date**: 2026-07-21

**Decision**:
- Record the Number 1 Command: "Buy cheap from eBay to resell on eBay for profit."
- Treat the existing multi-month eBay listing history as the core project asset, even where outcome coverage is incomplete.
- Make final-price recovery, ended/no-sale separation, product-identity cleanup, and comp integrity the immediate priority.
- Reframe paper trading as candidate validation, not a trading strategy.
- Do not advance Buy Desk labels until a lane has confirmed final-price comp depth, clean product identity rules, realistic cost assumptions, and enough validation outcomes to support human review.
- Use `OPERATING_DEFINITION.md` as the short project definition for future work.

**Why**:
- The commercial mission needs to be plain enough that future work does not drift into abstract dashboards, unfocused research, or automation theatre.
- Some markets appear to have six to twelve months of observed listing data, which is valuable only if final outcomes can be recovered and separated from stale or dirty rows.
- ROI scoring before outcome recovery creates false confidence, especially when parts or incomplete items inherit whole-product comps.
- The system needs to decide what it is doing before adding more dashboards, alerts, or recommendation layers.
- Human approval remains mandatory for capital deployment and marketplace writes.

### Adopt Pydantic, FastAPI, LangGraph, and PostgreSQL as the Core Stack
**Date**: 2026-05-31

**Decision**:
- Treat Pydantic models, FastAPI, LangGraph, and PostgreSQL as the core technical stack for the operational system.
- Use Pydantic models for durable contracts at important boundaries, including API payloads, pipeline inputs and outputs, decision records, assessment payloads, paper-trade snapshots, and database-facing DTOs where practical.
- Use FastAPI for service/API surfaces, keeping route handlers thin and delegating workflow execution to pipeline/LangGraph services.
- Use LangGraph for pipeline orchestration and PostgreSQL for live operational persistence.
- Deprecate Laravel and Laravel Herd as disposable experiments. Do not add new Laravel dependencies, schedulers, ingestion endpoints, sync jobs, or communication paths.
- Move any useful Laravel behavior into the Python/FastAPI/LangGraph stack before deleting Laravel files or schema remnants.
- Delete the deprecated Laravel application trees and scheduler templates once Python-side communication paths no longer depend on them.
- Require production-readable code with doc-blocks/docstrings for public modules, pipeline nodes, non-trivial helpers, scripts, Pydantic models, FastAPI routers, safety gates, and scoring/filtering logic.

**Why**:
- The system is moving from an experimental script collection into a high-stakes commercial decision-support platform.
- Typed Pydantic contracts make implicit payload shapes explicit, easier to test, and easier for future operators or agents to audit.
- FastAPI provides a clean service boundary without letting a UI framework or Laravel/Herd drive persistence or workflow architecture.
- LangGraph is already the right fit for deterministic, auditable, pipeline-first execution.
- PostgreSQL remains the durable source of truth for operational state.
- Readable, documented code is a safety feature: it reduces the chance of future changes loosening marketplace gates, corrupting data, or hiding commercial assumptions.

### Enable Budgeted Background Final-Price Backfill
**Date**: 2026-05-31

**Decision**:
- Let the fast Python heartbeat spawn a separate final-price backfill worker instead of performing Trading API outcome checks inside the heartbeat process.
- Use a PostgreSQL advisory lock so only one backfill worker can run at a time, whether started by heartbeat or manually.
- Give the worker a dedicated daily local backfill budget, defaulting to 1,000 checked rows/day, with a small per-heartbeat spawn limit, defaulting to 5 rows/tick.
- Keep source/model-key guardrails: Apple first, Samsung second, then other useful lanes; newest useful stale rows first inside each lane; only specific model keys tied to current live comp gaps; and vehicle/parts-heavy sources excluded by default.

**Why**:
- Confirmed final prices are the key missing evidence for buy-side prediction quality and paper-trade validation.
- Apple devices are the strongest current review lane and paper-trading focus, so final-price evidence there has the highest durable prediction value.
- The heartbeat runs every five minutes and must remain quick; long backfill batches would delay freshness, alerts, and normal pipeline work.
- A separate locked worker allows continuous progress without overlapping jobs or spending calls after the dedicated daily budget is exhausted.
- Keeping the planner guardrails prevents final-price backfill from strengthening dirty comp groups or broad parts/accessory buckets.

### Increase Heartbeat Backfill Batch Size
**Date**: 2026-06-03

**Decision**:
- Increase the default heartbeat-spawned final-price backfill batch from 5 rows/tick to 50 rows/tick.
- Keep the dedicated local backfill budget at 1,000 checked rows/day by default.
- Keep the 5,000 daily API cap, 500-call reserve, PostgreSQL advisory lock, source/model-key planner gates, and vehicle/parts-heavy source exclusions unchanged.

**Why**:
- The 5-row/tick default only produced 49 checked rows over the last 24 hours, which is too slow for the current stale-finalization backlog and live comp gaps.
- The LaunchAgent already runs every 5 minutes, so the scheduler cadence is not the primary bottleneck.
- A larger per-spawn batch lets the system catch up when only some heartbeat ticks complete, while the daily budget and API reserve still prevent runaway Trading API usage.
- The planner still spends calls only on stale rows whose model keys support current live comp gaps, so faster backfill remains targeted rather than broad historical cleanup.

### Allow Slack as an Observer-Only Notification Channel
**Date**: 2026-05-28

**Decision**:
- Add Slack incoming-webhook notifications for LangGraph heartbeat status, failures, and recoveries.
- Include a concrete review-only auction shortlist in Slack heartbeat summaries, built from deterministic ROI guardrails and stricter paper-trade cost assumptions.
- Treat Slack as an outbound observer only. Slack messages may summarize pipeline health, paper-trading learning state, API usage, and blockers, but must not accept commands or trigger bids, buys, messages, relists, refunds, or any marketplace write.
- Keep Slack disabled unless explicitly configured with `SLACK_NOTIFICATIONS_ENABLED=1` and a webhook URL stored outside code.
- Rotate the Slack webhook if it is ever exposed in chat, logs, docs, or commits.

**Why**:
- The operator needs a lightweight way to follow what the pipeline is doing without relying only on email or local logs.
- The operator also needs to see actual auctions worth manual inspection before the full Buy Desk is ready; showing review-only rows makes the system useful while preserving safety gates.
- Slack notifications are low-risk compared with marketplace writes, but the webhook is still an external state-changing call and a bearer secret.
- Keeping Slack observer-only preserves the human approval hierarchy and avoids turning chat into an execution surface.
- A live test message succeeded on 2026-05-28, confirming the configured workspace/channel can receive pipeline notifications.

### Build a Market Discovery Workstream
**Date**: 2026-05-28

**Decision**:
- Add a first-class workstream for discovering new profitable eBay markets, not just scoring the niches already being collected.
- Treat market discovery as a research and paper-trading problem first: propose niches, measure sold-price depth, active supply, spread, fees, shipping complexity, return risk, and model-key feasibility before adding them to buy signals.
- Do not rely on vibes or one-off examples. A new market should graduate only after deterministic evidence, documented assumptions, and paper-trade validation.

**Why**:
- The system's long-term value depends on finding repeatable profitable lanes, not only monitoring the current Apple/device, console, vehicle, and collectible sources.
- eBay profit pools move; the pipeline needs a way to explore, rank, and reject markets deliberately.
- Unresearched niches can create false confidence because they may have poor comps, fragile items, high postage, scams, parts/accessory ambiguity, or low resale velocity.
- A discovery workflow keeps exploration useful while preserving the no-autonomous-buying and evidence-first safety model.

### Use Purchase-Side P&L for Paper Trading
**Date**: 2026-05-28

**Decision**:
- Treat a watched auction's confirmed eBay final price as the hypothetical purchase price, not as resale income.
- Store paper reserves for inbound postage, packaging, and selling/relist risk in addition to resale-platform fees and outbound postage.
- Mark whether the stored max bid would have won the auction.
- Calculate observed paper profit only when the final price stayed within max bid.
- Close auctions above max bid as disciplined no-buy outcomes.

**Why**:
- The previous paper outcome calculation could make an ended auction look like resale revenue, which overstated learning quality.
- Real buy-check-ship economics need purchase price, inbound postage, outbound postage, fees, packaging, and selling/relist risk before a candidate can be trusted.
- Above-max-bid outcomes are useful evidence, but they should not count as profitable or losing trades because the system should not have bought them.

### Persist Paper-Trade Predictions Before Buy Desk Labels
**Date**: 2026-05-27

**Decision**:
- Snapshot deterministic ROI shortlist candidates into `paper_trade_predictions` before treating any recommendations as buy-desk-ready.
- Update those snapshots from local PostgreSQL listing outcomes, treating confirmed final prices as hypothetical purchase prices and comparing paper profit against the original resale/cost assumptions.
- Resolve overdue watched predictions with a small capped Trading API batch before broad historical stale backfill, because paper-trading outcomes directly improve active scoring.
- Run the paper-trading loop after ROI in the main heartbeat, and surface its metrics in the hourly operations email.
- Generate deterministic lesson categories from persisted predictions, but treat those lessons as review guidance rather than execution signals.

**Why**:
- The system needs measured prediction outcomes before real capital deployment.
- Paper trading lets us learn from candidate quality, comp accuracy, and guardrail misses without bidding, buying, messaging, or changing marketplace state.
- Persisted predictions make future tuning auditable instead of relying on chat memory or transient logs.
- Lesson categories make repeated misses visible quickly while preserving the hard rule that no capital or marketplace write follows automatically.
- A capped resolver spends scarce API calls on the most valuable learning rows first: watched candidates whose own end time has passed.

### Run Weekly PostgreSQL Backups
**Date**: 2026-05-27

**Decision**:
- Run a weekly compressed `pg_dump` backup of the primary Python/PostgreSQL database.
- Store backups under `backups/postgres/`, ignore them from git, and retain 180 days by default.
- Schedule the backup through local cron at `03:15` every Sunday.

**Why**:
- Historical listings, price history, ROI snapshots, comps, and assessments are becoming the project moat.
- Point Zero/backfill work may touch valuable operational data, so recoverability must exist before deeper mutation work.
- The current database is around 1.3 GB but compresses to roughly 80 MB in custom dump format, so weekly retention is cheap.

### Use Confirmed Final Prices Only for Comps
**Date**: 2026-05-27

**Decision**:
- Build `comps` only from rows with `final_price IS NOT NULL`.
- Do not use `price_current` as a fallback realized sale price for `ended` rows.
- Plan stale backfills using spare API budget, but spend calls only on model keys that are specific enough to improve current live opportunities.
- Do not treat completion of the full stale backlog as a prerequisite for useful opportunity work.

**Why**:
- `price_current` is not a realized sale price.
- Using `COALESCE(final_price, price_current)` allowed ended/no-sale rows to contaminate resale estimates.
- Bad comps directly create bad max bids and false Buy Desk candidates.

### Treat Backfill as Incremental, Not a Global Blocker
**Date**: 2026-05-27

**Decision**:
- The full historical stale backlog is not a blocking prerequisite for Buy Desk or paper-trading progress.
- Backfill should run as an incremental enrichment loop that prioritizes current live niches, specific model keys, and comp gaps that affect active decisions.
- Readiness gates should be based on source/model-key quality, confirmed final-price comp coverage, parts/accessory guardrails, and paper-trading evidence, not on processing every stale row.

**Why**:
- The stale backlog is roughly 80k+ rows, but the immediately useful API spend is concentrated in a much smaller pool tied to current live opportunities.
- Waiting for complete historical backfill would delay learning while adding many low-value calls.
- A targeted backfill loop lets the system improve continuously while still protecting API budget and avoiding unsafe broad comp groups.

### Require Production-Readable Code
**Date**: 2026-05-27

**Decision**:
- Treat readability, doc-blocks, and useful comments as a project constraint, not a nice-to-have.
- New or materially changed public modules, pipeline nodes, non-trivial helpers, scripts, safety gates, and scoring/filtering logic must document purpose, inputs, outputs, side effects, and safety assumptions.
- Comments should explain domain-specific or intentionally conservative logic, not restate obvious syntax.

**Why**:
- This project is becoming a commercial decision-support system where future audits need to be fast and reliable.
- Hidden assumptions in scoring, backfill, model-key extraction, or marketplace-adjacent logic can create bad buy recommendations.
- Clear code reduces the chance that future agents or operators accidentally loosen safety gates.

### Run a Point Zero Health Check Before Buy Desk Build
**Date**: 2026-05-27

**Decision**:
- Before building the Buy Desk, run a system health check over the Python/LangGraph pipeline and PostgreSQL data.
- The health check must answer:
  - `0.1` Are we collecting ended/sold prices reliably enough to trust comps and paper trading?
  - `0.2` Do we need to backfill ended prices, missing attributes, model keys, comps, or assessments?
  - `0.3` What else is blocking reliable opportunity scoring, including stale sources, broken status transitions, bad comp groups, duplicate listings, missing timestamps, cost assumptions, model usage logs, or scheduler failures?
  - `0.4` Are we maximizing eBay API call value under the daily call cap, including deduping searches, reducing unnecessary refreshes, and prioritizing high-value sources?

**Why**:
- Building a Buy Desk on unhealthy data would make false confidence worse.
- Ended/sold prices are the backbone for comps, ROI, max bid, and paper trading.
- The current data already shows false positives where accessories can be valued against whole-product comps, so data health must come before more automation.
- A daily API limit around 5,000 calls is tight for multi-source collection, comp refresh, end-price checks, and backfills, so call budget must be treated as an operational resource.

### Keep Python/LangGraph as the Primary System
**Date**: 2026-05-27

**Decision**:
- Treat the Python/LangGraph app and PostgreSQL operational schema as the primary system.
- Laravel was treated as a side project/operator UI idea, but as of 2026-05-31 it is deprecated and disposable. It must not define core pipeline architecture, persistence, API surfaces, notifications, or communication paths.

**Why**:
- The original project goal is data collection, assessment, and buy-check-ship decision support.
- The working operational data model already lives around Python pipeline tables such as `auction_listings`, `latest_comps`, and `listing_assessments`.
- Removing Laravel/Herd from the core path prevents UI/product experiments from steering automation design or creating duplicate sync surfaces.

### Adopt a Reliability Stack for Agent Steering
**Date**: 2026-05-27

**Decision**:
- Govern this project with `AGENTS.md`, `CONSTRAINTS.md`, `DECISION_LOG.md`, `SKILL.md`, `SCHEMA.md`, and `PROGRESS.txt`.

**Why**:
- The project is moving from collection toward commercial decision support.
- Buying and relisting workflows involve money, external accounts, and reputation risk.
- Persistent rules reduce the chance that future agents repeat known mistakes or treat old chat context as authoritative.

### Keep Execution Deterministic and Auditable
**Date**: 2026-05-27

**Decision**:
- Use deterministic pipeline gates for ingestion, filtering, scoring, and execution readiness.
- Use LLMs only for structured extraction, risk commentary, comp sanity checks, and operator-facing explanations unless a later decision expands the scope.

**Why**:
- The current opportunity feed can produce false positives when parts/accessories are valued against whole-item comps.
- Rule-backed filtering is easier to test, audit, and tune than free-form model judgement.
- LLMs remain valuable for catching nuance, but they should not be the sole source of a buy decision.

### Require Human Approval for Capital and Marketplace Writes
**Date**: 2026-05-27

**Decision**:
- The system may recommend, rank, and explain opportunities.
- The system may not bid, buy, list, revise, message, refund, or mark shipped without explicit human approval and a safety gate.

**Why**:
- Marketplace actions can spend money, damage account standing, or create contractual obligations.
- Human approval keeps the workflow useful while the scoring model is still being calibrated.

### Build Toward a Buy-Check-Ship Operating Loop
**Date**: 2026-05-27

**Decision**:
- Treat the product direction as a buy-check-ship loop:
  - Buy: identify candidates with believable profit and max bid.
  - Check: inspect received items and record actual condition/costs.
  - Ship: relist, package, dispatch, and track outcomes.
  - Learn: compare predicted profit against actual profit.

**Why**:
- Collected listings only create value when converted into decisions and feedback.
- The system needs actual outcome data to improve future scoring.
- Paper trading should precede real capital deployment for newly tuned niches.
