# LangGraph eBay — Orchestrated Listing Pipelines

This repository contains the **V2 rebuild** of the GhostFrog eBay ingestion system, using **LangGraph** as the orchestration layer.

The goal is not to build a single scraper script, but a **reliable, inspectable pipeline system** that can safely evolve into AI-assisted assessment, ranking, and alerting.

---

## Why This Exists

The original eBay implementation (V1) proved the concept:
- listings could be fetched
- data had value
- the idea worked

However, V1 was script-based:
- control flow was implicit
- scheduling logic leaked into business logic
- extending the system safely became harder over time

This repository represents a **deliberate architectural reset**.

Instead of adding more logic to V1, the system is being rebuilt around:
- explicit pipelines
- explicit state
- explicit termination
- orchestration-first design

---

## Reliability Stack

This repository now includes an agentic reliability stack adapted from `ghostfrog-ltd/agentic-blueprint`.

These files are part of the project contract:

- [AGENTS.md](AGENTS.md): roles, operating protocol, and agent permissions.
- [CONSTRAINTS.md](CONSTRAINTS.md): hard guardrails for buy, bid, listing, messaging, persistence, and cost tracking.
- [DECISION_LOG.md](DECISION_LOG.md): architectural memory and explicit project decisions.
- [SKILL.md](SKILL.md): verified workflows for opportunity assessment, paper trading, and buy-check-ship lifecycle tracking.
- [SCHEMA.md](SCHEMA.md): persistence contracts for audit logs, model costs, candidates, and lifecycle events.
- [PROGRESS.txt](PROGRESS.txt): lightweight state marker for significant automated work.

The key rule is simple: AI can recommend, rank, explain, and draft. It must not bid, buy, relist, message, refund, or otherwise mutate external marketplace state without a deterministic safety gate and explicit human approval.

---

## Current Handoff

Latest operating state is captured in [PROGRESS.txt](PROGRESS.txt) and the Point Zero report at [POINT_ZERO_HEALTH_CHECK.md](POINT_ZERO_HEALTH_CHECK.md).

As of 2026-05-28:

- PostgreSQL is the source of truth; Laravel/Herd is deprecated and disposable.
- Weekly PostgreSQL backups are configured.
- Comps use confirmed `final_price` only.
- Targeted backfill exists and is incremental; the full stale backlog is not a Buy Desk blocker.
- Vehicle-like sources are review-only for ROI unless explicitly enabled.
- Apple/device listings are the first paper-trading lane.
- Paper trading persists review-only predictions in PostgreSQL and runs after ROI in the heartbeat.
- Paper P&L is purchase-side: a watched auction's confirmed `final_price` is treated as the would-have-paid purchase price, not resale revenue.
- Paper costs include fees, outbound postage, inbound postage reserve, packaging reserve, and selling/relist reserve.
- Slack notifications are wired into the Python/LangGraph heartbeat as observer-only operational summaries, Market Discovery review guidance, and failure/recovery alerts.
- No row may be labelled `BUY`, `BID`, or `ACTIONABLE` until closed paper outcomes produce enough evidence to tune rules.

Next practical session:

1. Let the current Apple/device paper predictions pass `end_time`.
2. Run the capped paper resolver to close overdue predictions.
3. Review lesson categories, especially `outbid_above_ceiling`, `paper_loss`, `thin_margin`, and `prediction_ok`.
4. Tighten Apple/device condition, cost, and model-key rules from observed misses.
5. Start designing the Market Discovery workstream: a way to find, measure, reject, and paper-trade new profitable eBay niches.
6. Only then start Buy Desk persistence/UI work.

Important strategic task: this system must learn how to explore new profitable markets on eBay, not only monitor the markets already configured. The planned Market Discovery workflow should rank candidate niches by sold-comp depth, active supply, post-cost spread, model-key feasibility, postage/return risk, capital required, and suitability for paper trading.

Useful commands:

```bash
.venv-mac/bin/python -m pipelines.listing.paper_trading --limit 25
.venv-mac/bin/python -m pipelines.listing.paper_trading --limit 25 --no-resolve
.venv-mac/bin/python -m pipelines.listing.market_discovery --limit 12 --min-score 20
.venv-mac/bin/python -m unittest discover tests
```

Slack notifications:

The Python/LangGraph heartbeat can post a compact hourly status update to a Slack
incoming webhook. Slack is observer-only: it cannot bid, buy, message, relist,
or mutate eBay state. The local webhook has been tested successfully, and future
heartbeat runs will post according to `GF_HEARTBEAT_SLACK_MINUTES`.

Heartbeat Slack summaries include Market Discovery counts, the top research lane
when one exists, and the next safe review step. They must remain advisory and
must not label rows `BUY`, `BID`, or `ACTIONABLE`.

```bash
SLACK_NOTIFICATIONS_ENABLED=1
SLACK_WEBHOOK_URL=https://hooks.slack.com/services/...
GF_HEARTBEAT_SLACK_MINUTES=60
```

The webhook URL is a secret and must stay in `.env` or another secure local
environment source. If the webhook URL is ever pasted into chat, logs, or a
shared document, rotate it in Slack and update the local `.env`.

---

## What This Repo Is (and Is Not)

### This **is**:
- a LangGraph-orchestrated pipeline system
- designed for safe iteration over multiple data sources
- gated to avoid hammering external APIs
- built to be triggered by a simple heartbeat (cron, loop, scheduler)

### This is **not**:
- an autonomous agent
- a long-running daemon
- a monolithic script
- a hype-driven “AI bot”

Those come *after* the foundations are solid.

---

## Core Concepts

### Pipelines, Not Scripts
Each pipeline:
- runs once
- mutates explicit state
- exits cleanly
- can be called by anything (CLI, cron, another pipeline, API)

### Orchestration Before AI
AI-based assessment, ranking, and automation only make sense once:
- ingestion is reliable
- execution is predictable
- failure modes are visible

LangGraph provides the structure needed to make that possible.

---

## Current Pipelines

### Retrieve Pipeline (V2)

**Purpose:**  
Safely retrieve listings from multiple sources.

**What it does:**
1. Iterates through a defined list of adapters (sources)
2. Applies per-source time-based gating
3. Either:
   - skips the source (with a reason and next allowed run time), or
   - runs the adapter and records the scrape
4. Stops once all sources are processed

**What it does not do (yet):**
- no retries
- no AI
- no ranking
- no notifications

This restraint is intentional.

---

## Architecture Direction

The Laravel/Herd experiment is deprecated. The Python/FastAPI/LangGraph system
is the primary app, and PostgreSQL is the operational source of truth.

Current direction:

- Python remains the ingestion and analysis engine
- PostgreSQL remains the operational source of truth
- FastAPI is the service/API surface for new operator workflows
- Laravel/Herd should not be used for sync, notifications, ingestion endpoints,
  schedulers, or operator communication
- core decisions, scoring, assessment, and buy-check-ship workflow belong in the Python pipeline first

The old Laravel application files have been removed. Any future UI or operator
API should be built in Python/FastAPI.
