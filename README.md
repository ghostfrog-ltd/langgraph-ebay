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