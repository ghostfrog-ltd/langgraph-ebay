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

