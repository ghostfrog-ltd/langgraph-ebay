# Decision Log

Only durable project decisions belong here.

## 2026-07-21 - Mission And Mechanism

Decision:

- Number 1 Command: buy cheap from eBay to resell on eBay for profit.
- The project exists to build a side-hustle evidence engine, not analytics for
  its own sake.
- Final-price recovery and product identity quality come before Buy Desk
  automation.
- Candidate validation replaces the vague "paper trading" framing.

Why:

- The historical listing data is valuable only if sold/unsold/stale/dirty rows
  are separated.
- Profit comes from buying below safe resale value, not from dashboards.
- The system must stay focused on finding good buys, avoiding bad buys, and
  learning which markets work.

## 2026-05-31 - Core Stack

Decision:

- Python + PostgreSQL remain the active data engine.
- LangGraph is used for orchestration where it helps.

Why:

- The existing collectors, pipelines, tests, and operational data are already in
  Python/PostgreSQL.
- Rewriting the engine is not the side-hustle priority.

## 2026-05-27 - Safety Boundary

Decision:

- The system may recommend, rank, calculate, explain, and notify.
- It may not bid, buy, message, relist, refund, mark shipped, or otherwise
  change marketplace state without explicit human approval.

Why:

- Marketplace actions spend money and affect account reputation.
