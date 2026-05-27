# CONSTRAINTS.md

## Hard Guardrails for Agentic Execution

These constraints override any generated plan, model recommendation, or user "vibe" unless the Architect records an explicit override in `DECISION_LOG.md`.

## Universal Hard Nos

- **No autonomous buying**: No agent may place a bid, buy an item, send an offer, accept an offer, or otherwise commit capital without explicit human approval.
- **No stealth external writes**: No agent may create, revise, delete, relist, message, mark shipped, refund, or change a third-party account state without a deterministic safety gate and human approval.
- **No credential exposure**: Secrets must stay in environment variables or secure local configuration. Never hard-code, print, document, or transmit API keys, passwords, OAuth tokens, session cookies, or database credentials.
- **No black-box execution**: Commercial decisions must map to documented rules, structured inputs, and auditable outputs. LLM text alone is not a valid execution reason.
- **No recursive looping**: If a workflow repeats without progress, halt, record the condition in `PROGRESS.txt`, and require operator review.
- **No live fallback to local files**: Live operations may not silently fall back to temp files, CSVs, or SQLite for state that should be in PostgreSQL.

## Buy Desk Safety Gate

Any candidate shown as actionable must pass deterministic checks before being labelled `BUY`, `BID`, or `ACTIONABLE`.

Minimum checks:

- Listing is active/live/open and has a valid item URL.
- Product identity matches the comp group. Accessories, parts, boxes, manuals, spares, repair-only items, and "empty packaging" must not be valued against whole-product comps.
- Expected net profit is positive after purchase price, inbound shipping, outbound shipping, packaging, likely repair/refurb cost, platform fees where applicable, and a risk reserve.
- Comp sample count and freshness meet the niche-specific threshold.
- Max bid is below the conservative resale estimate after costs.
- Confidence and red-flag fields are persisted with the candidate.
- Human approval is required before any real bid or purchase.

## Resource and Cost Limits

- Track model usage and estimated cost for external model calls used in assessment or decisioning.
- Prefer cheap deterministic filters before LLM calls.
- Halt or downgrade to review-only mode if cost tracking is unavailable for a paid model workflow.

## Compliance and Marketplace Conduct

- Respect marketplace terms, rate limits, API policies, robots constraints, and account safety.
- Do not build or run code intended to evade eBay controls, conceal automation, manipulate feedback, scrape aggressively, or bypass payment/listing rules.
- Do not message buyers or sellers automatically without human review and approval.

## Conflict Resolution

If a request conflicts with this file:

1. Stop the execution chain.
2. Cite the specific constraint.
3. Ask for an explicit Architect decision.
4. Record approved overrides in `DECISION_LOG.md` before implementation.

