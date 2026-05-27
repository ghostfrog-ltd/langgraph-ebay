# SKILL.md

## Capability Playbook

This file defines verified workflows agents should follow when working in this repository. Add new skills here only when the workflow is repeatable, testable, and tied to the constraints in `CONSTRAINTS.md`.

## Skill Definition Template

Each skill must include:

- **Goal**: The specific outcome.
- **Inputs**: Required data, credentials, and tables.
- **Deterministic Logic**: Step-by-step flow.
- **Safety Checks**: Relevant rules from `CONSTRAINTS.md`.
- **Audit/Logging**: Structured records persisted to PostgreSQL.
- **Verification**: Tests, SQL checks, or UI checks that prove the workflow worked.

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
8. Show candidates in Laravel for human review.

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
- Laravel UI check that candidates expose enough evidence for human approval.

## Verified Skill: Paper Trading Loop

**Goal**: Validate opportunity scoring before committing capital.

**Inputs**:

- Candidate listings from the opportunity assessment skill.
- Final sale outcome or observed ended price.
- Cost assumptions used at decision time.

**Workflow**:

1. Snapshot top candidates at decision time.
2. Record the predicted max bid, resale estimate, expected profit, and confidence.
3. Track whether the listing ended, sold, relisted, or disappeared.
4. Compare predicted profit against observed outcome.
5. Tune deterministic filters and niche rules based on misses.

**Safety Checks**:

- Paper trading never places bids or purchases.
- Results must be persisted in PostgreSQL before being used to loosen any buy gate.

**Audit/Logging**:

- Store prediction timestamp, observed outcome timestamp, predicted values, observed values, and error reason.

**Verification**:

- Weekly report of hit rate, false positives, missed opportunities, and top recurring red flags.

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

