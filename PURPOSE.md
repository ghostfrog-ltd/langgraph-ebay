# Purpose

## Number 1 Command

Buy cheap from eBay to resell on eBay for profit.

## Mechanism

Use historical eBay evidence to find live listings priced below safe resale
value.

The system should:

1. Collect listings in selected eBay markets.
2. Recover confirmed final sold prices.
3. Group listings by real product identity.
4. Block junk: parts, spares, repairs, boxes, manuals, accessories, scams,
   locked devices, and incomplete products.
5. Build comps only from confirmed sold prices.
6. Calculate a safe max buy price after eBay fees, postage, packaging,
   repair/refurb reserve, risk reserve, and target profit.
7. Produce a small human review shortlist.
8. Record manual buy/check/resell outcomes so the system learns which markets
   work.

## Current Focus

Final-price recovery comes first. The multi-month listing history is valuable,
but only after sold, unsold, stale, and dirty rows are separated.

Candidate validation is useful only as a test bench: would the shortlist have
worked if a human had acted?

## Boundary

The software may recommend, rank, calculate, explain, and notify.

It must not bid, buy, message, relist, refund, mark shipped, or otherwise change
third-party marketplace state without explicit human approval.
