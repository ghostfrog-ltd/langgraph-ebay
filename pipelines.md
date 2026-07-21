
## V2 - Ebay / LangGraph pipelines

#### Graph 0: “Point Zero Health Check”
Start → Inspect database/schema → Check source freshness → Verify ended/sold price collection → Identify backfill gaps → Inspect comp quality and scoring blockers → Audit API call efficiency → Write health report → End

This graph runs before Buy Desk work. It is read-only by default and must answer:

0.1. Are we collecting ended/sold prices reliably enough to trust comps and paper trading?
0.2. Do we need to backfill ended prices, attributes, model keys, comps, assessments, or source metadata?
0.3. What else blocks reliable opportunity scoring, including stale sources, duplicate listings, bad comp groups, broken status transitions, missing timestamps, missing cost assumptions, scheduler failures, or missing model cost logs?
0.4. Are we maximizing eBay API call value under the daily call cap, including deduped searches, refresh cadence, backfill strategy, source priority, and avoiding low-value calls?

#### Graph 1: “Ingest listings” 
This one is basically ETL:
Start → For each niche → Query API → Normalize → Deduplicate → Save to DB → End
That’s a perfect first LangGraph, because it’s deterministic and easy to test.

#### Graph 2: “Assess listings” (LLM optional)
This is where “AI” may appear, but it’s not the core:
Start → Fetch unassessed from DB → Quick rules → (Optional) LLM judge → Risk score → Write assessment → End

#### Graph 3: “Notify”
Start → Build operational summary → Apply notification throttles → Format message → Send email/Slack → End

Notifications are observer-only. The current heartbeat sends an HTML operations email and, when configured, a compact Slack summary with pipeline stage status, API usage, confirmed comp coverage, and paper-trading state. Notification channels must not accept commands or trigger bids, purchases, messages, relists, refunds, shipping updates, or any other third-party write.

#### Graph 4: “Buy Desk” (human approval required)
Start → Fetch candidates → Apply deterministic safety gate → Calculate net profit/max bid → Attach evidence and red flags → Write candidate decision → End

This graph may recommend or rank opportunities. It must not bid, buy, message, or relist. See `CONSTRAINTS.md`.

#### Graph 5: “Paper Trade”
Start → Snapshot top candidates → Wait for outcome/ended price → Treat final auction price as hypothetical purchase price → Compare max bid and full paper P&L → Record miss reason → Tune rules → End

This graph validates the economics before real capital is used.

Implemented as `pipelines.listing.paper_trading`. It runs after ROI in the main heartbeat, writes review-only predictions to PostgreSQL, updates observed outcomes from `auction_listings`, resolves watched predictions past `end_time` with a small capped Trading API batch, and reports totals plus deterministic lesson categories in the heartbeat email and configured Slack summary. It does not bid, buy, message, relist, or write to eBay.

Paper-trade P&L is purchase-side:

- `final_price` from the watched auction is treated as the would-have-paid purchase price.
- Expected resale comes from confirmed sold comps.
- Cost stack includes predicted fees, outbound postage, inbound postage, packaging, and selling/relist reserve.
- Observed paper profit is calculated only when the final price stayed within the stored max bid.
- Auctions above max bid become `outbid_above_ceiling`, a disciplined no-buy outcome.

#### Graph 6: “Buy Check Ship”
Start → Human marks bought → Record actual costs → Inspect/check item → Draft relist package → Record sale outcome → Feed learning data back into assessment → End

All marketplace writes remain human-approved unless a future decision in `DECISION_LOG.md` explicitly changes the gate.

#### Graph 7: “Market Discovery” (planned)
Start → Generate market hypotheses → Sample active and sold listings → Estimate post-cost spread → Score supply/comp depth/risk/model-key feasibility → Reject, park, or promote to paper-trading lane → End

This graph is important because the system should not only score known lanes; it should deliberately discover new profitable eBay markets. Discovery remains research-only until a market has enough sold evidence, active supply, clear product identity, manageable postage/returns risk, and a paper-trading validation path. It must not bid, buy, message, relist, or perform broad uncapped API scans.


## V1 - Heart beat 

1. Close Ended
2. Scrape Sources
3. Refresh Comps
4. Attributes Back Fill
5. Scan for Hot Listings
6. ROI Alerts
7. New Listing Alerts
