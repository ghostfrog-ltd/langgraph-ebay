
## V2 - Ebay / LangGraph pipelines

#### Graph 1: “Ingest listings” 
This one is basically ETL:
Start → For each niche → Query API → Normalize → Deduplicate → Save to DB → End
That’s a perfect first LangGraph, because it’s deterministic and easy to test.

#### Graph 2: “Assess listings” (LLM optional)
This is where “AI” may appear, but it’s not the core:
Start → Fetch unassessed from DB → Quick rules → (Optional) LLM judge → Risk score → Write assessment → End

#### Graph 3: “Notify”
Start → Fetch ‘actionable’ items → Apply user filters → Format message → Send Telegram → End

#### Graph 4: “Buy Desk” (human approval required)
Start → Fetch candidates → Apply deterministic safety gate → Calculate net profit/max bid → Attach evidence and red flags → Write candidate decision → End

This graph may recommend or rank opportunities. It must not bid, buy, message, or relist. See `CONSTRAINTS.md`.

#### Graph 5: “Paper Trade”
Start → Snapshot top candidates → Wait for outcome/ended price → Compare predicted vs observed result → Record miss reason → Tune rules → End

This graph validates the economics before real capital is used.

#### Graph 6: “Buy Check Ship”
Start → Human marks bought → Record actual costs → Inspect/check item → Draft relist package → Record sale outcome → Feed learning data back into assessment → End

All marketplace writes remain human-approved unless a future decision in `DECISION_LOG.md` explicitly changes the gate.


## V1 - Heart beat 

1. Close Ended
2. Scrape Sources
3. Refresh Comps
4. Attributes Back Fill
5. Scan for Hot Listings
6. ROI Alerts
7. New Listing Alerts
