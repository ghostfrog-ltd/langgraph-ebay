
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


## V1 - Heart beat 

1. Close Ended
2. Scrape Sources
3. Refresh Comps
4. Attributes Back Fill
5. Scan for Hot Listings
6. ROI Alerts
7. New Listing Alerts