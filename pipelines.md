# Pipelines

## Active Mechanism

1. Retrieve listings.
2. Mark ended/sold outcomes.
3. Recover final prices for stale valuable rows.
4. Extract product identity/model keys.
5. Rebuild confirmed-price comps.
6. Score live listings against safe max buy price.
7. Produce review shortlist.
8. Validate later outcomes.

## Main Heartbeat

The heartbeat should stay short and predictable. Long backfills should be
separate locked workers.

## Priority Fixes

- Add indexes or query changes for slow heartbeat reads.
- Improve parts/accessory/repair filters.
- Prioritize final-price recovery by market/model key value.
