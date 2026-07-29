# Workflows

Keep this file short. Detailed behaviour belongs in code and tests.

## Final-Price Recovery

Recover confirmed sold prices and ended/no-sale outcomes from unknown or overdue
listings whose end time has passed. Spend API calls first on markets/model keys
that can improve current buy decisions.

If eBay Trading says an item is inaccessible, not found, or deleted and no
confirmed final price is available, delete the local listing row. It is junk for
the mission and must not become a comp, history row, or retry target.

## Product Identity

Build and repair model keys so listings compare like-for-like. Parts,
accessories, repair-only items, and incomplete products must not inherit
whole-product comps.

For Apple/Mac keys, keep product identity strict and condition separate. Mac
identity is line/chassis/screen/CPU/RAM/SSD. If CPU/RAM/SSD is missing or
conflicting, use `unknown` rather than guessing.

## Comps

Build resale estimates only from confirmed `final_price` rows for the same
`model_key` and `condition_grade`. Never use current listing price as a sold
price.

## Live Potentials

Use the live-listing potentials query as the core operator output:

```bash
.venv-mac/bin/python -m pipelines.listing.market_discovery --limit 20 --min-profit 50 --min-comps 3
```

This report compares each live listing against confirmed sold comps for the same
clean `model_key` and `condition_grade`, then prints current price, sold median,
comp count, max safe buy, expected profit, ROI, and a simple signal string.

Do not replace this with abstract lane/status language. The human needs the
maths and closest misses, then decides whether to punt.

## Candidate Validation

Formerly "paper trading". Snapshot clean review candidates, then compare later
confirmed outcomes against the stored max buy price and cost assumptions. This
is evidence gathering, not permission to buy.

## Shortlist

Produce a small list of live listings worth human inspection. Quality beats
volume.

## Notifications

Email/Slack may report status and review candidates. They must remain
observer-only.
