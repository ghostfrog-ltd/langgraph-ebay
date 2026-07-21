# Workflows

Keep this file short. Detailed behaviour belongs in code and tests.

## Final-Price Recovery

Recover confirmed sold prices and ended/no-sale outcomes from stale or overdue
listings. Spend API calls first on markets/model keys that can improve current
buy decisions.

## Product Identity

Build and repair model keys so listings compare like-for-like. Parts,
accessories, repair-only items, and incomplete products must not inherit
whole-product comps.

## Comps

Build resale estimates only from confirmed `final_price` rows. Never use current
listing price as a sold price.

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
