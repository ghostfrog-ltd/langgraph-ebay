# GhostFrog eBay

Mission: **buy cheap from eBay to resell on eBay for profit**.

This repo is the data engine for that side hustle. It collects eBay listings,
recovers final prices, builds product-and-condition comps, filters bad listings,
calculates safe max buy prices, and prints live listing potentials plus closest
misses.

Read first: [PURPOSE.md](PURPOSE.md).

## Stack

- Python pipeline engine
- PostgreSQL operational database
- LangGraph orchestration where useful

## Current Priority

1. Recover final sold prices in useful markets.
2. Keep product identity, condition grades, and parts/accessory filters clean.
3. Rebuild comps from confirmed sold prices only.
4. Run the live-listing potentials query.
5. Let the human decide whether near-misses are worth a punt.

The core report is not an LLM judgement. It is plain maths:

`listing | model_key | condition_grade | current_price | sold_median | comp_count | max_safe_buy | expected_profit | signal`

Default gate: expected profit >= £50 and at least 3 confirmed sold comps for the
same clean `model_key` and condition grade. ROI is displayed as context, not
used as the default blocker.

## Safety

The system can recommend and explain. It must not bid, buy, message, relist, or
change eBay/account state without explicit human approval.

## Useful Commands

```bash
ebay
ebay stats
ebay serious
ebay punt
ebay backfill-plan
ebay backfill --limit 10000 --daily-limit 10000
.venv-mac/bin/python -m unittest discover tests
.venv-mac/bin/python scripts/run_migrations.py --dry-run
.venv-mac/bin/python -m pipelines.listing.market_discovery --limit 20 --min-profit 50
.venv-mac/bin/python -m pipelines.listing.market_discovery --limit 20 --min-profit 50 --min-comps 3
.venv-mac/bin/python -m pipelines.listing.text_condition --queue-once --status all --enqueue-limit 1000
scripts/run_text_condition_once.sh
.venv-mac/bin/python -m pipelines.listing.paper_trading --limit 25 --no-resolve
```

Exploratory CLI opportunity reports use category-specific comp floors: Apple 10,
Samsung 5, graphics cards 5, consoles 15, action cameras 15, LEGO 15, and
fallback 15. Passing `--min-comps N` is an explicit global override for
diagnostics. Heartbeat email/Slack buy-signal lists use the stricter
`GF_HEARTBEAT_BUY_SIGNAL_MIN_COMPS` floor, defaulting to 50 comps, so
low-evidence exploratory rows do not appear as purchase prompts.

The repo also keeps the wrapper at `./ebay`. Plain `ebay` works when
`/opt/homebrew/bin/ebay` points to that wrapper.

```bash
/opt/homebrew/bin/ebay -> /Volumes/Bob/www/ghostfrog-langgraph-ebay/ebay
```

## Evidence Website

The authenticated Laravel + Livewire app lives in `web/`. OrbStack runs the app
and its separate PostgreSQL database locally:

```bash
cd web
docker compose up --build -d
open http://127.0.0.1:8080/register
```

The website never connects back to the operational database. A local read-only
publisher mirrors live priced rows, removes raw/seller fields, attaches
confirmed same-key/same-grade sold evidence where available, signs the payload,
and pushes a complete snapshot to Laravel. The broad historical sold evidence
mirror is separate, so Search can inspect old confirmed outcomes even when they
are not attached to a current live listing:

```bash
ebay web-sync
ebay web-sync --sold-evidence-only
ebay web-sync --endpoint http://127.0.0.1:8080 \
  --secret local-development-sync-secret-change-me --send
```

To keep the local Docker Laravel app and production display site warm:

```bash
scripts/install_web_sync_launch_agent.sh
```

That installed runner publishes the live listing snapshot to local Docker first
and then production every 5 minutes. It refreshes the broad historical sold
evidence mirror at most once per hour. The 3D print seller leaderboard process
is disabled by default.

For a remote deployment, set `GHOSTFROG_WEB_URL` and
`GHOSTFROG_SYNC_SECRET` locally, set the matching secret on Laravel, and run
`ebay web-sync --send`. Public registration is disabled by default outside the
local OrbStack compose setup.

To quickly refresh one or more model keys in both local Laravel and production,
run:

```bash
ebay push apple-macbook-pro-a2442-14in-m1pro-16gb-512gb
```

The command always pushes to local first, then pushes the same model-key comp
summary to `https://ebay.ghostfrog.co.uk`. If a production sync secret is not
already present in `GHOSTFROG_PUSH_PROD_SYNC_SECRET` or
`GHOSTFROG_PROD_SYNC_SECRET`, it reads the secret from the deployed Laravel
environment over the configured SSH deploy connection without printing it. Use
`GHOSTFROG_PUSH_LOCAL_URL`, `GHOSTFROG_PUSH_PROD_URL`,
`GHOSTFROG_PROD_SSH_HOST`, `GHOSTFROG_PROD_SSH_KEY`, or
`GHOSTFROG_PROD_APP_DIR` only when overriding the defaults.

To refresh all confirmed sold evidence in both local Laravel and production,
run:

```bash
ebay evidence-sync
```

This sends every local confirmed final-price row as an upsert. The scheduled
web-sync runner also publishes this sold-evidence mirror periodically before
refreshing live listings.

To refresh the full live listing snapshot in both local Laravel and production,
run:

```bash
ebay web-sync-all
```

The 3D print seller leaderboard is preserved for history but disabled by
default. To opt back into the manual local/prod refresh, run:

```bash
GF_3D_PRINT_LEADERBOARD_ENABLED=1 ebay 3d-print-leaderboard-sync --send
```

Google OAuth login/register is available in the Laravel app via Socialite. Add
`GOOGLE_CLIENT_ID`, `GOOGLE_CLIENT_SECRET`, and `GOOGLE_REDIRECT_URI` to the app
environment; the local OrbStack callback is
`http://127.0.0.1:8080/auth/google/callback`.
