# GhostFrog Laravel App

This directory contains the new Laravel application for the GhostFrog SaaS/product layer.

The current split is:

- `../pipelines` and `../utils`: existing Python ingestion, enrichment, scoring, and alerting system
- `./`: new Laravel app for auth, subscriptions, dashboards, admin tools, and user-facing product features

## Local Status

This app was scaffolded locally with:

- PHP `8.1`
- Composer `2.4`
- Laravel `10`

Laravel `12` was not installed because this machine is currently on PHP `8.1`. Upgrading to PHP `8.2+` will let you move to newer Laravel versions later.

## Current DB Wiring

The app is configured to connect to the same local PostgreSQL database the Python system is already using.

That is intentional for phase 1, so Laravel can become the product shell while Python remains the processing engine.

## Run Locally

From this directory:

```bash
php artisan serve
```

Then open:

```text
http://127.0.0.1:8000
```

## Important Constraint

Do not run the default Laravel migrations against the shared production-style database blindly.

This database already belongs to the Python system. The Laravel side should get its own reviewed migrations for:

- users
- subscriptions / billing
- saved filters
- account entitlements
- admin dashboard support tables

## Next Recommended Steps

1. Add auth scaffolding.
2. Add a billing package such as Laravel Cashier.
3. Decide whether Laravel reads existing Python tables directly or through a product-layer projection.
4. Introduce SaaS-specific tables in a controlled migration set.
