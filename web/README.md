# GhostFrog Evidence Desk

Private Laravel 13 + Livewire 4 UI for live eBay potentials and their confirmed
sold evidence.

## Local OrbStack

```bash
docker compose up --build -d
open http://127.0.0.1:8080/register
```

The app uses its own PostgreSQL database. The operational database remains
local and is never exposed to Laravel.

Google login is wired in through Laravel Socialite, but it needs OAuth
credentials before it can complete a sign-in. For local OrbStack, create a
Google OAuth web client with this callback:

```text
http://127.0.0.1:8080/auth/google/callback
```

Then set these environment values in `compose.yaml` or your deployment
environment:

```text
GOOGLE_CLIENT_ID=
GOOGLE_CLIENT_SECRET=
GOOGLE_REDIRECT_URI=http://127.0.0.1:8080/auth/google/callback
```

From the repository root, publish the safe local snapshot with:

```bash
./ebay web-sync --endpoint http://127.0.0.1:8080 \
  --secret local-development-sync-secret-change-me --send
```

The default snapshot mirrors all live priced rows and attaches same-key,
same-grade sold comps where available. To refresh it hourly on macOS:

```bash
../scripts/install_web_sync_launch_agent.sh
```

Stop the local app without deleting its database volume:

```bash
docker compose down
```

## Checks

```bash
composer test
npm run build
```
