# GhostFrog read-only MCP

The Mac mini runs one stdio MCP server for local Codex and OpenAI Secure MCP
Tunnel. PostgreSQL stays on loopback. Nothing here starts a listener, collector,
scheduled job, or website change.

| Tool | Evidence |
| --- | --- |
| `ghostfrog_db_catalog` | Available operational schemas, tables and views, column types, declared foreign keys and logical join keys |
| `ghostfrog_db_query` | One bounded, read-only PostgreSQL SELECT across approved operational relations |
| `ghostfrog_source_coverage` | Configured eBay sources, enabled status, scrape cadence and last recorded scrape |
| `ghostfrog_macbook_listings` | Auctions seen live in the last 24 hours or finalized, positive-price sales ending in the last 30 days |
| `ghostfrog_macbook_bid_history` | Price and total bid count snapshots by 12-digit eBay item ID |
| `ghostfrog_macbook_valuation` | 90-day confirmed sales for the exact MacBook key and effective grade, plus acquisition ceiling, profit, ROI and reject reason |

The original listing and coverage tools return at most 25 rows per list. Bid
counts are collector observations. The valuation excludes unsafe listings, checks the stored key against the current
classifier, and requires line, chassis, screen, CPU, RAM and storage. A ceiling
requires the current Apple comp floor of 10. If the 200-candidate scan would
truncate, it withholds the ceiling. Coverage timestamps describe collector
state and may be stale. Outputs omit raw titles, seller fields, raw attributes,
URLs, notes and credentials. PostgreSQL reads have a three-second connection
timeout and five-second per-statement timeout.

## Operational diagnostics

Start with `ghostfrog_db_catalog`. It lists reviewed relations visible to the
dedicated login on this database, including their physical schema and available
columns. Use unqualified relation names from that catalog in
`ghostfrog_db_query`. It distinguishes PostgreSQL foreign keys from logical joins.
The catalog covers all configured markets, including listing observations and
outcomes, price histories, collector cadence and call counts, stored comps,
condition queues, backfill attempts, scores and tracked outcomes. An optional
relation appears when it exists and the login has appropriate grants.

The query tool accepts one SELECT with joins, subqueries, nonrecursive CTEs,
grouping, filtering, window functions and a set of built-in functions and casts.
It runs on loopback in a read-only transaction, with an eight-second statement
timeout, one-second lock timeout and a dedicated login limited to three
connections. It returns 25 rows by default, at most 100 when requested, with a
40-column maximum, a 48 KB row-data limit and a truncation indicator. Use a
time range or listing key and aggregate within that range. A row limit alone
does not make a full-history aggregate fast.

Diagnostics omit credentials, seller fields, free-text listing and error
payloads, URLs and raw attributes. Approved text fields expose short operational
codes. Values that fail the text checks are redacted, and external listing IDs
outside supported public eBay formats return as SQL NULL. Queue diagnoses use
states, timestamps and attempt counts. Database administration, raw system
catalogs and SQL writes are unavailable through these tools.

For a recent lifecycle investigation, compare the stored status and finalization
flag over an explicit time window. See the
[status definitions](../docs/listing-statuses.md) when interpreting those totals.

```sql
SELECT source_id, status, finalized, count(*) AS listings
FROM auction_listings
WHERE end_time >= now() - interval '7 days'
  AND end_time < now()
GROUP BY source_id, status, finalized
ORDER BY listings DESC, source_id, status, finalized
```

For collector health, inspect `sources.last_scraped_at` alongside
`scrape_interval_seconds` and compare it with listing observation times.
For processing failures, group recent queue records by source and status and
report the attempt counts. For price-history checks, filter by the public eBay
item ID. For comp quality, report the stored sample count and computation
timestamp from `latest_comps`. Join on both model and grade when the grade
matches the comp cohort, and do not use whole-product comps for parts,
accessories or repair-only listings.

When drafting a GitHub ticket from a diagnostic result, include the query or
filters, a UTC observation time, denominator, truncation status and aggregate
evidence. Keep individual operational records and sensitive values out of the
ticket.

## Local setup after review

From the repository root on the Mac mini, use a separate virtual environment.

```sh
uv venv --python .venv-mac/bin/python .venv/ghostfrog-mcp
uv pip install --python .venv/ghostfrog-mcp/bin/python -r ops/requirements-mcp.txt
.venv/ghostfrog-mcp/bin/python -m unittest ops.test_ghostfrog_mcp
```

Have the PostgreSQL administrator review and apply the
[`ghostfrog_mcp` role migration](../migrations/20261121_001_add_ghostfrog_mcp_reader.sql)
using the repository migration process. Review the pending migrations before
applying them. The role starts with `NOLOGIN`. Set its password interactively
with psql's `\password ghostfrog_mcp`, then enable `LOGIN` for this role. Keep
the password in a local libpq password file with mode `0600` (normally
`~/.pgpass`). Use a separate entry for `127.0.0.1`, the database port, database
name and `ghostfrog_mcp`. The service reads the database name and port from a
local `~/.pg_service.conf` entry.

```ini
[ghostfrog_mcp]
port=5432
dbname=your_ghostfrog_database_name
```

Keep both files on the Mac mini, outside Git. The launcher forces the role and
loopback address. It uses read-only transactions and never reads the collector's
`.env`. To test or disable login as the administrator, use
`ALTER ROLE ghostfrog_mcp LOGIN` or `ALTER ROLE ghostfrog_mcp NOLOGIN`.

```sh
codex mcp add ghostfrog -- /usr/bin/env "PYTHONPATH=$PWD" \
  "$PWD/.venv/ghostfrog-mcp/bin/python" -m ops.ghostfrog_mcp
codex mcp get ghostfrog
```

In a fresh local Codex session, ask it to call `ghostfrog_source_coverage`
with `limit=1` and return the provenance and query timestamp.

### Enable the diagnostics login after review

Apply the [diagnostics grant migration](../migrations/20261121_002_add_ghostfrog_diagnostics_reader.sql)
through the repository migration process. It grants read access only to listed
columns, with the new role initially set to `NOLOGIN`. Review the effective
privileges for this role, including privileges inherited through `PUBLIC`.
For a relation added after the migration, review new grants explicitly.

Before enabling login, have the database administrator require SCRAM for this
role on the intended database over IPv4 loopback. For a host with broad trust
rules, put these role-specific entries above those rules in `pg_hba.conf`.
Replace `your_ghostfrog_database_name` with the actual database name.

```conf
local all ghostfrog_diagnostics reject
host your_ghostfrog_database_name ghostfrog_diagnostics 127.0.0.1/32 scram-sha-256
host all ghostfrog_diagnostics all reject
```

Validate and reload the authentication configuration. Set a SCRAM password
interactively with psql's `\password ghostfrog_diagnostics`, enable `LOGIN`,
and save the credential as a separate entry in the local mode-0600
`~/.pgpass`. Add a separate service entry to `~/.pg_service.conf`.

```ini
[ghostfrog_diagnostics]
port=5432
dbname=your_ghostfrog_database_name
```

From the MCP virtual environment, run
`.venv/ghostfrog-mcp/bin/python -m unittest ops.test_ghostfrog_diagnostics ops.test_ghostfrog_mcp`.
In local Codex, call `ghostfrog_db_catalog` and issue the sample lifecycle query.
For the ChatGPT acceptance check, refresh the existing GhostFrog app's tools
after the updated MCP server is in use. In a fresh chat, call the catalog and
run the same SELECT. Confirm the tool invocation, UTC timestamp and provenance
against the local result without sharing credentials or individual records.

## ChatGPT website acceptance test

1. In the authorized OpenAI Platform organization, check access to
   [Secure MCP Tunnel](https://developers.openai.com/api/docs/guides/secure-mcp-tunnels).
   Tunnel creation requires Tunnels Read + Manage. Associate the tunnel with
   the intended ChatGPT workspace and use a restricted runtime key with
   Tunnels Read + Use. Keep the key local and out of command arguments and Git.
2. On the Mac mini, follow the
   [official tunnel-client stdio onboarding](https://github.com/openai/tunnel-client/blob/master/docs/onboarding.md).
   Point the profile at the same launcher used for Codex. Run
   `tunnel-client doctor --profile ghostfrog --explain` and then
   `tunnel-client run --profile ghostfrog` in the foreground for the test.
3. In the intended signed-in ChatGPT workspace, choose **Plugins → Add →
   Add custom MCP server** and **Connection → Tunnel**. Select the associated
   tunnel and create a private test plugin. This stdio server relies on the
   private tunnel and ChatGPT workspace access controls. If the workspace
   requires its own MCP OAuth authentication, stop there because this PR does
   not implement OAuth. Custom MCP access depends on the account and workspace
   settings. [ChatGPT setup and permissions](https://help.openai.com/en/articles/12584461-connecting-custom-mcp-servers-in-chatgpt)
4. In a fresh ChatGPT website chat, select the private GhostFrog plugin and ask
   “Call `ghostfrog_source_coverage` with limit 1. Give me the source, its last
   scrape timestamp, the query timestamp and provenance.” Verify a real tool
   invocation and compare the result privately to the equivalent local Codex
   call. Then stop only the foreground tunnel client and repeat the request.
   ChatGPT should report the tool as unavailable. Restart it and verify recovery.

The website test requires the owner’s workspace and Platform access. Keep it
pending until both the live read and tunnel stop test have been observed. If
the Mac or tunnel is offline, there is no live connection or cached response
from this service.

## Persistent tunnel on the Mac mini

Before enabling the LaunchAgent, get account-specific confirmation from OpenAI
about whether continuous idle polling and MCP request, response, or bandwidth
usage are billable, what paid API billing or credits are required, and any
included allowance or rates. The
[Secure MCP Tunnel guide](https://developers.openai.com/api/docs/guides/secure-mcp-tunnels#limits)
documents quotas, including 600 MCP requests per minute per tunnel and calling
application. The [API pricing page](https://developers.openai.com/api/docs/pricing)
does not specify a tunnel charge. Keep the agent uninstalled until the cost and
any necessary spend approval are established.

This user LaunchAgent runs after macOS login. It needs the logged-in user's
session and the repository volume for the MCP server. It retries exits with a
60-second launchd throttle. Use the existing `ghostfrog` profile in
`~/.config/tunnel-client/ghostfrog.yaml` and its file reference to the restricted
key in `~/.config/ghostfrog-mcp/platform-runtime-key`. Keep the profile, key and
`~/.pgpass` private and owned by the login user.

From the repo root, render and validate the plist. This only prepares a local
file.

```sh
label=com.ghostfrog.langgraph-ebay.mcp-tunnel
domain="gui/$(id -u)"
service="$domain/$label"
plist="$HOME/Library/LaunchAgents/$label.plist"
logs="$HOME/Library/Logs/GhostFrog/mcp-tunnel"
staged="$(mktemp -t ghostfrog-tunnel)"
.venv-mac/bin/python ops/launchd/ghostfrog_tunnel_agent.py render > "$staged"
plutil -lint "$staged"
```

After the spend check, stop the existing foreground GhostFrog client with
Ctrl-C in its terminal and verify it exited. The agent also checks for an
existing client using the `ghostfrog` profile before starting. Install only
after the foreground client has stopped.

```sh
mkdir -p "$HOME/Library/LaunchAgents"
install -d -m 700 "$logs"
install -m 600 "$staged" "$plist"
launchctl enable "$service"
launchctl bootstrap "$domain" "$plist"
```

Use the same variables for lifecycle commands. **Stop** persists across
logins until **Start** re-enables the agent.

| Action | Command |
| --- | --- |
| Start after a stop | `launchctl enable "$service" && launchctl bootstrap "$domain" "$plist"` |
| Stop | `launchctl disable "$service" && launchctl bootout "$service"` |
| Restart while loaded | `launchctl kickstart -k "$service"` |
| Status | `launchctl print "$service"` |
| Uninstall | Disable and boot out the service if loaded, then remove only `"$plist"`. The profile, credentials and logs can be retained. |

The loopback health address is in `"$logs/health.url"` for the current run.
Check the launchd state first because the file can remain after a stop.
`/healthz` and `/readyz` should return HTTP 200. Check
`/health?details=true` or `/metrics` for a successful control-plane poll.
Warnings and startup errors go to `"$logs/stdout.log"` and
`"$logs/stderr.log"`. The agent disables raw HTTP, tracing and payload capture.

With the foreground terminal closed, verify one process for the profile and
run `ghostfrog_source_coverage(limit=1)` in a fresh ChatGPT website chat.
Verify an actual tool invocation with PostgreSQL provenance and a current
query timestamp. Stop through launchd and repeat in a fresh chat to confirm
unavailability. Start through launchd and verify health, one process and a
new successful read. Test login recovery during a planned login or reboot
window.

## Manual fallback

Run the following from the repo root on the Mac mini. It prints a timestamped
redacted coverage report to stdout and exits with an error if PostgreSQL is
unavailable. Save it locally and upload it to a private ChatGPT chat when
needed. The export is a snapshot.

```sh
.venv/ghostfrog-mcp/bin/python -m ops.ghostfrog_mcp --coverage-json
```
