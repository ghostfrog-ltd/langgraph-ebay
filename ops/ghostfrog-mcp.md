# GhostFrog read-only MCP

The Mac mini runs one stdio MCP server for local Codex and OpenAI Secure MCP
Tunnel. PostgreSQL stays on loopback. Nothing here starts a listener, collector,
scheduled job, or website change.

| Tool | Evidence |
| --- | --- |
| `ghostfrog_source_coverage` | Configured eBay sources, enabled status, scrape cadence and last recorded scrape |
| `ghostfrog_macbook_listings` | Auctions seen live in the last 24 hours or finalized, positive-price sales ending in the last 30 days |
| `ghostfrog_macbook_bid_history` | Price and total bid count snapshots by 12-digit eBay item ID |
| `ghostfrog_macbook_valuation` | 90-day confirmed sales for the exact MacBook key and effective grade, plus acquisition ceiling, profit, ROI and reject reason |

Returns at most 25 rows per list. Bid counts are collector observations. The
valuation excludes unsafe listings, checks the stored key against the current
classifier, and requires line, chassis, screen, CPU, RAM and storage. A ceiling
requires the current Apple comp floor of 10. If the 200-candidate scan would
truncate, it withholds the ceiling. Coverage timestamps describe collector
state and may be stale. Outputs omit raw titles, seller fields, raw attributes,
URLs, notes and credentials. PostgreSQL reads have a three-second connection
timeout and five-second per-statement timeout.

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

## Manual fallback

Run the following from the repo root on the Mac mini. It prints a timestamped
redacted coverage report to stdout and exits with an error if PostgreSQL is
unavailable. Save it locally and upload it to a private ChatGPT chat when
needed. The export is a snapshot.

```sh
.venv/ghostfrog-mcp/bin/python -m ops.ghostfrog_mcp --coverage-json
```
