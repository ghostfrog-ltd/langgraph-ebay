# GhostFrog OpenAI Platform spend

The read-only MCP tool `ghostfrog_openai_spend` serves a private local summary.
A separate refresher on the Mac mini calls the fixed
[OpenAI Costs API](https://developers.openai.com/api/reference/resources/admin/subresources/organization/subresources/usage/methods/costs)
endpoint. The API credential is never passed to the MCP server, exposed in a tool
response, or written to a launchd plist.

## Scope and baseline

The Costs API records organization API costs in daily UTC buckets. Its fields
can identify a project, API key, source or line item, but there is no tunnel ID
field. The watch reports organization-wide recorded API costs. An increased
cost is not evidence that tunnel traffic was billed or that a payment was
collected. The Platform Billing page is the source for credit balances and
invoices. The Costs API can post charges and corrections later.

The first complete reading establishes a baseline and the UTC day from which to
re-query. A successful response that explicitly covers every requested day with
empty results represents zero recorded costs. A missing day, broken pagination,
failed fetch or expired snapshot reports **monitoring unavailable**. No stale
totals are returned in that state.

For USD costs, the threshold uses the latest
[ECB reference rates](https://www.ecb.europa.eu/stats/policy_and_exchange_rates/euro_reference_exchange_rates/html/index.en.html)
to compute GBP per USD. The quote date and source are returned. Comparisons use
the unrounded figure. The rate estimates the GBP equivalent of the recorded cost
change and is not an invoice exchange rate.

## Local setup

The owner supplies an Admin API key for the organization that owns the tunnel.
Create `~/.config/ghostfrog-spend` with mode `0700`. Populate `admin-key` using a
hidden prompt and put the verified organization ID in `organization-id`. Both
files must be owned by the login user and mode `0600`. The key has broader
organization capabilities than this reader uses. Native monthly spend alerts
avoid a local key but do not supply hourly readings or repeat-until-acknowledged
notifications. A Platform Cost export can reconcile a snapshot without a key.

From the stable repository checkout used on the Mac mini:

```sh
.venv-mac/bin/python -m ops.ghostfrog_spend refresh
.venv-mac/bin/python -m ops.ghostfrog_spend status
```

Validate the reading privately against the intended Platform organization's
Spend dashboard. Check whether the permission or cost history is missing if
the tool reports **monitoring unavailable**. Do not put cost amounts or the key
in GitHub.

After validating the live read, render the separate cost-only LaunchAgent with
`.venv-mac/bin/python ops/launchd/ghostfrog_spend_agent.py`. Use
`plutil -lint` on the output, create `~/Library/Logs/GhostFrog/spend` with mode
`0700`, and install the plist under `~/Library/LaunchAgents` with mode `0600`.
Bootstrap it into the login user's GUI launchd domain. It refreshes every 15
minutes while the user is logged in. This agent does not control the GhostFrog
tunnel or any collector. A snapshot over 75 minutes old is unavailable.

## Alert and acknowledgement

Configure the existing **GhostFrog £5 Spend Watch** to call
`ghostfrog_openai_spend` every hour. A first positive spend change yields one
stable `first_charge_event`. Notify once for that event ID and retain the ID
in the watch's chat history so subsequent checks can deduplicate it.
A nonempty `repeat_alert_id` calls for a reminder on every hourly check.
If both are present on the same check, send one alert. If the source is
unavailable, report **monitoring unavailable** without amounts.

Acknowledge the current repeat alert locally from the repo root:

```sh
.venv-mac/bin/python -m ops.ghostfrog_spend acknowledge ALERT_ID
```

Acknowledgement persists across runs and restarts for that native cost level.
An increase above the acknowledged total can trigger again if the total
increase since baseline is over £5. A stale acknowledgement ID is rejected.
No acknowledgement tool is exposed to the scheduled task.

Verify a real interactive ChatGPT tool call and a scheduled run after the
private plugin learns the new tool. Verify the existing watch actually uses
its tool and remembers the first event before treating notifications as live.

## Tests

```sh
.venv/ghostfrog-mcp/bin/python -m unittest ops.test_ghostfrog_spend ops.test_ghostfrog_mcp
```
