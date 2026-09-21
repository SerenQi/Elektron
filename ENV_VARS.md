# Environment variables

## Core

| Variable | Purpose | Default |
|---|---|---|
| `OMBRE_TRANSPORT` | `stdio`, `streamable-http`, or `sse` | `stdio` |
| `OMBRE_PORT` | HTTP/Dashboard port | `8000` |
| `OMBRE_BUCKETS_DIR` | private Markdown and continuity data | `./buckets` |
| `OMBRE_API_KEY` | optional OpenAI-compatible model key | empty |
| `OMBRE_BASE_URL` | optional common API base URL | config file |
| `OMBRE_MODEL` | chat/compression model | config file |
| `OMBRE_API_PASSWORD` | Dashboard and administrative API password | first-run setup |

`OMBRE_DASHBOARD_PASSWORD` remains a compatibility alias for the password.

## Identity

| Variable | Purpose | Default |
|---|---|---|
| `OMBRE_AGENT_NAME` | local agent display label | `Agent` |
| `OMBRE_HUMAN_NAME` | local human display label | `Human` |
| `OMBRE_AGENT_PERSONA` | optional local first-person orientation | neutral text |

Identity configuration belongs to each installation and is never required to
use the continuity mechanisms.

## Optional integrations

| Variable | Purpose |
|---|---|
| `OMBRE_HOOK_URL` | fire-and-forget event webhook |
| `OMBRE_HOOK_SKIP` | disable event webhooks |
| `OMBRE_NOW_PLAYING_COMMAND` | local executable for optional now-playing data |
| `OMBRE_MEMORY_ANALYZER` | `dp` or `cli` memory-analysis path |
| `OMBRE_QUIET_HOURS` | quiet-hour range used by absence/longing logic |
| `OMBRE_QUIET_TZ` | timezone for quiet hours | `UTC` |

Keep values in an untracked `.env` or secret manager.
