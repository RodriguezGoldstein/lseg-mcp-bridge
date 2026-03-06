# lseg-python-mcp-bridge

Local stdio MCP server for the LSEG Data Library for Python. The bridge keeps a read-only tool surface for session verification, schema inspection, code validation, live snapshot retrieval, historical retrieval, local example search, and reusable `lseg.data.content.search` retrieval workflows.

The project targets Python 3.14 and uses environment variables only for authentication. Credentials must never be committed.

## Package Target

- `lseg-data>=2.1.1,<3`
- The bridge is built around the current `lseg.data` package shape, including:
  - `lseg.data.get_data`
  - `lseg.data.get_history`
  - `lseg.data.session.platform.Definition`
  - `lseg.data.content.search`

## Features

- Local stdio MCP server based on the Python `mcp` SDK
- Read-only live and historical data retrieval
- Dynamic schema/signature/docstring introspection
- AST-based validation for common `lseg.data` usage patterns
- Safe live validation for confidently resolved literal `get_data` and `get_history` calls
- Reusable content-search workflows for metadata discovery, regional retrieval, company resolution, and RIC lookup
- Local example and documentation search with cached ranking
- Structured JSON-safe errors with secret redaction
- Session reuse via a singleton `SessionManager`

## Requirements

- Python 3.14
- Access to `lseg-data` and a valid auth mode
- Local reference files under `LSEG_EXAMPLES_DIR` if you want `search_examples` results

## Python 3.14 Setup

Create the dedicated project virtual environment in the repo root:

```bash
python3.14 -m venv .venv
```

Activate it:

```bash
source .venv/bin/activate
```

Upgrade `pip` in the venv:

```bash
python -m pip install --upgrade pip
```

Install project requirements:

```bash
pip install -r requirements.txt
```

Verify that the active interpreter is Python 3.14:

```bash
python --version
pip --version
pip show lseg-data mcp pydantic pandas
```

Expected output:

```text
Python 3.14.x
```

## Re-Activating Later

When you come back to the project:

```bash
cd /absolute/path/to/lseg-python-mcp-bridge
source .venv/bin/activate
python --version
```

## Installation

1. Create and activate a virtual environment:

   ```bash
   python3.14 -m venv .venv
   source .venv/bin/activate
   ```

2. Install dependencies:

   ```bash
   python -m pip install --upgrade pip
   python -m pip install -r requirements.txt
   ```

3. Copy the sample env file and fill in only the auth mode you intend to use:

   ```bash
   cp .env.example .env
   ```

4. Export the variables or load them with your shell tooling before starting the server.

## Environment Variables

### Common

- `LSEG_AUTH_MODE=auto`
- `LSEG_APP_KEY`
- `LSEG_HTTP_TIMEOUT=120`
- `LSEG_DEFAULT_ROW_LIMIT=50`
- `LSEG_EXAMPLES_DIR=./references/examples`
- `DEBUG_MCP=false`

### Platform client credentials

- `LSEG_CLIENT_ID`
- `LSEG_CLIENT_SECRET`

### Platform password

- `LSEG_USERNAME`
- `LSEG_PASSWORD`

### Desktop

- `LSEG_APP_KEY`
- Requires local LSEG Workspace/Desktop access

## Auth Auto-Detection

When `LSEG_AUTH_MODE=auto`, the bridge tries auth modes in this order:

1. `platform_client_credentials`
2. `platform_password`
3. `desktop`

`ping_session` reports both the detected credential bundles and the mode actually used. If credentials are absent or a session cannot be opened, the tool returns a structured error instead of crashing the server.

## Payload Sizing

- Default row limit: `50`
- Hard cap: `250`
- Applies to both `get_live_data` and `get_history`
- Preview rows are intentionally compact for Codex-style tool output

## Content Search Capability Model

The bridge includes a reusable internal `content_search.py` layer for `from lseg.data.content import search`. It is not examples-only code. Downstream bridge code can use it to:

- fetch and filter metadata definitions for property discovery
- validate selected fields and `order_by` clauses against metadata
- search a topic, company, or subject across multiple regions
- resolve companies using multiple identifiers such as ticker, SEDOL, and market context
- resolve RIC candidates for a ticker on a specific exchange country or exchange code

The public MCP wrappers for these workflows are:

- `get_search_metadata`
- `search_by_region`
- `company_lookup`
- `lookup_ric`

## Run As A Stdio MCP Server

```bash
python server.py
```

If you prefer not to activate the shell first:

```bash
.venv/bin/python server.py
```

## Codex MCP Launch Configuration

Always point Codex at the virtualenv interpreter instead of system Python:

```json
{
  "mcpServers": {
    "lseg-python-mcp-bridge": {
      "command": "/absolute/path/to/lseg-python-mcp-bridge/.venv/bin/python",
      "args": ["/absolute/path/to/lseg-python-mcp-bridge/server.py"],
      "cwd": "/absolute/path/to/lseg-python-mcp-bridge",
      "env": {
        "LSEG_APP_KEY": "your-app-key",
        "LSEG_CLIENT_ID": "your-client-id",
        "LSEG_CLIENT_SECRET": "your-client-secret",
        "LSEG_AUTH_MODE": "auto",
        "LSEG_HTTP_TIMEOUT": "120",
        "LSEG_DEFAULT_ROW_LIMIT": "100"
      }
    }
  }
}
```

This makes Codex launch the bridge with `.venv/bin/python`, which keeps the runtime reproducible and avoids relying on the system interpreter.

## MCP Tools

### `ping_session`

Opens or reuses a session and returns:

- `session_open`
- `auth_mode_used`
- `library_version`
- `python_version`
- `connectivity_summary`
- `detected_credential_types`
- `error` on failure

### `list_capabilities`

Returns support flags for:

- live data
- history
- schema introspection
- code validation
- example search
- content search
- metadata discovery
- regional search
- company lookup
- RIC lookup
- supported auth modes

### `get_schema`

Dynamic schema lookup for import paths such as:

- `lseg.data.get_data`
- `lseg.data.get_history`
- `lseg.data.session.platform.Definition`

### `validate_code`

Parses Python with `ast`, validates imports and symbol resolution, compares calls against actual signatures, flags likely hallucinated API usage, and optionally performs a safe live check when `check_live=true`.

Recognized live-check patterns include:

- `import lseg.data as ld` followed by `ld.get_data(...)` or `ld.get_history(...)`
- `from lseg.data import get_data, get_history`
- simple alias assignments such as `reader = ld.get_data`

### `get_live_data`

Read-only wrapper around `ld.get_data()` with JSON-safe table output.

### `get_history`

Read-only wrapper around `ld.get_history()` with JSON-safe table output.

### `get_search_metadata`

Returns normalized metadata for `lseg.data.content.search.metadata.Definition`, including:

- property definitions
- property type
- searchable flag
- sortable flag
- navigable flag
- groupable flag
- exact flag
- symbol flag

It also supports filtering by property name or any of the boolean metadata attributes.

### `search_by_region`

Runs a reusable regional content search workflow for a topic, company, or subject. Results are accumulated across regions and returned in a stable normalized shape.

### `company_lookup`

Resolves one or more company lookup requests using multiple identifiers. The bridge separates the query anchor from the structured filter so downstream code can reuse the matching logic cleanly.

### `lookup_ric`

Resolves RIC candidates for a ticker with exchange-country and/or exchange-code filters. This is intended for downstream symbol resolution workflows.

### `search_examples`

Searches local files under `LSEG_EXAMPLES_DIR`. It supports:

- keyword ranking
- symbol/path boosting for names like `get_data`, `get_history`, and `session.platform.Definition`
- optional `exact_lookup=true` for symbol/path-style queries

### `explain_symbol`

Summarizes a symbol, common usage, typical pitfalls, and related symbols.

## Structured Errors

Every MCP tool returns JSON-safe errors in a stable shape:

```json
{
  "error": {
    "code": "schema_lookup_failed",
    "message": "Import path 'lseg.data.missing' could not be resolved.",
    "details": {
      "path": "lseg.data.missing",
      "reason": "missing_attribute"
    }
  }
}
```

Common error codes:

- `missing_credentials`
- `missing_dependency`
- `session_open_failed`
- `schema_lookup_failed`
- `validation_failed`
- `data_request_failed`
- `data_normalization_failed`
- `example_search_failed`

Secrets from configured environment variables are redacted from returned messages and details.

Search-specific validation failures usually surface as:

- `validation_failed` for unsupported view names, property names, `select_fields`, or non-sortable `order_by` properties
- `data_request_failed` for LSEG content-search execution failures or entitlement issues

## Compact Tool Examples

### `ping_session`

```json
{
  "auth_mode": "auto"
}
```

### `get_schema`

```json
{
  "path": "lseg.data.get_history",
  "include_docstring": true,
  "include_members": false
}
```

### `validate_code`

```json
{
  "code": "import lseg.data as ld\nld.get_data(universe=['IBM.N'], fields=['BID'])",
  "strict": true,
  "check_live": false
}
```

### `get_live_data`

```json
{
  "universe": ["IBM.N", "VOD.L"],
  "fields": ["BID", "ASK"],
  "row_limit": 25
}
```

### `get_history`

```json
{
  "universe": "GOOG.O",
  "fields": ["TR.Revenue"],
  "interval": "1Y",
  "start": "2021-01-01",
  "end": "2025-01-01",
  "count": 10
}
```

### `get_search_metadata`

```json
{
  "view": "SEARCH_ALL",
  "searchable": true
}
```

### `search_by_region`

```json
{
  "query": "semiconductors",
  "regions": ["USA", "GBR", "JPN"],
  "view": "SEARCH_ALL",
  "select_fields": [
    "RIC",
    "PrimaryRIC",
    "TickerSymbol",
    "PermID",
    "CompanyName",
    "PrimaryExchange",
    "ExchangeCountry"
  ],
  "top_per_region": 10
}
```

### `company_lookup`

```json
{
  "requests": [
    {
      "ticker": "AA",
      "sedol": "BYNF418",
      "exchange_country": "USA",
      "exchange_code": "NYS",
      "name": "ALCOA CORP"
    }
  ]
}
```

### `lookup_ric`

```json
{
  "ticker": "AA",
  "exchange_country": "USA",
  "exchange_code": "NYS",
  "view": "EQUITY_QUOTES",
  "select_fields": [
    "RIC",
    "PrimaryRIC",
    "TickerSymbol",
    "CommonName",
    "ExchangeName",
    "ExchangeCode",
    "ExchangeCountry",
    "AssetState"
  ],
  "top": 25,
  "order_by": "ExchangeName asc"
}
```

### `search_examples`

```json
{
  "query": "lseg.data.get_data",
  "language": "python",
  "top_k": 5,
  "exact_lookup": true
}
```

## Smoke Tests

Syntax check:

```bash
.venv/bin/python -m py_compile server.py auth.py schemas.py validator.py live_data.py examples.py content_search.py
```

Schema smoke check:

```bash
.venv/bin/python - <<'PY'
from schemas import get_schema
print(get_schema("lseg.data.get_data"))
print(get_schema("lseg.data.get_history"))
PY
```

No-credentials auth check:

```bash
.venv/bin/python - <<'PY'
from auth import SessionManager
print(SessionManager.instance().ping_session())
PY
```

Validation smoke check:

```bash
.venv/bin/python - <<'PY'
from validator import validate_code
print(validate_code("import lseg.data as ld\nld.get_data(universe=['IBM.N'], fields=['BID'])"))
print(validate_code("from lseg.data import get_history\nget_history(universe='GOOG.O')"))
PY
```

Content-search model and error-path smoke check:

```bash
.venv/bin/python - <<'PY'
import lseg.data as ld
from server import get_search_metadata
from content_search import CompanyLookupRequest, RicLookupRequest, RegionalSearchRequest, resolve_search_view

print(resolve_search_view(ld.discovery.Views.SEARCH_ALL))
print(RegionalSearchRequest.model_validate({"query": "banks", "regions": ["USA", "GBR"]}).model_dump())
print(CompanyLookupRequest.model_validate({"ticker": "AA", "sedol": "BYNF418", "exchange_country": "USA"}).model_dump())
print(RicLookupRequest.model_validate({"ticker": "AA", "exchange_country": "USA"}).model_dump())
print(get_search_metadata())
PY
```

## Troubleshooting

### Wrong interpreter in Codex

- If Codex launches the MCP server with the wrong interpreter, ensure the `command` field points to `.venv/bin/python`.
- Confirm the virtual environment exists at `.venv`.
- Confirm dependencies are installed with `.venv/bin/python -m pip install -r requirements.txt`.

### Missing credentials

- Confirm that one full credential bundle is present.
- Use `ping_session` first to see detected credential types and the selected auth mode.

### Session open failures

- Verify the env vars match the selected auth mode.
- For desktop mode, confirm LSEG Workspace/Desktop is available locally.
- Increase `LSEG_HTTP_TIMEOUT` if the environment is slow to connect.

### Missing packages

- Install `requirements.txt` into the same Python environment used to launch the server.
- Prefer `.venv/bin/python -m pip install -r requirements.txt` if you are not in an activated shell.
- The server exits immediately if the `mcp` package is missing.

### Schema lookup failures

- Confirm the path matches a real importable symbol.
- Start with `lseg.data.get_data`, `lseg.data.get_history`, or `lseg.data.session.platform.Definition`.

### Content search metadata or filter failures

- Use `get_search_metadata` to inspect the view-specific property set before choosing `select_fields` or `order_by`.
- The bridge validates `select_fields` and `order_by` against live metadata, so unsupported properties fail early with `validation_failed`.
- If a short property name is ambiguous in a nested metadata tree, use the fully qualified property path instead of the leaf name.
- Available properties and views can vary with the installed SDK and the authenticated account entitlements.

### Example search returns no matches

- Confirm `LSEG_EXAMPLES_DIR` points to a real directory.
- Populate it with local `.py`, `.ipynb`, `.md`, `.rst`, `.txt`, `.json`, `.yaml`, or `.yml` files.
