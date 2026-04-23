# MeeMee Runtime MCP

Read-only MCP surface for querying current MeeMee / TRADEX runtime state.

## Launch

From the repo root:

```powershell
python tools/mcp/meemee_runtime_mcp.py
```

The server speaks MCP over stdio and does not write state.

## Local Client Config

The repo includes a minimal local client config at:

- [tools/mcp/meemee_runtime_mcp.client.json](./meemee_runtime_mcp.client.json)

Example launcher:

```powershell
python tools/mcp/meemee_runtime_mcp_smoke.py --config tools/mcp/meemee_runtime_mcp.client.json
```

That smoke client starts the server from the config, opens the MCP session, and calls:

- `get_runtime_stock_db_status`
- `get_rankings_freshness`
- `get_stock_analysis_bundle`
- `get_screening_review_bundle`

Example config shape:

```json
{
  "name": "meemee-runtime-readonly",
  "command": "python",
  "args": ["tools/mcp/meemee_runtime_mcp.py"],
  "cwd": "../.."
}
```

## Available tools

- `get_runtime_stock_db_status`
- `get_rankings_freshness`
- `get_publish_runtime_state`
- `get_meemee_artifact_boundary`
- `get_release_build_status`
- `get_stock_analysis_bundle`
- `get_screening_review_bundle`

## New bridge tools

### `get_stock_analysis_bundle`

Read-only consultation bundle for a single stock code.

- `code` is required.
- `asof` is optional and accepts `YYYY-MM-DD` or `YYYYMMDD`.
- `risk_mode` is optional and defaults to `balanced`.
- The returned bundle keeps `MeeMee` and `TRADEX` sections separated and includes runtime freshness warnings.

### `get_screening_review_bundle`

Read-only screening bundle for candidate auditing.

- `asof` is required.
- Exactly one of `top_n` or `codes` must be provided.
- `side` accepts `long`, `short`, or `both` and defaults to `both`.
- `risk_mode` accepts `defensive`, `balanced`, or `aggressive`.
- `include_near_boundary` adds boundary observability only; it does not change candidate selection.
- When `codes` is used, `near_boundary` is not applicable and the bundle returns an explicit warning.

## Contract

- Read-only only.
- No ranking logic, publish policy, or research logic is implemented here.
- The server reuses existing repo contracts and services as the source of truth.
- Missing data is reported explicitly; nothing is faked or backfilled.
- Runtime and ranking freshness warnings remain explicit when the local data is stale.
- `MeeMee` output is for confirmation and operational support.
- `TRADEX` output is for research, comparison, and validation.

## Generic smoke invocation

The smoke client also supports a single generic MCP tool call:

```powershell
python tools/mcp/meemee_runtime_mcp_smoke.py --config tools/mcp/meemee_runtime_mcp.client.json --tool get_stock_analysis_bundle --arguments-json "{\"code\":\"0001\"}"
```

When `--tool` is omitted, the smoke client keeps its original default behavior.
