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

## Contract

- Read-only only.
- No ranking logic, publish policy, or research logic is implemented here.
- The server reuses existing repo contracts and services as the source of truth.
- Missing data is reported explicitly; nothing is faked or backfilled.
