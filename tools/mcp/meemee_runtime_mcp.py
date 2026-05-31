from __future__ import annotations

import argparse
import json
import os
import sys
import zipfile
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable

REPO_ROOT = Path(__file__).resolve().parents[2]
APP_BACKEND_ROOT = REPO_ROOT / "app" / "backend"

for candidate in (REPO_ROOT, APP_BACKEND_ROOT):
    candidate_text = str(candidate)
    if candidate_text not in sys.path:
        sys.path.insert(0, candidate_text)

import duckdb

from app.core.config import config as app_config
from app.backend.infra.files.config_repo import ConfigRepository
from app.backend.services import codex_bridge_service
from app.backend.services import rankings_cache
from app.backend.services.chart_reading_bundle import get_chart_reading_bundle
from app.backend.services.meemee_artifact_boundary import (
    BLOCKED_HOLD_ARTIFACT_FILENAMES,
    MEEMEE_SAFE_ARTIFACT_FILENAMES,
    TRADEX_ONLY_ARTIFACT_FILENAMES,
    to_meemee_publish_queue_view,
    to_meemee_publish_state_view,
    to_meemee_runtime_selection_view,
)
from app.backend.services.runtime_selection_service import build_runtime_selection_snapshot
from shared.runtime_stock_db_contract import inspect_runtime_stock_db, resolve_runtime_stock_db_selection

_MCP_PROTOCOL_VERSION = "2024-11-05"
_DEFAULT_RANKINGS_THRESHOLD_DAYS = 5
_SAFE_RELEASE_ARTIFACTS = MEEMEE_SAFE_ARTIFACT_FILENAMES


def _compact_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, default=str, separators=(",", ":"))


def _normalize_text(value: Any) -> str:
    return str(value or "").strip()


def _normalize_ymd_value(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, date):
        return int(value.strftime("%Y%m%d"))
    if isinstance(value, datetime):
        return int(value.strftime("%Y%m%d"))
    if isinstance(value, (int,)):
        raw = int(value)
        if 19_000_101 <= raw <= 20_991_231:
            return raw
        if raw >= 1_000_000_000_000:
            try:
                return int(datetime.fromtimestamp(raw / 1000, tz=timezone.utc).strftime("%Y%m%d"))
            except Exception:
                return raw
        if raw >= 1_000_000_000:
            try:
                return int(datetime.fromtimestamp(raw, tz=timezone.utc).strftime("%Y%m%d"))
            except Exception:
                return raw
        return raw if raw > 0 else None
    if isinstance(value, float):
        if not value == value:
            return None
        return _normalize_ymd_value(int(value))
    text = _normalize_text(value)
    if not text:
        return None
    if text.isdigit():
        return _normalize_ymd_value(int(text))
    for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%Y%m%d"):
        try:
            return int(datetime.strptime(text, fmt).strftime("%Y%m%d"))
        except Exception:
            continue
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        return int(parsed.strftime("%Y%m%d"))
    except Exception:
        return None


def _ymd_to_iso(value: int | None) -> str | None:
    normalized = _normalize_ymd_value(value)
    if normalized is None:
        return None
    try:
        resolved = datetime.strptime(str(normalized), "%Y%m%d").date()
    except Exception:
        return None
    return resolved.isoformat()


def _current_jst_date() -> date:
    return (datetime.now(timezone.utc) + timedelta(hours=9)).date()


def _freshness_threshold_days() -> int:
    raw = os.getenv("MEEMEE_RANK_CURRENT_CANDIDATE_MAX_AGE_DAYS", str(_DEFAULT_RANKINGS_THRESHOLD_DAYS))
    try:
        return max(1, int(str(raw).strip()))
    except Exception:
        return _DEFAULT_RANKINGS_THRESHOLD_DAYS


def _freshness_days_from_ymd(value: int | None) -> int | None:
    normalized = _normalize_ymd_value(value)
    if normalized is None:
        return None
    try:
        as_of = datetime.strptime(str(normalized), "%Y%m%d").date()
    except Exception:
        return None
    return (_current_jst_date() - as_of).days


def _quoted_identifier(value: str) -> str:
    return '"' + str(value).replace('"', '""') + '"'


def _table_columns(conn: duckdb.DuckDBPyConnection, table_name: str) -> set[str]:
    rows = conn.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'main' AND lower(table_name) = lower(?)
        """,
        [table_name],
    ).fetchall()
    return {str(row[0]).strip().lower() for row in rows if str(row[0]).strip()}


def _latest_ymd_from_table(conn: duckdb.DuckDBPyConnection, table_name: str) -> int | None:
    columns = _table_columns(conn, table_name)
    if not columns:
        return None
    for candidate in ("date", "dt", "as_of", "asof", "snapshot_date", "trade_date", "ymd"):
        if candidate not in columns:
            continue
        row = conn.execute(
            f"""
            SELECT {_quoted_identifier(candidate)}
            FROM {_quoted_identifier(table_name)}
            WHERE {_quoted_identifier(candidate)} IS NOT NULL
            ORDER BY {_quoted_identifier(candidate)} DESC
            LIMIT 1
            """
        ).fetchone()
        if not row:
            continue
        normalized = _normalize_ymd_value(row[0])
        if normalized is not None:
            return normalized
    return None


def _inspect_latest_table_dates(db_path: Path) -> dict[str, int | None]:
    if not db_path.exists():
        return {
            "daily_bars": None,
            "feature_snapshot_daily": None,
            "ml_pred_20d": None,
        }
    conn = duckdb.connect(str(db_path), read_only=True)
    try:
        return {
            "daily_bars": _latest_ymd_from_table(conn, "daily_bars"),
            "feature_snapshot_daily": _latest_ymd_from_table(conn, "feature_snapshot_daily"),
            "ml_pred_20d": _latest_ymd_from_table(conn, "ml_pred_20d"),
        }
    finally:
        conn.close()


def _runtime_stock_db_freshness_state(latest_global_date: int | None) -> dict[str, Any]:
    threshold_days = _freshness_threshold_days()
    freshness_days = _freshness_days_from_ymd(latest_global_date)
    fresh = bool(freshness_days is not None and freshness_days < threshold_days)
    return {
        "freshness_state": "fresh" if fresh else "stale",
        "freshness_days": freshness_days,
        "freshness_threshold_days": threshold_days,
        "stale": not fresh,
    }


def get_runtime_stock_db_status() -> dict[str, Any]:
    return codex_bridge_service.get_runtime_stock_db_status()


def get_rankings_freshness(
    *,
    tf: str = "D",
    which: str = "latest",
    direction: str = "up",
    mode: str = "trade",
    risk_mode: str = "balanced",
    limit: int = 50,
) -> dict[str, Any]:
    return codex_bridge_service.get_rankings_freshness(
        tf=tf,
        which=which,
        direction=direction,
        mode=mode,
        risk_mode=risk_mode,
        limit=limit,
    )


def get_publish_runtime_state(*, config_data_dir: str | Path | None = None, db_path: str | Path | None = None) -> dict[str, Any]:
    data_dir = Path(config_data_dir).expanduser().resolve(strict=False) if config_data_dir else Path(app_config.DATA_DIR)
    repo = ConfigRepository(str(data_dir))
    snapshot = build_runtime_selection_snapshot(config_repo=repo, db_path=str(db_path) if db_path is not None else None)
    runtime_selection = to_meemee_runtime_selection_view(snapshot)
    publish_state = to_meemee_publish_state_view(snapshot)
    publish_queue = to_meemee_publish_queue_view(snapshot)
    source_of_truth = _normalize_text(snapshot.get("source_of_truth"))
    runtime_surface_dependency = "TRADEX" if source_of_truth == "external_analysis" or bool(snapshot.get("shadow_only")) else "MeeMee"
    return {
        "confirmed": True,
        "sanitized": True,
        "runtime_surface_dependency": runtime_surface_dependency,
        "runtime_selection": runtime_selection,
        "publish_state": publish_state,
        "publish_queue": publish_queue,
        "publish_pointer_summary": {
            "default_logic_pointer": snapshot.get("default_logic_pointer"),
            "champion_logic_key": snapshot.get("champion_logic_key"),
            "challenger_logic_key": snapshot.get("challenger_logic_key"),
            "challenger_logic_keys": list(snapshot.get("challenger_logic_keys") or []),
        },
        "source_of_truth": source_of_truth,
        "registry_sync_state": snapshot.get("registry_sync_state"),
        "degraded": bool(snapshot.get("degraded")),
        "shadow_integration_available": bool(snapshot.get("shadow_integration_available")),
        "shadow_only": bool(snapshot.get("shadow_only")),
        "shadow_integration_state": snapshot.get("shadow_integration_state"),
        "last_sync_time": snapshot.get("last_sync_time"),
    }


def get_meemee_artifact_boundary(*, boundary_module_path: str | Path | None = None) -> dict[str, Any]:
    module_path = Path(boundary_module_path).expanduser().resolve(strict=False) if boundary_module_path else REPO_ROOT / "app" / "backend" / "services" / "meemee_artifact_boundary.py"
    try:
        updated_at = datetime.fromtimestamp(module_path.stat().st_mtime, tz=timezone.utc).isoformat()
    except Exception:
        updated_at = None
    return {
        "confirmed": True,
        "boundary_module_path": str(module_path),
        "last_updated_signal": updated_at,
        "deny_by_default": True,
        "allowlisted_meemee_safe_artifacts": list(MEEMEE_SAFE_ARTIFACT_FILENAMES),
        "known_tradex_only_artifacts": list(TRADEX_ONLY_ARTIFACT_FILENAMES),
        "known_blocked_hold_artifacts": list(BLOCKED_HOLD_ARTIFACT_FILENAMES),
        "allowlist_count": len(MEEMEE_SAFE_ARTIFACT_FILENAMES),
        "tradex_only_count": len(TRADEX_ONLY_ARTIFACT_FILENAMES),
        "blocked_hold_count": len(BLOCKED_HOLD_ARTIFACT_FILENAMES),
    }


def _release_package_root_candidates(*, repo_root: Path) -> list[Path]:
    candidates: list[Path] = []
    env_root = os.getenv("MEEMEE_RELEASE_PACKAGE_ROOT")
    if env_root:
        candidates.append(Path(env_root).expanduser().resolve(strict=False) / "MeeMeeScreener")
    desktop_root = Path(os.path.expanduser("~")) / "Desktop"
    candidates.append(desktop_root / "MeeMeeScreener")
    candidates.append(repo_root / "release" / "MeeMeeScreener")
    return candidates


def _file_mtime(path: Path) -> float | None:
    try:
        return path.stat().st_mtime
    except Exception:
        return None


def _pick_latest_existing_path(paths: list[Path]) -> Path | None:
    existing = [(path, _file_mtime(path)) for path in paths if path.exists()]
    existing = [(path, mtime) for path, mtime in existing if mtime is not None]
    if not existing:
        return None
    return max(existing, key=lambda item: item[1] or 0.0)[0]


def _inspect_zip_safe_artifacts(zip_path: Path) -> dict[str, Any]:
    if not zip_path.exists():
        return {
            "present": False,
            "path": str(zip_path),
            "missing": list(_SAFE_RELEASE_ARTIFACTS),
        }
    try:
        with zipfile.ZipFile(zip_path, "r") as archive:
            names = set(Path(name).as_posix() for name in archive.namelist())
    except Exception:
        return {
            "present": False,
            "path": str(zip_path),
            "missing": list(_SAFE_RELEASE_ARTIFACTS),
            "invalid_zip": True,
        }
    missing = []
    for artifact_name in _SAFE_RELEASE_ARTIFACTS:
        expected = f"_internal/artifacts/research_inventory/{artifact_name}"
        if expected not in names:
            missing.append(artifact_name)
    return {
        "present": True,
        "path": str(zip_path),
        "missing": missing,
        "complete": not missing,
    }


def _inspect_package_safe_artifacts(package_root: Path) -> dict[str, Any]:
    inventory_root = package_root / "_internal" / "artifacts" / "research_inventory"
    missing = [artifact_name for artifact_name in _SAFE_RELEASE_ARTIFACTS if not (inventory_root / artifact_name).is_file()]
    return {
        "present": package_root.exists(),
        "path": str(package_root),
        "inventory_path": str(inventory_root),
        "missing": missing,
        "complete": package_root.exists() and not missing,
    }


def get_release_build_status(*, repo_root: str | Path | None = None) -> dict[str, Any]:
    repo = Path(repo_root).expanduser().resolve(strict=False) if repo_root is not None else REPO_ROOT
    release_zip = repo / "release" / "MeeMeeScreener-portable.zip"
    release_package_root = _pick_latest_existing_path(_release_package_root_candidates(repo_root=repo))
    package_summary = _inspect_package_safe_artifacts(release_package_root) if release_package_root else {
        "present": False,
        "path": None,
        "inventory_path": None,
        "missing": list(_SAFE_RELEASE_ARTIFACTS),
        "complete": False,
    }
    zip_summary = _inspect_zip_safe_artifacts(release_zip)
    exe_candidates = [path / "MeeMeeScreener.exe" for path in _release_package_root_candidates(repo_root=repo)]
    if release_package_root:
        exe_candidates.insert(0, release_package_root / "MeeMeeScreener.exe")
    latest_exe = _pick_latest_existing_path(exe_candidates)
    timestamps = [ts for ts in (_file_mtime(release_zip), _file_mtime(latest_exe) if latest_exe else None) if ts is not None]
    if release_package_root:
        timestamps.extend(
            [
                ts
                for artifact_name in _SAFE_RELEASE_ARTIFACTS
                if (ts := _file_mtime(release_package_root / "_internal" / "artifacts" / "research_inventory" / artifact_name)) is not None
            ]
        )
    latest_timestamp = max(timestamps) if timestamps else None
    smoke_ready = bool(
        latest_exe
        and zip_summary.get("complete")
        and package_summary.get("complete")
        and release_zip.exists()
    )
    return {
        "confirmed": True,
        "latest_exe_path": str(latest_exe) if latest_exe else None,
        "latest_portable_zip_path": str(release_zip) if release_zip.exists() else None,
        "latest_build_timestamp": datetime.fromtimestamp(latest_timestamp, tz=timezone.utc).isoformat() if latest_timestamp is not None else None,
        "release_package_root": package_summary.get("path"),
        "portable_zip_summary": zip_summary,
        "packaged_safe_artifacts_summary": package_summary,
        "allowlisted_meemee_safe_artifacts_bundled": bool(zip_summary.get("complete") and package_summary.get("complete")),
        "smoke_pass_ready": smoke_ready,
        "missing_release_outputs": {
            "exe": latest_exe is None,
            "portable_zip": not release_zip.exists(),
        },
    }


@dataclass(frozen=True)
class ToolDefinition:
    name: str
    description: str
    handler: Callable[[dict[str, Any]], dict[str, Any]]
    input_schema: dict[str, Any]


def _tool_schema() -> dict[str, Any]:
    return {
        "type": "object",
        "properties": {},
        "additionalProperties": False,
    }


def _tool_schema_with_properties(
    *,
    properties: dict[str, Any],
    required: list[str] | None = None,
    one_of: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    schema: dict[str, Any] = {
        "type": "object",
        "properties": properties,
        "additionalProperties": False,
    }
    if required:
        schema["required"] = required
    if one_of:
        schema["oneOf"] = one_of
    return schema


def _strict_arguments(arguments: dict[str, Any], *, allowed: set[str], required: set[str] | None = None) -> dict[str, Any]:
    if not isinstance(arguments, dict):
        raise ValueError("arguments must be an object")
    unexpected = sorted(set(arguments) - allowed)
    if unexpected:
        raise ValueError(f"unexpected arguments: {', '.join(unexpected)}")
    if required:
        missing = sorted(name for name in required if name not in arguments)
        if missing:
            raise ValueError(f"missing required arguments: {', '.join(missing)}")
    return dict(arguments)


def _wrap_tool_handler(func: Callable[[], dict[str, Any]]) -> Callable[[dict[str, Any]], dict[str, Any]]:
    def _handler(arguments: dict[str, Any]) -> dict[str, Any]:
        if arguments:
            raise ValueError("this tool does not accept arguments")
        return func()

    return _handler


def _wrap_argument_tool_handler(
    func: Callable[[dict[str, Any]], dict[str, Any]],
) -> Callable[[dict[str, Any]], dict[str, Any]]:
    def _handler(arguments: dict[str, Any]) -> dict[str, Any]:
        return func(dict(arguments or {}))

    return _handler


def _call_stock_analysis_bundle(arguments: dict[str, Any]) -> dict[str, Any]:
    args = _strict_arguments(arguments, allowed={"code", "asof", "risk_mode"})
    return codex_bridge_service.build_stock_analysis_bundle(
        code=args.get("code"),
        asof=args.get("asof"),
        risk_mode=args.get("risk_mode", "balanced"),
    )


def _call_screening_review_bundle(arguments: dict[str, Any]) -> dict[str, Any]:
    args = _strict_arguments(
        arguments,
        allowed={"asof", "top_n", "codes", "side", "risk_mode", "include_near_boundary"},
    )
    return codex_bridge_service.build_screening_review_bundle(
        asof=args.get("asof"),
        top_n=args.get("top_n"),
        codes=args.get("codes"),
        side=args.get("side", "both"),
        risk_mode=args.get("risk_mode", "balanced"),
        include_near_boundary=args.get("include_near_boundary", False),
    )


def _call_chart_reading_bundle(arguments: dict[str, Any]) -> dict[str, Any]:
    args = _strict_arguments(arguments, allowed={"code", "as_of_date"}, required={"code", "as_of_date"})
    conn = duckdb.connect(str(app_config.DB_PATH), read_only=True)
    try:
        return get_chart_reading_bundle(conn, code=args["code"], as_of_date=args["as_of_date"])
    finally:
        conn.close()


TOOLS: dict[str, ToolDefinition] = {
    "get_runtime_stock_db_status": ToolDefinition(
        name="get_runtime_stock_db_status",
        description="Inspect the freshness-selected local runtime stock DB and its latest local dates.",
        handler=_wrap_tool_handler(get_runtime_stock_db_status),
        input_schema=_tool_schema(),
    ),
    "get_rankings_freshness": ToolDefinition(
        name="get_rankings_freshness",
        description="Inspect current rankings freshness using the live rankings cache contract.",
        handler=_wrap_tool_handler(get_rankings_freshness),
        input_schema=_tool_schema(),
    ),
    "get_publish_runtime_state": ToolDefinition(
        name="get_publish_runtime_state",
        description="Return a sanitized runtime/publish summary without raw research payloads.",
        handler=_wrap_tool_handler(get_publish_runtime_state),
        input_schema=_tool_schema(),
    ),
    "get_meemee_artifact_boundary": ToolDefinition(
        name="get_meemee_artifact_boundary",
        description="Return the current MeeMee artifact allowlist boundary contract.",
        handler=_wrap_tool_handler(get_meemee_artifact_boundary),
        input_schema=_tool_schema(),
    ),
    "get_release_build_status": ToolDefinition(
        name="get_release_build_status",
        description="Inspect the latest known local release outputs and bundled safe artifacts.",
        handler=_wrap_tool_handler(get_release_build_status),
        input_schema=_tool_schema(),
    ),
    "get_stock_analysis_bundle": ToolDefinition(
        name="get_stock_analysis_bundle",
        description="Return a read-only MeeMee/TRADEX consultation bundle for a single stock code.",
        handler=_wrap_argument_tool_handler(_call_stock_analysis_bundle),
        input_schema=_tool_schema_with_properties(
            properties={
                "code": {"type": "string"},
                "asof": {"type": ["string", "integer"], "description": "YYYY-MM-DD or YYYYMMDD"},
                "risk_mode": {"type": "string", "enum": ["defensive", "balanced", "aggressive"]},
            },
            required=["code"],
        ),
    ),
    "get_screening_review_bundle": ToolDefinition(
        name="get_screening_review_bundle",
        description="Return a read-only screening review bundle for ranking candidates.",
        handler=_wrap_argument_tool_handler(_call_screening_review_bundle),
        input_schema=_tool_schema_with_properties(
            properties={
                "asof": {"type": ["string", "integer"], "description": "YYYY-MM-DD or YYYYMMDD"},
                "top_n": {"type": "integer", "minimum": 1, "maximum": 20},
                "codes": {"type": "array", "items": {"type": "string"}, "minItems": 1, "maxItems": 20},
                "side": {"type": "string", "enum": ["long", "short", "both"]},
                "risk_mode": {"type": "string", "enum": ["defensive", "balanced", "aggressive"]},
                "include_near_boundary": {"type": "boolean"},
            },
            required=["asof"],
        ),
    ),
    "get_chart_reading_bundle": ToolDefinition(
        name="get_chart_reading_bundle",
        description="Return a read-only chart reading bundle with chart context, chart notes, annotations, tags, and position state for a symbol/date.",
        handler=_wrap_argument_tool_handler(_call_chart_reading_bundle),
        input_schema=_tool_schema_with_properties(
            properties={
                "code": {"type": "string"},
                "as_of_date": {"type": "string", "description": "YYYY-MM-DD or YYYYMMDD"},
            },
            required=["code", "as_of_date"],
        ),
    ),
}


def list_tools() -> list[dict[str, Any]]:
    return [
        {
            "name": definition.name,
            "description": definition.description,
            "inputSchema": definition.input_schema,
        }
        for definition in TOOLS.values()
    ]


def call_tool(name: str, arguments: dict[str, Any] | None = None) -> dict[str, Any]:
    definition = TOOLS.get(str(name or "").strip())
    if definition is None:
        raise KeyError(f"unknown tool: {name}")
    payload = definition.handler(dict(arguments or {}))
    return {
        "content": [
            {
                "type": "text",
                "text": _compact_json(payload),
            }
        ],
        "isError": False,
    }


def _jsonrpc_result(request_id: Any, result: Any) -> dict[str, Any]:
    return {"jsonrpc": "2.0", "id": request_id, "result": result}


def _jsonrpc_error(request_id: Any, code: int, message: str, data: Any | None = None) -> dict[str, Any]:
    error = {"code": code, "message": message}
    if data is not None:
        error["data"] = data
    return {"jsonrpc": "2.0", "id": request_id, "error": error}


def _read_message(stream) -> dict[str, Any] | None:
    headers: dict[str, str] = {}
    while True:
        line = stream.readline()
        if not line:
            return None
        if line in (b"\n", b"\r\n"):
            break
        try:
            text = line.decode("ascii", errors="ignore").strip()
        except Exception:
            text = ""
        if not text or ":" not in text:
            continue
        key, value = text.split(":", 1)
        headers[key.strip().lower()] = value.strip()
    content_length = headers.get("content-length")
    if not content_length:
        return None
    try:
        size = int(content_length)
    except Exception:
        return None
    body = stream.read(size)
    if not body:
        return None
    return json.loads(body.decode("utf-8"))


def _write_message(stream, payload: dict[str, Any]) -> None:
    body = json.dumps(payload, ensure_ascii=False, separators=(",", ":"), default=str).encode("utf-8")
    header = f"Content-Length: {len(body)}\r\n\r\n".encode("ascii")
    stream.write(header)
    stream.write(body)
    stream.flush()


def _handle_request(message: dict[str, Any]) -> dict[str, Any] | None:
    method = _normalize_text(message.get("method"))
    request_id = message.get("id", None)
    params = message.get("params")
    if method == "initialize":
        return _jsonrpc_result(
            request_id,
            {
                "protocolVersion": _MCP_PROTOCOL_VERSION,
                "serverInfo": {
                    "name": "meemee-runtime-readonly",
                    "version": "0.1.0",
                },
                "capabilities": {
                    "tools": {},
                },
            },
        )
    if method == "initialized":
        return None
    if method == "ping":
        return _jsonrpc_result(request_id, {})
    if method == "tools/list":
        return _jsonrpc_result(request_id, {"tools": list_tools()})
    if method == "tools/call":
        if not isinstance(params, dict):
            return _jsonrpc_error(request_id, -32602, "params must be an object")
        name = params.get("name")
        arguments = params.get("arguments") if isinstance(params.get("arguments"), dict) else {}
        try:
            return _jsonrpc_result(request_id, call_tool(str(name), arguments))
        except KeyError as exc:
            return _jsonrpc_error(request_id, -32602, str(exc))
        except ValueError as exc:
            return _jsonrpc_error(request_id, -32602, str(exc))
        except Exception as exc:
            return _jsonrpc_error(request_id, -32603, "tool execution failed", str(exc))
    if request_id is None:
        return None
    return _jsonrpc_error(request_id, -32601, f"method not found: {method}")


def serve_stdio() -> None:
    stdin = sys.stdin.buffer
    stdout = sys.stdout.buffer
    while True:
        message = _read_message(stdin)
        if message is None:
            break
        response = _handle_request(message)
        if response is not None:
            _write_message(stdout, response)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="MeeMee/TRADEX read-only runtime MCP server")
    parser.add_argument("--stdio", action="store_true", help="Run the MCP server over stdio (default).")
    parser.parse_args(argv)
    serve_stdio()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
