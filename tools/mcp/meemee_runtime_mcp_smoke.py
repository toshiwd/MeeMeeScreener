from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_CONFIG_PATH = Path(__file__).resolve().with_name("meemee_runtime_mcp.client.json")


def _compact_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, default=str, separators=(",", ":"))


def _load_config(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("client config must be a JSON object")
    return payload


def _resolve_command(config: dict[str, Any], *, config_dir: Path) -> tuple[list[str], Path]:
    command = str(config.get("command") or "").strip()
    if not command:
        raise ValueError("config.command is required")
    args = config.get("args") if isinstance(config.get("args"), list) else []
    resolved_args = [str(arg) for arg in args]
    cwd_value = str(config.get("cwd") or ".").strip()
    cwd = (config_dir / cwd_value).resolve(strict=False) if not Path(cwd_value).is_absolute() else Path(cwd_value).resolve(strict=False)
    if command.lower() == "python":
        command = sys.executable
    return [command, *resolved_args], cwd


def _send_message(proc: subprocess.Popen[bytes], payload: dict[str, Any]) -> dict[str, Any]:
    body = json.dumps(payload, ensure_ascii=False, separators=(",", ":"), default=str).encode("utf-8")
    header = f"Content-Length: {len(body)}\r\n\r\n".encode("ascii")
    assert proc.stdin is not None
    assert proc.stdout is not None
    proc.stdin.write(header)
    proc.stdin.write(body)
    proc.stdin.flush()
    headers: dict[str, str] = {}
    while True:
        line = proc.stdout.readline()
        if not line:
            raise RuntimeError("MCP server closed stdout unexpectedly")
        if line in (b"\n", b"\r\n"):
            break
        text = line.decode("ascii", errors="ignore").strip()
        if ":" not in text:
            continue
        key, value = text.split(":", 1)
        headers[key.strip().lower()] = value.strip()
    content_length = int(headers["content-length"])
    response = proc.stdout.read(content_length)
    if not response:
        raise RuntimeError("MCP server closed before response body")
    return json.loads(response.decode("utf-8"))


def _send_notification(proc: subprocess.Popen[bytes], method: str, params: dict[str, Any] | None = None) -> None:
    body = json.dumps(
        {
            "jsonrpc": "2.0",
            "method": method,
            "params": params or {},
        },
        ensure_ascii=False,
        separators=(",", ":"),
        default=str,
    ).encode("utf-8")
    header = f"Content-Length: {len(body)}\r\n\r\n".encode("ascii")
    assert proc.stdin is not None
    proc.stdin.write(header)
    proc.stdin.write(body)
    proc.stdin.flush()


def _call_tool(proc: subprocess.Popen[bytes], name: str) -> dict[str, Any]:
    response = _send_message(
        proc,
        {
            "jsonrpc": "2.0",
            "id": f"call:{name}",
            "method": "tools/call",
            "params": {
                "name": name,
                "arguments": {},
            },
        },
    )
    if "error" in response:
        raise RuntimeError(_compact_json(response["error"]))
    content = response.get("result", {}).get("content", [])
    if not content:
        raise RuntimeError(f"tool {name} returned no content")
    text = content[0].get("text")
    if not isinstance(text, str):
        raise RuntimeError(f"tool {name} returned non-text content")
    return json.loads(text)


def run_smoke(config_path: Path) -> dict[str, Any]:
    config = _load_config(config_path)
    command, cwd = _resolve_command(config, config_dir=config_path.parent)
    proc = subprocess.Popen(
        command,
        cwd=str(cwd),
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    try:
        init_response = _send_message(
            proc,
            {
                "jsonrpc": "2.0",
                "id": "init",
                "method": "initialize",
                "params": {},
            },
        )
        if "error" in init_response:
            raise RuntimeError(_compact_json(init_response["error"]))
        _send_notification(proc, "initialized", {})
        tools_response = _send_message(proc, {"jsonrpc": "2.0", "id": "tools", "method": "tools/list", "params": {}})
        if "error" in tools_response:
            raise RuntimeError(_compact_json(tools_response["error"]))
        tools = [tool.get("name") for tool in tools_response.get("result", {}).get("tools", [])]
        runtime_status = _call_tool(proc, "get_runtime_stock_db_status")
        rankings_freshness = _call_tool(proc, "get_rankings_freshness")
        return {
            "config_path": str(config_path),
            "command": command,
            "cwd": str(cwd),
            "initialized_protocol": init_response.get("result", {}).get("protocolVersion"),
            "tools": tools,
            "runtime_stock_db_status": runtime_status,
            "rankings_freshness": rankings_freshness,
        }
    finally:
        if proc.stdin:
            try:
                proc.stdin.close()
            except Exception:
                pass
        stderr_text = ""
        if proc.stderr:
            try:
                stderr_text = proc.stderr.read().decode("utf-8", errors="replace")
            except Exception:
                stderr_text = ""
        try:
            proc.terminate()
        except Exception:
            pass
        try:
            proc.wait(timeout=10)
        except Exception:
            try:
                proc.kill()
            except Exception:
                pass
        if stderr_text:
            sys.stderr.write(stderr_text)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Smoke test the MeeMee runtime MCP client path")
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG_PATH)
    args = parser.parse_args(argv)
    result = run_smoke(args.config.resolve(strict=False))
    print(_compact_json(result))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
