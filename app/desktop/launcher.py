from __future__ import annotations

import csv
import ctypes
import json
import os
import shutil
import socket
import subprocess
import sys
import threading
import time
import urllib.request
import urllib.parse
import urllib.error
from pathlib import Path

import traceback


_repo_root = Path(__file__).resolve().parents[2]
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))

from app.desktop.runtime_paths import base_path, local_app_dir, resolve_path

APP_NAME = "MeeMeeScreener"
WINDOW_TITLE = "MeeMee Screener"
MUTEX_NAME = "Global\\MeeMeeScreenerSingleton"
HEALTH_TIMEOUT_SECONDS = 25
_LOGGED_RESOLVED_PATHS = False
_DEV_ENV_KEYS = ("MEEMEE_DEV", "MEEMEE_DEV_MODE")


def _is_dev_mode() -> bool:
    return os.getenv("MEEMEE_DEV", "").lower() in ("1", "true", "yes", "on") or os.getenv(
        "MEEMEE_DEV_MODE", ""
    ).lower() in ("1", "true", "yes", "on")


def _is_selftest_mode() -> bool:
    return os.getenv("MEEMEE_SELFTEST", "").lower() in ("1", "true", "yes", "on")


def _check_webview2_runtime() -> bool:
    """Check if Microsoft Edge WebView2 Runtime is installed."""
    import winreg
    
    # Check common installation paths
    paths = [
        os.path.join(os.environ.get("ProgramFiles(x86)", "C:\\Program Files (x86)"), 
                     "Microsoft", "EdgeWebView", "Application", "msedgewebview2.exe"),
        os.path.join(os.environ.get("ProgramFiles", "C:\\Program Files"), 
                     "Microsoft", "EdgeWebView", "Application", "msedgewebview2.exe"),
    ]
    
    for path in paths:
        if os.path.exists(path):
            return True
    
    # Check registry
    try:
        key = winreg.OpenKey(winreg.HKEY_LOCAL_MACHINE, 
                            r"SOFTWARE\WOW6432Node\Microsoft\EdgeUpdate\Clients\{F3017226-FE2A-4295-8BDF-00C3A9A7E4C5}")
        winreg.CloseKey(key)
        return True
    except:
        pass
    
    try:
        key = winreg.OpenKey(winreg.HKEY_LOCAL_MACHINE, 
                            r"SOFTWARE\Microsoft\EdgeUpdate\Clients\{F3017226-FE2A-4295-8BDF-00C3A9A7E4C5}")
        winreg.CloseKey(key)
        return True
    except:
        pass
    
    return False


def _check_dotnet_framework() -> bool:
    """Check if .NET Framework 4.8 or higher is installed."""
    import winreg
    
    try:
        # Check for .NET Framework 4.8 or higher
        # Release value 528040 = .NET 4.8
        key = winreg.OpenKey(winreg.HKEY_LOCAL_MACHINE,
                            r"SOFTWARE\Microsoft\NET Framework Setup\NDP\v4\Full")
        try:
            release, _ = winreg.QueryValueEx(key, "Release")
            winreg.CloseKey(key)
            # 528040 = .NET 4.8, 528049 = .NET 4.8 on Windows 10 May 2019 Update
            return release >= 528040
        except:
            winreg.CloseKey(key)
            return False
    except:
        return False


def _message_box(text: str, title: str) -> None:
    ctypes.windll.user32.MessageBoxW(None, text, title, 0x00000010)


def _acquire_mutex() -> ctypes.wintypes.HANDLE | None:
    mutex = ctypes.windll.kernel32.CreateMutexW(None, True, MUTEX_NAME)
    already_exists = ctypes.windll.kernel32.GetLastError() == 183
    if already_exists:
        ctypes.windll.kernel32.CloseHandle(mutex)
        return None
    return mutex


def _release_mutex(handle: ctypes.wintypes.HANDLE | None) -> None:
    if handle:
        ctypes.windll.kernel32.ReleaseMutex(handle)
        ctypes.windll.kernel32.CloseHandle(handle)


def _find_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]

def _can_bind_port(port: int) -> bool:
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.bind(("127.0.0.1", port))
        return True
    except OSError:
        return False


def _tasklist_rows(*, image_name: str | None = None, pid: int | None = None) -> list[list[str]]:
    cmd = ["tasklist", "/FO", "CSV", "/NH"]
    if image_name:
        cmd.extend(["/FI", f"IMAGENAME eq {image_name}"])
    if pid is not None:
        cmd.extend(["/FI", f"PID eq {int(pid)}"])
    try:
        raw = subprocess.check_output(
            cmd,
            stderr=subprocess.STDOUT,
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
            text=True,
            encoding="cp932",
            errors="ignore",
        )
    except Exception:
        return []
    rows: list[list[str]] = []
    for row in csv.reader(line for line in raw.splitlines() if line.strip()):
        if not row:
            continue
        if row[0].startswith("INFO:"):
            continue
        rows.append(row)
    return rows


def _list_pids_by_image(image_name: str) -> list[int]:
    pids: list[int] = []
    for row in _tasklist_rows(image_name=image_name):
        if len(row) < 2:
            continue
        try:
            pids.append(int(str(row[1]).replace(",", "").strip()))
        except Exception:
            continue
    return pids


def _get_process_commandline(pid: int) -> str:
    script = (
        f"$p = Get-CimInstance Win32_Process -Filter \"ProcessId = {int(pid)}\"; "
        "if ($p) { $p.CommandLine }"
    )
    try:
        out = subprocess.check_output(
            ["powershell", "-NoProfile", "-Command", script],
            stderr=subprocess.DEVNULL,
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
            text=True,
            encoding="cp932",
            errors="ignore",
        )
        return out.strip()
    except Exception:
        return ""


def _is_backend_process(pid: int) -> bool:
    cmdline = _get_process_commandline(pid).lower()
    if not cmdline:
        return False
    return "--backend" in cmdline or "meemee_backend_only" in cmdline


def _terminate_pid(pid: int) -> bool:
    try:
        subprocess.check_call(
            ["taskkill", "/PID", str(int(pid)), "/T", "/F"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
        )
        return True
    except Exception:
        return False


def _cleanup_stale_backend_processes() -> int:
    if not getattr(sys, "frozen", False):
        return 0
    image_name = os.path.basename(sys.executable)
    if not image_name:
        return 0
    current_pid = os.getpid()
    killed = 0
    for pid in _list_pids_by_image(image_name):
        if pid == current_pid:
            continue
        if not _is_backend_process(pid):
            continue
        if _terminate_pid(pid):
            killed += 1
            print(f"[launcher] Terminated stale backend process PID={pid}")
    return killed


def _list_listening_pids_on_port(port: int) -> list[int]:
    try:
        raw = subprocess.check_output(
            ["netstat", "-ano", "-p", "tcp"],
            stderr=subprocess.STDOUT,
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
            text=True,
            encoding="cp932",
            errors="ignore",
        )
    except Exception:
        return []
    pids: set[int] = set()
    suffix = f":{int(port)}"
    for line in raw.splitlines():
        parts = line.split()
        if len(parts) < 5:
            continue
        local_addr = parts[1]
        state = parts[3].upper()
        if state != "LISTENING":
            continue
        if not local_addr.endswith(suffix):
            continue
        try:
            pids.add(int(parts[4]))
        except Exception:
            continue
    return sorted(pids)


def _terminate_unhealthy_backend_on_port(port: int) -> int:
    killed = 0
    current_pid = os.getpid()
    for pid in _list_listening_pids_on_port(port):
        if pid == current_pid:
            continue
        if not _is_backend_process(pid):
            continue
        if _terminate_pid(pid):
            killed += 1
            print(f"[launcher] Terminated unhealthy backend PID={pid} on port {port}")
    return killed


def _get_health_timeout_seconds() -> int:
    raw = os.getenv("MEEMEE_HEALTH_TIMEOUT_SECONDS")
    if not raw:
        return HEALTH_TIMEOUT_SECONDS
    try:
        value = int(raw)
    except Exception:
        return HEALTH_TIMEOUT_SECONDS
    return max(5, min(120, value))


def _wait_for_health_detail(port: int, timeout_seconds: int) -> tuple[bool, str | None]:
    deadline = time.monotonic() + timeout_seconds
    url = f"http://127.0.0.1:{port}/api/health"
    # Ensure localhost health checks are not routed through system proxy settings
    # (common on corporate Windows setups), otherwise we can mistakenly think the
    # backend is down and shut it back off.
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    last_err: Exception | None = None
    last_detail: str | None = None
    while time.monotonic() < deadline:
        try:
            with opener.open(url, timeout=1) as response:
                body = response.read()
                payload: dict[str, object] = {}
                if body:
                    try:
                        payload = json.loads(body.decode("utf-8"))
                    except Exception:
                        payload = {}
                is_http_ok = 200 <= int(response.status) < 300
                is_ready = payload.get("ready")
                if is_http_ok and is_ready is True:
                    return True, None
                phase = str(payload.get("phase") or "")
                message = str(payload.get("message") or "")
                last_detail = f"status={response.status} phase={phase} message={message}".strip()
        except urllib.error.HTTPError as exc:
            last_err = exc
            detail = f"status={exc.code}"
            try:
                body = exc.read()
                if body:
                    payload = json.loads(body.decode("utf-8"))
                    phase = str(payload.get("phase") or "")
                    message = str(payload.get("message") or "")
                    detail = f"status={exc.code} phase={phase} message={message}".strip()
            except Exception:
                pass
            last_detail = detail
        except Exception as exc:
            last_err = exc
            last_detail = str(exc)
        time.sleep(0.2)
    if last_err is not None:
        print(f"[launcher] Health check failed for {url}: {last_err}")
    return False, (last_detail or (str(last_err) if last_err else None))


def _wait_for_health(port: int, timeout_seconds: int) -> bool:
    ok, _ = _wait_for_health_detail(port, timeout_seconds)
    return ok


def _escape_html(value: str) -> str:
    return (
        value.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
        .replace("'", "&#x27;")
    )


def _tail_file(path: str, max_lines: int = 200) -> tuple[str, str | None]:
    try:
        with open(path, "r", encoding="utf-8", errors="replace") as handle:
            lines = handle.read().splitlines()
        if not lines:
            return "", None
        return "\n".join(lines[-max_lines:]), None
    except Exception as exc:
        return "", str(exc)


def _resolved_paths_snapshot(paths: dict[str, str]) -> list[tuple[str, str]]:
    exe_dir = os.path.dirname(sys.executable)
    cwd = os.getcwd()
    app_env = os.getenv("APP_ENV", "")
    data_dir = os.getenv("MEEMEE_DATA_DIR", paths.get("data_dir", ""))
    data_store = os.getenv("MEEMEE_DATA_STORE", paths.get("data_store_dir", ""))
    db_path = os.getenv("STOCKS_DB_PATH", paths.get("stocks_db", ""))
    auto_update_enabled = os.getenv("MEEMEE_ENABLE_AUTO_UPDATE", "").lower() in ("1", "true", "yes", "on")
    return [
        ("exe_dir", exe_dir),
        ("cwd", cwd),
        ("APP_ENV", app_env),
        ("MEEMEE_DATA_DIR", data_dir),
        ("MEEMEE_DATA_STORE", data_store),
        ("STOCKS_DB_PATH", db_path),
        ("auto_update_enabled", str(auto_update_enabled)),
    ]


def _build_error_html(
    title: str,
    message: str,
    paths: dict[str, str],
    backend_log_path: str,
    health_error: str | None = None,
) -> str:
    resolved_rows = "\n".join(
        f"<tr><td>{_escape_html(k)}</td><td>{_escape_html(v)}</td></tr>"
        for k, v in _resolved_paths_snapshot(paths)
    )
    log_tail, log_err = _tail_file(backend_log_path, 200)
    log_display = log_tail if log_tail else ""
    if log_err:
        log_display = f"(failed to read backend.log: {log_err})\n{backend_log_path}"
    elif not log_tail:
        log_display = f"(backend.log is empty)\n{backend_log_path}"
    health_block = f"<p><strong>health:</strong> {_escape_html(health_error)}</p>" if health_error else ""
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>MeeMee Screener - Error</title>
  <style>
    body {{
      margin: 0;
      font-family: "Segoe UI", "Meiryo", sans-serif;
      background: #0b1020;
      color: #e2e8f0;
    }}
    .container {{
      max-width: 980px;
      margin: 32px auto;
      padding: 0 20px 40px;
    }}
    .card {{
      background: #0f172a;
      border: 1px solid #1e293b;
      border-radius: 14px;
      padding: 20px 22px;
      box-shadow: 0 18px 40px rgba(15, 23, 42, 0.45);
      margin-bottom: 18px;
    }}
    h1 {{
      font-size: 20px;
      margin: 0 0 8px;
    }}
    h2 {{
      font-size: 14px;
      text-transform: uppercase;
      letter-spacing: 0.08em;
      color: #94a3b8;
      margin: 0 0 10px;
    }}
    .message {{
      font-size: 14px;
      color: #cbd5f5;
      }}
    table {{
      width: 100%;
      border-collapse: collapse;
      font-size: 12px;
    }}
    td {{
      padding: 6px 8px;
      border-bottom: 1px solid #1e293b;
      vertical-align: top;
      word-break: break-all;
    }}
    td:first-child {{
      width: 200px;
      color: #94a3b8;
      }}
    pre {{
      white-space: pre-wrap;
      background: #0b1020;
      border: 1px solid #1e293b;
      border-radius: 10px;
      padding: 12px;
      color: #e2e8f0;
      font-size: 12px;
      max-height: 360px;
      overflow: auto;
    }}
  </style>
</head>
<body>
  <div class="container">
    <div class="card">
      <h1>{_escape_html(title)}</h1>
      <p class="message">{_escape_html(message)}</p>
      {health_block}
      <p class="message">backend.log: {_escape_html(backend_log_path)}</p>
    </div>
    <div class="card">
      <h2>Resolved Paths</h2>
      <table>{resolved_rows}</table>
    </div>
    <div class="card">
      <h2>backend.log (last 200 lines)</h2>
      <pre>{_escape_html(log_display)}</pre>
    </div>
  </div>
</body>
</html>"""


def _build_loading_html() -> str:
    return """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>MeeMee Screener - Starting</title>
  <style>
    :root {
      --bg-1: #030712;
      --bg-2: #0b1220;
      --ink: #e2e8f0;
      --muted: #94a3b8;
      --accent: #38bdf8;
      --accent-2: #22d3ee;
      --card: rgba(10, 16, 30, 0.86);
      --card-border: rgba(148, 163, 184, 0.22);
    }
    * {
      box-sizing: border-box;
    }
    body {
      margin: 0;
      min-height: 100vh;
      font-family: "Segoe UI", "Meiryo", sans-serif;
      background:
        radial-gradient(1200px 720px at -10% -10%, #1d4ed8 0%, transparent 55%),
        radial-gradient(980px 620px at 120% 110%, #0f766e 0%, transparent 60%),
        linear-gradient(140deg, var(--bg-1) 0%, var(--bg-2) 60%, #060a12 100%);
      color: var(--ink);
      display: flex;
      align-items: center;
      justify-content: center;
      padding: 24px;
      box-sizing: border-box;
      overflow: hidden;
    }
    .glow {
      position: fixed;
      border-radius: 999px;
      filter: blur(48px);
      opacity: 0.32;
      pointer-events: none;
      animation: drift 8s ease-in-out infinite alternate;
    }
    .glow.one {
      width: 320px;
      height: 320px;
      left: -40px;
      top: -60px;
      background: #2563eb;
    }
    .glow.two {
      width: 360px;
      height: 360px;
      right: -80px;
      bottom: -80px;
      background: #0d9488;
      animation-delay: -2.4s;
    }
    @keyframes drift {
      from { transform: translate3d(0, 0, 0); }
      to { transform: translate3d(18px, -14px, 0); }
    }
    .card {
      width: min(560px, 100%);
      background: var(--card);
      border: 1px solid var(--card-border);
      border-radius: 20px;
      padding: 28px 30px;
      box-shadow: 0 24px 60px rgba(2, 6, 23, 0.5);
      backdrop-filter: blur(12px);
      position: relative;
      overflow: hidden;
    }
    .card::after {
      content: "";
      position: absolute;
      inset: 0;
      background: linear-gradient(
        110deg,
        rgba(56, 189, 248, 0.0) 0%,
        rgba(56, 189, 248, 0.08) 36%,
        rgba(56, 189, 248, 0.0) 74%
      );
      transform: translateX(-120%);
      animation: sheen 2.8s ease-in-out infinite;
      pointer-events: none;
    }
    @keyframes sheen {
      to { transform: translateX(130%); }
    }
    .row {
      display: flex;
      align-items: center;
      gap: 14px;
      margin-bottom: 12px;
    }
    h1 {
      margin: 0;
      font-size: 30px;
      letter-spacing: 0.01em;
      font-weight: 700;
    }
    .pill {
      font-size: 11px;
      letter-spacing: 0.08em;
      text-transform: uppercase;
      color: #bae6fd;
      padding: 4px 8px;
      border-radius: 999px;
      border: 1px solid rgba(56, 189, 248, 0.4);
      background: rgba(2, 6, 23, 0.55);
    }
    .status {
      margin: 0;
      color: #dbeafe;
      line-height: 1.6;
      font-size: 15px;
      min-height: 24px;
      position: relative;
      z-index: 1;
    }
    .status::after {
      content: "";
      display: inline-block;
      width: 6px;
      height: 6px;
      border-radius: 999px;
      background: var(--accent);
      margin-left: 8px;
      vertical-align: middle;
      animation: pulse 1.2s ease-in-out infinite;
    }
    @keyframes pulse {
      0%, 100% { opacity: 0.35; transform: scale(1); }
      50% { opacity: 1; transform: scale(1.25); }
    }
    .sub {
      margin-top: 8px;
      color: var(--muted);
      font-size: 13px;
      line-height: 1.5;
    }
    .progress {
      margin-top: 18px;
      width: 100%;
      height: 8px;
      border-radius: 999px;
      background: rgba(51, 65, 85, 0.65);
      overflow: hidden;
      position: relative;
      z-index: 1;
    }
    .progress::before {
      content: "";
      position: absolute;
      inset: 0;
      width: 32%;
      border-radius: 999px;
      background: linear-gradient(90deg, var(--accent), var(--accent-2));
      box-shadow: 0 0 18px rgba(34, 211, 238, 0.45);
      animation: loading 1.4s ease-in-out infinite;
    }
    @keyframes loading {
      0% { transform: translateX(-120%); }
      60% { transform: translateX(220%); }
      100% { transform: translateX(220%); }
    }
    .spinner {
      width: 46px;
      height: 46px;
      border-radius: 50%;
      border: 3px solid rgba(148, 163, 184, 0.24);
      border-top-color: var(--accent);
      border-right-color: rgba(56, 189, 248, 0.7);
      animation: spin 0.95s linear infinite;
    }
    @keyframes spin {
      from { transform: rotate(0deg); }
      to { transform: rotate(360deg); }
    }
  </style>
</head>
<body>
  <div class="glow one" aria-hidden="true"></div>
  <div class="glow two" aria-hidden="true"></div>
  <div class="card">
    <div class="row">
      <div class="spinner" aria-hidden="true"></div>
      <h1>MeeMee Screener</h1>
      <span class="pill">booting</span>
    </div>
    <p id="boot-status" class="status">Starting backend...</p>
    <p class="sub">Please wait while services are prepared.</p>
    <div class="progress" aria-hidden="true"></div>
  </div>
  <script>
    (function () {
      var status = document.getElementById("boot-status");
      window.__setStatus = function (text) {
        if (!status) return;
        if (typeof text !== "string") return;
        var next = text.trim();
        if (!next) return;
        status.textContent = next;
      };
    })();
  </script>
</body>
</html>"""


def _show_error_page(window, html: str) -> None:
    try:
        window.load_html(html)
    except Exception:
        try:
            window.load_url("data:text/html;charset=utf-8," + urllib.parse.quote(html))
        except Exception:
            pass


def _check_frontend_render(window, paths: dict[str, str], backend_log_path: str) -> None:
    try:
        result = window.evaluate_js(
            "(() => { const root = document.getElementById('root'); return root ? root.innerHTML.length : 0; })();"
        )
    except Exception:
        result = 0
    try:
        length = int(result) if result is not None else 0
    except Exception:
        length = 0
    if length <= 0:
        error_html = _build_error_html(
            "Frontend failed to render",
            "UI root is empty after backend ready. Check static assets and console logs.",
            paths,
            backend_log_path,
            health_error="frontend_render_timeout",
        )
        _show_error_page(window, error_html)


def _schedule_frontend_watchdog(window, paths: dict[str, str], backend_log_path: str) -> None:
    timer = threading.Timer(6.0, _check_frontend_render, args=(window, paths, backend_log_path))
    timer.daemon = True
    timer.start()


def _write_text(path: str, content: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as handle:
        handle.write(content)


def _write_json(path: str, payload: object) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2)


def _http_get_json(url: str, timeout: float = 5.0) -> dict | list:
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    with opener.open(url, timeout=timeout) as response:
        raw = response.read().decode("utf-8", errors="replace")
    return json.loads(raw)


def _http_post_multipart(
    url: str,
    field_name: str,
    filename: str,
    content: bytes,
    fields: dict[str, str] | None = None,
    timeout: float = 10.0,
) -> dict:
    boundary = f"----MeeMeeBoundary{int(time.time() * 1000)}"
    parts: list[bytes] = []
    if fields:
        for key, value in fields.items():
            part = (
                f"--{boundary}\r\n"
                f'Content-Disposition: form-data; name="{key}"\r\n\r\n'
                f"{value}\r\n"
            ).encode("utf-8")
            parts.append(part)
    file_header = (
        f"--{boundary}\r\n"
        f'Content-Disposition: form-data; name="{field_name}"; filename="{filename}"\r\n'
        "Content-Type: text/csv\r\n\r\n"
    ).encode("utf-8")
    parts.append(file_header + content + b"\r\n")
    parts.append(f"--{boundary}--\r\n".encode("utf-8"))
    body = b"".join(parts)
    req = urllib.request.Request(url, data=body, method="POST")
    req.add_header("Content-Type", f"multipart/form-data; boundary={boundary}")
    req.add_header("Content-Length", str(len(body)))
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    try:
        with opener.open(req, timeout=timeout) as response:
            raw = response.read().decode("utf-8", errors="replace")
        return json.loads(raw)
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"HTTP {exc.code}: {detail}") from exc


def _copy_if_missing(src: str, dst: str) -> None:
    if os.path.isfile(dst):
        return
    os.makedirs(os.path.dirname(dst), exist_ok=True)
    if os.path.isfile(src):
        shutil.copy2(src, dst)


def _write_json_if_missing(dst: str, payload: dict) -> None:
    if os.path.isfile(dst):
        return
    os.makedirs(os.path.dirname(dst), exist_ok=True)
    with open(dst, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=True, indent=2)


def _extract_model_version_from_name(name: str) -> str | None:
    stem = Path(name).stem
    if "_" not in stem:
        return None
    prefix = stem.split("_", 1)[0]
    if len(prefix) == 14 and prefix.isdigit():
        return prefix
    return None


def _latest_model_version(model_dir: Path) -> str | None:
    versions: set[str] = set()
    for file in model_dir.glob("*.txt"):
        version = _extract_model_version_from_name(file.name)
        if version:
            versions.add(version)
    if not versions:
        return None
    return sorted(versions)[-1]


def _has_active_ml_model(stocks_db: str) -> bool:
    if not os.path.isfile(stocks_db):
        return False
    try:
        import duckdb
    except Exception:
        return False
    try:
        with duckdb.connect(stocks_db) as conn:
            row = conn.execute(
                "SELECT 1 FROM ml_model_registry WHERE is_active = TRUE LIMIT 1"
            ).fetchone()
        return row is not None
    except Exception:
        return False


def _register_seed_model(stocks_db: str, model_dir: Path, model_version: str) -> None:
    try:
        import duckdb
    except Exception:
        return
    cls_path = model_dir / f"{model_version}_cls.txt"
    reg_path = model_dir / f"{model_version}_reg.txt"
    if (not cls_path.exists()) or (not reg_path.exists()):
        return

    turn_up_path = model_dir / f"{model_version}_turn_up.txt"
    turn_down_path = model_dir / f"{model_version}_turn_down.txt"

    horizon_models: dict[str, dict[str, str | None]] = {}
    for horizon in (5, 10, 20):
        cls_h = model_dir / f"{model_version}_cls_{horizon}.txt"
        reg_h = model_dir / f"{model_version}_reg_{horizon}.txt"
        turn_down_h = model_dir / f"{model_version}_turn_down_{horizon}.txt"
        horizon_models[str(horizon)] = {
            "cls_model_path": str(cls_h) if cls_h.exists() else None,
            "reg_model_path": str(reg_h) if reg_h.exists() else None,
            "turn_down_model_path": str(turn_down_h) if turn_down_h.exists() else None,
        }

    artifact = {
        "cls_model_path": str(cls_path),
        "reg_model_path": str(reg_path),
        "turn_up_model_path": str(turn_up_path) if turn_up_path.exists() else None,
        "turn_down_model_path": str(turn_down_path) if turn_down_path.exists() else None,
        "horizon_models": horizon_models,
    }

    # Keep model registration portable across machines.
    model_key = "ml_ev20_simple_v1"
    objective = "ret20_regression_with_p_up_gate"
    feature_version = 2
    label_version = 3

    try:
        with duckdb.connect(stocks_db) as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS ml_model_registry (
                    model_version TEXT PRIMARY KEY,
                    model_key TEXT,
                    objective TEXT,
                    feature_version INTEGER,
                    label_version INTEGER,
                    train_start_dt INTEGER,
                    train_end_dt INTEGER,
                    metrics_json TEXT,
                    artifact_path TEXT,
                    n_train INTEGER,
                    created_at TIMESTAMP,
                    is_active BOOLEAN
                );
                """
            )
            row = conn.execute(
                "SELECT model_version FROM ml_model_registry WHERE is_active = TRUE LIMIT 1"
            ).fetchone()
            if row:
                return
            conn.execute(
                "UPDATE ml_model_registry SET is_active = FALSE WHERE model_key = ?",
                [model_key],
            )
            conn.execute(
                """
                INSERT OR REPLACE INTO ml_model_registry (
                    model_version,
                    model_key,
                    objective,
                    feature_version,
                    label_version,
                    train_start_dt,
                    train_end_dt,
                    metrics_json,
                    artifact_path,
                    n_train,
                    created_at,
                    is_active
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, TRUE)
                """,
                [
                    model_version,
                    model_key,
                    objective,
                    feature_version,
                    label_version,
                    None,
                    None,
                    "{}",
                    json.dumps(artifact, ensure_ascii=False),
                    0,
                ],
            )
    except Exception:
        return


def _seed_ml_models(paths: dict[str, str]) -> None:
    seed_dir = Path(resolve_path("seed", "models", "ml"))
    if not seed_dir.is_dir():
        return

    target_dir = Path(paths["data_dir"]) / "models" / "ml"
    target_dir.mkdir(parents=True, exist_ok=True)
    for src in seed_dir.glob("*.txt"):
        dst = target_dir / src.name
        if not dst.exists():
            shutil.copy2(src, dst)

    model_version = _latest_model_version(target_dir)
    if not model_version:
        return
    if _has_active_ml_model(paths["stocks_db"]):
        return
    _register_seed_model(paths["stocks_db"], target_dir, model_version)


def _db_has_data(path: str) -> bool:
    if not os.path.isfile(path):
        return False
    try:
        import duckdb
    except Exception:
        return False
    try:
        with duckdb.connect(path, read_only=True) as conn:
            tables = {row[0] for row in conn.execute("SELECT table_name FROM duckdb_tables()").fetchall()}
            if not {"tickers", "daily_bars", "monthly_bars"}.issubset(tables):
                return False
            tickers = conn.execute("SELECT COUNT(*) FROM tickers").fetchone()[0]
            daily = conn.execute("SELECT COUNT(*) FROM daily_bars").fetchone()[0]
            monthly = conn.execute("SELECT COUNT(*) FROM monthly_bars").fetchone()[0]
        return tickers > 0 and daily > 0 and monthly > 0
    except Exception:
        return False


def _count_txt_files(txt_dir: str) -> int:
    try:
        entries = os.listdir(txt_dir)
    except OSError:
        return 0
    return sum(1 for name in entries if name.lower().endswith(".txt") and name.lower() != "code.txt")


def _run_ingest(txt_dir: str, db_path: str) -> bool:
    from importlib import import_module

    sys.path.insert(0, str(base_path()))
    sys.modules.setdefault("db", import_module("app.backend.db"))
    ingest_mod = import_module("app.backend.ingest_txt")
    os.environ["PAN_OUT_TXT_DIR"] = txt_dir
    os.environ["TXT_DATA_DIR"] = txt_dir
    try:
        ingest_mod.ingest()
        return _db_has_data(db_path)
    except Exception:
        traceback.print_exc()
        return False


def _update_loading(window, text: str) -> None:
    try:
        window.evaluate_js(f"window.__setStatus({json.dumps(text)});")
    except Exception:
        pass


def _maximize_window(window) -> None:
    try:
        window.maximize()
    except Exception:
        pass


def _prepare_appdata() -> dict[str, str]:
    # Use config for data_dir resolution logic
    data_root = local_app_dir(APP_NAME)
    data_dir = data_root if data_root.name == "data" else data_root / "data"
    
    # We still use local_app for other non-data stuff? Or just unify?
    # Original 'root' was local_app_dir(APP_NAME).
    # If we are in portable mode, config.DATA_DIR is ./data.
    # We want logs/config/state to follow data_dir ideally?
    # Or keep them separate?
    # Plan says "Log/DB/CSV保存先をdataDir配下に統一".
    # So we should base *everything* on config.DATA_DIR or a parent?
    # config.py defines DATA_DIR = .../data.
    # So ROOT might be implicitly the parent of data_dir?
    # Or we just assume "data", "txt", "logs" are all under DATA_DIR?
    # config.py: LOG_FILE_PATH = DATA_DIR / "logs" / "app.log"
    # So logs are inside DATA_DIR.
    
    # Let's pivot everything to be inside `config.DATA_DIR` for portability!
    
    csv_dir = data_dir / "csv"
    txt_dir = data_dir / "txt"
    config_dir = data_dir / "config"
    state_dir = data_dir / "state"
    logs_dir = data_dir / "logs"
    data_store_dir = data_dir / "data_store"
    
    # Ensure dirs
    for path in (data_dir, csv_dir, config_dir, state_dir, logs_dir, txt_dir, data_store_dir):
        path.mkdir(parents=True, exist_ok=True)

    bundled_db = resolve_path("app", "backend", "stocks.duckdb")
    bundled_favorites = resolve_path("app", "backend", "favorites.sqlite")
    bundled_practice = resolve_path("app", "backend", "practice.sqlite")
    bundled_rank_config = resolve_path("app", "backend", "rank_config.json")
    bundled_update_state = resolve_path("app", "backend", "update_state.json")
    bundled_code_txt = resolve_path("tools", "code.txt")

    stocks_db = str(data_dir / "stocks.duckdb")
    favorites_db = str(data_dir / "favorites.sqlite")
    practice_db = str(data_dir / "practice.sqlite")
    rank_config = str(config_dir / "rank_config.json")
    # Keep update_state in the data root. Older builds used data/state/update_state.json
    # which caused the backend/UI to read stale values depending on env overrides.
    legacy_update_state_path = state_dir / "update_state.json"
    update_state_path = data_dir / "update_state.json"
    update_state = str(update_state_path)
    code_txt = str(data_dir / "code.txt")

    _copy_if_missing(bundled_db, stocks_db)
    _copy_if_missing(bundled_favorites, favorites_db)
    _copy_if_missing(bundled_practice, practice_db)
    _copy_if_missing(bundled_rank_config, rank_config)
    # Prefer bundled defaults if nothing exists; otherwise migrate legacy state forward.
    if legacy_update_state_path.exists():
        try:
            if (not update_state_path.exists()) or (legacy_update_state_path.stat().st_mtime > update_state_path.stat().st_mtime):
                shutil.copy2(str(legacy_update_state_path), str(update_state_path))
        except OSError:
            pass
    if os.path.isfile(bundled_update_state):
        _copy_if_missing(bundled_update_state, update_state)
    else:
        _write_json_if_missing(update_state, {})
    _copy_if_missing(bundled_code_txt, code_txt)

    if os.path.isfile(bundled_db) and os.path.isfile(stocks_db):
        if not _db_has_data(stocks_db) and _db_has_data(bundled_db):
            backup_path = f"{stocks_db}.empty"
            try:
                if os.path.exists(backup_path):
                    os.remove(backup_path)
                os.replace(stocks_db, backup_path)
            except OSError:
                pass
            shutil.copy2(bundled_db, stocks_db)

    # Return dict with STRINGS as requested by consumer
    prepared = {
        "root": str(data_dir.parent), # Guessing parent?
        "data_dir": str(data_dir),
        "data_store_dir": str(data_store_dir),
        "csv_dir": str(csv_dir),
        "config_dir": str(config_dir),
        "state_dir": str(state_dir),
        "logs_dir": str(logs_dir),
        "txt_dir": str(txt_dir),
        "stocks_db": stocks_db,
        "favorites_db": favorites_db,
        "practice_db": practice_db,
        "rank_config": rank_config,
        "update_state": update_state,
        "code_txt": code_txt
    }
    _seed_ml_models(prepared)
    return prepared


def _configure_environment(paths: dict[str, str]) -> None:
    if _is_dev_mode():
        os.environ.setdefault("APP_ENV", "dev")
        os.environ.setdefault("DEBUG", "1")
    else:
        os.environ.setdefault("APP_ENV", "prod")
        os.environ.setdefault("DEBUG", "0")
    # Unify data-dir resolution across split modules (app.core.config vs app.backend.core.config).
    # The backend API uses app.core.config, which prioritizes MEEMEE_DATA_DIR.
    os.environ["MEEMEE_DATA_DIR"] = paths["data_dir"]
    os.environ["MEEMEE_DATA_STORE"] = paths["data_store_dir"]
    os.environ["STOCKS_DB_PATH"] = paths["stocks_db"]
    os.environ["FAVORITES_DB_PATH"] = paths["favorites_db"]
    os.environ["PRACTICE_DB_PATH"] = paths["practice_db"]
    os.environ["RANK_CONFIG_PATH"] = paths["rank_config"]
    os.environ["UPDATE_STATE_PATH"] = paths["update_state"]
    os.environ["PAN_OUT_TXT_DIR"] = paths["txt_dir"]
    os.environ["TXT_DATA_DIR"] = paths["txt_dir"]
    # Prefer external vbs if available (allow user modification).
    # Release builds may ship export_pan.vbs either at the app root or under tools/.
    exe_dir = os.path.dirname(sys.executable)
    external_vbs_candidates = [
        os.path.join(exe_dir, "export_pan.vbs"),
        os.path.join(exe_dir, "tools", "export_pan.vbs"),
    ]
    if getattr(sys, "frozen", False):
        for candidate in external_vbs_candidates:
            if os.path.exists(candidate):
                os.environ["PAN_EXPORT_VBS_PATH"] = candidate
                break
        else:
            os.environ["PAN_EXPORT_VBS_PATH"] = resolve_path("tools", "export_pan.vbs")
    else:
        os.environ["PAN_EXPORT_VBS_PATH"] = resolve_path("tools", "export_pan.vbs")
    os.environ["PAN_CODE_TXT_PATH"] = paths["code_txt"]
    os.environ["STATIC_DIR"] = resolve_path("app", "backend", "static")
    os.environ["TRADE_CSV_DIR"] = paths["csv_dir"]
    os.environ.setdefault(
        "WATCHLIST_TRASH_PATTERNS",
        os.path.join(paths["csv_dir"], "{code}*.csv")
        + ";"
        + os.path.join(paths["txt_dir"], "{code}*.txt")
    )


def _log_resolved_paths_once(paths: dict[str, str]) -> None:
    global _LOGGED_RESOLVED_PATHS
    if _LOGGED_RESOLVED_PATHS:
        return
    _LOGGED_RESOLVED_PATHS = True
    exe_dir = os.path.dirname(sys.executable)
    app_env = os.getenv("APP_ENV", "")
    data_dir = os.getenv("MEEMEE_DATA_DIR", "")
    data_store = os.getenv("MEEMEE_DATA_STORE", "")
    db_path = os.getenv("STOCKS_DB_PATH", paths.get("stocks_db", ""))
    auto_update_enabled = os.getenv("MEEMEE_ENABLE_AUTO_UPDATE", "").lower() in ("1", "true", "yes", "on")
    print(
        "[launcher] Resolved paths:"
        f" exe_dir={exe_dir}"
        f" APP_ENV={app_env}"
        f" MEEMEE_DATA_DIR={data_dir}"
        f" MEEMEE_DATA_STORE={data_store}"
        f" STOCKS_DB_PATH={db_path}"
        f" auto_update_enabled={auto_update_enabled}"
    )
    os.environ["MEEMEE_RESOLVED_PATHS_LOGGED"] = "1"


def _configure_logging(logs_dir: str) -> Path:
    log_path = Path(logs_dir) / "launcher.log"
    log_handle = open(log_path, "a", encoding="utf-8")
    sys.stdout = log_handle
    sys.stderr = log_handle
    return log_path


def _write_app_lock(data_dir: str) -> str | None:
    lock_path = os.path.join(data_dir, "app.lock")
    payload = {
        "pid": os.getpid(),
        "created_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
    }
    try:
        _write_json(lock_path, payload)
        return lock_path
    except Exception as exc:
        print(f"[launcher] Failed to write app lock: {exc}")
        return None


def _remove_app_lock(lock_path: str | None) -> None:
    if not lock_path:
        return
    try:
        if os.path.exists(lock_path):
            os.remove(lock_path)
    except Exception as exc:
        print(f"[launcher] Failed to remove app lock: {exc}")


def _backend_command() -> list[str]:
    if _is_dev_mode() and not getattr(sys, "frozen", False):
        return [sys.executable, "-m", "uvicorn", "app.main:app"]
    if getattr(sys, "frozen", False):
        return [sys.executable, "--backend"]
    return [sys.executable, str(Path(__file__).resolve()), "--backend"]


def _start_backend_process(port: int, backend_log_path: str) -> tuple[subprocess.Popen, object]:
    os.makedirs(os.path.dirname(backend_log_path), exist_ok=True)
    log_handle = open(backend_log_path, "a", encoding="utf-8")
    env = os.environ.copy()
    env["MEEMEE_BACKEND_ONLY"] = "1"
    env["MEEMEE_BACKEND_PORT"] = str(port)
    env.setdefault("PYTHONUNBUFFERED", "1")
    creation_flags = getattr(subprocess, "CREATE_NO_WINDOW", 0)
    cmd = _backend_command()
    if _is_dev_mode() and not getattr(sys, "frozen", False):
        cmd = cmd + ["--host", "127.0.0.1", "--port", str(port)]
        if os.getenv("MEEMEE_DEV_RELOAD", "").lower() in ("1", "true", "yes", "on"):
            cmd.append("--reload")
    proc = subprocess.Popen(
        cmd,
        stdout=log_handle,
        stderr=log_handle,
        env=env,
        cwd=str(base_path()),
        creationflags=creation_flags,
    )
    return proc, log_handle


def _stop_backend_process(proc: subprocess.Popen | None, log_handle: object | None, timeout: float = 5.0) -> None:
    if proc is None:
        if log_handle and hasattr(log_handle, "close"):
            log_handle.close()
        return
    try:
        proc.terminate()
        proc.wait(timeout=timeout)
    except Exception:
        try:
            proc.kill()
        except Exception:
            pass
    if log_handle and hasattr(log_handle, "close"):
        log_handle.close()


def _run_backend_only() -> None:
    from importlib import import_module
    import uvicorn

    port = int(os.getenv("MEEMEE_BACKEND_PORT", "28888"))
    sys.path.insert(0, str(base_path()))
    try:
        sys.modules.setdefault("db", import_module("app.backend.db"))
        sys.modules.setdefault("box_detector", import_module("app.backend.box_detector"))
    except Exception:
        pass
    backend = import_module("app.backend.main")
    config = uvicorn.Config(
        backend.app,
        host="127.0.0.1",
        port=port,
        log_level="info",
        access_log=False,
    )
    server = uvicorn.Server(config=config)
    server.run()


def _run_selftest() -> int:
    os.environ.setdefault("MEEMEE_SELFTEST", "1")
    paths = _prepare_appdata()
    log_path = _configure_logging(paths["logs_dir"])
    _configure_environment(paths)
    _log_resolved_paths_once(paths)

    artifacts_dir = os.path.join(paths["data_dir"], "selftest_artifacts")
    os.makedirs(artifacts_dir, exist_ok=True)
    selftest_log_path = os.path.join(artifacts_dir, "selftest.log")
    log_handle = open(selftest_log_path, "a", encoding="utf-8")

    def log(message: str) -> None:
        timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
        line = f"[selftest] {timestamp} {message}"
        print(line)
        try:
            log_handle.write(line + "\n")
            log_handle.flush()
        except Exception:
            pass

    backend_log_path = os.path.join(paths["logs_dir"], "backend.log")
    proc = None
    proc_log_handle = None
    try:
        log("Starting backend...")
        proc, proc_log_handle = _start_backend_process(28888, backend_log_path)
        ok, err = _wait_for_health_detail(28888, int(os.getenv("MEEMEE_SELFTEST_HEALTH_TIMEOUT", "20")))
        if not ok:
            log(f"FAIL: backend health timeout: {err}")
            _write_text(os.path.join(artifacts_dir, "health_error.txt"), str(err))
            return 1

        log("Fetching heatmap API...")
        heatmap_url = "http://127.0.0.1:28888/api/market/heatmap?period=1d"
        heatmap = _http_get_json(heatmap_url, timeout=10)
        _write_json(os.path.join(artifacts_dir, "api_dump.json"), heatmap)
        items = heatmap.get("items") if isinstance(heatmap, dict) else None
        diagnostics = heatmap.get("diagnostics") if isinstance(heatmap, dict) else None
        if not isinstance(items, list) or not diagnostics:
            log("FAIL: heatmap response missing items/diagnostics")
            return 1
        if not diagnostics.get("industry_master_present"):
            log("FAIL: industry_master_present is false")
            return 1
        if diagnostics.get("industry_master_rows", 0) <= 0:
            log("FAIL: industry_master_rows is 0")
            return 1
        if diagnostics.get("computed_from") != "industry_master":
            log("FAIL: computed_from is fallback")
            return 1
        if len(items) == 0:
            log("FAIL: heatmap items empty")
            return 1
        if all(abs(float(item.get("color", 0) or 0)) < 1e-9 for item in items):
            log("FAIL: heatmap colors all 0.0")
            return 1

        log("Importing trade CSV fixtures...")
        fixtures = [
            (resolve_path("fixtures", "sbi_sample.csv"), "sbi_sample.csv", "sbi"),
            (resolve_path("fixtures", "rakuten_sample.csv"), "rakuten_sample.csv", "rakuten"),
        ]
        trade_results = []
        for path, filename, broker in fixtures:
            if not os.path.isfile(path):
                log(f"FAIL: fixture missing: {path}")
                return 1
            with open(path, "rb") as handle:
                content = handle.read()
            resp = _http_post_multipart(
                "http://127.0.0.1:28888/api/imports/trade-history",
                "file",
                filename,
                content,
                fields={"broker": broker},
                timeout=20,
            )
            trade_results.append(resp)
            ingest = resp.get("ingest") if isinstance(resp, dict) else None
            if not isinstance(ingest, dict):
                ingest = resp if isinstance(resp, dict) else {}
            received = ingest.get("received", 0)
            inserted = ingest.get("inserted", 0)
            ok_flag = None
            if isinstance(resp, dict):
                ok_flag = resp.get("ok")
                if ok_flag is None and resp.get("result") == "success":
                    ok_flag = True
            if not ok_flag or received <= 0 or inserted <= 0:
                log(f"FAIL: trade import failed for {filename}")
                _write_json(os.path.join(artifacts_dir, f"trade_import_{filename}.json"), resp)
                return 1
            _write_json(os.path.join(artifacts_dir, f"trade_import_{filename}.json"), resp)

        log("Launching browser for UI smoke...")
        frontend_base = os.getenv("MEEMEE_SELFTEST_FRONTEND_URL")
        if not frontend_base:
            if _is_dev_mode():
                frontend_base = os.getenv("MEEMEE_DEV_FRONTEND_URL", "http://127.0.0.1:5173")
            else:
                frontend_base = "http://127.0.0.1:28888"
        frontend_base = frontend_base.rstrip("/")
        target_url = f"{frontend_base}/market"

        def _find_external_python() -> str | None:
            for candidate in ("python", "py"):
                resolved = shutil.which(candidate)
                if resolved:
                    return candidate
            return None

        def _run_playwright_external(url: str, screenshot_path: str) -> tuple[str, dict | None]:
            py = _find_external_python()
            if not py:
                log("FAIL: python not found in PATH for external Playwright")
                return "error", None
            log("Installing Playwright via system Python...")
            subprocess.run([py, "-m", "pip", "install", "playwright"], check=False)
            subprocess.run([py, "-m", "playwright", "install", "chromium"], check=False)
            script_path = os.path.join(artifacts_dir, "playwright_selftest.py")
            script = (
                "import json,sys\n"
                "from playwright.sync_api import sync_playwright\n"
                "try:\n"
                "    with sync_playwright() as p:\n"
                "        browser = p.chromium.launch(headless=True)\n"
                "        page = browser.new_page()\n"
                f"        page.goto('{url}', timeout=30000)\n"
                "        page.wait_for_selector('.market-heatmap', timeout=10000)\n"
                "        page.wait_for_selector('.heatmap-canvas[data-heatmap-state]', timeout=15000)\n"
                "        page.wait_for_function(\"() => { const el = document.querySelector('.heatmap-canvas[data-heatmap-state]'); return !!el && el.dataset.heatmapState !== 'loading'; }\", timeout=15000)\n"
                f"        page.screenshot(path=r'{screenshot_path}')\n"
                "        print(json.dumps({'status': 'ok'}))\n"
                "        browser.close()\n"
                "except Exception as e:\n"
                "    print(json.dumps({'status': 'error', 'detail': str(e)}))\n"
            )
            _write_text(script_path, script)
            try:
                out = subprocess.check_output([py, script_path], timeout=60, encoding="utf-8")
                res = json.loads(out)
                return res.get("status", "error"), res
            except Exception as e:
                return "error", {"detail": str(e)}

        screenshot_path = os.path.join(artifacts_dir, "screenshot.png")
        status, detail = _run_playwright_external(target_url, screenshot_path)
        if status != "ok":
            log(f"FAIL: UI smoke test failed: {detail}")
            return 1
        
        log("Selftest passed.")
        return 0

    except Exception as exc:
        log(f"FAIL: Unhandled exception: {exc}")
        traceback.print_exc(file=log_handle)
        return 1
    finally:
        if proc is not None:
            _stop_backend_process(proc, proc_log_handle)
        try:
            log_handle.close()
        except Exception:
            pass


def main() -> None:
    if os.getenv("MEEMEE_BACKEND_ONLY") == "1" or "--backend" in sys.argv:
        _run_backend_only()
        return
    if _is_selftest_mode():
        code = _run_selftest()
        raise SystemExit(code)
    mutex = _acquire_mutex()
    if not mutex:
        _message_box("MeeMee Screener is already running.", WINDOW_TITLE)
        return

    log_path: Path | None = None
    app_lock_path: str | None = None
    try:
        icon_path = resolve_path("resources", "icons", "app_icon.ico")
        if not os.path.isfile(icon_path):
            _message_box(
                f"Missing icon file:\n{icon_path}\nPlace app_icon.ico under resources/icons.",
                WINDOW_TITLE
            )
            return

        paths = _prepare_appdata()
        app_lock_path = _write_app_lock(paths["data_dir"])
        log_path = _configure_logging(paths["logs_dir"])
        _configure_environment(paths)
        _log_resolved_paths_once(paths)

        # Check for .NET Framework 4.8 before initializing pywebview
        if not _check_dotnet_framework():
            error_msg = (
                ".NET Framework 4.8 or higher is not installed.\n\n"
                "Please install it from:\n"
                "https://go.microsoft.com/fwlink/?LinkId=2085155\n\n"
                "Or run portable_bootstrap.ps1 as administrator to install automatically."
            )
            _message_box(error_msg, WINDOW_TITLE)
            return

        if not _check_webview2_runtime():
            error_msg = (
                "Microsoft Edge WebView2 Runtime is not installed.\n\n"
                "Please install it from:\n"
                "https://go.microsoft.com/fwlink/p/?LinkId=2124703\n\n"
                "Or run portable_bootstrap.ps1 as administrator to install automatically."
            )
            _message_box(error_msg, WINDOW_TITLE)
            return


        # Force pywebview to use Edge backend (avoid pythonnet/clr dependency)
        os.environ["PYWEBVIEW_GUI"] = "edgechromium"
        
        import webview
        import base64

        class JsApi:
            def save_screenshot(self, data_uri: str, filename: str) -> dict:
                try:
                    # Remove header if present (data:image/png;base64,...)
                    if "," in data_uri:
                        header, encoded = data_uri.split(",", 1)
                    else:
                        encoded = data_uri

                    data = base64.b64decode(encoded)
                    
                    # Determine save path (Downloads folder)
                    downloads_path = str(Path.home() / "Downloads" / "MeeMeeScreener")
                    os.makedirs(downloads_path, exist_ok=True)
                    save_path = os.path.join(downloads_path, filename)
                    
                    # Write file
                    with open(save_path, "wb") as f:
                        f.write(data)
                        
                    return {
                        "success": True,
                        "savedPath": save_path,
                        "savedDir": downloads_path,
                        "fileName": filename
                    }
                except Exception as e:
                    return {
                        "success": False,
                        "error": str(e)
                    }

            def open_path(self, path: str) -> bool:
                try:
                    if not os.path.exists(path):
                        return False
                    
                    # Select file in explorer
                    if os.path.isfile(path):
                        subprocess.run(['explorer', '/select,', path])
                    else:
                        os.startfile(path)
                    return True
                except Exception:
                    return False

            def open_screenshot_dir(self) -> bool:
                try:
                    downloads_path = str(Path.home() / "Downloads" / "MeeMeeScreener")
                    os.makedirs(downloads_path, exist_ok=True)
                    os.startfile(downloads_path)
                    return True
                except Exception:
                    return False


        # Determine backend parameters
        fixed_port = 28888
        port = fixed_port
        dev_mode = _is_dev_mode()
        if dev_mode:
            port = int(os.getenv("MEEMEE_DEV_BACKEND_PORT", str(fixed_port)))
        
        # Prepare startup surface.
        # In prod, render a lightweight loading page first and load frontend URL after backend is healthy.
        # In dev, keep loading the frontend dev server URL directly.
        
        # We need to find the static directory.
        static_dir = os.environ.get("STATIC_DIR")
        if not static_dir or not os.path.isdir(static_dir):
             # Fallback
             static_dir = resolve_path("app", "backend", "static")
        
        index_html_path = os.path.join(static_dir, "index.html")
        
        start_url = ""
        html_content = ""

        if dev_mode and os.getenv("MEEMEE_DEV_FRONTEND_URL"):
             start_url = os.getenv("MEEMEE_DEV_FRONTEND_URL")
        else:
             # Production / Bundled mode: ensure build exists, then show launcher loading HTML.
             if not os.path.isfile(index_html_path):
                 # Fallback if no build found
                 html_content = _build_error_html("Missing Frontend", f"index.html not found at {index_html_path}", paths, "")
             else:
                 html_content = _build_loading_html()

        # Create window immediately
        window = webview.create_window(
            WINDOW_TITLE,
            url=start_url if start_url else None,
            html=html_content if not start_url else None,
            width=1280,
            height=720,
            resizable=True,
            background_color="#05070f",
            js_api=JsApi(),
            text_select=False 
        )
        
        def _on_shown() -> None:
            _maximize_window(window)

        window.events.shown += _on_shown

        server_state: dict[str, object | None] = {
            "proc": None,
            "log_handle": None,
            "port": port,
            "backend_log": None,
        }

        def _on_closed() -> None:
            proc = server_state.get("proc")
            log_handle = server_state.get("log_handle")
            _stop_backend_process(proc if isinstance(proc, subprocess.Popen) else None, log_handle)

        window.events.closed += _on_closed

        def _bootstrap(win) -> None:
            # Start backend in background immediately
            backend_log_path = os.path.join(paths["logs_dir"], "backend.log")
            server_state["backend_log"] = backend_log_path
            _update_loading(win, "Starting backend...")
            cleaned = _cleanup_stale_backend_processes()
            if cleaned > 0:
                print(f"[launcher] Cleaned up stale backend processes: {cleaned}")
            
            # Check port availability / existing backend health
            final_port = port
            reuse_existing_backend = False
            if not _can_bind_port(final_port):
                existing_ok, existing_err = _wait_for_health_detail(final_port, 3)
                if existing_ok:
                    reuse_existing_backend = True
                    print(f"[launcher] Reusing existing healthy backend on port {final_port}")
                elif existing_err and "status=503" in existing_err:
                    reuse_existing_backend = True
                    print(
                        f"[launcher] Reusing existing backend still starting on port {final_port}: {existing_err}"
                    )
                else:
                    killed = _terminate_unhealthy_backend_on_port(final_port)
                    if killed > 0:
                        time.sleep(0.4)
                    if _can_bind_port(final_port):
                        print(f"[launcher] Reclaimed backend port {final_port} after terminating stale process")
                    else:
                        fallback_port = _find_free_port()
                        print(
                            f"[launcher] Port {final_port} is busy and unhealthy ({existing_err}); "
                            f"retrying with free port {fallback_port}"
                        )
                        final_port = fallback_port

            if not reuse_existing_backend:
                try:
                    proc, log_handle = _start_backend_process(final_port, backend_log_path)
                    server_state["proc"] = proc
                    server_state["log_handle"] = log_handle
                except Exception as exc:
                    print(f"[launcher] Backend start failed: {exc}")
                    # Continue; we may still have an existing healthy backend on the fixed port.

            _update_loading(win, "Waiting for backend health...")
            health_timeout = _get_health_timeout_seconds()
            ok, health_err = _wait_for_health_detail(final_port, health_timeout)
            if not ok:
                proc = server_state.get("proc")
                log_handle = server_state.get("log_handle")
                exit_note = ""
                if isinstance(proc, subprocess.Popen):
                    rc = proc.poll()
                    if rc is not None:
                        exit_note = f" backend_exit={rc}"
                _stop_backend_process(proc if isinstance(proc, subprocess.Popen) else None, log_handle)
                server_state["proc"] = None
                server_state["log_handle"] = None
                error_html = _build_error_html(
                    "Backend failed to start",
                    "Backend did not become ready on /api/health.",
                    paths,
                    backend_log_path,
                    health_error=f"{health_err or 'health_check_timeout'} (timeout={health_timeout}s){exit_note}",
                )
                _show_error_page(win, error_html)
                return

            server_state["port"] = final_port
            _maximize_window(win)
            _update_loading(win, "Loading frontend...")
            if dev_mode and os.getenv("MEEMEE_DEV_FRONTEND_URL"):
                win.load_url(os.getenv("MEEMEE_DEV_FRONTEND_URL"))
            else:
                win.load_url(f"http://127.0.0.1:{final_port}/?t={int(time.time())}")
                _schedule_frontend_watchdog(win, paths, backend_log_path)
            threading.Timer(0.2, _maximize_window, args=(win,)).start()
            
            # Update Check (async)
            try:
                 enable_updates = os.getenv("MEEMEE_ENABLE_AUTO_UPDATE", "").lower() in ("1", "true", "yes", "on")
                 if _is_dev_mode():
                     enable_updates = False
                 if enable_updates:
                     from app.backend.infra.google_drive.update_client import UpdateClient
                     client = UpdateClient()
                     current_ver = "2.0.0"
                     update = client.check_for_updates(current_ver)
                     if update:
                         do_update = ctypes.windll.user32.MessageBoxW(
                             0, 
                             f"New version {update.version} is available.\n\n{update.notes}\n\nUpdate now?", 
                             "Update Available", 
                             4
                         ) == 6
                         if do_update:
                             # We can't easily show loading screen anymore because we are already in the app.
                             # This UX might need revisiting later, but for now we focus on startup speed.
                             # Maybe we can use window.evaluate_js to show a modal in React?
                             # For now, just shelling out to updater might be abrupt.
                             pass
            except Exception:
                pass


        # Trigger bootstrap after window creation
        # webview.start blocks, so we can't run code after it in main thread easily unless we use the func argument
        webview.start(_bootstrap, window, icon=icon_path, private_mode=False, storage_path=paths["state_dir"])
    except Exception as exc:
        detail = "".join(traceback.format_exception(exc))
        if log_path:
            print(detail)
            _message_box(f"Launch failed. See log:\n{log_path}", WINDOW_TITLE)
        else:
            fallback = Path(sys.executable).parent / "launcher_error.log"
            try:
                fallback.write_text(detail, encoding="utf-8")
                _message_box(f"Launch failed. See log:\n{fallback}", WINDOW_TITLE)
            except Exception:
                _message_box(f"Launch failed:\n{exc}", WINDOW_TITLE)
    finally:
        _remove_app_lock(app_lock_path)
        _release_mutex(mutex)


if __name__ == "__main__":
    main()
