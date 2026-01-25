from __future__ import annotations

import ctypes
import json
import os
import shutil
import socket
import sys
import threading
import time
import urllib.request
from pathlib import Path

import traceback


from app.desktop.runtime_paths import base_path, local_app_dir, resolve_path
from app.backend.core.config import config

APP_NAME = "MeeMeeScreener"
WINDOW_TITLE = "MeeMee Screener"
MUTEX_NAME = "Global\\MeeMeeScreenerSingleton"
HEALTH_TIMEOUT_SECONDS = 10


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


def _wait_for_health(port: int, timeout_seconds: int) -> bool:
    deadline = time.monotonic() + timeout_seconds
    url = f"http://127.0.0.1:{port}/health"
    while time.monotonic() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=1) as response:
                if response.status == 200:
                    return True
        except Exception:
            time.sleep(0.2)
    return False


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


def _loading_html() -> str:
    return """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>MeeMee Screener</title>
  <style>
    :root {
      color-scheme: light;
    }
    body {
      margin: 0;
      font-family: "Segoe UI", "Meiryo", sans-serif;
      background: #0f172a;
      color: #e2e8f0;
      display: grid;
      place-items: center;
      height: 100vh;
    }
    .card {
      width: min(520px, 86vw);
      background: #111827;
      border: 1px solid #1f2937;
      border-radius: 18px;
      padding: 24px 28px;
      box-shadow: 0 20px 40px rgba(15, 23, 42, 0.5);
    }
    .title {
      font-size: 18px;
      margin: 0 0 8px;
    }
    .status {
      font-size: 14px;
      color: #94a3b8;
    }
    .bar {
      margin-top: 16px;
      height: 6px;
      background: #1e293b;
      border-radius: 999px;
      overflow: hidden;
    }
    .bar span {
      display: block;
      height: 100%;
      width: 40%;
      background: linear-gradient(90deg, #38bdf8, #6366f1);
      animation: slide 1.2s ease-in-out infinite;
      border-radius: 999px;
    }
    @keyframes slide {
      0% { transform: translateX(-60%); }
      50% { transform: translateX(60%); }
      100% { transform: translateX(-60%); }
    }
  </style>
</head>
<body>
  <div class="card">
    <h1 class="title">MeeMee Screener</h1>
    <div id="status" class="status">Starting...</div>
    <div class="bar"><span></span></div>
  </div>
  <script>
    window.__setStatus = function (text) {
      var el = document.getElementById("status");
      if (el) { el.textContent = text; }
    };
  </script>
</body>
</html>
"""


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
    data_dir = config.DATA_DIR
    
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
    txt_dir = config.PAN_OUT_TXT_DIR # which is data_dir / "txt" via config
    config_dir = data_dir / "config"
    state_dir = data_dir / "state"
    logs_dir = data_dir / "logs"
    
    # Ensure dirs
    for path in (data_dir, csv_dir, config_dir, state_dir, logs_dir, txt_dir):
        path.mkdir(parents=True, exist_ok=True)

    bundled_db = resolve_path("app", "backend", "stocks.duckdb")
    bundled_favorites = resolve_path("app", "backend", "favorites.sqlite")
    bundled_practice = resolve_path("app", "backend", "practice.sqlite")
    bundled_rank_config = resolve_path("app", "backend", "rank_config.json")
    bundled_update_state = resolve_path("app", "backend", "update_state.json")
    bundled_code_txt = resolve_path("tools", "code.txt")

    stocks_db = str(config.DB_PATH)
    favorites_db = str(config.FAVORITES_DB_PATH)
    practice_db = str(config.PRACTICE_DB_PATH)
    rank_config = str(config_dir / "rank_config.json")
    update_state = str(state_dir / "update_state.json")
    code_txt = str(data_dir / "code.txt")

    _copy_if_missing(bundled_db, stocks_db)
    _copy_if_missing(bundled_favorites, favorites_db)
    _copy_if_missing(bundled_practice, practice_db)
    _copy_if_missing(bundled_rank_config, rank_config)
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
    return {
        "root": str(data_dir.parent), # Guessing parent?
        "data_dir": str(data_dir),
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


def _configure_environment(paths: dict[str, str]) -> None:
    os.environ.setdefault("APP_ENV", "prod")
    os.environ.setdefault("DEBUG", "0")
    os.environ["STOCKS_DB_PATH"] = paths["stocks_db"]
    os.environ["FAVORITES_DB_PATH"] = paths["favorites_db"]
    os.environ["PRACTICE_DB_PATH"] = paths["practice_db"]
    os.environ["RANK_CONFIG_PATH"] = paths["rank_config"]
    os.environ["UPDATE_STATE_PATH"] = paths["update_state"]
    os.environ["PAN_OUT_TXT_DIR"] = paths["txt_dir"]
    os.environ["TXT_DATA_DIR"] = paths["txt_dir"]
    # Prefer external tools folder if available (allow user modification)
    external_tools_vbs = os.path.join(os.path.dirname(sys.executable), "tools", "export_pan.vbs")
    if getattr(sys, "frozen", False) and os.path.exists(external_tools_vbs):
        os.environ["PAN_EXPORT_VBS_PATH"] = external_tools_vbs
    else:
        os.environ["PAN_EXPORT_VBS_PATH"] = resolve_path("tools", "export_pan.vbs")
    os.environ["PAN_CODE_TXT_PATH"] = paths["code_txt"]
    os.environ["STATIC_DIR"] = resolve_path("app", "backend", "static")
    os.environ["TRADE_CSV_DIR"] = paths["data_dir"]
    os.environ.setdefault(
        "WATCHLIST_TRASH_PATTERNS",
        os.path.join(paths["csv_dir"], "{code}*.csv")
        + ";"
        + os.path.join(paths["txt_dir"], "{code}*.txt")
    )


def _configure_logging(logs_dir: str) -> Path:
    log_path = Path(logs_dir) / "launcher.log"
    log_handle = open(log_path, "a", encoding="utf-8")
    sys.stdout = log_handle
    sys.stderr = log_handle
    return log_path


def _start_server(port: int):
    from importlib import import_module
    import uvicorn

    sys.path.insert(0, str(base_path()))
    try:
        sys.modules.setdefault("db", import_module("app.backend.db"))
        sys.modules.setdefault("box_detector", import_module("app.backend.box_detector"))
    except Exception:
        # Backend aliases are best-effort for frozen imports.
        pass
    backend = import_module("app.backend.main")
    config = uvicorn.Config(
        backend.app,
        host="127.0.0.1",
        port=port,
        log_level="info",
        access_log=False
    )
    server = uvicorn.Server(config=config)
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    return server, thread


def _stop_server(server, thread: threading.Thread, timeout: float = 5.0) -> None:
    server.should_exit = True
    server.force_exit = True
    thread.join(timeout=timeout)
    if thread.is_alive():
        os._exit(0)


def main() -> None:
    mutex = _acquire_mutex()
    if not mutex:
        _message_box("MeeMee Screener is already running.", WINDOW_TITLE)
        return

    if not mutex:
        _message_box("MeeMee Screener is already running.", WINDOW_TITLE)
        return

    import sys
    log_path: Path | None = None
    try:
        icon_path = resolve_path("resources", "icons", "app_icon.ico")
        if not os.path.isfile(icon_path):
            _message_box(
                f"Missing icon file:\n{icon_path}\nPlace app_icon.ico under resources/icons.",
                WINDOW_TITLE
            )
            return

        paths = _prepare_appdata()
        log_path = _configure_logging(paths["logs_dir"])
        _configure_environment(paths)

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
        import subprocess

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


        window = webview.create_window(
            WINDOW_TITLE,
            html=_loading_html(),
            width=1280,
            height=720,
            resizable=True,
            js_api=JsApi()
        )
        def _on_shown() -> None:
            _maximize_window(window)

        window.events.shown += _on_shown

        server_state: dict[str, object | None] = {"server": None, "thread": None}

        def _on_closed() -> None:
            server = server_state.get("server")
            thread = server_state.get("thread")
            if server and thread:
                _stop_server(server, thread)

        window.events.closed += _on_closed

        def _bootstrap(win) -> None:
            if os.path.isfile(paths["stocks_db"]) and not _db_has_data(paths["stocks_db"]):
                if _count_txt_files(paths["txt_dir"]) > 0:
                    _update_loading(win, "Loading data files...")
                    _run_ingest(paths["txt_dir"], paths["stocks_db"])

            _update_loading(win, "Starting backend...")
            # Use a fixed port to ensure LocalStorage persistence (Origin must stay same)
            fixed_port = 28888
            port = fixed_port
            if _wait_for_health(port, 1):
                _update_loading(win, "Opening app...")
                _maximize_window(win)
                win.load_url(f"http://127.0.0.1:{port}/?t={int(time.time())}")
                threading.Timer(0.2, _maximize_window, args=(win,)).start()
                return
            if not _can_bind_port(port):
                if _wait_for_health(port, HEALTH_TIMEOUT_SECONDS):
                    _update_loading(win, "Opening app...")
                    _maximize_window(win)
                    win.load_url(f"http://127.0.0.1:{port}/?t={int(time.time())}")
                    threading.Timer(0.2, _maximize_window, args=(win,)).start()
                    return
                alt_port = _find_free_port()
                _update_loading(win, f"Port {fixed_port} in use. Switching to {alt_port}...")
                print(f"[launcher] Port {fixed_port} in use. Switching to {alt_port}.")
                port = alt_port
            server, thread = _start_server(port)
            server_state["server"] = server
            server_state["thread"] = thread

            if not _wait_for_health(port, HEALTH_TIMEOUT_SECONDS):
                if _wait_for_health(port, 3):
                    _update_loading(win, "Opening app...")
                    _maximize_window(win)
                    win.load_url(f"http://127.0.0.1:{port}/?t={int(time.time())}")
                    threading.Timer(0.2, _maximize_window, args=(win,)).start()
                    return
                if port == fixed_port and not _can_bind_port(fixed_port):
                    alt_port = _find_free_port()
                    _update_loading(win, f"Port {fixed_port} in use. Switching to {alt_port}...")
                    print(f"[launcher] Port {fixed_port} in use. Switching to {alt_port}.")
                    _stop_server(server, thread)
                    server, thread = _start_server(alt_port)
                    server_state["server"] = server
                    server_state["thread"] = thread
                    port = alt_port
                    if _wait_for_health(port, HEALTH_TIMEOUT_SECONDS):
                        _update_loading(win, "Opening app...")
                        _maximize_window(win)
                        win.load_url(f"http://127.0.0.1:{port}/?t={int(time.time())}")
                        threading.Timer(0.2, _maximize_window, args=(win,)).start()
                        return
                _update_loading(win, "Backend failed to start.")
                _stop_server(server, thread)
                _message_box("Backend failed to start. See logs for details.", WINDOW_TITLE)
                try:
                    win.destroy()
                except Exception:
                    pass
                return

            _update_loading(win, "Opening app...")
            _maximize_window(win)
            win.load_url(f"http://127.0.0.1:{port}/?t={int(time.time())}")
            threading.Timer(0.2, _maximize_window, args=(win,)).start()

        # Use None to let pywebview auto-detect the best available backend
        # EdgeChromium requires WebView2 runtime, falls back to mshtml if unavailable
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
        _release_mutex(mutex)


if __name__ == "__main__":
    main()
