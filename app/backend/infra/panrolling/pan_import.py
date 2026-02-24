"""Automate pandtmgr.exe F5 (Pan Rolling data import) via pywinauto.

This module launches the Pan Data Manager (pandtmgr.exe), sends F5 to
trigger "Pan Rolling" data import, waits for completion, and closes the
application.  The GUI will be visible briefly during the operation.
"""
from __future__ import annotations

import logging
import os
import time

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
DEFAULT_PANDTMGR_PATH = r"C:\Program Files (x86)\Pan\pandtmgr.exe"
WINDOW_TITLE_RE = r"Pan.*"  # window title is locale/encoding dependent; keep match broad
IMPORT_TIMEOUT = 120  # seconds to wait for the import to complete
WINDOW_CONNECT_TIMEOUT = 15  # seconds to wait for the app to launch
PYWINAUTO_BACKENDS = ("uia", "win32")
COMPLETION_BUTTON_TITLES = ("OK", "閉じる", "Close")
COMPLETION_BUTTON_KEYWORDS = ("ok", "close", "閉じる")
COMPLETION_DIALOG_MARKERS = ("結果表示", "結果表示(s)", "ｷｬﾝｾﾙ", "キャンセル", "ﾍﾙﾌﾟ", "ヘルプ")
COMPLETION_TEXT_KEYWORDS = ("更新を終了", "完了", "終了しました", "100%")


def run_pan_import(
    pandtmgr_path: str | None = None,
    timeout: int = IMPORT_TIMEOUT,
) -> bool:
    """Run Pan Rolling F5 import via GUI automation.

    Args:
        pandtmgr_path: Full path to pandtmgr.exe.
        timeout: Max seconds to wait for import completion.

    Returns:
        True if import completed successfully, False otherwise.
    """
    path = pandtmgr_path or DEFAULT_PANDTMGR_PATH
    if not os.path.isfile(path):
        logger.error("pandtmgr.exe not found: %s", path)
        return False

    try:
        from pywinauto import Application, timings  # type: ignore
    except ImportError:
        logger.error("pywinauto is not installed – cannot automate Pan import")
        return False

    for backend in PYWINAUTO_BACKENDS:
        app: Application | None = None
        try:
            logger.info("Launching pandtmgr.exe: %s (backend=%s)", path, backend)
            app = Application(backend=backend).start(path)

            # Wait for the main window to appear
            timings.wait_until_passes(
                WINDOW_CONNECT_TIMEOUT,
                0.5,
                lambda: app.window(title_re=WINDOW_TITLE_RE),
            )
            main_spec = app.window(title_re=WINDOW_TITLE_RE)
            main_spec.wait("ready", timeout=WINDOW_CONNECT_TIMEOUT)
            main_win = main_spec.wrapper_object()
            logger.info("pandtmgr window ready (backend=%s)", backend)

            # Give the app a moment to fully initialise
            time.sleep(1.0)

            # Send F5 to trigger Pan Rolling import
            logger.info("Sending F5 (Pan Rolling import)...")
            try:
                main_win.set_focus()
            except Exception as exc:
                logger.debug("Failed to focus pandtmgr window (non-fatal): %s", exc)
            main_win.type_keys("{F5}")

            # Wait for the import progress dialog to appear and then close
            # pandtmgr shows a progress dialog during import; when import is
            # done it may show a completion message box or just return focus
            # to the main window.
            completed = _wait_for_import_completion(app, main_win, timeout)
            if not completed:
                logger.warning("Pan import completion was not confirmed before timeout")
                return False

            logger.info("Pan Rolling import completed successfully")
            return True

        except Exception:
            logger.exception("Pan import automation failed (backend=%s)", backend)
        finally:
            _close_app_safely(app)

    return False


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _wait_for_import_completion(
    app: "Application",
    main_win: "object",
    timeout: int,
) -> bool:
    """Wait until the import operation finishes.

    Strategy:
    1. Detect any modal dialog (progress / completion).
    2. Poll until only the main window remains (import done).
    3. If a completion message box appears, dismiss it.
    """
    start = time.time()
    import_started = False

    while time.time() - start < timeout:
        time.sleep(1.0)

        try:
            # Some Pan versions show completion controls as descendants of the
            # main window (not as a separate top-level dialog).
            if _dismiss_embedded_completion_dialog(main_win):
                logger.info("Dismissed embedded completion dialog")
                return True

            # Look for any dialog / popup on top of the main window
            dialogs = app.windows()
            non_main = [
                d for d in dialogs
                if d.handle != main_win.handle and d.is_visible()
            ]

            if non_main:
                import_started = True
                # Check if any dialog has a completion button and close it.
                for dlg in non_main:
                    if _dismiss_completion_dialog(dlg):
                        logger.info("Dismissing completion dialog")
                        time.sleep(0.5)
                        return True
                # Still importing – continue polling
                continue

            # No non-main dialogs visible
            if import_started:
                # Import was running and all dialogs closed → done
                logger.info("Import dialog closed – import complete")
                return True

        except Exception as exc:
            logger.debug("Polling error (non-fatal): %s", exc)
            continue

    logger.warning("Import wait timed out after %ds", timeout)
    return False


def _dismiss_completion_dialog(dialog: "object") -> bool:
    """Try to close completion dialogs shown after import finishes."""
    for title in COMPLETION_BUTTON_TITLES:
        try:
            btn = dialog.child_window(title=title, class_name="Button")
            if btn.exists(timeout=0.2):
                if _click_control(btn):
                    return True
        except Exception:
            continue

    try:
        buttons = dialog.descendants(control_type="Button")
    except Exception:
        buttons = []

    for btn in buttons:
        try:
            text = str(btn.window_text() or "").strip().replace("&", "").lower()
            if any(keyword in text for keyword in COMPLETION_BUTTON_KEYWORDS):
                if _click_control(btn):
                    return True
        except Exception:
            continue

    if _has_completion_text(dialog):
        return _press_enter(dialog)
    return _press_enter(dialog) if buttons else False


def _dismiss_embedded_completion_dialog(main_win: "object") -> bool:
    """Close completion controls embedded under the main window."""
    buttons: list[tuple[object, str, str]] = []
    try:
        for btn in main_win.descendants(control_type="Button"):
            text = str(btn.window_text() or "").strip()
            norm = text.replace("&", "").replace(" ", "").lower()
            if not norm:
                continue
            buttons.append((btn, text, norm))
    except Exception:
        return False

    if not buttons:
        return False

    marker_found = any(any(marker in norm for marker in COMPLETION_DIALOG_MARKERS) for _, _, norm in buttons)
    if not marker_found:
        marker_found = _has_completion_text(main_win)
    if not marker_found:
        return False

    # Prefer lower-positioned "閉じる" button to avoid the main frame close button.
    candidates: list[tuple[int, object]] = []
    for btn, _, norm in buttons:
        if not any(keyword in norm for keyword in COMPLETION_BUTTON_KEYWORDS):
            continue
        top = -1
        try:
            top = int(btn.rectangle().top)
        except Exception:
            pass
        candidates.append((top, btn))

    for _, btn in sorted(candidates, key=lambda item: item[0], reverse=True):
        try:
            if hasattr(btn, "is_enabled") and not btn.is_enabled():
                continue
            if _click_control(btn):
                return True
        except Exception:
            continue

    return _press_enter(main_win)


def _click_control(control: "object") -> bool:
    try:
        control.click()
        return True
    except Exception:
        return False


def _press_enter(window: "object") -> bool:
    try:
        try:
            window.set_focus()
        except Exception:
            pass
        window.type_keys("{ENTER}")
        return True
    except Exception:
        return False


def _has_completion_text(window: "object") -> bool:
    try:
        text_controls = window.descendants(control_type="Text")
    except Exception:
        text_controls = []
    for ctrl in text_controls:
        try:
            text = str(ctrl.window_text() or "").strip()
            if any(keyword in text for keyword in COMPLETION_TEXT_KEYWORDS):
                return True
        except Exception:
            continue
    return False


def _close_app_safely(app: "Application | None") -> None:
    """Best-effort close of pandtmgr.exe."""
    if app is None:
        return
    try:
        for win in app.windows():
            try:
                win.close()
            except Exception:
                pass
        time.sleep(0.5)
        # If still running, kill
        if app.is_process_running():
            app.kill()
    except Exception:
        logger.debug("Error closing pandtmgr (non-fatal)", exc_info=True)
