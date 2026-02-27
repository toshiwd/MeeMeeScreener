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


def _env_int(name: str, default: int, *, minimum: int) -> int:
    raw = os.getenv(name)
    try:
        value = int(raw) if raw is not None else int(default)
    except (TypeError, ValueError):
        value = int(default)
    return max(int(minimum), value)


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
DEFAULT_PANDTMGR_PATH = r"C:\Program Files (x86)\Pan\pandtmgr.exe"
WINDOW_TITLE_RE = r"Pan.*"  # window title is locale/encoding dependent; keep match broad
IMPORT_TIMEOUT = _env_int("MEEMEE_PAN_IMPORT_TIMEOUT", 240, minimum=60)
WINDOW_CONNECT_TIMEOUT = _env_int("MEEMEE_PAN_WINDOW_CONNECT_TIMEOUT", 15, minimum=5)
SETTLE_WAIT_SECONDS = _env_int("MEEMEE_PAN_IMPORT_SETTLE_SECONDS", 5, minimum=2)
# If an import-like dialog is still open near timeout, nudge it once with Enter.
DIALOG_NUDGE_BEFORE_TIMEOUT = _env_int("MEEMEE_PAN_DIALOG_NUDGE_BEFORE_TIMEOUT", 90, minimum=20)
UNKNOWN_DIALOG_START_POLLS = _env_int("MEEMEE_PAN_UNKNOWN_DIALOG_START_POLLS", 3, minimum=1)
# Prefer win32 first: Pan is a legacy desktop app and UIA can miss dialog details.
PYWINAUTO_BACKENDS = ("win32", "uia")
COMPLETION_BUTTON_TITLES = ("OK", "閉じる", "Close")
COMPLETION_BUTTON_KEYWORDS = ("ok", "close", "閉じる")
COMPLETION_DIALOG_MARKERS = ("結果表示", "結果表示(s)", "ｷｬﾝｾﾙ", "キャンセル", "ﾍﾙﾌﾟ", "ヘルプ")
COMPLETION_TEXT_KEYWORDS = ("更新を終了", "完了", "終了しました", "100%")
IMPORT_DIALOG_KEYWORDS = (
    "データ更新",
    "ﾃﾞｰﾀ更新",
    "更新",
    "取り込み",
    "import",
)
RUNNING_BUTTON_KEYWORDS = (
    "cancel",
    "キャンセル",
    "ｷｬﾝｾﾙ",
    "中止",
)
RUNNING_TEXT_KEYWORDS = (
    "更新中",
    "処理中",
    "取り込み中",
)


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
        from pywinauto.keyboard import send_keys  # type: ignore
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
            sent = False
            for key in ("{VK_F5}", "{F5}"):
                try:
                    main_win.type_keys(key, set_foreground=True, with_vk_packet=False)
                    sent = True
                    logger.debug("Sent %s via main window", key)
                    break
                except Exception as exc:
                    logger.debug("Failed to send %s via main window: %s", key, exc)
            if not sent:
                send_keys("{F5}")
                logger.debug("Sent {F5} via global keyboard fallback")

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
    settled_since: float | None = None
    unknown_non_main_polls = 0
    dialog_nudged = False

    while time.time() - start < timeout:
        time.sleep(1.0)

        try:
            # Some Pan versions show completion controls as descendants of the
            # main window (not as a separate top-level dialog).
            if _dismiss_embedded_completion_dialog(main_win):
                logger.info("Dismissed embedded completion dialog")
                import_started = True
                settled_since = time.time()

            # Look for any dialog / popup on top of the main window
            dialogs = app.windows()
            non_main = [
                d for d in dialogs
                if d.handle != main_win.handle and d.is_visible()
            ]

            if non_main:
                settled_since = None
                # A visible non-main dialog can be startup noise.
                # Mark "started" only when it looks import-related.
                dismissed = False
                related_found = False
                for dlg in non_main:
                    if _is_import_related_dialog(dlg):
                        related_found = True
                        import_started = True
                    if _dismiss_completion_dialog(dlg):
                        dismissed = True
                if related_found:
                    unknown_non_main_polls = 0
                else:
                    unknown_non_main_polls += 1
                    # Some 32/64-bit combinations return mojibake window titles.
                    # Treat persistent non-main dialogs as import started.
                    if unknown_non_main_polls >= UNKNOWN_DIALOG_START_POLLS:
                        import_started = True

                elapsed = time.time() - start
                if (
                    import_started
                    and (not dialog_nudged)
                    and elapsed >= max(20, float(timeout) - float(DIALOG_NUDGE_BEFORE_TIMEOUT))
                ):
                    if _nudge_dialog(non_main[0]):
                        logger.info("Nudged import dialog with Enter near timeout")
                    dialog_nudged = True
                if dismissed:
                    logger.info("Dismissing completion dialog")
                    import_started = True
                    settled_since = time.time()
                # Still importing – continue polling
                continue

            unknown_non_main_polls = 0
            if _has_running_indicator(main_win):
                import_started = True
                settled_since = None
                continue

            # No non-main dialogs visible
            if import_started:
                if settled_since is None:
                    settled_since = time.time()
                if time.time() - settled_since >= SETTLE_WAIT_SECONDS:
                    logger.info("Import settled for %ss - import complete", SETTLE_WAIT_SECONDS)
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

    for btn in _iter_controls_by_class(dialog, "button"):
        try:
            text = _normalize_text(btn.window_text())
            if any(keyword in text for keyword in COMPLETION_BUTTON_KEYWORDS):
                if hasattr(btn, "is_enabled") and (not btn.is_enabled()):
                    continue
                if _click_control(btn):
                    return True
        except Exception:
            continue

    if _has_completion_text(dialog):
        return _press_enter(dialog)
    return False


def _is_import_related_dialog(dialog: "object") -> bool:
    """Heuristic check to avoid treating unrelated popups as import progress."""
    texts = _collect_dialog_texts(dialog)
    for text in texts:
        norm = text.replace("&", "").replace(" ", "").lower()
        if any(keyword in norm for keyword in IMPORT_DIALOG_KEYWORDS):
            return True
        if any(keyword in norm for keyword in RUNNING_BUTTON_KEYWORDS):
            return True
        if any(keyword in norm for keyword in RUNNING_TEXT_KEYWORDS):
            return True
        if any(keyword in norm for keyword in COMPLETION_TEXT_KEYWORDS):
            return True
        if "%" in norm:
            return True
    return False


def _collect_dialog_texts(dialog: "object") -> list[str]:
    texts: list[str] = []
    try:
        title = str(dialog.window_text() or "").strip()
        if title:
            texts.append(title)
    except Exception:
        pass

    for ctrl in _iter_controls(dialog):
        try:
            text = str(ctrl.window_text() or "").strip()
            if text:
                texts.append(text)
        except Exception:
            continue

    return texts


def _nudge_dialog(dialog: "object") -> bool:
    try:
        try:
            dialog.set_focus()
        except Exception:
            pass
        dialog.type_keys("{ENTER}")
        return True
    except Exception:
        return False


def _dismiss_embedded_completion_dialog(main_win: "object") -> bool:
    """Close completion controls embedded under the main window."""
    buttons: list[tuple[object, str, str]] = []
    for btn in _iter_controls_by_class(main_win, "button"):
        try:
            text = str(btn.window_text() or "").strip()
            norm = _normalize_text(text)
            if not norm:
                continue
            buttons.append((btn, text, norm))
        except Exception:
            continue

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
    for text in _collect_dialog_texts(window):
        norm = _normalize_text(text)
        if any(keyword in norm for keyword in COMPLETION_TEXT_KEYWORDS):
            return True
        if "100%" in norm:
            return True
    return False


def _has_running_indicator(window: "object") -> bool:
    for btn in _iter_controls_by_class(window, "button"):
        try:
            text = _normalize_text(btn.window_text())
            if any(keyword in text for keyword in RUNNING_BUTTON_KEYWORDS):
                return True
        except Exception:
            continue

    for text in _collect_dialog_texts(window):
        norm = _normalize_text(text)
        if not norm:
            continue
        # Keep waiting while progress is not yet complete.
        if "%" in norm and "100%" not in norm:
            return True
        if any(keyword in norm for keyword in RUNNING_TEXT_KEYWORDS):
            return True
    return False


def _normalize_text(value: object) -> str:
    return str(value or "").strip().replace("&", "").replace(" ", "").lower()


def _iter_controls(window: "object") -> list[object]:
    controls: list[object] = []
    seen: set[int] = set()

    def _append(items: list[object]) -> None:
        for ctrl in items:
            try:
                handle = int(getattr(ctrl, "handle", 0) or 0)
            except Exception:
                handle = 0
            if handle and handle in seen:
                continue
            if handle:
                seen.add(handle)
            controls.append(ctrl)

    try:
        _append(list(window.children()))
    except Exception:
        pass
    try:
        _append(list(window.descendants()))
    except Exception:
        pass
    return controls


def _iter_controls_by_class(window: "object", class_keyword: str) -> list[object]:
    out: list[object] = []
    needle = class_keyword.lower()
    for ctrl in _iter_controls(window):
        try:
            klass = str(ctrl.friendly_class_name() or "").lower()
        except Exception:
            klass = ""
        if needle in klass:
            out.append(ctrl)
    return out


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
