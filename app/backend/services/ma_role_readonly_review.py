from __future__ import annotations

import json
from pathlib import Path
from typing import Any
from datetime import datetime, timezone

from scripts import tradex_ma_role_transition_research_phase15 as phase15


DEFAULT_CATALOG_ROOT = Path(r"G:\Tradex\ma_role_meemee_readonly_catalog_phase16")
MAX_CHART_MARKERS = 8


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _latest_catalog_path(root: Path = DEFAULT_CATALOG_ROOT) -> Path | None:
    latest_path = root / "latest_research_decision.json"
    if not latest_path.exists():
        return None
    latest = _read_json(latest_path)
    run_root = Path(str(latest.get("run_root") or ""))
    catalog_path = run_root / "meemee_readonly_signal_catalog.json"
    return catalog_path if catalog_path.exists() else None


def _to_phase15_row(row: tuple) -> dict[str, Any]:
    return {
        "open": float(row[1]),
        "high": float(row[2]),
        "low": float(row[3]),
        "close": float(row[4]),
        "ma7": float(row[6]),
        "ma20": float(row[7]),
        "ma60": float(row[8]),
        "ma100": float(row[9]),
        "ma200": float(row[10]),
    }


def _date_to_iso(value: Any) -> str | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    if raw.isdigit():
        if len(raw) >= 10:
            try:
                return datetime.fromtimestamp(int(raw[:10]), tz=timezone.utc).date().isoformat()
            except Exception:
                return raw
        if len(raw) == 8:
            return f"{raw[:4]}-{raw[4:6]}-{raw[6:8]}"
    return raw


def _state_at(confirmed_rows: list[tuple], index: int) -> dict[str, str] | None:
    if index < 20 or index < 5 or index < 2:
        return None
    selected = confirmed_rows[index]
    previous1 = confirmed_rows[index - 1]
    previous2 = confirmed_rows[index - 2]
    row = _to_phase15_row(selected)
    prev5 = confirmed_rows[index - 5]
    prev20 = confirmed_rows[index - 20]
    row.update(
        {
            "open_prev1": float(previous1[1]),
            "high_prev1": float(previous1[2]),
            "low_prev1": float(previous1[3]),
            "close_prev1": float(previous1[4]),
            "open_prev2": float(previous2[1]),
            "high_prev2": float(previous2[2]),
            "low_prev2": float(previous2[3]),
            "close_prev2": float(previous2[4]),
            "ma7_prev5": float(prev5[6]),
            "ma20_prev5": float(prev5[7]),
            "ma60_prev5": float(prev5[8]),
            "ma100_prev20": float(prev20[9]),
            "ma200_prev20": float(prev20[10]),
        }
    )
    return phase15._state(row)


def _current_state(rows: list[tuple]) -> dict[str, str] | None:
    confirmed_rows = [row for row in rows if str(row[11] if len(row) > 11 else "pan").strip().lower() != "yahoo"]
    if len(confirmed_rows) < 21:
        return None
    return _state_at(confirmed_rows, len(confirmed_rows) - 1)


def _match_rules(state: dict[str, str], rules: list[dict[str, Any]]) -> list[dict[str, Any]]:
    matches = []
    for rule in rules:
        if (
            rule.get("entry_exit") == state.get("entry_exit")
            and rule.get("trend") == state.get("trend")
            and rule.get("environment") == state.get("environment")
        ):
            matches.append(
                {
                    "rule_id": rule.get("rule_id"),
                    "display_label": rule.get("display_label"),
                    "evidence": rule.get("evidence"),
                    "actionability": rule.get("actionability"),
                    "meemee_display_mode": rule.get("meemee_display_mode"),
                }
            )
    return matches


def _historical_chart_markers(rows: list[tuple], rules: list[dict[str, Any]], *, limit: int = MAX_CHART_MARKERS) -> list[dict[str, Any]]:
    confirmed_rows = [row for row in rows if str(row[11] if len(row) > 11 else "pan").strip().lower() != "yahoo"]
    markers: list[dict[str, Any]] = []
    for index in range(len(confirmed_rows) - 1, -1, -1):
        state = _state_at(confirmed_rows, index)
        if state is None:
            continue
        matches = _match_rules(state, rules)
        if not matches:
            continue
        best = matches[0]
        markers.append(
            {
                "date": _date_to_iso(confirmed_rows[index][0]),
                "kind": "ranking-up",
                "label": "MA",
                "rule_id": best.get("rule_id"),
                "display_label": best.get("display_label"),
                "actionability": best.get("actionability"),
                "evidence": best.get("evidence"),
            }
        )
        if len(markers) >= limit:
            break
    return list(reversed(markers))


def build_ma_role_review_payload(rows: list[tuple], *, catalog_root: Path = DEFAULT_CATALOG_ROOT) -> dict[str, Any]:
    catalog_path = _latest_catalog_path(catalog_root)
    if catalog_path is None:
        return {
            "schema_version": "ma_role_readonly_review_v1",
            "available": False,
            "reason": "catalog_missing",
            "matches": [],
            "chart_markers": [],
            "read_only": True,
            "ranking_effect": False,
            "automatic_trade_action": False,
        }
    state = _current_state(rows)
    if state is None:
        return {
            "schema_version": "ma_role_readonly_review_v1",
            "available": False,
            "reason": "insufficient_confirmed_daily_history",
            "catalog_path": str(catalog_path),
            "matches": [],
            "chart_markers": [],
            "read_only": True,
            "ranking_effect": False,
            "automatic_trade_action": False,
        }
    catalog = _read_json(catalog_path)
    rules = catalog.get("rules") if isinstance(catalog.get("rules"), list) else []
    chart_markers = _historical_chart_markers(rows, rules)
    return {
        "schema_version": "ma_role_readonly_review_v1",
        "available": True,
        "reason": None,
        "catalog_path": str(catalog_path),
        "current_state": state,
        "matches": _match_rules(state, rules),
        "chart_markers": chart_markers,
        "chart_marker_limit": MAX_CHART_MARKERS,
        "read_only": True,
        "display_only": True,
        "ranking_effect": False,
        "runtime_db_write": False,
        "automatic_trade_action": False,
        "validated_buy_claim": False,
        "validated_sell_claim": False,
    }
