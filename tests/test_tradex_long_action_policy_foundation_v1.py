from __future__ import annotations

import json
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from scripts.tradex_long_action_policy_foundation_v1 import (
    EVALUATION_COST_MODEL,
    ENTRY_GATE_VARIANT,
    build_long_action_policy_foundation,
)


def _business_days(start: date, count: int) -> list[date]:
    days: list[date] = []
    current = start
    while len(days) < count:
        if current.weekday() < 5:
            days.append(current)
        current += timedelta(days=1)
    return days


def _make_rows(start: date, count: int, *, base: float, step: float, high_wick_every: int = 0, volume: int = 10_000) -> list[tuple[int, float, float, float, float, int]]:
    rows: list[tuple[int, float, float, float, float, int]] = []
    for index, current in enumerate(_business_days(start, count)):
        open_price = base + index * step
        close_price = open_price + step * 0.8
        high_price = close_price * (1.06 if high_wick_every and index % high_wick_every == 0 else 1.01)
        low_price = open_price * 0.99
        rows.append((int(current.strftime("%Y%m%d")), open_price, high_price, low_price, close_price, volume))
    return rows


class MiniRepo:
    def __init__(self) -> None:
        start = date(2025, 3, 1)
        count = 90
        self.data = {
            "1001": _make_rows(start, count, base=100.0, step=1.5, high_wick_every=4, volume=50_000),
            "1002": _make_rows(start, count, base=150.0, step=-1.0, high_wick_every=0, volume=500),
            "1306": self._benchmark_rows(start, count),
        }

    def _benchmark_rows(self, start: date, count: int) -> list[tuple[int, float, float, float, float, int]]:
        rows: list[tuple[int, float, float, float, float, int]] = []
        for index, current in enumerate(_business_days(start, count)):
            if current < date(2025, 4, 10):
                close_price = 100.0 - index * 0.8
            else:
                close_price = 92.0 + (index - 25) * 1.1
            open_price = close_price - 0.4
            high_price = close_price * 1.01
            low_price = close_price * 0.99
            rows.append((int(current.strftime("%Y%m%d")), open_price, high_price, low_price, close_price, 1_000_000))
        return rows

    def get_all_codes(self):
        return list(self.data.keys())

    def get_latest_params_for_screening(self, codes=None):  # noqa: ANN001
        selected = list(codes or self.data.keys())
        return [(code, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0) for code in selected if code in self.data]

    def get_daily_bars_batch(self, codes, limit=420, asof_dt=None):  # noqa: ANN001
        result = {}
        asof_ymd = None
        if asof_dt is not None:
            asof_ymd = int(datetime.fromtimestamp(int(asof_dt), tz=timezone.utc).strftime("%Y%m%d"))
        for code in codes:
            rows = list(self.data.get(str(code), []))
            if asof_ymd is not None:
                rows = [row for row in rows if row[0] <= asof_ymd]
            result[str(code)] = rows[-limit:]
        return result


def _load_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def test_long_action_foundation_generates_required_artifacts(tmp_path: Path):
    repo = MiniRepo()
    output_root = tmp_path / "long_action_policy"
    result = build_long_action_policy_foundation(
        repo,
        output_root,
        window_specs=[
            {"window_id": "smoke_down", "label": "down", "window_start_date": "2025-03-31", "window_end_date": "2025-04-09", "window_months": 1},
            {"window_id": "smoke_up", "label": "up", "window_start_date": "2025-04-10", "window_end_date": "2025-04-25", "window_months": 1},
        ],
    )

    session_dir = Path(result["output_dir"])
    assert session_dir.exists()
    expected = {
        "policy_candidate_manifest.json",
        "evaluation_contract.json",
        "portfolio_replay_summary.json",
        "daily_action_ledger.json",
        "regime_split_summary.json",
        "cost_slippage_summary.json",
        "opportunity_cost_summary.json",
        "hedge_pressure_summary.json",
        "risk_audit.json",
        "final_decision.json",
        "_ARTIFACT_COMPLETE.json",
    }
    assert expected == {path.name for path in session_dir.iterdir()}

    evaluation_contract = _load_json(session_dir / "evaluation_contract.json")
    assert evaluation_contract["execution_convention"] == "next_session_open"
    assert evaluation_contract["cost_slippage_model"]["enabled"] is True

    manifest = _load_json(session_dir / "policy_candidate_manifest.json")
    assert manifest["selected_candidate_family"] == "long_entry_cash_gate_v1"
    assert manifest["allowed_actions"] == ["buy", "stay_cash", "hold"]
    assert manifest["replay_continuity_actions"] == ["forced_exit"]

    final_decision = _load_json(session_dir / "final_decision.json")
    assert final_decision["final_status"] == "implementation_done"
    assert final_decision["final_status"] not in {"keep", "hold", "drop"}


def test_long_action_foundation_ledger_and_diagnostics(tmp_path: Path):
    repo = MiniRepo()
    output_root = tmp_path / "long_action_policy"
    result = build_long_action_policy_foundation(
        repo,
        output_root,
        window_specs=[
            {"window_id": "smoke_down", "label": "down", "window_start_date": "2025-03-31", "window_end_date": "2025-04-09", "window_months": 1},
            {"window_id": "smoke_up", "label": "up", "window_start_date": "2025-04-10", "window_end_date": "2025-04-25", "window_months": 1},
        ],
    )
    session_dir = Path(result["output_dir"])
    ledger = _load_json(session_dir / "daily_action_ledger.json")
    rows = ledger["items"]
    assert rows
    assert any(row["action"] == "buy" and row["order_status"] == "filled" for row in rows)
    first_row = rows[0]
    required_fields = {
        "date",
        "decision_date",
        "fill_date",
        "symbol",
        "action",
        "side",
        "reason_codes",
        "cash",
        "gross_exposure",
        "net_exposure",
        "position_qty",
        "position_value",
        "realized_pnl",
        "unrealized_pnl",
        "daily_pnl",
        "cumulative_pnl",
        "drawdown",
        "cost_amount",
        "slippage_amount",
        "execution_price",
        "execution_timing",
        "execution_model",
        "price_provenance",
        "data_asof",
        "no_lookahead_check",
        "order_status",
        "unfilled_reason",
        "notes",
    }
    assert required_fields <= set(first_row)
    assert all(row["action"] in {"buy", "stay_cash", "hold", "forced_exit"} for row in rows)
    assert "add" not in {row["action"] for row in rows}
    assert "hedge" not in {row["action"] for row in rows}
    assert "rotate" not in {row["action"] for row in rows}
    assert "short" not in {row["action"] for row in rows}

    paired = {}
    for row in rows:
        paired.setdefault((row["window_id"], row["date"], row["symbol"]), {})[row["scenario"]] = row
    assert any(
        pair.get("baseline", {}).get("action") == "buy"
        and pair.get("long_entry_cash_gate_v1", {}).get("action") == "stay_cash"
        and pair["baseline"].get("order_status") == "filled"
        for pair in paired.values()
        if {"baseline", "long_entry_cash_gate_v1"} <= set(pair)
    )
    assert any(
        pair["baseline"].get("cash") != pair["long_entry_cash_gate_v1"].get("cash")
        or pair["baseline"].get("position_value") != pair["long_entry_cash_gate_v1"].get("position_value")
        for pair in paired.values()
        if {"baseline", "long_entry_cash_gate_v1"} <= set(pair)
    )

    summary = _load_json(session_dir / "portfolio_replay_summary.json")
    assert summary["baseline_name"] == "long_entry_cash_gate_baseline_v1"
    assert summary["variant_name"] == "long_entry_cash_gate_v1"
    assert summary["execution_convention"] == "next_session_open"

    opportunity = _load_json(session_dir / "opportunity_cost_summary.json")
    hedge = _load_json(session_dir / "hedge_pressure_summary.json")
    assert opportunity["no_executed_rotation"] is True
    assert hedge["no_executed_hedge"] is True
    assert _load_json(session_dir / "risk_audit.json")["cost_slippage_risk"]["status"] == "controlled"
    assert ENTRY_GATE_VARIANT["liquidity20d_min"] > 0
    assert EVALUATION_COST_MODEL["enabled"] is True
