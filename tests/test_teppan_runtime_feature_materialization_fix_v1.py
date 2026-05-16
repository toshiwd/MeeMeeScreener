from __future__ import annotations

import json
from pathlib import Path

from app.backend.services.teppan_live_safe_materialization import materialize_teppan_features_from_anchors
from scripts import teppan_runtime_feature_materialization_fix_v1 as mod


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")


def _pattern_root(tmp_path: Path) -> Path:
    root = tmp_path / "pattern"
    _write_json(
        root / "teppan_candidates.json",
        {
            "candidates": [
                {
                    "pattern_family": "multi_tf_trend_core",
                    "pattern_key": "daily_ma_stack=approved_daily|weekly_trend_state=approved_weekly|monthly_trend_state=approved_monthly",
                    "pattern_decision": "high_return_candidate",
                    "teppan_score": 1.0,
                    "pattern_features": {
                        "daily_ma_stack": "approved_daily",
                        "weekly_trend_state": "approved_weekly",
                        "monthly_trend_state": "approved_monthly",
                    },
                }
            ]
        },
    )
    return root


def _active_rows() -> list[dict[str, object]]:
    return [
        {
            "anchor_date": "2026-05-13",
            "anchor_ymd": 20260513,
            "symbol": "1001",
            "name": "one",
            "side": "long",
            "champion_rank": 1,
            "runtime_rank": 1,
            "champion_score": 1.0,
            "display_score": 1.0,
        },
        {
            "anchor_date": "2026-05-13",
            "anchor_ymd": 20260513,
            "symbol": "1006",
            "name": "six",
            "side": "long",
            "champion_rank": 6,
            "runtime_rank": 6,
            "champion_score": 0.95,
            "display_score": 0.95,
        },
    ]


def _anchor(symbol: str, *, match: bool, risk: bool = False) -> dict[str, object]:
    daily = "approved_daily" if match else "other_daily"
    weekly = "approved_weekly" if match else "other_weekly"
    monthly = "approved_monthly" if match else "other_monthly"
    return {
        "symbol": symbol,
        "anchor_date": "2026-05-13",
        "anchor_ymd": 20260513,
        "daily_ma_stack": daily,
        "weekly_trend_state": weekly,
        "monthly_trend_state": monthly,
        "daily_ret20_state": "daily20_strong_up" if risk else "daily20_flat",
        "daily_candle_state": "daily_upper_wick_warning" if risk else "daily_small_neutral",
        "weekly_candle_state": "weekly_small_neutral",
        "weekly_ret4_state": "weekly4_flat",
        "daily_ma60_slope_state": "daily_ma60_flat",
        "daily_sequence_state": "daily_sequence_mixed",
        "daily_volume_state": "daily_volume_normal",
        "weekly_volume_state": "weekly_volume_normal",
        "monthly_ret6_state": "monthly6_flat",
        "monthly_candle_state": "monthly_small_neutral",
        "monthly_volume_state": "monthly_volume_normal",
    }


def test_live_safe_materialization_from_anchors_uses_no_future_labels() -> None:
    candidates = [
        {
            "pattern_family": "multi_tf_trend_core",
            "pattern_key": "daily_ma_stack=approved_daily|weekly_trend_state=approved_weekly|monthly_trend_state=approved_monthly",
            "pattern_decision": "high_return_candidate",
            "teppan_score": 1.0,
        }
    ]
    payload = materialize_teppan_features_from_anchors(
        _active_rows(),
        [_anchor("1001", match=False), _anchor("1006", match=True)],
        candidates,
    )

    by_symbol = {row["symbol"]: row for row in payload["rows"]}
    assert by_symbol["1006"]["teppan_pattern_match"] is True
    assert by_symbol["1006"]["teppan_guard_pass"] is True
    assert by_symbol["1006"]["loss_guard_blocked"] is False
    assert by_symbol["1006"]["future_label_inputs_used"] is False
    assert by_symbol["1001"]["teppan_pattern_match"] is False
    assert payload["input_dependency_audit"]["future_labels_used"] is False
    assert payload["summary"]["teppan_pattern_match_count"] == 1


def test_materialization_fix_script_outputs_ready_artifacts(tmp_path: Path) -> None:
    payload = mod.run_teppan_runtime_feature_materialization_fix_v1(
        pattern_root=_pattern_root(tmp_path),
        output_parent=tmp_path / "out",
        run_id="fix",
        runtime_status={"selected_runtime_db_path": str(tmp_path / "stocks.duckdb")},
        active_rows=_active_rows(),
        anchor_rows=[_anchor("1001", match=False), _anchor("1006", match=True)],
    )

    assert payload["research_decision"]["decision"] == "live_safe_materialization_ready"
    assert payload["research_decision"]["future_labels_used"] is False
    assert payload["research_decision"]["parity_pass"] is True
    assert payload["coverage"]["by_topk"]["top100"]["teppan_pattern_match_count"] == 1
    assert payload["artifact_complete"]["complete"] is True
    output_root = Path(payload["output_root"])
    for name in mod.REQUIRED_OUTPUTS:
        assert (output_root / name).exists(), name


def test_materialization_fix_holds_for_parity_gap(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(
        mod,
        "independent_exact_match_rows",
        lambda rows, candidates: [
            {"symbol": row["symbol"], "anchor_date": row["anchor_date"], "independent_teppan_pattern_match": False}
            for row in rows
        ],
    )
    payload = mod.run_teppan_runtime_feature_materialization_fix_v1(
        pattern_root=_pattern_root(tmp_path),
        output_parent=tmp_path / "out",
        run_id="fix",
        runtime_status={"selected_runtime_db_path": str(tmp_path / "stocks.duckdb")},
        active_rows=_active_rows(),
        anchor_rows=[_anchor("1001", match=False), _anchor("1006", match=True)],
    )

    assert payload["research_decision"]["decision"] == "hold_for_parity_gap"
    assert payload["parity"]["parity_pass"] is False
