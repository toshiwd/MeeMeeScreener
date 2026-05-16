from __future__ import annotations

import json
from pathlib import Path

from scripts import tradex_pre_strength_defensive_overlay_probe_v1 as mod


SAFE_FAMILY = (
    "pre_strength::pre_base_to_strength::"
    "pre_ret20_state=pre20_flat|pre_ret5_state=pre5_flat|"
    "pre_ma20_path_state=pre_ma20_reclaim_base|weekly_prior_state=weekly_prior_mixed|monthly_prior_state=monthly_prior_uptrend"
)


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")


def _write_jsonl(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(json.dumps(row, ensure_ascii=False) + "\n" for row in rows), encoding="utf-8")


def _risk_root(tmp_path: Path, *, future_label_condition: bool = False, weak_safe: bool = False) -> Path:
    risk = tmp_path / "risk"
    probe = tmp_path / "probe"
    validation = tmp_path / "validation"
    source = tmp_path / "source"
    _write_json(risk / "research_decision.json", {"decision": "defensive_filter_only"})
    _write_json(risk / "source_artifact_refs.json", {"probe_root": str(probe), "validation_root": str(validation), "pre_strength_root": str(source)})
    _write_json(probe / "probe_contract.json", {"source_pre_strength_root": str(source), "source_validation_root": str(validation)})
    conditions = _conditions(SAFE_FAMILY)
    if future_label_condition:
        conditions = {"ret20_fwd": "0.1"}
    _write_json(
        validation / "family_validation_report.json",
        {"rows": [{"family_id": SAFE_FAMILY, "family_group": "pre_base_to_strength", "conditions": conditions}]},
    )
    _write_jsonl(source / "pre_strength_event_ledger.jsonl", _events(weak_safe=weak_safe))
    return risk


def _conditions(family_id: str) -> dict[str, str]:
    return dict(item.split("=", 1) for item in family_id.split("::", 2)[2].split("|"))


def _events(*, weak_safe: bool) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for day in range(1, 18):
        date = f"2024-03-{day:02d}"
        for rank in range(1, 8):
            safe = rank == 5
            risky = rank == 4
            ret = 0.04
            if risky:
                ret = -0.12
            if safe:
                ret = -0.08 if weak_safe else 0.08
            rows.append(
                {
                    "code": f"{7000 + day * 10 + rank}",
                    "event_date": date,
                    "event_month": "2024-03",
                    "event_strength_score": 10 - rank,
                    "ret20_fwd": ret,
                    "mfe20": max(ret, 0.0) + 0.02,
                    "mae20": -0.04,
                    "win20": ret > 0,
                    "severe_loss20": ret <= -0.10,
                    "pre_ret20_state": "pre20_flat" if safe else "other",
                    "pre_ret5_state": "pre5_flat" if safe else "other",
                    "pre_ma20_path_state": "pre_ma20_reclaim_base" if safe else "other",
                    "weekly_prior_state": "weekly_prior_mixed" if safe or risky else "weekly_prior_uptrend",
                    "monthly_prior_state": "monthly_prior_uptrend" if safe else "monthly_prior_down_or_drawdown" if risky else "monthly_prior_uptrend",
                    "pre_wick_warning_state": "pre_upper_wick_or_failed_push" if risky else "pre_wicks_clean",
                    "event_daily_ret20_state": "daily20_down" if risky else "daily20_up",
                    "pre_candle_energy_state": "other",
                    "event_daily_candle_state": "daily_strong_bull",
                    "pre_ma60_context_state": "other",
                    "pre_volume_state": "other",
                    "pre_compression_state": "other",
                }
            )
    return rows


def test_defensive_overlay_keeps_variant_that_removes_bad_picks(tmp_path: Path) -> None:
    payload = mod.run_pre_strength_defensive_overlay_probe_v1(
        risk_root=_risk_root(tmp_path),
        output_parent=tmp_path / "out",
        run_id="overlay",
    )

    assert payload["research_decision"]["decision"] in {"defensive_overlay_keep_candidate", "defensive_overlay_hold"}
    assert payload["research_decision"]["activation_allowed"] is False
    assert payload["artifact_complete"]["complete"] is True
    assert any(row["bad_pick_removed_count"] > 0 for row in payload["overlay_variant_leaderboard"]["rows"])
    for name in mod.REQUIRED_OUTPUTS:
        assert (Path(payload["output_root"]) / name).exists(), name


def test_defensive_overlay_drops_when_safe_overlay_is_bad(tmp_path: Path) -> None:
    payload = mod.run_pre_strength_defensive_overlay_probe_v1(
        risk_root=_risk_root(tmp_path, weak_safe=True),
        output_parent=tmp_path / "out",
        run_id="overlay",
    )

    assert payload["research_decision"]["decision"] in {
        "defensive_overlay_keep_candidate",
        "defensive_overlay_hold",
        "defensive_overlay_drop",
    }
    assert payload["artifact_complete"]["complete"] is True


def test_defensive_overlay_rejects_future_label_condition(tmp_path: Path) -> None:
    try:
        mod.run_pre_strength_defensive_overlay_probe_v1(
            risk_root=_risk_root(tmp_path, future_label_condition=True),
            output_parent=tmp_path / "out",
            run_id="overlay",
        )
    except ValueError as exc:
        assert "future label condition" in str(exc)
    else:
        raise AssertionError("expected future label condition rejection")
