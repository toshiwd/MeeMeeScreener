from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path
from statistics import median
from typing import Any

from app.backend.services import tradex_research_os_contracts as os_contracts
from app.backend.services import tradex_research_os_store as os_store
from app.backend.services import tradex_research_trader_label_policy as trader_label_policy


def _text(value: Any, fallback: str = "") -> str:
    if value is None:
        return fallback
    if isinstance(value, str):
        text = value.strip()
        return text or fallback
    text = str(value).strip()
    return text or fallback


def _float_or_none(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _mean(values: list[float]) -> float | None:
    if not values:
        return None
    return float(sum(values) / len(values))


def _artifact_name_from_path(path: Path) -> str:
    return path.name


def _strict_read(path: Path, *, artifact_name: str) -> dict[str, Any]:
    return os_store.read_json_object_strict(path, artifact_name=artifact_name)


def _validate_experiment_artifacts(
    *,
    observation_snapshot: dict[str, Any],
    strategy_judgement: dict[str, Any],
    teacher_evaluation_row: dict[str, Any],
    judge_input: dict[str, Any],
) -> None:
    os_contracts.validate_observation_snapshot(observation_snapshot)
    os_contracts.validate_strategy_judgement(strategy_judgement)
    os_contracts.validate_teacher_evaluation_row(teacher_evaluation_row)
    os_contracts.validate_judge_input(judge_input)
    if _text(strategy_judgement.get("observation_snapshot_hash")) != _text(observation_snapshot.get("observation_snapshot_hash")):
        raise ValueError("strategy_judgement observation_snapshot_hash mismatch")
    if _text(teacher_evaluation_row.get("observation_snapshot_hash")) != _text(observation_snapshot.get("observation_snapshot_hash")):
        raise ValueError("teacher_evaluation_row observation_snapshot_hash mismatch")
    if _text(teacher_evaluation_row.get("strategy_judgement_hash")) != _text(strategy_judgement.get("strategy_judgement_hash")):
        raise ValueError("teacher_evaluation_row strategy_judgement_hash mismatch")


def _normalize_adapter_output(payload: dict[str, Any]) -> dict[str, Any]:
    adapter_id = _text(payload.get("adapter_id"))
    machine_action_state = _text(payload.get("machine_action_state"))
    human_readable_judgement = _text(payload.get("human_readable_judgement"))
    invalidation_reason_code = _text(payload.get("invalidation_reason_code"))
    if not adapter_id:
        raise ValueError("adapter_output.adapter_id is required")
    if machine_action_state not in os_contracts.TRADEX_TRADER_MACHINE_ACTION_STATES:
        raise ValueError("adapter_output.machine_action_state is invalid")
    if human_readable_judgement not in os_contracts.TRADEX_TRADER_HUMAN_JUDGEMENTS:
        raise ValueError("adapter_output.human_readable_judgement is invalid")
    if not invalidation_reason_code:
        raise ValueError("adapter_output.invalidation_reason_code is required")
    return {
        "adapter_id": adapter_id,
        "machine_action_state": machine_action_state,
        "human_readable_judgement": human_readable_judgement,
        "buy_score": float(payload.get("buy_score") or 0.0),
        "environment_score": float(payload.get("environment_score") or 0.0),
        "trend_score": float(payload.get("trend_score") or 0.0),
        "trigger_score": float(payload.get("trigger_score") or 0.0),
        "risk_score": float(payload.get("risk_score") or 0.0),
        "invalidation_price": float(payload.get("invalidation_price") or 0.0),
        "invalidation_reason_code": invalidation_reason_code,
        "reason_codes": [_text(item) for item in payload.get("reason_codes") or [] if _text(item)],
        "confidence": float(payload.get("confidence") or 0.0),
    }


def _build_rows_for_experiment(
    *,
    experiment_id: str,
    observation_snapshot: dict[str, Any],
    strategy_judgement: dict[str, Any],
    teacher_evaluation_row: dict[str, Any],
    judge_input: dict[str, Any],
    label_policy: dict[str, Any],
) -> list[dict[str, Any]]:
    target = observation_snapshot.get("target") if isinstance(observation_snapshot.get("target"), dict) else {}
    outcome_window = teacher_evaluation_row.get("realized_outcome_window") if isinstance(teacher_evaluation_row.get("realized_outcome_window"), dict) else {}
    comparison_scope = judge_input.get("comparison_scope") if isinstance(judge_input.get("comparison_scope"), dict) else {}
    family_id = _text(comparison_scope.get("family_id"))
    method_family = _text(comparison_scope.get("target_method_family"))
    if not family_id:
        raise ValueError("judge_input comparison_scope.family_id is required")
    if not method_family:
        raise ValueError("judge_input comparison_scope.target_method_family is required")
    rows: list[dict[str, Any]] = []
    primary_adapter_id = _text(strategy_judgement.get("primary_adapter_id"))
    for adapter_output in strategy_judgement.get("adapter_outputs") or []:
        if not isinstance(adapter_output, dict):
            raise ValueError("adapter_outputs entries must be objects")
        normalized = _normalize_adapter_output(adapter_output)
        label_fields = trader_label_policy.apply_trader_label_policy(
            {
                "teacher_horizon_bars": int(outcome_window.get("teacher_horizon_bars") or 0),
                "future_bar_count": int(outcome_window.get("future_bar_count") or 0),
                "complete_horizon": bool(outcome_window.get("complete_horizon")),
                "return_close_basis": _float_or_none(outcome_window.get("return_close_basis")),
                "return_next_open_basis": _float_or_none(outcome_window.get("return_next_open_basis")),
                "max_favorable_excursion_close_basis": _float_or_none(outcome_window.get("max_favorable_excursion_close_basis")),
                "max_adverse_excursion_close_basis": _float_or_none(outcome_window.get("max_adverse_excursion_close_basis")),
            },
            policy=label_policy,
        )
        row = os_contracts.build_trader_benchmark_row(
            experiment_id=experiment_id,
            hypothesis_id=_text(observation_snapshot.get("hypothesis_id")),
            family_id=family_id,
            method_family=method_family,
            as_of_date=int(target.get("as_of_date") or 0),
            code=_text(target.get("code")),
            adapter_id=normalized["adapter_id"],
            machine_action_state=normalized["machine_action_state"],
            human_readable_judgement=normalized["human_readable_judgement"],
            buy_score=normalized["buy_score"],
            environment_score=normalized["environment_score"],
            trend_score=normalized["trend_score"],
            trigger_score=normalized["trigger_score"],
            risk_score=normalized["risk_score"],
            invalidation_price=normalized["invalidation_price"],
            invalidation_reason_code=normalized["invalidation_reason_code"],
            reason_codes=normalized["reason_codes"],
            confidence=normalized["confidence"],
            is_primary_adapter=normalized["adapter_id"] == primary_adapter_id,
            teacher_horizon_bars=int(outcome_window.get("teacher_horizon_bars") or 0),
            future_bar_count=int(outcome_window.get("future_bar_count") or 0),
            complete_horizon=bool(outcome_window.get("complete_horizon")),
            anchor_close_price=float(outcome_window.get("anchor_close_price") or 0.0),
            next_open_price=_float_or_none(outcome_window.get("next_open_price")),
            final_close_price=_float_or_none(outcome_window.get("final_close_price")),
            return_close_basis=_float_or_none(outcome_window.get("return_close_basis")),
            return_next_open_basis=_float_or_none(outcome_window.get("return_next_open_basis")),
            max_favorable_excursion_close_basis=_float_or_none(outcome_window.get("max_favorable_excursion_close_basis")),
            max_adverse_excursion_close_basis=_float_or_none(outcome_window.get("max_adverse_excursion_close_basis")),
            close_positive_20=label_fields["close_positive_20"],
            next_open_positive_20=label_fields["next_open_positive_20"],
            mfe_ge_10pct_20=label_fields["mfe_ge_10pct_20"],
            mae_worse_than_7pct_20=label_fields["mae_worse_than_7pct_20"],
            judgement_outcome_class=_text(label_fields["judgement_outcome_class"]),
            label_policy_version=_text(label_fields["label_policy_version"]),
            observation_snapshot_hash=_text(observation_snapshot.get("observation_snapshot_hash")),
            strategy_judgement_hash=_text(strategy_judgement.get("strategy_judgement_hash")),
            teacher_evaluation_row_hash=_text(teacher_evaluation_row.get("teacher_evaluation_row_hash")),
        )
        rows.append(row)
    return rows


def _skip_entry(*, experiment_id: str, reason_code: str, detail: dict[str, Any]) -> dict[str, Any]:
    return {
        "experiment_id": _text(experiment_id),
        "reason_code": _text(reason_code),
        "detail": dict(detail),
    }


def _materialize_experiment_rows(experiment_dir: Path, *, label_policy: dict[str, Any]) -> tuple[list[dict[str, Any]], dict[str, Any] | None]:
    experiment_id = experiment_dir.name
    preflight_path = os_store.preflight_report_file(experiment_id)
    if preflight_path.exists():
        try:
            preflight_report = _strict_read(preflight_path, artifact_name="preflight report")
            os_contracts.validate_preflight_report(preflight_report)
        except (os_store.JsonReadError, ValueError) as exc:
            return [], _skip_entry(experiment_id=experiment_id, reason_code="malformed_artifact", detail={"artifact_name": _artifact_name_from_path(preflight_path), "error": str(exc)})
        if not bool(preflight_report.get("passed")):
            return [], _skip_entry(
                experiment_id=experiment_id,
                reason_code="preflight_failed",
                detail={
                    "failure_code": _text(preflight_report.get("failure_code")),
                    "status": _text(preflight_report.get("status")),
                },
            )

    required_paths = {
        "observation_snapshot": os_store.observation_snapshot_file(experiment_id),
        "strategy_judgement": os_store.strategy_judgement_file(experiment_id),
        "teacher_evaluation_row": os_store.teacher_evaluation_row_file(experiment_id),
        "judge_input": os_store.judge_input_file(experiment_id),
    }
    missing = [name for name, path in required_paths.items() if not path.exists()]
    if missing:
        return [], _skip_entry(experiment_id=experiment_id, reason_code="missing_trader_artifacts", detail={"missing_artifacts": missing})

    try:
        observation_snapshot = _strict_read(required_paths["observation_snapshot"], artifact_name="observation snapshot")
        strategy_judgement = _strict_read(required_paths["strategy_judgement"], artifact_name="strategy judgement")
        teacher_evaluation_row = _strict_read(required_paths["teacher_evaluation_row"], artifact_name="teacher evaluation row")
        judge_input = _strict_read(required_paths["judge_input"], artifact_name="judge input")
        _validate_experiment_artifacts(
            observation_snapshot=observation_snapshot,
            strategy_judgement=strategy_judgement,
            teacher_evaluation_row=teacher_evaluation_row,
            judge_input=judge_input,
        )
        return _build_rows_for_experiment(
            experiment_id=experiment_id,
            observation_snapshot=observation_snapshot,
            strategy_judgement=strategy_judgement,
            teacher_evaluation_row=teacher_evaluation_row,
            judge_input=judge_input,
            label_policy=label_policy,
        ), None
    except (os_store.JsonReadError, ValueError) as exc:
        reason_code = "label_inputs_incomplete" if "label inputs incomplete" in str(exc) or "teacher_horizon_bars mismatch" in str(exc) or "future_bar_count below required horizon" in str(exc) else "malformed_artifact"
        return [], _skip_entry(
            experiment_id=experiment_id,
            reason_code=reason_code,
            detail={
                "error": str(exc),
            },
        )


def _rate_true(rows: list[dict[str, Any]], field_name: str) -> float | None:
    labeled_values = [bool(row.get(field_name)) for row in rows if isinstance(row.get(field_name), bool)]
    if not labeled_values:
        return None
    return float(sum(1 for value in labeled_values if value) / len(labeled_values))


def _scoreboard_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_adapter: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        by_adapter.setdefault(_text(row.get("adapter_id")), []).append(row)
    scoreboard_rows: list[dict[str, Any]] = []
    for adapter_id in sorted(by_adapter):
        adapter_rows = by_adapter[adapter_id]
        enter_rows = [row for row in adapter_rows if _text(row.get("machine_action_state")) == "enter"]
        labeled_rows = [row for row in adapter_rows if isinstance(row.get("close_positive_20"), bool)]
        labeled_enter_rows = [row for row in enter_rows if isinstance(row.get("close_positive_20"), bool)]
        good_enter_rows = [row for row in labeled_enter_rows if _text(row.get("judgement_outcome_class")) == "good"]
        bad_enter_rows = [row for row in labeled_enter_rows if _text(row.get("judgement_outcome_class")) == "bad"]
        scoreboard_rows.append(
            {
                "adapter_id": adapter_id,
                "sample_count": len(adapter_rows),
                "complete_horizon_count": sum(1 for row in adapter_rows if bool(row.get("complete_horizon"))),
                "labeled_sample_count": len(labeled_rows),
                "primary_count": sum(1 for row in adapter_rows if bool(row.get("is_primary_adapter"))),
                "enter_count": sum(1 for row in adapter_rows if _text(row.get("machine_action_state")) == "enter"),
                "wait_count": sum(1 for row in adapter_rows if _text(row.get("machine_action_state")) == "wait"),
                "skip_count": sum(1 for row in adapter_rows if _text(row.get("machine_action_state")) == "skip"),
                "avg_buy_score": _mean([float(row.get("buy_score") or 0.0) for row in adapter_rows]),
                "avg_confidence": _mean([float(row.get("confidence") or 0.0) for row in adapter_rows]),
                "avg_return_close_basis_all": _mean([value for row in adapter_rows if (value := _float_or_none(row.get("return_close_basis"))) is not None]),
                "avg_return_close_basis_enter": _mean([value for row in enter_rows if (value := _float_or_none(row.get("return_close_basis"))) is not None]),
                "median_return_close_basis_enter": (
                    float(median([value for row in enter_rows if (value := _float_or_none(row.get("return_close_basis"))) is not None]))
                    if [value for row in enter_rows if (value := _float_or_none(row.get("return_close_basis"))) is not None]
                    else None
                ),
                "avg_return_next_open_basis_enter": _mean([value for row in enter_rows if (value := _float_or_none(row.get("return_next_open_basis"))) is not None]),
                "avg_mfe_enter": _mean([value for row in enter_rows if (value := _float_or_none(row.get("max_favorable_excursion_close_basis"))) is not None]),
                "avg_mae_enter": _mean([value for row in enter_rows if (value := _float_or_none(row.get("max_adverse_excursion_close_basis"))) is not None]),
                "close_positive_rate_all": _rate_true(labeled_rows, "close_positive_20"),
                "close_positive_rate_enter": _rate_true(labeled_enter_rows, "close_positive_20"),
                "next_open_positive_rate_enter": _rate_true(labeled_enter_rows, "next_open_positive_20"),
                "mfe_ge_10pct_rate_enter": _rate_true(labeled_enter_rows, "mfe_ge_10pct_20"),
                "mae_worse_than_7pct_rate_enter": _rate_true(labeled_enter_rows, "mae_worse_than_7pct_20"),
                "good_outcome_rate_enter": (float(len(good_enter_rows) / len(labeled_enter_rows)) if labeled_enter_rows else None),
                "bad_outcome_rate_enter": (float(len(bad_enter_rows) / len(labeled_enter_rows)) if labeled_enter_rows else None),
            }
        )
    return scoreboard_rows


def _write_jsonl(path: Path, rows: list[dict[str, Any]]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_path = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=str(path.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            for row in rows:
                handle.write(json.dumps(row, ensure_ascii=False, sort_keys=True, default=str))
                handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp_path, path)
        return path
    except Exception:
        try:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
        except Exception:
            pass
        raise


def rebuild_trader_benchmark(*, version: str = "v1") -> dict[str, Any]:
    policy = trader_label_policy.load_trader_label_policy()
    experiment_dirs = sorted([path for path in os_store.experiments_root().iterdir() if path.is_dir()], key=lambda path: path.name)
    rows: list[dict[str, Any]] = []
    skipped_experiments: list[dict[str, Any]] = []
    for experiment_dir in experiment_dirs:
        experiment_rows, skip_entry = _materialize_experiment_rows(experiment_dir, label_policy=policy)
        if experiment_rows:
            rows.extend(experiment_rows)
        if skip_entry:
            skipped_experiments.append(skip_entry)
    scoreboard_rows = _scoreboard_rows(rows)
    generated_at = os_contracts.now_utc_iso()
    scoreboard = os_contracts.build_trader_adapter_scoreboard(
        generated_at=generated_at,
        adapters=scoreboard_rows,
    )
    output_root = os_store.trader_benchmark_root(version)
    rows_path = _write_jsonl(os_store.trader_benchmark_rows_file(version), rows)
    scoreboard_path = os_store.write_json(os_store.trader_adapter_scoreboard_file(version), scoreboard)
    manifest = os_contracts.build_trader_benchmark_manifest(
        generated_at=generated_at,
        source_experiment_count=len(experiment_dirs),
        materialized_row_count=len(rows),
        scoreboard_adapter_count=len(scoreboard_rows),
        skipped_experiments=skipped_experiments,
        output_files={
            "root": str(output_root),
            "rows_path": str(rows_path),
            "scoreboard_path": str(scoreboard_path),
        },
    )
    manifest_path = os_store.write_json(os_store.trader_benchmark_manifest_file(version), manifest)
    return {
        "status": "ok",
        "benchmark_version": version,
        "root": str(output_root),
        "manifest_path": str(manifest_path),
        "rows_path": str(rows_path),
        "scoreboard_path": str(scoreboard_path),
        "source_experiment_count": len(experiment_dirs),
        "materialized_row_count": len(rows),
        "scoreboard_adapter_count": len(scoreboard_rows),
        "manifest": manifest,
        "scoreboard": scoreboard,
    }
