from __future__ import annotations

import argparse
import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd


AXIS_ID = "short_visual_setup_to_continuation_replay_v1"
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\short_visual_setup_to_continuation_replay_v1")
DEFAULT_EVENTS_PATH = Path(
    r"G:\Tradex\short_visual_micro_path_replay_v1"
    r"\20260605T010450Z-short_visual_micro_path_replay_v1"
    r"\short_visual_micro_path_replay_events.jsonl"
)
SETUP_LABELS = {"SellableRollover", "CleanContinuationDown", "PullbackBeforeBreak", "NoCleanPath", "TooExtendedDown"}


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _run_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + f"-{AXIS_ID}"


def _json_ready(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): _json_ready(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_ready(v) for v in value]
    if isinstance(value, Path):
        return str(value)
    try:
        import numpy as np

        if isinstance(value, np.generic):
            return _json_ready(value.item())
    except Exception:
        pass
    if isinstance(value, float) and not math.isfinite(value):
        return None
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass
    return value


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(_json_ready(payload), ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "".join(json.dumps(_json_ready(row), ensure_ascii=False) + "\n" for row in rows),
        encoding="utf-8",
    )


def _load_jsonl(path: Path) -> list[dict[str, Any]]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def _setup_state(row: dict[str, Any]) -> str:
    label = str(row.get("visual_micro_label") or "")
    lower_high = bool(row.get("visual_lower_high_10v10"))
    ma_overhead = bool(row.get("visual_ma_overhead"))
    clean_pressure = bool(row.get("visual_clean_pressure"))
    support_touch = bool(row.get("visual_support_touch_20"))
    too_extended = bool(row.get("visual_too_extended"))
    lower_wick = bool(row.get("visual_lower_wick_reclaim"))
    if label in {"SellableRollover", "CleanContinuationDown"}:
        return "SetupReady"
    if label == "PullbackBeforeBreak":
        return "SetupWaitBreak"
    if lower_high and ma_overhead and clean_pressure and not lower_wick:
        return "SetupPressure"
    if too_extended or support_touch or lower_wick:
        return "SetupRisk"
    return "SetupNoEdge"


def _transition_target(row: dict[str, Any]) -> bool:
    return str(row.get("visual_micro_action") or "") == "VisualContinuationPermit"


def _summarize(df: pd.DataFrame, field: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for key, group in df.groupby(field, dropna=False):
        ret = pd.to_numeric(group["short_ret"], errors="coerce")
        mfe = pd.to_numeric(group["mfe_20"], errors="coerce")
        rows.append(
            {
                field: str(key),
                "n": int(len(group)),
                "symbols": int(group["code"].nunique()),
                "months": int(group["month"].nunique()),
                "to_visual_continuation_permit_rate": float(group["to_visual_continuation_permit"].astype(bool).mean()),
                "mean_short_ret_after_signal": float(ret.mean()),
                "win_rate_after_signal": float((ret > 0).mean()),
                "mean_mfe_20": float(mfe.mean()),
                "p_mfe_ge_6pct": float((mfe >= 0.06).mean()),
                "p_mfe_ge_8pct": float((mfe >= 0.08).mean()),
                "stop_hit_rate": float(group["stop_hit"].astype(bool).mean()),
                "denial_exit_rate": float(group["denial_exit"].astype(bool).mean()),
                "early_bucket_counts": group["early_bucket"].value_counts().to_dict(),
            }
        )
    rows.sort(
        key=lambda row: (
            row["to_visual_continuation_permit_rate"],
            row["p_mfe_ge_6pct"],
            row["mean_short_ret_after_signal"],
            -row["stop_hit_rate"],
        ),
        reverse=True,
    )
    return rows


def _baseline(df: pd.DataFrame) -> dict[str, Any]:
    ret = pd.to_numeric(df["short_ret"], errors="coerce")
    mfe = pd.to_numeric(df["mfe_20"], errors="coerce")
    return {
        "n": int(len(df)),
        "symbols": int(df["code"].nunique()),
        "months": int(df["month"].nunique()),
        "to_visual_continuation_permit_rate": float(df["to_visual_continuation_permit"].astype(bool).mean()),
        "mean_short_ret_after_signal": float(ret.mean()),
        "win_rate_after_signal": float((ret > 0).mean()),
        "mean_mfe_20": float(mfe.mean()),
        "p_mfe_ge_6pct": float((mfe >= 0.06).mean()),
        "p_mfe_ge_8pct": float((mfe >= 0.08).mean()),
        "stop_hit_rate": float(df["stop_hit"].astype(bool).mean()),
    }


def _decision(setup_rows: list[dict[str, Any]], baseline: dict[str, Any]) -> dict[str, Any]:
    candidate = next((row for row in setup_rows if row["setup_state"] == "SetupReady"), None)
    if not candidate:
        return {
            "authoritative_decision": "drop_setup_transition_no_setup_ready_bucket",
            "reason": "No SetupReady bucket was produced.",
        }
    if candidate["n"] < 30:
        return {
            "authoritative_decision": "hold_setup_transition_insufficient_sample",
            "candidate_local_decision": candidate,
            "reason": "SetupReady sample is below 30.",
        }
    improves_transition = candidate["to_visual_continuation_permit_rate"] > baseline["to_visual_continuation_permit_rate"]
    controls_stop = candidate["stop_hit_rate"] <= baseline["stop_hit_rate"]
    improves_mfe = candidate["p_mfe_ge_6pct"] >= baseline["p_mfe_ge_6pct"]
    if improves_transition and controls_stop and improves_mfe:
        return {
            "authoritative_decision": "keep_visual_setup_for_watch_ranking_candidate",
            "candidate_local_decision": candidate,
            "baseline": baseline,
            "reason": "SetupReady improves transition to VisualContinuationPermit while controlling stop risk.",
        }
    return {
        "authoritative_decision": "hold_visual_setup_needs_refinement",
        "candidate_local_decision": candidate,
        "baseline": baseline,
        "reason": "SetupReady did not satisfy all watch-ranking keep gates.",
    }


def _markdown(payload: dict[str, Any]) -> str:
    lines = [
        "# Short Visual Setup To Continuation Replay v1",
        "",
        f"- authoritative_decision: `{payload['research_decision']['authoritative_decision']}`",
        f"- source_events_path: `{payload['source_events_path']}`",
        "",
        "## By Setup State",
        "",
        "| setup | n | to_permit | mean_ret | p6 | p8 | stop | denial |",
        "|---|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for row in payload["setup_state_summary"]:
        lines.append(
            f"| {row['setup_state']} | {row['n']} | {row['to_visual_continuation_permit_rate']:.3f} | "
            f"{row['mean_short_ret_after_signal']:.4f} | {row['p_mfe_ge_6pct']:.3f} | {row['p_mfe_ge_8pct']:.3f} | "
            f"{row['stop_hit_rate']:.3f} | {row['denial_exit_rate']:.3f} |"
        )
    lines.extend(
        [
            "",
            "## Notes",
            "",
            "- Setup state uses signal-day observable visual features only.",
            "- Transition target is post-signal VisualContinuationPermit within the existing 3-session early window.",
            "- Review-only. No production ranking, MeeMee, or runtime DB changed.",
        ]
    )
    return "\n".join(lines) + "\n"


def run(events_path: Path, output_root: Path) -> Path:
    run_dir = output_root / _run_id()
    events = _load_jsonl(events_path)
    rows: list[dict[str, Any]] = []
    for event in events:
        setup_state = _setup_state(event)
        rows.append(
            {
                **event,
                "setup_state": setup_state,
                "to_visual_continuation_permit": _transition_target(event),
                "watch_ranking_observable": True,
            }
        )
    df = pd.DataFrame(rows)
    baseline = _baseline(df)
    setup_summary = _summarize(df, "setup_state")
    label_summary = _summarize(df, "visual_micro_label")
    payload = {
        "run_id": run_dir.name,
        "created_at": _utc_now(),
        "axis_id": AXIS_ID,
        "source_events_path": str(events_path),
        "fixed_evaluation_conditions": {
            "changed_axis": "setup-to-continuation transition only",
            "setup_features": "signal-day observable visual micro features only",
            "transition_target": "VisualContinuationPermit after existing 3-session early window",
            "forbidden_for_setup": ["early_bucket", "future prices", "VisualContinuationPermit as feature"],
            "production_change": False,
        },
        "baseline": baseline,
        "setup_state_summary": setup_summary,
        "visual_micro_label_summary": label_summary,
        "research_decision": _decision(setup_summary, baseline),
        "events": rows,
        "runtime_db_write": False,
        "meemee_modified": False,
        "production_ranking_modified": False,
    }
    _write_json(run_dir / "visual_setup_to_continuation_replay.json", payload)
    _write_jsonl(run_dir / "visual_setup_to_continuation_events.jsonl", rows)
    (run_dir / "visual_setup_to_continuation_replay_summary.md").write_text(_markdown(payload), encoding="utf-8")
    _write_json(
        run_dir / "_ARTIFACT_COMPLETE.json",
        {
            "status": "complete",
            "axis_id": AXIS_ID,
            "generated_at": _utc_now(),
            "required_files": [
                "visual_setup_to_continuation_replay.json",
                "visual_setup_to_continuation_events.jsonl",
                "visual_setup_to_continuation_replay_summary.md",
                "_ARTIFACT_COMPLETE.json",
            ],
        },
    )
    return run_dir


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--events-path", type=Path, default=DEFAULT_EVENTS_PATH)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    args = parser.parse_args()
    print(run(args.events_path, args.output_root))


if __name__ == "__main__":
    main()
