from __future__ import annotations

import argparse
import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd


AXIS_ID = "short_visual_micro_path_downside_stats_v1"
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\short_visual_micro_path_downside_stats_v1")
DEFAULT_VISUAL_REPLAY_EVENTS = Path(
    r"G:\Tradex\short_visual_micro_path_replay_v1"
    r"\20260605T010450Z-short_visual_micro_path_replay_v1"
    r"\short_visual_micro_path_replay_events.jsonl"
)
DEFAULT_VISUAL_REPLAY_ARTIFACT = Path(
    r"G:\Tradex\short_visual_micro_path_replay_v1"
    r"\20260605T010450Z-short_visual_micro_path_replay_v1"
    r"\short_visual_micro_path_replay.json"
)
MIN_ROBUST_N = 30
DOWNSIDE_THRESHOLDS = (0.04, 0.06, 0.08, 0.10, 0.15)


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


def _load_jsonl(path: Path) -> list[dict[str, Any]]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def _downside_confidence(row: dict[str, Any], baseline: dict[str, Any]) -> str:
    n = int(row["n"])
    if n < 10:
        return "thin_sample"
    hit6 = float(row["p_mfe_ge_6pct"])
    hit8 = float(row["p_mfe_ge_8pct"])
    stop = float(row["stop_hit_rate"])
    mean_ret = float(row["mean_short_ret"])
    if n >= MIN_ROBUST_N and hit8 >= 0.45 and stop <= baseline["stop_hit_rate"] and mean_ret > baseline["mean_short_ret"]:
        return "high"
    if n >= MIN_ROBUST_N and hit6 >= 0.45 and mean_ret > baseline["mean_short_ret"]:
        return "medium"
    if hit6 >= 0.40 and mean_ret > baseline["mean_short_ret"]:
        return "provisional"
    return "low"


def _summarize(df: pd.DataFrame, field: str, baseline: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for key, group in df.groupby(field, dropna=False):
        ret = pd.to_numeric(group["short_ret"], errors="coerce")
        mfe = pd.to_numeric(group["mfe_20"], errors="coerce")
        mae = pd.to_numeric(group["mae_20"], errors="coerce")
        row = {
            field: str(key),
            "n": int(len(group)),
            "symbols": int(group["code"].nunique()),
            "months": int(group["month"].nunique()),
            "mean_short_ret": float(ret.mean()),
            "median_short_ret": float(ret.median()),
            "short_ret_p25": float(ret.quantile(0.25)),
            "short_ret_p75": float(ret.quantile(0.75)),
            "win_rate": float((ret > 0).mean()),
            "mean_mfe_20": float(mfe.mean()),
            "median_mfe_20": float(mfe.median()),
            "mfe_20_p25": float(mfe.quantile(0.25)),
            "mfe_20_p75": float(mfe.quantile(0.75)),
            "mean_mae_20": float(mae.mean()),
            "median_mae_20": float(mae.median()),
            "target_hit_rate": float(group["target_hit"].astype(bool).mean()),
            "stop_hit_rate": float(group["stop_hit"].astype(bool).mean()),
            "denial_exit_rate": float(group["denial_exit"].astype(bool).mean()),
            "avg_expected_downside_pct": float(pd.to_numeric(group["expected_downside_pct"], errors="coerce").mean()),
            "avg_risk_reward_to_sl8": float(pd.to_numeric(group["risk_reward_to_sl8"], errors="coerce").mean()),
            "exit_reason_counts": group["exit_reason"].value_counts().to_dict(),
        }
        for threshold in DOWNSIDE_THRESHOLDS:
            row[f"p_mfe_ge_{int(threshold * 100)}pct"] = float((mfe >= threshold).mean())
        row["downside_confidence"] = _downside_confidence(row, baseline)
        rows.append(row)
    rows.sort(
        key=lambda row: (
            {"high": 4, "medium": 3, "provisional": 2, "low": 1, "thin_sample": 0}[row["downside_confidence"]],
            row["mean_mfe_20"],
            row["mean_short_ret"],
        ),
        reverse=True,
    )
    return rows


def _baseline(df: pd.DataFrame) -> dict[str, Any]:
    ret = pd.to_numeric(df["short_ret"], errors="coerce")
    mfe = pd.to_numeric(df["mfe_20"], errors="coerce")
    out = {
        "n": int(len(df)),
        "symbols": int(df["code"].nunique()),
        "months": int(df["month"].nunique()),
        "mean_short_ret": float(ret.mean()),
        "median_short_ret": float(ret.median()),
        "win_rate": float((ret > 0).mean()),
        "mean_mfe_20": float(mfe.mean()),
        "median_mfe_20": float(mfe.median()),
        "stop_hit_rate": float(df["stop_hit"].astype(bool).mean()),
        "target_hit_rate": float(df["target_hit"].astype(bool).mean()),
    }
    for threshold in DOWNSIDE_THRESHOLDS:
        out[f"p_mfe_ge_{int(threshold * 100)}pct"] = float((mfe >= threshold).mean())
    return out


def _decision(action_rows: list[dict[str, Any]], label_rows: list[dict[str, Any]]) -> dict[str, Any]:
    best = action_rows[0] if action_rows else None
    if best is None:
        return {
            "authoritative_decision": "blocked_no_visual_micro_events",
            "reason": "No events were available for downside statistics.",
        }
    if best["downside_confidence"] in {"high", "medium"}:
        return {
            "authoritative_decision": "keep_visual_micro_downside_probability_table",
            "candidate_local_decision": best,
            "reason": "At least one visual micro action bucket has robust downside probability separation.",
        }
    provisional = next((row for row in action_rows + label_rows if row["downside_confidence"] == "provisional"), None)
    if provisional:
        return {
            "authoritative_decision": "hold_visual_micro_downside_probability_table_provisional",
            "candidate_local_decision": provisional,
            "reason": "Some downside separation exists, but sample or stop balance is not robust enough.",
        }
    return {
        "authoritative_decision": "drop_visual_micro_downside_probability_table_no_separation",
        "reason": "No visual micro bucket separated downside probability from baseline.",
    }


def _markdown(payload: dict[str, Any]) -> str:
    lines = [
        "# Short Visual Micro Path Downside Stats v1",
        "",
        f"- authoritative_decision: `{payload['research_decision']['authoritative_decision']}`",
        f"- source_events_path: `{payload['source_events_path']}`",
        "",
        "## Baseline",
        "",
        "| n | mean_ret | win_rate | mean_mfe20 | P>=4% | P>=6% | P>=8% | P>=10% | stop_hit |",
        "|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    b = payload["baseline"]
    lines.append(
        f"| {b['n']} | {b['mean_short_ret']:.4f} | {b['win_rate']:.3f} | {b['mean_mfe_20']:.4f} | "
        f"{b['p_mfe_ge_4pct']:.3f} | {b['p_mfe_ge_6pct']:.3f} | {b['p_mfe_ge_8pct']:.3f} | "
        f"{b['p_mfe_ge_10pct']:.3f} | {b['stop_hit_rate']:.3f} |"
    )
    for title, key, name in [
        ("By Visual Micro Action", "visual_micro_action_downside_stats", "visual_micro_action"),
        ("By Visual Micro Label", "visual_micro_label_downside_stats", "visual_micro_label"),
    ]:
        lines.extend(
            [
                "",
                f"## {title}",
                "",
                f"| {name} | confidence | n | mean_ret | win_rate | mean_mfe20 | med_mfe20 | P>=4% | P>=6% | P>=8% | P>=10% | stop_hit | denial |",
                "|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
            ]
        )
        for row in payload[key]:
            lines.append(
                f"| {row[name]} | {row['downside_confidence']} | {row['n']} | {row['mean_short_ret']:.4f} | "
                f"{row['win_rate']:.3f} | {row['mean_mfe_20']:.4f} | {row['median_mfe_20']:.4f} | "
                f"{row['p_mfe_ge_4pct']:.3f} | {row['p_mfe_ge_6pct']:.3f} | {row['p_mfe_ge_8pct']:.3f} | "
                f"{row['p_mfe_ge_10pct']:.3f} | {row['stop_hit_rate']:.3f} | {row['denial_exit_rate']:.3f} |"
            )
    lines.extend(
        [
            "",
            "## Notes",
            "",
            "- MFE is the maximum favorable downside within the frozen 20-session window after signal.",
            "- Review-only statistics. No ranking, entry, exit, DB, MeeMee, or production behavior changed.",
        ]
    )
    return "\n".join(lines) + "\n"


def run(events_path: Path, replay_artifact_path: Path, output_root: Path) -> Path:
    run_dir = output_root / _run_id()
    events = _load_jsonl(events_path)
    df = pd.DataFrame(events)
    baseline = _baseline(df)
    action_rows = _summarize(df, "visual_micro_action", baseline)
    label_rows = _summarize(df, "visual_micro_label", baseline)
    payload = {
        "run_id": run_dir.name,
        "created_at": _utc_now(),
        "axis_id": AXIS_ID,
        "source_events_path": str(events_path),
        "source_visual_replay_artifact_path": str(replay_artifact_path),
        "fixed_evaluation_conditions": {
            "inherits_from": str(replay_artifact_path),
            "entry_population": "unchanged from short_visual_micro_path_replay_v1",
            "target_model": "unchanged realistic_downside_target_4_15pct_band",
            "stop_loss": "unchanged sl8",
            "max_hold_days": 20,
            "metric": "historical downside probability by visual micro pattern",
        },
        "baseline": baseline,
        "visual_micro_action_downside_stats": action_rows,
        "visual_micro_label_downside_stats": label_rows,
        "research_decision": _decision(action_rows, label_rows),
        "runtime_db_write": False,
        "meemee_modified": False,
        "production_ranking_modified": False,
    }
    _write_json(run_dir / "short_visual_micro_path_downside_stats.json", payload)
    (run_dir / "short_visual_micro_path_downside_stats_summary.md").write_text(_markdown(payload), encoding="utf-8")
    _write_json(
        run_dir / "_ARTIFACT_COMPLETE.json",
        {
            "status": "complete",
            "axis_id": AXIS_ID,
            "generated_at": _utc_now(),
            "required_files": [
                "short_visual_micro_path_downside_stats.json",
                "short_visual_micro_path_downside_stats_summary.md",
                "_ARTIFACT_COMPLETE.json",
            ],
        },
    )
    return run_dir


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--events-path", type=Path, default=DEFAULT_VISUAL_REPLAY_EVENTS)
    parser.add_argument("--replay-artifact-path", type=Path, default=DEFAULT_VISUAL_REPLAY_ARTIFACT)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    args = parser.parse_args()
    print(run(args.events_path, args.replay_artifact_path, args.output_root))


if __name__ == "__main__":
    main()
