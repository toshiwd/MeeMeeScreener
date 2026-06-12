from __future__ import annotations

import argparse
import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd


AXIS_ID = "short_setup_profitability_audit_v1"
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\short_setup_profitability_audit_v1")
DEFAULT_SETUP_REPLAY = Path(
    r"G:\Tradex\short_visual_setup_to_continuation_replay_v1"
    r"\20260605T023154Z-short_visual_setup_to_continuation_replay_v1"
    r"\visual_setup_to_continuation_replay.json"
)
SETUP_STATE = "SetupReady"


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _run_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + f"-{AXIS_ID}"


def _read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


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


def _summarize(rows: list[dict[str, Any]], policy_id: str) -> dict[str, Any]:
    if not rows:
        return {"policy_id": policy_id, "n": 0}
    df = pd.DataFrame(rows)
    ret = pd.to_numeric(df["policy_short_ret"], errors="coerce")
    mfe = pd.to_numeric(df["mfe_20"], errors="coerce")
    by_month = df.assign(policy_ret=ret).groupby("month")["policy_ret"].mean()
    return {
        "policy_id": policy_id,
        "n": int(len(df)),
        "symbols": int(df["code"].nunique()),
        "months": int(df["month"].nunique()),
        "mean_short_ret": float(ret.mean()),
        "median_short_ret": float(ret.median()),
        "win_rate": float((ret > 0).mean()),
        "loss_rate": float((ret < 0).mean()),
        "avg_win": float(ret[ret > 0].mean()) if (ret > 0).any() else 0.0,
        "avg_loss": float(ret[ret < 0].mean()) if (ret < 0).any() else 0.0,
        "p_mfe_ge_6pct": float((mfe >= 0.06).mean()),
        "p_mfe_ge_8pct": float((mfe >= 0.08).mean()),
        "p_mfe_ge_10pct": float((mfe >= 0.10).mean()),
        "stop_hit_rate": float(df["stop_hit"].astype(bool).mean()),
        "target_hit_rate": float(df["target_hit"].astype(bool).mean()),
        "denial_exit_rate": float(df["denial_exit"].astype(bool).mean()),
        "positive_month_rate": float((by_month > 0).mean()),
        "mean_monthly_avg_ret": float(by_month.mean()),
        "exit_reason_counts": df["exit_reason"].value_counts().to_dict(),
        "label_counts": df["visual_micro_label"].value_counts().to_dict(),
    }


def _policy_rows(events: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    setup = [row for row in events if row.get("setup_state") == SETUP_STATE]
    immediate: list[dict[str, Any]] = []
    confirmed: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []
    for row in setup:
        immediate.append({**row, "policy_id": "setup_ready_immediate_next_open", "policy_short_ret": row.get("short_ret")})
        if row.get("to_visual_continuation_permit"):
            confirmed.append({**row, "policy_id": "setup_ready_then_continuation_permit", "policy_short_ret": row.get("short_ret")})
        else:
            skipped.append({**row, "policy_id": "setup_ready_skipped_no_permit", "policy_short_ret": row.get("short_ret")})
    return immediate, confirmed, skipped


def _decision(immediate: dict[str, Any], confirmed: dict[str, Any], skipped: dict[str, Any]) -> dict[str, Any]:
    if confirmed.get("n", 0) < 20:
        return {
            "authoritative_decision": "hold_setup_profitability_insufficient_confirmed_sample",
            "candidate_local_decision": confirmed,
            "reason": "Confirmed setup sample is below 20.",
        }
    improves_immediate = confirmed["mean_short_ret"] > immediate.get("mean_short_ret", -999)
    improves_win = confirmed["win_rate"] > immediate.get("win_rate", -999)
    controls_stop = confirmed["stop_hit_rate"] <= immediate.get("stop_hit_rate", 1)
    separates_skipped = confirmed["mean_short_ret"] > skipped.get("mean_short_ret", -999)
    if improves_immediate and improves_win and controls_stop and separates_skipped:
        return {
            "authoritative_decision": "keep_setup_then_continuation_profitability",
            "candidate_local_decision": confirmed,
            "immediate_comparison": immediate,
            "skipped_comparison": skipped,
            "reason": "SetupReady is profitable when used as watch state and entered only after continuation confirmation.",
        }
    return {
        "authoritative_decision": "hold_setup_profitability_needs_refinement",
        "candidate_local_decision": confirmed,
        "immediate_comparison": immediate,
        "skipped_comparison": skipped,
        "reason": "Confirmed setup route did not satisfy all profitability gates.",
    }


def _markdown(payload: dict[str, Any]) -> str:
    lines = [
        "# Short Setup Profitability Audit v1",
        "",
        f"- authoritative_decision: `{payload['research_decision']['authoritative_decision']}`",
        f"- setup_replay_path: `{payload['setup_replay_path']}`",
        "",
        "| policy | n | mean_ret | win | p6 | p8 | stop | target | positive_month |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for row in payload["policy_summaries"]:
        lines.append(
            f"| {row['policy_id']} | {row['n']} | {row.get('mean_short_ret', 0):.4f} | "
            f"{row.get('win_rate', 0):.3f} | {row.get('p_mfe_ge_6pct', 0):.3f} | "
            f"{row.get('p_mfe_ge_8pct', 0):.3f} | {row.get('stop_hit_rate', 0):.3f} | "
            f"{row.get('target_hit_rate', 0):.3f} | {row.get('positive_month_rate', 0):.3f} |"
        )
    lines.extend(
        [
            "",
            "## Notes",
            "",
            "- SetupReady immediate means shorting from the original signal replay entry.",
            "- SetupReady then ContinuationPermit means keeping SetupReady as watch state and only treating confirmed continuation as actionable.",
            "- Review-only. No production ranking, MeeMee, or runtime DB changed.",
        ]
    )
    return "\n".join(lines) + "\n"


def run(setup_replay_path: Path, output_root: Path) -> Path:
    run_dir = output_root / _run_id()
    setup_replay = _read_json(setup_replay_path)
    events = list(setup_replay.get("events", []))
    immediate_rows, confirmed_rows, skipped_rows = _policy_rows(events)
    immediate = _summarize(immediate_rows, "setup_ready_immediate_next_open")
    confirmed = _summarize(confirmed_rows, "setup_ready_then_continuation_permit")
    skipped = _summarize(skipped_rows, "setup_ready_skipped_no_permit")
    payload = {
        "run_id": run_dir.name,
        "created_at": _utc_now(),
        "axis_id": AXIS_ID,
        "setup_replay_path": str(setup_replay_path),
        "fixed_evaluation_conditions": {
            "setup_state": SETUP_STATE,
            "policy_immediate": "short at original signal replay entry",
            "policy_confirmed": "watch SetupReady and only act when existing VisualContinuationPermit condition occurs",
            "exit": "inherits upstream realistic target/sl8/bullish denial replay",
            "production_change": False,
        },
        "policy_summaries": [immediate, confirmed, skipped],
        "research_decision": _decision(immediate, confirmed, skipped),
        "confirmed_examples": sorted(confirmed_rows, key=lambda row: float(row.get("policy_short_ret") or 0.0), reverse=True)[:100],
        "skipped_examples": sorted(skipped_rows, key=lambda row: float(row.get("policy_short_ret") or 0.0), reverse=True)[:100],
        "runtime_db_write": False,
        "meemee_modified": False,
        "production_ranking_modified": False,
    }
    _write_json(run_dir / "short_setup_profitability_audit.json", payload)
    _write_jsonl(run_dir / "setup_ready_confirmed_examples.jsonl", payload["confirmed_examples"])
    _write_jsonl(run_dir / "setup_ready_skipped_examples.jsonl", payload["skipped_examples"])
    (run_dir / "short_setup_profitability_audit_summary.md").write_text(_markdown(payload), encoding="utf-8")
    _write_json(
        run_dir / "_ARTIFACT_COMPLETE.json",
        {
            "status": "complete",
            "axis_id": AXIS_ID,
            "generated_at": _utc_now(),
            "required_files": [
                "short_setup_profitability_audit.json",
                "setup_ready_confirmed_examples.jsonl",
                "setup_ready_skipped_examples.jsonl",
                "short_setup_profitability_audit_summary.md",
                "_ARTIFACT_COMPLETE.json",
            ],
        },
    )
    return run_dir


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--setup-replay-path", type=Path, default=DEFAULT_SETUP_REPLAY)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    args = parser.parse_args()
    print(run(args.setup_replay_path, args.output_root))


if __name__ == "__main__":
    main()
