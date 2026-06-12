from __future__ import annotations

import argparse
import json
import math
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


AXIS_ID = "current_short_visual_shadow_rerank_v1"
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\current_short_visual_shadow_rerank_v1")
DEFAULT_CURRENT_VISUAL_BOARD = Path(
    r"G:\Tradex\current_short_visual_micro_path_board_v1"
    r"\20260605T010729Z-current_short_visual_micro_path_board_v1"
    r"\current_short_visual_micro_path_board.json"
)
DEFAULT_DOWNSIDE_STATS = Path(
    r"G:\Tradex\short_visual_micro_path_downside_stats_v1"
    r"\20260605T011408Z-short_visual_micro_path_downside_stats_v1"
    r"\short_visual_micro_path_downside_stats.json"
)


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
    if isinstance(value, float) and not math.isfinite(value):
        return None
    return value


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(_json_ready(payload), ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _stat_lookup(stats: dict[str, Any], key: str) -> dict[str, dict[str, Any]]:
    return {str(row[key]): row for row in stats}


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        out = float(value)
    except Exception:
        return default
    return out if math.isfinite(out) else default


def _visual_probability_payload(candidate: dict[str, Any], action_stats: dict[str, dict[str, Any]], label_stats: dict[str, dict[str, Any]]) -> dict[str, Any]:
    action = str(candidate.get("visual_micro_action") or "")
    label = str(candidate.get("visual_micro_label") or "")
    stat = action_stats.get(action) or label_stats.get(label) or {}
    fallback_source = "visual_micro_action" if action in action_stats else "visual_micro_label" if label in label_stats else "missing"
    return {
        "visual_probability_source": fallback_source,
        "visual_stats_n": stat.get("n"),
        "visual_downside_confidence": stat.get("downside_confidence"),
        "visual_mean_mfe_20": stat.get("mean_mfe_20"),
        "visual_p_mfe_ge_4pct": stat.get("p_mfe_ge_4pct"),
        "visual_p_mfe_ge_6pct": stat.get("p_mfe_ge_6pct"),
        "visual_p_mfe_ge_8pct": stat.get("p_mfe_ge_8pct"),
        "visual_p_mfe_ge_10pct": stat.get("p_mfe_ge_10pct"),
        "visual_stop_hit_rate": stat.get("stop_hit_rate"),
        "visual_expected_short_ret": stat.get("mean_short_ret"),
    }


def _visual_overlay_score(candidate: dict[str, Any], probs: dict[str, Any]) -> float:
    p6 = _safe_float(probs.get("visual_p_mfe_ge_6pct"))
    p8 = _safe_float(probs.get("visual_p_mfe_ge_8pct"))
    p10 = _safe_float(probs.get("visual_p_mfe_ge_10pct"))
    stop = _safe_float(probs.get("visual_stop_hit_rate"), 0.5)
    expected = _safe_float(probs.get("visual_expected_short_ret"))
    confidence = str(probs.get("visual_downside_confidence") or "")
    base = (0.40 * p6) + (0.35 * p8) + (0.25 * p10) - (0.35 * stop) + (1.5 * expected)
    if confidence == "high":
        base += 0.15
    elif confidence == "medium":
        base += 0.05
    elif confidence in {"low", "thin_sample"}:
        base -= 0.08
    if str(candidate.get("final_review_status")) in {"Avoid"}:
        base -= 0.20
    if str(candidate.get("regime_permission_status")) == "BlockShort":
        base -= 0.12
    return base


def _rank_bucket(candidate: dict[str, Any]) -> int:
    final_status = str(candidate.get("final_review_status") or "")
    visual_action = str(candidate.get("visual_micro_action") or "")
    if final_status == "Avoid":
        return 3
    if visual_action == "VisualContinuationPermit":
        return 0
    if visual_action == "VisualSetupWatch":
        return 1
    return 2


def _topk_members(rows: list[dict[str, Any]], k: int, rank_key: str) -> set[str]:
    ordered = sorted(rows, key=lambda row: int(row[rank_key]))
    return {f"{row.get('code')}:{row.get('signal_ymd')}" for row in ordered[:k]}


def _rank_change_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for k in (5, 10, 20):
        original = _topk_members(rows, k, "original_rank")
        shadow = _topk_members(rows, k, "shadow_visual_rank")
        out[f"top{k}"] = {
            "changed_members_count": len(original.symmetric_difference(shadow)) // 2,
            "added": sorted(shadow - original),
            "removed": sorted(original - shadow),
        }
    out["changed_rank_count"] = sum(1 for row in rows if int(row["original_rank"]) != int(row["shadow_visual_rank"]))
    return out


def _topk_breakdown(rows: list[dict[str, Any]], k: int) -> dict[str, int]:
    ordered = sorted(rows, key=lambda row: int(row["shadow_visual_rank"]))[:k]
    return dict(Counter(str(row.get("visual_micro_action")) for row in ordered))


def _markdown(payload: dict[str, Any]) -> str:
    lines = [
        "# Current Short Visual Shadow Rerank v1",
        "",
        f"- authoritative_decision: `{payload['authoritative_decision']}`",
        f"- current_visual_board_path: `{payload['current_visual_board_path']}`",
        f"- downside_stats_path: `{payload['downside_stats_path']}`",
        "",
        "## Rank Change",
        "",
    ]
    for key, value in payload["observed_branching"].items():
        lines.append(f"- {key}: {value}")
    lines.extend(
        [
            "",
            "## Shadow Top20",
            "",
            "| shadow_rank | original_rank | code | signal | final | visual_action | p6 | p8 | p10 | stop | overlay | original_score | shadow_score |",
            "|---:|---:|---|---:|---|---|---:|---:|---:|---:|---:|---:|---:|",
        ]
    )
    for row in sorted(payload["candidates"], key=lambda item: int(item["shadow_visual_rank"]))[:20]:
        lines.append(
            f"| {row['shadow_visual_rank']} | {row['original_rank']} | {row['code']} | {row['signal_ymd']} | "
            f"{row.get('final_review_status')} | {row.get('visual_micro_action')} | "
            f"{_safe_float(row.get('visual_p_mfe_ge_6pct')):.3f} | {_safe_float(row.get('visual_p_mfe_ge_8pct')):.3f} | "
            f"{_safe_float(row.get('visual_p_mfe_ge_10pct')):.3f} | {_safe_float(row.get('visual_stop_hit_rate')):.3f} | "
            f"{row['visual_overlay_score']:.4f} | {_safe_float(row.get('original_score')):.4f} | {row['shadow_visual_score']:.4f} |"
        )
    lines.extend(
        [
            "",
            "## Notes",
            "",
            "- Shadow rerank only. No production ranking score changed.",
            "- This artifact is the ranking-integration preflight, not a trade recommendation.",
        ]
    )
    return "\n".join(lines) + "\n"


def run(current_visual_board_path: Path, downside_stats_path: Path, output_root: Path) -> Path:
    run_dir = output_root / _run_id()
    board = _read_json(current_visual_board_path)
    stats = _read_json(downside_stats_path)
    action_stats = _stat_lookup(stats["visual_micro_action_downside_stats"], "visual_micro_action")
    label_stats = _stat_lookup(stats["visual_micro_label_downside_stats"], "visual_micro_label")
    candidates: list[dict[str, Any]] = []
    for row in board.get("candidates", []):
        probs = _visual_probability_payload(row, action_stats, label_stats)
        overlay = _visual_overlay_score(row, probs)
        original_score = _safe_float(row.get("original_score"))
        shadow_score = original_score + overlay
        candidates.append(
            {
                **row,
                **probs,
                "visual_overlay_score": overlay,
                "shadow_visual_score": shadow_score,
                "shadow_rank_bucket": _rank_bucket(row),
            }
        )
    candidates.sort(key=lambda row: (int(row["shadow_rank_bucket"]), -float(row["shadow_visual_score"]), int(row["original_rank"])))
    for idx, row in enumerate(candidates, start=1):
        row["shadow_visual_rank"] = idx
        row["shadow_rank_delta"] = int(row["original_rank"]) - idx
    candidates.sort(key=lambda row: int(row["original_rank"]))

    branching = _rank_change_summary(candidates)
    shadow_top10 = _topk_breakdown(candidates, 10)
    top10_rows = sorted(candidates, key=lambda row: int(row["shadow_visual_rank"]))[:10]
    avoid_promoted = any(str(row.get("final_review_status")) == "Avoid" for row in top10_rows) and any(
        str(row.get("final_review_status")) != "Avoid" for row in candidates[10:]
    )
    if avoid_promoted:
        decision = "hold_shadow_visual_rerank_contract_failed_avoid_promoted"
    elif shadow_top10.get("VisualContinuationPermit", 0) > 0 and branching["top10"]["changed_members_count"] > 0:
        decision = "hold_shadow_visual_rerank_ready_for_historical_same_condition_replay"
    elif shadow_top10.get("VisualContinuationPermit", 0) > 0:
        decision = "hold_visual_overlay_adds_signal_but_does_not_branch_top10"
    else:
        decision = "drop_current_visual_shadow_rerank_no_top10_actionable_signal"

    payload = {
        "run_id": run_dir.name,
        "created_at": _utc_now(),
        "axis_id": AXIS_ID,
        "current_visual_board_path": str(current_visual_board_path),
        "downside_stats_path": str(downside_stats_path),
        "score_formula": {
            "shadow_visual_score": "original_score + visual_overlay_score",
            "visual_overlay_score": "0.40*p6 + 0.35*p8 + 0.25*p10 - 0.35*stop + 1.5*expected_short_ret + confidence_adjustment - status_penalties",
            "rank_order": "shadow_rank_bucket first, then shadow_visual_score, then original_rank",
            "rank_bucket": {
                "0": "non-Avoid VisualContinuationPermit",
                "1": "non-Avoid VisualSetupWatch",
                "2": "other non-Avoid",
                "3": "Avoid never promoted above non-Avoid",
            },
            "confidence_adjustment": {"high": 0.15, "medium": 0.05, "low": -0.08, "thin_sample": -0.08},
            "status_penalties": {"final_review_status=Avoid": -0.20, "regime_permission_status=BlockShort": -0.12},
        },
        "observed_branching": branching,
        "shadow_topK_breakdown": {f"top{k}": _topk_breakdown(candidates, k) for k in (5, 10, 20)},
        "candidates": candidates,
        "authoritative_decision": decision,
        "runtime_db_write": False,
        "meemee_modified": False,
        "production_ranking_modified": False,
    }
    _write_json(run_dir / "current_short_visual_shadow_rerank.json", payload)
    (run_dir / "current_short_visual_shadow_rerank_summary.md").write_text(_markdown(payload), encoding="utf-8")
    _write_json(
        run_dir / "_ARTIFACT_COMPLETE.json",
        {
            "status": "complete",
            "axis_id": AXIS_ID,
            "generated_at": _utc_now(),
            "required_files": [
                "current_short_visual_shadow_rerank.json",
                "current_short_visual_shadow_rerank_summary.md",
                "_ARTIFACT_COMPLETE.json",
            ],
        },
    )
    return run_dir


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--current-visual-board-path", type=Path, default=DEFAULT_CURRENT_VISUAL_BOARD)
    parser.add_argument("--downside-stats-path", type=Path, default=DEFAULT_DOWNSIDE_STATS)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    args = parser.parse_args()
    print(run(args.current_visual_board_path, args.downside_stats_path, args.output_root))


if __name__ == "__main__":
    main()
