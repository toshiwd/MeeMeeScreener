from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.tradex_short_downside_target_overlay_v1 import _add_context_features, _load_code_bars
from scripts.tradex_short_visual_micro_path_replay_v1 import (
    AXIS_ID as PRIOR_VISUAL_AXIS_ID,
    _json_ready,
    _micro_path_label,
    _write_json,
)
from shared.runtime_stock_db_contract import inspect_runtime_stock_db, resolve_runtime_stock_db_path


AXIS_ID = "current_short_visual_micro_path_board_v1"
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\current_short_visual_micro_path_board_v1")
DEFAULT_SOURCE_BOARD = Path(
    r"G:\Tradex\current_short_decision_support_board_v1"
    r"\20260605T004637Z-current_short_decision_support_board_v1"
    r"\current_short_decision_support_board.json"
)
PRIOR_VISUAL_REPLAY_ARTIFACT = Path(
    r"G:\Tradex\short_visual_micro_path_replay_v1"
    r"\20260605T010450Z-short_visual_micro_path_replay_v1"
    r"\short_visual_micro_path_replay.json"
)


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _run_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + f"-{AXIS_ID}"


def _read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _visual_micro_action(label: str, continuation_status: str) -> str:
    if label in {"BounceRiskHigh", "TooExtendedDown"}:
        return "VisualBlock"
    if label in {"SellableRollover", "CleanContinuationDown"} and continuation_status == "ContinuationPermit":
        return "VisualContinuationPermit"
    if label in {"SellableRollover", "CleanContinuationDown"}:
        return "VisualSetupWatch"
    if label == "PullbackBeforeBreak":
        return "VisualWaitBreak"
    return "VisualNoEdge"


def _find_signal_row(enriched: pd.DataFrame, signal_ymd: int) -> tuple[int | None, pd.Series | None]:
    matches = enriched.index[enriched["ymd"].astype(int) == int(signal_ymd)].tolist()
    if not matches:
        return None, None
    idx = int(matches[-1])
    return idx, enriched.iloc[idx]


def _topk_breakdown(candidates: list[dict[str, Any]]) -> dict[str, dict[str, int]]:
    out: dict[str, dict[str, int]] = {}
    for k in (5, 10, 20):
        out[f"top{k}"] = dict(Counter(str(item["visual_micro_action"]) for item in candidates[:k]))
    return out


def _markdown(payload: dict[str, Any]) -> str:
    lines = [
        "# Current Short Visual Micro Path Board v1",
        "",
        f"- source_board_path: `{payload['source_board_path']}`",
        f"- prior_visual_replay_artifact_path: `{payload['prior_visual_replay_artifact_path']}`",
        f"- authoritative_decision: `{payload['authoritative_decision']}`",
        "",
        "## Counts",
        "",
    ]
    for key, value in payload["counts"].items():
        lines.append(f"- {key}: {value}")
    lines.extend(
        [
            "",
            "## Top20",
            "",
            "| rank | code | signal | final | continuation | visual_label | visual_action | downside | rr | reasons |",
            "|---:|---|---:|---|---|---|---|---:|---:|---|",
        ]
    )
    for item in payload["candidates"][:20]:
        downside = item.get("expected_downside_pct")
        rr = item.get("risk_reward_to_sl8")
        lines.append(
            f"| {item['original_rank']} | {item['code']} | {item['signal_ymd']} | "
            f"{item.get('final_review_status')} | {item.get('continuation_status')} | "
            f"{item.get('visual_micro_label')} | {item.get('visual_micro_action')} | "
            f"{'' if downside is None else round(float(downside) * 100, 2)} | "
            f"{'' if rr is None else round(float(rr), 2)} | {', '.join(item.get('visual_micro_reasons') or [])} |"
        )
    lines.extend(
        [
            "",
            "## Notes",
            "",
            "- Review-only visual annotation. This is not a trade recommendation.",
            "- Ranking, score, EntryReady geometry, exit, runtime DB, MeeMee, and production behavior are unchanged.",
        ]
    )
    return "\n".join(lines) + "\n"


def run(db_path: Path, output_root: Path, source_board_path: Path, prior_visual_replay_artifact: Path) -> Path:
    run_dir = output_root / _run_id()
    source = _read_json(source_board_path)
    source_candidates = list(source.get("candidates", []))
    codes = {str(item["code"]) for item in source_candidates if item.get("code") is not None}
    signal_ymds = [int(item["signal_ymd"]) for item in source_candidates if item.get("signal_ymd") is not None]
    min_ymd = min(signal_ymds) - 20000 if signal_ymds else 20150101
    bars_by_code = _load_code_bars(db_path, codes, min_ymd, 20991231)

    candidates: list[dict[str, Any]] = []
    missing_visual = 0
    for item in source_candidates:
        code = str(item.get("code"))
        signal_ymd = int(item.get("signal_ymd"))
        enriched = _add_context_features(bars_by_code.get(code, pd.DataFrame()))
        idx, _ = _find_signal_row(enriched, signal_ymd) if not enriched.empty else (None, None)
        if idx is None:
            missing_visual += 1
            visual = {
                "visual_micro_label": "VisualDataMissing",
                "visual_micro_reasons": ["signal_bar_missing_for_visual_micro_annotation"],
            }
        else:
            visual = _micro_path_label(enriched, idx)
        visual_action = _visual_micro_action(
            str(visual["visual_micro_label"]),
            str(item.get("continuation_status") or ""),
        )
        candidates.append(
            {
                **item,
                **visual,
                "visual_micro_action": visual_action,
                "visual_review_note": "Visual micro path is a review-only annotation and does not change ranking or final status.",
            }
        )

    counts = {
        "total_candidates": len(candidates),
        "visual_micro_label_counts": dict(Counter(str(item["visual_micro_label"]) for item in candidates)),
        "visual_micro_action_counts": dict(Counter(str(item["visual_micro_action"]) for item in candidates)),
        "visual_data_missing_count": missing_visual,
        "source_final_status_counts": source.get("counts", {}).get("final_status_counts", {}),
    }
    action_counts = counts["visual_micro_action_counts"]
    if action_counts.get("VisualContinuationPermit", 0) > 0:
        decision = "current_board_has_visual_continuation_permit_annotations_review_only"
    elif action_counts.get("VisualSetupWatch", 0) > 0:
        decision = "current_board_has_visual_setup_watch_but_no_visual_continuation_permit"
    elif action_counts.get("VisualBlock", 0) >= max(1, len(candidates) // 2):
        decision = "current_board_visual_micro_mostly_blocked_or_bounce_risk"
    else:
        decision = "current_board_visual_micro_no_actionable_group"

    payload = {
        "run_id": run_dir.name,
        "created_at": _utc_now(),
        "axis_id": AXIS_ID,
        "source_board_path": str(source_board_path),
        "prior_visual_axis_id": PRIOR_VISUAL_AXIS_ID,
        "prior_visual_replay_artifact_path": str(prior_visual_replay_artifact),
        "db_path": str(db_path),
        "runtime_status": inspect_runtime_stock_db(runtime_db_path=db_path),
        "counts": counts,
        "topK_breakdown": _topk_breakdown(candidates),
        "candidates": candidates,
        "authoritative_decision": decision,
        "runtime_db_write": False,
        "meemee_modified": False,
        "production_ranking_modified": False,
    }
    _write_json(run_dir / "current_short_visual_micro_path_board.json", payload)
    (run_dir / "current_short_visual_micro_path_board_summary.md").write_text(_markdown(payload), encoding="utf-8")
    _write_json(
        run_dir / "_ARTIFACT_COMPLETE.json",
        {
            "status": "complete",
            "axis_id": AXIS_ID,
            "generated_at": _utc_now(),
            "required_files": [
                "current_short_visual_micro_path_board.json",
                "current_short_visual_micro_path_board_summary.md",
                "_ARTIFACT_COMPLETE.json",
            ],
        },
    )
    return run_dir


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db-path", type=Path, default=resolve_runtime_stock_db_path())
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--source-board-path", type=Path, default=DEFAULT_SOURCE_BOARD)
    parser.add_argument("--prior-visual-replay-artifact", type=Path, default=PRIOR_VISUAL_REPLAY_ARTIFACT)
    args = parser.parse_args()
    print(run(args.db_path, args.output_root, args.source_board_path, args.prior_visual_replay_artifact))


if __name__ == "__main__":
    main()
