from __future__ import annotations

import argparse
import json
import math
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

if __package__ in {None, ""}:
    sys.path.append(str(Path(__file__).resolve().parents[1]))


AXIS_ID = "starter_candidate_review_pack_v3"
DEFAULT_CONTRACT_ROOT = Path(r"G:\Tradex\daily_feature_freshness_contract_v1\20260525T054141Z-daily-feature-freshness-contract-v1")
DEFAULT_V2_ROOT = Path(r"G:\Tradex\starter_candidate_review_pack_v2\20260525T054226Z-starter-candidate-review-pack-v2")
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\starter_candidate_review_pack_v3")

REQUIRED_ARTIFACTS = (
    "review_pack_summary.json",
    "review_candidate_rows.csv",
    "review_candidate_cards.json",
    "candidate_manual_checklist.json",
    "starter_promotion_conditions.json",
    "avoid_conditions.json",
    "family_specific_review_rules.json",
    "data_freshness_summary.json",
    "review_pack_decision.json",
    "_ARTIFACT_COMPLETE.json",
)

CLASS_ORDER = {"validated_buy": 0, "starter_review": 1, "watch": 2, "wait": 3, "avoid": 4}


def _now_tag() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _json_ready(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): _json_ready(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_ready(v) for v in value]
    if isinstance(value, Path):
        return str(value)
    if hasattr(value, "item"):
        return _json_ready(value.item())
    if isinstance(value, float) and not math.isfinite(value):
        return None
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass
    return value


def _write_json(path: Path, payload: dict[str, Any] | list[dict[str, Any]]) -> None:
    path.write_text(json.dumps(_json_ready(payload), ensure_ascii=False, sort_keys=True, indent=2) + "\n", encoding="utf-8")


def _parse_json(value: Any, default: Any) -> Any:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return default
    try:
        return json.loads(str(value))
    except Exception:
        return default


def _split_pipe(value: Any) -> list[str]:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return []
    return [part for part in str(value).split("|") if part]


def _as_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    return str(value).strip().lower() in {"1", "true", "yes", "y"}


def family_rules() -> dict[str, dict[str, list[str] | str]]:
    return {
        "pullback_reclaim_source": {
            "primary_watch_reason": "Pullback/reclaim context is present, but entry timing/liquidity confirmation is not complete.",
            "starter_promotion_conditions": [
                "MA7/MA20 reclaim is confirmed on the current chart.",
                "Close remains above MA20 after reclaim and is not severely extended from MA20.",
                "No large bearish candle, failed high, or high-volatility weak candle appears at the entry point.",
            ],
            "wait_conditions": [
                "Pullback is still forming and close has not reclaimed MA7/MA20.",
                "Reclaim exists but candle quality is neutral or volume confirmation is unclear.",
            ],
            "avoid_conditions": [
                "Reclaim fails and close returns below MA20.",
                "Lower-high or failed-reclaim pattern appears.",
                "High volatility combines with a weak candle.",
            ],
            "manual_chart_checkpoints": [
                "Check close versus MA7 and MA20.",
                "Check whether the latest candle confirms reclaim rather than only intraday touch.",
                "Check failed-high or lower-high risk near recent resistance.",
            ],
        },
        "breakout_retest_source": {
            "primary_watch_reason": "Breakout/retest context is present, but retest hold and failure-risk checks are still manual.",
            "starter_promotion_conditions": [
                "Retest holds or pullback remains shallow after breakout.",
                "No upper-wick failure or failed high is visible.",
                "Breakout is not severely overextended and volume quality is acceptable.",
            ],
            "wait_conditions": [
                "Breakout is valid but already extended enough to wait for retest.",
                "Retest has started but hold is not yet confirmed.",
            ],
            "avoid_conditions": [
                "Failed breakout or failed high occurs.",
                "Upper wick appears after breakout.",
                "High volatility follows breakout without constructive hold.",
            ],
            "manual_chart_checkpoints": [
                "Check whether breakout is fresh or already extended.",
                "Check if retest held above the breakout zone.",
                "Check volume quality on breakout and retest.",
            ],
        },
        "early_trend_source": {
            "primary_watch_reason": "Early trend context is present, but trend confirmation is thin and needs manual chart confirmation.",
            "starter_promotion_conditions": [
                "Trend is emerging but not overextended.",
                "MA7/MA20 structure is constructive.",
                "Close remains above key moving averages without failed-start candle risk.",
            ],
            "wait_conditions": [
                "Early trend is forming but MA structure is not yet constructive.",
                "Entry candle is neutral and confirmation needs another session.",
            ],
            "avoid_conditions": [
                "Early trend false start appears.",
                "Close breaks below key moving average support.",
            ],
            "manual_chart_checkpoints": [
                "Check MA7/MA20 slope and ordering.",
                "Check whether the trend has enough sessions above MA20.",
                "Check for false-start candle or immediate rejection.",
            ],
        },
        "overextension_risk_source": {
            "primary_watch_reason": "Overextension risk context is present; default handling is wait/watch, not starter.",
            "starter_promotion_conditions": [
                "Risk flags are low despite overextension family assignment.",
                "Continuation evidence is visible and there is no failed-high/upper-wick signal.",
                "Entry remains close enough to a support/retest area to avoid chasing.",
            ],
            "wait_conditions": [
                "Continuation exists but distance from MA20/MA60 is still extended.",
                "Price needs pullback or consolidation before entry review.",
            ],
            "avoid_conditions": [
                "High volatility, steep MA slope, failed high, or upper wick appears.",
                "Extension accelerates without nearby support.",
            ],
            "manual_chart_checkpoints": [
                "Check distance from MA20 and MA60.",
                "Check steep MA7 slope and upper wick.",
                "Check whether continuation evidence outweighs chase risk.",
            ],
        },
        "default": {
            "primary_watch_reason": "Family context requires manual confirmation before any starter review.",
            "starter_promotion_conditions": ["Fresh family context is confirmed and risk flags remain low."],
            "wait_conditions": ["Timing or chart confirmation is unclear."],
            "avoid_conditions": ["Coverage, risk, or failed setup condition appears."],
            "manual_chart_checkpoints": ["Check moving-average support, candle quality, and current risk flags."],
        },
    }


def _rule_for(family: str) -> dict[str, Any]:
    return family_rules().get(family, family_rules()["default"])


def _confidence_level(action_class: str) -> str:
    if action_class in CLASS_ORDER:
        return action_class
    return "watch"


def enrich_rows(v2_rows: pd.DataFrame, surface_rows: pd.DataFrame) -> pd.DataFrame:
    rows = v2_rows.copy()
    rows["code"] = rows["code"].astype(str)
    surface = surface_rows.copy()
    surface["code"] = surface["code"].astype(str)
    keep_cols = [
        "code",
        "family_assignment_reason_json",
        "research_family_assignment_reason_json",
        "close_y",
        "ma7",
        "ma20",
        "ma60",
        "dist_ma20_pct",
        "dist_ma60_pct",
        "upper_wick_ratio",
        "volume_ma20_ratio",
        "daily_bar_source",
        "daily_bar_max_date",
        "feature_source_max_date",
        "feature_freshness_status",
        "provisional_used",
    ]
    available = [c for c in keep_cols if c in surface.columns]
    rows = rows.merge(surface[available].drop_duplicates("code"), on="code", how="left", suffixes=("", "_surface"))
    enriched: list[dict[str, Any]] = []
    for rec in rows.to_dict("records"):
        family = str(rec.get("research_candidate_source_family") or "")
        rule = _rule_for(family)
        assignment = _parse_json(rec.get("research_family_assignment_reason_json") or rec.get("family_assignment_reason_json"), {})
        risk_flags = _split_pipe(rec.get("risk_flags"))
        reasons = _split_pipe(rec.get("classification_reason"))
        action = str(rec.get("candidate_action_class") or "watch")
        primary_reason = str(rule["primary_watch_reason"])
        if "entry_liquidity_or_data_coverage_gap" in reasons:
            primary_reason += " Current v2 blocker: entry/liquidity confirmation is incomplete."
        if "overextension_limited_to_watch_wait" in reasons:
            primary_reason += " Overextension family is capped at watch/wait unless continuation evidence is strong."
        rec.update(
            {
                "data_source": rec.get("daily_bar_source") or rec.get("daily_bar_source_surface"),
                "feature_freshness_status": rec.get("feature_freshness_status") or rec.get("feature_freshness_status_surface"),
                "family_assignment_reason": json.dumps(assignment, sort_keys=True, ensure_ascii=False),
                "primary_watch_reason": primary_reason,
                "risk_flags": "|".join(risk_flags),
                "starter_promotion_conditions": json.dumps(rule["starter_promotion_conditions"], ensure_ascii=False),
                "wait_conditions": json.dumps(rule["wait_conditions"], ensure_ascii=False),
                "avoid_conditions": json.dumps(rule["avoid_conditions"], ensure_ascii=False),
                "manual_chart_checkpoints": json.dumps(rule["manual_chart_checkpoints"], ensure_ascii=False),
                "confidence_level": _confidence_level(action),
                "validation_status": "manual_review_only",
                "not_validated_reason": "not_validated_challenger; no keep-gated validated buy artifact",
            }
        )
        enriched.append(rec)
    out = pd.DataFrame(enriched)
    out["_class_order"] = out["candidate_action_class"].map(CLASS_ORDER).fillna(9)
    return out.sort_values(["_class_order", "review_rank"], ascending=[True, True]).drop(columns=["_class_order"]).reset_index(drop=True)


def _cards(rows: pd.DataFrame, limit: int = 5) -> list[dict[str, Any]]:
    cards: list[dict[str, Any]] = []
    for row in rows.head(limit).to_dict("records"):
        cards.append(
            {
                "code": row["code"],
                "decision_date": int(row["decision_date"]),
                "candidate_action_class": row["candidate_action_class"],
                "research_candidate_source_family": row["research_candidate_source_family"],
                "baseline_rank": row["baseline_rank"],
                "baseline_score": row["baseline_score"],
                "data_source": row.get("data_source"),
                "feature_freshness_status": row.get("feature_freshness_status"),
                "family_assignment_reason": _parse_json(row.get("family_assignment_reason"), {}),
                "primary_watch_reason": row["primary_watch_reason"],
                "risk_flags": _split_pipe(row.get("risk_flags")),
                "starter_promotion_conditions": _parse_json(row.get("starter_promotion_conditions"), []),
                "wait_conditions": _parse_json(row.get("wait_conditions"), []),
                "avoid_conditions": _parse_json(row.get("avoid_conditions"), []),
                "manual_chart_checkpoints": _parse_json(row.get("manual_chart_checkpoints"), []),
                "confidence_level": row["confidence_level"],
                "validation_status": "not_validated_challenger; manual_review_only",
            }
        )
    return cards


def build_pack(contract_root: Path, v2_root: Path, output_root: Path) -> Path:
    out = output_root / f"{_now_tag()}-starter-candidate-review-pack-v3"
    out.mkdir(parents=True, exist_ok=True)
    v2_rows = pd.read_csv(v2_root / "review_candidate_rows.csv", low_memory=False)
    surface_rows = pd.read_csv(contract_root / "current_family_surface_rows.csv", low_memory=False)
    root_decision = json.loads((contract_root / "research_decision.json").read_text(encoding="utf-8"))
    freshness = json.loads((contract_root / "family_feature_freshness_report.json").read_text(encoding="utf-8"))
    v2_summary = json.loads((v2_root / "review_pack_summary.json").read_text(encoding="utf-8"))
    rows = enrich_rows(v2_rows, surface_rows)
    rows.to_csv(out / "review_candidate_rows.csv", index=False)
    cards = _cards(rows)
    _write_json(out / "review_candidate_cards.json", cards)
    checklist = {
        row["code"]: {
            "starter_promotion_conditions": _parse_json(row["starter_promotion_conditions"], []),
            "wait_conditions": _parse_json(row["wait_conditions"], []),
            "avoid_conditions": _parse_json(row["avoid_conditions"], []),
            "manual_chart_checkpoints": _parse_json(row["manual_chart_checkpoints"], []),
        }
        for row in rows.to_dict("records")
    }
    _write_json(out / "candidate_manual_checklist.json", checklist)
    _write_json(out / "starter_promotion_conditions.json", {k: v["starter_promotion_conditions"] for k, v in checklist.items()})
    _write_json(out / "avoid_conditions.json", {k: v["avoid_conditions"] for k, v in checklist.items()})
    _write_json(out / "family_specific_review_rules.json", family_rules())
    counts = {name: int((rows["candidate_action_class"] == name).sum()) for name in CLASS_ORDER}
    data_freshness = {
        "review_date": int(v2_summary["review_date"]),
        "latest_global_candidate_date": int(v2_summary["date_report"]["candidate_family_source_latest_date"]),
        "current_review_ready_confirmed": root_decision.get("research_decision") == "current_review_ready_confirmed",
        "daily_bar_source": freshness.get("daily_bar_source"),
        "daily_bar_max_date": freshness.get("daily_bar_max_date"),
        "feature_source_max_date": freshness.get("feature_source_max_date"),
        "feature_freshness_status_counts": freshness.get("feature_freshness_status_counts"),
        "provisional_used": freshness.get("provisional_used"),
    }
    _write_json(out / "data_freshness_summary.json", data_freshness)
    manual_available = bool(v2_summary.get("manual_review_available")) and data_freshness["current_review_ready_confirmed"]
    summary = {
        "axis_id": AXIS_ID,
        "scope": "TRADEX-only",
        "review_date": data_freshness["review_date"],
        "candidate_count": int(len(rows)),
        "primary_card_count": len(cards),
        "class_counts": counts,
        "manual_review_available": manual_available,
        "current_review_ready_confirmed": data_freshness["current_review_ready_confirmed"],
        "source_roots": {"daily_feature_contract_root": str(contract_root), "v2_root": str(v2_root)},
    }
    _write_json(out / "review_pack_summary.json", summary)
    _write_json(
        out / "review_pack_decision.json",
        {
            "validated_buy_count": counts["validated_buy"],
            "starter_review_count": counts["starter_review"],
            "watch_count": counts["watch"],
            "wait_count": counts["wait"],
            "avoid_count": counts["avoid"],
            "manual_review_available": manual_available,
            "no_validated_buy_today": counts["validated_buy"] == 0,
            "current_review_ready_confirmed": data_freshness["current_review_ready_confirmed"],
            "meemee_reflectable_candidate": False,
            "blocker_reason": "not validated challenger; manual review only",
            "validated_buy_claim": False,
            "runtime_db_write": False,
            "production_ranking_changed": False,
        },
    )
    existing = [name for name in REQUIRED_ARTIFACTS if name != "_ARTIFACT_COMPLETE.json" and (out / name).exists()]
    _write_json(out / "_ARTIFACT_COMPLETE.json", {"axis_id": AXIS_ID, "required_artifacts": list(REQUIRED_ARTIFACTS), "complete": len(existing) == len(REQUIRED_ARTIFACTS) - 1})
    return out


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--contract-root", type=Path, default=DEFAULT_CONTRACT_ROOT)
    parser.add_argument("--v2-root", type=Path, default=DEFAULT_V2_ROOT)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    args = parser.parse_args()
    print(build_pack(args.contract_root, args.v2_root, args.output_root))


if __name__ == "__main__":
    main()
