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


AXIS_ID = "starter_candidate_review_pack_v2"
DEFAULT_FAMILY_SOURCE_ROOT = Path(r"G:\Tradex\starter_entry_family_source_split_design_v1\20260525T041110Z-starter-entry-family-source-split-design-v1")
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\starter_candidate_review_pack_v2")

REQUIRED_ARTIFACTS = (
    "review_pack_summary.json",
    "review_candidate_rows.csv",
    "review_candidate_cards.json",
    "family_pick_summary.csv",
    "classification_reason_summary.json",
    "no_validated_buy_warning.json",
    "review_pack_decision.json",
    "_ARTIFACT_COMPLETE.json",
)

SURFACE_FILES = {
    "pullback_reclaim_source": "family_surface_pullback_reclaim.csv",
    "breakout_retest_source": "family_surface_breakout_retest.csv",
    "early_trend_source": "family_surface_early_trend.csv",
    "mature_trend_continuation_source": "family_surface_mature_trend_continuation.csv",
    "overextension_risk_source": "family_surface_overextension_risk.csv",
}
CURRENT_SURFACE_FILE = "current_family_surface_rows.csv"
PREFERRED_FAMILIES = ("pullback_reclaim_source", "breakout_retest_source", "early_trend_source", "mature_trend_continuation_source", "range_reversal_source")
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


def _as_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    return str(value).strip().lower() in {"1", "true", "yes", "y"}


def _parse_json_list(value: Any) -> list[str]:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return []
    if isinstance(value, list):
        return [str(v) for v in value]
    try:
        parsed = json.loads(str(value))
    except Exception:
        return []
    return [str(v) for v in parsed] if isinstance(parsed, list) else []


def _max_decision_date(path: Path) -> int | None:
    latest: int | None = None
    for chunk in pd.read_csv(path, usecols=["decision_date"], chunksize=500_000, low_memory=False):
        value = pd.to_numeric(chunk["decision_date"], errors="coerce").max()
        if pd.notna(value):
            latest = int(value) if latest is None else max(latest, int(value))
    return latest


def _read_date_rows(path: Path, decision_date: int) -> pd.DataFrame:
    wanted = [
        "decision_date",
        "code",
        "baseline_rank",
        "baseline_score",
        "research_candidate_source_family",
        "primary_family",
        "diagnostic_candidate_role",
        "selected_loser",
        "starter_good",
        "starter_bad",
        "immediate_adverse_entry",
        "next_open_available",
        "entry_allowed_by_score",
        "path20_available",
        "research_risk_tags_json",
        "research_setup_tags_json",
        "research_regime_tags_json",
        "source_artifact_path",
        "source_run_id",
        "daily_bar_source",
        "daily_bar_max_date",
        "feature_source_max_date",
        "feature_freshness_status",
        "provisional_used",
    ]
    available = set(pd.read_csv(path, nrows=0).columns)
    cols = [c for c in wanted if c in available]
    parts: list[pd.DataFrame] = []
    for chunk in pd.read_csv(path, usecols=cols, chunksize=500_000, low_memory=False):
        dates = pd.to_numeric(chunk["decision_date"], errors="coerce")
        part = chunk[dates.eq(decision_date)].copy()
        if not part.empty:
            parts.append(part)
    if not parts:
        return pd.DataFrame(columns=cols)
    rows = pd.concat(parts, ignore_index=True)
    rows["baseline_rank"] = pd.to_numeric(rows["baseline_rank"], errors="coerce")
    rows["baseline_score"] = pd.to_numeric(rows["baseline_score"], errors="coerce")
    return rows.sort_values(["baseline_rank", "baseline_score", "code"], ascending=[True, False, True]).reset_index(drop=True)


def resolve_review_date(root: Path) -> tuple[int, dict[str, Any]]:
    source_path = root / CURRENT_SURFACE_FILE if (root / CURRENT_SURFACE_FILE).exists() else root / "candidate_family_source_rows.csv"
    source_latest = _max_decision_date(source_path)
    surface_latest = {
        family: _max_decision_date(root / filename)
        for family, filename in SURFACE_FILES.items()
        if (root / filename).exists()
    }
    if not surface_latest and (root / CURRENT_SURFACE_FILE).exists():
        rows = pd.read_csv(root / CURRENT_SURFACE_FILE, usecols=["decision_date", "research_candidate_source_family"], low_memory=False)
        surface_latest = {
            str(family): int(pd.to_numeric(g["decision_date"], errors="coerce").max())
            for family, g in rows.groupby("research_candidate_source_family")
        }
    usable = [v for k, v in surface_latest.items() if v is not None]
    if not usable:
        raise RuntimeError("no preferred family surface dates available")
    review_date = max(usable)
    return review_date, {
        "candidate_family_source_latest_date": source_latest,
        "family_surface_latest_dates": surface_latest,
        "source_rows_path": str(source_path),
        "review_date_selection_reason": "latest date with preferred family surface candidates",
        "latest_global_date_not_used_reason": None if source_latest == review_date else "latest global rows do not contain any family surface coverage",
    }


def _risk_level(risk_tags: list[str], row: pd.Series) -> str:
    severe_tags = {"large_bearish_candle_risk", "failed_high_update_risk"}
    over_tags = {"ma20_overextension_risk", "ma60_overextension_risk", "steep_ma7_slope_risk"}
    if _as_bool(row.get("selected_loser")) or _as_bool(row.get("immediate_adverse_entry")) or severe_tags.intersection(risk_tags):
        return "severe"
    if "high_volatility_risk" in risk_tags and over_tags.intersection(risk_tags):
        return "high"
    if len(set(risk_tags).intersection(over_tags)) >= 2:
        return "high"
    if risk_tags and risk_tags != ["no_shadow_risk_tag"]:
        return "moderate"
    return "low"


def classify(row: pd.Series, keep_gated: bool = False) -> tuple[str, list[str], float]:
    family = str(row.get("research_candidate_source_family") or "")
    risk_tags = _parse_json_list(row.get("research_risk_tags_json"))
    setup_tags = _parse_json_list(row.get("research_setup_tags_json"))
    risk_level = _risk_level(risk_tags, row)
    entry_ok = _as_bool(row.get("entry_allowed_by_score"))
    next_open_ok = _as_bool(row.get("next_open_available"))
    feature_status = str(row.get("feature_freshness_status") or "")
    coverage_ok = feature_status == "fresh" if feature_status else _as_bool(row.get("path20_available"))
    good_family = family in PREFERRED_FAMILIES
    overextension = family == "overextension_risk_source" or "overextension_candidate" in setup_tags
    reasons: list[str] = []
    score = float(row.get("baseline_score") or 0)

    if keep_gated:
        reasons.append("keep_gated_artifact_present")
        return "validated_buy", reasons, score + 20
    reasons.append("no_keep_gated_artifact")
    if good_family:
        reasons.append("family_surface_pick")
        score += 4
    if family == "pullback_reclaim_source":
        reasons.append("pullback_reclaim_source")
        score += 2
    if family == "breakout_retest_source":
        reasons.append("breakout_retest_source")
        score += 1
    if family in {"early_trend_source", "mature_trend_continuation_source"}:
        reasons.append("thin_but_relevant_family")
    if overextension:
        reasons.append("overextension_limited_to_watch_wait")
        score -= 3
    if risk_level in {"severe", "high"}:
        reasons.append(f"{risk_level}_risk_flags")
        score -= 5
    elif risk_level == "low":
        reasons.append("low_risk_flags")
        score += 1
    else:
        reasons.append("moderate_risk_flags")
    if not entry_ok or not next_open_ok:
        reasons.append("entry_liquidity_or_data_coverage_gap")
        score -= 2
    if not coverage_ok:
        reasons.append("feature_or_data_coverage_gap")
        score -= 5

    if risk_level == "severe" or not coverage_ok:
        return "avoid", reasons, score
    if overextension:
        return ("wait" if risk_level != "low" else "watch"), reasons, score
    if good_family and risk_level == "low" and entry_ok and next_open_ok and coverage_ok:
        return "starter_review", reasons, score
    if good_family:
        return "watch", reasons, score
    if risk_level == "high":
        return "wait", reasons, score
    return "watch", reasons, score


def select_candidates(rows: pd.DataFrame, max_rows: int = 10) -> pd.DataFrame:
    pools: list[pd.DataFrame] = []
    global_pick = rows.sort_values(["baseline_rank", "baseline_score"], ascending=[True, False]).head(3).copy()
    global_pick["pick_source"] = "global_baseline_top"
    pools.append(global_pick)
    for family in ["pullback_reclaim_source", "breakout_retest_source", "early_trend_source", "mature_trend_continuation_source", "range_reversal_source"]:
        pick = rows[rows["research_candidate_source_family"].eq(family)].sort_values(["baseline_rank", "baseline_score"], ascending=[True, False]).head(2).copy()
        pick["pick_source"] = f"{family}_top"
        pools.append(pick)
    over = rows[rows["research_candidate_source_family"].eq("overextension_risk_source")].sort_values(["baseline_rank", "baseline_score"], ascending=[True, False]).head(2).copy()
    over["pick_source"] = "overextension_watch_wait_top"
    pools.append(over)
    selected = pd.concat([p for p in pools if not p.empty], ignore_index=True)
    selected["_dedupe_key"] = selected["code"].astype(str)
    selected = selected.drop_duplicates("_dedupe_key", keep="first").drop(columns=["_dedupe_key"])
    return selected.head(max_rows).reset_index(drop=True)


def build_pack(family_source_root: Path, output_root: Path) -> Path:
    out = output_root / f"{_now_tag()}-starter-candidate-review-pack-v2"
    out.mkdir(parents=True, exist_ok=True)
    review_date, date_report = resolve_review_date(family_source_root)
    source_rows_path = Path(date_report["source_rows_path"])
    all_rows = _read_date_rows(source_rows_path, review_date)
    if all_rows.empty:
        raise RuntimeError(f"no candidate rows for review date {review_date}")
    picked = select_candidates(all_rows, max_rows=10)

    out_rows: list[dict[str, Any]] = []
    for _, row in picked.iterrows():
        klass, reasons, score = classify(row, keep_gated=False)
        out_rows.append(
            {
                "decision_date": int(row["decision_date"]),
                "code": str(row["code"]).removesuffix(".0"),
                "candidate_action_class": klass,
                "validated_buy": klass == "validated_buy",
                "baseline_rank": int(row["baseline_rank"]) if pd.notna(row["baseline_rank"]) else None,
                "baseline_score": float(row["baseline_score"]) if pd.notna(row["baseline_score"]) else None,
                "review_score": score,
                "pick_source": row.get("pick_source"),
                "research_candidate_source_family": row.get("research_candidate_source_family"),
                "primary_family": row.get("primary_family"),
                "starter_role": row.get("diagnostic_candidate_role"),
                "selected_loser": _as_bool(row.get("selected_loser")),
                "starter_good": _as_bool(row.get("starter_good")),
                "starter_bad": _as_bool(row.get("starter_bad")),
                "immediate_adverse_entry": _as_bool(row.get("immediate_adverse_entry")),
                "next_open_available": _as_bool(row.get("next_open_available")),
                "entry_allowed_by_score": _as_bool(row.get("entry_allowed_by_score")),
                "path20_available": _as_bool(row.get("path20_available")),
                "risk_flags": "|".join(_parse_json_list(row.get("research_risk_tags_json"))),
                "setup_tags": "|".join(_parse_json_list(row.get("research_setup_tags_json"))),
                "regime_tags": "|".join(_parse_json_list(row.get("research_regime_tags_json"))),
                "classification_reason": "|".join(reasons),
                "not_validated_buy_reason": "no keep-gated validated challenger artifact",
                "daily_bar_source": row.get("daily_bar_source"),
                "daily_bar_max_date": row.get("daily_bar_max_date"),
                "feature_source_max_date": row.get("feature_source_max_date"),
                "feature_freshness_status": row.get("feature_freshness_status"),
                "provisional_used": _as_bool(row.get("provisional_used")),
            }
        )
    review = pd.DataFrame(out_rows)
    review["_class_order"] = review["candidate_action_class"].map(CLASS_ORDER).fillna(9)
    review = review.sort_values(["_class_order", "review_score", "baseline_rank"], ascending=[True, False, True]).drop(columns=["_class_order"]).reset_index(drop=True)
    review.insert(0, "review_rank", range(1, len(review) + 1))
    review.to_csv(out / "review_candidate_rows.csv", index=False)

    cards = []
    for row in review.head(5).to_dict("records"):
        cards.append(
            {
                "review_rank": row["review_rank"],
                "code": row["code"],
                "candidate_action_class": row["candidate_action_class"],
                "family_context": row["research_candidate_source_family"],
                "pick_source": row["pick_source"],
                "reason": row["classification_reason"].split("|") if row["classification_reason"] else [],
                "why_not_validated_buy": row["not_validated_buy_reason"],
                "manual_check_next": ["entry/liquidity freshness", "overextension risk flags", "chart timing versus pullback/reclaim context"],
            }
        )
    _write_json(out / "review_candidate_cards.json", cards)

    review.groupby(["pick_source", "research_candidate_source_family", "candidate_action_class"], dropna=False).size().reset_index(name="count").to_csv(out / "family_pick_summary.csv", index=False)
    reason_counts = review["classification_reason"].str.get_dummies(sep="|").sum().astype(int).sort_values(ascending=False).reset_index()
    reason_counts.columns = ["classification_reason", "count"]
    reason_counts.to_json(out / "classification_reason_summary.json", orient="records", force_ascii=False, indent=2)

    counts = {name: int((review["candidate_action_class"] == name).sum()) for name in CLASS_ORDER}
    latest_global_date = date_report["candidate_family_source_latest_date"]
    stale_review_pack = bool(latest_global_date is not None and review_date < int(latest_global_date))
    all_uncategorized = bool(not all_rows.empty and all_rows["research_candidate_source_family"].fillna("").eq("uncategorized_source").all())
    feature_stale = bool("feature_freshness_status" in all_rows and not all_rows["feature_freshness_status"].fillna("").eq("fresh").all())
    provisional_review = bool("provisional_used" in all_rows and all_rows["provisional_used"].map(_as_bool).any())
    staleness_days = None
    if stale_review_pack:
        try:
            staleness_days = int((pd.to_datetime(str(latest_global_date)) - pd.to_datetime(str(review_date))).days)
        except Exception:
            staleness_days = None
    warning = {
        "validated_buy_count": counts["validated_buy"],
        "no_validated_buy_today": counts["validated_buy"] == 0,
        "reason": "validated_buy requires keep-gated artifact; current request states validated MeeMee reflection candidate is none",
    }
    _write_json(out / "no_validated_buy_warning.json", warning)
    summary = {
        "axis_id": AXIS_ID,
        "scope": "TRADEX-only",
        "review_date": review_date,
        "candidate_count": int(len(review)),
        "primary_card_count": int(len(cards)),
        "class_counts": counts,
        "stale_review_pack": stale_review_pack,
        "all_uncategorized_latest_rows": all_uncategorized,
        "provisional_review": provisional_review,
        "feature_stale": feature_stale,
        "staleness_days": staleness_days,
        "manual_review_available": bool(counts["starter_review"] + counts["watch"] + counts["wait"] > 0) and not stale_review_pack and not all_uncategorized and not feature_stale,
        "date_report": date_report,
        "source_root": str(family_source_root),
        "latest_baseline_candidate_snapshot": str(all_rows["source_artifact_path"].dropna().iloc[0]) if all_rows["source_artifact_path"].notna().any() else None,
    }
    _write_json(out / "review_pack_summary.json", summary)
    _write_json(
        out / "review_pack_decision.json",
        {
            **counts,
            "validated_buy_count": counts["validated_buy"],
            "starter_review_count": counts["starter_review"],
            "watch_count": counts["watch"],
            "wait_count": counts["wait"],
            "avoid_count": counts["avoid"],
            "no_validated_buy_today": counts["validated_buy"] == 0,
            "manual_review_available": summary["manual_review_available"],
            "stale_review_pack": stale_review_pack,
            "all_uncategorized_latest_rows": all_uncategorized,
            "provisional_review": provisional_review,
            "feature_stale": feature_stale,
            "staleness_days": staleness_days,
            "meemee_reflectable_candidate": False,
            "blocker_reason": (
                "stale review pack; do not use as current candidate"
                if stale_review_pack
                else ("family_assignment_unavailable" if all_uncategorized else ("point_in_time_features_stale" if feature_stale else "manual review candidate pack only; no keep-gated validated challenger and candidate_source_contract_needed remains unresolved"))
            ),
            "latest_global_date_not_used_reason": date_report["latest_global_date_not_used_reason"],
        },
    )
    existing = [name for name in REQUIRED_ARTIFACTS if name != "_ARTIFACT_COMPLETE.json" and (out / name).exists()]
    _write_json(out / "_ARTIFACT_COMPLETE.json", {"axis_id": AXIS_ID, "required_artifacts": list(REQUIRED_ARTIFACTS), "complete": len(existing) == len(REQUIRED_ARTIFACTS) - 1})
    return out


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--family-source-root", type=Path, default=DEFAULT_FAMILY_SOURCE_ROOT)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    args = parser.parse_args()
    print(build_pack(args.family_source_root, args.output_root))


if __name__ == "__main__":
    main()
