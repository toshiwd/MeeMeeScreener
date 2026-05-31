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


AXIS_ID = "starter_candidate_review_pack_v1"
DEFAULT_FAMILY_SOURCE_ROOT = Path(r"G:\Tradex\starter_entry_family_source_split_design_v1\20260525T041110Z-starter-entry-family-source-split-design-v1")
DEFAULT_ROLE_ROOT = Path(r"G:\Tradex\starter_entry_role_backfill_v1\20260525T020451Z-starter-entry-role-backfill-v1")
DEFAULT_TAXONOMY_ROOT = Path(r"G:\Tradex\candidate_family_taxonomy_shadow_v1\20260524T135527Z-candidate-family-taxonomy-shadow-v1")
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\starter_candidate_review_pack_v1")

REQUIRED_ARTIFACTS = (
    "review_pack_summary.json",
    "starter_candidate_review_rows.csv",
    "starter_candidate_cards.json",
    "family_context_summary.csv",
    "risk_flag_summary.csv",
    "selection_reason_summary.json",
    "review_pack_decision.json",
    "_ARTIFACT_COMPLETE.json",
)

OUTPUT_COLUMNS = [
    "review_rank",
    "decision_date",
    "code",
    "candidate_action_class",
    "baseline_rank",
    "baseline_score",
    "review_score",
    "research_candidate_source_family",
    "primary_family",
    "starter_role",
    "selected_loser",
    "starter_good",
    "starter_bad",
    "immediate_adverse_entry",
    "next_open_available",
    "entry_allowed_by_score",
    "risk_flags",
    "setup_tags",
    "regime_tags",
    "classification_reason",
    "review_limit_note",
]


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
    text = str(value).strip().lower()
    return text in {"1", "true", "yes", "y"}


def _parse_json_list(value: Any) -> list[str]:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return []
    if isinstance(value, list):
        return [str(v) for v in value]
    try:
        parsed = json.loads(str(value))
    except Exception:
        return []
    if isinstance(parsed, list):
        return [str(v) for v in parsed]
    return []


def _latest_candidate_rows(path: Path) -> pd.DataFrame:
    cols = [
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
        "research_risk_tags_json",
        "research_setup_tags_json",
        "research_regime_tags_json",
        "source_artifact_path",
        "source_run_id",
    ]
    latest_date: int | None = None
    for chunk in pd.read_csv(path, usecols=["decision_date"], chunksize=500_000, low_memory=False):
        value = int(pd.to_numeric(chunk["decision_date"], errors="coerce").max())
        latest_date = value if latest_date is None else max(latest_date, value)
    if latest_date is None:
        return pd.DataFrame(columns=cols)
    found: list[pd.DataFrame] = []
    for chunk in pd.read_csv(path, usecols=cols, chunksize=500_000, low_memory=False):
        dates = pd.to_numeric(chunk["decision_date"], errors="coerce")
        part = chunk[dates.eq(latest_date)].copy()
        if not part.empty:
            found.append(part)
    if not found:
        return pd.DataFrame(columns=cols)
    rows = pd.concat(found, ignore_index=True)
    rows["baseline_rank"] = pd.to_numeric(rows["baseline_rank"], errors="coerce")
    rows["baseline_score"] = pd.to_numeric(rows["baseline_score"], errors="coerce")
    return rows.sort_values(["baseline_rank", "baseline_score", "code"], ascending=[True, False, True]).reset_index(drop=True)


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


def _classify(row: pd.Series) -> tuple[str, list[str], float]:
    family = str(row.get("research_candidate_source_family") or "")
    risk_tags = _parse_json_list(row.get("research_risk_tags_json"))
    setup_tags = _parse_json_list(row.get("research_setup_tags_json"))
    risk_level = _risk_level(risk_tags, row)
    has_good_context = family in {
        "pullback_reclaim_source",
        "early_trend_source",
        "mature_trend_continuation_source",
        "range_reversal_source",
    } or bool({"pullback_candidate", "early_trend_candidate", "mature_trend_candidate", "trend_continuation_candidate", "range_candidate"}.intersection(setup_tags))
    overextension = family == "overextension_risk_source" or "overextension_candidate" in setup_tags
    liquid = _as_bool(row.get("next_open_available")) and _as_bool(row.get("entry_allowed_by_score"))

    reasons: list[str] = []
    score = float(row.get("baseline_score") or 0)
    if has_good_context:
        score += 3
        reasons.append("good_family_or_setup_context")
    if family == "pullback_reclaim_source":
        score += 2
        reasons.append("pullback_reclaim_context")
    if overextension:
        score -= 3
        reasons.append("overextension_context")
    if risk_level in {"severe", "high"}:
        score -= 5
        reasons.append(f"{risk_level}_risk_flags")
    elif risk_level == "low":
        score += 1
        reasons.append("low_risk_flags")
    if not liquid:
        score -= 5
        reasons.append("liquidity_or_entry_coverage_insufficient")

    if not liquid or risk_level == "severe":
        return "avoid", reasons, score
    if overextension and risk_level != "low":
        return "wait", reasons, score
    if has_good_context and risk_level == "low" and not overextension:
        return "starter", reasons, score
    if has_good_context:
        return "watch", reasons, score
    if overextension:
        return "wait", reasons, score
    return "watch", reasons or ["baseline_top_candidate_but_context_unclear"], score


def _family_context_summary(rows: pd.DataFrame) -> pd.DataFrame:
    rec: list[dict[str, Any]] = []
    for family, g in rows.groupby("research_candidate_source_family", dropna=False):
        rec.append(
            {
                "research_candidate_source_family": family,
                "candidate_count": len(g),
                "starter_count": int((g["candidate_action_class"] == "starter").sum()),
                "watch_count": int((g["candidate_action_class"] == "watch").sum()),
                "wait_count": int((g["candidate_action_class"] == "wait").sum()),
                "avoid_count": int((g["candidate_action_class"] == "avoid").sum()),
                "avg_review_score": float(g["review_score"].mean()) if not g.empty else None,
            }
        )
    return pd.DataFrame(rec)


def _risk_flag_summary(rows: pd.DataFrame) -> pd.DataFrame:
    rec: dict[str, dict[str, Any]] = {}
    for _, row in rows.iterrows():
        tags = _parse_json_list(row.get("research_risk_tags_json")) or ["no_risk_tag_available"]
        for tag in tags:
            info = rec.setdefault(tag, {"risk_flag": tag, "candidate_count": 0, "classes": {}})
            info["candidate_count"] += 1
            cls = str(row["candidate_action_class"])
            info["classes"][cls] = info["classes"].get(cls, 0) + 1
    return pd.DataFrame([{**v, "class_counts_json": json.dumps(v.pop("classes"), sort_keys=True)} for v in rec.values()])


def build_pack(family_source_root: Path, role_root: Path, taxonomy_root: Path, output_root: Path) -> Path:
    out = output_root / f"{_now_tag()}-starter-candidate-review-pack-v1"
    out.mkdir(parents=True, exist_ok=True)
    source_path = family_source_root / "candidate_family_source_rows.csv"
    rows = _latest_candidate_rows(source_path)
    if rows.empty:
        raise RuntimeError(f"no latest candidate rows found in {source_path}")

    records: list[dict[str, Any]] = []
    for _, row in rows.iterrows():
        action, reasons, score = _classify(row)
        risk_tags = _parse_json_list(row.get("research_risk_tags_json"))
        setup_tags = _parse_json_list(row.get("research_setup_tags_json"))
        regime_tags = _parse_json_list(row.get("research_regime_tags_json"))
        records.append(
            {
                "decision_date": int(row["decision_date"]),
                "code": str(row["code"]).removesuffix(".0"),
                "candidate_action_class": action,
                "baseline_rank": int(row["baseline_rank"]) if pd.notna(row["baseline_rank"]) else None,
                "baseline_score": float(row["baseline_score"]) if pd.notna(row["baseline_score"]) else None,
                "review_score": score,
                "research_candidate_source_family": row.get("research_candidate_source_family"),
                "primary_family": row.get("primary_family"),
                "starter_role": row.get("diagnostic_candidate_role"),
                "selected_loser": _as_bool(row.get("selected_loser")),
                "starter_good": _as_bool(row.get("starter_good")),
                "starter_bad": _as_bool(row.get("starter_bad")),
                "immediate_adverse_entry": _as_bool(row.get("immediate_adverse_entry")),
                "next_open_available": _as_bool(row.get("next_open_available")),
                "entry_allowed_by_score": _as_bool(row.get("entry_allowed_by_score")),
                "risk_flags": "|".join(risk_tags),
                "setup_tags": "|".join(setup_tags),
                "regime_tags": "|".join(regime_tags),
                "classification_reason": "|".join(reasons),
                "review_limit_note": "manual_review_pack_not_validated_production_ranking",
                "_risk_json": row.get("research_risk_tags_json"),
            }
        )
    classified = pd.DataFrame(records)
    class_order = {"starter": 0, "watch": 1, "wait": 2, "avoid": 3}
    classified["_class_order"] = classified["candidate_action_class"].map(class_order).fillna(9)
    top = classified.sort_values(["_class_order", "review_score", "baseline_rank"], ascending=[True, False, True]).head(5).copy()
    top["review_rank"] = range(1, len(top) + 1)
    top[OUTPUT_COLUMNS].to_csv(out / "starter_candidate_review_rows.csv", index=False)

    cards = []
    for row in top[OUTPUT_COLUMNS].to_dict("records"):
        cards.append(
            {
                "review_rank": row["review_rank"],
                "code": row["code"],
                "candidate_action_class": row["candidate_action_class"],
                "reason": row["classification_reason"].split("|") if row["classification_reason"] else [],
                "family_context": row["research_candidate_source_family"],
                "risk_flags": row["risk_flags"].split("|") if row["risk_flags"] else [],
                "baseline_rank": row["baseline_rank"],
                "baseline_score": row["baseline_score"],
                "manual_review_note": row["review_limit_note"],
            }
        )
    _write_json(out / "starter_candidate_cards.json", cards)
    _family_context_summary(top).to_csv(out / "family_context_summary.csv", index=False)
    risk_input = top.rename(columns={"risk_flags": "research_risk_tags_json"}).copy()
    risk_input["research_risk_tags_json"] = risk_input["research_risk_tags_json"].map(lambda s: json.dumps(str(s).split("|")) if s else "[]")
    _risk_flag_summary(risk_input).to_csv(out / "risk_flag_summary.csv", index=False)

    counts = {k: int((top["candidate_action_class"] == k).sum()) for k in ["starter", "watch", "wait", "avoid"]}
    summary = {
        "axis_id": AXIS_ID,
        "scope": "TRADEX-only",
        "decision_date": int(rows["decision_date"].iloc[0]),
        "candidate_count": int(len(top)),
        "class_counts": counts,
        "no_starter_today": counts["starter"] == 0,
        "source_latest_candidate_rows": str(source_path),
        "latest_baseline_candidate_snapshot": str(rows["source_artifact_path"].dropna().iloc[0]) if rows["source_artifact_path"].notna().any() else None,
        "input_roots": {
            "family_source_root": str(family_source_root),
            "role_root": str(role_root),
            "taxonomy_root": str(taxonomy_root),
        },
    }
    _write_json(out / "review_pack_summary.json", summary)
    _write_json(out / "selection_reason_summary.json", {"selection_policy": "top 5 after manual review class priority, review score, baseline rank", "reason_counts": top["classification_reason"].str.get_dummies(sep="|").sum().astype(int).to_dict()})
    _write_json(
        out / "review_pack_decision.json",
        {
            "candidate_local_decision": "manual_review_pack_created",
            "session_aggregate_decision": "hold_for_manual_selection",
            "authoritative_rollup_decision": "not_validated_challenger",
            "meemee_reflectable": False,
            "blocker_reason": "manual review pack only; no keep-gated validated challenger and candidate_source_contract_needed remains unresolved",
            "no_starter_today": counts["starter"] == 0,
        },
    )
    existing_artifacts = [name for name in REQUIRED_ARTIFACTS if name != "_ARTIFACT_COMPLETE.json" and (out / name).exists()]
    _write_json(
        out / "_ARTIFACT_COMPLETE.json",
        {
            "axis_id": AXIS_ID,
            "required_artifacts": list(REQUIRED_ARTIFACTS),
            "complete": len(existing_artifacts) == len(REQUIRED_ARTIFACTS) - 1,
        },
    )
    return out


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--family-source-root", type=Path, default=DEFAULT_FAMILY_SOURCE_ROOT)
    parser.add_argument("--role-root", type=Path, default=DEFAULT_ROLE_ROOT)
    parser.add_argument("--taxonomy-root", type=Path, default=DEFAULT_TAXONOMY_ROOT)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    args = parser.parse_args()
    print(build_pack(args.family_source_root, args.role_root, args.taxonomy_root, args.output_root))


if __name__ == "__main__":
    main()
