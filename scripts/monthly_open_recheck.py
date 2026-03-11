from __future__ import annotations

import argparse
import csv
import json
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
import sys
from typing import Any

import duckdb

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.core.config import config


PRIMARY_STRICT = {"p_up_min": 0.47, "ev20_net_min": -0.005, "turnover20_min": 12_000_000}
STRICT = {"p_up_min": 0.46, "ev20_net_min": -0.005, "turnover20_min": 5_000_000}
STRICT_PLUS = {"p_up_min": 0.45, "ev20_net_min": -0.005, "turnover20_min": 5_000_000}
RELAXED = {"p_up_min": 0.44, "ev20_net_min": -0.010, "turnover20_min": 3_000_000}


@dataclass
class Candidate:
    code: str
    name: str | None
    sector: str | None
    tier: str
    source_bucket: str
    source_rank: int
    monthly_score_up: float | None
    base_composite: float | None
    base_priority_rank: int | None


def _as_code(v: Any) -> str:
    s = str(v).strip()
    if s.isdigit() and len(s) < 4:
        return s.zfill(4)
    return s


def _to_float(v: Any) -> float | None:
    if v is None:
        return None
    try:
        return float(v)
    except Exception:
        return None


def _load_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _resolve_phase(plan: dict, phase: int | None, asof: date) -> dict:
    phases = plan.get("phase_execution", [])
    if not phases:
        raise ValueError("phase_execution is empty")
    if phase is not None:
        for item in phases:
            if int(item.get("phase", -1)) == phase:
                return item
        raise ValueError(f"phase={phase} not found")
    future = sorted(phases, key=lambda x: date.fromisoformat(str(x["date"])))
    for item in future:
        if date.fromisoformat(str(item["date"])) >= asof:
            return item
    return future[-1]


def _build_candidates(phase_block: dict, base_plan: dict) -> tuple[list[Candidate], int]:
    base_by_code = {_as_code(x.get("code")): x for x in base_plan.get("execution_list", [])}
    primaries: list[Candidate] = []
    for p in phase_block.get("primaries", []):
        code = _as_code(p.get("code"))
        base = base_by_code.get(code, {})
        primaries.append(
            Candidate(
                code=code,
                name=p.get("name"),
                sector=p.get("sector"),
                tier="primary",
                source_bucket="primary",
                source_rank=int(p.get("priority_rank", 9999)),
                monthly_score_up=_to_float((base.get("monthly") or {}).get("score_up")),
                base_composite=_to_float(base.get("composite")),
                base_priority_rank=int(p.get("priority_rank", 9999)),
            )
        )

    backups: list[Candidate] = []
    ladder = phase_block.get("backup_ladder", {})
    for bucket in ("strict_plus_first", "relaxed_reserve"):
        for b in ladder.get(bucket, []):
            backups.append(
                Candidate(
                    code=_as_code(b.get("code")),
                    name=b.get("name"),
                    sector=b.get("sector"),
                    tier=str(b.get("tier") or "relaxed"),
                    source_bucket=bucket,
                    source_rank=int(b.get("rank", 9999)),
                    monthly_score_up=_to_float((b.get("monthly") or {}).get("score_up")),
                    base_composite=_to_float(b.get("composite")),
                    base_priority_rank=None,
                )
            )
    return primaries + backups, len(primaries)


def _fetch_live_metrics(conn: duckdb.DuckDBPyConnection, codes: list[str]) -> tuple[int, dict[str, dict]]:
    if not codes:
        raise ValueError("codes is empty")
    latest_daily = int(conn.execute("SELECT max(dt) FROM ml_pred_20d").fetchone()[0])
    placeholders = ",".join(["?"] * len(codes))
    pred_rows = conn.execute(
        f"""
        SELECT CAST(code AS VARCHAR) AS code, CAST(p_up AS DOUBLE), CAST(ev20_net AS DOUBLE), CAST(rank_up_20 AS DOUBLE)
        FROM ml_pred_20d
        WHERE dt = ? AND CAST(code AS VARCHAR) IN ({placeholders})
        """,
        [latest_daily, *codes],
    ).fetchall()
    feat_rows = conn.execute(
        f"""
        SELECT CAST(code AS VARCHAR) AS code, CAST(turnover20 AS DOUBLE)
        FROM ml_feature_daily
        WHERE dt = ? AND CAST(code AS VARCHAR) IN ({placeholders})
        """,
        [latest_daily, *codes],
    ).fetchall()
    metrics: dict[str, dict] = {}
    for code, p_up, ev20_net, rank_up_20 in pred_rows:
        metrics[_as_code(code)] = {
            "p_up": _to_float(p_up),
            "ev20_net": _to_float(ev20_net),
            "rank_up_20": _to_float(rank_up_20),
        }
    for code, turnover20 in feat_rows:
        metrics.setdefault(_as_code(code), {})["turnover20"] = _to_float(turnover20)
    return latest_daily, metrics


def _fetch_events(
    conn: duckdb.DuckDBPyConnection,
    codes: list[str],
    phase_date: date,
) -> dict[str, list[dict]]:
    if not codes:
        return {}
    start = phase_date - timedelta(days=3)
    end = phase_date + timedelta(days=2)
    placeholders = ",".join(["?"] * len(codes))
    rows = conn.execute(
        f"""
        SELECT CAST(code AS VARCHAR) AS code, CAST(planned_date AS VARCHAR) AS event_date, 'earnings_planned' AS event_type
        FROM earnings_planned
        WHERE planned_date BETWEEN ? AND ? AND CAST(code AS VARCHAR) IN ({placeholders})
        UNION ALL
        SELECT CAST(code AS VARCHAR) AS code, CAST(ex_date AS VARCHAR) AS event_date, 'ex_rights' AS event_type
        FROM ex_rights
        WHERE ex_date BETWEEN ? AND ? AND CAST(code AS VARCHAR) IN ({placeholders})
        """,
        [start.isoformat(), end.isoformat(), *codes, start.isoformat(), end.isoformat(), *codes],
    ).fetchall()
    out: dict[str, list[dict]] = {}
    for code, d, t in rows:
        out.setdefault(_as_code(code), []).append({"type": t, "date": str(d)[:10]})
    return out


def _threshold_for(tier: str) -> dict[str, float]:
    if tier == "primary":
        return PRIMARY_STRICT
    if tier == "strict":
        return STRICT
    if tier == "strict_plus":
        return STRICT_PLUS
    return RELAXED


def _rotate_by_phase(items: list[dict], phase: int, extra_shift: int = 0) -> list[dict]:
    if not items:
        return items
    offset = ((max(phase, 1) - 1) + max(extra_shift, 0)) % len(items)
    if offset == 0:
        return items
    return items[offset:] + items[:offset]


def _live_score(
    monthly_score_up: float | None,
    p_up: float | None,
    ev20_net: float | None,
    turnover20: float | None,
) -> float:
    m = monthly_score_up if monthly_score_up is not None else 0.5
    p = p_up if p_up is not None else 0.0
    ev = ev20_net if ev20_net is not None else -0.03
    liq = turnover20 if turnover20 is not None else 0.0
    ev_capped = max(min(ev, 0.03), -0.03)
    ev_norm = (ev_capped + 0.03) / 0.06
    liq_norm = min(max(liq, 0.0) / 20_000_000.0, 1.0)
    return float(0.55 * m + 0.25 * p + 0.1 * ev_norm + 0.1 * liq_norm)


def _evaluate_candidate(candidate: Candidate, metrics: dict, events: list[dict]) -> dict:
    p_up = _to_float(metrics.get("p_up"))
    ev20_net = _to_float(metrics.get("ev20_net"))
    rank_up_20 = _to_float(metrics.get("rank_up_20"))
    turnover20 = _to_float(metrics.get("turnover20"))
    th = _threshold_for(candidate.tier)
    reasons = []
    if p_up is None:
        reasons.append("missing_p_up")
    elif p_up < th["p_up_min"]:
        reasons.append(f"p_up_below_{th['p_up_min']}")
    if ev20_net is None:
        reasons.append("missing_ev20_net")
    elif ev20_net < th["ev20_net_min"]:
        reasons.append(f"ev20_net_below_{th['ev20_net_min']}")
    if turnover20 is None:
        reasons.append("missing_turnover20")
    elif turnover20 < th["turnover20_min"]:
        reasons.append(f"turnover20_below_{int(th['turnover20_min'])}")
    if events:
        reasons.append("event_blocked")
    eligible = len(reasons) == 0
    return {
        "code": candidate.code,
        "name": candidate.name,
        "sector": candidate.sector,
        "tier": candidate.tier,
        "source_bucket": candidate.source_bucket,
        "source_rank": candidate.source_rank,
        "base_priority_rank": candidate.base_priority_rank,
        "base_composite": candidate.base_composite,
        "monthly_score_up": candidate.monthly_score_up,
        "live": {
            "p_up": p_up,
            "ev20_net": ev20_net,
            "rank_up_20": rank_up_20,
            "turnover20": turnover20,
            "latest_daily_dt": None,
        },
        "events": events,
        "thresholds": th,
        "eligible": eligible,
        "ineligible_reasons": reasons,
        "live_score": _live_score(candidate.monthly_score_up, p_up, ev20_net, turnover20),
    }


def _compose_recommendation(evaluated: list[dict], primary_count: int, phase: int) -> dict:
    primaries = sorted(
        [x for x in evaluated if x["source_bucket"] == "primary"],
        key=lambda x: int(x.get("base_priority_rank") or 9999),
    )
    strict_plus = sorted(
        [x for x in evaluated if x["source_bucket"] == "strict_plus_first"],
        key=lambda x: int(x.get("source_rank") or 9999),
    )
    relaxed = sorted(
        [x for x in evaluated if x["source_bucket"] == "relaxed_reserve"],
        key=lambda x: int(x.get("source_rank") or 9999),
    )
    strict_plus_eligible = _rotate_by_phase([x for x in strict_plus if x["eligible"]], phase, extra_shift=0)
    relaxed_eligible = _rotate_by_phase([x for x in relaxed if x["eligible"]], phase, extra_shift=1)

    used_codes: set[str] = set()
    slots = []
    replacements = 0
    for slot_idx in range(primary_count):
        primary = primaries[slot_idx] if slot_idx < len(primaries) else None
        if primary is None:
            continue
        if primary["eligible"]:
            used_codes.add(primary["code"])
            slots.append(
                {
                    "slot": slot_idx + 1,
                    "action": "keep_primary",
                    "selected": primary,
                    "replaced_primary": None,
                }
            )
            continue
        replacement = None
        for pool in (strict_plus_eligible, relaxed_eligible):
            for cand in pool:
                if cand["code"] in used_codes:
                    continue
                replacement = cand
                break
            if replacement is not None:
                break
        if replacement is not None:
            used_codes.add(replacement["code"])
            replacements += 1
            slots.append(
                {
                    "slot": slot_idx + 1,
                    "action": "replace_primary",
                    "selected": replacement,
                    "replaced_primary": primary,
                }
            )
        else:
            slots.append(
                {
                    "slot": slot_idx + 1,
                    "action": "vacant_no_replacement",
                    "selected": None,
                    "replaced_primary": primary,
                }
            )
    return {
        "slots": slots,
        "summary": {
            "primary_slots": primary_count,
            "kept_primary": sum(1 for x in slots if x["action"] == "keep_primary"),
            "replaced_primary": replacements,
            "vacant_slots": sum(1 for x in slots if x["action"] == "vacant_no_replacement"),
            "strict_plus_eligible": len(strict_plus_eligible),
            "relaxed_eligible": len(relaxed_eligible),
            "rotation_offset_strict_plus": (max(phase, 1) - 1),
            "rotation_offset_relaxed": max(phase, 1),
        },
    }


def run(args: argparse.Namespace) -> dict:
    plan = _load_json(Path(args.plan_file))
    base = _load_json(Path(args.base_plan_file))
    asof = date.fromisoformat(args.asof_date) if args.asof_date else date.today()
    phase_block = _resolve_phase(plan, args.phase, asof)
    phase_no = int(phase_block["phase"])
    phase_date = date.fromisoformat(str(phase_block["date"]))
    candidates, primary_count = _build_candidates(phase_block, base)
    candidate_codes = sorted({_as_code(x.code) for x in candidates})

    with duckdb.connect(str(config.DB_PATH), read_only=True) as conn:
        latest_daily_dt, metrics = _fetch_live_metrics(conn, candidate_codes)
        events = _fetch_events(conn, candidate_codes, phase_date)

    evaluated = []
    for c in candidates:
        row = _evaluate_candidate(c, metrics.get(c.code, {}), events.get(c.code, []))
        row["live"]["latest_daily_dt"] = latest_daily_dt
        evaluated.append(row)

    recommendation = _compose_recommendation(evaluated, primary_count, phase_no)
    return {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "asof_date": asof.isoformat(),
        "phase": phase_no,
        "phase_date": phase_date.isoformat(),
        "latest_daily_dt": latest_daily_dt,
        "source_plan_file": str(args.plan_file),
        "source_base_plan_file": str(args.base_plan_file),
        "thresholds": {"primary_strict": PRIMARY_STRICT, "strict": STRICT, "strict_plus": STRICT_PLUS, "relaxed": RELAXED},
        "candidate_count": len(candidates),
        "evaluated_candidates": evaluated,
        "recommendation": recommendation,
    }


def _build_selected_rows(result: dict) -> list[dict]:
    rows = []
    slots = result.get("recommendation", {}).get("slots", [])
    for slot in slots:
        selected = slot.get("selected")
        replaced = slot.get("replaced_primary")
        live = (selected or {}).get("live") or {}
        replaced_reasons = (replaced or {}).get("ineligible_reasons") or []
        rows.append(
            {
                "asof_date": result.get("asof_date"),
                "phase": result.get("phase"),
                "phase_date": result.get("phase_date"),
                "slot": slot.get("slot"),
                "action": slot.get("action"),
                "selected_code": (selected or {}).get("code"),
                "selected_tier": (selected or {}).get("tier"),
                "selected_bucket": (selected or {}).get("source_bucket"),
                "selected_name": (selected or {}).get("name"),
                "selected_sector": (selected or {}).get("sector"),
                "selected_live_score": (selected or {}).get("live_score"),
                "selected_p_up": live.get("p_up"),
                "selected_ev20_net": live.get("ev20_net"),
                "selected_turnover20": live.get("turnover20"),
                "replaced_primary_code": (replaced or {}).get("code"),
                "replaced_primary_reasons": "|".join(str(x) for x in replaced_reasons),
            }
        )
    return rows


def _write_selected_csv(result: dict, path: Path) -> None:
    rows = _build_selected_rows(result)
    fieldnames = [
        "asof_date",
        "phase",
        "phase_date",
        "slot",
        "action",
        "selected_code",
        "selected_tier",
        "selected_bucket",
        "selected_name",
        "selected_sector",
        "selected_live_score",
        "selected_p_up",
        "selected_ev20_net",
        "selected_turnover20",
        "replaced_primary_code",
        "replaced_primary_reasons",
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Pre-open phase recheck. Replace failed primaries from strict_plus then relaxed ladder."
    )
    parser.add_argument(
        "--plan-file",
        default="tmp/monthly_execution_plan_202603_strictplus_backups.json",
        help="Phase plan JSON with backup ladder",
    )
    parser.add_argument(
        "--base-plan-file",
        default="tmp/monthly_execution_plan_202603.json",
        help="Base execution JSON including primary composites",
    )
    parser.add_argument("--phase", type=int, default=None, help="Target phase. If omitted, choose first phase >= asof-date")
    parser.add_argument("--asof-date", default=None, help="YYYY-MM-DD. Default: today")
    parser.add_argument("--output", default=None, help="Output JSON path")
    parser.add_argument("--output-csv", default=None, help="Output CSV path for selected slots")
    args = parser.parse_args()

    result = run(args)
    default_json = Path(
        f"tmp/monthly_open_check_{result['phase_date'].replace('-', '')}_phase{result['phase']}.json"
    )
    out_json = Path(args.output) if args.output else default_json
    out_json.parent.mkdir(parents=True, exist_ok=True)
    out_json.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")

    default_csv = Path(
        f"tmp/monthly_open_selected_{result['phase_date'].replace('-', '')}_phase{result['phase']}.csv"
    )
    out_csv = Path(args.output_csv) if args.output_csv else default_csv
    _write_selected_csv(result, out_csv)

    summary = result["recommendation"]["summary"]
    print(
        json.dumps(
            {
                "ok": True,
                "phase": result["phase"],
                "phase_date": result["phase_date"],
                "latest_daily_dt": result["latest_daily_dt"],
                "candidate_count": result["candidate_count"],
                "summary": summary,
                "output_json": str(out_json),
                "output_csv": str(out_csv),
            },
            ensure_ascii=False,
        )
    )


if __name__ == "__main__":
    main()
