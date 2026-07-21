from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd


AXIS_ID = "tradex_sideways_blind_review_sample_v1"
SEED = "sideways-human-blind-review-120-v1"
YEARS = tuple(range(2020, 2026))
GROUP_COUNTS = {"DETECTOR_POSITIVE": 40, "NEAR_BOUNDARY_NEGATIVE": 40, "RANDOM_NEGATIVE": 40}
EXTRA_YEAR_INDEXES = {
    "DETECTOR_POSITIVE": {0, 1, 2, 3},
    "NEAR_BOUNDARY_NEGATIVE": {2, 3, 4, 5},
    "RANDOM_NEGATIVE": {0, 1, 4, 5},
}


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def stable_hash(*parts: Any) -> str:
    return hashlib.sha256("|".join(map(str, parts)).encode("utf-8")).hexdigest()


def load_candidates(db_path: Path) -> pd.DataFrame:
    query = """
    with b0 as (
      select cast(code as varchar) code,date,
             cast(strftime(to_timestamp(date),'%Y%m%d') as integer) ymd,
             h,l,c,
             row_number() over(partition by code order by date) rn,
             lag(c) over(partition by code order by date) prev_c
      from daily_bars where source='pan' and c>0 and h>=l
    ), b1 as (
      select *,abs(c-prev_c) step from b0
    ), obs as (
      select *,
        abs(c-lag(c,14) over w)/nullif(sum(step) over(partition by code order by date rows between 13 preceding and current row),0) direction_efficiency,
        abs(regr_slope(c,rn) over(partition by code order by date rows between 14 preceding and current row))*14/
          nullif(max(c) over(partition by code order by date rows between 14 preceding and current row)-min(c) over(partition by code order by date rows between 14 preceding and current row),0) slope_share,
        (c-min(l) over(partition by code order by date rows between 14 preceding and current row))/
          nullif(max(h) over(partition by code order by date rows between 14 preceding and current row)-min(l) over(partition by code order by date rows between 14 preceding and current row),0) close_pos15,
        c/nullif(lag(c,60) over w,0)-1 ret60
      from b1 window w as(partition by code order by date)
    ), state as (
      select *,coalesce(direction_efficiency<=0.20 and slope_share<=0.35,false) sideways_state
      from obs
    ), marked as (
      select *,sideways_state and not coalesce(lag(sideways_state) over(partition by code order by date),false) sideways_start
      from state
    )
    select code,ymd,direction_efficiency,slope_share,close_pos15,ret60,sideways_state,sideways_start,
           greatest(coalesce(direction_efficiency-0.20,0),coalesce(slope_share-0.35,0),0) boundary_distance
    from marked where ymd between 20200101 and 20251231
      and direction_efficiency is not null and slope_share is not null
    order by code,ymd
    """
    with duckdb.connect(str(db_path), read_only=True) as con:
        frame = con.execute(query).fetchdf()
    frame["code"] = frame.code.astype(str).str.zfill(4)
    frame["year"] = frame.ymd // 10000
    return frame


def year_quotas(total: int, group: str = "DETECTOR_POSITIVE") -> dict[int, int]:
    base, remainder = divmod(total, len(YEARS))
    indexes = EXTRA_YEAR_INDEXES.get(group, set(range(remainder)))
    if len(indexes) != remainder:
        raise ValueError(f"invalid extra-year allocation for {group}: {indexes}")
    return {year: base + (1 if index in indexes else 0) for index, year in enumerate(YEARS)}


def choose_stratified(pool: pd.DataFrame, group: str, count: int, used_codes: set[str]) -> pd.DataFrame:
    quotas = year_quotas(count, group)
    selected: list[pd.DataFrame] = []
    for year, quota in quotas.items():
        candidates = pool[(pool.year == year) & ~pool.code.isin(used_codes)].copy()
        candidates["event_hash"] = [stable_hash(SEED, group, code, ymd) for code, ymd in zip(candidates.code, candidates.ymd)]
        if group == "NEAR_BOUNDARY_NEGATIVE":
            candidates = candidates.sort_values(["boundary_distance", "event_hash", "code", "ymd"]).head(2000)
        candidates = candidates.sort_values(["event_hash", "code", "ymd"]).drop_duplicates("code")
        take = candidates.head(quota).copy()
        if len(take) != quota:
            raise RuntimeError(f"{group} year {year} supply {len(take)}/{quota}")
        used_codes.update(take.code.tolist())
        selected.append(take)
    out = pd.concat(selected, ignore_index=True)
    if len(out) != count:
        raise RuntimeError(f"{group} supply {len(out)}/{count}")
    out["sample_group"] = group
    return out


def build_sample(candidates: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    used_codes: set[str] = set()
    positive = candidates[candidates.sideways_start].copy()
    near = candidates[~candidates.sideways_state & candidates.boundary_distance.between(0, 0.10, inclusive="right")].copy()
    random_negative = candidates[~candidates.sideways_state].copy()
    parts = [
        choose_stratified(positive, "DETECTOR_POSITIVE", 40, used_codes),
        choose_stratified(near, "NEAR_BOUNDARY_NEGATIVE", 40, used_codes),
        choose_stratified(random_negative, "RANDOM_NEGATIVE", 40, used_codes),
    ]
    sealed = pd.concat(parts, ignore_index=True)
    sealed["display_hash"] = [stable_hash(SEED, "DISPLAY", code, ymd) for code, ymd in zip(sealed.code, sealed.ymd)]
    sealed = sealed.sort_values(["display_hash", "code", "ymd"]).reset_index(drop=True)
    sealed.insert(0, "case_id", [f"SW{i:03d}" for i in range(1, len(sealed) + 1)])
    sealed["outcome_joined"] = False
    board = sealed[["case_id", "code", "ymd", "display_hash"]].copy()
    board["chart_cutoff_ymd"] = board.ymd
    board["max_daily_ymd_used"] = board.ymd
    board["outcome_joined"] = False
    return board, sealed


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--sealed-output", type=Path, required=True)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)
    args.sealed_output.mkdir(parents=True, exist_ok=False)
    candidates = load_candidates(args.db)
    board, sealed = build_sample(candidates)
    board_path = args.output / "blind_review_board.parquet"
    sealed_path = args.sealed_output / "machine_labels_sealed.parquet"
    board.to_parquet(board_path, index=False)
    board.to_csv(args.output / "blind_review_board.csv", index=False, encoding="utf-8-sig")
    sealed.to_parquet(sealed_path, index=False)
    counts = sealed.sample_group.value_counts().to_dict()
    gates = {
        "rows_120": len(board) == 120,
        "unique_codes_120": board.code.nunique() == 120,
        "unique_events_120": len(board[["code", "ymd"]].drop_duplicates()) == 120,
        "group_counts_40_each": all(int(counts.get(group, 0)) == count for group, count in GROUP_COUNTS.items()),
        "year_counts_20_each": all(int((sealed.year == year).sum()) == 20 for year in YEARS),
        "outcome_joined_false": not bool(board.outcome_joined.any()),
        "machine_columns_hidden_from_board": not bool({"sample_group", "sideways_state", "direction_efficiency", "slope_share"} & set(board.columns)),
    }
    if not all(gates.values()):
        raise RuntimeError(gates)
    compare = {
        "schema_version": f"{AXIS_ID}.compare.v1", "artifact_role": "authoritative_outcome_blind_sideways_sample",
        "review_only": True, "status": "frozen_pending_human_sideways_review",
        "fixed_conditions": {"years": YEARS, "seed": SEED, "one_case_per_code": True, "selection_uses_future_outcomes": False, "human_labels": ["SIDEWAYS", "NOT_SIDEWAYS", "BORDERLINE"]},
        "sealed_group_counts": {str(k): int(v) for k, v in counts.items()}, "year_counts": {str(year): int((sealed.year == year).sum()) for year in YEARS},
        "gates": gates, "not_changed": ["sideways detector", "MeeMee", "ranking", "runtime DB", "trade rules"],
    }
    compare_path = args.output / "compare.json"
    _write = lambda path, payload: path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    _write(compare_path, compare)
    _write(args.output / "audit.json", {"source_db": str(args.db), "source_db_sha256": sha(args.db), "board_sha256": sha(board_path), "sealed_sha256": sha(sealed_path), "gates": gates})
    _write(args.output / "_ARTIFACT_COMPLETE.json", {"complete": True, "authoritative": "compare.json", "sha256": sha(compare_path)})
    _write(args.sealed_output / "seal_audit.json", {"reviewer_bundle": str(args.output), "sealed_sha256": sha(sealed_path), "outcome_joined": False})
    print(json.dumps({"output": str(args.output), "sealed": str(args.sealed_output), "counts": counts, "gates": gates}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
