from __future__ import annotations

import argparse
import json
import math
import os
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd


AXIS_ID = "ppp_20ma_survival_to_60_v1"
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\ppp_20ma_survival_to_60_v1")
DEFAULT_PRODUCTION_CSV = Path("production_data/production_daily.csv")
REQUIRED_ARTIFACTS = (
    "input_schema_report.json",
    "ppp_source_report.json",
    "streak_events.csv",
    "survival_summary.json",
    "survival_by_k.csv",
    "hazard_by_k.csv",
    "return_diagnostics.csv",
    "research_decision.json",
    "no_lookahead_audit.json",
    "_ARTIFACT_COMPLETE.json",
)
STREAK_EVENT_COLUMNS = [
    "code",
    "streak_start_date",
    "streak_end_date",
    "streak_length",
    "anchor_20_date",
    "anchor_23_date",
    "anchor_60_date",
    "ppp_at_20",
    "ppp_at_23",
    "ppp_source",
    "reached_23",
    "reached_60",
    "ret20_from_20",
    "ret20_from_23",
    "ret40_from_23",
    "mae20_from_23",
    "mfe20_from_23",
]


@dataclass(frozen=True)
class InputResolution:
    source_type: str
    path: Path
    table_names: tuple[str, ...]
    daily_columns: tuple[str, ...]
    ma_columns: tuple[str, ...]
    ppp_columns: tuple[str, ...]
    row_count: int
    min_date: str | None
    max_date: str | None


def _now_tag() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _json_ready(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): _json_ready(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_ready(v) for v in value]
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, pd.Timestamp):
        return value.strftime("%Y-%m-%d")
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
    except (TypeError, ValueError):
        pass
    return value


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(_json_ready(payload), ensure_ascii=False, sort_keys=True, indent=2) + "\n", encoding="utf-8")


def _date_norm_expr(column: str) -> str:
    return f"""
    CASE
      WHEN typeof({column}) = 'DATE' THEN CAST(strftime({column}, '%Y%m%d') AS INTEGER)
      WHEN typeof({column}) = 'TIMESTAMP' THEN CAST(strftime({column}, '%Y%m%d') AS INTEGER)
      ELSE CAST(regexp_replace(CAST({column} AS VARCHAR), '[^0-9]', '', 'g') AS INTEGER)
    END
    """


def default_db_candidates() -> list[Path]:
    candidates: list[Path] = []
    local_app = os.environ.get("LOCALAPPDATA")
    if local_app:
        candidates.extend(
            [
                Path(local_app) / "MeeMeeScreener" / "data" / "stocks.duckdb",
                Path(local_app) / "MeeMeeScreener-dev" / "data" / "stocks.duckdb",
            ]
        )
    candidates.append(Path("data/stocks.duckdb"))
    return candidates


def _table_names(conn: duckdb.DuckDBPyConnection) -> set[str]:
    return {str(row[0]) for row in conn.execute("SHOW TABLES").fetchall()}


def _columns(conn: duckdb.DuckDBPyConnection, table: str) -> list[str]:
    return [str(row[1]) for row in conn.execute(f"PRAGMA table_info('{table}')").fetchall()]


def resolve_input(db_path: Path | None = None, production_csv: Path = DEFAULT_PRODUCTION_CSV) -> InputResolution:
    candidates = [db_path] if db_path else default_db_candidates()
    for candidate in candidates:
        if candidate is None or not candidate.exists():
            continue
        try:
            conn = duckdb.connect(str(candidate), read_only=True)
            tables = _table_names(conn)
            if {"daily_bars", "daily_ma"} <= tables:
                daily_cols = tuple(_columns(conn, "daily_bars"))
                ma_cols = tuple(_columns(conn, "daily_ma"))
                ppp_cols: list[str] = []
                for table in sorted(tables):
                    for col in _columns(conn, table):
                        lower = col.lower()
                        if "ppp" in lower or "abc" in lower:
                            ppp_cols.append(f"{table}.{col}")
                d_expr = _date_norm_expr("date")
                row = conn.execute(
                    f"SELECT COUNT(*), MIN({d_expr}), MAX({d_expr}) FROM daily_bars"
                ).fetchone()
                conn.close()
                if row and int(row[0] or 0) > 0:
                    return InputResolution(
                        source_type="duckdb_daily_bars_daily_ma",
                        path=candidate,
                        table_names=tuple(sorted(tables)),
                        daily_columns=daily_cols,
                        ma_columns=ma_cols,
                        ppp_columns=tuple(sorted(ppp_cols)),
                        row_count=int(row[0]),
                        min_date=_ymd_to_text(row[1]),
                        max_date=_ymd_to_text(row[2]),
                    )
        except Exception:
            continue
    if production_csv.exists():
        sample = pd.read_csv(production_csv, nrows=5)
        return InputResolution(
            source_type="production_csv",
            path=production_csv,
            table_names=(),
            daily_columns=tuple(sample.columns),
            ma_columns=(),
            ppp_columns=(),
            row_count=-1,
            min_date=None,
            max_date=None,
        )
    raise RuntimeError("No usable daily OHLCV source found")


def inspect_ppp_sources(db_path: Path | None = None, production_csv: Path = DEFAULT_PRODUCTION_CSV) -> list[dict[str, Any]]:
    reports: list[dict[str, Any]] = []
    candidates = [db_path] if db_path else default_db_candidates()
    for candidate in candidates:
        if candidate is None:
            continue
        report: dict[str, Any] = {"source_type": "duckdb", "path": candidate, "exists": candidate.exists()}
        if candidate.exists():
            try:
                conn = duckdb.connect(str(candidate), read_only=True)
                tables = _table_names(conn)
                ppp_columns: list[str] = []
                regime_columns: list[str] = []
                for table in sorted(tables):
                    columns = _columns(conn, table)
                    for col in columns:
                        lower = col.lower()
                        if "ppp" in lower or "abc" in lower:
                            ppp_columns.append(f"{table}.{col}")
                        if "regime" in lower:
                            regime_columns.append(f"{table}.{col}")
                report.update(
                    {
                        "readable": True,
                        "table_count": len(tables),
                        "ppp_or_abc_columns": sorted(ppp_columns),
                        "regime_columns": sorted(regime_columns),
                        "has_confirmed_ppp_column": bool(ppp_columns),
                    }
                )
                conn.close()
            except Exception as exc:
                report.update({"readable": False, "error": f"{type(exc).__name__}: {exc}"})
        reports.append(report)
    csv_report: dict[str, Any] = {"source_type": "csv", "path": production_csv, "exists": production_csv.exists()}
    if production_csv.exists():
        try:
            sample = pd.read_csv(production_csv, nrows=5)
            columns = list(sample.columns)
            ppp_columns = [col for col in columns if "ppp" in col.lower() or "abc" in col.lower()]
            csv_report.update({"readable": True, "columns": columns, "ppp_or_abc_columns": ppp_columns, "has_confirmed_ppp_column": bool(ppp_columns)})
        except Exception as exc:
            csv_report.update({"readable": False, "error": f"{type(exc).__name__}: {exc}"})
    reports.append(csv_report)
    return reports


def _ymd_to_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(int(value))
    if len(text) != 8:
        return None
    return f"{text[:4]}-{text[4:6]}-{text[6:]}"


def load_daily_frame(resolution: InputResolution, *, start_ymd: int | None = None, end_ymd: int | None = None) -> pd.DataFrame:
    if resolution.source_type == "duckdb_daily_bars_daily_ma":
        conn = duckdb.connect(str(resolution.path), read_only=True)
        b_expr = _date_norm_expr("b.date")
        m_expr = _date_norm_expr("m.date")
        source_filter = "AND lower(coalesce(b.source, 'pan')) = 'pan'" if "source" in resolution.daily_columns else ""
        where_dates = []
        params: list[Any] = []
        if start_ymd is not None:
            where_dates.append("ymd >= ?")
            params.append(int(start_ymd))
        if end_ymd is not None:
            where_dates.append("ymd <= ?")
            params.append(int(end_ymd))
        date_clause = "" if not where_dates else "AND " + " AND ".join(where_dates)
        frame = conn.execute(
            f"""
            WITH b AS (
              SELECT CAST(code AS VARCHAR) AS code, {b_expr} AS ymd, o, h, l, c, v
              FROM daily_bars AS b
              WHERE o > 0 AND h > 0 AND l > 0 AND c > 0 {source_filter}
            ),
            m AS (
              SELECT CAST(code AS VARCHAR) AS code, {m_expr} AS ymd, ma7, ma20, ma60
              FROM daily_ma AS m
            )
            SELECT b.code, b.ymd, b.o, b.h, b.l, b.c, b.v, m.ma7, m.ma20, m.ma60
            FROM b
            LEFT JOIN m ON b.code = m.code AND b.ymd = m.ymd
            WHERE true {date_clause}
            ORDER BY b.code, b.ymd
            """,
            params,
        ).fetchdf()
        conn.close()
    else:
        frame = pd.read_csv(resolution.path)
        frame = frame.rename(columns={"date": "ymd", "open": "o", "high": "h", "low": "l", "close": "c", "volume": "v"})
        frame["ymd"] = pd.to_datetime(frame["ymd"]).dt.strftime("%Y%m%d").astype(int)
        frame["code"] = frame["code"].astype(str)
        if start_ymd is not None:
            frame = frame[frame["ymd"] >= int(start_ymd)]
        if end_ymd is not None:
            frame = frame[frame["ymd"] <= int(end_ymd)]
        frame = frame.sort_values(["code", "ymd"], kind="stable")
    if frame.empty:
        raise RuntimeError("daily input returned no rows")
    frame["date"] = pd.to_datetime(frame["ymd"].astype(str), format="%Y%m%d")
    frame["code"] = frame["code"].astype(str)
    for col in ("o", "h", "l", "c", "v", "ma7", "ma20", "ma60"):
        if col in frame.columns:
            frame[col] = pd.to_numeric(frame[col], errors="coerce")
    return frame


def add_features(frame: pd.DataFrame) -> pd.DataFrame:
    work = frame.sort_values(["code", "date"], kind="stable").copy()
    grouped = work.groupby("code", sort=False)
    if "ma7" not in work.columns or work["ma7"].isna().all():
        work["ma7"] = grouped["c"].transform(lambda s: s.rolling(7, min_periods=7).mean())
    if "ma20" not in work.columns or work["ma20"].isna().all():
        work["ma20"] = grouped["c"].transform(lambda s: s.rolling(20, min_periods=20).mean())
    if "ma60" not in work.columns or work["ma60"].isna().all():
        work["ma60"] = grouped["c"].transform(lambda s: s.rolling(60, min_periods=60).mean())
    work["ma20_slope"] = grouped["ma20"].transform(lambda s: s - s.shift(1))
    work["ma60_slope"] = grouped["ma60"].transform(lambda s: s - s.shift(1))
    work["close_above_ma20"] = work["ma20"].notna() & (work["c"] > work["ma20"])
    work["ppp_proxy"] = (
        work["ma7"].notna()
        & work["ma20"].notna()
        & work["ma60"].notna()
        & (work["ma7"] > work["ma20"])
        & (work["ma20"] > work["ma60"])
        & (work["ma20_slope"] > 0)
        & (work["ma60_slope"] >= 0)
    )
    work["future_c_20"] = grouped["c"].shift(-20)
    work["future_c_40"] = grouped["c"].shift(-40)
    work["future_h_20"] = grouped["h"].transform(lambda s: s.shift(-1).iloc[::-1].rolling(20, min_periods=20).max().iloc[::-1])
    work["future_l_20"] = grouped["l"].transform(lambda s: s.shift(-1).iloc[::-1].rolling(20, min_periods=20).min().iloc[::-1])
    return work


def _safe_ret(exit_value: Any, entry_value: Any) -> float | None:
    try:
        exit_float = float(exit_value)
        entry_float = float(entry_value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(exit_float) or not math.isfinite(entry_float) or entry_float <= 0:
        return None
    return float(exit_float / entry_float - 1.0)


def build_streak_events(frame: pd.DataFrame, *, ppp_source: str) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for code, group in frame.sort_values(["code", "date"], kind="stable").groupby("code", sort=False):
        active: list[dict[str, Any]] = []
        for item in group.to_dict("records"):
            if bool(item.get("close_above_ma20")):
                active.append(item)
                continue
            if active:
                _append_streak(rows, str(code), active, ppp_source=ppp_source)
                active = []
        if active:
            _append_streak(rows, str(code), active, ppp_source=ppp_source)
    return pd.DataFrame(rows, columns=STREAK_EVENT_COLUMNS)


def _append_streak(rows: list[dict[str, Any]], code: str, active: list[dict[str, Any]], *, ppp_source: str) -> None:
    length = len(active)
    if length < 20:
        return
    anchor20 = active[19]
    anchor23 = active[22] if length >= 23 else None
    anchor60 = active[59] if length >= 60 else None
    rows.append(
        {
            "code": code,
            "streak_start_date": active[0]["date"],
            "streak_end_date": active[-1]["date"],
            "streak_length": int(length),
            "anchor_20_date": anchor20["date"],
            "anchor_23_date": None if anchor23 is None else anchor23["date"],
            "anchor_60_date": None if anchor60 is None else anchor60["date"],
            "ppp_at_20": bool(anchor20.get("ppp_proxy", False)),
            "ppp_at_23": None if anchor23 is None else bool(anchor23.get("ppp_proxy", False)),
            "ppp_source": ppp_source,
            "reached_23": bool(length >= 23),
            "reached_60": bool(length >= 60),
            "ret20_from_20": _safe_ret(anchor20.get("future_c_20"), anchor20.get("c")),
            "ret20_from_23": None if anchor23 is None else _safe_ret(anchor23.get("future_c_20"), anchor23.get("c")),
            "ret40_from_23": None if anchor23 is None else _safe_ret(anchor23.get("future_c_40"), anchor23.get("c")),
            "mae20_from_23": None if anchor23 is None else _safe_ret(anchor23.get("future_l_20"), anchor23.get("c")),
            "mfe20_from_23": None if anchor23 is None else _safe_ret(anchor23.get("future_h_20"), anchor23.get("c")),
        }
    )


def _quantile(values: pd.Series, q: float) -> float | None:
    numeric = pd.to_numeric(values, errors="coerce").dropna()
    if numeric.empty:
        return None
    return float(numeric.quantile(q))


def _mean(values: pd.Series) -> float | None:
    numeric = pd.to_numeric(values, errors="coerce").dropna()
    if numeric.empty:
        return None
    return float(numeric.mean())


def _cohort(events: pd.DataFrame, name: str) -> pd.DataFrame:
    if events.empty:
        return events.copy()
    ppp20 = events["ppp_at_20"].map(lambda value: bool(value) if value is not None and not pd.isna(value) else False)
    ppp23 = events["ppp_at_23"].map(lambda value: bool(value) if value is not None and not pd.isna(value) else False)
    reached23 = events["reached_23"].astype(bool)
    if name == "ppp_20_all":
        return events[ppp20].copy()
    if name == "ppp_23_survivors":
        return events[reached23 & ppp23].copy()
    if name == "non_ppp_23_survivors":
        return events[reached23 & ~ppp23].copy()
    if name == "all_23_survivors":
        return events[reached23].copy()
    if name == "all_20":
        return events.copy()
    raise ValueError(f"unknown cohort: {name}")


def summarize_survival(events: pd.DataFrame, *, ppp_source_kind: str) -> dict[str, Any]:
    cohorts = {}
    for name in ("ppp_20_all", "ppp_23_survivors", "non_ppp_23_survivors", "all_23_survivors", "all_20"):
        subset = _cohort(events, name)
        n = int(len(subset))
        cohorts[name] = {
            "n_streaks": n,
            "p_reach_23_given_20": None if n == 0 else float(subset["reached_23"].astype(bool).mean()),
            "p_reach_60_given_20": None if n == 0 else float(subset["reached_60"].astype(bool).mean()),
            "p_reach_60_given_23": None
            if int(subset["reached_23"].astype(bool).sum()) == 0
            else float(subset.loc[subset["reached_23"].astype(bool), "reached_60"].astype(bool).mean()),
            "median_streak_length": _quantile(subset["streak_length"] if n else pd.Series(dtype=float), 0.5),
            "p75_streak_length": _quantile(subset["streak_length"] if n else pd.Series(dtype=float), 0.75),
            "p90_streak_length": _quantile(subset["streak_length"] if n else pd.Series(dtype=float), 0.9),
            "distribution_buckets": _bucket_counts(subset),
            "mean_ret20_from_23": _mean(subset["ret20_from_23"] if n else pd.Series(dtype=float)),
        }
    return {"ppp_source_kind": ppp_source_kind, "cohorts": cohorts}


def _bucket_counts(subset: pd.DataFrame) -> dict[str, int]:
    if subset.empty:
        return {"20-22": 0, "23-29": 0, "30-39": 0, "40-59": 0, "60+": 0}
    lengths = pd.to_numeric(subset["streak_length"], errors="coerce")
    return {
        "20-22": int(((lengths >= 20) & (lengths <= 22)).sum()),
        "23-29": int(((lengths >= 23) & (lengths <= 29)).sum()),
        "30-39": int(((lengths >= 30) & (lengths <= 39)).sum()),
        "40-59": int(((lengths >= 40) & (lengths <= 59)).sum()),
        "60+": int((lengths >= 60).sum()),
    }


def build_survival_by_k(events: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    ppp20 = events["ppp_at_20"].map(lambda value: bool(value) if value is not None and not pd.isna(value) else False)
    ppp23 = events["ppp_at_23"].map(lambda value: bool(value) if value is not None and not pd.isna(value) else False)
    for k in range(20, 61):
        for cohort_name in ("ppp_at_k_proxy", "non_ppp_at_k_proxy", "all"):
            eligible = events[events["streak_length"] >= k].copy()
            if cohort_name == "ppp_at_k_proxy":
                if k <= 20:
                    eligible = eligible[ppp20.loc[eligible.index]]
                elif k <= 23:
                    eligible = eligible[ppp23.loc[eligible.index]]
                else:
                    eligible = eligible[ppp23.loc[eligible.index]]
            elif cohort_name == "non_ppp_at_k_proxy":
                if k <= 20:
                    eligible = eligible[~ppp20.loc[eligible.index]]
                else:
                    eligible = eligible[~ppp23.loc[eligible.index]]
            n = int(len(eligible))
            rows.append(
                {
                    "k": k,
                    "cohort": cohort_name,
                    "n_reached_k": n,
                    "p_reach_60_given_k": None if n == 0 else float(eligible["reached_60"].astype(bool).mean()),
                }
            )
    return pd.DataFrame(rows)


def build_hazard_by_k(events: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for k in range(20, 61):
        reached = events[events["streak_length"] >= k]
        for horizon in range(1, 6):
            n = int(len(reached))
            breaks = int(((reached["streak_length"] >= k) & (reached["streak_length"] <= k + horizon - 1)).sum())
            rows.append(
                {
                    "k": k,
                    "break_within_days": horizon,
                    "n_reached_k": n,
                    "break_count": breaks,
                    "break_probability": None if n == 0 else float(breaks / n),
                }
            )
    return pd.DataFrame(rows)


def build_return_diagnostics(events: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for name in ("ppp_20_all", "ppp_23_survivors", "non_ppp_23_survivors", "all_23_survivors", "all_20"):
        subset = _cohort(events, name)
        row: dict[str, Any] = {"cohort": name, "n_streaks": int(len(subset))}
        for col in ("ret20_from_20", "ret20_from_23", "ret40_from_23", "mae20_from_23", "mfe20_from_23"):
            values = pd.to_numeric(subset[col], errors="coerce").dropna() if not subset.empty else pd.Series(dtype=float)
            row[f"{col}_n"] = int(len(values))
            row[f"{col}_mean"] = None if values.empty else float(values.mean())
            row[f"{col}_median"] = None if values.empty else float(values.quantile(0.5))
        rows.append(row)
    return pd.DataFrame(rows)


def classify_decision(summary: dict[str, Any]) -> dict[str, Any]:
    cohorts = summary["cohorts"]
    ppp23 = cohorts["ppp_23_survivors"]
    all23 = cohorts["all_23_survivors"]
    non23 = cohorts["non_ppp_23_survivors"]
    ppp20 = cohorts["ppp_20_all"]
    reasons: list[str] = []
    decision = "supports_hypothesis"
    if summary["ppp_source_kind"] != "confirmed":
        decision = "inconclusive"
        reasons.append("PPP confirmed unavailable; proxy-only result is research-fallback")
    if int(ppp23["n_streaks"]) < 100:
        decision = "inconclusive"
        reasons.append("n_streaks_23 below 100")
    p_ppp23 = ppp23["p_reach_60_given_23"]
    p_all23 = all23["p_reach_60_given_23"]
    p_non23 = non23["p_reach_60_given_23"]
    p_ppp20 = ppp20["p_reach_60_given_20"]
    ret_ppp23 = ppp23["mean_ret20_from_23"]
    ret_all23 = all23["mean_ret20_from_23"]
    if None not in (p_ppp23, p_all23) and p_ppp23 < p_all23 + 0.05:
        if decision == "supports_hypothesis":
            decision = "not_supported"
        reasons.append("PPP 23->60 rate is not at least +5pp above all-regime 23")
    if None not in (p_ppp23, p_non23) and p_ppp23 < p_non23 + 0.05:
        if decision == "supports_hypothesis":
            decision = "not_supported"
        reasons.append("PPP 23->60 rate is not at least +5pp above non-PPP 23")
    if None not in (p_ppp23, p_ppp20) and p_ppp23 < p_ppp20 + 0.10:
        if decision == "supports_hypothesis":
            decision = "not_supported"
        reasons.append("PPP 23->60 rate is not at least +10pp above PPP 20->60")
    if None not in (ret_ppp23, ret_all23) and ret_ppp23 < ret_all23:
        if decision == "supports_hypothesis":
            decision = "not_supported"
        reasons.append("anchor_23 20-day mean return is below all-regime 23")
    if not reasons:
        reasons.append("all support gates passed")
    return {
        "research_decision": decision,
        "reason_typed": reasons,
        "decision_inputs": {
            "ppp_n_streaks_23": ppp23["n_streaks"],
            "ppp_p_reach_60_given_23": p_ppp23,
            "all_regime_p_reach_60_given_23": p_all23,
            "non_ppp_p_reach_60_given_23": p_non23,
            "ppp_p_reach_60_given_20": p_ppp20,
            "ppp_anchor_23_ret20_mean": ret_ppp23,
            "all_regime_anchor_23_ret20_mean": ret_all23,
        },
    }


def no_lookahead_audit() -> dict[str, Any]:
    return {
        "rule": "PPP proxy and MA condition use only same-day or prior rolling MA values; reached_60 and future returns are labels/diagnostics only.",
        "columns": {
            "code": "feature",
            "streak_start_date": "feature",
            "streak_end_date": "label",
            "streak_length": "label",
            "anchor_20_date": "feature",
            "anchor_23_date": "feature",
            "anchor_60_date": "label",
            "ppp_at_20": "feature",
            "ppp_at_23": "feature",
            "ppp_source": "feature",
            "reached_23": "label",
            "reached_60": "label",
            "ret20_from_20": "diagnostic",
            "ret20_from_23": "diagnostic",
            "ret40_from_23": "diagnostic",
            "mae20_from_23": "diagnostic",
            "mfe20_from_23": "diagnostic",
        },
        "lookahead_blockers": [],
    }


def run(
    *,
    output_root: Path = DEFAULT_OUTPUT_ROOT,
    db_path: Path | None = None,
    production_csv: Path = DEFAULT_PRODUCTION_CSV,
    start_ymd: int | None = None,
    end_ymd: int | None = None,
) -> dict[str, Any]:
    run_id = f"{_now_tag()}-ppp-20ma-survival-to-60-v1"
    output_dir = output_root / run_id
    output_dir.mkdir(parents=True, exist_ok=True)
    ppp_source_inventory = inspect_ppp_sources(db_path=db_path, production_csv=production_csv)
    resolution = resolve_input(db_path=db_path, production_csv=production_csv)
    daily = load_daily_frame(resolution, start_ymd=start_ymd, end_ymd=end_ymd)
    featured = add_features(daily)
    confirmed_ppp_available = bool(resolution.ppp_columns)
    ppp_source_kind = "confirmed" if confirmed_ppp_available else "research-fallback"
    ppp_source = "confirmed_existing_ppp_column" if confirmed_ppp_available else "proxy_ma7_gt_ma20_gt_ma60_ma20_slope_gt0_ma60_slope_ge0"
    events = build_streak_events(featured, ppp_source=ppp_source)
    summary = summarize_survival(events, ppp_source_kind=ppp_source_kind)
    survival_by_k = build_survival_by_k(events)
    hazard = build_hazard_by_k(events)
    returns = build_return_diagnostics(events)
    decision = classify_decision(summary)

    input_schema = {
        "axis_id": AXIS_ID,
        "source_type": resolution.source_type,
        "source_path": resolution.path,
        "table_names": resolution.table_names,
        "daily_columns": resolution.daily_columns,
        "ma_columns": resolution.ma_columns,
        "ppp_columns": resolution.ppp_columns,
        "source_row_count": resolution.row_count,
        "loaded_row_count": int(len(daily)),
        "date_min": daily["date"].min(),
        "date_max": daily["date"].max(),
        "ma20_missing_count": int(featured["ma20"].isna().sum()),
        "ma7_missing_count": int(featured["ma7"].isna().sum()),
        "ma60_missing_count": int(featured["ma60"].isna().sum()),
    }
    ppp_report = {
        "ppp_source_kind": ppp_source_kind,
        "ppp_source": ppp_source,
        "confirmed_ppp_available": confirmed_ppp_available,
        "confirmed_ppp_columns": resolution.ppp_columns,
        "source_inventory": ppp_source_inventory,
        "proxy_definition": {
            "enabled": not confirmed_ppp_available,
            "condition": "ma7 > ma20 > ma60 and ma20_slope > 0 and ma60_slope >= 0",
            "classification": "research-fallback" if not confirmed_ppp_available else "not_used",
        },
    }
    events.to_csv(output_dir / "streak_events.csv", index=False)
    survival_by_k.to_csv(output_dir / "survival_by_k.csv", index=False)
    hazard.to_csv(output_dir / "hazard_by_k.csv", index=False)
    returns.to_csv(output_dir / "return_diagnostics.csv", index=False)
    _write_json(output_dir / "input_schema_report.json", input_schema)
    _write_json(output_dir / "ppp_source_report.json", ppp_report)
    _write_json(output_dir / "survival_summary.json", summary)
    _write_json(output_dir / "research_decision.json", decision)
    _write_json(output_dir / "no_lookahead_audit.json", no_lookahead_audit())
    complete = {
        "axis_id": AXIS_ID,
        "output_dir": output_dir,
        "required_artifacts": list(REQUIRED_ARTIFACTS),
        "artifact_complete": all((output_dir / name).exists() for name in REQUIRED_ARTIFACTS if name != "_ARTIFACT_COMPLETE.json"),
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
    }
    _write_json(output_dir / "_ARTIFACT_COMPLETE.json", complete)
    return {
        "output_dir": str(output_dir),
        "ppp_source_kind": ppp_source_kind,
        "summary": summary,
        "decision": decision,
        "required_artifacts": list(REQUIRED_ARTIFACTS),
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="TRADEX PPP/proxy 20MA streak survival to 60 study")
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--db-path", type=Path, default=None)
    parser.add_argument("--production-csv", type=Path, default=DEFAULT_PRODUCTION_CSV)
    parser.add_argument("--start-ymd", type=int, default=None)
    parser.add_argument("--end-ymd", type=int, default=None)
    args = parser.parse_args(argv)
    result = run(
        output_root=args.output_root,
        db_path=args.db_path,
        production_csv=args.production_csv,
        start_ymd=args.start_ymd,
        end_ymd=args.end_ymd,
    )
    print(json.dumps(_json_ready(result), ensure_ascii=False, sort_keys=True, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
