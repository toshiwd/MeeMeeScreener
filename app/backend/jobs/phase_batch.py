import argparse
import json
import logging
import math
from datetime import datetime, timezone

import pandas as pd

from app.backend.core.legacy_analysis_control import is_legacy_analysis_disabled
from app.backend.db import get_conn


LABEL_VERSION = 1
PRED_VERSION = 2
SHRINK_ALPHA = 20.0

logger = logging.getLogger(__name__)


def _is_missing(value: object) -> bool:
    if value is None:
        return True
    if isinstance(value, float) and math.isnan(value):
        return True
    return False


def _parse_dt(value: str | int | None) -> int | None:
    if value is None:
        return None
    if isinstance(value, int):
        raw = str(value)
    else:
        raw = str(value).strip()
    if not raw:
        return None
    if raw.isdigit() and len(raw) == 8:
        parsed = datetime.strptime(raw, "%Y%m%d").replace(tzinfo=timezone.utc)
        return int(parsed.timestamp())
    for fmt in ("%Y-%m-%d", "%Y/%m/%d"):
        try:
            parsed = datetime.strptime(raw, fmt).replace(tzinfo=timezone.utc)
            return int(parsed.timestamp())
        except ValueError:
            continue
    if raw.isdigit():
        value_int = int(raw)
        if value_int > 1_000_000_000_000:
            return int(value_int / 1000)
        return value_int
    return None


def _normalize_dt_arg(value: int | None) -> int | None:
    if value is None:
        return None
    return _parse_dt(value)


def _load_feature_snapshot(conn, start_dt: int | None, end_dt: int | None) -> pd.DataFrame:
    sql = (
        "SELECT dt, code, close, ma7, ma20, ma60, diff20_pct, cnt_20_above, cnt_7_above "
        "FROM feature_snapshot_daily"
    )
    params: list[object] = []
    where: list[str] = []
    if start_dt is not None:
        where.append("dt >= ?")
        params.append(start_dt)
    if end_dt is not None:
        where.append("dt <= ?")
        params.append(end_dt)
    if where:
        sql = f"{sql} WHERE " + " AND ".join(where)
    return conn.execute(sql, params).df()


def _build_label_records(
    feature_df: pd.DataFrame,
    start_dt: int,
    end_dt: int,
) -> list[tuple]:
    records: list[tuple] = []
    if feature_df.empty:
        return records

    computed_at = datetime.now(tz=timezone.utc).replace(tzinfo=None)
    for code, group in feature_df.groupby("code"):
        group = group.sort_values("dt").reset_index(drop=True)
        closes = group["close"].tolist()
        ma20 = group["ma20"].tolist()
        ma7 = group["ma7"].tolist()
        dts = group["dt"].tolist()
        max_index = len(group) - 20
        if max_index <= 0:
            continue
        for i in range(max_index):
            j = i + 20
            window_ma20 = ma20[i + 1 : j + 1]
            window_ma7 = ma7[i + 1 : j + 1]
            if any(_is_missing(value) for value in window_ma20):
                continue
            if any(_is_missing(value) for value in window_ma7):
                continue
            if _is_missing(closes[j]) or _is_missing(ma20[j]):
                continue
            cont_label = 1 if closes[j] > ma20[j] else 0
            ex_label = 0
            for k in range(i + 1, j + 1):
                if closes[k] < ma20[k] and closes[k - 1] < ma20[k - 1]:
                    ex_label = 1
                    break
                if closes[k] < ma7[k] and closes[k - 1] < ma7[k - 1]:
                    ex_label = 1
                    break
            if dts[i] < start_dt or dts[i] > end_dt:
                continue
            records.append(
                (
                    dts[i],
                    code,
                    cont_label,
                    ex_label,
                    20,
                    LABEL_VERSION,
                    computed_at,
                )
            )
    return records


def _bucket_diff20(value: float | None) -> str | None:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return None
    if value < -0.04:
        return "lt_-4"
    if value < -0.02:
        return "-4_-2"
    if value < 0:
        return "-2_0"
    if value < 0.02:
        return "0_2"
    if value < 0.04:
        return "2_4"
    return "gt_4"


def _bucket_cnt20(value: float | int | None) -> str | None:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return None
    value_int = int(value)
    if value_int <= 0:
        return "0"
    if value_int <= 3:
        return "1_3"
    if value_int <= 8:
        return "4_8"
    if value_int <= 15:
        return "9_15"
    return "16_plus"


def _bucket_cnt7(value: float | int | None) -> str | None:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return None
    value_int = int(value)
    if value_int <= 0:
        return "0"
    if value_int <= 3:
        return "1_3"
    if value_int <= 7:
        return "4_7"
    return "8_plus"


def _build_stats(label_df: pd.DataFrame, feature_df: pd.DataFrame) -> dict[tuple, tuple[int, int, int]]:
    if label_df.empty:
        return {}
    snap_df = feature_df[["dt", "code", "diff20_pct", "cnt_20_above", "cnt_7_above"]]
    market_df = feature_df[feature_df["code"] == "1001"][
        ["dt", "diff20_pct", "cnt_20_above"]
    ].rename(
        columns={
            "diff20_pct": "mkt_diff20_pct",
            "cnt_20_above": "mkt_cnt_20_above",
        }
    )
    merged = label_df.merge(snap_df, on=["dt", "code"], how="left")
    merged = merged.merge(market_df, on="dt", how="left")
    stats: dict[tuple, tuple[int, int, int]] = {}
    for row in merged.itertuples(index=False):
        key = (
            _bucket_diff20(row.diff20_pct),
            _bucket_cnt20(row.cnt_20_above),
            _bucket_cnt7(row.cnt_7_above),
            _bucket_diff20(row.mkt_diff20_pct),
            _bucket_cnt20(row.mkt_cnt_20_above),
        )
        if any(part is None for part in key):
            continue
        n, cont_pos, ex_pos = stats.get(key, (0, 0, 0))
        stats[key] = (
            n + 1,
            cont_pos + int(row.cont_label or 0),
            ex_pos + int(row.ex_label or 0),
        )
    return stats


def _shape_early(cnt_20_above: float | None, diff20_pct: float | None) -> float:
    score = 1.0
    if cnt_20_above is not None and not math.isnan(cnt_20_above):
        if cnt_20_above <= 3:
            score *= 1.0
        elif cnt_20_above <= 8:
            score *= 0.7
        elif cnt_20_above <= 15:
            score *= 0.4
        else:
            score *= 0.2
    if diff20_pct is not None and not math.isnan(diff20_pct):
        if diff20_pct >= 0.02:
            score *= 0.7
        elif diff20_pct <= -0.02:
            score *= 0.8
    return min(1.0, max(0.0, score))


def _shape_late(cnt_20_above: float | None, diff20_pct: float | None) -> float:
    score = 1.0
    if cnt_20_above is not None and not math.isnan(cnt_20_above):
        if cnt_20_above >= 16:
            score *= 1.0
        elif cnt_20_above >= 9:
            score *= 0.8
        elif cnt_20_above >= 4:
            score *= 0.5
        else:
            score *= 0.3
    if diff20_pct is not None and not math.isnan(diff20_pct):
        if diff20_pct >= 0.04:
            score *= 1.0
        elif diff20_pct >= 0.02:
            score *= 0.8
        elif diff20_pct >= 0:
            score *= 0.6
        else:
            score *= 0.4
    return min(1.0, max(0.0, score))


def _build_reasons(
    cnt_20_above: float | None,
    diff20_pct: float | None,
    mkt_diff20_pct: float | None,
    n: int,
) -> list[str]:
    reasons: list[str] = []
    if n == 0:
        reasons.append("サンプル不足（n=0）")
    if cnt_20_above is not None and not math.isnan(cnt_20_above):
        if cnt_20_above <= 3:
            reasons.append("cnt_20_above が短い→初動寄り")
        elif cnt_20_above >= 9:
            reasons.append("cnt_20_above が長い→終盤寄り")
    if diff20_pct is not None and not math.isnan(diff20_pct):
        if diff20_pct >= 0.04:
            reasons.append("diff20_pct が大きい→終盤寄り")
        elif diff20_pct <= 0:
            reasons.append("diff20_pct が小さい→初動寄り")
    if mkt_diff20_pct is not None and not math.isnan(mkt_diff20_pct):
        if mkt_diff20_pct < 0:
            reasons.append("日経平均 diff20_pct が弱い→継続弱め")
    if not reasons:
        reasons.append("条件一致が弱い")
    return reasons[:3]


def _build_phase_records(
    feature_df: pd.DataFrame,
    label_df: pd.DataFrame,
    start_dt: int,
    end_dt: int,
) -> list[tuple]:
    records: list[tuple] = []
    if feature_df.empty:
        return records

    # Position-based scoring: percentile ranks within each stock's own history.
    base = feature_df[["dt", "code", "diff20_pct", "cnt_20_above"]].copy()
    base["diff20_rank"] = base.groupby("code")["diff20_pct"].rank(pct=True)
    base["cnt20_rank"] = base.groupby("code")["cnt_20_above"].rank(pct=True)
    base["hist_n"] = base.groupby("code")["diff20_pct"].transform(lambda s: int(s.notna().sum()))

    target = base[
        (base["dt"] >= start_dt) & (base["dt"] <= end_dt) & (base["code"] != "1001")
    ]
    computed_at = datetime.now(tz=timezone.utc).replace(tzinfo=None)

    def _finite(value: object) -> bool:
        return isinstance(value, (int, float)) and math.isfinite(value)

    for row in target.itertuples(index=False):
        p_diff = row.diff20_rank if _finite(row.diff20_rank) else 0.5
        p_cnt = row.cnt20_rank if _finite(row.cnt20_rank) else 0.5
        stage = (float(p_diff) + float(p_cnt)) / 2.0

        early_score = max(0.0, (0.6 - stage) / 0.6)
        late_score = max(0.0, (stage - 0.4) / 0.6)
        body_score = 1.0 - min(1.0, abs(stage - 0.5) * 2.0)

        early_score = min(1.0, max(0.0, float(early_score)))
        late_score = min(1.0, max(0.0, float(late_score)))
        body_score = min(1.0, max(0.0, float(body_score)))

        reasons: list[str] = []
        if not _finite(row.diff20_rank) or not _finite(row.cnt20_rank):
            reasons.append("過去データ不足のため参考値")

        if p_cnt <= 0.2:
            reasons.append("20日線の上での連続日数が少なめ")
        elif p_cnt >= 0.8:
            reasons.append("20日線の上での連続日数が多め")

        if p_diff <= 0.2:
            reasons.append("20日線からの乖離が小さめ")
        elif p_diff >= 0.8:
            reasons.append("20日線からの乖離が大きめ")

        if not reasons:
            reasons.append("材料は中立")

        records.append(
            (
                row.dt,
                row.code,
                early_score,
                late_score,
                body_score,
                int(row.hist_n) if row.hist_n is not None else 0,
                json.dumps(reasons[:3], ensure_ascii=False),
                PRED_VERSION,
                computed_at,
            )
        )
    return records


def run_batch(
    start_dt: int,
    end_dt: int,
    dry_run: bool = False,
) -> None:
    if is_legacy_analysis_disabled():
        logger.info("Skipping phase batch because legacy analysis is disabled.")
        return
    with get_conn() as conn:
        normalized_start_dt = _normalize_dt_arg(start_dt)
        normalized_end_dt = _normalize_dt_arg(end_dt)
        if normalized_start_dt is None or normalized_end_dt is None:
            raise ValueError("start_dt/end_dt is invalid")
        start_dt = int(normalized_start_dt)
        end_dt = int(normalized_end_dt)
        feature_df = _load_feature_snapshot(conn, None, None)
        if feature_df.empty:
            logger.info("feature_snapshot_daily is empty. Nothing to do.")
            return

        label_count = conn.execute("SELECT COUNT(*) FROM label_20d").fetchone()[0]
        label_df: pd.DataFrame | None = None
        if label_count == 0:
            min_dt = int(feature_df["dt"].min())
            max_dt = int(feature_df["dt"].max())
            label_records = _build_label_records(feature_df, min_dt, max_dt)
            logger.info(
                "label_20d backfill range=%s..%s records=%s",
                min_dt,
                max_dt,
                len(label_records),
            )
            if label_records and not dry_run:
                conn.executemany(
                    """
                    INSERT OR REPLACE INTO label_20d (
                        dt,
                        code,
                        cont_label,
                        ex_label,
                        n_forward,
                        label_version,
                        computed_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    label_records,
                )
            if dry_run:
                if label_records:
                    label_df = (
                        pd.DataFrame(
                            label_records,
                            columns=[
                                "dt",
                                "code",
                                "cont_label",
                                "ex_label",
                                "n_forward",
                                "label_version",
                                "computed_at",
                            ],
                        )
                        .loc[:, ["dt", "code", "cont_label", "ex_label"]]
                    )
                else:
                    label_df = pd.DataFrame(
                        columns=["dt", "code", "cont_label", "ex_label"]
                    )
        else:
            label_records = _build_label_records(feature_df, start_dt, end_dt)
            logger.info("label_20d records=%s", len(label_records))
            if label_records and not dry_run:
                conn.executemany(
                    """
                    INSERT OR REPLACE INTO label_20d (
                        dt,
                        code,
                        cont_label,
                        ex_label,
                        n_forward,
                        label_version,
                        computed_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    label_records,
                )

        if label_df is None:
            label_df = conn.execute(
                "SELECT dt, code, cont_label, ex_label FROM label_20d"
            ).df()
        phase_records = _build_phase_records(feature_df, label_df, start_dt, end_dt)
        logger.info("phase_pred_daily records=%s", len(phase_records))
        if phase_records and not dry_run:
            conn.executemany(
                """
                INSERT OR REPLACE INTO phase_pred_daily (
                    dt,
                    code,
                    early_score,
                    late_score,
                    body_score,
                    n,
                    reasons_top3,
                    pred_version,
                    computed_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                phase_records,
            )


def _resolve_range(conn, date_arg: str | int | None, start_arg: str | int | None, end_arg: str | int | None) -> tuple[int, int]:
    if date_arg is not None:
        target = _parse_dt(date_arg)
        if target is None:
            raise ValueError("date is invalid")
        return target, target
    start_dt = _parse_dt(start_arg)
    end_dt = _parse_dt(end_arg)
    if start_dt is None or end_dt is None:
        row = conn.execute("SELECT MAX(dt) FROM feature_snapshot_daily").fetchone()
        if not row or row[0] is None:
            raise ValueError("feature_snapshot_daily is empty")
        max_dt = int(row[0])
        return max_dt, max_dt
    if start_dt > end_dt:
        start_dt, end_dt = end_dt, start_dt
    return start_dt, end_dt


def main() -> None:
    parser = argparse.ArgumentParser(description="Build 20d labels and phase predictions.")
    parser.add_argument("--date", help="Target date (YYYYMMDD or unix seconds)")
    parser.add_argument("--start-dt", help="Start date (YYYYMMDD or unix seconds)")
    parser.add_argument("--end-dt", help="End date (YYYYMMDD or unix seconds)")
    parser.add_argument("--dry-run", action="store_true", help="Compute without DB writes")
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args()

    logging.basicConfig(level=args.log_level.upper())
    with get_conn() as conn:
        start_dt, end_dt = _resolve_range(conn, args.date, args.start_dt, args.end_dt)
    logger.info("phase batch start range=%s..%s dry_run=%s", start_dt, end_dt, args.dry_run)
    run_batch(start_dt, end_dt, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
