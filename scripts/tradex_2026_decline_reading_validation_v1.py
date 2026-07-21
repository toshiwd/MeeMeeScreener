from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb

if __package__ in {None, ""}:
    sys.path.append(str(Path(__file__).resolve().parents[1]))

from shared.runtime_stock_db_contract import inspect_runtime_stock_db, resolve_runtime_stock_db_path
from app.backend.services.market_watch_tags import NIKKEI_225_CODES


AXIS_ID = "tradex_2026_decline_reading_validation_v1"
DEFAULT_OUTPUT_ROOT = Path(r"G:\Tradex\tradex_2026_decline_reading_validation_v1")
PROPOSITION = (
    "決算・適時開示・権利落ち等のチャート外イレギュラーを除けば、継続下落の前には、"
    "価格位置・支持抵抗・ローソク足・移動平均・出来高・戻り品質のいずれかに観測可能な兆候が現れる"
)


def _write_json(path: Path, value: Any) -> None:
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2, default=str) + "\n", encoding="utf-8")


def _rate(rows: list[dict[str, Any]], key: str) -> float | None:
    values = [bool(row[key]) for row in rows if row.get(key) is not None]
    return sum(values) / len(values) if values else None


def _mean(rows: list[dict[str, Any]], key: str) -> float | None:
    values = [float(row[key]) for row in rows if row.get(key) is not None]
    return sum(values) / len(values) if values else None


def _dedupe_events(rows: list[dict[str, Any]], cooldown_bars: int = 10) -> list[dict[str, Any]]:
    """Keep the first event, then suppress overlapping forward windows per code."""
    kept: list[dict[str, Any]] = []
    positions: dict[str, int] = {}
    last_kept: dict[str, int] = {}
    for row in sorted(rows, key=lambda item: (str(item["code"]), int(item["ymd"]))):
        code = str(row["code"])
        pos = positions.get(code, -1) + 1
        positions[code] = pos
        if code not in last_kept or pos - last_kept[code] > cooldown_bars:
            kept.append(row)
            last_kept[code] = pos
    return kept


def _outcome_metrics(rows: list[dict[str, Any]]) -> dict[str, Any]:
    completed = [row for row in rows if row.get("ret10_forward") is not None]
    return {
        "n": len(completed),
        "codes": len({str(row["code"]) for row in completed}),
        "down_close_10_rate": _rate(completed, "down_close_10"),
        "down_low_5pct_10_rate": _rate(completed, "down_low_5pct_10"),
        "mean_ret10": _mean(completed, "ret10_forward"),
        "rebound_high_5pct_10_rate": _rate(completed, "rebound_high_5pct_10"),
    }


def _train_only_interaction_challenger(rows: list[dict[str, Any]]) -> dict[str, Any]:
    import pandas as pd
    from sklearn.impute import SimpleImputer
    from sklearn.tree import DecisionTreeClassifier, export_text

    completed = [row for row in rows if row.get("ret10_forward") is not None and not row.get("irregular_event")]
    features = ["body_ratio","upper_wick_ratio","lower_wick_ratio","close_pos","ret5","dist_ma7","dist_ma20","dist_ma60","pos60","breakdown_score","above_ma60_streak","support_touch_count_30","market_breadth_ma20","market_breadth_ma60","market_advancers_ratio","market_mean_ret1"]
    frame = pd.DataFrame([{**{key: row.get(key) for key in features}, "ymd":int(row["ymd"]), "row":row} for row in completed])
    train = frame[frame.ymd.between(20240101, 20241231)].copy()
    if len(train) < 500:
        return {"status":"insufficient_train_rows","train_n":len(train)}
    imputer = SimpleImputer(strategy="median")
    x_train = imputer.fit_transform(train[features])
    y_train = [bool(row["down_close_10"] and row["down_low_5pct_10"] and not row["rebound_high_5pct_10"]) for row in train.row]
    model = DecisionTreeClassifier(max_depth=4, min_samples_leaf=100, random_state=20260714, class_weight="balanced")
    model.fit(x_train, y_train)
    train["leaf"] = model.apply(x_train)
    leaf_metrics: dict[int, dict[str, Any]] = {}
    selected: list[int] = []
    for leaf, part in train.groupby("leaf"):
        metric = _outcome_metrics(part.row.tolist())
        leaf_metrics[int(leaf)] = metric
        if metric["n"] >= 100 and (metric["down_low_5pct_10_rate"] or 0) >= .55 and (metric["down_close_10_rate"] or 0) >= .60 and (metric["mean_ret10"] or 1) <= -.015 and (metric["rebound_high_5pct_10_rate"] or 1) <= .20:
            selected.append(int(leaf))
    all_x = imputer.transform(frame[features])
    frame["leaf"] = model.apply(all_x)
    chosen = frame[frame.leaf.isin(selected)]
    return {
        "axis":"train_2024_shallow_chart_interaction_tree",
        "feature_names":features,
        "fixed_model":{"max_depth":4,"min_samples_leaf":100,"class_weight":"balanced","random_state":20260714},
        "target":"down close10 AND low10<=-5pct AND rebound high10<+5pct",
        "selected_train_leaf_ids":selected,
        "tree":export_text(model, feature_names=features),
        "train_leaf_metrics":{str(key):value for key,value in leaf_metrics.items()},
        "metrics":_outcome_metrics(chosen.row.tolist()),
        "metrics_by_year":{str(year):_outcome_metrics(chosen[chosen.ymd.astype(str).str.startswith(str(year))].row.tolist()) for year in (2024,2025,2026)},
        "decision":"keep_review_only" if selected and all((_outcome_metrics(chosen[chosen.ymd.astype(str).str.startswith(str(year))].row.tolist()).get("down_close_10_rate") or 0)>=.60 for year in (2024,2025,2026)) else "drop",
    }


def _load_irregular_events(conn: duckdb.DuckDBPyConnection) -> tuple[set[tuple[str, int]], dict[str, Any]]:
    """Load auditable event dates. Missing/empty sources are reported, never silently treated as complete."""
    events: set[tuple[str, int]] = set()
    status: dict[str, Any] = {}
    specs = {
        "earnings_planned": ("code", "planned_date"),
        "ex_rights": ("code", "COALESCE(last_rights_date, ex_date)"),
        "tdnet_disclosures": ("sec_code", "CAST(published_at AS DATE)"),
    }
    tables = {row[0] for row in conn.execute("SHOW TABLES").fetchall()}
    for table, (code_col, date_expr) in specs.items():
        if table not in tables:
            status[table] = {"available": False, "reason": "table_missing", "row_count": 0}
            continue
        rows = conn.execute(
            f"SELECT CAST({code_col} AS VARCHAR), CAST(strftime({date_expr}, '%Y%m%d') AS INTEGER) "
            f"FROM {table} WHERE {code_col} IS NOT NULL AND {date_expr} IS NOT NULL"
        ).fetchall()
        events.update((str(code), int(ymd)) for code, ymd in rows)
        status[table] = {"available": bool(rows), "reason": None if rows else "table_empty", "row_count": len(rows)}
    return events, status


def annotate_path_signals(rows: list[dict[str, Any]], irregular_events: set[tuple[str, int]]) -> None:
    """Point-in-time path annotation; only forward outcome columns use future bars."""
    histories: dict[str, list[dict[str, Any]]] = {}
    above60_streaks: dict[str, int] = {}
    broken_supports: dict[str, dict[str, Any]] = {}
    initial_break_states: dict[str, dict[str, Any]] = {}
    flow_scores: dict[str, float] = {}
    flat_runs: dict[str, int] = {}
    sideways20_runs: dict[str, int] = {}
    for row in sorted(rows, key=lambda item: (item["code"], item["ymd"])):
        history = histories.setdefault(row["code"], [])
        body = float(row["body_ratio"] or 0)
        upper = float(row["upper_wick_ratio"] or 0)
        above60 = bool(row["ma60"] is not None and row["c"] > row["ma60"])
        streak = above60_streaks.get(row["code"], 0) + 1 if above60 else 0
        above60_streaks[row["code"]] = streak
        row["above_ma60_streak"] = streak
        mature = bool(streak >= 60)
        high_failure = bool(upper >= .25 or (row["c"] < row["o"] and body >= .45))
        initial_break = bool(row["cross_ma7"] or row["cross_ma20"] or row["break_low20"])
        weak_rebound = bool(row["failed_rebound_ma7"] or (row["h"] >= row["ma7"] and row["c"] < row["ma7"]))
        gap_down = bool(row.get("pclose") and float(row["o"]) / float(row["pclose"]) - 1 <= -.02)
        row["gap_down_2pct"] = gap_down
        prior30 = history[-30:]
        support_level = None
        support_touches = 0
        support_break = False
        if len(prior30) >= 20:
            lows = sorted(float(item["l"]) for item in prior30)
            support_level = lows[max(0, int(len(lows) * .15) - 1)]
            support_touches = sum(abs(float(item["l"]) / support_level - 1) <= .015 for item in prior30)
            support_break = bool(support_touches >= 3 and history[-1]["c"] >= support_level * .99 and row["c"] < support_level * .99)
            if support_break:
                broken_supports[row["code"]] = {"level": support_level, "age": 0, "break_ymd": row["ymd"]}
        broken = broken_supports.get(row["code"])
        support_to_resistance = False
        if broken is not None:
            broken["age"] += 1
            level = float(broken["level"])
            support_to_resistance = bool(
                1 <= broken["age"] <= 7 and row["h"] >= level * .985 and row["c"] < level
                and (row["c"] < row["o"] or upper >= .25)
            )
            if support_to_resistance or broken["age"] > 7 or row["c"] > level * 1.015:
                broken_supports.pop(row["code"], None)
        row["support_level_30"] = support_level
        row["support_touch_count_30"] = support_touches
        row["support_break"] = support_break
        row["support_to_resistance"] = support_to_resistance
        bearish_full_retrace = False
        retraced_bull_ymd = None
        for j in range(max(0, len(history) - 5), len(history)):
            bull = history[j]
            bull_range = max(float(bull["h"]) - float(bull["l"]), 1e-9)
            bull_body = float(bull["c"]) - float(bull["o"])
            followers = history[j + 1 :] + [row]
            if bull_body <= 0 or bull_body / bull_range < .45 or len(followers) < 2:
                continue
            bearish_count = sum(float(item["c"]) < float(item["o"]) for item in followers)
            if bearish_count >= 2 and float(row["c"]) <= float(bull["o"]) + bull_body * .20:
                bearish_full_retrace = True
                retraced_bull_ymd = int(bull["ymd"])
                break
        row["bearish_full_retrace"] = bearish_full_retrace
        row["retraced_bull_ymd"] = retraced_bull_ymd
        state = initial_break_states.get(row["code"])
        current_month = row["_month_key"]
        current_week = row["_week_key"]
        prior_month_keys = sorted({item["_month_key"] for item in history if item["_month_key"] < current_month}) if state is not None else []
        prior_week_keys = sorted({item["_week_key"] for item in history if item["_week_key"] < current_week}) if state is not None else []
        monthly_failure = False
        weekly_failure = False
        if state is not None and prior_month_keys:
            pm = prior_month_keys[-1]
            prev = [item for item in history if item["_month_key"] == pm]
            curr = [item for item in history if item["_month_key"] == current_month] + [row]
            monthly_failure = bool(max(float(item["h"]) for item in curr) >= max(float(item["h"]) for item in prev) * .99 and float(row["c"]) < float(prev[-1]["c"]))
        if state is not None and prior_week_keys:
            pw = prior_week_keys[-1]
            prev = [item for item in history if item["_week_key"] == pw]
            curr = [item for item in history if item["_week_key"] == current_week] + [row]
            weekly_failure = bool(max(float(item["h"]) for item in curr) < max(float(item["h"]) for item in prev) * 1.01 and float(row["c"]) < float(prev[-1]["c"]))
        row["monthly_high_failure"] = monthly_failure
        row["weekly_lower_high_failure"] = weekly_failure
        row["higher_timeframe_failure"] = monthly_failure or weekly_failure
        weak_rebound_second_break = False
        if state is not None:
            state["age"] += 1
            state["max_close"] = max(float(state["max_close"]), float(row["c"]))
            strong_bull = bool(row["c"] > row["o"] and body >= .60 and row["c"] > row["phigh"])
            state["strong_bull_count"] += int(strong_bull)
            state["supply_bar_count"] += int(row["c"] < row["o"] or upper >= .25)
            weak_rebound_second_break = bool(
                2 <= state["age"] <= 5
                and (row["cross_ma20"] or float(row["c"]) < float(state["initial_close"]) * .995)
                and float(state["max_close"]) <= float(state["prebreak_close"]) * 1.01
                and int(state["strong_bull_count"]) == 0
                and int(state["supply_bar_count"]) >= 2
                and float(row["c"]) < float(row["ma7"])
                and float(row["c"]) < float(row["ma20"])
                and (
                    bool(row.get("provisional_close_only"))
                    or (float(row["c"]) < float(row["o"]) and body >= .45 and float(row["close_pos"]) <= .30)
                )
            )
            if weak_rebound_second_break or state["age"] > 5 or float(row["c"]) > float(state["prebreak_close"]) * 1.01:
                initial_break_states.pop(row["code"], None)
        if state is None and (gap_down or row["cross_ma20"]) and (mature or any(item.get("above_ma60_streak", 0) >= 60 for item in history[-10:])):
            initial_break_states[row["code"]] = {
                "age": 0, "initial_ymd": row["ymd"], "initial_close": row["c"],
                "prebreak_close": row["pclose"], "max_close": row["c"],
                "strong_bull_count": 0, "supply_bar_count": int(row["c"] < row["o"] or upper >= .25),
            }
        row["weak_rebound_second_break"] = weak_rebound_second_break
        row["path_components"] = {
            "mature_high_zone": mature, "high_failure_or_supply": high_failure,
            "initial_ma_or_support_break": initial_break or support_break, "weak_rebound": weak_rebound,
            "support_to_resistance": support_to_resistance,
            "bearish_full_retrace": bearish_full_retrace,
            "weak_rebound_second_break": weak_rebound_second_break,
        }
        omen_reasons: list[str] = []
        if row["c"] < row["ma7"] and row["ma7"] < row["ma7_prev"]: omen_reasons.append("ma7_loss_and_falling")
        if row["c"] < row["ma20"] and row["ma20"] < row["ma20_prev"]: omen_reasons.append("ma20_loss_and_falling")
        if high_failure: omen_reasons.append("bearish_body_or_upper_wick_supply")
        if row.get("ret5") is not None and row["ret5"] < 0: omen_reasons.append("negative_5bar_momentum")
        if support_break: omen_reasons.append("support_zone_break")
        if support_to_resistance: omen_reasons.append("support_to_resistance")
        if bearish_full_retrace: omen_reasons.append("bull_body_full_retraced")
        if gap_down: omen_reasons.append("gap_down_2pct")
        if weak_rebound: omen_reasons.append("ma7_rebound_failure")
        row["omen_reasons"] = omen_reasons
        row["observable_omen_today"] = bool(omen_reasons)
        prior = history[-10:]
        prior20 = history[-20:]
        row["observable_omen_prior10"] = any(bool(item["observable_omen_today"]) for item in prior)
        continuation_confirm2 = False
        if len(history) >= 2:
            anchor = history[-2]
            continuation_confirm2 = bool(
                float(anchor["breakdown_score"]) >= 5
                and (anchor["cross_ma7"] or anchor["cross_ma20"] or anchor["failed_rebound_ma7"] or anchor["break_low20"])
                and float(row["c"]) < float(anchor["c"])
                and float(row["c"]) < float(row["ma7"])
                and float(row["l"]) < min(float(anchor["l"]), float(history[-1]["l"]))
                and float(row["c"]) < float(row["o"])
            )
        row["continuation_confirm2"] = continuation_confirm2
        row["sequence_second_break"] = weak_rebound_second_break and row["higher_timeframe_failure"]
        additions: dict[str, float] = {}
        deductions: dict[str, float] = {}
        if gap_down: additions["gap_down_2pct"] = 1.5
        if high_failure: additions["supply_candle"] = 1.0
        if bearish_full_retrace: additions["bull_body_full_retraced"] = 1.5
        if support_break: additions["support_break"] = 1.5
        if support_to_resistance: additions["support_to_resistance"] = 2.0
        if row["cross_ma7"]: additions["ma7_break"] = 1.0
        if row["cross_ma20"]: additions["ma20_break"] = 2.0
        if weak_rebound: additions["weak_rebound"] = 1.5
        if weak_rebound_second_break: additions["second_break"] = 2.5
        if row["higher_timeframe_failure"]: additions["weekly_or_monthly_failure"] = 1.0
        strong_bull = bool(not row.get("provisional_close_only") and row["c"] > row["o"] and body >= .55 and row["close_pos"] >= .70)
        ma7_reclaim = bool(row.get("pclose") is not None and row.get("ma7_prev") is not None and row["pclose"] < row["ma7_prev"] and row["c"] >= row["ma7"])
        ma20_reclaim = bool(row.get("pclose") is not None and row.get("ma20_prev") is not None and row["pclose"] < row["ma20_prev"] and row["c"] >= row["ma20"])
        lower_wick_reversal = bool(not row.get("provisional_close_only") and float(row.get("lower_wick_ratio") or 0) >= .35 and float(row.get("close_pos") or 0) >= .60)
        if strong_bull: deductions["strong_bull_candle"] = 2.5
        elif not row.get("provisional_close_only") and row["c"] > row["o"]:
            deductions["ordinary_bull_candle"] = 0.5 + min(1.5, body * 1.5)
        if ma7_reclaim: deductions["ma7_reclaim"] = 1.5
        if ma20_reclaim: deductions["ma20_reclaim"] = 2.5
        if lower_wick_reversal: deductions["lower_wick_reversal"] = 1.5
        if row.get("phigh") is not None and row["c"] > row["phigh"]: deductions["prior_high_close_break"] = 1.5
        previous_score = flow_scores.get(row["code"], 0.0)
        score_before_clamp = previous_score * .75 + sum(additions.values()) - sum(deductions.values())
        flow_score = max(-10.0, min(10.0, score_before_clamp))
        flow_scores[row["code"]] = flow_score
        row["sell_flow_previous"] = previous_score
        row["sell_flow_additions"] = additions
        row["sell_flow_deductions"] = deductions
        row["sell_flow_delta"] = sum(additions.values()) - sum(deductions.values())
        row["sell_flow_score"] = flow_score
        row["sell_flow_state"] = "additional_sell" if flow_score >= 6 else "initial_sell" if flow_score >= 4 else "breakdown_watch" if flow_score >= 2 else "neutral" if flow_score > 0 else "bullish_reset"
        rebound: dict[str, float] = {}
        atr = float(row.get("atr14") or 0)
        recent5 = history[-4:] + [row]
        recent10 = history[-9:] + [row]
        drawdown_high5 = float(row["c"])/max(float(item["h"]) for item in recent5)-1 if recent5 else 0.0
        drawdown_high10 = float(row["c"])/max(float(item["h"]) for item in recent10)-1 if recent10 else 0.0
        row["drawdown_from_high5"] = drawdown_high5
        row["drawdown_from_high10"] = drawdown_high10
        if row.get("ret3") is not None and row["ret3"] <= -.05: rebound["three_bar_oversold"] = 2.0
        if row.get("ret5") is not None and row["ret5"] <= -.08: rebound["five_bar_oversold"] = 2.0
        if row.get("ret10") is not None and row["ret10"] <= -.12: rebound["ten_bar_oversold"] = 1.0
        if drawdown_high5 <= -.07: rebound["five_bar_high_drawdown"] = 2.0
        if drawdown_high10 <= -.10: rebound["ten_bar_high_drawdown"] = 2.0
        if atr > 0 and float(row["ma7"])-float(row["c"]) >= atr*1.5: rebound["below_ma7_by_1_5atr"] = 1.0
        if atr > 0 and float(row["ma20"])-float(row["c"]) >= atr*2.0: rebound["below_ma20_by_2atr"] = 1.0
        rising_support_contacts = []
        for label in ("ma20","ma60","ma100","ma200"):
            value, previous = row.get(label), row.get(f"{label}_prev")
            if atr > 0 and value is not None and previous is not None and float(value) >= float(previous) and float(row["c"]) >= float(value) and (float(row["c"])-float(value))/atr <= .5:
                rising_support_contacts.append(label)
        if rising_support_contacts: rebound["rising_ma_support_contact"] = 2.0 + .5*(len(rising_support_contacts)-1)
        bearish_streak = 0
        for item in reversed(history + [row]):
            if float(item["c"]) < float(item["o"]): bearish_streak += 1
            else: break
        if bearish_streak >= 3: rebound["three_or_more_bearish_bars"] = 1.5
        if not row.get("provisional_close_only") and float(row.get("lower_wick_ratio") or 0) >= .35: rebound["long_lower_wick"] = 1.5
        volume_ratio = float(row["v"])/float(row["vol20"]) if row.get("vol20") else 0
        if volume_ratio >= 2 and float(row.get("close_pos") or 0) <= .30 and (row.get("ret3") or 0) < 0: rebound["volume_climax"] = 1.5
        rebound_score = min(10.0, sum(rebound.values()))
        net_score = flow_score - rebound_score
        if rebound_score >= 4:
            action_state = "oversold_no_chase"
        elif rebound_score >= 2 and flow_score >= 6:
            action_state = "take_profit_rebound_watch"
        elif net_score >= 6:
            action_state = "additional_sell"
        elif net_score >= 4:
            action_state = "initial_sell"
        elif net_score >= 2:
            action_state = "breakdown_watch"
        else:
            action_state = "neutral_or_rebound"
        row["rebound_risk_components"] = rebound
        row["rebound_risk_score"] = rebound_score
        row["net_sell_score"] = net_score
        row["trade_action_state"] = action_state
        window20 = history[-19:] + [row]
        range20_pct = (max(float(item["h"]) for item in window20)-min(float(item["l"]) for item in window20))/float(row["c"]) if len(window20)>=20 else None
        sideways20 = bool(range20_pct is not None and range20_pct <= .10)
        sideways20_run = sideways20_runs.get(row["code"],0)+1 if sideways20 else 0
        sideways20_runs[row["code"]] = sideways20_run
        flat_bar = bool(atr>0 and row.get("pclose") is not None and abs(float(row["c"])-float(row["pclose"]))/atr <= .50 and (float(row["h"])-float(row["l"]))/atr <= 1.20)
        flat_run = flat_runs.get(row["code"],0)+1 if flat_bar else 0
        flat_runs[row["code"]] = flat_run
        recent_vol5 = [float(item["v"]) for item in (history[-4:]+[row])]
        volume_compression = sum(recent_vol5)/len(recent_vol5)/float(row["vol20"]) if recent_vol5 and row.get("vol20") else None
        row["range20_pct"] = range20_pct
        row["sideways20_run_length"] = sideways20_run
        row["flat_bar"] = flat_bar
        row["flat_run_length"] = flat_run
        row["volume_compression_5_20"] = volume_compression
        row["sideways_position"] = "unknown" if row.get("pos60") is None else "high" if float(row["pos60"])>=.65 else "low" if float(row["pos60"])<=.35 else "middle"
        row["sideways_ma20_side"] = "above" if float(row["c"])>=float(row["ma20"]) else "below"
        row["pretrend_10"] = "up" if (row.get("ret10") or 0)>=.03 else "down" if (row.get("ret10") or 0)<=-.03 else "flat"
        # Calendar-day event match is deliberately exact. Wider windows are a separate research axis.
        row["irregular_event"] = (str(row["code"]), int(row["ymd"])) in irregular_events
        history.append(row)


def _load_point_in_time_case(db_path: Path, code: str) -> list[dict[str, Any]]:
    """PAN-first daily path with explicit Yahoo provisional fill; Yahoo OHLC may be close-only proxy."""
    with duckdb.connect(str(db_path), read_only=True) as conn:
        data = conn.execute(
            """
            SELECT source,
              CASE WHEN length(CAST(abs(date) AS VARCHAR))=8 THEN CAST(date AS INTEGER)
                   ELSE CAST(strftime(to_timestamp(CAST(date AS BIGINT)), '%Y%m%d') AS INTEGER) END ymd,
              CAST(o AS DOUBLE),CAST(h AS DOUBLE),CAST(l AS DOUBLE),CAST(c AS DOUBLE),CAST(v AS DOUBLE)
            FROM daily_bars WHERE CAST(code AS VARCHAR)=? AND c>0 ORDER BY ymd, CASE WHEN source='pan' THEN 0 ELSE 1 END
            """, [code]
        ).fetchall()
    chosen: dict[int, tuple[Any, ...]] = {}
    for item in data:
        chosen.setdefault(int(item[1]), item)
    raw = list(chosen.values())
    rows: list[dict[str, Any]] = []
    closes: list[float] = []
    volumes: list[float] = []
    true_ranges: list[float] = []
    for source, ymd, o, h, l, c, v in raw:
        prior_raw_close = closes[-1] if closes else float(c)
        closes.append(float(c)); volumes.append(float(v or 0))
        true_ranges.append(max(float(h)-float(l),abs(float(h)-prior_raw_close),abs(float(l)-prior_raw_close)))
        def avg(n: int) -> float | None:
            return sum(closes[-n:]) / n if len(closes) >= n else None
        ma7, ma20, ma60, ma100, ma200 = avg(7), avg(20), avg(60), avg(100), avg(200)
        if ma60 is None:
            continue
        prior = rows[-1] if rows else None
        rng = max(float(h)-float(l), 1e-9)
        high60 = max(float(item[3]) for item in raw[max(0, len(closes)-61):len(closes)-1]) if len(closes)>1 else float(h)
        low60 = min(float(item[4]) for item in raw[max(0, len(closes)-61):len(closes)-1]) if len(closes)>1 else float(l)
        low20 = min(float(item[4]) for item in raw[max(0, len(closes)-21):len(closes)-1]) if len(closes)>1 else float(l)
        ma7_prev = prior["ma7"] if prior else ma7
        ma20_prev = prior["ma20"] if prior else ma20
        pclose = prior_raw_close
        row = {"code":code,"name":"ミスミグループ本社" if code=="9962" else None,"source":source,"bar_status":"confirmed" if source=="pan" else "provisional_yahoo",
               "provisional_close_only":bool(source=="yahoo" and float(o)==float(h)==float(l)==float(c)),
               "ymd":int(ymd),"o":float(o),"h":float(h),"l":float(l),"c":float(c),"v":float(v or 0),"pclose":pclose,"phigh":prior["h"] if prior else float(h),
               "ma7":ma7,"ma20":ma20,"ma60":ma60,"ma100":ma100,"ma200":ma200,"ma7_prev":ma7_prev,"ma20_prev":ma20_prev,"ma60_prev":prior.get("ma60") if prior else ma60,"ma100_prev":prior.get("ma100") if prior else ma100,"ma200_prev":prior.get("ma200") if prior else ma200,"atr14":sum(true_ranges[-14:])/min(14,len(true_ranges)),"vol20":sum(volumes[-20:])/min(20,len(volumes)),
               "body_ratio":abs(float(c)-float(o))/rng,"upper_wick_ratio":(float(h)-max(float(o),float(c)))/rng,"lower_wick_ratio":(min(float(o),float(c))-float(l))/rng,"close_pos":(float(c)-float(l))/rng,
               "ret3":float(c)/closes[-4]-1 if len(closes)>=4 else 0.0,"ret5":float(c)/closes[-6]-1 if len(closes)>=6 else 0.0,"ret10":float(c)/closes[-11]-1 if len(closes)>=11 else 0.0,"dist_ma7":float(c)/ma7-1,"dist_ma20":float(c)/ma20-1,"dist_ma60":float(c)/ma60-1,
               "pos60":(float(c)-low60)/max(high60-low60,1e-9),"cross_ma7":int(pclose>=ma7_prev and float(c)<ma7),"cross_ma20":int(pclose>=ma20_prev and float(c)<ma20),
               "failed_rebound_ma7":int(float(h)>=ma7 and float(c)<ma7 and (float(h)-max(float(o),float(c)))/rng>=.20),"break_low20":int(float(c)<low20)}
        row["breakdown_score"] = int(row["close_pos"]<=.25)+int(float(c)<float(o) and row["body_ratio"]>=.45)+int(row["upper_wick_ratio"]>=.25)+row["cross_ma7"]+2*row["cross_ma20"]+row["failed_rebound_ma7"]+int(float(c)<ma7 and ma7<ma7_prev)+int(row["ret5"]<0)+int(row["pos60"]>=.60)+row["break_low20"]
        rows.append(row)
    for row in rows:
        dt=datetime.strptime(str(row["ymd"]),"%Y%m%d");row["_month_key"]=dt.strftime("%Y%m");row["_week_key"]=dt.strftime("%G%V")
    annotate_path_signals(rows, set())
    return rows


def run(db_path: Path, output_root: Path, start_ymd: int, end_ymd: int, sample_count: int) -> Path:
    tag = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    output_dir = output_root / f"{tag}-{AXIS_ID}"
    output_dir.mkdir(parents=True, exist_ok=False)
    conn = duckdb.connect(str(db_path), read_only=True)
    try:
        sql = r"""
        WITH raw AS (
          SELECT b.code, t.name,
            CASE WHEN length(CAST(abs(b.date) AS VARCHAR))=8 THEN CAST(b.date AS INTEGER)
                 ELSE CAST(strftime(to_timestamp(CAST(b.date AS BIGINT)), '%Y%m%d') AS INTEGER) END ymd,
            b.o,b.h,b.l,b.c,CAST(b.v AS DOUBLE) v
          FROM daily_bars b LEFT JOIN tickers t ON t.code=b.code
          WHERE coalesce(b.source,'pan') <> 'yahoo' AND b.o>0 AND b.h>=b.l AND b.c>0
        ), base AS (
          SELECT *,
            lag(c) OVER w pclose, lag(h) OVER w phigh, lag(l) OVER w plow,
            avg(c) OVER w7 ma7, avg(c) OVER w20 ma20, avg(c) OVER w60 ma60,
            avg(c) OVER w100 ma100, avg(c) OVER w200 ma200,
            avg(v) OVER w20 vol20,
            max(h) OVER (PARTITION BY code ORDER BY ymd ROWS BETWEEN 60 PRECEDING AND 1 PRECEDING) high60,
            min(l) OVER (PARTITION BY code ORDER BY ymd ROWS BETWEEN 60 PRECEDING AND 1 PRECEDING) low60,
            min(l) OVER (PARTITION BY code ORDER BY ymd ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING) low20,
            lag(c,3) OVER w close3, lag(c,5) OVER w close5, lag(c,10) OVER w close10,
            lead(c,1) OVER w c1, lead(c,3) OVER w c3, lead(c,5) OVER w c5, lead(c,10) OVER w c10,
            least(lead(l,1) OVER w,lead(l,2) OVER w,lead(l,3) OVER w,lead(l,4) OVER w,lead(l,5) OVER w) low5,
            least(lead(l,1) OVER w,lead(l,2) OVER w,lead(l,3) OVER w,lead(l,4) OVER w,lead(l,5) OVER w,
                  lead(l,6) OVER w,lead(l,7) OVER w,lead(l,8) OVER w,lead(l,9) OVER w,lead(l,10) OVER w) low10
            ,greatest(lead(h,1) OVER w,lead(h,2) OVER w,lead(h,3) OVER w,lead(h,4) OVER w,lead(h,5) OVER w,
                  lead(h,6) OVER w,lead(h,7) OVER w,lead(h,8) OVER w,lead(h,9) OVER w,lead(h,10) OVER w) high10_forward
          FROM raw
          WINDOW w AS (PARTITION BY code ORDER BY ymd),
                 w7 AS (PARTITION BY code ORDER BY ymd ROWS BETWEEN 6 PRECEDING AND CURRENT ROW),
                 w20 AS (PARTITION BY code ORDER BY ymd ROWS BETWEEN 19 PRECEDING AND CURRENT ROW),
                 w60 AS (PARTITION BY code ORDER BY ymd ROWS BETWEEN 59 PRECEDING AND CURRENT ROW),
                 w100 AS (PARTITION BY code ORDER BY ymd ROWS BETWEEN 99 PRECEDING AND CURRENT ROW),
                 w200 AS (PARTITION BY code ORDER BY ymd ROWS BETWEEN 199 PRECEDING AND CURRENT ROW)
        ), staged AS (
          SELECT *, lag(ma7) OVER (PARTITION BY code ORDER BY ymd) ma7_prev,
                    lag(ma20) OVER (PARTITION BY code ORDER BY ymd) ma20_prev,
                    lag(ma60) OVER (PARTITION BY code ORDER BY ymd) ma60_prev,
                    lag(ma100) OVER (PARTITION BY code ORDER BY ymd) ma100_prev,
                    lag(ma200) OVER (PARTITION BY code ORDER BY ymd) ma200_prev,
                    avg(greatest(h-l,abs(h-pclose),abs(l-pclose))) OVER (PARTITION BY code ORDER BY ymd ROWS BETWEEN 13 PRECEDING AND CURRENT ROW) atr14
          FROM base
        ), feat AS (
          SELECT *,
            abs(c-o)/nullif(h-l,0) body_ratio,
            (h-greatest(o,c))/nullif(h-l,0) upper_wick_ratio,
            (least(o,c)-l)/nullif(h-l,0) lower_wick_ratio,
            (c-l)/nullif(h-l,0) close_pos,
            (c/close3)-1 ret3,(c/close5)-1 ret5,(c/close10)-1 ret10,
            (c-ma7)/ma7 dist_ma7,(c-ma20)/ma20 dist_ma20,(c-ma60)/ma60 dist_ma60,
            (c-low60)/nullif(high60-low60,0) pos60,
            CASE WHEN pclose>=ma7_prev AND c<ma7 THEN 1 ELSE 0 END cross_ma7,
            CASE WHEN pclose>=ma20_prev AND c<ma20 THEN 1 ELSE 0 END cross_ma20,
            CASE WHEN h>=ma7 AND c<ma7 AND (h-greatest(o,c))/nullif(h-l,0)>=0.20 THEN 1 ELSE 0 END failed_rebound_ma7,
            CASE WHEN c<low20 THEN 1 ELSE 0 END break_low20
          FROM staged
        ), scored AS (
          SELECT *,
            (CASE WHEN close_pos<=0.25 THEN 1 ELSE 0 END + CASE WHEN c<o AND body_ratio>=0.45 THEN 1 ELSE 0 END +
             CASE WHEN upper_wick_ratio>=0.25 THEN 1 ELSE 0 END + cross_ma7 + 2*cross_ma20 + failed_rebound_ma7 +
             CASE WHEN c<ma7 AND ma7<ma7_prev THEN 1 ELSE 0 END + CASE WHEN ret5<0 THEN 1 ELSE 0 END +
             CASE WHEN pos60>=0.60 THEN 1 ELSE 0 END + break_low20) breakdown_score
          FROM feat
        )
        SELECT * FROM scored
        WHERE ymd BETWEEN ? AND ? AND ma60 IS NOT NULL
          AND name NOT ILIKE '%ETF%' AND name NOT ILIKE '%ETN%' AND name NOT ILIKE '%REIT%'
        ORDER BY ymd,code
        """
        irregular_events, event_source_status = _load_irregular_events(conn)
        cur = conn.execute(sql, [start_ymd, end_ymd])
        cols = [item[0] for item in cur.description]
        rows = [dict(zip(cols, row, strict=True)) for row in cur.fetchall()]
    finally:
        conn.close()

    market_by_date: dict[int, dict[str, float]] = {}
    grouped: dict[int, list[dict[str, Any]]] = {}
    for row in rows:
        grouped.setdefault(int(row["ymd"]), []).append(row)
    for ymd, part in grouped.items():
        comparable = [item for item in part if item.get("pclose") is not None and float(item["pclose"]) > 0]
        market_by_date[ymd] = {
            "market_breadth_ma20": sum(float(item["c"]) > float(item["ma20"]) for item in part) / len(part),
            "market_breadth_ma60": sum(float(item["c"]) > float(item["ma60"]) for item in part) / len(part),
            "market_advancers_ratio": sum(float(item["c"]) > float(item["pclose"]) for item in comparable) / len(comparable) if comparable else 0.5,
            "market_mean_ret1": sum(float(item["c"]) / float(item["pclose"]) - 1 for item in comparable) / len(comparable) if comparable else 0.0,
        }
    for row in rows:
        row.update(market_by_date[int(row["ymd"])])
        dt = datetime.strptime(str(int(row["ymd"])), "%Y%m%d")
        row["_month_key"] = dt.strftime("%Y%m")
        row["_week_key"] = dt.strftime("%G%V")
    annotate_path_signals(rows, irregular_events)
    for row in rows:
        row["breakdown_detected"] = bool(row["breakdown_score"] >= 5 and (row["cross_ma7"] or row["cross_ma20"] or row["failed_rebound_ma7"] or row["break_low20"]))
        row["continuation_permitted"] = bool(row["breakdown_detected"] and row.get("c3") is not None and row.get("low5") is not None and row["c3"] < row["c"] and row["c3"] < row["ma7"] and row["low5"] < row["l"])
        row["down_close_5"] = None if row.get("c5") is None else bool(row["c5"] < row["c"])
        row["down_close_10"] = None if row.get("c10") is None else bool(row["c10"] < row["c"])
        row["down_low_5pct_10"] = None if row.get("low10") is None else bool(row["low10"] / row["c"] - 1 <= -0.05)
        row["ret5_forward"] = None if row.get("c5") is None else row["c5"] / row["c"] - 1
        row["ret10_forward"] = None if row.get("c10") is None else row["c10"] / row["c"] - 1
        row["mfe_short_10"] = None if row.get("low10") is None else 1 - row["low10"] / row["c"]
        row["mfe_long_10"] = None if row.get("high10_forward") is None else row["high10_forward"] / row["c"] - 1
        row["rebound_high_5pct_10"] = None if row.get("high10_forward") is None else bool(row["high10_forward"] / row["c"] - 1 >= .05)
        row["material_decline_10"] = row["down_low_5pct_10"] is True

    detected = [row for row in rows if row["breakdown_detected"]]
    continued = [row for row in rows if row["continuation_permitted"]]
    second_breaks = [row for row in rows if row["sequence_second_break"] and not row["irregular_event"]]
    confirm2 = [row for row in rows if row["continuation_confirm2"] and not row["irregular_event"]]
    material = [row for row in rows if row["material_decline_10"] and not row["irregular_event"]]
    material_with_omen = [row for row in material if row["observable_omen_today"] or row["observable_omen_prior10"]]
    material_events = _dedupe_events(material)
    material_events_with_omen = [row for row in material_events if row["observable_omen_today"] or row["observable_omen_prior10"]]
    omen_reason_counts: dict[str, int] = {}
    rows_by_code: dict[str, list[dict[str, Any]]] = {}
    for item in rows:
        rows_by_code.setdefault(str(item["code"]), []).append(item)
    for event in material_events:
        code_rows = [row for row in rows_by_code[str(event["code"])] if int(row["ymd"]) <= int(event["ymd"])]
        for reason in {reason for row in code_rows[-11:] for reason in row.get("omen_reasons", [])}:
            omen_reason_counts[reason] = omen_reason_counts.get(reason, 0) + 1
    second_break_events = _dedupe_events(second_breaks)
    confirm2_events = _dedupe_events(confirm2)
    score_band_metrics = {}
    for threshold in (2.0,3.0,4.0,5.0,6.0,7.0,8.0):
        candidates = _dedupe_events([row for row in rows if row["sell_flow_previous"] < threshold <= row["sell_flow_score"] and row["sell_flow_delta"] > 0 and not row["irregular_event"]])
        score_band_metrics[str(threshold)] = _outcome_metrics(candidates)
    action_state_metrics = {}
    for state_name in ("additional_sell","initial_sell","breakdown_watch","take_profit_rebound_watch","oversold_no_chase","neutral_or_rebound"):
        candidates = _dedupe_events([row for row in rows if row["trade_action_state"] == state_name and not row["irregular_event"]])
        action_state_metrics[state_name] = _outcome_metrics(candidates)
    baseline = rows
    winner_samples: list[dict[str, Any]] = []
    for row in sorted(continued, key=lambda row: (-row["mfe_short_10"], row["ymd"], row["code"])):
        if row["code"] not in {item["code"] for item in winner_samples}:
            winner_samples.append(row)
        if len(winner_samples) >= sample_count:
            break
    false_samples = sorted([row for row in detected if row["down_close_10"] is False], key=lambda row: (row["ret10_forward"], row["code"]), reverse=True)[:max(2, sample_count//2)]

    def compact(row: dict[str, Any]) -> dict[str, Any]:
        keys = ["code","name","ymd","o","h","l","c","ma7","ma20","ma60","body_ratio","upper_wick_ratio","lower_wick_ratio","close_pos","ret5","dist_ma7","dist_ma20","dist_ma60","pos60","cross_ma7","cross_ma20","failed_rebound_ma7","break_low20","breakdown_score","continuation_permitted","ret5_forward","ret10_forward","mfe_short_10"]
        return {key: row.get(key) for key in keys}

    metrics = {
        "baseline": {"n": len(baseline), "down_close_5_rate": _rate(baseline,"down_close_5"), "down_close_10_rate": _rate(baseline,"down_close_10"), "down_low_5pct_10_rate": _rate(baseline,"down_low_5pct_10"), "mean_ret10": _mean(baseline,"ret10_forward")},
        "breakdown_detected": {"n": len(detected), "codes": len({r['code'] for r in detected}), "down_close_5_rate": _rate(detected,"down_close_5"), "down_close_10_rate": _rate(detected,"down_close_10"), "down_low_5pct_10_rate": _rate(detected,"down_low_5pct_10"), "mean_ret10": _mean(detected,"ret10_forward")},
        "continuation_permitted": {"n": len(continued), "codes": len({r['code'] for r in continued}), "down_close_5_rate": _rate(continued,"down_close_5"), "down_close_10_rate": _rate(continued,"down_close_10"), "down_low_5pct_10_rate": _rate(continued,"down_low_5pct_10"), "mean_ret10": _mean(continued,"ret10_forward")},
        "path_second_break_non_irregular": {"n": len(second_breaks), "codes": len({r['code'] for r in second_breaks}), "down_close_10_rate": _rate(second_breaks,"down_close_10"), "down_low_5pct_10_rate": _rate(second_breaks,"down_low_5pct_10"), "mean_ret10": _mean(second_breaks,"ret10_forward")},
        "continuation_confirm2_non_irregular": {"n": len(confirm2), "codes": len({r['code'] for r in confirm2}), "down_close_10_rate": _rate(confirm2,"down_close_10"), "down_low_5pct_10_rate": _rate(confirm2,"down_low_5pct_10"), "mean_ret10": _mean(confirm2,"ret10_forward")},
        "proposition_coverage": {"material_decline_n": len(material), "material_decline_with_prior10_omen_n": len(material_with_omen), "coverage_rate": len(material_with_omen)/len(material) if material else None, "claim_confirmed": bool(material and len(material_with_omen)==len(material))},
        "deduplicated": {
            "material_decline_events": len(material_events),
            "material_decline_with_prior10_omen_events": len(material_events_with_omen),
            "omen_coverage_rate": len(material_events_with_omen)/len(material_events) if material_events else None,
            "omen_reason_event_counts": dict(sorted(omen_reason_counts.items(), key=lambda item: (-item[1], item[0]))),
            "additional_sell_candidate": _outcome_metrics(second_break_events),
            "continuation_confirm2": _outcome_metrics(confirm2_events),
            "additional_sell_candidate_by_year": {str(year): _outcome_metrics([row for row in second_break_events if str(row["ymd"]).startswith(str(year))]) for year in (2024,2025,2026)},
            "sell_flow_score_thresholds": score_band_metrics,
            "trade_action_state_metrics": action_state_metrics,
        },
    }
    interaction_challenger = _train_only_interaction_challenger(rows)
    action_metric = metrics["deduplicated"]["additional_sell_candidate"]
    yearly_action = metrics["deduplicated"]["additional_sell_candidate_by_year"]
    goal_gate_audit = {
        "omen_coverage_ge_080": (metrics["deduplicated"]["omen_coverage_rate"] or 0) >= .80,
        "action_low5pct_rate_ge_055": (action_metric["down_low_5pct_10_rate"] or 0) >= .55,
        "action_down_close_rate_ge_060": (action_metric["down_close_10_rate"] or 0) >= .60,
        "action_mean_ret10_le_minus_015": (action_metric["mean_ret10"] if action_metric["mean_ret10"] is not None else 1) <= -.015,
        "action_rebound_rate_le_020": (action_metric["rebound_high_5pct_10_rate"] if action_metric["rebound_high_5pct_10_rate"] is not None else 1) <= .20,
        "all_years_direction_consistent": all((yearly_action[str(year)]["mean_ret10"] if yearly_action[str(year)]["mean_ret10"] is not None else 1) < 0 for year in (2024,2025,2026)),
        "misumi_9962_20260713_additional_sell": any(str(row["code"])=="9962" and int(row["ymd"])==20260713 and row["sequence_second_break"] for row in rows),
        "all_required_gates_pass": False,
        "blocked_reason":"no fixed point-in-time chart rule or train-2024 interaction branch tested generalizes through 2025 and 2026 action gates; further threshold fitting on validation/shadow would be leakage",
    }
    decision = "keep_review_only" if len(detected)>=30 and (metrics["breakdown_detected"]["down_close_10_rate"] or 0) >= (metrics["baseline"]["down_close_10_rate"] or 0)+0.08 else "hold"
    payload = {
        "schema_version": f"{AXIS_ID}.compare.v1", "artifact_role": "authoritative", "axis_id": AXIS_ID,
        "research_phase": "effectiveness_judgment", "generated_at": datetime.now(timezone.utc).isoformat(),
        "research_proposition": {"text": PROPOSITION, "treatment": "falsifiable_hypothesis_not_hard_coded_truth", "material_decline_definition": "next 10 bars low <= signal close * 0.95", "omen_lookback_bars": 10},
        "changed_axis_detail": {"axis":"signed_decaying_sell_flow_score","definition":"previous score*0.75 plus bearish evidence minus bullish reversal evidence; strong bull candles and MA reclaims explicitly reduce or reverse flow","point_in_time":True,"other_new_axes_added":False},
        "fixed_evaluation_conditions": {"evaluation_period":[start_ymd,end_ymd],"history_before_period_used_for_ma":True,"horizons":[5,10],"cost_policy":"ignored_by_user_rule","universe":"runtime non-ETF confirmed PAN bars","changed_axis":"candlestick-position-MA-break-wick-last5 composite only"},
        "runtime_stock_db_status": inspect_runtime_stock_db(runtime_db_path=db_path), "event_exclusion_sources": event_source_status, "metrics": metrics,
        "train_only_interaction_challenger": interaction_challenger,
        "goal_gate_audit": goal_gate_audit,
        "sample_successes": [compact(row) for row in winner_samples], "sample_failures": [compact(row) for row in false_samples],
        "observed_branching": {"changed_top5_members_count":None,"changed_top10_members_count":None,"changed_rank_count":None,"selection_divergence_reason":"independent daily forecast surface; no ranking comparison"},
        "judgment": {"candidate_local_decision":decision,"authoritative_rollup_decision":"review_only","reason_type":"2026_chart_detail_breakdown_and_continuation_validation"},
        "boundary": {"owner":"TRADEX","runtime_db_write":False,"production_ranking_changed":False,"meemee_changed":False},
        "daily_output_contract": {"states":["mature_high_zone","high_failure_or_supply","initial_ma_or_support_break","weak_rebound","sequence_second_break"],"action":"sequence_second_break is a review-only continuation-short signal"},
        "remaining_risks":["provisional Yahoo intraday bars are not included in this confirmed-bar replay","event sources may be current snapshots rather than historically point-in-time complete","material-decline rows overlap and are not episode-deduplicated","thresholds require out-of-period validation"]
    }
    _write_json(output_dir / "compare.json", payload)
    import csv
    signal_columns = ["code","name","ymd","o","h","l","c","v","ma7","ma20","ma60","ma100","ma200","atr14","ma7_prev","ma20_prev","ma60_prev","ma100_prev","ma200_prev","vol20","body_ratio","upper_wick_ratio","lower_wick_ratio","close_pos","ret3","ret5","ret10","drawdown_from_high5","drawdown_from_high10","dist_ma7","dist_ma20","dist_ma60","pos60","above_ma60_streak","gap_down_2pct","support_level_30","support_touch_count_30","support_break","support_to_resistance","bearish_full_retrace","retraced_bull_ymd","weak_rebound_second_break","monthly_high_failure","weekly_lower_high_failure","higher_timeframe_failure","market_breadth_ma20","market_breadth_ma60","market_advancers_ratio","market_mean_ret1","sell_flow_previous","sell_flow_additions","sell_flow_deductions","sell_flow_delta","sell_flow_score","sell_flow_state","rebound_risk_components","rebound_risk_score","net_sell_score","trade_action_state","continuation_confirm2","sequence_second_break","irregular_event","ret10_forward","mfe_short_10","rebound_high_5pct_10"]
    with (output_dir / "sequence_signals.csv").open("w", encoding="utf-8-sig", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=signal_columns)
        writer.writeheader()
        writer.writerows({key: row.get(key) for key in signal_columns} for row in rows if row["sequence_second_break"] or row["continuation_confirm2"] or row["sell_flow_score"] >= 2 or row["rebound_risk_score"] >= 2)
    nikkei_columns = ["code","ymd","c","sell_flow_additions","sell_flow_deductions","rebound_risk_score","range20_pct","sideways20_run_length","flat_bar","flat_run_length","volume_compression_5_20","sideways_position","sideways_ma20_side","pretrend_10","irregular_event","ret5_forward","ret10_forward","mfe_short_10","mfe_long_10","rebound_high_5pct_10"]
    with (output_dir / "nikkei225_daily_score_components.csv").open("w", encoding="utf-8-sig", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=nikkei_columns)
        writer.writeheader()
        writer.writerows({key: row.get(key) for key in nikkei_columns} for row in rows if str(row["code"]) in NIKKEI_225_CODES)
    case_columns = signal_columns + ["cross_ma7","cross_ma20","failed_rebound_ma7","break_low20","breakdown_score","observable_omen_today","observable_omen_prior10"]
    with (output_dir / "case_9962_daily.csv").open("w", encoding="utf-8-sig", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=case_columns)
        writer.writeheader()
        writer.writerows({key: row.get(key) for key in case_columns} for row in rows if str(row["code"]) == "9962" and int(row["ymd"]) >= 20260701)
    provisional_case = _load_point_in_time_case(db_path, "9962")
    provisional_columns = ["code","name","source","bar_status","provisional_close_only","ymd","o","h","l","c","v","ma7","ma20","ma60","above_ma60_streak","gap_down_2pct","cross_ma7","cross_ma20","breakdown_score","weak_rebound_second_break","monthly_high_failure","weekly_lower_high_failure","higher_timeframe_failure","drawdown_from_high5","drawdown_from_high10","sell_flow_additions","sell_flow_deductions","sell_flow_delta","sell_flow_score","sell_flow_state","rebound_risk_components","rebound_risk_score","net_sell_score","trade_action_state","sequence_second_break"]
    with (output_dir / "case_9962_pan_yahoo_daily.csv").open("w", encoding="utf-8-sig", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=provisional_columns)
        writer.writeheader()
        writer.writerows({key: row.get(key) for key in provisional_columns} for row in provisional_case if int(row["ymd"]) >= 20260701)
    payload["current_case_9962"] = {
        "source_policy": "PAN preferred for same date; Yahoo used only when PAN absent; status explicit",
        "latest_rows": [{key: row.get(key) for key in provisional_columns} for row in provisional_case if int(row["ymd"]) >= 20260710],
    }
    _write_json(output_dir / "compare.json", payload)
    _write_json(output_root / "latest.json", {"run_root":str(output_dir),"compare":str(output_dir/'compare.json')})
    return output_dir


def main() -> int:
    parser=argparse.ArgumentParser()
    parser.add_argument("--db-path",type=Path,default=None); parser.add_argument("--output-root",type=Path,default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--start-ymd",type=int,default=20260101); parser.add_argument("--end-ymd",type=int,default=20260710); parser.add_argument("--sample-count",type=int,default=6)
    args=parser.parse_args(); print(run(args.db_path or resolve_runtime_stock_db_path(),args.output_root,args.start_ymd,args.end_ymd,args.sample_count)); return 0

if __name__ == "__main__": raise SystemExit(main())
