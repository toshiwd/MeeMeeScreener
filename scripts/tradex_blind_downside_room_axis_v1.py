"""Discover a PIT downside-room complement axis on the frozen human review set."""
import argparse
import hashlib
import json
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd


THRESHOLDS = (0.0, 0.25, 0.5, 0.75, 1.0, 1.5, 2.0)
SUPPORT_COLUMNS = ("prior_low20", "prior_low60", "ma20", "ma60", "ma100", "ma200")


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def stats(frame: pd.DataFrame) -> dict:
    complete = frame[frame.status.eq("complete")]
    returns = complete.return_fixed3_pct.dropna()
    gain, loss = returns[returns > 0].sum(), -returns[returns < 0].sum()
    return {
        "n": int(len(frame)), "D": int(complete.outcome_fixed3.eq("D").sum()),
        "R": int(complete.outcome_fixed3.eq("R").sum()), "N": int(complete.outcome_fixed3.eq("N").sum()),
        "mean_fixed3_pct": None if returns.empty else float(returns.mean()),
        "mean_h5_close_pct": None if complete.empty else float(complete.return_h5_close_pct.mean()),
        "profit_factor": None if loss == 0 else float(gain / loss),
        "max_loss_pct": None if returns.empty else float(returns.min()),
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--board", type=Path, required=True)
    parser.add_argument("--agreement", type=Path, required=True)
    parser.add_argument("--outcomes", type=Path, required=True)
    parser.add_argument("--db", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)

    board = pd.read_parquet(args.board)
    board.code = board.code.astype(str).str.zfill(4)
    agreement = pd.read_parquet(args.agreement / "human_agreement_ledger.parquet")
    outcomes = pd.read_parquet(args.outcomes / "outcome_reveal_ledger.parquet")
    codes = board.code.unique().tolist()
    connection = duckdb.connect(str(args.db), read_only=True)
    prices = connection.execute(
        "select code, strftime(to_timestamp(date), '%Y%m%d')::integer ymd, o, h, l, c "
        "from daily_bars where code in (select unnest(?)) order by code, date", [codes]
    ).fetchdf()
    prices.code = prices.code.astype(str).str.zfill(4)
    histories = {code: group.reset_index(drop=True) for code, group in prices.groupby("code")}

    feature_rows = []
    close_mismatch = []
    for row in board.itertuples():
        history = histories[row.code]
        hit = history.index[history.ymd.eq(int(row.ymd))]
        if len(hit) != 1:
            raise RuntimeError(f"missing price row {row.code} {row.ymd}")
        index = int(hit[0])
        if not np.isclose(float(history.iloc[index].c), float(row.c), rtol=0, atol=max(0.01, abs(float(row.c)) * 1e-6)):
            close_mismatch.append({"case_id": row.case_id, "board_c": float(row.c), "db_c": float(history.iloc[index].c)})
        prior = history.iloc[:index]
        through = history.iloc[:index + 1].copy()
        previous_close = through.c.shift(1)
        true_range = pd.concat([
            through.h - through.l,
            (through.h - previous_close).abs(),
            (through.l - previous_close).abs(),
        ], axis=1).max(axis=1)
        atr14 = float(true_range.tail(14).mean())
        values = {
            "prior_low20": float(prior.tail(20).l.min()),
            "prior_low60": float(prior.tail(60).l.min()),
            "ma20": float(row.ma20), "ma60": float(row.ma60),
            "ma100": float(row.ma100), "ma200": float(row.ma200),
        }
        lower = {name: value for name, value in values.items() if np.isfinite(value) and value < float(row.c)}
        nearest_name, nearest_value = max(lower.items(), key=lambda item: item[1]) if lower else (None, np.nan)
        feature = {
            "case_id": row.case_id, "code": row.code, "ymd": int(row.ymd), "c": float(row.c),
            "atr14": atr14, "nearest_lower_support_type": nearest_name,
            "nearest_lower_support": nearest_value,
            "downside_room_pct": None if not np.isfinite(nearest_value) else 100 * (float(row.c) - nearest_value) / float(row.c),
            "downside_room_atr": None if not np.isfinite(nearest_value) or atr14 <= 0 else (float(row.c) - nearest_value) / atr14,
            "lower_support_count": len(lower),
        }
        for name, value in values.items():
            feature[name] = value
            feature[f"room_to_{name}_atr"] = (float(row.c) - value) / atr14 if np.isfinite(value) and atr14 > 0 else None
        feature_rows.append(feature)
    features = pd.DataFrame(feature_rows)
    joined = agreement.merge(features, on=["case_id", "code", "ymd"], validate="one_to_one").merge(
        outcomes[["case_id", "status", "outcome_fixed3", "return_fixed3_pct", "return_h5_close_pct"]],
        on="case_id", validate="one_to_one",
    )
    joined["human_direction"] = joined.human_new_entry_decision.map({"SELL": "SELL", "WAIT": "NO_SELL", "AVOID": "NO_SELL"}).fillna("")
    joined["model_direction"] = joined.model_action.map(
        lambda value: "SELL" if value in {"PROBE", "CORE", "ADD", "REENTRY_PROBE"} else "NO_SELL" if value == "AVOID" else "MANAGEMENT"
    )
    answered = joined[joined.human_direction.ne("")].copy()
    scans = {}
    for threshold in THRESHOLDS:
        enough_room = answered.downside_room_atr.ge(threshold)
        scans[str(threshold)] = {
            "human_sell_gate": stats(answered[answered.human_direction.eq("SELL") & enough_room]),
            "human_sell_model_veto_and_room": stats(answered[answered.human_direction.eq("SELL") & answered.model_direction.eq("SELL") & enough_room]),
            "model_sell_user_no_sell_suggestion": stats(answered[answered.human_direction.eq("NO_SELL") & answered.model_direction.eq("SELL") & enough_room]),
            "model_sell_all_answered": stats(answered[answered.model_direction.eq("SELL") & enough_room]),
        }
    pair_diagnostics = {}
    answered["direction_pair"] = answered.model_direction + "__" + answered.human_direction
    for pair, group in answered.groupby("direction_pair"):
        pair_diagnostics[pair] = {
            "n": int(len(group)),
            "downside_room_atr_mean": None if group.downside_room_atr.dropna().empty else float(group.downside_room_atr.mean()),
            "downside_room_atr_median": None if group.downside_room_atr.dropna().empty else float(group.downside_room_atr.median()),
            "nearest_support_type_counts": {str(key): int(value) for key, value in group.nearest_lower_support_type.fillna("NONE").value_counts().items()},
            "outcome": stats(group),
        }
    ledger_path = args.output / "downside_room_diagnostic_ledger.parquet"
    joined.to_parquet(ledger_path, index=False)
    result = {
        "schema_version": "tradex_blind_downside_room_axis_v1.compare.v1",
        "artifact_role": "authoritative_discovery_diagnostic",
        "review_only": True,
        "research_phase": "single_axis_discovery_on_frozen_human_review",
        "fixed_conditions": {
            "axis": "distance from signal close to nearest observed lower support",
            "support_candidates": list(SUPPORT_COLUMNS),
            "prior_lows_exclude_signal_day": True,
            "ma_values_confirmed_at_signal_close": True,
            "thresholds_atr": list(THRESHOLDS),
            "execution": "next_session_open", "horizon_sessions": 5,
            "weekly_inputs": [], "costs": "ignored", "clean_oos": False,
        },
        "baselines": {
            "human_sell": stats(answered[answered.human_direction.eq("SELL")]),
            "model_sell_answered": stats(answered[answered.model_direction.eq("SELL")]),
        },
        "pair_diagnostics": pair_diagnostics,
        "threshold_scan": scans,
        "judgment": {"decision": "hold_pending_threshold_selection_and_unseen_validation"},
        "not_changed": ["frozen benchmark model", "MeeMee", "ranking", "runtime DB", "production trading logic"],
    }
    compare_path = args.output / "compare.json"
    compare_path.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    audit = {
        "board_sha256": sha(args.board), "agreement_sha256": sha(args.agreement / "compare.json"),
        "outcomes_sha256": sha(args.outcomes / "compare.json"), "db_path": str(args.db.resolve()),
        "db_read_only": True, "rows": len(joined), "direction_answered": len(answered),
        "close_mismatch_count": len(close_mismatch), "close_mismatches": close_mismatch,
        "weekly_columns_used": [], "future_selection_columns_used": [], "ledger_sha256": sha(ledger_path),
    }
    (args.output / "audit.json").write_text(json.dumps(audit, indent=2) + "\n", encoding="utf-8")
    (args.output / "_ARTIFACT_COMPLETE.json").write_text(
        json.dumps({"complete": True, "authoritative": "compare.json", "sha256": sha(compare_path)}, indent=2) + "\n", encoding="utf-8"
    )
    print(json.dumps({"output": str(args.output), "close_mismatch_count": len(close_mismatch), "baselines": result["baselines"], "pair_diagnostics": pair_diagnostics}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
