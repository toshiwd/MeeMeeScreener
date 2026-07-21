from __future__ import annotations

import argparse
import hashlib
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from scripts.tradex_nikkei225_daily_assessment_baseline_v1 import HORIZONS, _labels


AXIS_ID = "tradex_nikkei225_bull_impulse_failure_rebreak_v1"
STATE_ORDER = (
    "S0_NONE",
    "S1_IMPULSE_LIVE",
    "S2_FULL_RETRACE",
    "S3_REBOUND_ATTEMPT",
    "S4_WEAK_REBOUND",
    "S5_REBREAK_CONFIRMED",
    "INVALIDATED",
    "EXPIRED",
)


def _is_impulse(rows: list[dict[str, Any]], index: int) -> bool:
    row = rows[index]
    atr = float(row["atr14"] or 0.0)
    if index < 5 or atr <= 0 or float(row["h"]) <= float(row["l"]):
        return False
    body_atr = (float(row["c"]) - float(row["o"])) / atr
    prior_close_high = max(float(item["c"]) for item in rows[index - 5:index])
    return bool(
        body_atr >= 0.60
        and float(row["close_pos"] or 0.0) >= 0.65
        and float(row["c"]) >= prior_close_high
    )


def _state_rows(part: pd.DataFrame) -> list[dict[str, Any]]:
    source = part.sort_values("ymd").to_dict("records")
    output: list[dict[str, Any]] = []
    state = "S0_NONE"
    anchor: dict[str, Any] | None = None
    retrace: dict[str, Any] | None = None
    rebound_date: int | None = None
    rebound_peak_ymd: int | None = None
    rebound_peak_high: float | None = None
    max_rebound_close: float | None = None
    above_ma20_count = 0

    def clear() -> None:
        nonlocal anchor, retrace, rebound_date, rebound_peak_ymd, rebound_peak_high
        nonlocal max_rebound_close, above_ma20_count
        anchor = None
        retrace = None
        rebound_date = None
        rebound_peak_ymd = None
        rebound_peak_high = None
        max_rebound_close = None
        above_ma20_count = 0

    for index, row in enumerate(source):
        ymd = int(row["ymd"])
        reason = "state_hold"
        emitted_state = state

        if state == "S5_REBREAK_CONFIRMED":
            state = "S0_NONE"
            clear()
            reason = "post_rebreak_reset"
        elif state in {"INVALIDATED", "EXPIRED"}:
            state = "S0_NONE"
            clear()
            reason = "terminal_reset"

        if _is_impulse(source, index):
            anchor = {
                "index": index,
                "ymd": ymd,
                "o": float(row["o"]),
                "h": float(row["h"]),
                "c": float(row["c"]),
                "mid": (float(row["o"]) + float(row["c"])) / 2.0,
            }
            retrace = None
            rebound_date = None
            rebound_peak_ymd = None
            rebound_peak_high = None
            max_rebound_close = None
            above_ma20_count = 0
            state = "S1_IMPULSE_LIVE"
            reason = "bull_impulse_anchor"
        elif anchor is not None:
            anchor_age = index - int(anchor["index"])
            close = float(row["c"])
            high = float(row["h"])
            low = float(row["l"])
            ma20 = float(row["ma20"])
            close_pos = float(row["close_pos"] or 0.0)

            if close > float(anchor["h"]):
                state = "INVALIDATED"
                reason = "anchor_high_close_break"
            elif state == "S1_IMPULSE_LIVE":
                bearish_count = sum(
                    float(source[j]["c"]) < float(source[j]["o"])
                    for j in range(int(anchor["index"]) + 1, index + 1)
                )
                if anchor_age <= 5 and close <= float(anchor["o"]) and bearish_count >= 2:
                    retrace_low = min(
                        float(source[j]["l"])
                        for j in range(int(anchor["index"]) + 1, index + 1)
                    )
                    retrace = {
                        "index": index,
                        "ymd": ymd,
                        "low": retrace_low,
                        "atr": float(row["atr14"] or 0.0),
                    }
                    state = "S2_FULL_RETRACE"
                    reason = "two_bear_full_body_retrace"
                elif anchor_age > 10:
                    state = "EXPIRED"
                    reason = "anchor_age_gt_10"
            elif retrace is not None:
                retrace_age = index - int(retrace["index"])
                if close > float(anchor["mid"]) and close > ma20:
                    above_ma20_count += 1
                else:
                    above_ma20_count = 0
                if above_ma20_count >= 2:
                    state = "INVALIDATED"
                    reason = "two_closes_above_ma20_and_anchor_mid"
                elif retrace_age > 5:
                    state = "EXPIRED"
                    reason = "retrace_age_gt_5"
                else:
                    prior_sequence_low = min(
                        float(source[j]["l"])
                        for j in range(int(retrace["index"]), index)
                    ) if index > int(retrace["index"]) else float(retrace["low"])
                    if (
                        state in {"S3_REBOUND_ATTEMPT", "S4_WEAK_REBOUND"}
                        and close < prior_sequence_low
                        and close < ma20
                        and close_pos <= 0.35
                    ):
                        state = "S5_REBREAK_CONFIRMED"
                        reason = "weak_rebound_low_and_ma20_rebreak"
                    else:
                        if rebound_peak_high is None or high > rebound_peak_high:
                            rebound_peak_high = high
                            rebound_peak_ymd = ymd
                        max_rebound_close = close if max_rebound_close is None else max(max_rebound_close, close)
                        rebound_size = (rebound_peak_high or high) - float(retrace["low"])
                        threshold = 0.50 * float(retrace["atr"])
                        if state == "S2_FULL_RETRACE" and retrace_age >= 1 and rebound_size >= threshold:
                            rebound_date = ymd
                            state = "S3_REBOUND_ATTEMPT"
                            reason = "rebound_ge_half_atr"
                        elif state == "S3_REBOUND_ATTEMPT":
                            if (max_rebound_close or close) <= float(anchor["mid"]) and above_ma20_count <= 1:
                                state = "S4_WEAK_REBOUND"
                                reason = "rebound_below_anchor_mid_and_ma20"
                        elif state == "S4_WEAK_REBOUND":
                            reason = "weak_rebound_hold"

        emitted_state = state
        anchor_age_value = index - int(anchor["index"]) if anchor is not None else None
        if retrace is not None and state in {"S2_FULL_RETRACE", "S3_REBOUND_ATTEMPT", "S4_WEAK_REBOUND", "S5_REBREAK_CONFIRMED"}:
            state_age = index - int(retrace["index"])
        else:
            state_age = anchor_age_value
        enriched = dict(row)
        enriched.update({
            "bull_failure_state": emitted_state,
            "bull_failure_reason_code": reason,
            "bull_failure_state_age": state_age,
            "bull_anchor_ymd": int(anchor["ymd"]) if anchor is not None else None,
            "bull_anchor_open": float(anchor["o"]) if anchor is not None else None,
            "bull_anchor_high": float(anchor["h"]) if anchor is not None else None,
            "bull_anchor_mid": float(anchor["mid"]) if anchor is not None else None,
            "bull_retrace_ymd": int(retrace["ymd"]) if retrace is not None else None,
            "bull_retrace_low": float(retrace["low"]) if retrace is not None else None,
            "bull_rebound_ymd": rebound_date,
            "bull_rebound_peak_ymd": rebound_peak_ymd,
            "bull_rebound_peak_high": rebound_peak_high,
        })
        output.append(enriched)
    return output


def _metrics(frame: pd.DataFrame, labels: np.ndarray) -> dict[str, Any]:
    if len(frame) == 0:
        return {"n": 0, "codes": 0, "months": 0}
    return {
        "n": int(len(frame)),
        "codes": int(frame["code"].nunique()),
        "months": int(frame["ymd"].astype(str).str[:6].nunique()),
        "downside_rate": float((labels == 0).mean()),
        "rebound_rate": float((labels == 1).mean()),
        "neutral_rate": float((labels == 2).mean()),
    }


def run(input_parquet: Path, output_root: Path) -> Path:
    frame = pd.read_parquet(input_parquet).sort_values(["code", "ymd"]).reset_index(drop=True)
    state_rows: list[dict[str, Any]] = []
    for _, part in frame.groupby("code", sort=False):
        state_rows.extend(_state_rows(part))
    ledger = pd.DataFrame(state_rows).sort_values(["code", "ymd"]).reset_index(drop=True)

    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    output = output_root / f"{stamp}-{AXIS_ID}"
    output.mkdir(parents=True, exist_ok=False)
    state_parquet = output / "bull_impulse_failure_rebreak_state_ledger.parquet"
    ledger.to_parquet(state_parquet, index=False, compression="zstd")

    periods = {
        "development_2019_2024": (20190101, 20241231),
        "locked_validation_2025": (20250101, 20251231),
        "exploratory_2026": (20260101, 20260713),
    }
    comparisons: dict[str, Any] = {}
    for horizon in HORIZONS:
        required = [f"ret_close_{horizon}", f"down_exc_{horizon}", f"up_exc_{horizon}"]
        part = ledger.dropna(subset=required).copy()
        labels = _labels(part, horizon)
        horizon_result: dict[str, Any] = {}
        for period_name, (start, end) in periods.items():
            period_mask = part["ymd"].between(start, end).to_numpy()
            period_result = {"universe_baseline": _metrics(part.loc[period_mask], labels[period_mask])}
            for state in ("S2_FULL_RETRACE", "S3_REBOUND_ATTEMPT", "S4_WEAK_REBOUND", "S5_REBREAK_CONFIRMED"):
                mask = period_mask & (part["bull_failure_state"].to_numpy() == state)
                period_result[state] = _metrics(part.loc[mask], labels[mask])
            horizon_result[period_name] = period_result
        comparisons[str(horizon)] = horizon_result

    h5 = comparisons.get("5", {})
    dev = h5.get("development_2019_2024", {})
    val = h5.get("locked_validation_2025", {})
    dev_base, dev_s5 = dev.get("universe_baseline", {}), dev.get("S5_REBREAK_CONFIRMED", {})
    val_base, val_s5 = val.get("universe_baseline", {}), val.get("S5_REBREAK_CONFIRMED", {})
    gates = {
        "development_n_ge_100": dev_s5.get("n", 0) >= 100,
        "validation_n_ge_80": val_s5.get("n", 0) >= 80,
        "development_downside_uplift_ge_5pp": dev_s5.get("downside_rate", 0) >= dev_base.get("downside_rate", 1) + 0.05,
        "validation_downside_uplift_ge_5pp": val_s5.get("downside_rate", 0) >= val_base.get("downside_rate", 1) + 0.05,
        "development_rebound_reduction_ge_3pp": dev_s5.get("rebound_rate", 1) <= dev_base.get("rebound_rate", 0) - 0.03,
        "validation_rebound_reduction_ge_3pp": val_s5.get("rebound_rate", 1) <= val_base.get("rebound_rate", 0) - 0.03,
        "development_codes_ge_50": dev_s5.get("codes", 0) >= 50,
        "validation_codes_ge_40": val_s5.get("codes", 0) >= 40,
        "development_months_ge_24": dev_s5.get("months", 0) >= 24,
        "validation_months_ge_9": val_s5.get("months", 0) >= 9,
    }
    keep = all(gates.values())
    source_hash = hashlib.sha256(input_parquet.read_bytes()).hexdigest()
    payload = {
        "schema_version": f"{AXIS_ID}.compare.v1",
        "artifact_role": "authoritative",
        "research_phase": "effectiveness_judgment",
        "source_parquet": str(input_parquet),
        "source_sha256": source_hash,
        "state_ledger": str(state_parquet),
        "fixed_conditions": {
            "single_axis": "ordered bullish impulse full retrace then weak rebound then low and MA20 rebreak",
            "feature_time": "current close or earlier only",
            "development": "2019-2024 fixed-rule chronological aggregation; no label fitting",
            "locked_validation": 2025,
            "exploratory_only": "2026 through 2026-07-13",
            "comparison_states": ["S2_FULL_RETRACE", "S3_REBOUND_ATTEMPT", "S4_WEAK_REBOUND", "S5_REBREAK_CONFIRMED"],
            "primary_gate_horizon": 5,
            "costs": "ignored by user rule",
        },
        "state_contract": {
            "states": list(STATE_ORDER),
            "s5_is_event_day_only": True,
            "audit_columns": [
                "bull_failure_reason_code", "bull_failure_state_age", "bull_anchor_ymd",
                "bull_retrace_ymd", "bull_rebound_ymd", "bull_rebound_peak_ymd",
            ],
        },
        "comparisons": comparisons,
        "primary_h5_gate_audit": gates,
        "observed_branching": {
            "state_counts": {str(key): int(value) for key, value in ledger["bull_failure_state"].value_counts().items()},
            "s5_rows": int((ledger["bull_failure_state"] == "S5_REBREAK_CONFIRMED").sum()),
            "s5_codes": int(ledger.loc[ledger["bull_failure_state"] == "S5_REBREAK_CONFIRMED", "code"].nunique()),
        },
        "decision": {
            "candidate_local_decision": "hold_for_clean_shadow" if keep else "drop_state_axis",
            "authoritative_rollup_decision": "review_only",
            "reason_type": "locked_2025_all_primary_gates_pass" if keep else "development_or_locked_validation_gate_failed",
        },
        "boundary": {
            "owner": "TRADEX", "meemee_changed": False, "runtime_db_write": False,
            "production_ranking_changed": False,
        },
        "remaining_risks": [
            "current Nikkei225 registry creates survivorship bias",
            "2019-2024 is development aggregation rather than learned rolling OOF because the state rule is fixed",
            "2026 was used for representative discovery and remains exploratory only",
        ],
    }
    compare = output / "compare.json"
    compare.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    (output / "_ARTIFACT_COMPLETE.json").write_text(
        json.dumps({"complete": True, "compare": str(compare), "state_ledger": str(state_parquet)}, indent=2) + "\n",
        encoding="utf-8",
    )
    return output


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-parquet", required=True, type=Path)
    parser.add_argument(
        "--output-root", type=Path,
        default=Path(r"G:\Tradex\tradex_nikkei225_bull_impulse_failure_rebreak_v1"),
    )
    args = parser.parse_args()
    print(run(args.input_parquet, args.output_root))


if __name__ == "__main__":
    main()
