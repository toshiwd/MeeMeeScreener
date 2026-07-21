from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

import tradex_nikkei225_20bar_morphology_sequence_v1 as formal

AXIS_ID = "tradex_nikkei225_first_passage_order_v1"
CLASS_NAMES = {0: "down_first", 1: "rebound_first", 2: "neutral"}
LABEL_CONTRACT = {
    "barrier_source": "D_h=clip(multiplier*ATR14/close, floor, cap)",
    "horizons": {
        str(h): {"multiplier": m, "floor": lo, "cap": hi, "up_ratio_to_down": 0.8}
        for h, (m, lo, hi) in formal.HORIZONS.items()
    },
    "scan": "exact OHLC t+1 through t+h; earliest barrier wins; later reversal never overwrites",
    "same_day_order": [
        "open <= down barrier => down_first",
        "open >= up barrier => rebound_first",
        "open between barriers and both high/low hit => neutral_path_ambiguous",
        "otherwise the single intraday barrier hit wins",
    ],
    "no_hit": "neutral_no_hit",
    "final_close_condition": False,
}


def _future_ohlc(frame: pd.DataFrame, day: int) -> tuple[np.ndarray, ...]:
    grouped = frame.groupby("code", sort=False)
    return tuple(grouped[col].shift(-day).to_numpy(dtype=float) for col in ("o", "h", "l"))


def first_passage(frame: pd.DataFrame, horizon: int) -> pd.DataFrame:
    multiplier, floor, cap = formal.HORIZONS[horizon]
    close = frame["c"].to_numpy(dtype=float)
    d = np.clip(multiplier * frame["atr14"].to_numpy(dtype=float) / close, floor, cap)
    down_price = close * (1.0 - d)
    up_price = close * (1.0 + 0.8 * d)
    n = len(frame)
    label = np.full(n, 2, dtype=np.int8)
    kind = np.full(n, "neutral_no_hit", dtype=object)
    hit_day = np.zeros(n, dtype=np.int8)
    resolved = np.zeros(n, dtype=bool)
    for day in range(1, horizon + 1):
        opn, high, low = _future_ohlc(frame, day)
        available = np.isfinite(opn) & np.isfinite(high) & np.isfinite(low)
        active = (~resolved) & available
        gap_down = active & (opn <= down_price)
        gap_up = active & (~gap_down) & (opn >= up_price)
        between = active & (~gap_down) & (~gap_up)
        low_hit = between & (low <= down_price)
        high_hit = between & (high >= up_price)
        both = low_hit & high_hit
        only_down = low_hit & (~high_hit)
        only_up = high_hit & (~low_hit)
        for mask, value, name in (
            (gap_down, 0, "down_open_gap"),
            (gap_up, 1, "rebound_open_gap"),
            (both, 2, "neutral_path_ambiguous"),
            (only_down, 0, "down_intraday"),
            (only_up, 1, "rebound_intraday"),
        ):
            label[mask] = value
            kind[mask] = name
            hit_day[mask] = day
            resolved[mask] = True
    return pd.DataFrame(
        {
            "label": label,
            "outcome_kind": kind,
            "hit_day": hit_day,
            "down_fraction": d,
            "up_fraction": 0.8 * d,
            "down_price": down_price,
            "up_price": up_price,
        },
        index=frame.index,
    )


def labels(frame: pd.DataFrame, horizon: int) -> np.ndarray:
    return first_passage(frame, horizon)["label"].to_numpy(dtype=np.int8)


def self_tests() -> dict[str, Any]:
    # ATR=10, close=100; the h1 cap applies, so barriers are 97.0/102.4.
    cases = [
        (96, 110, 95, 0, "down_open_gap"),
        (103, 104, 90, 1, "rebound_open_gap"),
        (100, 103, 96, 2, "neutral_path_ambiguous"),
        (100, 101, 96, 0, "down_intraday"),
        (100, 103, 98, 1, "rebound_intraday"),
        (100, 101, 98, 2, "neutral_no_hit"),
    ]
    rows = []
    for idx, (opn, high, low, expected, kind) in enumerate(cases):
        rows.extend(
            [
                {"code": str(idx), "ymd": 20200101, "o": 100.0, "h": 100.0, "l": 100.0, "c": 100.0, "atr14": 10.0},
                {"code": str(idx), "ymd": 20200102, "o": float(opn), "h": float(high), "l": float(low), "c": 100.0, "atr14": 10.0},
            ]
        )
    test_frame = pd.DataFrame(rows)
    got = first_passage(test_frame, 1).iloc[::2].reset_index(drop=True)
    assertions = []
    for i, (_, _, _, expected, expected_kind) in enumerate(cases):
        ok = int(got.loc[i, "label"]) == expected and got.loc[i, "outcome_kind"] == expected_kind
        assertions.append({"case": i, "expected_label": expected, "expected_kind": expected_kind, "pass": bool(ok)})
    # Earliest hit must survive an opposite hit on a later day.
    reversal = pd.DataFrame(
        [
            {"code": "x", "ymd": 1, "o": 100, "h": 100, "l": 100, "c": 100, "atr14": 10},
            {"code": "x", "ymd": 2, "o": 100, "h": 103, "l": 94, "c": 95, "atr14": 10},
            {"code": "x", "ymd": 3, "o": 100, "h": 120, "l": 99, "c": 115, "atr14": 10},
        ]
    )
    rev = first_passage(reversal, 3).iloc[0]
    assertions.append({"case": "later_reversal_never_overwrites", "pass": bool(rev.label == 0 and rev.hit_day == 1)})
    if not all(a["pass"] for a in assertions):
        raise AssertionError(assertions)
    return {"status": "pass", "assertions": assertions}


def diagnostics(raw: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, Any]]:
    ledgers = []
    summary: dict[str, Any] = {}
    raw = raw.sort_values(["code", "ymd"]).reset_index(drop=True)
    for horizon in formal.HORIZONS:
        required = [f"ret_close_{horizon}", f"down_exc_{horizon}", f"up_exc_{horizon}", "atr14", "c"]
        valid = raw[required].notna().all(axis=1)
        frame = raw.loc[valid].copy()
        first = first_passage(frame, horizon)
        old = formal.labels(frame, horizon)
        ledger = frame[["code", "ymd"]].reset_index(drop=True)
        first = first.reset_index(drop=True)
        ledger["horizon"] = horizon
        ledger["label"] = first.label.map(CLASS_NAMES)
        ledger["label_id"] = first.label
        ledger["outcome_kind"] = first.outcome_kind
        ledger["hit_day"] = first.hit_day
        ledger["down_fraction"] = first.down_fraction
        ledger["up_fraction"] = first.up_fraction
        ledger["legacy_close_confirmed_label"] = pd.Series(old).map(CLASS_NAMES)
        ledgers.append(ledger)
        transition = pd.crosstab(
            ledger["legacy_close_confirmed_label"], ledger["label"], dropna=False
        ).reindex(index=list(CLASS_NAMES.values()), columns=list(CLASS_NAMES.values()), fill_value=0)
        hit_diag = (
            ledger.groupby(["outcome_kind", "hit_day"], dropna=False).size().rename("n").reset_index().to_dict("records")
        )
        counts = ledger.label.value_counts().reindex(CLASS_NAMES.values(), fill_value=0)
        summary[str(horizon)] = {
            "n": int(len(ledger)),
            "class_counts": {str(k): int(v) for k, v in counts.items()},
            "class_shares": {str(k): float(v / len(ledger)) for k, v in counts.items()},
            "ambiguity_count": int((ledger.outcome_kind == "neutral_path_ambiguous").sum()),
            "ambiguity_share": float((ledger.outcome_kind == "neutral_path_ambiguous").mean()),
            "transition_matrix_legacy_rows_first_passage_columns": transition.to_dict(orient="index"),
            "hit_day_diagnostics": hit_diag,
        }
    return pd.concat(ledgers, ignore_index=True), summary


def run(input_path: Path, output_root: Path) -> Path:
    tests = self_tests()
    raw = pd.read_parquet(input_path)
    label_ledger, label_summary = diagnostics(raw)
    original_labels = formal.labels
    original_axis = formal.AXIS_ID
    try:
        formal.labels = labels
        formal.AXIS_ID = AXIS_ID
        out = formal.run(input_path, output_root)
    finally:
        formal.labels = original_labels
        formal.AXIS_ID = original_axis
    ledger_path = out / "first_passage_label_ledger.parquet"
    summary_path = out / "label_diagnostics.json"
    tests_path = out / "label_self_tests.json"
    label_ledger.to_parquet(ledger_path, index=False)
    formal.dump(summary_path, label_summary)
    formal.dump(tests_path, tests)
    compare_path = out / "compare.json"
    compare = json.loads(compare_path.read_text(encoding="utf-8"))
    compare["schema_version"] = AXIS_ID + ".compare.v1"
    compare["label_contract"] = LABEL_CONTRACT
    compare["label_diagnostics"] = label_summary
    compare["label_artifacts"] = {
        "ledger": {"path": str(ledger_path), "sha256": formal.sha(ledger_path)},
        "diagnostics": {"path": str(summary_path), "sha256": formal.sha(summary_path)},
        "self_tests": {"path": str(tests_path), "sha256": formal.sha(tests_path)},
    }
    compare["comparison_policy"] = "do_not_compare_Brier_across_label_contracts; beat own constant and fixed gates"
    formal.dump(compare_path, compare)
    formal.dump(out / "_ARTIFACT_COMPLETE.json", {"complete": True, "compare": str(compare_path), "compare_sha256": formal.sha(compare_path)})
    return out


def run_labels_only(input_path: Path, output_root: Path) -> Path:
    tests = self_tests()
    raw = pd.read_parquet(input_path)
    label_ledger, label_summary = diagnostics(raw)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out = output_root / f"{stamp}-{AXIS_ID}-labels"
    out.mkdir(parents=True, exist_ok=False)
    ledger_path = out / "first_passage_label_ledger.parquet"
    label_ledger.to_parquet(ledger_path, index=False)
    formal.dump(out / "label_diagnostics.json", label_summary)
    formal.dump(out / "label_self_tests.json", tests)
    payload = {
        "schema_version": AXIS_ID + ".labels.v1",
        "artifact_role": "authoritative_label_diagnostics",
        "source": {"path": str(input_path), "sha256": formal.sha(input_path)},
        "label_contract": LABEL_CONTRACT,
        "label_diagnostics": label_summary,
        "artifacts": {
            "ledger": {"path": str(ledger_path), "sha256": formal.sha(ledger_path)},
            "diagnostics": {"path": str(out / "label_diagnostics.json"), "sha256": formal.sha(out / "label_diagnostics.json")},
            "self_tests": {"path": str(out / "label_self_tests.json"), "sha256": formal.sha(out / "label_self_tests.json")},
        },
        "boundary": {"owner": "TRADEX", "meemee_changed": False, "runtime_db_write": False, "production_ranking_changed": False},
    }
    formal.dump(out / "label_compare.json", payload)
    formal.dump(out / "_ARTIFACT_COMPLETE.json", {"complete": True, "label_compare": str(out / "label_compare.json"), "sha256": formal.sha(out / "label_compare.json")})
    return out


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True, type=Path)
    parser.add_argument("--output-root", type=Path, default=Path(r"G:\Tradex\fp_order_v1"))
    parser.add_argument("--labels-only", action="store_true")
    args = parser.parse_args()
    print(run_labels_only(args.input, args.output_root) if args.labels_only else run(args.input, args.output_root))


if __name__ == "__main__":
    main()
