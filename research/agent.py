from __future__ import annotations

from dataclasses import asdict, dataclass, field
from hashlib import sha1
import json
import math
import shutil
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from research.storage import ResearchPaths, now_utc_iso, parse_date, read_csv, write_csv, write_json
from research.study_build import (
    _assign_regime_and_clusters,
    _assign_universe,
    _context_bias_from_bars,
    _load_snapshot as _load_snapshot_with_industry,
    _merge_context,
    _resample_bars,
)


@dataclass(frozen=True)
class AgentWalkforwardConfig:
    min_train_years: int = 10
    valid_months: int = 24
    test_months: int = 12
    step_months: int = 12


@dataclass(frozen=True)
class AgentConfig:
    cost_enabled: bool = False
    entry_mode: str = "close"
    max_hold_days: int = 60
    horizons: tuple[int, ...] = (1, 3, 5, 10, 20, 40, 60)
    priority: tuple[str, ...] = ("skip", "buy", "sell", "takeprofit", "stop", "failure", "add", "hedge")
    walkforward: AgentWalkforwardConfig = field(default_factory=AgentWalkforwardConfig)
    buy_sell_min_samples: int = 80
    buy_sell_positive_fold_ratio: float = 0.55
    buy_sell_max_p90_close_mae: float = 0.08
    buy_sell_max_median_hold: float = 30.0
    skip_improvement_min: float = 0.002
    failure_rate_ratio_min: float = 1.5
    failure_min_repro_folds: int = 3
    failure_min_loser_share: float = 0.25
    takeprofit_thresholds: tuple[float, ...] = (0.05, 0.08, 0.10, 0.15)
    stop_thresholds: tuple[float, ...] = (0.03, 0.05, 0.08)


@dataclass(frozen=True)
class Hypothesis:
    hypothesis_id: str
    theme: str
    stage: str
    side: str
    name: str
    tokens: tuple[str, ...]
    conditions: tuple[dict[str, Any], ...]
    notes: str = ""


@dataclass(frozen=True)
class AgentPaths:
    snapshot_id: str
    root: Path
    specs_dir: Path
    features_dir: Path
    labels_dir: Path
    experiments_dir: Path
    results_dir: Path
    results_history_dir: Path
    rulebooks_dir: Path
    candidates_dir: Path
    handoff_dir: Path
    cache_dir: Path
    state_file: Path
    manifest_file: Path
    rule_cards_json: Path
    rule_cards_csv: Path
    failure_cards_json: Path
    failure_cards_csv: Path
    cycle_manifest_dir: Path

    @classmethod
    def build(cls, paths: ResearchPaths, snapshot_id: str) -> "AgentPaths":
        root = paths.workspace_root / "agent_research" / str(snapshot_id).strip()
        results_dir = root / "04_results"
        candidates_dir = root / "06_candidates"
        return cls(
            snapshot_id=str(snapshot_id).strip(),
            root=root,
            specs_dir=root / "00_specs",
            features_dir=root / "01_features",
            labels_dir=root / "02_labels",
            experiments_dir=root / "03_experiments",
            results_dir=results_dir,
            results_history_dir=results_dir / "history",
            rulebooks_dir=root / "05_rulebooks",
            candidates_dir=candidates_dir,
            handoff_dir=root / "07_handoff",
            cache_dir=root / "_cache",
            state_file=root / "state.json",
            manifest_file=root / "manifest.json",
            rule_cards_json=candidates_dir / "rule_cards.json",
            rule_cards_csv=candidates_dir / "rule_cards.csv",
            failure_cards_json=candidates_dir / "failure_cards.json",
            failure_cards_csv=candidates_dir / "failure_cards.csv",
            cycle_manifest_dir=root / "_cycle_manifests",
        )

    def ensure_dirs(self) -> None:
        for path in (
            self.root,
            self.specs_dir,
            self.features_dir,
            self.labels_dir,
            self.experiments_dir,
            self.results_dir,
            self.results_history_dir,
            self.rulebooks_dir,
            self.candidates_dir,
            self.handoff_dir,
            self.cache_dir,
            self.cycle_manifest_dir,
        ):
            path.mkdir(parents=True, exist_ok=True)


FEATURE_DEFINITIONS: tuple[dict[str, str], ...] = (
    {"name": "ma7", "category": "ma", "description": "7-day moving average."},
    {"name": "ma20", "category": "ma", "description": "20-day moving average."},
    {"name": "ma60", "category": "ma", "description": "60-day moving average."},
    {"name": "ma100", "category": "ma", "description": "100-day moving average."},
    {"name": "ma200", "category": "ma", "description": "200-day moving average."},
    {"name": "dev_ma7", "category": "ma", "description": "Close deviation from MA7."},
    {"name": "dev_ma20", "category": "ma", "description": "Close deviation from MA20."},
    {"name": "dev_ma60", "category": "ma", "description": "Close deviation from MA60."},
    {"name": "dev_ma100", "category": "ma", "description": "Close deviation from MA100."},
    {"name": "dev_ma200", "category": "ma", "description": "Close deviation from MA200."},
    {"name": "ma7_slope_5", "category": "ma", "description": "MA7 slope over 5 sessions."},
    {"name": "ma20_slope_5", "category": "ma", "description": "MA20 slope over 5 sessions."},
    {"name": "ma60_slope_10", "category": "ma", "description": "MA60 slope over 10 sessions."},
    {"name": "ma20_slope_delta", "category": "ma", "description": "Change in MA20 slope."},
    {"name": "cnt_above_ma7_20", "category": "state", "description": "Days above MA7 in last 20 sessions."},
    {"name": "cnt_below_ma7_20", "category": "state", "description": "Days below MA7 in last 20 sessions."},
    {"name": "cnt_above_ma20_20", "category": "state", "description": "Days above MA20 in last 20 sessions."},
    {"name": "cnt_below_ma20_20", "category": "state", "description": "Days below MA20 in last 20 sessions."},
    {"name": "cnt_above_ma60_20", "category": "state", "description": "Days above MA60 in last 20 sessions."},
    {"name": "cnt_below_ma60_20", "category": "state", "description": "Days below MA60 in last 20 sessions."},
    {"name": "ma_cluster_width", "category": "ma", "description": "Width between the fastest and slowest MA."},
    {"name": "ma_spread_score", "category": "ma", "description": "Average MA separation score."},
    {"name": "body_size_pct", "category": "candle", "description": "Real body size relative to close."},
    {"name": "upper_wick_ratio", "category": "candle", "description": "Upper wick relative to candle range."},
    {"name": "lower_wick_ratio", "category": "candle", "description": "Lower wick relative to candle range."},
    {"name": "is_koma", "category": "candle", "description": "Small-body indecision candle."},
    {"name": "is_cross", "category": "candle", "description": "Cross / doji style candle."},
    {"name": "is_bull_engulf", "category": "pattern", "description": "Bullish engulfing pattern."},
    {"name": "is_bear_engulf", "category": "pattern", "description": "Bearish engulfing pattern."},
    {"name": "is_bull_harami", "category": "pattern", "description": "Bullish harami pattern."},
    {"name": "is_bear_harami", "category": "pattern", "description": "Bearish harami pattern."},
    {"name": "all_erase_5", "category": "pattern", "description": "5-day full erase pattern."},
    {"name": "all_return_10", "category": "pattern", "description": "10-day full return pattern."},
    {"name": "high_update_20", "category": "breakout", "description": "Close broke the prior 20-day high."},
    {"name": "low_update_20", "category": "breakout", "description": "Close broke the prior 20-day low."},
    {"name": "box_position_20", "category": "box", "description": "Close location inside 20-day range."},
    {"name": "box_position_60", "category": "box", "description": "Close location inside 60-day range."},
    {"name": "dist_recent_high_20", "category": "box", "description": "Distance to prior 20-day high."},
    {"name": "dist_recent_low_20", "category": "box", "description": "Distance to prior 20-day low."},
    {"name": "vol_ratio20", "category": "volume", "description": "Volume divided by 20-day average volume."},
    {"name": "atr14_ratio", "category": "volatility", "description": "ATR14 divided by close."},
    {"name": "atr_pct60", "category": "volatility", "description": "ATR14 percentile rank over 60 sessions."},
    {"name": "weekly_context_bias", "category": "regime", "description": "Weekly bias projected to daily bars."},
    {"name": "monthly_context_bias", "category": "regime", "description": "Monthly bias projected to daily bars."},
    {"name": "regime_key", "category": "regime", "description": "Combined market trend and volatility regime."},
    {"name": "n_shape_up", "category": "pattern", "description": "Simplified N-shape upward context."},
    {"name": "inverse_n_shape", "category": "pattern", "description": "Simplified inverse N-shape downward context."},
    {"name": "state_7up7", "category": "state", "description": "Seven straight closes above MA7."},
    {"name": "state_20up20", "category": "state", "description": "Twenty straight closes above MA20."},
    {"name": "state_60up60", "category": "state", "description": "Sixty straight closes above MA60."},
    {"name": "state_7down7", "category": "state", "description": "Seven straight closes below MA7."},
    {"name": "state_20down20", "category": "state", "description": "Twenty straight closes below MA20."},
    {"name": "state_60down60", "category": "state", "description": "Sixty straight closes below MA60."},
    {"name": "phase_initial", "category": "phase", "description": "Early / initial stage."},
    {"name": "phase_mid", "category": "phase", "description": "Middle stage."},
    {"name": "phase_late", "category": "phase", "description": "Late stage."},
    {"name": "phase_overheated", "category": "phase", "description": "Overheated stage."},
)


def _feature_frame() -> pd.DataFrame:
    return pd.DataFrame(list(FEATURE_DEFINITIONS))


def agent_config_to_dict(config: AgentConfig) -> dict[str, Any]:
    payload = asdict(config)
    payload["horizons"] = list(config.horizons)
    payload["priority"] = list(config.priority)
    payload["takeprofit_thresholds"] = list(config.takeprofit_thresholds)
    payload["stop_thresholds"] = list(config.stop_thresholds)
    return payload


def default_agent_config() -> AgentConfig:
    return AgentConfig()


def _load_json_list(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    payload = json.loads(path.read_text(encoding="utf-8"))
    return payload if isinstance(payload, list) else []


def _write_json_list(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(rows, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")


def _write_if_missing(path: Path, text: str) -> None:
    if path.exists():
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _safe_read_json(path: Path, default: dict[str, Any]) -> dict[str, Any]:
    if not path.exists():
        return dict(default)
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return dict(default)
    return payload if isinstance(payload, dict) else dict(default)


def _tag_from_threshold(value: float) -> str:
    return f"{int(round(value * 100)):02d}"


def _hypothesis_to_state_row(hypothesis: Hypothesis) -> dict[str, Any]:
    return {
        "hypothesis_id": hypothesis.hypothesis_id,
        "theme": hypothesis.theme,
        "stage": hypothesis.stage,
        "side": hypothesis.side,
        "name": hypothesis.name,
        "tokens": list(hypothesis.tokens),
        "conditions": [dict(item) for item in hypothesis.conditions],
        "notes": hypothesis.notes,
        "status": "pending",
        "decision": None,
        "last_cycle_id": None,
        "history": [],
    }


def _seed_library() -> tuple[Hypothesis, ...]:
    return (
        Hypothesis(
            hypothesis_id="skip_box_mid_low_vol",
            theme="skip",
            stage="skip",
            side="both",
            name="Box center with dry volume",
            tokens=("state:box_mid", "state:volume_dry", "context:ma_compressed"),
            conditions=(
                {"column": "box_position_60", "op": "between", "value": [0.35, 0.65]},
                {"column": "vol_ratio20", "op": "<=", "value": 0.95},
                {"column": "ma_cluster_width", "op": "<=", "value": 0.08},
            ),
            notes="Skip candidate for low-range range-bound states.",
        ),
        Hypothesis(
            hypothesis_id="skip_noise_low_atr",
            theme="skip",
            stage="skip",
            side="both",
            name="Low ATR noise pocket",
            tokens=("state:low_atr", "change:small_body", "context:range"),
            conditions=(
                {"column": "atr_pct60", "op": "<=", "value": 0.40},
                {"column": "body_size_pct", "op": "<=", "value": 0.02},
                {"column": "box_position_20", "op": "between", "value": [0.25, 0.75]},
            ),
        ),
        Hypothesis(
            hypothesis_id="skip_overheated_upper_wick",
            theme="skip",
            stage="skip",
            side="both",
            name="Late overheated upper-wick zone",
            tokens=("state:overheated", "change:upper_wick", "context:late"),
            conditions=(
                {"column": "phase_overheated", "op": ">=", "value": 1.0},
                {"column": "upper_wick_ratio", "op": ">=", "value": 0.35},
                {"column": "box_position_20", "op": ">=", "value": 0.75},
            ),
        ),
        Hypothesis(
            hypothesis_id="buy_ma20_recovery_initial",
            theme="buy",
            stage="buy",
            side="long",
            name="MA20 recovery initial thrust",
            tokens=("state:ma20_recovery", "change:cross_up", "context:initial"),
            conditions=(
                {"column": "cross_up_ma20", "op": ">=", "value": 1.0},
                {"column": "phase_initial", "op": ">=", "value": 1.0},
                {"column": "monthly_context_bias", "op": ">=", "value": -0.15},
            ),
        ),
        Hypothesis(
            hypothesis_id="buy_ma60_pullback_support",
            theme="buy",
            stage="buy",
            side="long",
            name="MA60 support pullback",
            tokens=("state:above_ma60", "change:lower_wick", "context:pullback"),
            conditions=(
                {"column": "close_above_ma60", "op": ">=", "value": 1.0},
                {"column": "lower_wick_ratio", "op": ">=", "value": 0.30},
                {"column": "box_position_20", "op": "<=", "value": 0.55},
            ),
        ),
        Hypothesis(
            hypothesis_id="buy_ma20_recovery_volume_expansion",
            theme="buy",
            stage="buy",
            side="long",
            name="MA20 recovery with volume expansion",
            tokens=("state:ma20_recovery", "change:cross_up", "context:initial", "context:volume_expand"),
            conditions=(
                {"column": "cross_up_ma20", "op": ">=", "value": 1.0},
                {"column": "phase_initial", "op": ">=", "value": 1.0},
                {"column": "vol_ratio20", "op": ">=", "value": 1.10},
                {"column": "monthly_context_bias", "op": ">=", "value": 0.0},
            ),
            notes="Second-wave buy candidate after initial MA20 recovery showed long hold times.",
        ),
        Hypothesis(
            hypothesis_id="buy_ma60_pullback_support_volume",
            theme="buy",
            stage="buy",
            side="long",
            name="MA60 pullback with support and volume",
            tokens=("state:above_ma60", "change:lower_wick", "context:pullback", "context:volume_support"),
            conditions=(
                {"column": "close_above_ma60", "op": ">=", "value": 1.0},
                {"column": "lower_wick_ratio", "op": ">=", "value": 0.35},
                {"column": "box_position_20", "op": "<=", "value": 0.40},
                {"column": "vol_ratio20", "op": ">=", "value": 1.00},
                {"column": "ma60_slope_10", "op": ">=", "value": 0.0},
            ),
            notes="Second-wave buy candidate to tighten MA60 pullback entries.",
        ),
        Hypothesis(
            hypothesis_id="sell_ma20_reject_under60",
            theme="sell",
            stage="sell",
            side="short",
            name="MA20 rejection under MA60",
            tokens=("state:below_ma60", "change:upper_wick", "context:rejection"),
            conditions=(
                {"column": "close_below_ma60", "op": ">=", "value": 1.0},
                {"column": "upper_wick_ratio", "op": ">=", "value": 0.30},
                {"column": "monthly_context_bias", "op": "<=", "value": 0.15},
            ),
        ),
        Hypothesis(
            hypothesis_id="sell_breakdown_box_lower",
            theme="sell",
            stage="sell",
            side="short",
            name="20-day low breakdown",
            tokens=("state:low_break", "change:trend_down", "context:range_exit"),
            conditions=(
                {"column": "low_update_20", "op": ">=", "value": 1.0},
                {"column": "ma20_slope_5", "op": "<=", "value": 0.0},
                {"column": "box_position_60", "op": "<=", "value": 0.35},
            ),
        ),
        Hypothesis(
            hypothesis_id="takeprofit_late_overheat",
            theme="takeprofit",
            stage="takeprofit",
            side="long",
            name="Take profit into overheated late phase",
            tokens=("state:overheated", "context:late", "exit:takeprofit"),
            conditions=(
                {"column": "phase_overheated", "op": ">=", "value": 1.0},
                {"column": "phase_late", "op": ">=", "value": 1.0},
            ),
        ),
        Hypothesis(
            hypothesis_id="takeprofit_box_upper_exhaustion",
            theme="takeprofit",
            stage="takeprofit",
            side="long",
            name="Take profit near upper box exhaustion",
            tokens=("state:box_upper", "change:upper_wick", "exit:takeprofit"),
            conditions=(
                {"column": "box_position_20", "op": ">=", "value": 0.80},
                {"column": "upper_wick_ratio", "op": ">=", "value": 0.25},
            ),
        ),
        Hypothesis(
            hypothesis_id="takeprofit_overheated_upper_wick",
            theme="takeprofit",
            stage="takeprofit",
            side="long",
            name="Take profit on overheated upper wick",
            tokens=("state:overheated", "change:upper_wick", "exit:takeprofit"),
            conditions=(
                {"column": "phase_overheated", "op": ">=", "value": 1.0},
                {"column": "upper_wick_ratio", "op": ">=", "value": 0.35},
                {"column": "box_position_20", "op": ">=", "value": 0.75},
            ),
            notes="Second-wave take-profit candidate to test faster exits on exhaustion.",
        ),
        Hypothesis(
            hypothesis_id="stop_cross_down_after_recovery",
            theme="stop",
            stage="stop",
            side="long",
            name="Stop on failed MA20 recovery",
            tokens=("state:ma20_fail", "change:cross_down", "exit:stop"),
            conditions=(
                {"column": "cross_down_ma20", "op": ">=", "value": 1.0},
                {"column": "monthly_context_bias", "op": "<=", "value": 0.10},
            ),
        ),
        Hypothesis(
            hypothesis_id="stop_box_break_lower",
            theme="stop",
            stage="stop",
            side="long",
            name="Stop on lower box break",
            tokens=("state:box_break", "change:range_exit", "exit:stop"),
            conditions=(
                {"column": "low_update_20", "op": ">=", "value": 1.0},
                {"column": "box_position_60", "op": "<=", "value": 0.30},
            ),
        ),
        Hypothesis(
            hypothesis_id="failure_dry_volume_under60",
            theme="failure",
            stage="failure",
            side="long",
            name="Dry volume under MA60",
            tokens=("state:below_ma60", "state:volume_dry", "failure:weak_signal"),
            conditions=(
                {"column": "close_below_ma60", "op": ">=", "value": 1.0},
                {"column": "vol_ratio20", "op": "<=", "value": 0.90},
            ),
        ),
        Hypothesis(
            hypothesis_id="failure_box_middle_late",
            theme="failure",
            stage="failure",
            side="long",
            name="Late phase in box middle",
            tokens=("state:box_mid", "context:late", "failure:value_thin"),
            conditions=(
                {"column": "box_position_60", "op": "between", "value": [0.35, 0.65]},
                {"column": "phase_late", "op": ">=", "value": 1.0},
            ),
        ),
        Hypothesis(
            hypothesis_id="failure_pullback_monthly_negative",
            theme="failure",
            stage="failure",
            side="long",
            name="Pullback buy against monthly bias",
            tokens=("state:above_ma60", "change:lower_wick", "context:pullback", "failure:monthly_conflict"),
            conditions=(
                {"column": "close_above_ma60", "op": ">=", "value": 1.0},
                {"column": "lower_wick_ratio", "op": ">=", "value": 0.30},
                {"column": "monthly_context_bias", "op": "<=", "value": -0.05},
            ),
            notes="Second-wave failure candidate from buy holds with weak higher-timeframe support.",
        ),
        Hypothesis(
            hypothesis_id="failure_box_middle_late_dry",
            theme="failure",
            stage="failure",
            side="long",
            name="Late box middle with dry volume",
            tokens=("state:box_mid", "context:late", "state:volume_dry", "failure:value_thin"),
            conditions=(
                {"column": "box_position_60", "op": "between", "value": [0.40, 0.60]},
                {"column": "phase_late", "op": ">=", "value": 1.0},
                {"column": "vol_ratio20", "op": "<=", "value": 0.95},
            ),
            notes="Second-wave failure candidate to sharpen range-bound late-phase losses.",
        ),
    )


def _initial_state(snapshot_id: str, config: AgentConfig) -> dict[str, Any]:
    hypotheses = [_hypothesis_to_state_row(item) for item in _seed_library()]
    return {
        "snapshot_id": snapshot_id,
        "created_at": now_utc_iso(),
        "updated_at": now_utc_iso(),
        "next_cycle_number": 1,
        "config": agent_config_to_dict(config),
        "hypotheses": hypotheses,
    }


def _load_or_init_state(agent_paths: AgentPaths, config: AgentConfig) -> dict[str, Any]:
    state = _safe_read_json(agent_paths.state_file, {})
    if not state:
        state = _initial_state(agent_paths.snapshot_id, config)
    existing = {str(row.get("hypothesis_id")): row for row in state.get("hypotheses", []) if isinstance(row, dict)}
    merged: list[dict[str, Any]] = []
    for hypothesis in _seed_library():
        row = _hypothesis_to_state_row(hypothesis)
        if hypothesis.hypothesis_id in existing:
            row.update(existing[hypothesis.hypothesis_id])
        merged.append(row)
    state["snapshot_id"] = agent_paths.snapshot_id
    state["config"] = agent_config_to_dict(config)
    state["hypotheses"] = merged
    state["updated_at"] = now_utc_iso()
    write_json(agent_paths.state_file, state)
    return state


def _snapshot_meta(paths: ResearchPaths, snapshot_id: str) -> dict[str, Any]:
    manifest_path = paths.snapshot_dir(snapshot_id) / "manifest.json"
    return _safe_read_json(manifest_path, {"snapshot_id": snapshot_id})


def _render_feature_catalog_md() -> str:
    lines = ["# Feature Catalog", "", "Agent research uses the following fixed feature catalog.", ""]
    for row in FEATURE_DEFINITIONS:
        lines.append(f"- `{row['name']}`: {row['description']}")
    lines.append("")
    return "\n".join(lines)


def _render_label_catalog_md() -> str:
    return "\n".join(
        [
            "# Label Catalog",
            "",
            "- `buy_candidate`: long-side signal candidate evaluated by walk-forward expectancy.",
            "- `sell_candidate`: short-side signal candidate evaluated by walk-forward expectancy.",
            "- `takeprofit_candidate`: close-based exit overlay relative to 60-day baseline exit.",
            "- `stop_candidate`: close-based stop overlay relative to 60-day baseline exit.",
            "- `skip_candidate`: avoid-entry state where residual expectancy improves after exclusion.",
            "- `failure_reason`: condition that appears meaningfully more often in losing cases than winners.",
            "",
        ]
    )


def _render_goal_md(snapshot_meta: dict[str, Any]) -> str:
    start = snapshot_meta.get("daily_start", "unknown")
    end = snapshot_meta.get("daily_end", "unknown")
    codes = snapshot_meta.get("daily_codes", "unknown")
    return "\n".join(
        [
            "# Research Goal",
            "",
            "MeeMee本体へ反映せず、銘柄別・局面別の売買辞書を研究専用で蓄積する。",
            "",
            f"- 対象 snapshot: `{snapshot_meta.get('snapshot_id', 'unknown')}`",
            f"- 対象銘柄数: `{codes}`",
            f"- 対象期間: `{start}` - `{end}`",
            "- 出力対象: 見送り / 買い / 売り / 利確 / 損切り / 失敗理由の辞書",
            "- 研究原則: 再現しないルールは捨て、勝てない理由も同じ重みで残す",
            "",
        ]
    )


def _render_fixed_assumptions_md(config: AgentConfig) -> str:
    return "\n".join(
        [
            "# Fixed Assumptions",
            "",
            "- 価格は `daily.csv` の `close` を調整後終値として扱う。",
            "- エントリーは当日終値固定、保有は最大60営業日。",
            "- 売買コスト、逆日歩、イベント要因、ギャップ依存エントリーは初期研究から除外する。",
            "- MeeMee本体、`app/`、`published/`、本番DBには書き込まない。",
            f"- walk-forward: train >= {config.walkforward.min_train_years} years, valid {config.walkforward.valid_months} months, test {config.walkforward.test_months} months, step {config.walkforward.step_months} months.",
            "",
        ]
    )


def _render_metrics_md(config: AgentConfig) -> str:
    return "\n".join(
        [
            "# Metrics",
            "",
            "## Buy / Sell Adoption",
            "",
            f"- samples >= {config.buy_sell_min_samples}",
            "- pooled expectancy > 0",
            "- median fold expectancy > 0",
            f"- positive-fold ratio >= {config.buy_sell_positive_fold_ratio:.2f}",
            f"- p90 close-MAE <= {config.buy_sell_max_p90_close_mae:.2f}",
            f"- median hold <= {config.buy_sell_max_median_hold:.0f}",
            "",
            "## Skip Adoption",
            "",
            f"- residual expectancy improvement >= {config.skip_improvement_min * 10000:.0f} bp",
            "- long/short both non-advantage, or move potential is too thin",
            "",
            "## Failure Reason Adoption",
            "",
            f"- loser occurrence / winner occurrence >= {config.failure_rate_ratio_min:.2f}",
            f"- reproduced in >= {config.failure_min_repro_folds} folds",
            f"- loser share >= {config.failure_min_loser_share:.2f}",
            "",
            "## Takeprofit / Stop Adoption",
            "",
            "- improvement versus 60-day close exit in expectancy, or",
            "- p90 close-MAE improves by at least 15%",
            "",
        ]
    )


def _ensure_results_placeholders(agent_paths: AgentPaths) -> None:
    placeholders = {
        agent_paths.results_dir / "progress.md": "# Progress\n\n初期化直後です。\n",
        agent_paths.results_dir / "rules_delta.md": "# Rules Delta\n\nまだ差分はありません。\n",
        agent_paths.results_dir / "failures_delta.md": "# Failures Delta\n\nまだ差分はありません。\n",
        agent_paths.results_dir / "backlog.md": "# Backlog\n\n初期 backlog を作成済みです。\n",
        agent_paths.results_dir / "executive_summary.md": "# Executive Summary\n\nまだ研究サイクルは実行されていません。\n",
        agent_paths.candidates_dir / "best_patterns_global.md": "# Best Patterns Global\n\nまだ採用ルールはありません。\n",
        agent_paths.candidates_dir / "best_patterns_by_regime.md": "# Best Patterns By Regime\n\nまだ採用ルールはありません。\n",
        agent_paths.candidates_dir / "best_patterns_by_ticker.md": "# Best Patterns By Ticker\n\nまだ採用ルールはありません。\n",
        agent_paths.handoff_dir / "meemee_integration_candidates.md": "# MeeMee Integration Candidates\n\n研究専用フェーズのため未記入です。\n",
        agent_paths.handoff_dir / "open_questions.md": "# Open Questions\n\n未解決事項はまだありません。\n",
    }
    for path, content in placeholders.items():
        _write_if_missing(path, content)


def run_agent_init(
    paths: ResearchPaths,
    snapshot_id: str,
    config: AgentConfig | None = None,
) -> dict[str, Any]:
    resolved_config = config or default_agent_config()
    agent_paths = AgentPaths.build(paths, snapshot_id)
    agent_paths.ensure_dirs()
    snapshot_meta = _snapshot_meta(paths, snapshot_id)
    write_json(
        agent_paths.manifest_file,
        {
            "snapshot_id": snapshot_id,
            "initialized_at": now_utc_iso(),
            "config": agent_config_to_dict(resolved_config),
            "snapshot_meta": snapshot_meta,
        },
    )
    _load_or_init_state(agent_paths, resolved_config)
    _write_if_missing(agent_paths.specs_dir / "research_goal.md", _render_goal_md(snapshot_meta))
    _write_if_missing(agent_paths.specs_dir / "fixed_assumptions.md", _render_fixed_assumptions_md(resolved_config))
    _write_if_missing(agent_paths.specs_dir / "metrics.md", _render_metrics_md(resolved_config))
    _write_if_missing(agent_paths.features_dir / "feature_catalog.md", _render_feature_catalog_md())
    if not (agent_paths.features_dir / "feature_defs.csv").exists():
        write_csv(agent_paths.features_dir / "feature_defs.csv", _feature_frame())
    _write_if_missing(agent_paths.labels_dir / "label_catalog.md", _render_label_catalog_md())
    _ensure_results_placeholders(agent_paths)
    if not agent_paths.rule_cards_json.exists():
        _write_json_list(agent_paths.rule_cards_json, [])
    if not agent_paths.failure_cards_json.exists():
        _write_json_list(agent_paths.failure_cards_json, [])
    return {
        "ok": True,
        "snapshot_id": snapshot_id,
        "root": str(agent_paths.root),
        "specs": [
            str(agent_paths.specs_dir / "research_goal.md"),
            str(agent_paths.specs_dir / "fixed_assumptions.md"),
            str(agent_paths.specs_dir / "metrics.md"),
        ],
    }


def _rolling_percentile(series: pd.Series, window: int) -> pd.Series:
    values = pd.to_numeric(series, errors="coerce")
    return values.rolling(window, min_periods=max(5, window // 4)).apply(
        lambda arr: float(np.sum(arr <= arr[-1]) / len(arr)) if len(arr) else 0.5,
        raw=True,
    )


def _consecutive_true(series: pd.Series) -> pd.Series:
    values = series.fillna(0).astype(int).to_numpy(dtype=int)
    out = np.zeros(len(values), dtype=float)
    run = 0
    for idx, value in enumerate(values):
        if value:
            run += 1
        else:
            run = 0
        out[idx] = float(run)
    return pd.Series(out, index=series.index)


def _build_daily_features(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.sort_values(["code", "date"]).copy()
    grouped = out.groupby("code", sort=False)
    out["event_date"] = out["date"]
    out["prev_close"] = grouped["close"].shift(1)
    out["ret1"] = out["close"] / (out["prev_close"] + 1e-12) - 1.0
    tr = np.maximum.reduce(
        [
            (out["high"] - out["low"]).to_numpy(dtype=float),
            (out["high"] - out["prev_close"]).abs().to_numpy(dtype=float),
            (out["low"] - out["prev_close"]).abs().to_numpy(dtype=float),
        ]
    )
    out["tr"] = tr
    out["atr"] = grouped["tr"].transform(lambda series: series.rolling(14, min_periods=4).mean())
    out["atr14_ratio"] = out["atr"] / (out["close"].abs() + 1e-12)
    out["atr_pct60"] = grouped["atr14_ratio"].transform(lambda series: _rolling_percentile(series, 60))

    for window in (7, 20, 60, 100, 200):
        out[f"ma{window}"] = grouped["close"].transform(lambda series, w=window: series.rolling(w, min_periods=max(3, w // 4)).mean())
        out[f"dev_ma{window}"] = out["close"] / (out[f"ma{window}"] + 1e-12) - 1.0

    out["close_above_ma60"] = (out["close"] >= out["ma60"]).astype(float)
    out["close_below_ma60"] = (out["close"] <= out["ma60"]).astype(float)
    out["ma7_slope_5"] = out["ma7"] / (grouped["ma7"].shift(5) + 1e-12) - 1.0
    out["ma20_slope_5"] = out["ma20"] / (grouped["ma20"].shift(5) + 1e-12) - 1.0
    out["ma60_slope_10"] = out["ma60"] / (grouped["ma60"].shift(10) + 1e-12) - 1.0
    out["ma20_slope_delta"] = out["ma20_slope_5"] - grouped["ma20_slope_5"].shift(3)
    out["ma_slope_mid"] = out["ma20_slope_5"].fillna(0.0)

    out["above_ma7"] = (out["close"] >= out["ma7"]).astype(float)
    out["below_ma7"] = (out["close"] <= out["ma7"]).astype(float)
    out["above_ma20"] = (out["close"] >= out["ma20"]).astype(float)
    out["below_ma20"] = (out["close"] <= out["ma20"]).astype(float)
    out["above_ma60"] = (out["close"] >= out["ma60"]).astype(float)
    out["below_ma60"] = (out["close"] <= out["ma60"]).astype(float)
    out["cnt_above_ma7_20"] = grouped["above_ma7"].transform(lambda series: series.rolling(20, min_periods=4).sum())
    out["cnt_below_ma7_20"] = grouped["below_ma7"].transform(lambda series: series.rolling(20, min_periods=4).sum())
    out["cnt_above_ma20_20"] = grouped["above_ma20"].transform(lambda series: series.rolling(20, min_periods=4).sum())
    out["cnt_below_ma20_20"] = grouped["below_ma20"].transform(lambda series: series.rolling(20, min_periods=4).sum())
    out["cnt_above_ma60_20"] = grouped["above_ma60"].transform(lambda series: series.rolling(20, min_periods=4).sum())
    out["cnt_below_ma60_20"] = grouped["below_ma60"].transform(lambda series: series.rolling(20, min_periods=4).sum())
    out["consec_above_ma7"] = grouped["above_ma7"].transform(_consecutive_true)
    out["consec_above_ma20"] = grouped["above_ma20"].transform(_consecutive_true)
    out["consec_above_ma60"] = grouped["above_ma60"].transform(_consecutive_true)
    out["consec_below_ma7"] = grouped["below_ma7"].transform(_consecutive_true)
    out["consec_below_ma20"] = grouped["below_ma20"].transform(_consecutive_true)
    out["consec_below_ma60"] = grouped["below_ma60"].transform(_consecutive_true)
    out["state_7up7"] = (out["consec_above_ma7"] >= 7).astype(float)
    out["state_20up20"] = (out["consec_above_ma20"] >= 20).astype(float)
    out["state_60up60"] = (out["consec_above_ma60"] >= 60).astype(float)
    out["state_7down7"] = (out["consec_below_ma7"] >= 7).astype(float)
    out["state_20down20"] = (out["consec_below_ma20"] >= 20).astype(float)
    out["state_60down60"] = (out["consec_below_ma60"] >= 60).astype(float)

    ma_cols = ["ma7", "ma20", "ma60", "ma100", "ma200"]
    ma_stack = out[ma_cols]
    out["ma_cluster_width"] = (ma_stack.max(axis=1) - ma_stack.min(axis=1)) / (out["close"].abs() + 1e-12)
    out["ma_spread_score"] = (
        (out["ma7"] - out["ma20"]).abs()
        + (out["ma20"] - out["ma60"]).abs()
        + (out["ma60"] - out["ma100"]).abs()
        + (out["ma100"] - out["ma200"]).abs()
    ) / (4.0 * (out["close"].abs() + 1e-12))
    out["ma_align_score_local"] = np.where(
        (out["ma7"] > out["ma20"]) & (out["ma20"] > out["ma60"]),
        1.0,
        np.where((out["ma7"] < out["ma20"]) & (out["ma20"] < out["ma60"]), -1.0, 0.0),
    )

    candle_range = (out["high"] - out["low"]).replace(0.0, np.nan)
    body = out["close"] - out["open"]
    body_abs = body.abs()
    upper_wick = out["high"] - np.maximum(out["open"], out["close"])
    lower_wick = np.minimum(out["open"], out["close"]) - out["low"]
    out["body_size_pct"] = body_abs / (out["close"].abs() + 1e-12)
    out["upper_wick_ratio"] = (upper_wick / candle_range).fillna(0.0)
    out["lower_wick_ratio"] = (lower_wick / candle_range).fillna(0.0)
    out["is_koma"] = (body_abs <= candle_range.fillna(0.0) * 0.30).astype(float)
    out["is_cross"] = (body_abs <= candle_range.fillna(0.0) * 0.10).astype(float)

    prev_open = grouped["open"].shift(1)
    prev_close = grouped["close"].shift(1)
    prev_body_top = np.maximum(prev_open, prev_close)
    prev_body_bottom = np.minimum(prev_open, prev_close)
    curr_body_top = np.maximum(out["open"], out["close"])
    curr_body_bottom = np.minimum(out["open"], out["close"])
    out["is_bull_engulf"] = ((out["close"] > out["open"]) & (prev_close < prev_open) & (curr_body_top >= prev_body_top) & (curr_body_bottom <= prev_body_bottom)).astype(float)
    out["is_bear_engulf"] = ((out["close"] < out["open"]) & (prev_close > prev_open) & (curr_body_top >= prev_body_top) & (curr_body_bottom <= prev_body_bottom)).astype(float)
    out["is_bull_harami"] = ((out["close"] > out["open"]) & (prev_close < prev_open) & (curr_body_top <= prev_body_top) & (curr_body_bottom >= prev_body_bottom)).astype(float)
    out["is_bear_harami"] = ((out["close"] < out["open"]) & (prev_close > prev_open) & (curr_body_top <= prev_body_top) & (curr_body_bottom >= prev_body_bottom)).astype(float)

    close_shift5 = grouped["close"].shift(5)
    close_shift10 = grouped["close"].shift(10)
    max5 = grouped["high"].transform(lambda series: series.rolling(5, min_periods=3).max())
    min10 = grouped["low"].transform(lambda series: series.rolling(10, min_periods=4).min())
    out["all_erase_5"] = (((out["close"] / (close_shift5 + 1e-12)) - 1.0).abs() <= 0.01).astype(float) * (((max5 / (close_shift5 + 1e-12)) - 1.0) >= 0.04).astype(float)
    out["all_return_10"] = (((out["close"] / (close_shift10 + 1e-12)) - 1.0).abs() <= 0.01).astype(float) * (((min10 / (close_shift10 + 1e-12)) - 1.0) <= -0.04).astype(float)

    out["high20_prev"] = grouped["high"].transform(lambda series: series.shift(1).rolling(20, min_periods=4).max())
    out["low20_prev"] = grouped["low"].transform(lambda series: series.shift(1).rolling(20, min_periods=4).min())
    out["high60_prev"] = grouped["high"].transform(lambda series: series.shift(1).rolling(60, min_periods=10).max())
    out["low60_prev"] = grouped["low"].transform(lambda series: series.shift(1).rolling(60, min_periods=10).min())
    out["high_update_20"] = (out["close"] >= out["high20_prev"]).astype(float)
    out["low_update_20"] = (out["close"] <= out["low20_prev"]).astype(float)
    out["box_position_20"] = ((out["close"] - out["low20_prev"]) / ((out["high20_prev"] - out["low20_prev"]).replace(0.0, np.nan))).fillna(0.5)
    out["box_position_60"] = ((out["close"] - out["low60_prev"]) / ((out["high60_prev"] - out["low60_prev"]).replace(0.0, np.nan))).fillna(0.5)
    out["dist_recent_high_20"] = out["close"] / (out["high20_prev"] + 1e-12) - 1.0
    out["dist_recent_low_20"] = out["close"] / (out["low20_prev"] + 1e-12) - 1.0

    out["vol_ma20"] = grouped["volume"].transform(lambda series: series.rolling(20, min_periods=4).mean())
    out["vol_ratio20"] = out["volume"] / (out["vol_ma20"] + 1e-12)
    out["liq_med20"] = (out["close"] * out["volume"]).groupby(out["code"], sort=False).transform(lambda series: series.rolling(20, min_periods=4).median())

    out["ret5"] = out["close"] / (grouped["close"].shift(5) + 1e-12) - 1.0
    out["ret10"] = out["close"] / (grouped["close"].shift(10) + 1e-12) - 1.0
    out["ret20"] = out["close"] / (grouped["close"].shift(20) + 1e-12) - 1.0
    out["n_shape_up"] = ((out["ret20"] >= 0.05) & (out["ret10"] <= 0.00) & (out["ret5"] >= 0.02)).astype(float)
    out["inverse_n_shape"] = ((out["ret20"] <= -0.05) & (out["ret10"] >= 0.00) & (out["ret5"] <= -0.02)).astype(float)

    prev_close_ma20 = grouped["ma20"].shift(1)
    prev_close_ma60 = grouped["ma60"].shift(1)
    prev_close_val = grouped["close"].shift(1)
    out["cross_up_ma20"] = ((out["close"] >= out["ma20"]) & (prev_close_val < prev_close_ma20)).astype(float)
    out["cross_down_ma20"] = ((out["close"] <= out["ma20"]) & (prev_close_val > prev_close_ma20)).astype(float)
    out["cross_up_ma60"] = ((out["close"] >= out["ma60"]) & (prev_close_val < prev_close_ma60)).astype(float)
    out["cross_down_ma60"] = ((out["close"] <= out["ma60"]) & (prev_close_val > prev_close_ma60)).astype(float)

    out["phase_overheated"] = ((out["dev_ma20"] >= 0.12) | (out["dev_ma60"] >= 0.20) | ((out["box_position_20"] >= 0.90) & (out["atr_pct60"] >= 0.80))).astype(float)
    out["phase_initial"] = ((out["cross_up_ma20"] >= 1.0) | (out["cross_down_ma20"] >= 1.0) | ((out["consec_above_ma20"] <= 5) & (out["ma20_slope_delta"].abs() >= 0.002))).astype(float)
    out["phase_late"] = (((out["consec_above_ma20"] >= 15) | (out["consec_below_ma20"] >= 15) | (out["box_position_60"] >= 0.80) | (out["box_position_60"] <= 0.20)) & (out["phase_overheated"] < 1.0)).astype(float)
    out["phase_mid"] = (((out["phase_initial"] < 1.0) & (out["phase_late"] < 1.0) & (out["phase_overheated"] < 1.0))).astype(float)
    return out


def _future_close_outcomes_for_code(frame: pd.DataFrame, config: AgentConfig) -> pd.DataFrame:
    out = frame.sort_values("event_date").copy()
    close = out["close"].to_numpy(dtype=float)
    n = len(out)
    data: dict[str, np.ndarray] = {}
    for horizon in config.horizons:
        data[f"long_ret_h{horizon}"] = np.full(n, np.nan, dtype=float)
        data[f"short_ret_h{horizon}"] = np.full(n, np.nan, dtype=float)
        data[f"long_close_mae_h{horizon}"] = np.full(n, np.nan, dtype=float)
        data[f"short_close_mae_h{horizon}"] = np.full(n, np.nan, dtype=float)
        data[f"long_close_mfe_h{horizon}"] = np.full(n, np.nan, dtype=float)
        data[f"short_close_mfe_h{horizon}"] = np.full(n, np.nan, dtype=float)
        data[f"hold_days_h{horizon}"] = np.full(n, np.nan, dtype=float)

    for threshold in config.takeprofit_thresholds:
        tag = _tag_from_threshold(threshold)
        for side in ("long", "short"):
            data[f"{side}_tp_{tag}_ret"] = np.full(n, np.nan, dtype=float)
            data[f"{side}_tp_{tag}_hold"] = np.full(n, np.nan, dtype=float)
            data[f"{side}_tp_{tag}_mae"] = np.full(n, np.nan, dtype=float)

    for threshold in config.stop_thresholds:
        tag = _tag_from_threshold(threshold)
        for side in ("long", "short"):
            data[f"{side}_stop_{tag}_ret"] = np.full(n, np.nan, dtype=float)
            data[f"{side}_stop_{tag}_hold"] = np.full(n, np.nan, dtype=float)
            data[f"{side}_stop_{tag}_mae"] = np.full(n, np.nan, dtype=float)

    max_hold = int(config.max_hold_days)
    for index in range(n):
        max_future = min(max_hold, n - index - 1)
        if max_future <= 0:
            continue
        entry = float(close[index])
        path = close[index + 1 : index + 1 + max_future]
        rel = path / entry - 1.0
        for horizon in config.horizons:
            if max_future < horizon:
                continue
            window = rel[:horizon]
            final_rel = float(window[-1])
            data[f"long_ret_h{horizon}"][index] = final_rel
            data[f"short_ret_h{horizon}"][index] = -final_rel
            data[f"long_close_mae_h{horizon}"][index] = max(0.0, float(-window.min()))
            data[f"short_close_mae_h{horizon}"][index] = max(0.0, float(window.max()))
            data[f"long_close_mfe_h{horizon}"][index] = max(0.0, float(window.max()))
            data[f"short_close_mfe_h{horizon}"][index] = max(0.0, float(-window.min()))
            data[f"hold_days_h{horizon}"][index] = float(horizon)

        for threshold in config.takeprofit_thresholds:
            tag = _tag_from_threshold(threshold)
            long_hits = np.where(rel >= threshold)[0]
            long_exit = int(long_hits[0] + 1) if len(long_hits) else int(max_future)
            long_window = rel[:long_exit]
            data[f"long_tp_{tag}_ret"][index] = float(long_window[-1])
            data[f"long_tp_{tag}_hold"][index] = float(long_exit)
            data[f"long_tp_{tag}_mae"][index] = max(0.0, float(-long_window.min()))

            short_hits = np.where(rel <= -threshold)[0]
            short_exit = int(short_hits[0] + 1) if len(short_hits) else int(max_future)
            short_window = rel[:short_exit]
            data[f"short_tp_{tag}_ret"][index] = float(-short_window[-1])
            data[f"short_tp_{tag}_hold"][index] = float(short_exit)
            data[f"short_tp_{tag}_mae"][index] = max(0.0, float(short_window.max()))

        for threshold in config.stop_thresholds:
            tag = _tag_from_threshold(threshold)
            long_hits = np.where(rel <= -threshold)[0]
            long_exit = int(long_hits[0] + 1) if len(long_hits) else int(max_future)
            long_window = rel[:long_exit]
            data[f"long_stop_{tag}_ret"][index] = float(long_window[-1])
            data[f"long_stop_{tag}_hold"][index] = float(long_exit)
            data[f"long_stop_{tag}_mae"][index] = max(0.0, float(-long_window.min()))

            short_hits = np.where(rel >= threshold)[0]
            short_exit = int(short_hits[0] + 1) if len(short_hits) else int(max_future)
            short_window = rel[:short_exit]
            data[f"short_stop_{tag}_ret"][index] = float(-short_window[-1])
            data[f"short_stop_{tag}_hold"][index] = float(short_exit)
            data[f"short_stop_{tag}_mae"][index] = max(0.0, float(short_window.max()))

    for column, values in data.items():
        out[column] = values
    return out


def _build_agent_dataset(
    paths: ResearchPaths,
    snapshot_id: str,
    config: AgentConfig,
    *,
    max_codes: int | None = None,
    force: bool = False,
) -> dict[str, Any]:
    agent_paths = AgentPaths.build(paths, snapshot_id)
    agent_paths.ensure_dirs()
    daily, universe, industry = _load_snapshot_with_industry(paths, snapshot_id)
    all_codes = sorted(daily["code"].astype(str).str.strip().dropna().unique().tolist())
    selected_codes = all_codes[: max_codes] if max_codes and max_codes > 0 else all_codes
    code_key = "all" if len(selected_codes) == len(all_codes) else f"codes_{len(selected_codes)}_{sha1('|'.join(selected_codes).encode('utf-8')).hexdigest()[:10]}"
    dataset_path = agent_paths.cache_dir / f"dataset_{code_key}.csv.gz"
    meta_path = agent_paths.cache_dir / f"dataset_{code_key}.json"
    if dataset_path.exists() and meta_path.exists() and not force:
        meta = _safe_read_json(meta_path, {})
        meta["path"] = str(dataset_path)
        meta["cached"] = True
        return meta

    daily = daily[daily["code"].astype(str).isin(selected_codes)].copy()
    universe = universe[universe["code"].astype(str).isin(selected_codes)].copy()
    industry = industry[industry["code"].astype(str).isin(selected_codes)].copy()

    local = _build_daily_features(daily)
    weekly_ctx = _context_bias_from_bars(_resample_bars(daily, "weekly"), fast_window=4, mid_window=13, long_window=26)
    monthly_ctx = _context_bias_from_bars(_resample_bars(daily, "monthly"), fast_window=3, mid_window=6, long_window=12)
    local = _merge_context(local, weekly_ctx, "weekly_context_bias")
    local = _merge_context(local, monthly_ctx, "monthly_context_bias")
    local = _assign_universe(local, universe, "daily")
    local = local.merge(industry, on="code", how="left")
    local["sector33_code"] = local["sector33_code"].fillna("__NA__")
    local["sector33_name"] = local["sector33_name"].fillna("UNCLASSIFIED")
    local = _assign_regime_and_clusters(local)
    parts = [_future_close_outcomes_for_code(group, config) for _, group in local.groupby("code", sort=False)]
    local = pd.concat(parts, ignore_index=True) if parts else local.copy()
    local["event_date"] = pd.to_datetime(local["event_date"], errors="coerce").dt.strftime("%Y-%m-%d")
    local["universe_asof_date"] = pd.to_datetime(local["universe_asof_date"], errors="coerce").dt.strftime("%Y-%m-%d")
    local = local.copy()
    local["snapshot_id"] = snapshot_id
    local = local.sort_values(["event_date", "code"]).reset_index(drop=True)
    dataset_path.parent.mkdir(parents=True, exist_ok=True)
    local.to_csv(dataset_path, index=False, compression="gzip", encoding="utf-8")
    meta = {
        "ok": True,
        "snapshot_id": snapshot_id,
        "cached": False,
        "rows": int(len(local)),
        "codes": int(len(selected_codes)),
        "path": str(dataset_path),
        "columns": list(local.columns),
        "code_key": code_key,
        "max_codes": int(max_codes) if max_codes else None,
    }
    write_json(meta_path, meta)
    return meta


def _load_agent_dataset(dataset_path: str | Path) -> pd.DataFrame:
    frame = read_csv(Path(str(dataset_path)))
    frame["month_bucket"] = pd.to_datetime(frame["event_date"], errors="coerce").dt.strftime("%Y-%m")
    return frame


def _walkforward_folds(months: list[str], config: AgentConfig) -> list[dict[str, list[str]]]:
    clean_months = [month for month in months if month]
    if not clean_months:
        return []
    train_span = max(1, int(config.walkforward.min_train_years) * 12)
    valid_span = max(1, int(config.walkforward.valid_months))
    test_span = max(1, int(config.walkforward.test_months))
    step = max(1, int(config.walkforward.step_months))
    folds: list[dict[str, list[str]]] = []
    cursor = train_span
    while cursor + valid_span + test_span <= len(clean_months):
        folds.append(
            {
                "train": clean_months[:cursor],
                "valid": clean_months[cursor : cursor + valid_span],
                "test": clean_months[cursor + valid_span : cursor + valid_span + test_span],
            }
        )
        cursor += step
    if folds:
        return folds
    if len(clean_months) >= 3:
        left = max(1, len(clean_months) // 3)
        mid = max(left + 1, (len(clean_months) * 2) // 3)
        return [
            {
                "train": clean_months[:left],
                "valid": clean_months[left:mid],
                "test": clean_months[mid:],
            }
        ]
    return []


def _condition_mask(frame: pd.DataFrame, conditions: tuple[dict[str, Any], ...]) -> pd.Series:
    mask = pd.Series(True, index=frame.index, dtype=bool)
    for condition in conditions:
        column = str(condition.get("column") or "")
        if column not in frame.columns:
            return pd.Series(False, index=frame.index, dtype=bool)
        op = str(condition.get("op") or "==").strip()
        value = condition.get("value")
        series = frame[column]
        if op == "between":
            low, high = value
            current = pd.to_numeric(series, errors="coerce")
            mask &= current.ge(float(low)) & current.le(float(high))
        elif op == "<=":
            mask &= pd.to_numeric(series, errors="coerce").le(float(value))
        elif op == ">=":
            mask &= pd.to_numeric(series, errors="coerce").ge(float(value))
        elif op == "<":
            mask &= pd.to_numeric(series, errors="coerce").lt(float(value))
        elif op == ">":
            mask &= pd.to_numeric(series, errors="coerce").gt(float(value))
        elif op == "==":
            mask &= series.astype(str) == str(value)
        elif op == "!=":
            mask &= series.astype(str) != str(value)
        else:
            raise ValueError(f"unsupported condition op: {op}")
    return mask.fillna(False)


def _series_stats(returns: pd.Series, mae: pd.Series | None = None, hold: pd.Series | None = None) -> dict[str, float]:
    clean_returns = pd.to_numeric(returns, errors="coerce").dropna()
    if clean_returns.empty:
        return {
            "samples": 0.0,
            "expectancy": math.nan,
            "win_rate": math.nan,
            "avg_gain": math.nan,
            "avg_loss": math.nan,
            "p90_close_mae": math.nan,
            "median_hold": math.nan,
        }
    gains = clean_returns[clean_returns > 0.0]
    losses = clean_returns[clean_returns <= 0.0]
    mae_series = pd.to_numeric(mae, errors="coerce").dropna() if mae is not None else pd.Series(dtype=float)
    hold_series = pd.to_numeric(hold, errors="coerce").dropna() if hold is not None else pd.Series(dtype=float)
    return {
        "samples": float(len(clean_returns)),
        "expectancy": float(clean_returns.mean()),
        "win_rate": float((clean_returns > 0.0).mean()),
        "avg_gain": float(gains.mean()) if not gains.empty else 0.0,
        "avg_loss": float(losses.mean()) if not losses.empty else 0.0,
        "p90_close_mae": float(mae_series.quantile(0.90)) if not mae_series.empty else math.nan,
        "median_hold": float(hold_series.median()) if not hold_series.empty else math.nan,
    }


def _fold_selection_columns(theme: str, side: str, horizon_or_tag: str) -> tuple[str, str, str]:
    if theme in {"buy", "sell"}:
        prefix = "long" if side == "long" else "short"
        return (
            f"{prefix}_ret_h{horizon_or_tag}",
            f"{prefix}_close_mae_h{horizon_or_tag}",
            f"hold_days_h{horizon_or_tag}",
        )
    prefix = "long" if side == "long" else "short"
    suffix = "tp" if theme == "takeprofit" else "stop"
    return (
        f"{prefix}_{suffix}_{horizon_or_tag}_ret",
        f"{prefix}_{suffix}_{horizon_or_tag}_mae",
        f"{prefix}_{suffix}_{horizon_or_tag}_hold",
    )


def _regime_lists(rows: pd.DataFrame, return_col: str) -> tuple[list[str], list[str]]:
    if rows.empty or "regime_key" not in rows.columns:
        return [], []
    grouped = rows.groupby("regime_key", dropna=False).agg(samples=(return_col, "count"), expectancy=(return_col, "mean")).reset_index()
    grouped["regime_key"] = grouped["regime_key"].astype(str)
    effective = grouped[(grouped["samples"] >= 5) & (grouped["expectancy"] > 0.0)]["regime_key"].tolist()
    ineffective = grouped[(grouped["samples"] >= 5) & (grouped["expectancy"] <= 0.0)]["regime_key"].tolist()
    return effective[:5], ineffective[:5]


def _evaluate_buy_sell(frame: pd.DataFrame, mask: pd.Series, hypothesis: Hypothesis, config: AgentConfig) -> dict[str, Any]:
    months = sorted(frame["month_bucket"].dropna().unique().tolist())
    folds = _walkforward_folds(months, config)
    fold_rows: list[dict[str, Any]] = []
    selected_parts: list[pd.DataFrame] = []
    candidate_horizons = [h for h in config.horizons if h <= config.max_hold_days and h >= 5]

    for fold_index, fold in enumerate(folds, start=1):
        valid_mask = frame["month_bucket"].isin(fold["valid"]) & mask
        test_mask = frame["month_bucket"].isin(fold["test"]) & mask
        valid = frame.loc[valid_mask].copy()
        test = frame.loc[test_mask].copy()
        if valid.empty or test.empty:
            continue
        chosen_horizon = None
        chosen_score = None
        for horizon in candidate_horizons:
            return_col, _, _ = _fold_selection_columns(hypothesis.theme, hypothesis.side, str(horizon))
            score = pd.to_numeric(valid[return_col], errors="coerce").dropna().mean()
            if pd.isna(score):
                continue
            if chosen_score is None or score > chosen_score or (score == chosen_score and (chosen_horizon or 10**9) > horizon):
                chosen_score = float(score)
                chosen_horizon = horizon
        if chosen_horizon is None:
            continue
        return_col, mae_col, hold_col = _fold_selection_columns(hypothesis.theme, hypothesis.side, str(chosen_horizon))
        test["selected_return"] = pd.to_numeric(test[return_col], errors="coerce")
        test["selected_mae"] = pd.to_numeric(test[mae_col], errors="coerce")
        test["selected_hold"] = pd.to_numeric(test[hold_col], errors="coerce")
        test["selected_horizon"] = chosen_horizon
        selected_parts.append(test.dropna(subset=["selected_return"]).copy())
        stats = _series_stats(test["selected_return"], mae=test["selected_mae"], hold=test["selected_hold"])
        fold_rows.append(
            {
                "fold_index": fold_index,
                "selected_horizon": int(chosen_horizon),
                "samples": int(stats["samples"]),
                "expectancy": float(stats["expectancy"]),
                "p90_close_mae": float(stats["p90_close_mae"]),
                "median_hold": float(stats["median_hold"]),
            }
        )

    selected_rows = pd.concat(selected_parts, ignore_index=True) if selected_parts else pd.DataFrame()
    aggregate = _series_stats(selected_rows.get("selected_return", pd.Series(dtype=float)), mae=selected_rows.get("selected_mae"), hold=selected_rows.get("selected_hold"))
    fold_expectancies = [float(row["expectancy"]) for row in fold_rows if not math.isnan(float(row["expectancy"]))]
    median_fold_expectancy = float(np.median(fold_expectancies)) if fold_expectancies else math.nan
    positive_fold_ratio = float(np.mean(np.array(fold_expectancies) > 0.0)) if fold_expectancies else 0.0
    adopt = (
        aggregate["samples"] >= config.buy_sell_min_samples
        and aggregate["expectancy"] > 0.0
        and median_fold_expectancy > 0.0
        and positive_fold_ratio >= config.buy_sell_positive_fold_ratio
        and float(aggregate["p90_close_mae"]) <= config.buy_sell_max_p90_close_mae
        and float(aggregate["median_hold"]) <= config.buy_sell_max_median_hold
    )
    hold = bool(aggregate["samples"] >= max(10, config.buy_sell_min_samples // 3) and aggregate["expectancy"] > 0.0)
    decision = "adopted" if adopt else "hold" if hold else "discarded"
    effective, ineffective = _regime_lists(selected_rows, "selected_return")
    return {
        "theme": hypothesis.theme,
        "decision": decision,
        "samples": int(aggregate["samples"]),
        "pooled_expectancy": float(aggregate["expectancy"]) if not math.isnan(float(aggregate["expectancy"])) else math.nan,
        "median_fold_expectancy": median_fold_expectancy,
        "positive_fold_ratio": positive_fold_ratio,
        "p90_close_mae": float(aggregate["p90_close_mae"]) if not math.isnan(float(aggregate["p90_close_mae"])) else math.nan,
        "median_hold": float(aggregate["median_hold"]) if not math.isnan(float(aggregate["median_hold"])) else math.nan,
        "win_rate": float(aggregate["win_rate"]) if not math.isnan(float(aggregate["win_rate"])) else math.nan,
        "avg_gain": float(aggregate["avg_gain"]) if not math.isnan(float(aggregate["avg_gain"])) else math.nan,
        "avg_loss": float(aggregate["avg_loss"]) if not math.isnan(float(aggregate["avg_loss"])) else math.nan,
        "effective_regimes": effective,
        "ineffective_regimes": ineffective,
        "folds": fold_rows,
        "selected_rows": selected_rows,
    }


def _evaluate_skip(frame: pd.DataFrame, mask: pd.Series, hypothesis: Hypothesis, config: AgentConfig) -> dict[str, Any]:
    months = sorted(frame["month_bucket"].dropna().unique().tolist())
    folds = _walkforward_folds(months, config)
    fold_rows: list[dict[str, Any]] = []
    selected_parts: list[pd.DataFrame] = []
    horizon = 20 if 20 in config.horizons else config.horizons[min(len(config.horizons) - 1, 0)]

    for fold_index, fold in enumerate(folds, start=1):
        test_all = frame.loc[frame["month_bucket"].isin(fold["test"])].copy()
        test_cond = test_all.loc[mask.reindex(test_all.index).fillna(False)].copy()
        if test_all.empty or test_cond.empty:
            continue
        remain = test_all.loc[~mask.reindex(test_all.index).fillna(False)].copy()
        long_col = f"long_ret_h{horizon}"
        short_col = f"short_ret_h{horizon}"
        move_col = f"long_close_mfe_h{horizon}"
        long_cond = pd.to_numeric(test_cond[long_col], errors="coerce").dropna()
        short_cond = pd.to_numeric(test_cond[short_col], errors="coerce").dropna()
        long_all = pd.to_numeric(test_all[long_col], errors="coerce").dropna()
        short_all = pd.to_numeric(test_all[short_col], errors="coerce").dropna()
        long_remain = pd.to_numeric(remain[long_col], errors="coerce").dropna()
        short_remain = pd.to_numeric(remain[short_col], errors="coerce").dropna()
        long_exp = float(long_cond.mean()) if not long_cond.empty else math.nan
        short_exp = float(short_cond.mean()) if not short_cond.empty else math.nan
        improvement = max(
            (float(long_remain.mean()) - float(long_all.mean())) if not long_remain.empty and not long_all.empty else 0.0,
            (float(short_remain.mean()) - float(short_all.mean())) if not short_remain.empty and not short_all.empty else 0.0,
        )
        move_potential = float(pd.to_numeric(test_cond[move_col], errors="coerce").dropna().mean())
        low_edge = (((long_exp <= 0.0) and (short_exp <= 0.0)) or move_potential < 0.02)
        test_cond["long_return"] = pd.to_numeric(test_cond[long_col], errors="coerce")
        test_cond["short_return"] = pd.to_numeric(test_cond[short_col], errors="coerce")
        test_cond["move_potential"] = pd.to_numeric(test_cond[move_col], errors="coerce")
        test_cond["improvement"] = improvement
        test_cond["selected_horizon"] = horizon
        selected_parts.append(test_cond.dropna(subset=["long_return", "short_return"], how="all").copy())
        fold_rows.append(
            {
                "fold_index": fold_index,
                "samples": int(len(test_cond)),
                "long_expectancy": long_exp,
                "short_expectancy": short_exp,
                "move_potential": move_potential,
                "improvement": improvement,
                "low_edge": bool(low_edge),
            }
        )

    selected_rows = pd.concat(selected_parts, ignore_index=True) if selected_parts else pd.DataFrame()
    improvements = [float(row["improvement"]) for row in fold_rows]
    long_expectancies = [float(row["long_expectancy"]) for row in fold_rows if not math.isnan(float(row["long_expectancy"]))]
    short_expectancies = [float(row["short_expectancy"]) for row in fold_rows if not math.isnan(float(row["short_expectancy"]))]
    move_potentials = [float(row["move_potential"]) for row in fold_rows if not math.isnan(float(row["move_potential"]))]
    low_edge_ratio = float(np.mean([bool(row["low_edge"]) for row in fold_rows])) if fold_rows else 0.0
    adopt = bool(
        selected_rows.shape[0] >= max(10, config.buy_sell_min_samples // 2)
        and improvements
        and np.mean(improvements) >= config.skip_improvement_min
        and low_edge_ratio >= 0.5
    )
    hold = bool(selected_rows.shape[0] >= 10 and improvements and np.mean(improvements) > 0.0)
    decision = "adopted" if adopt else "hold" if hold else "discarded"
    return {
        "theme": hypothesis.theme,
        "decision": decision,
        "samples": int(len(selected_rows)),
        "pooled_expectancy": float(np.nanmean(long_expectancies + short_expectancies)) if (long_expectancies or short_expectancies) else math.nan,
        "median_fold_expectancy": math.nan,
        "positive_fold_ratio": float(np.mean(np.array(improvements) > 0.0)) if improvements else 0.0,
        "p90_close_mae": math.nan,
        "median_hold": float(horizon),
        "win_rate": math.nan,
        "avg_gain": math.nan,
        "avg_loss": math.nan,
        "long_expectancy": float(np.mean(long_expectancies)) if long_expectancies else math.nan,
        "short_expectancy": float(np.mean(short_expectancies)) if short_expectancies else math.nan,
        "move_potential": float(np.mean(move_potentials)) if move_potentials else math.nan,
        "improvement": float(np.mean(improvements)) if improvements else math.nan,
        "effective_regimes": [],
        "ineffective_regimes": [],
        "folds": fold_rows,
        "selected_rows": selected_rows,
    }


def _evaluate_exit_overlay(frame: pd.DataFrame, mask: pd.Series, hypothesis: Hypothesis, config: AgentConfig) -> dict[str, Any]:
    months = sorted(frame["month_bucket"].dropna().unique().tolist())
    folds = _walkforward_folds(months, config)
    fold_rows: list[dict[str, Any]] = []
    selected_parts: list[pd.DataFrame] = []
    candidates = config.takeprofit_thresholds if hypothesis.theme == "takeprofit" else config.stop_thresholds
    tags = [_tag_from_threshold(value) for value in candidates]
    baseline_return_col, baseline_mae_col, baseline_hold_col = _fold_selection_columns("buy", hypothesis.side, "60")

    for fold_index, fold in enumerate(folds, start=1):
        valid = frame.loc[frame["month_bucket"].isin(fold["valid"]) & mask].copy()
        test = frame.loc[frame["month_bucket"].isin(fold["test"]) & mask].copy()
        if valid.empty or test.empty:
            continue
        chosen_tag = None
        chosen_improvement = None
        for tag in tags:
            return_col, _, _ = _fold_selection_columns(hypothesis.theme, hypothesis.side, tag)
            improvement = pd.to_numeric(valid[return_col], errors="coerce").dropna().mean() - pd.to_numeric(valid[baseline_return_col], errors="coerce").dropna().mean()
            if pd.isna(improvement):
                continue
            if chosen_improvement is None or improvement > chosen_improvement:
                chosen_improvement = float(improvement)
                chosen_tag = tag
        if chosen_tag is None:
            continue
        return_col, mae_col, hold_col = _fold_selection_columns(hypothesis.theme, hypothesis.side, chosen_tag)
        test["selected_return"] = pd.to_numeric(test[return_col], errors="coerce")
        test["selected_mae"] = pd.to_numeric(test[mae_col], errors="coerce")
        test["selected_hold"] = pd.to_numeric(test[hold_col], errors="coerce")
        test["baseline_return"] = pd.to_numeric(test[baseline_return_col], errors="coerce")
        test["baseline_mae"] = pd.to_numeric(test[baseline_mae_col], errors="coerce")
        test["selected_tag"] = chosen_tag
        selected_parts.append(test.dropna(subset=["selected_return", "baseline_return"]).copy())
        improvement = float((test["selected_return"] - test["baseline_return"]).dropna().mean())
        baseline_p90 = float(test["baseline_mae"].dropna().quantile(0.90)) if not test["baseline_mae"].dropna().empty else math.nan
        selected_p90 = float(test["selected_mae"].dropna().quantile(0.90)) if not test["selected_mae"].dropna().empty else math.nan
        mae_improvement_ratio = ((baseline_p90 - selected_p90) / baseline_p90) if baseline_p90 and not math.isnan(baseline_p90) else math.nan
        fold_rows.append(
            {
                "fold_index": fold_index,
                "selected_tag": chosen_tag,
                "samples": int(test["selected_return"].dropna().shape[0]),
                "improvement": improvement,
                "mae_improvement_ratio": mae_improvement_ratio,
            }
        )

    selected_rows = pd.concat(selected_parts, ignore_index=True) if selected_parts else pd.DataFrame()
    improvements = [float(row["improvement"]) for row in fold_rows if not math.isnan(float(row["improvement"]))]
    mae_improvements = [float(row["mae_improvement_ratio"]) for row in fold_rows if not math.isnan(float(row["mae_improvement_ratio"]))]
    selected_stats = _series_stats(selected_rows.get("selected_return", pd.Series(dtype=float)), mae=selected_rows.get("selected_mae"), hold=selected_rows.get("selected_hold"))
    baseline_stats = _series_stats(selected_rows.get("baseline_return", pd.Series(dtype=float)), mae=selected_rows.get("baseline_mae"), hold=selected_rows.get(baseline_hold_col))
    adopt = bool(improvements and ((np.mean(improvements) > 0.0) or (mae_improvements and np.mean(mae_improvements) >= 0.15)))
    hold = bool(improvements and np.mean(improvements) > -0.001)
    decision = "adopted" if adopt else "hold" if hold else "discarded"
    effective, ineffective = _regime_lists(selected_rows.assign(selected_return=selected_rows.get("selected_return")), "selected_return")
    return {
        "theme": hypothesis.theme,
        "decision": decision,
        "samples": int(selected_stats["samples"]),
        "pooled_expectancy": float(selected_stats["expectancy"]) if not math.isnan(float(selected_stats["expectancy"])) else math.nan,
        "median_fold_expectancy": float(np.median(improvements)) if improvements else math.nan,
        "positive_fold_ratio": float(np.mean(np.array(improvements) > 0.0)) if improvements else 0.0,
        "p90_close_mae": float(selected_stats["p90_close_mae"]) if not math.isnan(float(selected_stats["p90_close_mae"])) else math.nan,
        "median_hold": float(selected_stats["median_hold"]) if not math.isnan(float(selected_stats["median_hold"])) else math.nan,
        "win_rate": float(selected_stats["win_rate"]) if not math.isnan(float(selected_stats["win_rate"])) else math.nan,
        "avg_gain": float(selected_stats["avg_gain"]) if not math.isnan(float(selected_stats["avg_gain"])) else math.nan,
        "avg_loss": float(selected_stats["avg_loss"]) if not math.isnan(float(selected_stats["avg_loss"])) else math.nan,
        "baseline_expectancy": float(baseline_stats["expectancy"]) if not math.isnan(float(baseline_stats["expectancy"])) else math.nan,
        "improvement": float(np.mean(improvements)) if improvements else math.nan,
        "mae_improvement_ratio": float(np.mean(mae_improvements)) if mae_improvements else math.nan,
        "effective_regimes": effective,
        "ineffective_regimes": ineffective,
        "folds": fold_rows,
        "selected_rows": selected_rows,
    }


def _evaluate_failure_reason(frame: pd.DataFrame, mask: pd.Series, hypothesis: Hypothesis, config: AgentConfig) -> dict[str, Any]:
    months = sorted(frame["month_bucket"].dropna().unique().tolist())
    folds = _walkforward_folds(months, config)
    fold_rows: list[dict[str, Any]] = []
    selected_parts: list[pd.DataFrame] = []
    horizon = 20 if 20 in config.horizons else config.horizons[min(len(config.horizons) - 1, 0)]
    return_col = f"{'long' if hypothesis.side == 'long' else 'short'}_ret_h{horizon}"

    for fold_index, fold in enumerate(folds, start=1):
        test = frame.loc[frame["month_bucket"].isin(fold["test"])].copy()
        if test.empty:
            continue
        test["condition"] = mask.reindex(test.index).fillna(False)
        test["selected_return"] = pd.to_numeric(test[return_col], errors="coerce")
        winners = test[test["selected_return"] > 0.0]
        losers = test[test["selected_return"] <= 0.0]
        loser_occurrence = float(losers["condition"].mean()) if not losers.empty else 0.0
        winner_occurrence = float(winners["condition"].mean()) if not winners.empty else 0.0
        rate_ratio = loser_occurrence / max(winner_occurrence, 1e-9) if (loser_occurrence or winner_occurrence) else 0.0
        selected = test[test["condition"]].copy()
        selected["is_loser"] = (selected["selected_return"] <= 0.0).astype(float)
        selected["rate_ratio"] = rate_ratio
        selected["selected_horizon"] = horizon
        selected_parts.append(selected.dropna(subset=["selected_return"]).copy())
        loser_share = float(selected["is_loser"].mean()) if not selected.empty else 0.0
        fold_rows.append(
            {
                "fold_index": fold_index,
                "samples": int(selected.shape[0]),
                "rate_ratio": rate_ratio,
                "loser_share": loser_share,
            }
        )

    selected_rows = pd.concat(selected_parts, ignore_index=True) if selected_parts else pd.DataFrame()
    rate_ratios = [float(row["rate_ratio"]) for row in fold_rows]
    loser_shares = [float(row["loser_share"]) for row in fold_rows]
    reproduced = int(np.sum(np.array(rate_ratios) >= config.failure_rate_ratio_min)) if rate_ratios else 0
    avg_rate_ratio = float(np.mean(rate_ratios)) if rate_ratios else math.nan
    avg_loser_share = float(np.mean(loser_shares)) if loser_shares else math.nan
    adopt = bool(
        selected_rows.shape[0] >= max(10, config.buy_sell_min_samples // 4)
        and not math.isnan(avg_rate_ratio)
        and avg_rate_ratio >= config.failure_rate_ratio_min
        and reproduced >= config.failure_min_repro_folds
        and not math.isnan(avg_loser_share)
        and avg_loser_share >= config.failure_min_loser_share
    )
    hold = bool(selected_rows.shape[0] >= 10 and rate_ratios and max(rate_ratios) >= 1.1)
    decision = "adopted" if adopt else "hold" if hold else "discarded"
    effective, ineffective = _regime_lists(selected_rows.assign(selected_return=-selected_rows.get("is_loser", pd.Series(dtype=float))), "selected_return")
    return {
        "theme": hypothesis.theme,
        "decision": decision,
        "samples": int(selected_rows.shape[0]),
        "pooled_expectancy": math.nan,
        "median_fold_expectancy": math.nan,
        "positive_fold_ratio": float(reproduced / max(len(fold_rows), 1)) if fold_rows else 0.0,
        "p90_close_mae": math.nan,
        "median_hold": float(horizon),
        "win_rate": math.nan,
        "avg_gain": math.nan,
        "avg_loss": math.nan,
        "rate_ratio": avg_rate_ratio,
        "reproduced_folds": reproduced,
        "loser_share": avg_loser_share,
        "effective_regimes": effective,
        "ineffective_regimes": ineffective,
        "folds": fold_rows,
        "selected_rows": selected_rows,
    }


def evaluate_hypothesis(frame: pd.DataFrame, hypothesis: Hypothesis, config: AgentConfig | None = None) -> dict[str, Any]:
    resolved_config = config or default_agent_config()
    mask = _condition_mask(frame, hypothesis.conditions)
    if hypothesis.theme in {"buy", "sell"}:
        result = _evaluate_buy_sell(frame, mask, hypothesis, resolved_config)
    elif hypothesis.theme == "skip":
        result = _evaluate_skip(frame, mask, hypothesis, resolved_config)
    elif hypothesis.theme in {"takeprofit", "stop"}:
        result = _evaluate_exit_overlay(frame, mask, hypothesis, resolved_config)
    elif hypothesis.theme == "failure":
        result = _evaluate_failure_reason(frame, mask, hypothesis, resolved_config)
    else:
        raise ValueError(f"unsupported hypothesis theme: {hypothesis.theme}")
    result["hypothesis_id"] = hypothesis.hypothesis_id
    result["name"] = hypothesis.name
    result["tokens"] = list(hypothesis.tokens)
    result["side"] = hypothesis.side
    result["stage"] = hypothesis.stage
    result["conditions"] = [dict(item) for item in hypothesis.conditions]
    return result


def _confidence_from_result(result: dict[str, Any]) -> float:
    samples = float(result.get("samples") or 0.0)
    sample_score = min(samples / 120.0, 1.0)
    decision_score = 1.0 if result.get("decision") == "adopted" else 0.6 if result.get("decision") == "hold" else 0.2
    fold_ratio = float(result.get("positive_fold_ratio") or 0.0)
    return round(0.4 * sample_score + 0.4 * decision_score + 0.2 * fold_ratio, 3)


def _narratives(ticker: str, hypothesis: Hypothesis, result: dict[str, Any]) -> tuple[str, str]:
    stage = hypothesis.stage
    tokens = " / ".join(hypothesis.tokens)
    effective = ", ".join(result.get("effective_regimes") or []) or "特定レジーム未確定"
    ineffective = ", ".join(result.get("ineffective_regimes") or []) or "特定レジーム未確定"
    if stage == "skip":
        short = f"{ticker} は {tokens} の局面で見送り優先。"
        long = (
            f"この銘柄は {tokens} の局面では値幅が薄く、見送りが優先です。"
            f" long/short の期待値は伸びにくく、除外後の残集合期待値改善は {float(result.get('improvement') or 0.0):.4f} でした。"
            f" 有効レジーム: {effective}。無効レジーム: {ineffective}。"
        )
    elif stage == "buy":
        short = f"{ticker} は {tokens} の初動で買い候補。"
        long = (
            f"この銘柄は {tokens} が揃った場面で買いが機能しやすいです。"
            f" 想定保有中央値は {float(result.get('median_hold') or 0.0):.1f} 日、期待値は {float(result.get('pooled_expectancy') or 0.0):.4f}。"
            f" 有効レジーム: {effective}。無効レジーム: {ineffective}。"
        )
    elif stage == "sell":
        short = f"{ticker} は {tokens} の戻り売り候補。"
        long = (
            f"この銘柄は {tokens} の戻り局面で売りが機能しやすいです。"
            f" 想定保有中央値は {float(result.get('median_hold') or 0.0):.1f} 日、期待値は {float(result.get('pooled_expectancy') or 0.0):.4f}。"
            f" 有効レジーム: {effective}。無効レジーム: {ineffective}。"
        )
    elif stage == "takeprofit":
        short = f"{ticker} は {tokens} で利確を急ぐ候補。"
        long = (
            f"この銘柄は {tokens} が出たら利確を優先する方が良い可能性があります。"
            f" 60日クローズ基準との差は {float(result.get('improvement') or 0.0):.4f}、MAE 改善率は {float(result.get('mae_improvement_ratio') or 0.0):.2%}。"
            f" 有効レジーム: {effective}。無効レジーム: {ineffective}。"
        )
    elif stage == "stop":
        short = f"{ticker} は {tokens} で損切り優先。"
        long = (
            f"この銘柄は {tokens} が出たら損切りを優先する方が良い可能性があります。"
            f" 60日クローズ基準との差は {float(result.get('improvement') or 0.0):.4f}、MAE 改善率は {float(result.get('mae_improvement_ratio') or 0.0):.2%}。"
            f" 有効レジーム: {effective}。無効レジーム: {ineffective}。"
        )
    else:
        short = f"{ticker} は {tokens} で失敗理由が増える。"
        long = (
            f"この銘柄では {tokens} が出ると失敗比率が上がりやすいです。"
            f" loser/winner 出現比は {float(result.get('rate_ratio') or 0.0):.2f} 倍、平均 loser share は {float(result.get('loser_share') or 0.0):.2%}。"
            f" 有効レジーム: {effective}。無効レジーム: {ineffective}。"
        )
    return short, long


def _rulebook_suffix(stage: str) -> str:
    mapping = {
        "buy": "buy_rules",
        "sell": "sell_rules",
        "takeprofit": "takeprofit_rules",
        "stop": "stop_rules",
        "skip": "skip_rules",
        "failure": "failure_reasons",
    }
    return mapping.get(stage, f"{stage}_rules")


def _flatten_stats(result: dict[str, Any]) -> dict[str, Any]:
    keys = (
        "samples",
        "pooled_expectancy",
        "median_fold_expectancy",
        "positive_fold_ratio",
        "p90_close_mae",
        "median_hold",
        "win_rate",
        "avg_gain",
        "avg_loss",
        "improvement",
        "mae_improvement_ratio",
        "rate_ratio",
        "loser_share",
        "long_expectancy",
        "short_expectancy",
        "move_potential",
    )
    return {key: result.get(key) for key in keys if key in result}


def _build_rule_cards(cycle_id: str, hypothesis: Hypothesis, result: dict[str, Any]) -> list[dict[str, Any]]:
    selected_rows = result.get("selected_rows")
    if not isinstance(selected_rows, pd.DataFrame) or selected_rows.empty or "code" not in selected_rows.columns:
        return []
    cards: list[dict[str, Any]] = []
    for ticker, group in selected_rows.groupby("code", dropna=False):
        ticker = str(ticker)
        if not ticker.strip():
            continue
        if hypothesis.theme == "skip":
            stats = {
                "samples": int(group.shape[0]),
                "long_expectancy": float(pd.to_numeric(group.get("long_return"), errors="coerce").dropna().mean()) if "long_return" in group.columns else math.nan,
                "short_expectancy": float(pd.to_numeric(group.get("short_return"), errors="coerce").dropna().mean()) if "short_return" in group.columns else math.nan,
                "avg_move_potential": float(pd.to_numeric(group.get("move_potential"), errors="coerce").dropna().mean()) if "move_potential" in group.columns else math.nan,
                "improvement": float(pd.to_numeric(group.get("improvement"), errors="coerce").dropna().mean()) if "improvement" in group.columns else math.nan,
            }
        elif hypothesis.theme == "failure":
            stats = {
                "samples": int(group.shape[0]),
                "loser_share": float(pd.to_numeric(group.get("is_loser"), errors="coerce").dropna().mean()) if "is_loser" in group.columns else math.nan,
                "rate_ratio": float(pd.to_numeric(group.get("rate_ratio"), errors="coerce").dropna().mean()) if "rate_ratio" in group.columns else math.nan,
            }
        else:
            stats = _series_stats(group.get("selected_return", pd.Series(dtype=float)), mae=group.get("selected_mae"), hold=group.get("selected_hold"))
            if "baseline_return" in group.columns:
                stats["baseline_expectancy"] = float(pd.to_numeric(group.get("baseline_return"), errors="coerce").dropna().mean())
        short, long = _narratives(ticker, hypothesis, result)
        cards.append(
            {
                "cycle_id": cycle_id,
                "hypothesis_id": hypothesis.hypothesis_id,
                "ticker": ticker,
                "side": hypothesis.side,
                "stage": hypothesis.stage,
                "status": result.get("decision"),
                "regime": ", ".join(result.get("effective_regimes") or []),
                "condition_tokens": list(hypothesis.tokens),
                "stats": stats,
                "narrative_short": short,
                "narrative_long": long,
                "effective_regimes": list(result.get("effective_regimes") or []),
                "ineffective_regimes": list(result.get("ineffective_regimes") or []),
                "confidence": _confidence_from_result(result),
            }
        )
    return cards


def _build_failure_cards(cycle_id: str, hypothesis: Hypothesis, result: dict[str, Any]) -> list[dict[str, Any]]:
    if hypothesis.theme != "failure":
        if result.get("decision") == "discarded":
            return [
                {
                    "cycle_id": cycle_id,
                    "hypothesis_id": hypothesis.hypothesis_id,
                    "theme": hypothesis.theme,
                    "reason": "gate_rejected",
                    "details": _flatten_stats(result),
                }
            ]
        return []
    rows = _build_rule_cards(cycle_id, hypothesis, result)
    return [
        {
            "cycle_id": cycle_id,
            "hypothesis_id": hypothesis.hypothesis_id,
            "ticker": row["ticker"],
            "stage": row["stage"],
            "status": row["status"],
            "condition_tokens": row["condition_tokens"],
            "stats": row["stats"],
            "narrative_short": row["narrative_short"],
            "narrative_long": row["narrative_long"],
        }
        for row in rows
    ]


def _append_cards(agent_paths: AgentPaths, cards: list[dict[str, Any]], failure_cards: list[dict[str, Any]]) -> None:
    existing_rules = _load_json_list(agent_paths.rule_cards_json)
    existing_failures = _load_json_list(agent_paths.failure_cards_json)
    existing_rules.extend(cards)
    existing_failures.extend(failure_cards)
    _write_json_list(agent_paths.rule_cards_json, existing_rules)
    _write_json_list(agent_paths.failure_cards_json, existing_failures)

    flat_rules: list[dict[str, Any]] = []
    for row in existing_rules:
        flat = {
            "cycle_id": row.get("cycle_id"),
            "hypothesis_id": row.get("hypothesis_id"),
            "ticker": row.get("ticker"),
            "side": row.get("side"),
            "stage": row.get("stage"),
            "status": row.get("status"),
            "regime": row.get("regime"),
            "condition_tokens": "|".join(row.get("condition_tokens") or []),
            "narrative_short": row.get("narrative_short"),
            "narrative_long": row.get("narrative_long"),
            "effective_regimes": "|".join(row.get("effective_regimes") or []),
            "ineffective_regimes": "|".join(row.get("ineffective_regimes") or []),
            "confidence": row.get("confidence"),
        }
        stats = row.get("stats") if isinstance(row.get("stats"), dict) else {}
        for key, value in stats.items():
            flat[f"stats_{key}"] = value
        flat_rules.append(flat)
    write_csv(agent_paths.rule_cards_csv, pd.DataFrame(flat_rules))

    flat_failures: list[dict[str, Any]] = []
    for row in existing_failures:
        flat = {key: value for key, value in row.items() if key not in {"stats", "condition_tokens"}}
        flat["condition_tokens"] = "|".join(row.get("condition_tokens") or [])
        stats = row.get("stats") if isinstance(row.get("stats"), dict) else {}
        for key, value in stats.items():
            flat[f"stats_{key}"] = value
        flat_failures.append(flat)
    write_csv(agent_paths.failure_cards_csv, pd.DataFrame(flat_failures))


def _render_rulebook(cards: list[dict[str, Any]], stage: str) -> str:
    lines = [f"# {stage.title()} Rulebook", ""]
    for row in cards:
        stats = row.get("stats") if isinstance(row.get("stats"), dict) else {}
        confidence = row.get("confidence")
        lines.extend(
            [
                f"## {row.get('hypothesis_id')} ({row.get('status')})",
                "",
                f"- 短い要約: {row.get('narrative_short')}",
                f"- どういう場面か: {' / '.join(row.get('condition_tokens') or [])}",
                f"- なぜ効くか: {row.get('narrative_long')}",
                "- どこで入るか: 当日終値基準。詳細は該当ルールの局面説明を参照。",
                "- どこで利確か: takeprofit ルールがある場合はそちらを優先、なければ60営業日以内の終値管理。",
                "- どこで損切りか: stop ルールがある場合はそちらを優先、なければ失敗理由シグナルを監視。",
                f"- どこは見送るか: {'skip' if stage != 'skip' else 'このルール自体が見送り条件'}",
                f"- 何が出たら失敗しやすいか: {', '.join(row.get('ineffective_regimes') or []) or '追加研究中'}",
                f"- サンプル数と信頼度: {stats.get('samples', 'n/a')} / {confidence}",
                "",
                "### 詳細版",
                "",
                f"- effective_regimes: {', '.join(row.get('effective_regimes') or []) or 'n/a'}",
                f"- ineffective_regimes: {', '.join(row.get('ineffective_regimes') or []) or 'n/a'}",
                f"- stats: {json.dumps(stats, ensure_ascii=False, sort_keys=True)}",
                "",
            ]
        )
    return "\n".join(lines)


def _write_rulebooks(agent_paths: AgentPaths) -> None:
    cards = _load_json_list(agent_paths.rule_cards_json)
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for row in cards:
        ticker = str(row.get("ticker") or "").strip()
        stage = str(row.get("stage") or "").strip()
        if not ticker or not stage:
            continue
        grouped.setdefault((ticker, stage), []).append(row)
    for (ticker, stage), rows in grouped.items():
        filename = f"ticker_{ticker}_{_rulebook_suffix(stage)}.md"
        (agent_paths.rulebooks_dir / filename).write_text(_render_rulebook(rows, stage), encoding="utf-8")


def _pending_hypotheses(state: dict[str, Any], theme: str | None = None) -> list[dict[str, Any]]:
    rows = [row for row in state.get("hypotheses", []) if isinstance(row, dict)]
    if theme:
        rows = [row for row in rows if str(row.get("theme")) == str(theme)]
    return [row for row in rows if str(row.get("status") or "pending") == "pending"]


def _hold_hypotheses(state: dict[str, Any], theme: str | None = None) -> list[dict[str, Any]]:
    rows = [row for row in state.get("hypotheses", []) if isinstance(row, dict)]
    if theme:
        rows = [row for row in rows if str(row.get("theme")) == str(theme)]
    return [row for row in rows if str(row.get("status") or "") == "hold"]


def _queued_hypotheses(state: dict[str, Any], theme: str | None = None) -> list[dict[str, Any]]:
    pending = _pending_hypotheses(state, theme)
    return pending if pending else _hold_hypotheses(state, theme)


def _first_pending_by_priority(state: dict[str, Any]) -> list[dict[str, Any]]:
    priorities = state.get("config", {}).get("priority") or list(default_agent_config().priority)
    for theme in priorities:
        pending = _pending_hypotheses(state, str(theme))
        if pending:
            return pending
    return []


def _row_to_hypothesis(row: dict[str, Any]) -> Hypothesis:
    return Hypothesis(
        hypothesis_id=str(row.get("hypothesis_id")),
        theme=str(row.get("theme")),
        stage=str(row.get("stage")),
        side=str(row.get("side")),
        name=str(row.get("name")),
        tokens=tuple(str(item) for item in row.get("tokens") or []),
        conditions=tuple(dict(item) for item in row.get("conditions") or []),
        notes=str(row.get("notes") or ""),
    )


def _status_ja(status: str) -> str:
    mapping = {"adopted": "採用", "hold": "保留", "discarded": "破棄"}
    return mapping.get(status, status)


def _update_state_after_cycle(state: dict[str, Any], evaluated_rows: list[tuple[dict[str, Any], dict[str, Any]]], cycle_id: str) -> dict[str, Any]:
    lookup = {str(row.get("hypothesis_id")): row for row in state.get("hypotheses", []) if isinstance(row, dict)}
    for hypothesis_row, result in evaluated_rows:
        target = lookup.get(str(hypothesis_row.get("hypothesis_id")))
        if target is None:
            continue
        target["status"] = "completed" if result.get("decision") == "adopted" else "hold" if result.get("decision") == "hold" else "discarded"
        target["decision"] = result.get("decision")
        target["last_cycle_id"] = cycle_id
        history = target.setdefault("history", [])
        history.append(
            {
                "cycle_id": cycle_id,
                "decision": result.get("decision"),
                "samples": result.get("samples"),
                "pooled_expectancy": result.get("pooled_expectancy"),
            }
        )
    state["next_cycle_number"] = int(state.get("next_cycle_number") or 1) + 1
    state["updated_at"] = now_utc_iso()
    return state


def _summarize_rule_cards(cards: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    added = [row for row in cards if row.get("status") == "adopted"]
    held = [row for row in cards if row.get("status") == "hold"]
    dropped = [row for row in cards if row.get("status") == "discarded"]
    return added, held, dropped


def _render_progress_md(cycle_id: str, hypothesis_results: list[dict[str, Any]], next_rows: list[dict[str, Any]]) -> str:
    lines = ["# Progress", "", f"- cycle_id: `{cycle_id}`", ""]
    for row in hypothesis_results:
        lines.append(f"- {row['hypothesis_id']}: {_status_ja(str(row.get('decision')))} / samples={row.get('samples')} / expectancy={row.get('pooled_expectancy')}")
    lines.extend(["", "## Next", ""])
    for row in next_rows[:10]:
        lines.append(f"- {row.get('theme')} / {row.get('hypothesis_id')} / {row.get('name')}")
    lines.append("")
    return "\n".join(lines)


def _render_rules_delta_md(cycle_id: str, cards: list[dict[str, Any]]) -> str:
    added, held, dropped = _summarize_rule_cards(cards)
    lines = ["# Rules Delta", "", f"- cycle_id: `{cycle_id}`", "", "## Added", ""]
    for row in added:
        lines.append(f"- {row['ticker']} / {row['stage']} / {row['hypothesis_id']} / {row['narrative_short']}")
    if not added:
        lines.append("- なし")
    lines.extend(["", "## Hold", ""])
    for row in held:
        lines.append(f"- {row['ticker']} / {row['stage']} / {row['hypothesis_id']} / {row['narrative_short']}")
    if not held:
        lines.append("- なし")
    lines.extend(["", "## Discarded", ""])
    for row in dropped:
        lines.append(f"- {row['ticker']} / {row['stage']} / {row['hypothesis_id']} / {row['narrative_short']}")
    if not dropped:
        lines.append("- なし")
    lines.append("")
    return "\n".join(lines)


def _render_failures_delta_md(cycle_id: str, failure_cards: list[dict[str, Any]]) -> str:
    lines = ["# Failures Delta", "", f"- cycle_id: `{cycle_id}`", ""]
    if not failure_cards:
        lines.extend(["- 新規 failure card はありません。", ""])
        return "\n".join(lines)
    for row in failure_cards:
        identifier = row.get("ticker") or row.get("hypothesis_id")
        lines.append(f"- {identifier}: {row.get('narrative_short', row.get('reason', 'failure'))}")
    lines.append("")
    return "\n".join(lines)


def _render_backlog_md(state: dict[str, Any]) -> str:
    lines = ["# Backlog", "", "## Pending", ""]
    pending = [row for row in state.get("hypotheses", []) if str(row.get("status") or "pending") == "pending"]
    if not pending:
        lines.append("- pending hypothesis はありません。")
    else:
        for row in pending[:20]:
            lines.append(f"- {row.get('theme')} / {row.get('hypothesis_id')} / {row.get('name')}")
    held = [row for row in state.get("hypotheses", []) if str(row.get("status") or "") == "hold"]
    if held:
        lines.extend(["", "## Hold", ""])
        for row in held[:20]:
            lines.append(f"- {row.get('theme')} / {row.get('hypothesis_id')} / {row.get('name')}")
    discarded = [row for row in state.get("hypotheses", []) if str(row.get("status")) == "discarded"]
    if discarded:
        lines.extend(["", "## Discarded", ""])
        for row in discarded[:20]:
            lines.append(f"- {row.get('theme')} / {row.get('hypothesis_id')} / {row.get('name')}")
    lines.append("")
    return "\n".join(lines)


def _render_executive_summary_md(cards: list[dict[str, Any]], state: dict[str, Any]) -> str:
    adopted = [row for row in cards if row.get("status") == "adopted"]
    lines = ["# Executive Summary", ""]
    if not adopted:
        lines.extend(
            [
                "- まだ採用済みルールはありません。",
                "- 見送り優先で backlog を継続し、再現する局面だけ残します。",
                "",
            ]
        )
        return "\n".join(lines)
    top_cards = sorted(adopted, key=lambda row: float(row.get("confidence") or 0.0), reverse=True)[:10]
    for row in top_cards:
        lines.append(f"- {row['ticker']} / {row['stage']} / {row['narrative_short']} / confidence={row.get('confidence')}")
    lines.extend(["", "## Next Focus", ""])
    for row in _first_pending_by_priority(state)[:10]:
        lines.append(f"- {row.get('theme')} / {row.get('name')}")
    lines.append("")
    return "\n".join(lines)


def _render_candidates(cards: list[dict[str, Any]]) -> tuple[str, str, str]:
    adopted = [row for row in cards if row.get("status") == "adopted"]
    if not adopted:
        placeholder = "# Candidates\n\nまだ採用ルールはありません。\n"
        return placeholder, placeholder, placeholder
    global_lines = ["# Best Patterns Global", ""]
    for row in sorted(adopted, key=lambda item: float(item.get("confidence") or 0.0), reverse=True)[:20]:
        global_lines.append(f"- {row['ticker']} / {row['stage']} / {row['narrative_short']} / confidence={row.get('confidence')}")
    regime_lines = ["# Best Patterns By Regime", ""]
    regime_groups: dict[str, list[dict[str, Any]]] = {}
    for row in adopted:
        for regime in row.get("effective_regimes") or ["unclassified"]:
            regime_groups.setdefault(regime, []).append(row)
    for regime, rows in sorted(regime_groups.items()):
        preview = "; ".join([f"{row['ticker']}:{row['stage']}" for row in rows[:5]])
        regime_lines.append(f"- {regime}: {preview}")
    ticker_lines = ["# Best Patterns By Ticker", ""]
    ticker_groups: dict[str, list[dict[str, Any]]] = {}
    for row in adopted:
        ticker_groups.setdefault(str(row.get("ticker")), []).append(row)
    for ticker, rows in sorted(ticker_groups.items()):
        preview = "; ".join([f"{row['stage']}:{row['hypothesis_id']}" for row in rows[:8]])
        ticker_lines.append(f"- {ticker}: {preview}")
    return "\n".join(global_lines) + "\n", "\n".join(regime_lines) + "\n", "\n".join(ticker_lines) + "\n"


def _render_handoff(cards: list[dict[str, Any]], state: dict[str, Any]) -> tuple[str, str]:
    adopted = [row for row in cards if row.get("status") == "adopted"]
    integration_lines = ["# MeeMee Integration Candidates", ""]
    if not adopted:
        integration_lines.append("- 採用ルールが増えるまで MeeMee 本体へは反映しません。")
    else:
        for row in adopted[:20]:
            integration_lines.append(f"- {row['ticker']} / {row['stage']} / {row['hypothesis_id']} / {row['narrative_short']}")
    open_question_lines = ["# Open Questions", ""]
    pending = _first_pending_by_priority(state)
    if not pending:
        open_question_lines.append("- 主要 hypothesis は一巡済みです。")
    else:
        for row in pending[:10]:
            open_question_lines.append(f"- {row.get('theme')} / {row.get('name')} / 追加検証が必要")
    return "\n".join(integration_lines) + "\n", "\n".join(open_question_lines) + "\n"


def _write_cycle_outputs(
    agent_paths: AgentPaths,
    cycle_id: str,
    hypothesis_rows: list[dict[str, Any]],
    cards: list[dict[str, Any]],
    failure_cards: list[dict[str, Any]],
    state: dict[str, Any],
    dataset_meta: dict[str, Any],
) -> None:
    progress = _render_progress_md(cycle_id, hypothesis_rows, _first_pending_by_priority(state))
    rules_delta = _render_rules_delta_md(cycle_id, cards)
    failures_delta = _render_failures_delta_md(cycle_id, failure_cards)
    backlog = _render_backlog_md(state)
    all_cards = _load_json_list(agent_paths.rule_cards_json)
    executive_summary = _render_executive_summary_md(all_cards, state)
    best_global, best_regime, best_ticker = _render_candidates(all_cards)
    handoff, open_questions = _render_handoff(all_cards, state)

    (agent_paths.results_dir / "progress.md").write_text(progress, encoding="utf-8")
    (agent_paths.results_dir / "rules_delta.md").write_text(rules_delta, encoding="utf-8")
    (agent_paths.results_dir / "failures_delta.md").write_text(failures_delta, encoding="utf-8")
    (agent_paths.results_dir / "backlog.md").write_text(backlog, encoding="utf-8")
    (agent_paths.results_dir / "executive_summary.md").write_text(executive_summary, encoding="utf-8")
    (agent_paths.candidates_dir / "best_patterns_global.md").write_text(best_global, encoding="utf-8")
    (agent_paths.candidates_dir / "best_patterns_by_regime.md").write_text(best_regime, encoding="utf-8")
    (agent_paths.candidates_dir / "best_patterns_by_ticker.md").write_text(best_ticker, encoding="utf-8")
    (agent_paths.handoff_dir / "meemee_integration_candidates.md").write_text(handoff, encoding="utf-8")
    (agent_paths.handoff_dir / "open_questions.md").write_text(open_questions, encoding="utf-8")

    cycle_history_dir = agent_paths.results_history_dir / f"cycle_{cycle_id}"
    if cycle_history_dir.exists():
        shutil.rmtree(cycle_history_dir, ignore_errors=True)
    cycle_history_dir.mkdir(parents=True, exist_ok=True)
    for name in ("progress.md", "rules_delta.md", "failures_delta.md", "backlog.md", "executive_summary.md"):
        shutil.copy2(agent_paths.results_dir / name, cycle_history_dir / name)

    def _json_safe_result(row: dict[str, Any]) -> dict[str, Any]:
        safe = {key: value for key, value in row.items() if key != "selected_rows"}
        selected_rows = row.get("selected_rows")
        if isinstance(selected_rows, pd.DataFrame):
            safe["selected_rows_summary"] = {
                "rows": int(selected_rows.shape[0]),
                "columns": list(selected_rows.columns),
            }
        return safe

    write_json(
        agent_paths.cycle_manifest_dir / f"cycle_{cycle_id}.json",
        {
            "cycle_id": cycle_id,
            "created_at": now_utc_iso(),
            "snapshot_id": agent_paths.snapshot_id,
            "dataset_meta": dataset_meta,
            "hypotheses": [_json_safe_result(row) for row in hypothesis_rows],
            "rule_cards": cards,
            "failure_cards": failure_cards,
        },
    )


def _experiment_markdown(cycle_id: str, snapshot_id: str, dataset_meta: dict[str, Any], hypothesis: Hypothesis, result: dict[str, Any]) -> str:
    return "\n".join(
        [
            f"# Experiment {cycle_id}",
            "",
            f"- 実験ID: `{cycle_id}`",
            f"- 仮説: {hypothesis.name}",
            f"- 対象銘柄: snapshot `{snapshot_id}` / codes={dataset_meta.get('codes')}",
            f"- 対象期間: `{snapshot_id}` dataset cache",
            f"- 特徴量: {', '.join(hypothesis.tokens)}",
            f"- ラベル定義: {hypothesis.theme}",
            "- 検証方法: expanding walk-forward",
            f"- サンプル数: {result.get('samples')}",
            f"- 結果: {_status_ja(str(result.get('decision')))}",
            f"- 勝率: {result.get('win_rate')}",
            f"- 期待値: {result.get('pooled_expectancy')}",
            f"- 最大逆行: {result.get('p90_close_mae')}",
            f"- 平均保有日数: {result.get('median_hold')}",
            f"- 有効レジーム: {', '.join(result.get('effective_regimes') or []) or 'n/a'}",
            f"- 無効レジーム: {', '.join(result.get('ineffective_regimes') or []) or 'n/a'}",
            f"- 失敗理由: {json.dumps(_flatten_stats(result), ensure_ascii=False, sort_keys=True)}",
            f"- 採用/保留/破棄: {_status_ja(str(result.get('decision')))}",
            "- 次アクション: backlog の次仮説へ進む",
            "",
        ]
    )


def run_agent_cycle(
    paths: ResearchPaths,
    snapshot_id: str,
    *,
    theme: str | None = None,
    max_hypotheses: int = 1,
    max_codes: int | None = None,
    resume: bool = False,
    force_dataset: bool = False,
    config: AgentConfig | None = None,
) -> dict[str, Any]:
    del resume
    resolved_config = config or default_agent_config()
    run_agent_init(paths, snapshot_id, resolved_config)
    agent_paths = AgentPaths.build(paths, snapshot_id)
    state = _load_or_init_state(agent_paths, resolved_config)
    dataset_meta = _build_agent_dataset(paths, snapshot_id, resolved_config, max_codes=max_codes, force=force_dataset)
    frame = _load_agent_dataset(dataset_meta["path"])
    queued = _queued_hypotheses(state, theme) if theme else _first_pending_by_priority(state)
    selected = queued[: max(1, int(max_hypotheses))]
    if not selected:
        return {"ok": True, "snapshot_id": snapshot_id, "cycle_id": None, "message": "no pending hypotheses"}

    cycle_number = int(state.get("next_cycle_number") or 1)
    cycle_id = f"{cycle_number:04d}"
    hypothesis_results: list[dict[str, Any]] = []
    cards: list[dict[str, Any]] = []
    failure_cards: list[dict[str, Any]] = []
    evaluated_rows: list[tuple[dict[str, Any], dict[str, Any]]] = []

    for hypothesis_row in selected:
        hypothesis = _row_to_hypothesis(hypothesis_row)
        result = evaluate_hypothesis(frame, hypothesis, resolved_config)
        evaluated_rows.append((hypothesis_row, result))
        hypothesis_results.append(result)
        cards.extend(_build_rule_cards(cycle_id, hypothesis, result))
        failure_cards.extend(_build_failure_cards(cycle_id, hypothesis, result))
        experiment_path = agent_paths.experiments_dir / f"exp_{cycle_id}_{hypothesis.theme}_{hypothesis.hypothesis_id}.md"
        experiment_path.write_text(_experiment_markdown(cycle_id, snapshot_id, dataset_meta, hypothesis, result), encoding="utf-8")

    _append_cards(agent_paths, cards, failure_cards)
    state = _update_state_after_cycle(state, evaluated_rows, cycle_id)
    write_json(agent_paths.state_file, state)
    _write_rulebooks(agent_paths)
    _write_cycle_outputs(agent_paths, cycle_id, hypothesis_results, cards, failure_cards, state, dataset_meta)
    return {
        "ok": True,
        "snapshot_id": snapshot_id,
        "cycle_id": cycle_id,
        "hypotheses": [
            {
                "hypothesis_id": row.get("hypothesis_id"),
                "decision": row.get("decision"),
                "samples": row.get("samples"),
                "expectancy": row.get("pooled_expectancy"),
            }
            for row in hypothesis_results
        ],
        "dataset": dataset_meta,
        "rule_cards": len(cards),
        "failure_cards": len(failure_cards),
        "root": str(agent_paths.root),
    }


def run_agent_loop(
    paths: ResearchPaths,
    snapshot_id: str,
    *,
    theme: str | None = None,
    max_cycles: int = 1,
    max_hypotheses: int = 1,
    max_codes: int | None = None,
    resume: bool = False,
    force_dataset: bool = False,
    config: AgentConfig | None = None,
) -> dict[str, Any]:
    resolved_config = config or default_agent_config()
    run_agent_init(paths, snapshot_id, resolved_config)
    results: list[dict[str, Any]] = []
    for _ in range(max(1, int(max_cycles))):
        cycle_result = run_agent_cycle(
            paths,
            snapshot_id,
            theme=theme,
            max_hypotheses=max_hypotheses,
            max_codes=max_codes,
            resume=resume,
            force_dataset=force_dataset if not results else False,
            config=resolved_config,
        )
        results.append(cycle_result)
        if not cycle_result.get("cycle_id"):
            break
    return {
        "ok": True,
        "snapshot_id": snapshot_id,
        "cycles": len([row for row in results if row.get("cycle_id")]),
        "results": results,
        "root": str(AgentPaths.build(paths, snapshot_id).root),
    }
