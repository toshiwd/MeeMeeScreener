from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any
import json

from app.backend.services.toredex_hash import hash_payload


_REPO_ROOT = Path(__file__).resolve().parents[3]
_CONFIG_PATH = _REPO_ROOT / "toredex_config.json"

_DEFAULT_CONFIG: dict[str, Any] = {
    "policyVersion": "toredex.v8",
    "mode": "LIVE",
    "operatingMode": "champion",
    "topN": 50,
    "runsDir": None,
    "initialCash": 10_000_000,
    "maxPerTicker": 10_000_000,
    "maxHoldings": 3,
    "unitOptions": [2, 3, 5],
    "rankingMode": "ml",
    "sides": {
        "longEnabled": True,
        "shortEnabled": False,
    },
    "thresholds": {
        "entryMinUpProb": 0.65,
        "entryMinEv": 0.03,
        "entryMaxRevRisk": 0.55,
        "exitMinUpProb": 0.5,
        "exitMinEv": -0.01,
        "revRiskWarn": 0.55,
        "revRiskHigh": 0.65,
        "addMinUpProb": 0.6,
        "addMinEv": 0.03,
        "addMaxRevRisk": 0.45,
        "addMinPnlPct": -1.0,
        "addMaxPnlPct": 12.0,
        "maxNewEntriesPerDay": 1.0,
        "newEntryMaxRank": 1.0,
        "switchMinEvGap": 0.05,
        "cutLossWarnPct": -5.5,
        "cutLossHardPct": -9.0,
        "gameOverPct": -20.0,
        "takeProfitHintPct": 10.0,
        "exitIfUnranked": 0.0,
    },
    "stageRules": {
        "goal20Pct": 20.0,
        "goal30Pct": 30.0,
    },
    "fieldMapping": {
        "revRisk": "mlPTurnDownShort|mlPDownShort",
        "regime": "trendUp/trendDown/trendUpStrict/trendDownStrict",
        "gate": "entryQualified+setupType",
    },
    "costModel": {
        "feesBps": 0.0,
        "slippageBps": 0.0,
        "slippageLiquidityFactorBps": 0.0,
        "borrowShortBpsAnnual": 0.0,
        "sensitivityBps": [5.0, 10.0, 15.0],
    },
    "riskGates": {
        "champion": {
            "maxDrawdownPct": -8.0,
            "worstMonthPct": -8.0,
            "maxTurnoverPctPerMonth": 300.0,
            "maxNetExposureUnits": 2.0,
            "enabled": True,
        },
        "challenger": {
            "maxDrawdownPct": -15.0,
            "worstMonthPct": -12.0,
            "maxTurnoverPctPerMonth": 600.0,
            "maxNetExposureUnits": 4.0,
            "enabled": True,
        },
    },
    "portfolioConstraints": {
        "grossUnitsCap": 10,
        "maxNetUnits": 2,
        "maxUnitsPerTicker": 2,
        "maxPerSector": 2,
        "minLiquidity20d": 0.0,
        "shortBlacklist": [],
    },
    "optimization": {
        "stage0Months": 2,
        "stage1Months": 12,
        "stage2Months": 36,
        "iterationsPerRun": 12,
        "stage2TopK": 3,
        "stage1MaxCandidates": 8,
        "parallelWorkers": 1,
        "parallelDbPaths": [],
        "optimizeCostModel": False,
        "minTradesStage0": 1,
        "minTradesStage1": 3,
        "minTradesStage2": 8,
        "top1EntryShareMaxStage1Pct": 95.0,
        "top1EntryShareMaxStage2Pct": 90.0,
        "scoreWeights": {
            "maxDrawdown": 0.45,
            "worstMonth": 0.30,
            "turnover": 0.01,
            "netExposure": 0.20,
            "costDrag": 0.40,
            "top1Concentration": 0.10,
            "tradeShortfall": 1.25,
            "riskGateFailPenalty": 1000.0,
        },
    },
}


@dataclass(frozen=True)
class ToredexConfig:
    data: dict[str, Any]
    config_hash: str

    @property
    def policy_version(self) -> str:
        return str(self.data.get("policyVersion") or "toredex.v8")

    @property
    def mode(self) -> str:
        return str(self.data.get("mode") or "LIVE").upper()

    @property
    def operating_mode(self) -> str:
        text = str(self.data.get("operatingMode") or "champion").strip().lower()
        return text if text in {"champion", "challenger"} else "champion"

    @property
    def top_n(self) -> int:
        return max(1, int(self.data.get("topN") or 50))

    @property
    def runs_dir(self) -> str | None:
        value = self.data.get("runsDir")
        if value is None:
            return None
        text = str(value).strip()
        return text or None

    @property
    def ranking_mode(self) -> str:
        return str(self.data.get("rankingMode") or "hybrid").lower()

    @property
    def initial_cash(self) -> float:
        return float(self.data.get("initialCash") or 10_000_000)

    @property
    def max_per_ticker(self) -> float:
        return float(self.data.get("maxPerTicker") or 10_000_000)

    @property
    def max_holdings(self) -> int:
        return max(1, int(self.data.get("maxHoldings") or 3))

    @property
    def unit_options(self) -> tuple[int, ...]:
        raw = self.data.get("unitOptions")
        if not isinstance(raw, list):
            return (2, 3, 5)
        normalized = tuple(sorted({int(v) for v in raw if int(v) in {2, 3, 5}}))
        return normalized or (2, 3, 5)

    @property
    def sides(self) -> dict[str, bool]:
        raw = self.data.get("sides")
        if not isinstance(raw, dict):
            return {"longEnabled": True, "shortEnabled": False}
        return {
            "longEnabled": bool(raw.get("longEnabled", True)),
            "shortEnabled": bool(raw.get("shortEnabled", False)),
        }

    @property
    def thresholds(self) -> dict[str, float]:
        raw = self.data.get("thresholds")
        if not isinstance(raw, dict):
            return dict(_DEFAULT_CONFIG["thresholds"])
        out = dict(_DEFAULT_CONFIG["thresholds"])
        for key, value in raw.items():
            try:
                out[str(key)] = float(value)
            except Exception:
                continue
        return out

    @property
    def stage_rules(self) -> dict[str, float]:
        raw = self.data.get("stageRules")
        if not isinstance(raw, dict):
            return dict(_DEFAULT_CONFIG["stageRules"])
        out = dict(_DEFAULT_CONFIG["stageRules"])
        for key, value in raw.items():
            try:
                out[str(key)] = float(value)
            except Exception:
                continue
        return out

    @property
    def cost_model(self) -> dict[str, Any]:
        raw = self.data.get("costModel")
        base = dict(_DEFAULT_CONFIG["costModel"])
        if not isinstance(raw, dict):
            return base
        out = dict(base)
        for key in ("feesBps", "slippageBps", "slippageLiquidityFactorBps", "borrowShortBpsAnnual"):
            if key in raw:
                try:
                    out[key] = float(raw.get(key))
                except Exception:
                    pass
        sensitivity = raw.get("sensitivityBps")
        if isinstance(sensitivity, list):
            parsed: list[float] = []
            for value in sensitivity:
                try:
                    parsed.append(float(value))
                except Exception:
                    continue
            if parsed:
                out["sensitivityBps"] = parsed
        return out

    @property
    def risk_gates(self) -> dict[str, dict[str, Any]]:
        raw = self.data.get("riskGates")
        base = _DEFAULT_CONFIG["riskGates"]
        if not isinstance(raw, dict):
            return {"champion": dict(base["champion"]), "challenger": dict(base["challenger"])}

        out: dict[str, dict[str, Any]] = {}
        for mode in ("champion", "challenger"):
            mode_raw = raw.get(mode)
            base_mode = dict(base[mode])
            if isinstance(mode_raw, dict):
                for key in ("maxDrawdownPct", "worstMonthPct", "maxTurnoverPctPerMonth", "maxNetExposureUnits"):
                    if key in mode_raw:
                        try:
                            base_mode[key] = float(mode_raw.get(key))
                        except Exception:
                            continue
                if "enabled" in mode_raw:
                    base_mode["enabled"] = bool(mode_raw.get("enabled"))
            out[mode] = base_mode
        return out

    @property
    def portfolio_constraints(self) -> dict[str, Any]:
        raw = self.data.get("portfolioConstraints")
        base = dict(_DEFAULT_CONFIG["portfolioConstraints"])
        if not isinstance(raw, dict):
            return base

        out = dict(base)
        for key in ("grossUnitsCap", "maxNetUnits", "maxUnitsPerTicker", "maxPerSector"):
            if key in raw:
                try:
                    out[key] = int(raw.get(key))
                except Exception:
                    continue
        if "minLiquidity20d" in raw:
            try:
                out["minLiquidity20d"] = float(raw.get("minLiquidity20d"))
            except Exception:
                pass
        blacklist = raw.get("shortBlacklist")
        if isinstance(blacklist, list):
            out["shortBlacklist"] = [str(x).strip() for x in blacklist if str(x).strip()]
        return out

    @property
    def optimization(self) -> dict[str, Any]:
        raw = self.data.get("optimization")
        base = dict(_DEFAULT_CONFIG["optimization"])
        if not isinstance(raw, dict):
            return base
        out = dict(base)
        for key in (
            "stage0Months",
            "stage1Months",
            "stage2Months",
            "iterationsPerRun",
            "stage2TopK",
            "stage1MaxCandidates",
            "parallelWorkers",
        ):
            if key in raw:
                try:
                    out[key] = max(1, int(raw.get(key)))
                except Exception:
                    continue
        for key in ("minTradesStage0", "minTradesStage1", "minTradesStage2"):
            if key in raw:
                try:
                    out[key] = max(0, int(raw.get(key)))
                except Exception:
                    continue
        for key in ("top1EntryShareMaxStage1Pct", "top1EntryShareMaxStage2Pct"):
            if key in raw:
                try:
                    out[key] = min(100.0, max(0.0, float(raw.get(key))))
                except Exception:
                    continue
        if "optimizeCostModel" in raw:
            out["optimizeCostModel"] = bool(raw.get("optimizeCostModel"))
        raw_paths = raw.get("parallelDbPaths")
        if isinstance(raw_paths, list):
            out["parallelDbPaths"] = [str(path).strip() for path in raw_paths if str(path).strip()]
        raw_weights = raw.get("scoreWeights")
        if isinstance(raw_weights, dict):
            weights = dict(base["scoreWeights"]) if isinstance(base.get("scoreWeights"), dict) else {}
            for key in list(weights.keys()):
                if key in raw_weights:
                    try:
                        weights[key] = float(raw_weights.get(key))
                    except Exception:
                        continue
            out["scoreWeights"] = weights
        return out


def _deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    keys = set(base.keys()) | set(override.keys())
    for key in keys:
        b = base.get(key)
        o = override.get(key)
        if isinstance(b, dict) and isinstance(o, dict):
            out[key] = _deep_merge(b, o)
        elif key in override:
            out[key] = o
        else:
            out[key] = b
    return out


def load_toredex_config(path: str | Path | None = None, *, override: dict[str, Any] | None = None) -> ToredexConfig:
    config_path = Path(path) if path is not None else _CONFIG_PATH
    payload: dict[str, Any] = {}
    if config_path.exists():
        with open(config_path, "r", encoding="utf-8-sig") as handle:
            data = json.load(handle)
            if isinstance(data, dict):
                payload = data
    merged = _deep_merge(_DEFAULT_CONFIG, payload)
    if isinstance(override, dict) and override:
        merged = _deep_merge(merged, override)
    config_hash = hash_payload(merged)
    return ToredexConfig(data=merged, config_hash=config_hash)


def toredex_config_path() -> Path:
    return _CONFIG_PATH
