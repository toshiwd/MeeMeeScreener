from __future__ import annotations

from typing import Any, TypedDict


ALLOWED_UNIT_SET = {2, 3, 5, -2, -3, -5}


class SnapshotRankingItem(TypedDict, total=False):
    ticker: str
    ev: float | None
    upProb: float | None
    revRisk: float | None
    timeframeSignals: dict[str, Any]
    regime: str
    gate: dict
    close: float | None
    liquidity20d: float | None
    sector: str
    shortable: bool
    entryScore: float | None
    sourceAsOf: str | None


class SnapshotPosition(TypedDict, total=False):
    ticker: str
    side: str
    units: int
    avgPrice: float
    pnlPct: float
    stage: str
    openedAt: str
    holdingDays: int


class DecisionAction(TypedDict, total=False):
    ticker: str
    side: str
    deltaUnits: int
    reasonId: str
    notes: str


REASON_ID_SET: set[str] = {
    "E_NEW_TOP1_GATE_OK",
    "E_NEW_TOPK_GATE_OK",
    "E_NEW_SWITCH_IN",
    "A_ADD_PROBE_TO_ADD_OK",
    "A_ADD_ADD_TO_MAIN_OK",
    "A_ADD_STAGE2_STRICT_OK",
    "T_TP_PARTIAL_5_TO_3",
    "T_TP_PARTIAL_3_TO_2",
    "T_TP_TARGET_REACHED",
    "T_TP_REV_RISK_RISING",
    "X_EXIT_UPPROB_DROP",
    "X_EXIT_EV_DROP",
    "X_EXIT_GATE_NG",
    "X_EXIT_REV_RISK_HIGH",
    "X_EXIT_TIME_LIMIT",
    "S_SWITCH_EV_GAP",
    "S_SWITCH_RISK_AVOID",
    "S_SWITCH_CAPACITY",
    "R_CUT_LOSS_WARN",
    "R_CUT_LOSS_HARD",
    "R_EXPOSURE_TRIM",
    "R_GAME_OVER",
    "R_MAX_HOLDINGS_BLOCK",
    "K_NO_SNAPSHOT",
    "K_MARKET_CLOSED",
    "K_POLICY_INCONSISTENT",
}
