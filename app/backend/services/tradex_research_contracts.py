from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
from typing import Any, Final


TRADEX_RESEARCH_CONTRACT_SCHEMA_VERSION: Final[str] = "tradex_research_contract_v1"
TRADEX_RUN_MANIFEST_SCHEMA_VERSION: Final[str] = "tradex_research_run_manifest_v1"
TRADEX_ARTIFACT_DETAIL_LEVEL_AUTHORITATIVE: Final[str] = "authoritative_full"
TRADEX_ARTIFACT_DETAIL_LEVEL_RESEARCH_FALLBACK: Final[str] = "research_fallback_light"
TRADEX_ARTIFACT_DETAIL_LEVELS: Final[tuple[str, ...]] = (
    TRADEX_ARTIFACT_DETAIL_LEVEL_AUTHORITATIVE,
    TRADEX_ARTIFACT_DETAIL_LEVEL_RESEARCH_FALLBACK,
)
TRADEX_FALLBACK_STATUS_AUTHORITATIVE: Final[str] = "authoritative"
TRADEX_FALLBACK_STATUS_RESEARCH: Final[str] = "research-fallback"
TRADEX_FALLBACK_STATUSES: Final[tuple[str, ...]] = (
    TRADEX_FALLBACK_STATUS_AUTHORITATIVE,
    TRADEX_FALLBACK_STATUS_RESEARCH,
)
TRADEX_FEATURE_FAMILIES: Final[tuple[str, ...]] = (
    "environment_recognition",
    "common_pattern",
    "regime_adjustment",
    "boundary_feature",
    "bad_pick_removal",
    "symbol_specific_adjustment",
    "image_context_support",
)
TRADEX_ENVIRONMENT_STATES: Final[tuple[str, ...]] = (
    "trend_long",
    "trend_short",
    "range_buy",
    "range_sell",
    "panic_rebound",
    "bottom_building",
    "top_warning",
    "break_risk",
    "avoid",
)
TRADEX_EXECUTION_SUPPORT_STATES: Final[tuple[str, ...]] = (
    "probe_entry",
    "add_ok",
    "concern_trim",
    "decisive_exit",
)
TRADEX_VICTORY_METRICS: Final[tuple[str, ...]] = (
    "hold_end_return_20d",
    "mfe_20d",
    "mae_20d",
    "win_flag_hold_end",
    "win_flag_mfe",
    "addability_score",
    "trimability_score",
    "opportunity_count",
    "avg_holding_days",
    "max_drawdown",
)
TRADEX_DECISION_FIELD_NAMES: Final[tuple[str, ...]] = (
    "candidate_local_decision",
    "session_aggregate_decision",
    "authoritative_rollup_decision",
)
TRADEX_COMPARE_REQUIRED_FIELDS: Final[tuple[str, ...]] = (
    "schema_version",
    "diagnostics_schema_version",
    "family_id",
    "generated_at",
    "baseline_run_id",
    "candidate_results",
    "same_condition_contract",
)
TRADEX_FAMILY_LEADERBOARD_REQUIRED_FIELDS: Final[tuple[str, ...]] = (
    "schema_version",
    "session_meta",
    "source_compare_path",
    "coverage_waterfall",
    "overview",
    "family_summary",
    "candidate_rows",
)
TRADEX_SESSION_ROLLUP_REQUIRED_FIELDS: Final[tuple[str, ...]] = (
    "schema_version",
    "session_meta",
    "source_family_leaderboard_paths",
    "overview",
    "family_summary",
    "candidate_rows",
)
TRADEX_SCOPE_ROLLUP_REQUIRED_FIELDS: Final[tuple[str, ...]] = (
    "schema_version",
    "overview",
    "session_rows",
)
TRADEX_RUN_MANIFEST_REQUIRED_FIELDS: Final[tuple[str, ...]] = (
    "schema_version",
    "session_id",
    "seed",
    "random_seed",
    "input_artifacts",
    "asof",
    "config",
    "universe",
    "period",
    "horizon",
    "artifact_detail_level",
    "fallback_status",
)
TRADEX_DEFAULT_COST_MODEL: Final[dict[str, Any]] = {
    "schema_version": "tradex_cost_model_v1",
    "mode": "flat_zero_cost",
    "transaction_cost_bps": 0.0,
    "slippage_bps": 0.0,
    "fee_bps": 0.0,
}


@dataclass(frozen=True)
class SameConditionContract:
    schema_version: str
    universe: tuple[str, ...]
    period: tuple[tuple[str, str, str], ...]
    top_k: int
    regime: str
    cost_model: dict[str, Any]
    artifact_detail_level: str
    fallback_status: str
    feature_family: str | None = None

    def to_dict(self) -> dict[str, Any]:
        payload = {
            "schema_version": self.schema_version,
            "universe": list(self.universe),
            "period": [
                {"label": label, "start_date": start_date, "end_date": end_date}
                for label, start_date, end_date in self.period
            ],
            "top_k": int(self.top_k),
            "regime": self.regime,
            "cost_model": dict(self.cost_model),
            "artifact_detail_level": self.artifact_detail_level,
            "fallback_status": self.fallback_status,
        }
        if self.feature_family:
            payload["feature_family"] = self.feature_family
        payload["contract_hash"] = _stable_hash(payload)
        return payload


def _stable_hash(payload: dict[str, Any]) -> str:
    return hashlib.sha256(json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str).encode("utf-8")).hexdigest()


def _text(value: Any, fallback: str = "") -> str:
    if value is None:
        return fallback
    if isinstance(value, str):
        text = value.strip()
        return text or fallback
    text = str(value).strip()
    return text or fallback


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise ValueError(message)


def normalize_feature_family(value: Any, *, field_name: str = "feature_family") -> str:
    text = _text(value)
    _require(text != "", f"{field_name} is required")
    _require(text in TRADEX_FEATURE_FAMILIES, f"{field_name} must be one of {TRADEX_FEATURE_FAMILIES}")
    return text


def normalize_artifact_detail_level(value: Any, *, field_name: str = "artifact_detail_level") -> str:
    text = _text(value)
    _require(text != "", f"{field_name} is required")
    _require(text in TRADEX_ARTIFACT_DETAIL_LEVELS, f"{field_name} must be one of {TRADEX_ARTIFACT_DETAIL_LEVELS}")
    return text


def normalize_fallback_status(value: Any, *, field_name: str = "fallback_status") -> str:
    text = _text(value)
    _require(text != "", f"{field_name} is required")
    _require(text in TRADEX_FALLBACK_STATUSES, f"{field_name} must be one of {TRADEX_FALLBACK_STATUSES}")
    return text


def build_cost_model(payload: dict[str, Any] | None = None) -> dict[str, Any]:
    model = dict(TRADEX_DEFAULT_COST_MODEL)
    if isinstance(payload, dict):
        for key in ("schema_version", "mode", "transaction_cost_bps", "slippage_bps", "fee_bps"):
            if key in payload and payload.get(key) is not None:
                model[key] = payload.get(key)
    return model


def build_same_condition_contract(
    *,
    universe: list[str] | tuple[str, ...],
    period_segments: list[dict[str, Any]] | tuple[dict[str, Any], ...],
    top_k: int,
    regime: str,
    cost_model: dict[str, Any] | None = None,
    artifact_detail_level: str = TRADEX_ARTIFACT_DETAIL_LEVEL_AUTHORITATIVE,
    fallback_status: str = TRADEX_FALLBACK_STATUS_AUTHORITATIVE,
    feature_family: str | None = None,
) -> SameConditionContract:
    normalized_period: list[tuple[str, str, str]] = []
    for segment in period_segments:
        if not isinstance(segment, dict):
            continue
        label = _text(segment.get("label"))
        start_date = _text(segment.get("start_date"))
        end_date = _text(segment.get("end_date"))
        if label and start_date and end_date:
            normalized_period.append((label, start_date, end_date))
    return SameConditionContract(
        schema_version=TRADEX_RESEARCH_CONTRACT_SCHEMA_VERSION,
        universe=tuple(_text(item) for item in universe if _text(item)),
        period=tuple(normalized_period),
        top_k=max(1, int(top_k)),
        regime=_text(regime, fallback="unknown"),
        cost_model=build_cost_model(cost_model),
        artifact_detail_level=normalize_artifact_detail_level(artifact_detail_level),
        fallback_status=normalize_fallback_status(fallback_status),
        feature_family=_text(feature_family) or None,
    )


def build_run_manifest(
    *,
    session_id: str,
    seed: int,
    random_seed: int,
    input_artifacts: list[dict[str, Any]] | tuple[dict[str, Any], ...],
    asof: str,
    config: dict[str, Any],
    universe: list[str] | tuple[str, ...],
    period: dict[str, Any],
    horizon: str,
    artifact_detail_level: str,
    fallback_status: str,
    cost_model: dict[str, Any] | None = None,
) -> dict[str, Any]:
    payload = {
        "schema_version": TRADEX_RUN_MANIFEST_SCHEMA_VERSION,
        "session_id": _text(session_id),
        "seed": int(seed),
        "random_seed": int(random_seed),
        "input_artifacts": [dict(item) for item in input_artifacts if isinstance(item, dict)],
        "asof": _text(asof),
        "config": dict(config),
        "universe": [_text(item) for item in universe if _text(item)],
        "period": dict(period),
        "horizon": _text(horizon),
        "artifact_detail_level": normalize_artifact_detail_level(artifact_detail_level),
        "fallback_status": normalize_fallback_status(fallback_status),
        "cost_model": build_cost_model(cost_model),
    }
    payload["run_manifest_hash"] = _stable_hash(payload)
    return payload


def _require_fields(payload: dict[str, Any], *, required_fields: tuple[str, ...], artifact_name: str) -> None:
    missing = [field for field in required_fields if field not in payload]
    _require(not missing, f"{artifact_name} missing required fields: {', '.join(missing)}")


def _validate_decision_aliases(
    payload: dict[str, Any],
    *,
    container_name: str,
    explicit_field: str,
    legacy_field: str = "decision",
    allowed_values: tuple[str, ...] | None = None,
) -> None:
    explicit = _text(payload.get(explicit_field))
    legacy = _text(payload.get(legacy_field))
    _require(explicit != "", f"{container_name}.{explicit_field} is required")
    if allowed_values:
        _require(explicit in allowed_values, f"{container_name}.{explicit_field} must be one of {allowed_values}")
    if legacy:
        _require(explicit == legacy, f"{container_name}.{legacy_field} must match {explicit_field}")


def validate_compare_artifact(payload: dict[str, Any]) -> None:
    _require_fields(payload, required_fields=TRADEX_COMPARE_REQUIRED_FIELDS, artifact_name="compare artifact")
    same_condition = payload.get("same_condition_contract")
    _require(isinstance(same_condition, dict), "compare artifact same_condition_contract must be an object")
    _require(_text(same_condition.get("schema_version")) == TRADEX_RESEARCH_CONTRACT_SCHEMA_VERSION, "compare artifact same_condition_contract schema mismatch")
    _require(_text(same_condition.get("artifact_detail_level")) in TRADEX_ARTIFACT_DETAIL_LEVELS, "compare artifact artifact_detail_level invalid")
    _require(_text(same_condition.get("fallback_status")) in TRADEX_FALLBACK_STATUSES, "compare artifact fallback_status invalid")
    _require(_text(same_condition.get("regime")), "compare artifact same_condition_contract.regime is required")
    _require(int(same_condition.get("top_k") or 0) > 0, "compare artifact same_condition_contract.top_k is required")
    for row in payload.get("candidate_results") or []:
        if not isinstance(row, dict):
            continue
        _validate_decision_aliases(row, container_name="candidate_result", explicit_field="candidate_local_decision", allowed_values=("keep", "hold", "drop"))
        reasons = row.get("decision_reasons")
        _require(isinstance(reasons, list) and bool(reasons), "candidate_result decision_reasons must be a non-empty list")
        _require(_text(row.get("feature_family")) in TRADEX_FEATURE_FAMILIES, "candidate_result feature_family invalid")
        _require(_text(row.get("artifact_detail_level")) in TRADEX_ARTIFACT_DETAIL_LEVELS, "candidate_result artifact_detail_level invalid")
        _require(_text(row.get("fallback_status")) in TRADEX_FALLBACK_STATUSES, "candidate_result fallback_status invalid")
        _require(isinstance(row.get("victory_metrics"), dict), "candidate_result victory_metrics must be an object")
        _require(row.get("long_horizon_regime_score") is not None, "candidate_result long_horizon_regime_score is required")
        _require(row.get("recent_adaptation_score") is not None, "candidate_result recent_adaptation_score is required")


def validate_family_leaderboard_artifact(payload: dict[str, Any]) -> None:
    _require_fields(payload, required_fields=TRADEX_FAMILY_LEADERBOARD_REQUIRED_FIELDS, artifact_name="family leaderboard artifact")
    _require(_text(payload.get("authoritative_rollup_decision")) in {"keep", "hold", "drop"}, "family leaderboard artifact authoritative_rollup_decision is required")
    for row in payload.get("family_summary") or []:
        if not isinstance(row, dict):
            continue
        _validate_decision_aliases(row, container_name="family_summary row", explicit_field="session_aggregate_decision", allowed_values=("keep", "hold", "drop"))
        reasons = row.get("decision_reasons")
        _require(isinstance(reasons, list) and bool(reasons), "family_summary row decision_reasons must be a non-empty list")
    for row in payload.get("candidate_rows") or []:
        if not isinstance(row, dict):
            continue
        _validate_decision_aliases(row, container_name="candidate_rows row", explicit_field="candidate_local_decision", allowed_values=("keep", "hold", "drop"))
        reasons = row.get("decision_reasons")
        _require(isinstance(reasons, list) and bool(reasons), "candidate_rows row decision_reasons must be a non-empty list")
        _require(_text(row.get("feature_family")) in TRADEX_FEATURE_FAMILIES, "candidate_rows row feature_family invalid")
        _require(_text(row.get("artifact_detail_level")) in TRADEX_ARTIFACT_DETAIL_LEVELS, "candidate_rows row artifact_detail_level invalid")
        _require(_text(row.get("fallback_status")) in TRADEX_FALLBACK_STATUSES, "candidate_rows row fallback_status invalid")
        _require(isinstance(row.get("victory_metrics"), dict), "candidate_rows row victory_metrics must be an object")


def validate_session_rollup_artifact(payload: dict[str, Any]) -> None:
    _require_fields(payload, required_fields=TRADEX_SESSION_ROLLUP_REQUIRED_FIELDS, artifact_name="session rollup artifact")
    _require(_text(payload.get("authoritative_rollup_decision")) in {"keep", "hold", "drop"}, "session rollup artifact authoritative_rollup_decision is required")
    for row in payload.get("family_summary") or []:
        if not isinstance(row, dict):
            continue
        _validate_decision_aliases(row, container_name="session rollup family_summary row", explicit_field="session_aggregate_decision", allowed_values=("keep", "hold", "drop"))
        reasons = row.get("decision_reasons")
        _require(isinstance(reasons, list) and bool(reasons), "session rollup family_summary row decision_reasons must be a non-empty list")
    for row in payload.get("candidate_rows") or []:
        if not isinstance(row, dict):
            continue
        _validate_decision_aliases(row, container_name="session rollup candidate_rows row", explicit_field="candidate_local_decision", allowed_values=("keep", "hold", "drop"))
        reasons = row.get("decision_reasons")
        _require(isinstance(reasons, list) and bool(reasons), "session rollup candidate_rows row decision_reasons must be a non-empty list")
        _require(_text(row.get("feature_family")) in TRADEX_FEATURE_FAMILIES, "session rollup candidate_rows row feature_family invalid")


def validate_scope_rollup_artifact(payload: dict[str, Any]) -> None:
    _require_fields(payload, required_fields=TRADEX_SCOPE_ROLLUP_REQUIRED_FIELDS, artifact_name="scope rollup artifact")
    _require(_text(payload.get("authoritative_rollup_decision")) in {"keep", "hold", "drop"}, "scope rollup artifact authoritative_rollup_decision is required")
    for row in payload.get("session_rows") or []:
        if not isinstance(row, dict):
            continue
        _validate_decision_aliases(row, container_name="scope rollup session_row", explicit_field="session_aggregate_decision", allowed_values=("keep", "hold", "drop"))
        reasons = row.get("decision_reasons")
        _require(isinstance(reasons, list) and bool(reasons), "scope rollup session_row decision_reasons must be a non-empty list")


def validate_run_manifest(payload: dict[str, Any]) -> None:
    _require_fields(payload, required_fields=TRADEX_RUN_MANIFEST_REQUIRED_FIELDS, artifact_name="run manifest")
    _require(_text(payload.get("schema_version")) == TRADEX_RUN_MANIFEST_SCHEMA_VERSION, "run manifest schema_version mismatch")
    _require(isinstance(payload.get("input_artifacts"), list), "run manifest input_artifacts must be a list")
    _require(isinstance(payload.get("config"), dict), "run manifest config must be an object")
    _require(isinstance(payload.get("period"), dict), "run manifest period must be an object")
    _require(isinstance(payload.get("universe"), list), "run manifest universe must be a list")
    _require(isinstance(payload.get("cost_model"), dict), "run manifest cost_model must be an object")
