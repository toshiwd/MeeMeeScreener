from __future__ import annotations

from pathlib import Path

from app.backend.core.legacy_analysis_control import is_legacy_analysis_disabled


def has_active_ml_model(stocks_db: str) -> bool:
    if is_legacy_analysis_disabled():
        return False
    if not Path(stocks_db).is_file():
        return False
    try:
        import duckdb
    except Exception:
        return False
    try:
        with duckdb.connect(stocks_db) as conn:
            row = conn.execute(
                "SELECT 1 FROM ml_model_registry WHERE is_active = TRUE LIMIT 1"
            ).fetchone()
        return row is not None
    except Exception:
        return False


def register_seed_model(stocks_db: str, model_dir: Path, model_version: str) -> None:
    if is_legacy_analysis_disabled():
        return
    try:
        import duckdb
    except Exception:
        return
    cls_path = model_dir / f"{model_version}_cls.txt"
    reg_path = model_dir / f"{model_version}_reg.txt"
    if (not cls_path.exists()) or (not reg_path.exists()):
        return

    turn_up_path = model_dir / f"{model_version}_turn_up.txt"
    turn_down_path = model_dir / f"{model_version}_turn_down.txt"

    horizon_models: dict[str, dict[str, str | None]] = {}
    for horizon in (5, 10, 20):
        cls_h = model_dir / f"{model_version}_cls_{horizon}.txt"
        reg_h = model_dir / f"{model_version}_reg_{horizon}.txt"
        turn_down_h = model_dir / f"{model_version}_turn_down_{horizon}.txt"
        horizon_models[str(horizon)] = {
            "cls_model_path": str(cls_h) if cls_h.exists() else None,
            "reg_model_path": str(reg_h) if reg_h.exists() else None,
            "turn_down_model_path": str(turn_down_h) if turn_down_h.exists() else None,
        }

    artifact = {
        "cls_model_path": str(cls_path),
        "reg_model_path": str(reg_path),
        "turn_up_model_path": str(turn_up_path) if turn_up_path.exists() else None,
        "turn_down_model_path": str(turn_down_path) if turn_down_path.exists() else None,
        "horizon_models": horizon_models,
    }

    model_key = "ml_ev20_simple_v1"
    objective = "ret20_regression_with_p_up_gate"
    feature_version = 2
    label_version = 3

    with duckdb.connect(stocks_db) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS ml_model_registry (
                model_version TEXT PRIMARY KEY,
                model_key TEXT,
                objective TEXT,
                feature_version INTEGER,
                label_version INTEGER,
                train_start_dt INTEGER,
                train_end_dt INTEGER,
                metrics_json TEXT,
                artifact_path TEXT,
                n_train INTEGER,
                created_at TIMESTAMP,
                is_active BOOLEAN
            );
            """
        )
        row = conn.execute(
            "SELECT model_version FROM ml_model_registry WHERE is_active = TRUE LIMIT 1"
        ).fetchone()
        if row:
            return
        conn.execute(
            "UPDATE ml_model_registry SET is_active = FALSE WHERE model_key = ?",
            [model_key],
        )
        conn.execute(
            """
            INSERT OR REPLACE INTO ml_model_registry (
                model_version,
                model_key,
                objective,
                feature_version,
                label_version,
                train_start_dt,
                train_end_dt,
                metrics_json,
                artifact_path,
                n_train,
                created_at,
                is_active
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, TRUE)
            """,
            [
                model_version,
                model_key,
                objective,
                feature_version,
                label_version,
                None,
                None,
                "{}",
                __import__("json").dumps(artifact, ensure_ascii=False),
                0,
            ],
        )
