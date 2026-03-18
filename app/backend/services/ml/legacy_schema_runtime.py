from __future__ import annotations

from app.db.schema import ensure_legacy_analysis_schema

_ML_FEATURE_DAILY_MIGRATIONS: tuple[str, ...] = (
    "ALTER TABLE ml_feature_daily ADD COLUMN IF NOT EXISTS close_prev1 DOUBLE",
    "ALTER TABLE ml_feature_daily ADD COLUMN IF NOT EXISTS close_prev5 DOUBLE",
    "ALTER TABLE ml_feature_daily ADD COLUMN IF NOT EXISTS close_prev10 DOUBLE",
    "ALTER TABLE ml_feature_daily ADD COLUMN IF NOT EXISTS close_ret2 DOUBLE",
    "ALTER TABLE ml_feature_daily ADD COLUMN IF NOT EXISTS close_ret3 DOUBLE",
    "ALTER TABLE ml_feature_daily ADD COLUMN IF NOT EXISTS close_ret20 DOUBLE",
    "ALTER TABLE ml_feature_daily ADD COLUMN IF NOT EXISTS close_ret60 DOUBLE",
    "ALTER TABLE ml_feature_daily ADD COLUMN IF NOT EXISTS ma7_prev1 DOUBLE",
    "ALTER TABLE ml_feature_daily ADD COLUMN IF NOT EXISTS ma20_prev1 DOUBLE",
    "ALTER TABLE ml_feature_daily ADD COLUMN IF NOT EXISTS ma60_prev1 DOUBLE",
    "ALTER TABLE ml_feature_daily ADD COLUMN IF NOT EXISTS atr14_pct DOUBLE",
    "ALTER TABLE ml_feature_daily ADD COLUMN IF NOT EXISTS range_pct DOUBLE",
    "ALTER TABLE ml_feature_daily ADD COLUMN IF NOT EXISTS gap_pct DOUBLE",
    "ALTER TABLE ml_feature_daily ADD COLUMN IF NOT EXISTS diff20_prev1 DOUBLE",
    "ALTER TABLE ml_feature_daily ADD COLUMN IF NOT EXISTS cnt_20_prev1 INTEGER",
    "ALTER TABLE ml_feature_daily ADD COLUMN IF NOT EXISTS cnt_7_prev1 INTEGER",
    "ALTER TABLE ml_feature_daily ADD COLUMN IF NOT EXISTS vol_ret5 DOUBLE",
    "ALTER TABLE ml_feature_daily ADD COLUMN IF NOT EXISTS vol_ret20 DOUBLE",
    "ALTER TABLE ml_feature_daily ADD COLUMN IF NOT EXISTS vol_ratio5_20 DOUBLE",
    "ALTER TABLE ml_feature_daily ADD COLUMN IF NOT EXISTS turnover20 DOUBLE",
    "ALTER TABLE ml_feature_daily ADD COLUMN IF NOT EXISTS turnover_z20 DOUBLE",
    "ALTER TABLE ml_feature_daily ADD COLUMN IF NOT EXISTS high20_dist DOUBLE",
    "ALTER TABLE ml_feature_daily ADD COLUMN IF NOT EXISTS low20_dist DOUBLE",
    "ALTER TABLE ml_feature_daily ADD COLUMN IF NOT EXISTS breakout20_up DOUBLE",
    "ALTER TABLE ml_feature_daily ADD COLUMN IF NOT EXISTS breakout20_down DOUBLE",
    "ALTER TABLE ml_feature_daily ADD COLUMN IF NOT EXISTS drawdown60 DOUBLE",
    "ALTER TABLE ml_feature_daily ADD COLUMN IF NOT EXISTS rebound60 DOUBLE",
    "ALTER TABLE ml_feature_daily ADD COLUMN IF NOT EXISTS market_ret1 DOUBLE",
    "ALTER TABLE ml_feature_daily ADD COLUMN IF NOT EXISTS market_ret5 DOUBLE",
    "ALTER TABLE ml_feature_daily ADD COLUMN IF NOT EXISTS market_ret20 DOUBLE",
    "ALTER TABLE ml_feature_daily ADD COLUMN IF NOT EXISTS rel_ret5 DOUBLE",
    "ALTER TABLE ml_feature_daily ADD COLUMN IF NOT EXISTS rel_ret20 DOUBLE",
    "ALTER TABLE ml_feature_daily ADD COLUMN IF NOT EXISTS breadth_above_ma20 DOUBLE",
    "ALTER TABLE ml_feature_daily ADD COLUMN IF NOT EXISTS breadth_above_ma60 DOUBLE",
    "ALTER TABLE ml_feature_daily ADD COLUMN IF NOT EXISTS sector_ret5 DOUBLE",
    "ALTER TABLE ml_feature_daily ADD COLUMN IF NOT EXISTS sector_ret20 DOUBLE",
    "ALTER TABLE ml_feature_daily ADD COLUMN IF NOT EXISTS rel_sector_ret5 DOUBLE",
    "ALTER TABLE ml_feature_daily ADD COLUMN IF NOT EXISTS rel_sector_ret20 DOUBLE",
    "ALTER TABLE ml_feature_daily ADD COLUMN IF NOT EXISTS sector_breadth_ma20 DOUBLE",
    "ALTER TABLE ml_feature_daily ADD COLUMN IF NOT EXISTS weekly_breakout_up_prob DOUBLE",
    "ALTER TABLE ml_feature_daily ADD COLUMN IF NOT EXISTS weekly_breakout_down_prob DOUBLE",
    "ALTER TABLE ml_feature_daily ADD COLUMN IF NOT EXISTS weekly_range_prob DOUBLE",
    "ALTER TABLE ml_feature_daily ADD COLUMN IF NOT EXISTS monthly_breakout_up_prob DOUBLE",
    "ALTER TABLE ml_feature_daily ADD COLUMN IF NOT EXISTS monthly_breakout_down_prob DOUBLE",
    "ALTER TABLE ml_feature_daily ADD COLUMN IF NOT EXISTS monthly_range_prob DOUBLE",
    "ALTER TABLE ml_feature_daily ADD COLUMN IF NOT EXISTS candle_triplet_up_prob DOUBLE",
    "ALTER TABLE ml_feature_daily ADD COLUMN IF NOT EXISTS candle_triplet_down_prob DOUBLE",
    "ALTER TABLE ml_feature_daily ADD COLUMN IF NOT EXISTS candle_body_ratio DOUBLE",
    "ALTER TABLE ml_feature_daily ADD COLUMN IF NOT EXISTS candle_upper_wick_ratio DOUBLE",
    "ALTER TABLE ml_feature_daily ADD COLUMN IF NOT EXISTS candle_lower_wick_ratio DOUBLE",
)

_ML_LABEL_DAILY_MIGRATIONS: tuple[str, ...] = (
    "ALTER TABLE ml_label_20d ADD COLUMN IF NOT EXISTS turn_up_label INTEGER",
    "ALTER TABLE ml_label_20d ADD COLUMN IF NOT EXISTS turn_down_label INTEGER",
    "ALTER TABLE ml_label_20d ADD COLUMN IF NOT EXISTS train_mask_turn INTEGER",
    "ALTER TABLE ml_label_20d ADD COLUMN IF NOT EXISTS ret5 DOUBLE",
    "ALTER TABLE ml_label_20d ADD COLUMN IF NOT EXISTS ret10 DOUBLE",
    "ALTER TABLE ml_label_20d ADD COLUMN IF NOT EXISTS up5_label INTEGER",
    "ALTER TABLE ml_label_20d ADD COLUMN IF NOT EXISTS up10_label INTEGER",
    "ALTER TABLE ml_label_20d ADD COLUMN IF NOT EXISTS train_mask_cls_5 INTEGER",
    "ALTER TABLE ml_label_20d ADD COLUMN IF NOT EXISTS train_mask_cls_10 INTEGER",
    "ALTER TABLE ml_label_20d ADD COLUMN IF NOT EXISTS turn_down_label_5 INTEGER",
    "ALTER TABLE ml_label_20d ADD COLUMN IF NOT EXISTS turn_down_label_20 INTEGER",
    "ALTER TABLE ml_label_20d ADD COLUMN IF NOT EXISTS turn_down_reversion_label_5 INTEGER",
    "ALTER TABLE ml_label_20d ADD COLUMN IF NOT EXISTS turn_down_reversion_label_10 INTEGER",
    "ALTER TABLE ml_label_20d ADD COLUMN IF NOT EXISTS turn_down_reversion_label_20 INTEGER",
    "ALTER TABLE ml_label_20d ADD COLUMN IF NOT EXISTS turn_down_break_label_5 INTEGER",
    "ALTER TABLE ml_label_20d ADD COLUMN IF NOT EXISTS turn_down_break_label_10 INTEGER",
    "ALTER TABLE ml_label_20d ADD COLUMN IF NOT EXISTS turn_down_break_label_20 INTEGER",
    "ALTER TABLE ml_label_20d ADD COLUMN IF NOT EXISTS train_mask_turn_5 INTEGER",
    "ALTER TABLE ml_label_20d ADD COLUMN IF NOT EXISTS train_mask_turn_20 INTEGER",
)

_ML_PRED_DAILY_MIGRATIONS: tuple[str, ...] = (
    "ALTER TABLE ml_pred_20d ADD COLUMN IF NOT EXISTS p_turn_up DOUBLE",
    "ALTER TABLE ml_pred_20d ADD COLUMN IF NOT EXISTS p_turn_down DOUBLE",
    "ALTER TABLE ml_pred_20d ADD COLUMN IF NOT EXISTS p_down DOUBLE",
    "ALTER TABLE ml_pred_20d ADD COLUMN IF NOT EXISTS rank_up_20 DOUBLE",
    "ALTER TABLE ml_pred_20d ADD COLUMN IF NOT EXISTS rank_down_20 DOUBLE",
    "ALTER TABLE ml_pred_20d ADD COLUMN IF NOT EXISTS p_up_5 DOUBLE",
    "ALTER TABLE ml_pred_20d ADD COLUMN IF NOT EXISTS p_up_10 DOUBLE",
    "ALTER TABLE ml_pred_20d ADD COLUMN IF NOT EXISTS ret_pred5 DOUBLE",
    "ALTER TABLE ml_pred_20d ADD COLUMN IF NOT EXISTS ret_pred10 DOUBLE",
    "ALTER TABLE ml_pred_20d ADD COLUMN IF NOT EXISTS ev5 DOUBLE",
    "ALTER TABLE ml_pred_20d ADD COLUMN IF NOT EXISTS ev10 DOUBLE",
    "ALTER TABLE ml_pred_20d ADD COLUMN IF NOT EXISTS ev5_net DOUBLE",
    "ALTER TABLE ml_pred_20d ADD COLUMN IF NOT EXISTS ev10_net DOUBLE",
    "ALTER TABLE ml_pred_20d ADD COLUMN IF NOT EXISTS p_turn_down_5 DOUBLE",
    "ALTER TABLE ml_pred_20d ADD COLUMN IF NOT EXISTS p_turn_down_10 DOUBLE",
    "ALTER TABLE ml_pred_20d ADD COLUMN IF NOT EXISTS p_turn_down_20 DOUBLE",
)

_MONTHLY_AND_AUDIT_SCHEMA: tuple[str, ...] = (
    """
    CREATE TABLE IF NOT EXISTS ml_monthly_label (
        dt INTEGER,
        code TEXT,
        ret1m DOUBLE,
        up_big INTEGER,
        down_big INTEGER,
        abs_big INTEGER,
        dir_up INTEGER,
        liquidity_proxy DOUBLE,
        liquidity_pass INTEGER,
        label_version INTEGER,
        computed_at TIMESTAMP,
        PRIMARY KEY(code, dt)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS ml_monthly_pred (
        dt INTEGER,
        code TEXT,
        p_abs_big DOUBLE,
        p_up_given_big DOUBLE,
        p_up_big DOUBLE,
        p_down_big DOUBLE,
        score_up DOUBLE,
        score_down DOUBLE,
        model_version TEXT,
        n_train_abs INTEGER,
        n_train_dir INTEGER,
        computed_at TIMESTAMP,
        PRIMARY KEY(code, dt)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS ml_monthly_model_registry (
        model_version TEXT PRIMARY KEY,
        model_key TEXT,
        label_version INTEGER,
        metrics_json TEXT,
        artifact_path TEXT,
        n_train_abs INTEGER,
        n_train_dir INTEGER,
        created_at TIMESTAMP,
        is_active BOOLEAN
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS ml_training_audit (
        run_id TEXT PRIMARY KEY,
        trained_at TIMESTAMP,
        model_version TEXT,
        promoted BOOLEAN,
        reason TEXT,
        wf_fold_count INTEGER,
        wf_daily_count INTEGER,
        wf_mean_ret20_net DOUBLE,
        wf_win_rate DOUBLE,
        wf_p05_ret20_net DOUBLE,
        wf_cvar05_ret20_net DOUBLE,
        wf_lcb95_ret20_net DOUBLE,
        wf_p_value_mean_gt0 DOUBLE,
        wf_robust_lb DOUBLE,
        gate_json TEXT,
        metrics_json TEXT
    );
    """,
    "ALTER TABLE ml_training_audit ADD COLUMN IF NOT EXISTS wf_lcb95_ret20_net DOUBLE",
    "ALTER TABLE ml_training_audit ADD COLUMN IF NOT EXISTS wf_p_value_mean_gt0 DOUBLE",
    """
    CREATE TABLE IF NOT EXISTS ml_live_guard_audit (
        run_id TEXT PRIMARY KEY,
        checked_at TIMESTAMP,
        active_model_version TEXT,
        passed BOOLEAN,
        action TEXT,
        reason TEXT,
        daily_count INTEGER,
        mean_ret20_net DOUBLE,
        win_rate DOUBLE,
        p05_ret20_net DOUBLE,
        cvar05_ret20_net DOUBLE,
        recent_days INTEGER,
        metrics_json TEXT
    );
    """,
)


def ensure_ml_runtime_schema(conn, *, legacy_schema_enabled: bool) -> None:
    if legacy_schema_enabled:
        ensure_legacy_analysis_schema(conn)
    for sql in _MONTHLY_AND_AUDIT_SCHEMA:
        conn.execute(sql)
    if not legacy_schema_enabled:
        return
    for sql in _ML_FEATURE_DAILY_MIGRATIONS:
        conn.execute(sql)
    for sql in _ML_LABEL_DAILY_MIGRATIONS:
        conn.execute(sql)
    for sql in _ML_PRED_DAILY_MIGRATIONS:
        conn.execute(sql)
