from __future__ import annotations

import sqlite3
import threading

import duckdb

from app.core.config import FAVORITES_DB_PATH, PRACTICE_DB_PATH, config

_SCHEMA_INIT_LOCK = threading.Lock()


def _init_duckdb_schema(conn: duckdb.DuckDBPyConnection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS tickers (
            code TEXT PRIMARY KEY,
            name TEXT
        );
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS daily_bars (
            code TEXT,
            date INTEGER,
            o DOUBLE,
            h DOUBLE,
            l DOUBLE,
            c DOUBLE,
            v BIGINT,
            PRIMARY KEY(code, date)
        );
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS daily_ma (
            code TEXT,
            date INTEGER,
            ma7 DOUBLE,
            ma20 DOUBLE,
            ma60 DOUBLE,
            PRIMARY KEY(code, date)
        );
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS feature_snapshot_daily (
            dt INTEGER,
            code TEXT,
            close DOUBLE,
            ma7 DOUBLE,
            ma20 DOUBLE,
            ma60 DOUBLE,
            atr14 DOUBLE,
            diff20_pct DOUBLE,
            diff20_atr DOUBLE,
            cnt_20_above INTEGER,
            cnt_7_above INTEGER,
            day_count INTEGER,
            candle_flags TEXT,
            PRIMARY KEY(code, dt)
        );
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS label_20d (
            dt INTEGER,
            code TEXT,
            cont_label INTEGER,
            ex_label INTEGER,
            n_forward INTEGER,
            label_version INTEGER,
            computed_at TIMESTAMP,
            PRIMARY KEY(code, dt)
        );
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS phase_pred_daily (
            dt INTEGER,
            code TEXT,
            early_score DOUBLE,
            late_score DOUBLE,
            body_score DOUBLE,
            n INTEGER,
            reasons_top3 TEXT,
            pred_version INTEGER,
            computed_at TIMESTAMP,
            PRIMARY KEY(code, dt)
        );
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS ml_feature_daily (
            dt INTEGER,
            code TEXT,
            close DOUBLE,
            ma7 DOUBLE,
            ma20 DOUBLE,
            ma60 DOUBLE,
            atr14 DOUBLE,
            diff20_pct DOUBLE,
            cnt_20_above INTEGER,
            cnt_7_above INTEGER,
            close_prev1 DOUBLE,
            close_prev5 DOUBLE,
            close_prev10 DOUBLE,
            ma7_prev1 DOUBLE,
            ma20_prev1 DOUBLE,
            ma60_prev1 DOUBLE,
            diff20_prev1 DOUBLE,
            cnt_20_prev1 INTEGER,
            cnt_7_prev1 INTEGER,
            feature_version INTEGER,
            computed_at TIMESTAMP,
            PRIMARY KEY(code, dt)
        );
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS ml_label_20d (
            dt INTEGER,
            code TEXT,
            ret20 DOUBLE,
            up20_label INTEGER,
            train_mask_cls INTEGER,
            turn_up_label INTEGER,
            turn_down_label INTEGER,
            train_mask_turn INTEGER,
            n_forward INTEGER,
            label_version INTEGER,
            computed_at TIMESTAMP,
            PRIMARY KEY(code, dt)
        );
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS ml_pred_20d (
            dt INTEGER,
            code TEXT,
            p_up DOUBLE,
            p_turn_up DOUBLE,
            p_turn_down DOUBLE,
            ret_pred20 DOUBLE,
            ev20 DOUBLE,
            ev20_net DOUBLE,
            model_version TEXT,
            n_train INTEGER,
            computed_at TIMESTAMP,
            PRIMARY KEY(code, dt)
        );
        """
    )
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
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS strategy_backtest_runs (
            run_id TEXT PRIMARY KEY,
            started_at TIMESTAMP,
            finished_at TIMESTAMP,
            status TEXT,
            start_dt INTEGER,
            end_dt INTEGER,
            max_codes INTEGER,
            config_json TEXT,
            metrics_json TEXT,
            note TEXT
        );
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS toredex_seasons (
            season_id TEXT PRIMARY KEY,
            mode TEXT,
            start_date DATE,
            end_date DATE,
            initial_cash BIGINT,
            policy_version TEXT,
            config_json TEXT,
            config_hash TEXT,
            created_at TIMESTAMP
        );
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS toredex_daily_snapshots (
            season_id TEXT,
            "asOf" DATE,
            snapshot_path TEXT,
            snapshot_hash TEXT,
            payload_json TEXT,
            PRIMARY KEY(season_id, "asOf")
        );
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS toredex_decisions (
            season_id TEXT,
            "asOf" DATE,
            decision_path TEXT,
            decision_hash TEXT,
            payload_json TEXT,
            PRIMARY KEY(season_id, "asOf")
        );
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS toredex_trades (
            season_id TEXT,
            "asOf" DATE,
            trade_id TEXT PRIMARY KEY,
            ticker TEXT,
            side TEXT,
            delta_units INTEGER CHECK (delta_units IN (-5,-3,-2,2,3,5)),
            price DOUBLE,
            reason_id TEXT,
            fees_bps DOUBLE,
            slippage_bps DOUBLE,
            borrow_bps_annual DOUBLE,
            notional DOUBLE,
            fees_cost DOUBLE,
            slippage_cost DOUBLE,
            borrow_cost DOUBLE,
            created_at TIMESTAMP
        );
        """
    )
    for sql in (
        "ALTER TABLE toredex_trades ADD COLUMN slippage_bps DOUBLE",
        "ALTER TABLE toredex_trades ADD COLUMN borrow_bps_annual DOUBLE",
        "ALTER TABLE toredex_trades ADD COLUMN notional DOUBLE",
        "ALTER TABLE toredex_trades ADD COLUMN fees_cost DOUBLE",
        "ALTER TABLE toredex_trades ADD COLUMN slippage_cost DOUBLE",
        "ALTER TABLE toredex_trades ADD COLUMN borrow_cost DOUBLE",
    ):
        try:
            conn.execute(sql)
        except Exception:
            pass
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS toredex_positions (
            season_id TEXT,
            ticker TEXT,
            side TEXT,
            units INTEGER CHECK (units > 0),
            avg_price DOUBLE,
            stage TEXT,
            opened_at DATE,
            holding_days INTEGER,
            pnl_pct DOUBLE,
            PRIMARY KEY(season_id, ticker, side)
        );
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS toredex_daily_metrics (
            season_id TEXT,
            "asOf" DATE,
            cash DOUBLE,
            equity DOUBLE,
            daily_pnl DOUBLE,
            cum_pnl DOUBLE,
            cum_return_pct DOUBLE,
            max_drawdown_pct DOUBLE,
            holdings_count INTEGER,
            goal20_reached BOOLEAN,
            goal30_reached BOOLEAN,
            game_over BOOLEAN,
            gross_daily_pnl DOUBLE,
            gross_cum_pnl DOUBLE,
            gross_cum_return_pct DOUBLE,
            net_daily_pnl DOUBLE,
            net_cum_pnl DOUBLE,
            net_cum_return_pct DOUBLE,
            fees_cost_daily DOUBLE,
            slippage_cost_daily DOUBLE,
            borrow_cost_daily DOUBLE,
            fees_cost_cum DOUBLE,
            slippage_cost_cum DOUBLE,
            borrow_cost_cum DOUBLE,
            turnover_notional_daily DOUBLE,
            turnover_notional_cum DOUBLE,
            turnover_pct_daily DOUBLE,
            long_units INTEGER,
            short_units INTEGER,
            gross_units INTEGER,
            net_units INTEGER,
            net_exposure_pct DOUBLE,
            risk_gate_pass BOOLEAN,
            risk_gate_reason TEXT,
            cost_sensitivity_json TEXT,
            PRIMARY KEY(season_id, "asOf")
        );
        """
    )
    for sql in (
        "ALTER TABLE toredex_daily_metrics ADD COLUMN gross_daily_pnl DOUBLE",
        "ALTER TABLE toredex_daily_metrics ADD COLUMN gross_cum_pnl DOUBLE",
        "ALTER TABLE toredex_daily_metrics ADD COLUMN gross_cum_return_pct DOUBLE",
        "ALTER TABLE toredex_daily_metrics ADD COLUMN net_daily_pnl DOUBLE",
        "ALTER TABLE toredex_daily_metrics ADD COLUMN net_cum_pnl DOUBLE",
        "ALTER TABLE toredex_daily_metrics ADD COLUMN net_cum_return_pct DOUBLE",
        "ALTER TABLE toredex_daily_metrics ADD COLUMN fees_cost_daily DOUBLE",
        "ALTER TABLE toredex_daily_metrics ADD COLUMN slippage_cost_daily DOUBLE",
        "ALTER TABLE toredex_daily_metrics ADD COLUMN borrow_cost_daily DOUBLE",
        "ALTER TABLE toredex_daily_metrics ADD COLUMN fees_cost_cum DOUBLE",
        "ALTER TABLE toredex_daily_metrics ADD COLUMN slippage_cost_cum DOUBLE",
        "ALTER TABLE toredex_daily_metrics ADD COLUMN borrow_cost_cum DOUBLE",
        "ALTER TABLE toredex_daily_metrics ADD COLUMN turnover_notional_daily DOUBLE",
        "ALTER TABLE toredex_daily_metrics ADD COLUMN turnover_notional_cum DOUBLE",
        "ALTER TABLE toredex_daily_metrics ADD COLUMN turnover_pct_daily DOUBLE",
        "ALTER TABLE toredex_daily_metrics ADD COLUMN long_units INTEGER",
        "ALTER TABLE toredex_daily_metrics ADD COLUMN short_units INTEGER",
        "ALTER TABLE toredex_daily_metrics ADD COLUMN gross_units INTEGER",
        "ALTER TABLE toredex_daily_metrics ADD COLUMN net_units INTEGER",
        "ALTER TABLE toredex_daily_metrics ADD COLUMN net_exposure_pct DOUBLE",
        "ALTER TABLE toredex_daily_metrics ADD COLUMN risk_gate_pass BOOLEAN",
        "ALTER TABLE toredex_daily_metrics ADD COLUMN risk_gate_reason TEXT",
        "ALTER TABLE toredex_daily_metrics ADD COLUMN cost_sensitivity_json TEXT",
    ):
        try:
            conn.execute(sql)
        except Exception:
            pass

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS toredex_optimization_runs (
            run_id TEXT PRIMARY KEY,
            config_hash TEXT,
            git_commit TEXT,
            operating_mode TEXT,
            season_id TEXT,
            stage TEXT,
            stage_order INTEGER,
            start_date DATE,
            end_date DATE,
            status TEXT,
            score_net_return_pct DOUBLE,
            max_drawdown_pct DOUBLE,
            worst_month_pct DOUBLE,
            turnover_pct_avg DOUBLE,
            net_exposure_units_max DOUBLE,
            metrics_json TEXT,
            artifact_path TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
    )
    try:
        conn.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS ux_toredex_opt_runs_dedup
            ON toredex_optimization_runs(config_hash, stage, start_date, end_date, operating_mode)
            """
        )
    except Exception:
        pass
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS toredex_logs (
            season_id TEXT,
            "asOf" DATE,
            log_path TEXT,
            kind TEXT,
            PRIMARY KEY(season_id, "asOf", kind)
        );
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS monthly_bars (
            code TEXT,
            month INTEGER,
            o DOUBLE,
            h DOUBLE,
            l DOUBLE,
            c DOUBLE,
            v BIGINT,
            PRIMARY KEY(code, month)
        );
        """
    )
    try:
        conn.execute("ALTER TABLE monthly_bars ADD COLUMN v BIGINT")
    except Exception:
        pass
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS monthly_ma (
            code TEXT,
            month INTEGER,
            ma7 DOUBLE,
            ma20 DOUBLE,
            ma60 DOUBLE,
            PRIMARY KEY(code, month)
        );
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS sys_jobs (
            id TEXT PRIMARY KEY,
            type TEXT,
            status TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            started_at TIMESTAMP,
            finished_at TIMESTAMP,
            progress INTEGER,
            message TEXT,
            error TEXT
        );
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS daily_memos (
            symbol TEXT,
            date TEXT,
            timeframe TEXT,
            memo TEXT,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (symbol, date, timeframe)
        );
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS edinetdb_company_map (
            sec_code TEXT PRIMARY KEY,
            edinet_code TEXT,
            name TEXT,
            industry TEXT,
            updated_at TIMESTAMP
        );
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS edinetdb_company_latest (
            edinet_code TEXT PRIMARY KEY,
            latest_fiscal_year TEXT,
            latest_hash TEXT,
            fetched_at TIMESTAMP,
            last_checked_at TIMESTAMP
        );
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS edinetdb_financials (
            edinet_code TEXT,
            fiscal_year TEXT,
            accounting_standard TEXT,
            payload_json TEXT,
            fetched_at TIMESTAMP,
            PRIMARY KEY(edinet_code, fiscal_year, accounting_standard)
        );
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS edinetdb_ratios (
            edinet_code TEXT,
            fiscal_year TEXT,
            accounting_standard TEXT,
            payload_json TEXT,
            fetched_at TIMESTAMP,
            PRIMARY KEY(edinet_code, fiscal_year, accounting_standard)
        );
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS edinetdb_text_blocks (
            edinet_code TEXT,
            fiscal_year TEXT,
            block_name TEXT,
            text TEXT,
            fetched_at TIMESTAMP,
            PRIMARY KEY(edinet_code, fiscal_year, block_name)
        );
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS edinetdb_analysis (
            edinet_code TEXT,
            asof_date TEXT,
            payload_json TEXT,
            fetched_at TIMESTAMP,
            PRIMARY KEY(edinet_code, asof_date)
        );
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS edinetdb_task_queue (
            task_key TEXT PRIMARY KEY,
            job_name TEXT,
            phase TEXT,
            edinet_code TEXT,
            endpoint TEXT,
            params_json TEXT,
            priority INTEGER,
            status TEXT,
            tries INTEGER DEFAULT 0,
            http_status INTEGER,
            last_error TEXT,
            retry_at TIMESTAMP,
            fetched_at TIMESTAMP,
            created_at TIMESTAMP,
            updated_at TIMESTAMP
        );
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS edinetdb_api_call_log (
            id TEXT PRIMARY KEY,
            called_at TIMESTAMP,
            jst_date DATE,
            job_name TEXT,
            endpoint TEXT,
            edinet_code TEXT,
            http_status INTEGER,
            error_type TEXT
        );
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS edinetdb_unmapped_codes (
            sec_code TEXT PRIMARY KEY,
            reason TEXT,
            first_seen_at TIMESTAMP,
            last_seen_at TIMESTAMP
        );
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS edinetdb_meta (
            key TEXT PRIMARY KEY,
            value_json TEXT,
            updated_at TIMESTAMP
        );
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS ranking_analysis_quality_daily (
            as_of_ymd INTEGER,
            scope TEXT,
            precision_top30_20d DOUBLE,
            avg_ret20_net DOUBLE,
            ece DOUBLE,
            samples INTEGER,
            decision_match_rate DOUBLE,
            decision_match_samples INTEGER,
            rolling_precision_delta_pt DOUBLE,
            rolling_avg_ret_delta DOUBLE,
            rolling_target_met BOOLEAN,
            up_gate_defensive DOUBLE,
            up_gate_balanced DOUBLE,
            up_gate_aggressive DOUBLE,
            table_health_json TEXT,
            alerts_json TEXT,
            computed_at TIMESTAMP,
            updated_at TIMESTAMP,
            PRIMARY KEY(as_of_ymd, scope)
        );
        """
    )

    # Trade history / positions (used by /api/trades and Positions UI).
    # Keep schemas compatible with legacy inserts from `app.backend.import_positions`.
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS trade_events (
            -- Legacy code sometimes selects `id` but current ingest does not populate it.
            id BIGINT,
            broker TEXT,
            exec_dt TIMESTAMP,
            symbol TEXT,
            action TEXT,
            qty DOUBLE,
            price DOUBLE,
            source_row_hash TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            transaction_type TEXT,
            side_type TEXT,
            margin_type TEXT,
            UNIQUE(source_row_hash)
        );
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS initial_positions_seed (
            symbol TEXT PRIMARY KEY,
            buy_qty DOUBLE,
            sell_qty DOUBLE,
            asof_dt TIMESTAMP,
            memo TEXT
        );
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS positions_live (
            symbol TEXT PRIMARY KEY,
            spot_qty DOUBLE,
            margin_long_qty DOUBLE,
            margin_short_qty DOUBLE,
            buy_qty DOUBLE,
            sell_qty DOUBLE,
            opened_at TIMESTAMP,
            updated_at TIMESTAMP,
            has_issue BOOLEAN,
            issue_note TEXT
        );
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS position_rounds (
            round_id TEXT PRIMARY KEY,
            symbol TEXT,
            opened_at TIMESTAMP,
            closed_at TIMESTAMP,
            closed_reason TEXT,
            last_state_sell_buy TEXT,
            has_issue BOOLEAN,
            issue_note TEXT
        );
        """
    )


def ensure_schema(conn: duckdb.DuckDBPyConnection) -> None:
    # CREATE TABLE IF NOT EXISTS is cheap and safe; avoid caching because the app
    # can switch DB paths via env vars (launcher/tests) and we must always ensure
    # the target DB has all required tables.
    with _SCHEMA_INIT_LOCK:
        _init_duckdb_schema(conn)


def init_schema() -> None:
    # Lazy import to avoid module import cycle (`session` imports `schema`).
    from app.db.session import get_conn_for_path

    with get_conn_for_path(str(config.DB_PATH), timeout_sec=2.5, read_only=False) as conn:
        ensure_schema(conn)


def _get_favorites_conn():
    conn = sqlite3.connect(FAVORITES_DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def _get_practice_conn():
    conn = sqlite3.connect(PRACTICE_DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def _ensure_practice_column(conn: sqlite3.Connection, table: str, column: str, column_type: str) -> None:
    existing = conn.execute(f"PRAGMA table_info({table})").fetchall()
    if any(row[1] == column for row in existing):
        return
    conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {column_type}")


def _init_favorites_schema() -> None:
    with _get_favorites_conn() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS favorites (
                code TEXT PRIMARY KEY,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """
        )


def _init_practice_schema() -> None:
    with _get_practice_conn() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS practice_sessions (
                session_id TEXT PRIMARY KEY,
                code TEXT NOT NULL,
                start_date TEXT,
                end_date TEXT,
                cursor_time INTEGER,
                max_unlocked_time INTEGER,
                lot_size INTEGER,
                range_months INTEGER,
                trades TEXT,
                notes TEXT,
                ui_state TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """
        )
        _ensure_practice_column(conn, "practice_sessions", "end_date", "TEXT")
        _ensure_practice_column(conn, "practice_sessions", "cursor_time", "INTEGER")
        _ensure_practice_column(conn, "practice_sessions", "max_unlocked_time", "INTEGER")
        _ensure_practice_column(conn, "practice_sessions", "lot_size", "INTEGER")
        _ensure_practice_column(conn, "practice_sessions", "range_months", "INTEGER")
        _ensure_practice_column(conn, "practice_sessions", "ui_state", "TEXT")


def init_extra_schemas() -> None:
    _init_favorites_schema()
    _init_practice_schema()
