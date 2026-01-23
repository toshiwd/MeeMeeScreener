

import duckdb
from core.config import config

def get_conn():
    return duckdb.connect(str(config.DB_PATH))


def init_schema() -> None:
    with get_conn() as conn:
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
            pass  # Already exists
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
            CREATE TABLE IF NOT EXISTS stock_meta (
                code TEXT PRIMARY KEY,
                name TEXT,
                stage TEXT,
                score DOUBLE,
                reason TEXT,
                score_status TEXT,
                missing_reasons_json TEXT,
                score_breakdown_json TEXT,
                latest_close REAL,
                monthly_box_status TEXT,
                box_duration INTEGER,
                box_upper REAL,
                box_lower REAL,
                ma20_monthly_trend INTEGER,
                days_since_peak INTEGER,
                days_since_bottom INTEGER,
                signal_flags TEXT,
                updated_at TIMESTAMP
            );
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS earnings_planned (
                code TEXT,
                planned_date DATE,
                kind TEXT,
                company_name TEXT,
                source TEXT,
                fetched_at TIMESTAMP
            );
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS ex_rights (
                code TEXT,
                ex_date DATE,
                record_date DATE,
                category TEXT,
                last_rights_date DATE,
                source TEXT,
                fetched_at TIMESTAMP
            );
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS events_meta (
                id INTEGER PRIMARY KEY,
                earnings_last_success_at TIMESTAMP,
                rights_last_success_at TIMESTAMP,
                last_error TEXT,
                last_attempt_at TIMESTAMP,
                is_refreshing BOOLEAN,
                refresh_lock_job_id TEXT,
                refresh_lock_started_at TIMESTAMP
            );
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS events_refresh_jobs (
                job_id TEXT PRIMARY KEY,
                status TEXT,
                reason TEXT,
                started_at TIMESTAMP,
                finished_at TIMESTAMP,
                error TEXT
            );
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS trade_events (
                id BIGINT,
                broker TEXT,
                exec_dt TIMESTAMP,
                symbol TEXT,
                action TEXT,
                qty DOUBLE,
                price DOUBLE,
                source_row_hash TEXT UNIQUE,
                transaction_type TEXT,
                side_type TEXT,
                margin_type TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS positions_live (
                symbol TEXT PRIMARY KEY,
                spot_qty DOUBLE DEFAULT 0,
                margin_long_qty DOUBLE DEFAULT 0,
                margin_short_qty DOUBLE DEFAULT 0,
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
            CREATE TABLE IF NOT EXISTS daily_memo (
                symbol TEXT NOT NULL,
                date DATE NOT NULL,
                timeframe TEXT NOT NULL DEFAULT 'D',
                memo TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (symbol, date, timeframe)
            );
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS sys_jobs (
                id TEXT PRIMARY KEY,
                type TEXT NOT NULL,
                status TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                started_at TIMESTAMP,
                finished_at TIMESTAMP,
                progress INTEGER DEFAULT 0,
                message TEXT,
                error TEXT,
                meta_json TEXT
            );
            """
        )
        def add_col(table, col_def):
            try:
                # Try simple add column
                # Extract column name for check? duckdb "ADD COLUMN IF NOT EXISTS" is standard but if it fails?
                # Actually, catch the specific error.
                conn.execute(f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {col_def}")
            except Exception:
                pass

        add_col("stock_meta", "score_status TEXT")
        add_col("stock_meta", "missing_reasons_json TEXT")
        add_col("stock_meta", "score_breakdown_json TEXT")
        add_col("stock_meta", "latest_close REAL")
        add_col("stock_meta", "monthly_box_status TEXT")
        add_col("stock_meta", "box_duration INTEGER")
        add_col("stock_meta", "box_upper REAL")
        add_col("stock_meta", "box_lower REAL")
        add_col("stock_meta", "ma20_monthly_trend INTEGER")
        add_col("stock_meta", "days_since_peak INTEGER")
        add_col("stock_meta", "days_since_bottom INTEGER")
        add_col("stock_meta", "signal_flags TEXT")
        add_col("earnings_planned", "company_name TEXT")
        add_col("earnings_planned", "source TEXT")
        add_col("earnings_planned", "fetched_at TIMESTAMP")
        add_col("ex_rights", "record_date DATE")
        add_col("ex_rights", "category TEXT")
        add_col("ex_rights", "last_rights_date DATE")
        add_col("ex_rights", "source TEXT")
        add_col("ex_rights", "fetched_at TIMESTAMP")
        add_col("events_meta", "earnings_last_success_at TIMESTAMP")
        add_col("events_meta", "rights_last_success_at TIMESTAMP")
        add_col("events_meta", "last_error TEXT")
        add_col("events_meta", "last_attempt_at TIMESTAMP")
        add_col("events_meta", "is_refreshing BOOLEAN")
        add_col("events_meta", "refresh_lock_job_id TEXT")
        add_col("events_meta", "refresh_lock_started_at TIMESTAMP")
        add_col("trade_events", "broker TEXT")
        add_col("trade_events", "exec_dt TIMESTAMP")
        add_col("trade_events", "symbol TEXT")
        add_col("trade_events", "action TEXT")
        add_col("trade_events", "qty DOUBLE")
        add_col("trade_events", "price DOUBLE")
        add_col("trade_events", "source_row_hash TEXT")
        add_col("positions_live", "buy_qty DOUBLE")
        add_col("positions_live", "sell_qty DOUBLE")
        add_col("positions_live", "opened_at TIMESTAMP")
        add_col("positions_live", "updated_at TIMESTAMP")
        add_col("positions_live", "has_issue BOOLEAN")
        add_col("positions_live", "issue_note TEXT")
        add_col("position_rounds", "symbol TEXT")
        add_col("position_rounds", "opened_at TIMESTAMP")
        add_col("position_rounds", "closed_at TIMESTAMP")
        add_col("position_rounds", "closed_reason TEXT")
        add_col("position_rounds", "has_issue BOOLEAN")
        add_col("positions_live", "spot_qty DOUBLE DEFAULT 0")
        add_col("positions_live", "margin_long_qty DOUBLE DEFAULT 0")
        add_col("positions_live", "margin_short_qty DOUBLE DEFAULT 0")
        add_col("trade_events", "transaction_type TEXT")
        add_col("trade_events", "side_type TEXT")
        add_col("trade_events", "margin_type TEXT")
        add_col("position_rounds", "issue_note TEXT")
        add_col("initial_positions_seed", "buy_qty DOUBLE")
        add_col("initial_positions_seed", "sell_qty DOUBLE")
        add_col("initial_positions_seed", "asof_dt TIMESTAMP")
        add_col("initial_positions_seed", "memo TEXT")
