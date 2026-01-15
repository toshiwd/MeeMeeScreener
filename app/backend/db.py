import os
import duckdb

DEFAULT_DB_PATH = os.path.join(os.path.dirname(__file__), "stocks.duckdb")


def get_conn():
    db_path = os.getenv("STOCKS_DB_PATH", DEFAULT_DB_PATH)
    return duckdb.connect(db_path)


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
        except duckdb.BinderException:
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
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS positions_live (
                symbol TEXT PRIMARY KEY,
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
        conn.execute("ALTER TABLE stock_meta ADD COLUMN IF NOT EXISTS score_status TEXT;")
        conn.execute("ALTER TABLE stock_meta ADD COLUMN IF NOT EXISTS missing_reasons_json TEXT;")
        conn.execute("ALTER TABLE stock_meta ADD COLUMN IF NOT EXISTS score_breakdown_json TEXT;")
        conn.execute("ALTER TABLE stock_meta ADD COLUMN IF NOT EXISTS latest_close REAL;")
        conn.execute("ALTER TABLE stock_meta ADD COLUMN IF NOT EXISTS monthly_box_status TEXT;")
        conn.execute("ALTER TABLE stock_meta ADD COLUMN IF NOT EXISTS box_duration INTEGER;")
        conn.execute("ALTER TABLE stock_meta ADD COLUMN IF NOT EXISTS box_upper REAL;")
        conn.execute("ALTER TABLE stock_meta ADD COLUMN IF NOT EXISTS box_lower REAL;")
        conn.execute("ALTER TABLE stock_meta ADD COLUMN IF NOT EXISTS ma20_monthly_trend INTEGER;")
        conn.execute("ALTER TABLE stock_meta ADD COLUMN IF NOT EXISTS days_since_peak INTEGER;")
        conn.execute("ALTER TABLE stock_meta ADD COLUMN IF NOT EXISTS days_since_bottom INTEGER;")
        conn.execute("ALTER TABLE stock_meta ADD COLUMN IF NOT EXISTS signal_flags TEXT;")
        conn.execute("ALTER TABLE earnings_planned ADD COLUMN IF NOT EXISTS company_name TEXT;")
        conn.execute("ALTER TABLE earnings_planned ADD COLUMN IF NOT EXISTS source TEXT;")
        conn.execute("ALTER TABLE earnings_planned ADD COLUMN IF NOT EXISTS fetched_at TIMESTAMP;")
        conn.execute("ALTER TABLE ex_rights ADD COLUMN IF NOT EXISTS record_date DATE;")
        conn.execute("ALTER TABLE ex_rights ADD COLUMN IF NOT EXISTS category TEXT;")
        conn.execute("ALTER TABLE ex_rights ADD COLUMN IF NOT EXISTS last_rights_date DATE;")
        conn.execute("ALTER TABLE ex_rights ADD COLUMN IF NOT EXISTS source TEXT;")
        conn.execute("ALTER TABLE ex_rights ADD COLUMN IF NOT EXISTS fetched_at TIMESTAMP;")
        conn.execute("ALTER TABLE events_meta ADD COLUMN IF NOT EXISTS earnings_last_success_at TIMESTAMP;")
        conn.execute("ALTER TABLE events_meta ADD COLUMN IF NOT EXISTS rights_last_success_at TIMESTAMP;")
        conn.execute("ALTER TABLE events_meta ADD COLUMN IF NOT EXISTS last_error TEXT;")
        conn.execute("ALTER TABLE events_meta ADD COLUMN IF NOT EXISTS last_attempt_at TIMESTAMP;")
        conn.execute("ALTER TABLE events_meta ADD COLUMN IF NOT EXISTS is_refreshing BOOLEAN;")
        conn.execute("ALTER TABLE events_meta ADD COLUMN IF NOT EXISTS refresh_lock_job_id TEXT;")
        conn.execute("ALTER TABLE events_meta ADD COLUMN IF NOT EXISTS refresh_lock_started_at TIMESTAMP;")
        conn.execute("ALTER TABLE trade_events ADD COLUMN IF NOT EXISTS broker TEXT;")
        conn.execute("ALTER TABLE trade_events ADD COLUMN IF NOT EXISTS exec_dt TIMESTAMP;")
        conn.execute("ALTER TABLE trade_events ADD COLUMN IF NOT EXISTS symbol TEXT;")
        conn.execute("ALTER TABLE trade_events ADD COLUMN IF NOT EXISTS action TEXT;")
        conn.execute("ALTER TABLE trade_events ADD COLUMN IF NOT EXISTS qty DOUBLE;")
        conn.execute("ALTER TABLE trade_events ADD COLUMN IF NOT EXISTS price DOUBLE;")
        conn.execute("ALTER TABLE trade_events ADD COLUMN IF NOT EXISTS source_row_hash TEXT;")
        conn.execute("ALTER TABLE positions_live ADD COLUMN IF NOT EXISTS buy_qty DOUBLE;")
        conn.execute("ALTER TABLE positions_live ADD COLUMN IF NOT EXISTS sell_qty DOUBLE;")
        conn.execute("ALTER TABLE positions_live ADD COLUMN IF NOT EXISTS opened_at TIMESTAMP;")
        conn.execute("ALTER TABLE positions_live ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP;")
        conn.execute("ALTER TABLE positions_live ADD COLUMN IF NOT EXISTS has_issue BOOLEAN;")
        conn.execute("ALTER TABLE positions_live ADD COLUMN IF NOT EXISTS issue_note TEXT;")
        conn.execute("ALTER TABLE position_rounds ADD COLUMN IF NOT EXISTS symbol TEXT;")
        conn.execute("ALTER TABLE position_rounds ADD COLUMN IF NOT EXISTS opened_at TIMESTAMP;")
        conn.execute("ALTER TABLE position_rounds ADD COLUMN IF NOT EXISTS closed_at TIMESTAMP;")
        conn.execute("ALTER TABLE position_rounds ADD COLUMN IF NOT EXISTS closed_reason TEXT;")
        conn.execute("ALTER TABLE position_rounds ADD COLUMN IF NOT EXISTS last_state_sell_buy TEXT;")
        conn.execute("ALTER TABLE position_rounds ADD COLUMN IF NOT EXISTS has_issue BOOLEAN;")
        conn.execute("ALTER TABLE position_rounds ADD COLUMN IF NOT EXISTS issue_note TEXT;")
        conn.execute("ALTER TABLE initial_positions_seed ADD COLUMN IF NOT EXISTS buy_qty DOUBLE;")
        conn.execute("ALTER TABLE initial_positions_seed ADD COLUMN IF NOT EXISTS sell_qty DOUBLE;")
        conn.execute("ALTER TABLE initial_positions_seed ADD COLUMN IF NOT EXISTS asof_dt TIMESTAMP;")
        conn.execute("ALTER TABLE initial_positions_seed ADD COLUMN IF NOT EXISTS memo TEXT;")
