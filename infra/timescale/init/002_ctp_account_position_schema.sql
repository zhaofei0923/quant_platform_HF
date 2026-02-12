-- Quant HFT CTP account/position migration.
-- Safe to re-run: all statements are idempotent.

ALTER TABLE IF EXISTS market_snapshots ADD COLUMN IF NOT EXISTS exchange_id TEXT NOT NULL DEFAULT '';
ALTER TABLE IF EXISTS market_snapshots ADD COLUMN IF NOT EXISTS trading_day TEXT NOT NULL DEFAULT '';
ALTER TABLE IF EXISTS market_snapshots ADD COLUMN IF NOT EXISTS action_day TEXT NOT NULL DEFAULT '';
ALTER TABLE IF EXISTS market_snapshots ADD COLUMN IF NOT EXISTS update_time TEXT NOT NULL DEFAULT '';
ALTER TABLE IF EXISTS market_snapshots ADD COLUMN IF NOT EXISTS update_millisec INTEGER NOT NULL DEFAULT 0;
ALTER TABLE IF EXISTS market_snapshots ADD COLUMN IF NOT EXISTS settlement_price DOUBLE PRECISION NOT NULL DEFAULT 0.0;
ALTER TABLE IF EXISTS market_snapshots ADD COLUMN IF NOT EXISTS average_price_raw DOUBLE PRECISION NOT NULL DEFAULT 0.0;
ALTER TABLE IF EXISTS market_snapshots ADD COLUMN IF NOT EXISTS average_price_norm DOUBLE PRECISION NOT NULL DEFAULT 0.0;
ALTER TABLE IF EXISTS market_snapshots ADD COLUMN IF NOT EXISTS is_valid_settlement BOOLEAN NOT NULL DEFAULT FALSE;

ALTER TABLE IF EXISTS order_events ADD COLUMN IF NOT EXISTS exchange_id TEXT NOT NULL DEFAULT '';
ALTER TABLE IF EXISTS order_events ADD COLUMN IF NOT EXISTS status_msg TEXT NOT NULL DEFAULT '';
ALTER TABLE IF EXISTS order_events ADD COLUMN IF NOT EXISTS order_submit_status TEXT NOT NULL DEFAULT '';
ALTER TABLE IF EXISTS order_events ADD COLUMN IF NOT EXISTS order_ref TEXT NOT NULL DEFAULT '';
ALTER TABLE IF EXISTS order_events ADD COLUMN IF NOT EXISTS front_id INTEGER NOT NULL DEFAULT 0;
ALTER TABLE IF EXISTS order_events ADD COLUMN IF NOT EXISTS session_id INTEGER NOT NULL DEFAULT 0;
ALTER TABLE IF EXISTS order_events ADD COLUMN IF NOT EXISTS trade_id TEXT NOT NULL DEFAULT '';
ALTER TABLE IF EXISTS order_events ADD COLUMN IF NOT EXISTS event_source TEXT NOT NULL DEFAULT '';
ALTER TABLE IF EXISTS order_events ADD COLUMN IF NOT EXISTS exchange_ts_ns BIGINT NOT NULL DEFAULT 0;
ALTER TABLE IF EXISTS order_events ADD COLUMN IF NOT EXISTS recv_ts_ns BIGINT NOT NULL DEFAULT 0;

CREATE TABLE IF NOT EXISTS ctp_trading_accounts (
    account_id TEXT NOT NULL,
    investor_id TEXT NOT NULL,
    balance DOUBLE PRECISION NOT NULL,
    available DOUBLE PRECISION NOT NULL,
    curr_margin DOUBLE PRECISION NOT NULL,
    frozen_margin DOUBLE PRECISION NOT NULL,
    frozen_cash DOUBLE PRECISION NOT NULL,
    frozen_commission DOUBLE PRECISION NOT NULL,
    commission DOUBLE PRECISION NOT NULL,
    close_profit DOUBLE PRECISION NOT NULL,
    position_profit DOUBLE PRECISION NOT NULL,
    trading_day TEXT NOT NULL DEFAULT '',
    ts_ns BIGINT NOT NULL,
    source TEXT NOT NULL DEFAULT ''
);
SELECT create_hypertable('ctp_trading_accounts', by_range('ts_ns'), if_not_exists => TRUE);
CREATE INDEX IF NOT EXISTS idx_ctp_trading_accounts_account_ts
    ON ctp_trading_accounts (account_id, ts_ns DESC);

CREATE TABLE IF NOT EXISTS ctp_investor_positions (
    account_id TEXT NOT NULL,
    investor_id TEXT NOT NULL,
    instrument_id TEXT NOT NULL,
    exchange_id TEXT NOT NULL DEFAULT '',
    posi_direction TEXT NOT NULL DEFAULT '',
    hedge_flag TEXT NOT NULL DEFAULT '',
    position_date TEXT NOT NULL DEFAULT '',
    position INTEGER NOT NULL,
    today_position INTEGER NOT NULL,
    yd_position INTEGER NOT NULL,
    long_frozen INTEGER NOT NULL,
    short_frozen INTEGER NOT NULL,
    open_volume INTEGER NOT NULL,
    close_volume INTEGER NOT NULL,
    position_cost DOUBLE PRECISION NOT NULL,
    open_cost DOUBLE PRECISION NOT NULL,
    position_profit DOUBLE PRECISION NOT NULL,
    close_profit DOUBLE PRECISION NOT NULL,
    margin_rate_by_money DOUBLE PRECISION NOT NULL,
    margin_rate_by_volume DOUBLE PRECISION NOT NULL,
    use_margin DOUBLE PRECISION NOT NULL,
    ts_ns BIGINT NOT NULL,
    source TEXT NOT NULL DEFAULT ''
);
SELECT create_hypertable('ctp_investor_positions', by_range('ts_ns'), if_not_exists => TRUE);
CREATE INDEX IF NOT EXISTS idx_ctp_investor_positions_acc_inst_ts
    ON ctp_investor_positions (account_id, instrument_id, ts_ns DESC);

CREATE TABLE IF NOT EXISTS ctp_broker_trading_params (
    account_id TEXT NOT NULL,
    investor_id TEXT NOT NULL,
    margin_price_type TEXT NOT NULL DEFAULT '',
    algorithm TEXT NOT NULL DEFAULT '',
    ts_ns BIGINT NOT NULL,
    source TEXT NOT NULL DEFAULT ''
);
SELECT create_hypertable('ctp_broker_trading_params', by_range('ts_ns'), if_not_exists => TRUE);
CREATE INDEX IF NOT EXISTS idx_ctp_broker_trading_params_account_ts
    ON ctp_broker_trading_params (account_id, ts_ns DESC);

CREATE TABLE IF NOT EXISTS ctp_instrument_meta (
    instrument_id TEXT NOT NULL,
    exchange_id TEXT NOT NULL DEFAULT '',
    product_id TEXT NOT NULL DEFAULT '',
    volume_multiple INTEGER NOT NULL,
    price_tick DOUBLE PRECISION NOT NULL,
    max_margin_side_algorithm BOOLEAN NOT NULL DEFAULT FALSE,
    ts_ns BIGINT NOT NULL,
    source TEXT NOT NULL DEFAULT ''
);
SELECT create_hypertable('ctp_instrument_meta', by_range('ts_ns'), if_not_exists => TRUE);
CREATE INDEX IF NOT EXISTS idx_ctp_instrument_meta_inst_ts
    ON ctp_instrument_meta (instrument_id, ts_ns DESC);
