-- Quant HFT Timescale schema (repository baseline)
-- Safe to re-run: all statements are idempotent.

DO $$
BEGIN
    CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;
EXCEPTION
    WHEN duplicate_object OR unique_violation THEN
        NULL;
END
$$;

CREATE TABLE IF NOT EXISTS market_snapshots (
    instrument_id TEXT NOT NULL,
    exchange_id TEXT NOT NULL DEFAULT '',
    trading_day TEXT NOT NULL DEFAULT '',
    action_day TEXT NOT NULL DEFAULT '',
    update_time TEXT NOT NULL DEFAULT '',
    update_millisec INTEGER NOT NULL DEFAULT 0,
    last_price DOUBLE PRECISION NOT NULL,
    bid_price_1 DOUBLE PRECISION NOT NULL,
    ask_price_1 DOUBLE PRECISION NOT NULL,
    bid_volume_1 BIGINT NOT NULL,
    ask_volume_1 BIGINT NOT NULL,
    volume BIGINT NOT NULL,
    settlement_price DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    average_price_raw DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    average_price_norm DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    is_valid_settlement BOOLEAN NOT NULL DEFAULT FALSE,
    exchange_ts_ns BIGINT NOT NULL,
    recv_ts_ns BIGINT NOT NULL
);

ALTER TABLE market_snapshots ADD COLUMN IF NOT EXISTS exchange_id TEXT NOT NULL DEFAULT '';
ALTER TABLE market_snapshots ADD COLUMN IF NOT EXISTS trading_day TEXT NOT NULL DEFAULT '';
ALTER TABLE market_snapshots ADD COLUMN IF NOT EXISTS action_day TEXT NOT NULL DEFAULT '';
ALTER TABLE market_snapshots ADD COLUMN IF NOT EXISTS update_time TEXT NOT NULL DEFAULT '';
ALTER TABLE market_snapshots ADD COLUMN IF NOT EXISTS update_millisec INTEGER NOT NULL DEFAULT 0;
ALTER TABLE market_snapshots ADD COLUMN IF NOT EXISTS settlement_price DOUBLE PRECISION NOT NULL DEFAULT 0.0;
ALTER TABLE market_snapshots ADD COLUMN IF NOT EXISTS average_price_raw DOUBLE PRECISION NOT NULL DEFAULT 0.0;
ALTER TABLE market_snapshots ADD COLUMN IF NOT EXISTS average_price_norm DOUBLE PRECISION NOT NULL DEFAULT 0.0;
ALTER TABLE market_snapshots ADD COLUMN IF NOT EXISTS is_valid_settlement BOOLEAN NOT NULL DEFAULT FALSE;

SELECT create_hypertable('market_snapshots', by_range('recv_ts_ns'), if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS idx_market_snapshots_instrument_recv_ts
    ON market_snapshots (instrument_id, recv_ts_ns DESC);

CREATE TABLE IF NOT EXISTS order_events (
    account_id TEXT NOT NULL,
    strategy_id TEXT NOT NULL DEFAULT '',
    client_order_id TEXT NOT NULL,
    exchange_order_id TEXT NOT NULL,
    instrument_id TEXT NOT NULL,
    exchange_id TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL,
    total_volume INTEGER NOT NULL,
    filled_volume INTEGER NOT NULL,
    avg_fill_price DOUBLE PRECISION NOT NULL,
    reason TEXT NOT NULL,
    status_msg TEXT NOT NULL DEFAULT '',
    order_submit_status TEXT NOT NULL DEFAULT '',
    order_ref TEXT NOT NULL DEFAULT '',
    front_id INTEGER NOT NULL DEFAULT 0,
    session_id INTEGER NOT NULL DEFAULT 0,
    trade_id TEXT NOT NULL DEFAULT '',
    event_source TEXT NOT NULL DEFAULT '',
    exchange_ts_ns BIGINT NOT NULL DEFAULT 0,
    recv_ts_ns BIGINT NOT NULL DEFAULT 0,
    ts_ns BIGINT NOT NULL,
    trace_id TEXT NOT NULL,
    execution_algo_id TEXT NOT NULL DEFAULT '',
    slice_index INTEGER NOT NULL DEFAULT 0,
    slice_total INTEGER NOT NULL DEFAULT 0,
    throttle_applied BOOLEAN NOT NULL DEFAULT FALSE,
    venue TEXT NOT NULL DEFAULT '',
    route_id TEXT NOT NULL DEFAULT '',
    slippage_bps DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    impact_cost DOUBLE PRECISION NOT NULL DEFAULT 0.0
);

ALTER TABLE order_events ADD COLUMN IF NOT EXISTS exchange_id TEXT NOT NULL DEFAULT '';
ALTER TABLE order_events ADD COLUMN IF NOT EXISTS strategy_id TEXT NOT NULL DEFAULT '';
ALTER TABLE order_events ADD COLUMN IF NOT EXISTS status_msg TEXT NOT NULL DEFAULT '';
ALTER TABLE order_events ADD COLUMN IF NOT EXISTS order_submit_status TEXT NOT NULL DEFAULT '';
ALTER TABLE order_events ADD COLUMN IF NOT EXISTS order_ref TEXT NOT NULL DEFAULT '';
ALTER TABLE order_events ADD COLUMN IF NOT EXISTS front_id INTEGER NOT NULL DEFAULT 0;
ALTER TABLE order_events ADD COLUMN IF NOT EXISTS session_id INTEGER NOT NULL DEFAULT 0;
ALTER TABLE order_events ADD COLUMN IF NOT EXISTS trade_id TEXT NOT NULL DEFAULT '';
ALTER TABLE order_events ADD COLUMN IF NOT EXISTS event_source TEXT NOT NULL DEFAULT '';
ALTER TABLE order_events ADD COLUMN IF NOT EXISTS exchange_ts_ns BIGINT NOT NULL DEFAULT 0;
ALTER TABLE order_events ADD COLUMN IF NOT EXISTS recv_ts_ns BIGINT NOT NULL DEFAULT 0;

SELECT create_hypertable('order_events', by_range('ts_ns'), if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS idx_order_events_client_order_ts
    ON order_events (client_order_id, ts_ns DESC);
CREATE INDEX IF NOT EXISTS idx_order_events_trace_ts
    ON order_events (trace_id, ts_ns DESC);

CREATE TABLE IF NOT EXISTS risk_decisions (
    account_id TEXT NOT NULL,
    client_order_id TEXT NOT NULL,
    instrument_id TEXT NOT NULL,
    side TEXT NOT NULL,
    offset_flag TEXT NOT NULL,
    volume INTEGER NOT NULL,
    price DOUBLE PRECISION NOT NULL,
    intent_ts_ns BIGINT NOT NULL,
    trace_id TEXT NOT NULL,
    risk_action TEXT NOT NULL,
    rule_id TEXT NOT NULL,
    rule_group TEXT NOT NULL,
    rule_version TEXT NOT NULL,
    policy_id TEXT NOT NULL,
    policy_scope TEXT NOT NULL,
    observed_value DOUBLE PRECISION NOT NULL,
    threshold_value DOUBLE PRECISION NOT NULL,
    decision_tags TEXT NOT NULL,
    reason TEXT NOT NULL,
    decision_ts_ns BIGINT NOT NULL
);

SELECT create_hypertable('risk_decisions', by_range('decision_ts_ns'), if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS idx_risk_decisions_client_order_ts
    ON risk_decisions (client_order_id, decision_ts_ns DESC);
CREATE INDEX IF NOT EXISTS idx_risk_decisions_trace_ts
    ON risk_decisions (trace_id, decision_ts_ns DESC);

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
