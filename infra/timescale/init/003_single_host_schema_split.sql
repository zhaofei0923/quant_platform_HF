-- Quant HFT single-host schema split:
-- - analytics_ts: time-series and query-heavy analytics tables
-- - trading_core: transaction-source-of-truth tables
-- Safe to re-run: all statements are idempotent.

DO $$
BEGIN
    CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;
EXCEPTION
    WHEN duplicate_object OR unique_violation THEN
        NULL;
END
$$;

CREATE SCHEMA IF NOT EXISTS analytics_ts;
CREATE SCHEMA IF NOT EXISTS trading_core;

-- Integer-time hypertable helper for nanosecond-based ts columns.
CREATE OR REPLACE FUNCTION analytics_ts.integer_now_ns()
RETURNS BIGINT
LANGUAGE SQL
STABLE
AS $$
    SELECT (EXTRACT(EPOCH FROM clock_timestamp()) * 1000000000)::BIGINT;
$$;

CREATE TABLE IF NOT EXISTS analytics_ts.market_snapshots (
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

SELECT create_hypertable('analytics_ts.market_snapshots',
                         by_range('recv_ts_ns'),
                         if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS idx_analytics_market_snapshots_instrument_recv_ts
    ON analytics_ts.market_snapshots (instrument_id, recv_ts_ns DESC);

CREATE TABLE IF NOT EXISTS analytics_ts.order_events (
    account_id TEXT NOT NULL,
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

SELECT create_hypertable('analytics_ts.order_events', by_range('ts_ns'), if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS idx_analytics_order_events_client_order_ts
    ON analytics_ts.order_events (client_order_id, ts_ns DESC);
CREATE INDEX IF NOT EXISTS idx_analytics_order_events_trace_ts
    ON analytics_ts.order_events (trace_id, ts_ns DESC);

CREATE TABLE IF NOT EXISTS analytics_ts.risk_decisions (
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

SELECT create_hypertable('analytics_ts.risk_decisions',
                         by_range('decision_ts_ns'),
                         if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS idx_analytics_risk_decisions_client_order_ts
    ON analytics_ts.risk_decisions (client_order_id, decision_ts_ns DESC);
CREATE INDEX IF NOT EXISTS idx_analytics_risk_decisions_trace_ts
    ON analytics_ts.risk_decisions (trace_id, decision_ts_ns DESC);

CREATE TABLE IF NOT EXISTS analytics_ts.ctp_trading_accounts (
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

SELECT create_hypertable('analytics_ts.ctp_trading_accounts',
                         by_range('ts_ns'),
                         if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS idx_analytics_ctp_trading_accounts_account_ts
    ON analytics_ts.ctp_trading_accounts (account_id, ts_ns DESC);

CREATE TABLE IF NOT EXISTS analytics_ts.ctp_investor_positions (
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

SELECT create_hypertable('analytics_ts.ctp_investor_positions',
                         by_range('ts_ns'),
                         if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS idx_analytics_ctp_investor_positions_acc_inst_ts
    ON analytics_ts.ctp_investor_positions (account_id, instrument_id, ts_ns DESC);

CREATE TABLE IF NOT EXISTS analytics_ts.ctp_broker_trading_params (
    account_id TEXT NOT NULL,
    investor_id TEXT NOT NULL,
    margin_price_type TEXT NOT NULL DEFAULT '',
    algorithm TEXT NOT NULL DEFAULT '',
    ts_ns BIGINT NOT NULL,
    source TEXT NOT NULL DEFAULT ''
);

SELECT create_hypertable('analytics_ts.ctp_broker_trading_params',
                         by_range('ts_ns'),
                         if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS idx_analytics_ctp_broker_trading_params_account_ts
    ON analytics_ts.ctp_broker_trading_params (account_id, ts_ns DESC);

CREATE TABLE IF NOT EXISTS analytics_ts.ctp_instrument_meta (
    instrument_id TEXT NOT NULL,
    exchange_id TEXT NOT NULL DEFAULT '',
    product_id TEXT NOT NULL DEFAULT '',
    volume_multiple INTEGER NOT NULL,
    price_tick DOUBLE PRECISION NOT NULL,
    max_margin_side_algorithm BOOLEAN NOT NULL DEFAULT FALSE,
    ts_ns BIGINT NOT NULL,
    source TEXT NOT NULL DEFAULT ''
);

SELECT create_hypertable('analytics_ts.ctp_instrument_meta',
                         by_range('ts_ns'),
                         if_not_exists => TRUE);

CREATE INDEX IF NOT EXISTS idx_analytics_ctp_instrument_meta_inst_ts
    ON analytics_ts.ctp_instrument_meta (instrument_id, ts_ns DESC);

SELECT set_integer_now_func('analytics_ts.market_snapshots',
                            'analytics_ts.integer_now_ns',
                            replace_if_exists => TRUE);
SELECT set_integer_now_func('analytics_ts.order_events',
                            'analytics_ts.integer_now_ns',
                            replace_if_exists => TRUE);
SELECT set_integer_now_func('analytics_ts.risk_decisions',
                            'analytics_ts.integer_now_ns',
                            replace_if_exists => TRUE);

-- Enable compression + retention for analytics tier.
ALTER TABLE analytics_ts.market_snapshots SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'instrument_id',
    timescaledb.compress_orderby = 'recv_ts_ns DESC'
);
SELECT add_compression_policy('analytics_ts.market_snapshots',
                              259200000000000::BIGINT,
                              if_not_exists => TRUE);
SELECT add_retention_policy('analytics_ts.market_snapshots',
                            2592000000000000::BIGINT,
                            if_not_exists => TRUE);

ALTER TABLE analytics_ts.order_events SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'client_order_id',
    timescaledb.compress_orderby = 'ts_ns DESC'
);
SELECT add_compression_policy('analytics_ts.order_events',
                              259200000000000::BIGINT,
                              if_not_exists => TRUE);
SELECT add_retention_policy('analytics_ts.order_events',
                            15552000000000000::BIGINT,
                            if_not_exists => TRUE);

ALTER TABLE analytics_ts.risk_decisions SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'client_order_id',
    timescaledb.compress_orderby = 'decision_ts_ns DESC'
);
SELECT add_compression_policy('analytics_ts.risk_decisions',
                              259200000000000::BIGINT,
                              if_not_exists => TRUE);
SELECT add_retention_policy('analytics_ts.risk_decisions',
                            15552000000000000::BIGINT,
                            if_not_exists => TRUE);

-- Transaction source-of-truth tier.
CREATE TABLE IF NOT EXISTS trading_core.order_events (
    trade_date DATE NOT NULL,
    idempotency_key TEXT NOT NULL,
    account_id TEXT NOT NULL,
    client_order_id TEXT NOT NULL,
    exchange_order_id TEXT NOT NULL DEFAULT '',
    instrument_id TEXT NOT NULL,
    exchange_id TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL,
    total_volume INTEGER NOT NULL,
    filled_volume INTEGER NOT NULL,
    avg_fill_price DOUBLE PRECISION NOT NULL,
    reason TEXT NOT NULL DEFAULT '',
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
    impact_cost DOUBLE PRECISION NOT NULL DEFAULT 0.0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (trade_date, idempotency_key)
) PARTITION BY RANGE (trade_date);

CREATE INDEX IF NOT EXISTS idx_trading_core_order_events_client_order_ts
    ON trading_core.order_events (client_order_id, ts_ns DESC);
CREATE INDEX IF NOT EXISTS idx_trading_core_order_events_trade_date_status
    ON trading_core.order_events (trade_date, status);
CREATE INDEX IF NOT EXISTS idx_trading_core_order_events_trace_ts
    ON trading_core.order_events (trace_id, ts_ns DESC);

CREATE TABLE IF NOT EXISTS trading_core.trade_events (
    trade_date DATE NOT NULL,
    idempotency_key TEXT NOT NULL,
    account_id TEXT NOT NULL,
    client_order_id TEXT NOT NULL,
    exchange_order_id TEXT NOT NULL DEFAULT '',
    instrument_id TEXT NOT NULL,
    exchange_id TEXT NOT NULL DEFAULT '',
    trade_id TEXT NOT NULL DEFAULT '',
    filled_volume INTEGER NOT NULL,
    avg_fill_price DOUBLE PRECISION NOT NULL,
    exchange_ts_ns BIGINT NOT NULL DEFAULT 0,
    recv_ts_ns BIGINT NOT NULL DEFAULT 0,
    ts_ns BIGINT NOT NULL,
    trace_id TEXT NOT NULL,
    event_source TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (trade_date, idempotency_key)
) PARTITION BY RANGE (trade_date);

CREATE INDEX IF NOT EXISTS idx_trading_core_trade_events_client_order_ts
    ON trading_core.trade_events (client_order_id, ts_ns DESC);
CREATE INDEX IF NOT EXISTS idx_trading_core_trade_events_trade_id
    ON trading_core.trade_events (trade_id);
CREATE INDEX IF NOT EXISTS idx_trading_core_trade_events_trace_ts
    ON trading_core.trade_events (trace_id, ts_ns DESC);

CREATE TABLE IF NOT EXISTS trading_core.account_snapshots (
    account_id TEXT NOT NULL,
    investor_id TEXT NOT NULL,
    trading_day TEXT NOT NULL DEFAULT '',
    balance DOUBLE PRECISION NOT NULL,
    available DOUBLE PRECISION NOT NULL,
    curr_margin DOUBLE PRECISION NOT NULL,
    frozen_margin DOUBLE PRECISION NOT NULL,
    frozen_cash DOUBLE PRECISION NOT NULL,
    frozen_commission DOUBLE PRECISION NOT NULL,
    commission DOUBLE PRECISION NOT NULL,
    close_profit DOUBLE PRECISION NOT NULL,
    position_profit DOUBLE PRECISION NOT NULL,
    ts_ns BIGINT NOT NULL,
    recv_ts_ns BIGINT NOT NULL DEFAULT 0,
    source TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (account_id, ts_ns)
);

CREATE INDEX IF NOT EXISTS idx_trading_core_account_snapshots_account_ts
    ON trading_core.account_snapshots (account_id, ts_ns DESC);

CREATE TABLE IF NOT EXISTS trading_core.position_snapshots (
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
    recv_ts_ns BIGINT NOT NULL DEFAULT 0,
    source TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (account_id, instrument_id, posi_direction, position_date, ts_ns)
);

CREATE INDEX IF NOT EXISTS idx_trading_core_position_snapshots_account_inst_ts
    ON trading_core.position_snapshots (account_id, instrument_id, ts_ns DESC);

CREATE TABLE IF NOT EXISTS trading_core.replay_offsets (
    stream_name TEXT PRIMARY KEY,
    last_seq BIGINT NOT NULL DEFAULT 0,
    updated_ts_ns BIGINT NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
