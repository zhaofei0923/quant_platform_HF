-- PostgreSQL-only record/snapshot tier for hosts that do not install TimescaleDB.
-- These definitions match the transaction tier of 003_single_host_schema_split.sql.
-- Apply with record writers stopped. Existing records and domain ledgers are preserved.
-- This migration creates partitions only; it never invokes retention maintenance.
BEGIN;

CREATE SCHEMA IF NOT EXISTS trading_core;

CREATE TABLE IF NOT EXISTS trading_core.order_events (
    trade_date DATE NOT NULL,
    idempotency_key TEXT NOT NULL,
    account_id TEXT NOT NULL,
    strategy_id TEXT NOT NULL DEFAULT '',
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

-- Explicit month input also permits preparing older WAL months before replay.
-- This helper is independent of ops.run_partition_maintenance and its retention rules.
CREATE OR REPLACE FUNCTION trading_core.ensure_online_record_partitions(p_month DATE)
RETURNS VOID
LANGUAGE plpgsql
AS $$
DECLARE
    month_start DATE := date_trunc('month', p_month)::DATE;
    month_end DATE := (month_start + INTERVAL '1 month')::DATE;
    table_name TEXT;
BEGIN
    IF p_month IS NULL THEN
        RAISE EXCEPTION 'partition month must not be null';
    END IF;
    FOREACH table_name IN ARRAY ARRAY['order_events', 'trade_events'] LOOP
        EXECUTE format(
            'CREATE TABLE IF NOT EXISTS trading_core.%I PARTITION OF trading_core.%I '
            'FOR VALUES FROM (%L) TO (%L)',
            table_name || '_' || to_char(month_start, 'YYYYMM'),
            table_name,
            month_start,
            month_end
        );
    END LOOP;
END;
$$;

DO $$
DECLARE
    month_offset INTEGER;
BEGIN
    FOR month_offset IN -1..3 LOOP
        PERFORM trading_core.ensure_online_record_partitions(
            (date_trunc('month', CURRENT_DATE) + make_interval(months => month_offset))::DATE
        );
    END LOOP;
END;
$$;

COMMIT;
