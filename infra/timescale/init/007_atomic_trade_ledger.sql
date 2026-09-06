-- Atomic trade ledger and restartable projections. Run with writers stopped.
-- Existing populated books require an explicit migration/reconciliation: this script
-- deliberately does not infer trade coverage from old order-status processed keys.
BEGIN;

ALTER TABLE trading_core.position_summary
    ADD COLUMN IF NOT EXISTS hedge_flag INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS trading_day TEXT NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS version BIGINT NOT NULL DEFAULT 0;
ALTER TABLE trading_core.position_summary DROP CONSTRAINT IF EXISTS position_summary_pkey;
ALTER TABLE trading_core.position_summary ADD PRIMARY KEY
    (account_id, strategy_id, instrument_id, exchange_id, hedge_flag);

ALTER TABLE trading_core.position_detail
    ADD COLUMN IF NOT EXISTS lot_id TEXT,
    ADD COLUMN IF NOT EXISTS open_day TEXT NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS side INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS hedge_flag INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS remaining_qty INTEGER NOT NULL DEFAULT 0;
ALTER TABLE trading_core.position_detail ALTER COLUMN open_trade_id TYPE TEXT;
CREATE UNIQUE INDEX IF NOT EXISTS uq_position_detail_lot
    ON trading_core.position_detail (lot_id, open_date);
ALTER TABLE trading_core.trades ALTER COLUMN trade_id TYPE TEXT;
ALTER TABLE trading_core.trades ALTER COLUMN order_id DROP NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS uq_orders_account_ref_insert
    ON trading_core.orders (account_id, order_ref, insert_time);

CREATE TABLE IF NOT EXISTS trading_core.account_brokers (
    account_id TEXT PRIMARY KEY,
    broker_id TEXT NOT NULL,
    commit_sequence BIGINT NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS trading_core.runtime_namespace_bindings (
    account_id TEXT PRIMARY KEY,
    environment TEXT NOT NULL,
    broker_id TEXT NOT NULL,
    initial_instance TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS trading_core.trade_applications (
    identity_key TEXT PRIMARY KEY,
    account_id TEXT NOT NULL,
    fingerprint TEXT NOT NULL,
    exchange_order_id TEXT NOT NULL DEFAULT '',
    disposition TEXT NOT NULL CHECK (disposition IN ('applied', 'covered'))
);
CREATE INDEX IF NOT EXISTS idx_trade_applications_account
    ON trading_core.trade_applications (account_id);
CREATE TABLE IF NOT EXISTS trading_core.trade_conflicts (
    conflict_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    identity_key TEXT NOT NULL,
    account_id TEXT NOT NULL,
    fingerprint TEXT NOT NULL,
    reason TEXT NOT NULL,
    received_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE TABLE IF NOT EXISTS trading_core.trade_outbox (
    outbox_id TEXT PRIMARY KEY,
    identity_key TEXT NOT NULL,
    event_kind TEXT NOT NULL DEFAULT 'trade',
    account_id TEXT NOT NULL,
    strategy_id TEXT NOT NULL,
    instrument_id TEXT NOT NULL,
    position_version BIGINT NOT NULL,
    commit_sequence BIGINT NOT NULL,
    broker_id TEXT NOT NULL,
    trading_day TEXT NOT NULL,
    raw_trade_id TEXT NOT NULL,
    order_ref TEXT NOT NULL,
    exchange_order_id TEXT NOT NULL,
    exchange_id TEXT NOT NULL,
    side INTEGER NOT NULL,
    offset_flag INTEGER NOT NULL,
    hedge_flag INTEGER NOT NULL,
    price DOUBLE PRECISION NOT NULL,
    quantity INTEGER NOT NULL,
    trade_ts_ns BIGINT NOT NULL,
    commission DOUBLE PRECISION NOT NULL,
    profit DOUBLE PRECISION NOT NULL,
    valuation_complete BOOLEAN NOT NULL,
    close_today INTEGER NOT NULL,
    close_yesterday INTEGER NOT NULL,
    close_rule_source TEXT NOT NULL,
    close_rule_version TEXT NOT NULL,
    valuation_source TEXT NOT NULL,
    long_qty INTEGER NOT NULL,
    short_qty INTEGER NOT NULL,
    long_today_qty INTEGER NOT NULL,
    short_today_qty INTEGER NOT NULL,
    long_yd_qty INTEGER NOT NULL,
    short_yd_qty INTEGER NOT NULL,
    avg_long_price DOUBLE PRECISION NOT NULL,
    avg_short_price DOUBLE PRECISION NOT NULL,
    position_profit DOUBLE PRECISION NOT NULL,
    margin DOUBLE PRECISION NOT NULL,
    acknowledged BOOLEAN NOT NULL DEFAULT FALSE
);
CREATE TABLE IF NOT EXISTS trading_core.trade_outbox_acks (
    consumer TEXT NOT NULL,
    outbox_id TEXT NOT NULL REFERENCES trading_core.trade_outbox(outbox_id),
    PRIMARY KEY (consumer, outbox_id)
);
CREATE INDEX IF NOT EXISTS idx_trade_outbox_pending
    ON trading_core.trade_outbox (account_id) WHERE NOT acknowledged;
CREATE TABLE IF NOT EXISTS trading_core.domain_receipts (
    receipt_key TEXT PRIMARY KEY,
    stream_id TEXT NOT NULL,
    sequence BIGINT NOT NULL,
    checksum BIGINT NOT NULL,
    identity_key TEXT NOT NULL DEFAULT '',
    UNIQUE (stream_id, sequence)
);
CREATE TABLE IF NOT EXISTS trading_core.domain_watermarks (
    stream_id TEXT PRIMARY KEY,
    first_sequence BIGINT NOT NULL,
    next_sequence BIGINT NOT NULL
);
CREATE TABLE IF NOT EXISTS trading_core.position_baselines (
    account_id TEXT PRIMARY KEY,
    baseline_id TEXT NOT NULL UNIQUE,
    trading_day TEXT NOT NULL,
    broker_id TEXT NOT NULL
);
COMMIT;
