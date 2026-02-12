-- Quant HFT trading_core domain truth tables (17-table model: trading_core slice).
-- Safe to re-run: all statements are idempotent.

CREATE SCHEMA IF NOT EXISTS trading_core;

-- 1) Account funds: account/day snapshot, partitioned by trading day.
CREATE TABLE IF NOT EXISTS trading_core.account_funds (
    account_id VARCHAR(32) NOT NULL,
    trading_day DATE NOT NULL,
    currency CHAR(3) NOT NULL DEFAULT 'CNY',
    pre_balance DECIMAL(18, 2) NOT NULL DEFAULT 0,
    deposit DECIMAL(18, 2) NOT NULL DEFAULT 0,
    withdraw DECIMAL(18, 2) NOT NULL DEFAULT 0,
    frozen_commission DECIMAL(18, 2) NOT NULL DEFAULT 0,
    frozen_margin DECIMAL(18, 2) NOT NULL DEFAULT 0,
    available DECIMAL(18, 2) NOT NULL DEFAULT 0,
    curr_margin DECIMAL(18, 2) NOT NULL DEFAULT 0,
    commission DECIMAL(18, 2) NOT NULL DEFAULT 0,
    close_profit DECIMAL(18, 2) NOT NULL DEFAULT 0,
    position_profit DECIMAL(18, 2) NOT NULL DEFAULT 0,
    balance DECIMAL(18, 2) NOT NULL DEFAULT 0,
    risk_degree DECIMAL(10, 4),
    update_time TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    ctp_time TIMESTAMPTZ,
    PRIMARY KEY (account_id, trading_day)
) PARTITION BY RANGE (trading_day);

ALTER TABLE trading_core.account_funds
    ADD COLUMN IF NOT EXISTS pre_settlement_balance DECIMAL(18, 2) NOT NULL DEFAULT 0;
ALTER TABLE trading_core.account_funds
    ADD COLUMN IF NOT EXISTS floating_profit DECIMAL(18, 2) NOT NULL DEFAULT 0;

-- 2) Position detail: lot-level position book, partitioned by open date.
CREATE TABLE IF NOT EXISTS trading_core.position_detail (
    position_id BIGINT GENERATED ALWAYS AS IDENTITY,
    account_id VARCHAR(32) NOT NULL,
    strategy_id VARCHAR(32) NOT NULL,
    instrument_id VARCHAR(30) NOT NULL,
    exchange_id VARCHAR(10) NOT NULL,
    open_date DATE NOT NULL,
    open_price DECIMAL(16, 4) NOT NULL,
    volume INT NOT NULL,
    is_today BOOLEAN NOT NULL DEFAULT FALSE,
    position_date DATE NOT NULL,
    open_order_ref VARCHAR(50),
    open_trade_id VARCHAR(50),
    close_volume INT NOT NULL DEFAULT 0,
    close_price DECIMAL(16, 4),
    close_profit DECIMAL(18, 2),
    position_status SMALLINT NOT NULL DEFAULT 1,
    update_time TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    ctp_time TIMESTAMPTZ,
    PRIMARY KEY (position_id, open_date)
) PARTITION BY RANGE (open_date);

ALTER TABLE trading_core.position_detail
    ADD COLUMN IF NOT EXISTS accumulated_mtm DECIMAL(18, 2) NOT NULL DEFAULT 0;
ALTER TABLE trading_core.position_detail
    ADD COLUMN IF NOT EXISTS last_settlement_date DATE;
ALTER TABLE trading_core.position_detail
    ADD COLUMN IF NOT EXISTS last_settlement_price DECIMAL(16, 4);
ALTER TABLE trading_core.position_detail
    ADD COLUMN IF NOT EXISTS last_settlement_profit DECIMAL(18, 2) NOT NULL DEFAULT 0;

CREATE UNIQUE INDEX IF NOT EXISTS uq_trading_core_position_detail_open_trade
    ON trading_core.position_detail (account_id, strategy_id, open_trade_id, open_date);
CREATE INDEX IF NOT EXISTS idx_trading_core_position_holding
    ON trading_core.position_detail (account_id, strategy_id, instrument_id, position_status);

-- 3) Position summary: high-frequency upsert table for risk checks.
CREATE TABLE IF NOT EXISTS trading_core.position_summary (
    account_id VARCHAR(32) NOT NULL,
    strategy_id VARCHAR(32) NOT NULL,
    instrument_id VARCHAR(30) NOT NULL,
    exchange_id VARCHAR(10) NOT NULL,
    long_volume INT NOT NULL DEFAULT 0,
    short_volume INT NOT NULL DEFAULT 0,
    net_volume INT NOT NULL DEFAULT 0,
    long_today_volume INT NOT NULL DEFAULT 0,
    short_today_volume INT NOT NULL DEFAULT 0,
    long_yd_volume INT NOT NULL DEFAULT 0,
    short_yd_volume INT NOT NULL DEFAULT 0,
    avg_long_price DECIMAL(16, 4),
    avg_short_price DECIMAL(16, 4),
    position_profit DECIMAL(18, 2),
    margin DECIMAL(18, 2),
    update_time TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (account_id, strategy_id, instrument_id)
);

-- 4) Orders: lifecycle table, partitioned by local insert timestamp.
CREATE TABLE IF NOT EXISTS trading_core.orders (
    order_id BIGINT GENERATED ALWAYS AS IDENTITY,
    order_ref VARCHAR(50) NOT NULL,
    front_id INT,
    session_id INT,
    account_id VARCHAR(32) NOT NULL,
    strategy_id VARCHAR(32) NOT NULL,
    instrument_id VARCHAR(30) NOT NULL,
    exchange_id VARCHAR(10) NOT NULL,
    order_type CHAR(1),
    direction CHAR(1) NOT NULL,
    offset_flag CHAR(1) NOT NULL,
    price_type CHAR(1) NOT NULL,
    limit_price DECIMAL(16, 4),
    volume_original INT NOT NULL,
    volume_traded INT NOT NULL DEFAULT 0,
    volume_canceled INT NOT NULL DEFAULT 0,
    order_status SMALLINT NOT NULL DEFAULT 0,
    insert_time TIMESTAMPTZ NOT NULL,
    update_time TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    ctp_insert_time TIMESTAMPTZ,
    cancel_time TIMESTAMPTZ,
    status_msg VARCHAR(255),
    PRIMARY KEY (order_id, insert_time)
) PARTITION BY RANGE (insert_time);

CREATE INDEX IF NOT EXISTS idx_trading_core_orders_account
    ON trading_core.orders (account_id, insert_time);
CREATE INDEX IF NOT EXISTS idx_trading_core_orders_ref
    ON trading_core.orders (order_ref, front_id, session_id, insert_time);

-- 5) Trades: fill table, partitioned by trade timestamp.
CREATE TABLE IF NOT EXISTS trading_core.trades (
    trade_id VARCHAR(50) NOT NULL,
    order_id BIGINT NOT NULL,
    order_ref VARCHAR(50) NOT NULL,
    account_id VARCHAR(32) NOT NULL,
    strategy_id VARCHAR(32) NOT NULL,
    instrument_id VARCHAR(30) NOT NULL,
    exchange_id VARCHAR(10) NOT NULL,
    direction CHAR(1) NOT NULL,
    offset_flag CHAR(1) NOT NULL,
    price DECIMAL(16, 4) NOT NULL,
    volume INT NOT NULL,
    trade_time TIMESTAMPTZ NOT NULL,
    ctp_trade_time TIMESTAMPTZ,
    commission DECIMAL(18, 2),
    profit DECIMAL(18, 2),
    PRIMARY KEY (trade_id, trade_time)
) PARTITION BY RANGE (trade_time);

CREATE INDEX IF NOT EXISTS idx_trading_core_trades_position
    ON trading_core.trades (account_id, strategy_id, instrument_id, trade_time);
CREATE INDEX IF NOT EXISTS idx_trading_core_trades_order
    ON trading_core.trades (order_id, trade_time);

-- 6) Instruments: static metadata snapshot.
CREATE TABLE IF NOT EXISTS trading_core.instruments (
    instrument_id VARCHAR(30) PRIMARY KEY,
    exchange_id VARCHAR(10) NOT NULL,
    product_id VARCHAR(20),
    instrument_name VARCHAR(60),
    contract_multiplier INT,
    price_tick DECIMAL(10, 4),
    long_margin_rate DECIMAL(10, 6),
    short_margin_rate DECIMAL(10, 6),
    commission_type CHAR(1),
    commission_rate DECIMAL(16, 10),
    close_today_commission_rate DECIMAL(16, 10),
    delivery_date DATE,
    expire_date DATE,
    is_trading BOOLEAN DEFAULT TRUE,
    update_time TIMESTAMPTZ
);

-- 7) Trading calendar.
CREATE TABLE IF NOT EXISTS trading_core.trading_calendar (
    exchange_id VARCHAR(10) NOT NULL,
    date DATE NOT NULL,
    is_trading_day BOOLEAN NOT NULL,
    night_open_time TIME,
    day_open_time TIME,
    close_time TIME,
    PRIMARY KEY (exchange_id, date)
);

-- 8) Strategy metadata.
CREATE TABLE IF NOT EXISTS trading_core.strategies (
    strategy_id VARCHAR(32) PRIMARY KEY,
    strategy_name VARCHAR(100) NOT NULL,
    version VARCHAR(20),
    account_id VARCHAR(32) NOT NULL,
    status SMALLINT NOT NULL DEFAULT 1,
    max_volume_per_order INT,
    max_position_per_instrument INT,
    max_total_position INT,
    max_daily_loss DECIMAL(18, 2),
    create_time TIMESTAMPTZ DEFAULT NOW(),
    update_time TIMESTAMPTZ DEFAULT NOW()
);

-- 9) Strategy params.
CREATE TABLE IF NOT EXISTS trading_core.strategy_params (
    param_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    strategy_id VARCHAR(32) NOT NULL,
    param_name VARCHAR(50) NOT NULL,
    param_value VARCHAR(255),
    param_type VARCHAR(20),
    update_time TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_trading_core_strategy_params
    ON trading_core.strategy_params (strategy_id);

-- 10) Risk events, partitioned by event timestamp.
CREATE TABLE IF NOT EXISTS trading_core.risk_events (
    event_id BIGINT GENERATED ALWAYS AS IDENTITY,
    account_id VARCHAR(32) NOT NULL,
    strategy_id VARCHAR(32),
    event_type SMALLINT NOT NULL,
    event_level SMALLINT NOT NULL,
    instrument_id VARCHAR(30),
    order_ref VARCHAR(50),
    event_desc VARCHAR(255),
    event_time TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (event_id, event_time)
) PARTITION BY RANGE (event_time);

CREATE INDEX IF NOT EXISTS idx_trading_core_risk_events
    ON trading_core.risk_events (account_id, event_time);

-- 11) Fund transfer journal.
CREATE TABLE IF NOT EXISTS trading_core.fund_transfer (
    transfer_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    account_id VARCHAR(32) NOT NULL,
    direction CHAR(1) NOT NULL,
    amount DECIMAL(18, 2) NOT NULL,
    currency CHAR(3) NOT NULL DEFAULT 'CNY',
    status SMALLINT NOT NULL DEFAULT 1,
    request_time TIMESTAMPTZ NOT NULL,
    confirm_time TIMESTAMPTZ,
    remark VARCHAR(200)
);
CREATE INDEX IF NOT EXISTS idx_trading_core_fund_transfer
    ON trading_core.fund_transfer (account_id, request_time);

-- 12) Fee/margin templates.
CREATE TABLE IF NOT EXISTS trading_core.fee_margin_template (
    template_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    exchange_id VARCHAR(10),
    instrument_id VARCHAR(30),
    product_id VARCHAR(20),
    account_type VARCHAR(20),
    commission_open DECIMAL(16, 10),
    commission_close DECIMAL(16, 10),
    commission_closetoday DECIMAL(16, 10),
    margin_rate DECIMAL(10, 6),
    effective_date DATE,
    expire_date DATE
);

-- 13) Daily settlement summary.
CREATE TABLE IF NOT EXISTS trading_core.settlement_summary (
    settlement_id BIGINT GENERATED ALWAYS AS IDENTITY,
    trading_day DATE NOT NULL,
    account_id VARCHAR(32) NOT NULL,
    pre_balance DECIMAL(18, 2) NOT NULL DEFAULT 0,
    deposit DECIMAL(18, 2) NOT NULL DEFAULT 0,
    withdraw DECIMAL(18, 2) NOT NULL DEFAULT 0,
    commission DECIMAL(18, 2) NOT NULL DEFAULT 0,
    close_profit DECIMAL(18, 2) NOT NULL DEFAULT 0,
    position_profit DECIMAL(18, 2) NOT NULL DEFAULT 0,
    balance DECIMAL(18, 2) NOT NULL DEFAULT 0,
    curr_margin DECIMAL(18, 2) NOT NULL DEFAULT 0,
    available DECIMAL(18, 2) NOT NULL DEFAULT 0,
    risk_degree DECIMAL(10, 4),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (settlement_id, trading_day)
) PARTITION BY RANGE (trading_day);

CREATE UNIQUE INDEX IF NOT EXISTS uq_trading_core_settlement_summary_day_account
    ON trading_core.settlement_summary (trading_day, account_id);

-- 14) Daily settlement detail.
CREATE TABLE IF NOT EXISTS trading_core.settlement_detail (
    detail_id BIGINT GENERATED ALWAYS AS IDENTITY,
    trading_day DATE NOT NULL,
    settlement_id BIGINT NOT NULL,
    position_id BIGINT NOT NULL,
    instrument_id VARCHAR(30) NOT NULL,
    volume INT NOT NULL,
    settlement_price DECIMAL(16, 4) NOT NULL,
    profit DECIMAL(18, 2) NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (detail_id, trading_day)
) PARTITION BY RANGE (trading_day);

CREATE UNIQUE INDEX IF NOT EXISTS uq_trading_core_settlement_detail_day_position
    ON trading_core.settlement_detail (trading_day, position_id);

-- 15) Settlement price records by source and final selected price.
CREATE TABLE IF NOT EXISTS trading_core.settlement_prices (
    price_id BIGINT GENERATED ALWAYS AS IDENTITY,
    trading_day DATE NOT NULL,
    instrument_id VARCHAR(30) NOT NULL,
    exchange_id VARCHAR(10) NOT NULL DEFAULT '',
    source VARCHAR(32) NOT NULL,
    settlement_price DECIMAL(16, 4) NOT NULL,
    is_final BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (price_id, trading_day)
) PARTITION BY RANGE (trading_day);

CREATE UNIQUE INDEX IF NOT EXISTS uq_trading_core_settlement_prices_day_inst_source
    ON trading_core.settlement_prices (trading_day, instrument_id, source);
