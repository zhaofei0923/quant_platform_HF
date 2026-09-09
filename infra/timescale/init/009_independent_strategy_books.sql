-- Writers stopped; economic legacy facts retain their allocation semantics.
BEGIN;
CREATE TABLE IF NOT EXISTS trading_core.strategy_capital (
 account_id TEXT NOT NULL, strategy_id TEXT NOT NULL,
 initial_capital DOUBLE PRECISION NOT NULL CHECK (initial_capital > 0),
 realized_pnl DOUBLE PRECISION NOT NULL DEFAULT 0, commission DOUBLE PRECISION NOT NULL DEFAULT 0,
 PRIMARY KEY (account_id,strategy_id));
CREATE TABLE IF NOT EXISTS trading_core.broker_position_summary (
 account_id TEXT NOT NULL, instrument_id TEXT NOT NULL, exchange_id TEXT NOT NULL, hedge_flag INTEGER NOT NULL,
 trading_day TEXT NOT NULL, version BIGINT NOT NULL,
 long_qty INTEGER NOT NULL, short_qty INTEGER NOT NULL,
 long_today_qty INTEGER NOT NULL, short_today_qty INTEGER NOT NULL,
 long_yd_qty INTEGER NOT NULL, short_yd_qty INTEGER NOT NULL,
 PRIMARY KEY (account_id,instrument_id,exchange_id,hedge_flag),
 CHECK (long_qty = long_today_qty + long_yd_qty AND short_qty = short_today_qty + short_yd_qty),
 CHECK (long_today_qty >= 0 AND short_today_qty >= 0 AND long_yd_qty >= 0 AND short_yd_qty >= 0));
ALTER TABLE trading_core.trade_outbox
 ADD COLUMN IF NOT EXISTS independent_books BOOLEAN NOT NULL DEFAULT FALSE,
 ADD COLUMN IF NOT EXISTS broker_close_today INTEGER NOT NULL DEFAULT 0,
 ADD COLUMN IF NOT EXISTS broker_close_yesterday INTEGER NOT NULL DEFAULT 0;
CREATE TABLE IF NOT EXISTS trading_core.strategy_close_reservations (
 account_id TEXT NOT NULL, order_ref TEXT NOT NULL, strategy_id TEXT NOT NULL,
 instrument_id TEXT NOT NULL, side INTEGER NOT NULL, hedge_flag INTEGER NOT NULL,
 quantity INTEGER NOT NULL CHECK (quantity > 0), reported_filled INTEGER NOT NULL DEFAULT 0,
 booked_qty INTEGER NOT NULL DEFAULT 0, terminal BOOLEAN NOT NULL DEFAULT FALSE,
 PRIMARY KEY (account_id,order_ref), CHECK (booked_qty >= 0 AND booked_qty <= quantity),
 CHECK (reported_filled >= 0 AND reported_filled <= quantity));
ALTER TABLE trading_core.orders ADD COLUMN IF NOT EXISTS component_id TEXT NOT NULL DEFAULT '';
ALTER TABLE trading_core.orders ADD COLUMN IF NOT EXISTS hedge_flag INTEGER NOT NULL DEFAULT 0;
ALTER TABLE trading_core.trades ADD COLUMN IF NOT EXISTS component_id TEXT NOT NULL DEFAULT '';
ALTER TABLE trading_core.trade_outbox ADD COLUMN IF NOT EXISTS component_id TEXT NOT NULL DEFAULT '';
ALTER TABLE trading_core.strategy_capital ADD COLUMN IF NOT EXISTS capital_adjustment DOUBLE PRECISION NOT NULL DEFAULT 0;
CREATE TABLE IF NOT EXISTS trading_core.strategy_capital_transfers (
 transfer_id TEXT PRIMARY KEY, account_id TEXT NOT NULL, from_strategy TEXT NOT NULL, to_strategy TEXT NOT NULL,
 amount DOUBLE PRECISION NOT NULL CHECK (amount>0), reason TEXT NOT NULL, recorded_ts_ns BIGINT NOT NULL);
CREATE TABLE IF NOT EXISTS trading_core.strategy_capital_reconciliations (
 account_id TEXT NOT NULL, observed_ts_ns BIGINT NOT NULL, trading_day TEXT NOT NULL,
 broker_equity DOUBLE PRECISION NOT NULL, strategy_equity DOUBLE PRECISION NOT NULL,
 strategy_allocated DOUBLE PRECISION NOT NULL, strategy_realized DOUBLE PRECISION NOT NULL,
 strategy_unrealized DOUBLE PRECISION NOT NULL, strategy_commission DOUBLE PRECISION NOT NULL,
 realized_basis_difference DOUBLE PRECISION NOT NULL, floating_basis_difference DOUBLE PRECISION NOT NULL,
 fee_basis_difference DOUBLE PRECISION NOT NULL, cash_and_settlement_bridge DOUBLE PRECISION NOT NULL,
 total_difference DOUBLE PRECISION NOT NULL, basis TEXT NOT NULL, automatic_allocation BOOLEAN NOT NULL,
 PRIMARY KEY (account_id, observed_ts_ns));
CREATE TABLE IF NOT EXISTS trading_core.strategy_open_reservations (
  account_id TEXT NOT NULL, order_ref TEXT NOT NULL, strategy_id TEXT NOT NULL,
  instrument_id TEXT NOT NULL, side INTEGER NOT NULL, price DOUBLE PRECISION NOT NULL,
  quantity INTEGER NOT NULL CHECK (quantity > 0), unit_funds DOUBLE PRECISION NOT NULL CHECK (unit_funds > 0),
  reported_filled INTEGER NOT NULL DEFAULT 0, booked_qty INTEGER NOT NULL DEFAULT 0,
  terminal BOOLEAN NOT NULL DEFAULT FALSE, PRIMARY KEY (account_id, order_ref),
  CHECK (reported_filled BETWEEN 0 AND quantity), CHECK (booked_qty BETWEEN 0 AND quantity)
);
ALTER TABLE trading_core.strategy_open_reservations
 ADD COLUMN IF NOT EXISTS hedge_flag INTEGER NOT NULL DEFAULT 0;

COMMIT;
