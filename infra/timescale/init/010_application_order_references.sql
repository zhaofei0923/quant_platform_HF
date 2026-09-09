-- Application-generated order IDs include strategy, signal, contract and nanosecond time.
-- They are not the short CTP OrderRef and can exceed the original VARCHAR(50) columns.
-- Apply with writers stopped; widening preserves every existing value and partition/index.
BEGIN;
ALTER TABLE trading_core.orders ALTER COLUMN order_ref TYPE TEXT;
ALTER TABLE trading_core.trades ALTER COLUMN order_ref TYPE TEXT;
ALTER TABLE trading_core.position_detail ALTER COLUMN open_order_ref TYPE TEXT;
ALTER TABLE trading_core.risk_events ALTER COLUMN order_ref TYPE TEXT;
ALTER TABLE IF EXISTS ops.processed_order_events
    ALTER COLUMN order_ref TYPE TEXT,
    ALTER COLUMN event_key TYPE TEXT;
ALTER TABLE IF EXISTS ops.system_logs ALTER COLUMN order_ref TYPE TEXT;
COMMIT;
