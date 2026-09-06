-- Run after 007 with writers stopped. Existing outbox facts are not revalued.
BEGIN;
ALTER TABLE trading_core.trade_outbox
    ADD COLUMN IF NOT EXISTS fee_model TEXT NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS fee_date_basis TEXT NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS fee_allocation_source TEXT NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS fee_allocation_version TEXT NOT NULL DEFAULT '';
COMMIT;
