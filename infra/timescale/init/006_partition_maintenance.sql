-- Quant HFT partition maintenance helpers.
-- Safe to re-run: functions are CREATE OR REPLACE and partition creation is idempotent.

CREATE SCHEMA IF NOT EXISTS ops;

CREATE OR REPLACE FUNCTION ops.ensure_monthly_partition(
    p_schema TEXT,
    p_table TEXT,
    p_month_start DATE
)
RETURNS VOID
LANGUAGE plpgsql
AS $$
DECLARE
    month_start DATE := date_trunc('month', p_month_start)::DATE;
    month_end DATE := (date_trunc('month', p_month_start) + INTERVAL '1 month')::DATE;
    part_name TEXT := format('%s_%s', p_table, to_char(month_start, 'YYYYMM'));
BEGIN
    EXECUTE format(
        'CREATE TABLE IF NOT EXISTS %I.%I PARTITION OF %I.%I FOR VALUES FROM (%L) TO (%L)',
        p_schema,
        part_name,
        p_schema,
        p_table,
        month_start,
        month_end
    );
END;
$$;

CREATE OR REPLACE FUNCTION ops.ensure_future_monthly_partitions(
    p_schema TEXT,
    p_table TEXT,
    p_months_before INT DEFAULT 1,
    p_months_after INT DEFAULT 3
)
RETURNS VOID
LANGUAGE plpgsql
AS $$
DECLARE
    idx INT;
    base_month DATE := date_trunc('month', NOW())::DATE;
BEGIN
    FOR idx IN -GREATEST(0, p_months_before)..GREATEST(0, p_months_after) LOOP
        PERFORM ops.ensure_monthly_partition(
            p_schema,
            p_table,
            (base_month + make_interval(months => idx))::DATE
        );
    END LOOP;
END;
$$;

CREATE OR REPLACE FUNCTION ops.drop_old_monthly_partitions(
    p_schema TEXT,
    p_table TEXT,
    p_keep_months INT
)
RETURNS INT
LANGUAGE plpgsql
AS $$
DECLARE
    rec RECORD;
    dropped INT := 0;
    cutoff_yyyymm TEXT := to_char(
        date_trunc('month', NOW()) - make_interval(months => GREATEST(1, p_keep_months)),
        'YYYYMM'
    );
    suffix TEXT;
BEGIN
    FOR rec IN
        SELECT child_ns.nspname AS child_schema,
               child.relname AS child_name
        FROM pg_inherits i
        JOIN pg_class parent ON parent.oid = i.inhparent
        JOIN pg_namespace parent_ns ON parent_ns.oid = parent.relnamespace
        JOIN pg_class child ON child.oid = i.inhrelid
        JOIN pg_namespace child_ns ON child_ns.oid = child.relnamespace
        WHERE parent_ns.nspname = p_schema
          AND parent.relname = p_table
    LOOP
        suffix := right(rec.child_name, 6);
        IF suffix ~ '^[0-9]{6}$' AND suffix < cutoff_yyyymm THEN
            EXECUTE format('DROP TABLE IF EXISTS %I.%I', rec.child_schema, rec.child_name);
            dropped := dropped + 1;
        END IF;
    END LOOP;
    RETURN dropped;
END;
$$;

CREATE OR REPLACE FUNCTION ops.run_partition_maintenance(
    p_months_before INT DEFAULT 1,
    p_months_after INT DEFAULT 3,
    p_hot_days INT DEFAULT 7,
    p_cold_days INT DEFAULT 180
)
RETURNS VOID
LANGUAGE plpgsql
AS $$
DECLARE
    hot_keep_months INT := GREATEST(1, CEIL(p_hot_days / 30.0)::INT);
    cold_keep_months INT := GREATEST(1, CEIL(p_cold_days / 30.0)::INT);
BEGIN
    PERFORM ops.ensure_future_monthly_partitions('trading_core', 'account_funds',
                                                 p_months_before, p_months_after);
    PERFORM ops.ensure_future_monthly_partitions('trading_core', 'position_detail',
                                                 p_months_before, p_months_after);
    PERFORM ops.ensure_future_monthly_partitions('trading_core', 'orders',
                                                 p_months_before, p_months_after);
    PERFORM ops.ensure_future_monthly_partitions('trading_core', 'trades',
                                                 p_months_before, p_months_after);
    PERFORM ops.ensure_future_monthly_partitions('trading_core', 'risk_events',
                                                 p_months_before, p_months_after);
    PERFORM ops.ensure_future_monthly_partitions('trading_core', 'settlement_summary',
                                                 p_months_before, p_months_after);
    PERFORM ops.ensure_future_monthly_partitions('trading_core', 'settlement_detail',
                                                 p_months_before, p_months_after);
    PERFORM ops.ensure_future_monthly_partitions('trading_core', 'settlement_prices',
                                                 p_months_before, p_months_after);
    PERFORM ops.ensure_future_monthly_partitions('ops', 'system_logs',
                                                 p_months_before, p_months_after);
    PERFORM ops.ensure_future_monthly_partitions('ops', 'settlement_reconcile_diff',
                                                 p_months_before, p_months_after);

    -- Logs keep short hot window; risk/trade records align with cold retention baseline.
    PERFORM ops.drop_old_monthly_partitions('ops', 'system_logs', hot_keep_months);
    PERFORM ops.drop_old_monthly_partitions('trading_core', 'orders', cold_keep_months);
    PERFORM ops.drop_old_monthly_partitions('trading_core', 'trades', cold_keep_months);
    PERFORM ops.drop_old_monthly_partitions('trading_core', 'risk_events', cold_keep_months);
    PERFORM ops.drop_old_monthly_partitions('trading_core', 'settlement_summary', cold_keep_months);
    PERFORM ops.drop_old_monthly_partitions('trading_core', 'settlement_detail', cold_keep_months);
    PERFORM ops.drop_old_monthly_partitions('trading_core', 'settlement_prices', cold_keep_months);
    PERFORM ops.drop_old_monthly_partitions('trading_core', 'account_funds', cold_keep_months);
    PERFORM ops.drop_old_monthly_partitions('trading_core', 'position_detail', cold_keep_months);
    PERFORM ops.drop_old_monthly_partitions('ops', 'settlement_reconcile_diff', cold_keep_months);
END;
$$;

-- Bootstrap default monthly partitions immediately.
SELECT ops.run_partition_maintenance(1, 3, 7, 180);
