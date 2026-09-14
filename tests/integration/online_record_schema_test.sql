-- Run with psql -X -v ON_ERROR_STOP=1 -f this_file in a fresh, disposable database
-- named quant_test_online_record_*. This test never selects a production connection.
\set ON_ERROR_STOP on
DO $$
BEGIN
    IF current_database() !~ '^quant_test_online_record_' THEN
        RAISE EXCEPTION 'use a disposable quant_test_online_record_* database';
    END IF;
END;
$$;

\ir ../../infra/timescale/init/011_online_record_tables.sql

-- A preexisting unrelated relation must also survive the second application.
CREATE TABLE trading_core.test_existing_ledger (identity TEXT PRIMARY KEY, quantity INTEGER);
INSERT INTO trading_core.test_existing_ledger VALUES ('already-committed', 7);

-- Adjacent trading dates across a month boundary must route to distinct partitions.
INSERT INTO trading_core.order_events (
    trade_date, idempotency_key, account_id, client_order_id, instrument_id, status,
    total_volume, filled_volume, avg_fill_price, ts_ns, trace_id
)
SELECT day, day::TEXT, 'test-account', 'test-order', 'test-contract', 'Filled',
       1, 1, 12.5, 123, 'test-trace'
FROM (VALUES (date_trunc('month', CURRENT_DATE)::DATE - 1),
             (date_trunc('month', CURRENT_DATE)::DATE)) AS samples(day);

INSERT INTO trading_core.trade_events (
    trade_date, idempotency_key, account_id, client_order_id, instrument_id,
    trade_id, filled_volume, avg_fill_price, ts_ns, trace_id
)
SELECT trade_date, idempotency_key, account_id, client_order_id, instrument_id,
       'test-trade', filled_volume, avg_fill_price, ts_ns, trace_id
FROM trading_core.order_events;

INSERT INTO trading_core.account_snapshots (
    account_id, investor_id, balance, available, curr_margin, frozen_margin,
    frozen_cash, frozen_commission, commission, close_profit, position_profit, ts_ns
) VALUES ('test-account', 'test-investor', 0, 0, 0, 0, 0, 0, 0, 0, 0, 123);

INSERT INTO trading_core.position_snapshots (
    account_id, investor_id, instrument_id, position, today_position, yd_position,
    long_frozen, short_frozen, open_volume, close_volume, position_cost, open_cost,
    position_profit, close_profit, margin_rate_by_money, margin_rate_by_volume, use_margin,
    ts_ns
) VALUES ('test-account', 'test-investor', 'test-contract', 0, 0, 0, 0, 0, 0, 0,
          0, 0, 0, 0, 0, 0, 0, 123);

INSERT INTO trading_core.replay_offsets (stream_name, last_seq, updated_ts_ns)
VALUES ('test-stream', 35, 123);

CREATE TEMP TABLE expected_rows (table_name TEXT PRIMARY KEY, contents JSONB);
DO $$
DECLARE
    relation_name TEXT;
BEGIN
    FOREACH relation_name IN ARRAY ARRAY[
        'order_events', 'trade_events', 'account_snapshots', 'position_snapshots',
        'replay_offsets', 'test_existing_ledger'
    ] LOOP
        EXECUTE format(
            'INSERT INTO expected_rows SELECT %L, jsonb_agg(to_jsonb(t) ORDER BY to_jsonb(t)) '
            'FROM trading_core.%I t', relation_name, relation_name
        );
    END LOOP;
END;
$$;

\ir ../../infra/timescale/init/011_online_record_tables.sql

DO $$
DECLARE
    expected RECORD;
    actual JSONB;
    relation_name TEXT;
    partition_count INTEGER;
    routed_count INTEGER;
    index_count INTEGER;
BEGIN
    FOR expected IN SELECT * FROM expected_rows LOOP
        EXECUTE format(
            'SELECT jsonb_agg(to_jsonb(t) ORDER BY to_jsonb(t)) FROM trading_core.%I t',
            expected.table_name
        ) INTO actual;
        IF actual IS DISTINCT FROM expected.contents THEN
            RAISE EXCEPTION 'existing content changed in %', expected.table_name;
        END IF;
    END LOOP;

    FOREACH relation_name IN ARRAY ARRAY['order_events', 'trade_events'] LOOP
        SELECT count(*) INTO partition_count FROM pg_inherits
        WHERE inhparent = format('trading_core.%I', relation_name)::regclass;
        IF partition_count <> 5 THEN
            RAISE EXCEPTION '% expected 5 initial monthly partitions, got %',
                relation_name, partition_count;
        END IF;
        EXECUTE format('SELECT count(DISTINCT tableoid) FROM trading_core.%I', relation_name)
            INTO routed_count;
        IF routed_count <> 2 THEN
            RAISE EXCEPTION '% did not route month-boundary records separately', relation_name;
        END IF;
    END LOOP;

    SELECT count(*) INTO index_count FROM pg_indexes
    WHERE schemaname = 'trading_core' AND indexname IN (
        'idx_trading_core_order_events_client_order_ts',
        'idx_trading_core_order_events_trade_date_status',
        'idx_trading_core_order_events_trace_ts',
        'idx_trading_core_trade_events_client_order_ts',
        'idx_trading_core_trade_events_trade_id',
        'idx_trading_core_trade_events_trace_ts',
        'idx_trading_core_account_snapshots_account_ts',
        'idx_trading_core_position_snapshots_account_inst_ts'
    );
    IF index_count <> 8 THEN
        RAISE EXCEPTION 'expected 8 record/snapshot indexes, got %', index_count;
    END IF;
END;
$$;

-- Older WAL months can be prepared explicitly without any retention/deletion side effect.
SELECT trading_core.ensure_online_record_partitions(DATE '2000-01-15');
SELECT trading_core.ensure_online_record_partitions(DATE '2000-01-01');
INSERT INTO trading_core.trade_events (
    trade_date, idempotency_key, account_id, client_order_id, instrument_id,
    trade_id, filled_volume, avg_fill_price, ts_ns, trace_id
) VALUES ('2000-01-31', 'historical', 'test-account', 'old-order', 'test-contract',
          'old-trade', 1, 12.5, 1, 'test-trace');

DO $$
DECLARE
    routed_table TEXT;
BEGIN
    SELECT tableoid::regclass::TEXT INTO routed_table
    FROM trading_core.trade_events WHERE idempotency_key = 'historical';
    IF routed_table <> 'trading_core.trade_events_200001' THEN
        RAISE EXCEPTION 'historical record routed incorrectly: %', routed_table;
    END IF;
    BEGIN
        PERFORM trading_core.ensure_online_record_partitions(NULL);
        RAISE EXCEPTION 'null month was accepted';
    EXCEPTION WHEN raise_exception THEN
        IF SQLERRM <> 'partition month must not be null' THEN
            RAISE;
        END IF;
    END;
END;
$$;

SELECT 'online record schema: PASS' AS result;
