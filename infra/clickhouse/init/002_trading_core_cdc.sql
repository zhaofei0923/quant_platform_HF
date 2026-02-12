CREATE DATABASE IF NOT EXISTS quant_hft;

CREATE TABLE IF NOT EXISTS quant_hft.order_events_cdc_kafka (
    raw_message String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:9092',
    kafka_topic_list = 'quant_hft.trading_core.order_events',
    kafka_group_name = 'quant_hft.trading_core.order_events.consumer',
    kafka_format = 'JSONAsString',
    kafka_num_consumers = 1,
    kafka_handle_error_mode = 'stream',
    kafka_skip_broken_messages = 1000;

CREATE TABLE IF NOT EXISTS quant_hft.order_events_cdc_fact (
    account_id String,
    client_order_id String,
    instrument_id String,
    event_source String,
    trade_id String,
    status String,
    filled_volume Int32,
    total_volume Int32,
    avg_fill_price Float64,
    ts_ns Int64,
    trade_date Date,
    raw_message String,
    ingest_ts DateTime DEFAULT now()
)
ENGINE = MergeTree
PARTITION BY trade_date
ORDER BY (trade_date, client_order_id, ts_ns);

CREATE MATERIALIZED VIEW IF NOT EXISTS quant_hft.order_events_cdc_mv
TO quant_hft.order_events_cdc_fact AS
SELECT
    ifNull(JSONExtractString(raw_message, 'account_id'), '') AS account_id,
    ifNull(JSONExtractString(raw_message, 'client_order_id'), '') AS client_order_id,
    ifNull(JSONExtractString(raw_message, 'instrument_id'), '') AS instrument_id,
    ifNull(JSONExtractString(raw_message, 'event_source'), '') AS event_source,
    ifNull(JSONExtractString(raw_message, 'trade_id'), '') AS trade_id,
    ifNull(JSONExtractString(raw_message, 'status'), '') AS status,
    toInt32OrZero(JSONExtractInt(raw_message, 'filled_volume')) AS filled_volume,
    toInt32OrZero(JSONExtractInt(raw_message, 'total_volume')) AS total_volume,
    toFloat64OrZero(JSONExtractFloat(raw_message, 'avg_fill_price')) AS avg_fill_price,
    toInt64OrZero(JSONExtractInt(raw_message, 'ts_ns')) AS ts_ns,
    toDate(toDateTime(intDiv(toInt64OrZero(JSONExtractInt(raw_message, 'ts_ns')), 1000000000))) AS trade_date,
    raw_message
FROM quant_hft.order_events_cdc_kafka;

CREATE VIEW IF NOT EXISTS quant_hft.order_events_latest AS
SELECT
    account_id,
    client_order_id,
    argMax(instrument_id, ts_ns) AS instrument_id,
    argMax(event_source, ts_ns) AS event_source,
    argMax(trade_id, ts_ns) AS trade_id,
    argMax(status, ts_ns) AS status,
    argMax(filled_volume, ts_ns) AS filled_volume,
    argMax(total_volume, ts_ns) AS total_volume,
    argMax(avg_fill_price, ts_ns) AS avg_fill_price,
    max(ts_ns) AS ts_ns
FROM quant_hft.order_events_cdc_fact
GROUP BY account_id, client_order_id;

CREATE TABLE IF NOT EXISTS quant_hft.trade_events_cdc_kafka (
    raw_message String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:9092',
    kafka_topic_list = 'quant_hft.trading_core.trade_events',
    kafka_group_name = 'quant_hft.trading_core.trade_events.consumer',
    kafka_format = 'JSONAsString',
    kafka_num_consumers = 1,
    kafka_handle_error_mode = 'stream',
    kafka_skip_broken_messages = 1000;

CREATE TABLE IF NOT EXISTS quant_hft.trade_events_cdc_fact (
    account_id String,
    client_order_id String,
    trade_id String,
    instrument_id String,
    filled_volume Int32,
    avg_fill_price Float64,
    ts_ns Int64,
    trade_date Date,
    raw_message String,
    ingest_ts DateTime DEFAULT now()
)
ENGINE = MergeTree
PARTITION BY trade_date
ORDER BY (trade_date, client_order_id, trade_id, ts_ns);

CREATE MATERIALIZED VIEW IF NOT EXISTS quant_hft.trade_events_cdc_mv
TO quant_hft.trade_events_cdc_fact AS
SELECT
    ifNull(JSONExtractString(raw_message, 'account_id'), '') AS account_id,
    ifNull(JSONExtractString(raw_message, 'client_order_id'), '') AS client_order_id,
    ifNull(JSONExtractString(raw_message, 'trade_id'), '') AS trade_id,
    ifNull(JSONExtractString(raw_message, 'instrument_id'), '') AS instrument_id,
    toInt32OrZero(JSONExtractInt(raw_message, 'filled_volume')) AS filled_volume,
    toFloat64OrZero(JSONExtractFloat(raw_message, 'avg_fill_price')) AS avg_fill_price,
    toInt64OrZero(JSONExtractInt(raw_message, 'ts_ns')) AS ts_ns,
    toDate(toDateTime(intDiv(toInt64OrZero(JSONExtractInt(raw_message, 'ts_ns')), 1000000000))) AS trade_date,
    raw_message
FROM quant_hft.trade_events_cdc_kafka;

CREATE VIEW IF NOT EXISTS quant_hft.trade_events_latest AS
SELECT
    account_id,
    client_order_id,
    trade_id,
    argMax(instrument_id, ts_ns) AS instrument_id,
    argMax(filled_volume, ts_ns) AS filled_volume,
    argMax(avg_fill_price, ts_ns) AS avg_fill_price,
    max(ts_ns) AS ts_ns
FROM quant_hft.trade_events_cdc_fact
GROUP BY account_id, client_order_id, trade_id;

CREATE TABLE IF NOT EXISTS quant_hft.account_snapshots_cdc_kafka (
    raw_message String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:9092',
    kafka_topic_list = 'quant_hft.trading_core.account_snapshots',
    kafka_group_name = 'quant_hft.trading_core.account_snapshots.consumer',
    kafka_format = 'JSONAsString',
    kafka_num_consumers = 1,
    kafka_handle_error_mode = 'stream',
    kafka_skip_broken_messages = 1000;

CREATE TABLE IF NOT EXISTS quant_hft.account_snapshots_cdc_fact (
    account_id String,
    investor_id String,
    balance Float64,
    available Float64,
    curr_margin Float64,
    frozen_margin Float64,
    commission Float64,
    close_profit Float64,
    position_profit Float64,
    trading_day String,
    ts_ns Int64,
    trade_date Date,
    raw_message String,
    ingest_ts DateTime DEFAULT now()
)
ENGINE = MergeTree
PARTITION BY trade_date
ORDER BY (trade_date, account_id, ts_ns);

CREATE MATERIALIZED VIEW IF NOT EXISTS quant_hft.account_snapshots_cdc_mv
TO quant_hft.account_snapshots_cdc_fact AS
SELECT
    ifNull(JSONExtractString(raw_message, 'account_id'), '') AS account_id,
    ifNull(JSONExtractString(raw_message, 'investor_id'), '') AS investor_id,
    toFloat64OrZero(JSONExtractFloat(raw_message, 'balance')) AS balance,
    toFloat64OrZero(JSONExtractFloat(raw_message, 'available')) AS available,
    toFloat64OrZero(JSONExtractFloat(raw_message, 'curr_margin')) AS curr_margin,
    toFloat64OrZero(JSONExtractFloat(raw_message, 'frozen_margin')) AS frozen_margin,
    toFloat64OrZero(JSONExtractFloat(raw_message, 'commission')) AS commission,
    toFloat64OrZero(JSONExtractFloat(raw_message, 'close_profit')) AS close_profit,
    toFloat64OrZero(JSONExtractFloat(raw_message, 'position_profit')) AS position_profit,
    ifNull(JSONExtractString(raw_message, 'trading_day'), '') AS trading_day,
    toInt64OrZero(JSONExtractInt(raw_message, 'ts_ns')) AS ts_ns,
    toDate(toDateTime(intDiv(toInt64OrZero(JSONExtractInt(raw_message, 'ts_ns')), 1000000000))) AS trade_date,
    raw_message
FROM quant_hft.account_snapshots_cdc_kafka;

CREATE VIEW IF NOT EXISTS quant_hft.account_snapshots_latest AS
SELECT
    account_id,
    argMax(investor_id, ts_ns) AS investor_id,
    argMax(balance, ts_ns) AS balance,
    argMax(available, ts_ns) AS available,
    argMax(curr_margin, ts_ns) AS curr_margin,
    argMax(frozen_margin, ts_ns) AS frozen_margin,
    argMax(commission, ts_ns) AS commission,
    argMax(close_profit, ts_ns) AS close_profit,
    argMax(position_profit, ts_ns) AS position_profit,
    argMax(trading_day, ts_ns) AS trading_day,
    max(ts_ns) AS ts_ns
FROM quant_hft.account_snapshots_cdc_fact
GROUP BY account_id;

CREATE TABLE IF NOT EXISTS quant_hft.position_snapshots_cdc_kafka (
    raw_message String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:9092',
    kafka_topic_list = 'quant_hft.trading_core.position_snapshots',
    kafka_group_name = 'quant_hft.trading_core.position_snapshots.consumer',
    kafka_format = 'JSONAsString',
    kafka_num_consumers = 1,
    kafka_handle_error_mode = 'stream',
    kafka_skip_broken_messages = 1000;

CREATE TABLE IF NOT EXISTS quant_hft.position_snapshots_cdc_fact (
    account_id String,
    instrument_id String,
    posi_direction String,
    position Int32,
    today_position Int32,
    yd_position Int32,
    use_margin Float64,
    position_profit Float64,
    ts_ns Int64,
    trade_date Date,
    raw_message String,
    ingest_ts DateTime DEFAULT now()
)
ENGINE = MergeTree
PARTITION BY trade_date
ORDER BY (trade_date, account_id, instrument_id, posi_direction, ts_ns);

CREATE MATERIALIZED VIEW IF NOT EXISTS quant_hft.position_snapshots_cdc_mv
TO quant_hft.position_snapshots_cdc_fact AS
SELECT
    ifNull(JSONExtractString(raw_message, 'account_id'), '') AS account_id,
    ifNull(JSONExtractString(raw_message, 'instrument_id'), '') AS instrument_id,
    ifNull(JSONExtractString(raw_message, 'posi_direction'), '') AS posi_direction,
    toInt32OrZero(JSONExtractInt(raw_message, 'position')) AS position,
    toInt32OrZero(JSONExtractInt(raw_message, 'today_position')) AS today_position,
    toInt32OrZero(JSONExtractInt(raw_message, 'yd_position')) AS yd_position,
    toFloat64OrZero(JSONExtractFloat(raw_message, 'use_margin')) AS use_margin,
    toFloat64OrZero(JSONExtractFloat(raw_message, 'position_profit')) AS position_profit,
    toInt64OrZero(JSONExtractInt(raw_message, 'ts_ns')) AS ts_ns,
    toDate(toDateTime(intDiv(toInt64OrZero(JSONExtractInt(raw_message, 'ts_ns')), 1000000000))) AS trade_date,
    raw_message
FROM quant_hft.position_snapshots_cdc_kafka;

CREATE VIEW IF NOT EXISTS quant_hft.position_snapshots_latest AS
SELECT
    account_id,
    instrument_id,
    posi_direction,
    argMax(position, ts_ns) AS position,
    argMax(today_position, ts_ns) AS today_position,
    argMax(yd_position, ts_ns) AS yd_position,
    argMax(use_margin, ts_ns) AS use_margin,
    argMax(position_profit, ts_ns) AS position_profit,
    max(ts_ns) AS ts_ns
FROM quant_hft.position_snapshots_cdc_fact
GROUP BY account_id, instrument_id, posi_direction;
