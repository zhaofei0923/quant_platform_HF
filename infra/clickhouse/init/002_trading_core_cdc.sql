CREATE DATABASE IF NOT EXISTS quant_hft;

-- Orders CDC
CREATE TABLE IF NOT EXISTS quant_hft.orders_cdc_kafka (
    raw String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:9092',
    kafka_topic_list = 'quant_hft_pg.trading_core.orders',
    kafka_group_name = 'quant_hft_orders_cdc_v1',
    kafka_format = 'JSONAsString',
    kafka_num_consumers = 1,
    kafka_handle_error_mode = 'stream';

CREATE TABLE IF NOT EXISTS quant_hft.orders_cdc (
    order_id Int64,
    order_ref String,
    account_id String,
    strategy_id String,
    instrument_id String,
    order_status String,
    volume_original Int32,
    volume_traded Int32,
    op String,
    source_ts_ms Int64,
    event_time DateTime64(3, 'UTC'),
    ingest_time DateTime64(3, 'UTC'),
    raw_payload String
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(event_time)
ORDER BY (account_id, instrument_id, order_ref, event_time)
TTL toDateTime(event_time) + INTERVAL 180 DAY DELETE;

CREATE MATERIALIZED VIEW IF NOT EXISTS quant_hft.orders_cdc_mv
TO quant_hft.orders_cdc
AS
SELECT
    toInt64OrZero(JSON_VALUE(raw, '$.order_id')) AS order_id,
    ifNull(JSON_VALUE(raw, '$.order_ref'), '') AS order_ref,
    ifNull(JSON_VALUE(raw, '$.account_id'), '') AS account_id,
    ifNull(JSON_VALUE(raw, '$.strategy_id'), '') AS strategy_id,
    ifNull(JSON_VALUE(raw, '$.instrument_id'), '') AS instrument_id,
    ifNull(JSON_VALUE(raw, '$.order_status'), '') AS order_status,
    toInt32OrZero(JSON_VALUE(raw, '$.volume_original')) AS volume_original,
    toInt32OrZero(JSON_VALUE(raw, '$.volume_traded')) AS volume_traded,
    ifNull(JSON_VALUE(raw, '$.__op'), '') AS op,
    toInt64OrZero(JSON_VALUE(raw, '$.__source_ts_ms')) AS source_ts_ms,
    if(
        source_ts_ms > 0,
        toDateTime64(source_ts_ms / 1000.0, 3, 'UTC'),
        now64(3, 'UTC')
    ) AS event_time,
    now64(3, 'UTC') AS ingest_time,
    raw AS raw_payload
FROM quant_hft.orders_cdc_kafka;

-- Trades CDC
CREATE TABLE IF NOT EXISTS quant_hft.trades_cdc_kafka (
    raw String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:9092',
    kafka_topic_list = 'quant_hft_pg.trading_core.trades',
    kafka_group_name = 'quant_hft_trades_cdc_v1',
    kafka_format = 'JSONAsString',
    kafka_num_consumers = 1,
    kafka_handle_error_mode = 'stream';

CREATE TABLE IF NOT EXISTS quant_hft.trades_cdc (
    trade_id String,
    order_id Int64,
    order_ref String,
    account_id String,
    strategy_id String,
    instrument_id String,
    direction String,
    offset_flag String,
    volume Int32,
    price Decimal(16, 4),
    op String,
    source_ts_ms Int64,
    event_time DateTime64(3, 'UTC'),
    ingest_time DateTime64(3, 'UTC'),
    raw_payload String
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(event_time)
ORDER BY (account_id, instrument_id, trade_id, event_time)
TTL toDateTime(event_time) + INTERVAL 180 DAY DELETE;

CREATE MATERIALIZED VIEW IF NOT EXISTS quant_hft.trades_cdc_mv
TO quant_hft.trades_cdc
AS
SELECT
    ifNull(JSON_VALUE(raw, '$.trade_id'), '') AS trade_id,
    toInt64OrZero(JSON_VALUE(raw, '$.order_id')) AS order_id,
    ifNull(JSON_VALUE(raw, '$.order_ref'), '') AS order_ref,
    ifNull(JSON_VALUE(raw, '$.account_id'), '') AS account_id,
    ifNull(JSON_VALUE(raw, '$.strategy_id'), '') AS strategy_id,
    ifNull(JSON_VALUE(raw, '$.instrument_id'), '') AS instrument_id,
    ifNull(JSON_VALUE(raw, '$.direction'), '') AS direction,
    ifNull(JSON_VALUE(raw, '$.offset_flag'), '') AS offset_flag,
    toInt32OrZero(JSON_VALUE(raw, '$.volume')) AS volume,
    toDecimal64OrZero(JSON_VALUE(raw, '$.price'), 4) AS price,
    ifNull(JSON_VALUE(raw, '$.__op'), '') AS op,
    toInt64OrZero(JSON_VALUE(raw, '$.__source_ts_ms')) AS source_ts_ms,
    if(
        source_ts_ms > 0,
        toDateTime64(source_ts_ms / 1000.0, 3, 'UTC'),
        now64(3, 'UTC')
    ) AS event_time,
    now64(3, 'UTC') AS ingest_time,
    raw AS raw_payload
FROM quant_hft.trades_cdc_kafka;

-- Position detail CDC
CREATE TABLE IF NOT EXISTS quant_hft.position_detail_cdc_kafka (
    raw String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:9092',
    kafka_topic_list = 'quant_hft_pg.trading_core.position_detail',
    kafka_group_name = 'quant_hft_position_detail_cdc_v1',
    kafka_format = 'JSONAsString',
    kafka_num_consumers = 1,
    kafka_handle_error_mode = 'stream';

CREATE TABLE IF NOT EXISTS quant_hft.position_detail_cdc (
    position_id Int64,
    account_id String,
    strategy_id String,
    instrument_id String,
    open_trade_id String,
    open_date String,
    volume Int32,
    close_volume Int32,
    position_status Int16,
    op String,
    source_ts_ms Int64,
    event_time DateTime64(3, 'UTC'),
    ingest_time DateTime64(3, 'UTC'),
    raw_payload String
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(event_time)
ORDER BY (account_id, strategy_id, instrument_id, position_id, event_time)
TTL toDateTime(event_time) + INTERVAL 180 DAY DELETE;

CREATE MATERIALIZED VIEW IF NOT EXISTS quant_hft.position_detail_cdc_mv
TO quant_hft.position_detail_cdc
AS
SELECT
    toInt64OrZero(JSON_VALUE(raw, '$.position_id')) AS position_id,
    ifNull(JSON_VALUE(raw, '$.account_id'), '') AS account_id,
    ifNull(JSON_VALUE(raw, '$.strategy_id'), '') AS strategy_id,
    ifNull(JSON_VALUE(raw, '$.instrument_id'), '') AS instrument_id,
    ifNull(JSON_VALUE(raw, '$.open_trade_id'), '') AS open_trade_id,
    ifNull(JSON_VALUE(raw, '$.open_date'), '') AS open_date,
    toInt32OrZero(JSON_VALUE(raw, '$.volume')) AS volume,
    toInt32OrZero(JSON_VALUE(raw, '$.close_volume')) AS close_volume,
    toInt16OrZero(JSON_VALUE(raw, '$.position_status')) AS position_status,
    ifNull(JSON_VALUE(raw, '$.__op'), '') AS op,
    toInt64OrZero(JSON_VALUE(raw, '$.__source_ts_ms')) AS source_ts_ms,
    if(
        source_ts_ms > 0,
        toDateTime64(source_ts_ms / 1000.0, 3, 'UTC'),
        now64(3, 'UTC')
    ) AS event_time,
    now64(3, 'UTC') AS ingest_time,
    raw AS raw_payload
FROM quant_hft.position_detail_cdc_kafka;

-- Account funds CDC
CREATE TABLE IF NOT EXISTS quant_hft.account_funds_cdc_kafka (
    raw String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:9092',
    kafka_topic_list = 'quant_hft_pg.trading_core.account_funds',
    kafka_group_name = 'quant_hft_account_funds_cdc_v1',
    kafka_format = 'JSONAsString',
    kafka_num_consumers = 1,
    kafka_handle_error_mode = 'stream';

CREATE TABLE IF NOT EXISTS quant_hft.account_funds_cdc (
    account_id String,
    trading_day String,
    balance Decimal(18, 2),
    available Decimal(18, 2),
    curr_margin Decimal(18, 2),
    commission Decimal(18, 2),
    position_profit Decimal(18, 2),
    close_profit Decimal(18, 2),
    op String,
    source_ts_ms Int64,
    event_time DateTime64(3, 'UTC'),
    ingest_time DateTime64(3, 'UTC'),
    raw_payload String
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(event_time)
ORDER BY (account_id, trading_day, event_time)
TTL toDateTime(event_time) + INTERVAL 180 DAY DELETE;

CREATE MATERIALIZED VIEW IF NOT EXISTS quant_hft.account_funds_cdc_mv
TO quant_hft.account_funds_cdc
AS
SELECT
    ifNull(JSON_VALUE(raw, '$.account_id'), '') AS account_id,
    ifNull(JSON_VALUE(raw, '$.trading_day'), '') AS trading_day,
    toDecimal64OrZero(JSON_VALUE(raw, '$.balance'), 2) AS balance,
    toDecimal64OrZero(JSON_VALUE(raw, '$.available'), 2) AS available,
    toDecimal64OrZero(JSON_VALUE(raw, '$.curr_margin'), 2) AS curr_margin,
    toDecimal64OrZero(JSON_VALUE(raw, '$.commission'), 2) AS commission,
    toDecimal64OrZero(JSON_VALUE(raw, '$.position_profit'), 2) AS position_profit,
    toDecimal64OrZero(JSON_VALUE(raw, '$.close_profit'), 2) AS close_profit,
    ifNull(JSON_VALUE(raw, '$.__op'), '') AS op,
    toInt64OrZero(JSON_VALUE(raw, '$.__source_ts_ms')) AS source_ts_ms,
    if(
        source_ts_ms > 0,
        toDateTime64(source_ts_ms / 1000.0, 3, 'UTC'),
        now64(3, 'UTC')
    ) AS event_time,
    now64(3, 'UTC') AS ingest_time,
    raw AS raw_payload
FROM quant_hft.account_funds_cdc_kafka;

-- Risk events CDC
CREATE TABLE IF NOT EXISTS quant_hft.risk_events_cdc_kafka (
    raw String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:9092',
    kafka_topic_list = 'quant_hft_pg.trading_core.risk_events',
    kafka_group_name = 'quant_hft_risk_events_cdc_v1',
    kafka_format = 'JSONAsString',
    kafka_num_consumers = 1,
    kafka_handle_error_mode = 'stream';

CREATE TABLE IF NOT EXISTS quant_hft.risk_events_cdc (
    event_id Int64,
    account_id String,
    strategy_id String,
    instrument_id String,
    event_type Int16,
    event_level Int16,
    event_desc String,
    op String,
    source_ts_ms Int64,
    event_time DateTime64(3, 'UTC'),
    ingest_time DateTime64(3, 'UTC'),
    raw_payload String
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(event_time)
ORDER BY (account_id, event_id, event_time)
TTL toDateTime(event_time) + INTERVAL 180 DAY DELETE;

CREATE MATERIALIZED VIEW IF NOT EXISTS quant_hft.risk_events_cdc_mv
TO quant_hft.risk_events_cdc
AS
SELECT
    toInt64OrZero(JSON_VALUE(raw, '$.event_id')) AS event_id,
    ifNull(JSON_VALUE(raw, '$.account_id'), '') AS account_id,
    ifNull(JSON_VALUE(raw, '$.strategy_id'), '') AS strategy_id,
    ifNull(JSON_VALUE(raw, '$.instrument_id'), '') AS instrument_id,
    toInt16OrZero(JSON_VALUE(raw, '$.event_type')) AS event_type,
    toInt16OrZero(JSON_VALUE(raw, '$.event_level')) AS event_level,
    ifNull(JSON_VALUE(raw, '$.event_desc'), '') AS event_desc,
    ifNull(JSON_VALUE(raw, '$.__op'), '') AS op,
    toInt64OrZero(JSON_VALUE(raw, '$.__source_ts_ms')) AS source_ts_ms,
    if(
        source_ts_ms > 0,
        toDateTime64(source_ts_ms / 1000.0, 3, 'UTC'),
        now64(3, 'UTC')
    ) AS event_time,
    now64(3, 'UTC') AS ingest_time,
    raw AS raw_payload
FROM quant_hft.risk_events_cdc_kafka;

-- Settlement summary CDC
CREATE TABLE IF NOT EXISTS quant_hft.settlement_summary_cdc_kafka (
    raw String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:9092',
    kafka_topic_list = 'quant_hft_pg.trading_core.settlement_summary',
    kafka_group_name = 'quant_hft_settlement_summary_cdc_v1',
    kafka_format = 'JSONAsString',
    kafka_num_consumers = 1,
    kafka_handle_error_mode = 'stream';

CREATE TABLE IF NOT EXISTS quant_hft.settlement_summary_cdc (
    settlement_id Int64,
    trading_day String,
    account_id String,
    balance Decimal(18, 2),
    available Decimal(18, 2),
    curr_margin Decimal(18, 2),
    position_profit Decimal(18, 2),
    close_profit Decimal(18, 2),
    commission Decimal(18, 2),
    op String,
    source_ts_ms Int64,
    event_time DateTime64(3, 'UTC'),
    ingest_time DateTime64(3, 'UTC'),
    raw_payload String
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(event_time)
ORDER BY (account_id, trading_day, settlement_id, event_time)
TTL toDateTime(event_time) + INTERVAL 180 DAY DELETE;

CREATE MATERIALIZED VIEW IF NOT EXISTS quant_hft.settlement_summary_cdc_mv
TO quant_hft.settlement_summary_cdc
AS
SELECT
    toInt64OrZero(JSON_VALUE(raw, '$.settlement_id')) AS settlement_id,
    ifNull(JSON_VALUE(raw, '$.trading_day'), '') AS trading_day,
    ifNull(JSON_VALUE(raw, '$.account_id'), '') AS account_id,
    toDecimal64OrZero(JSON_VALUE(raw, '$.balance'), 2) AS balance,
    toDecimal64OrZero(JSON_VALUE(raw, '$.available'), 2) AS available,
    toDecimal64OrZero(JSON_VALUE(raw, '$.curr_margin'), 2) AS curr_margin,
    toDecimal64OrZero(JSON_VALUE(raw, '$.position_profit'), 2) AS position_profit,
    toDecimal64OrZero(JSON_VALUE(raw, '$.close_profit'), 2) AS close_profit,
    toDecimal64OrZero(JSON_VALUE(raw, '$.commission'), 2) AS commission,
    ifNull(JSON_VALUE(raw, '$.__op'), '') AS op,
    toInt64OrZero(JSON_VALUE(raw, '$.__source_ts_ms')) AS source_ts_ms,
    if(
        source_ts_ms > 0,
        toDateTime64(source_ts_ms / 1000.0, 3, 'UTC'),
        now64(3, 'UTC')
    ) AS event_time,
    now64(3, 'UTC') AS ingest_time,
    raw AS raw_payload
FROM quant_hft.settlement_summary_cdc_kafka;

-- Settlement detail CDC
CREATE TABLE IF NOT EXISTS quant_hft.settlement_detail_cdc_kafka (
    raw String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:9092',
    kafka_topic_list = 'quant_hft_pg.trading_core.settlement_detail',
    kafka_group_name = 'quant_hft_settlement_detail_cdc_v1',
    kafka_format = 'JSONAsString',
    kafka_num_consumers = 1,
    kafka_handle_error_mode = 'stream';

CREATE TABLE IF NOT EXISTS quant_hft.settlement_detail_cdc (
    detail_id Int64,
    trading_day String,
    settlement_id Int64,
    position_id Int64,
    instrument_id String,
    volume Int32,
    settlement_price Decimal(16, 4),
    profit Decimal(18, 2),
    op String,
    source_ts_ms Int64,
    event_time DateTime64(3, 'UTC'),
    ingest_time DateTime64(3, 'UTC'),
    raw_payload String
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(event_time)
ORDER BY (trading_day, instrument_id, position_id, event_time)
TTL toDateTime(event_time) + INTERVAL 180 DAY DELETE;

CREATE MATERIALIZED VIEW IF NOT EXISTS quant_hft.settlement_detail_cdc_mv
TO quant_hft.settlement_detail_cdc
AS
SELECT
    toInt64OrZero(JSON_VALUE(raw, '$.detail_id')) AS detail_id,
    ifNull(JSON_VALUE(raw, '$.trading_day'), '') AS trading_day,
    toInt64OrZero(JSON_VALUE(raw, '$.settlement_id')) AS settlement_id,
    toInt64OrZero(JSON_VALUE(raw, '$.position_id')) AS position_id,
    ifNull(JSON_VALUE(raw, '$.instrument_id'), '') AS instrument_id,
    toInt32OrZero(JSON_VALUE(raw, '$.volume')) AS volume,
    toDecimal64OrZero(JSON_VALUE(raw, '$.settlement_price'), 4) AS settlement_price,
    toDecimal64OrZero(JSON_VALUE(raw, '$.profit'), 2) AS profit,
    ifNull(JSON_VALUE(raw, '$.__op'), '') AS op,
    toInt64OrZero(JSON_VALUE(raw, '$.__source_ts_ms')) AS source_ts_ms,
    if(
        source_ts_ms > 0,
        toDateTime64(source_ts_ms / 1000.0, 3, 'UTC'),
        now64(3, 'UTC')
    ) AS event_time,
    now64(3, 'UTC') AS ingest_time,
    raw AS raw_payload
FROM quant_hft.settlement_detail_cdc_kafka;

-- Settlement prices CDC
CREATE TABLE IF NOT EXISTS quant_hft.settlement_prices_cdc_kafka (
    raw String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:9092',
    kafka_topic_list = 'quant_hft_pg.trading_core.settlement_prices',
    kafka_group_name = 'quant_hft_settlement_prices_cdc_v1',
    kafka_format = 'JSONAsString',
    kafka_num_consumers = 1,
    kafka_handle_error_mode = 'stream';

CREATE TABLE IF NOT EXISTS quant_hft.settlement_prices_cdc (
    price_id Int64,
    trading_day String,
    instrument_id String,
    exchange_id String,
    source String,
    settlement_price Decimal(16, 4),
    is_final UInt8,
    op String,
    source_ts_ms Int64,
    event_time DateTime64(3, 'UTC'),
    ingest_time DateTime64(3, 'UTC'),
    raw_payload String
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(event_time)
ORDER BY (trading_day, instrument_id, source, event_time)
TTL toDateTime(event_time) + INTERVAL 180 DAY DELETE;

CREATE MATERIALIZED VIEW IF NOT EXISTS quant_hft.settlement_prices_cdc_mv
TO quant_hft.settlement_prices_cdc
AS
SELECT
    toInt64OrZero(JSON_VALUE(raw, '$.price_id')) AS price_id,
    ifNull(JSON_VALUE(raw, '$.trading_day'), '') AS trading_day,
    ifNull(JSON_VALUE(raw, '$.instrument_id'), '') AS instrument_id,
    ifNull(JSON_VALUE(raw, '$.exchange_id'), '') AS exchange_id,
    ifNull(JSON_VALUE(raw, '$.source'), '') AS source,
    toDecimal64OrZero(JSON_VALUE(raw, '$.settlement_price'), 4) AS settlement_price,
    toUInt8OrZero(JSON_VALUE(raw, '$.is_final')) AS is_final,
    ifNull(JSON_VALUE(raw, '$.__op'), '') AS op,
    toInt64OrZero(JSON_VALUE(raw, '$.__source_ts_ms')) AS source_ts_ms,
    if(
        source_ts_ms > 0,
        toDateTime64(source_ts_ms / 1000.0, 3, 'UTC'),
        now64(3, 'UTC')
    ) AS event_time,
    now64(3, 'UTC') AS ingest_time,
    raw AS raw_payload
FROM quant_hft.settlement_prices_cdc_kafka;
