CREATE DATABASE IF NOT EXISTS quant_hft;

CREATE TABLE IF NOT EXISTS quant_hft.market_snapshots_kafka (
    instrument_id String,
    exchange_id String,
    trading_day String,
    action_day String,
    update_time String,
    update_millisec Int32,
    last_price Float64,
    bid_price_1 Float64,
    ask_price_1 Float64,
    bid_volume_1 Int64,
    ask_volume_1 Int64,
    volume Int64,
    settlement_price Float64,
    average_price_raw Float64,
    average_price_norm Float64,
    is_valid_settlement Bool,
    exchange_ts_ns Int64,
    recv_ts_ns Int64
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:9092',
    kafka_topic_list = 'quant_hft.market.snapshots.v1',
    kafka_group_name = 'quant_hft.market.snapshots.v1.consumer',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1,
    kafka_handle_error_mode = 'stream',
    kafka_skip_broken_messages = 1000;

CREATE TABLE IF NOT EXISTS quant_hft.market_snapshots_fact (
    instrument_id String,
    exchange_id String,
    trading_day String,
    action_day String,
    update_time String,
    update_millisec Int32,
    last_price Float64,
    bid_price_1 Float64,
    ask_price_1 Float64,
    bid_volume_1 Int64,
    ask_volume_1 Int64,
    volume Int64,
    settlement_price Float64,
    average_price_raw Float64,
    average_price_norm Float64,
    is_valid_settlement Bool,
    exchange_ts_ns Int64,
    recv_ts_ns Int64,
    ingest_ts DateTime DEFAULT now()
)
ENGINE = MergeTree
PARTITION BY toDate(toDateTime(intDiv(recv_ts_ns, 1000000000)))
ORDER BY (instrument_id, recv_ts_ns)
TTL toDateTime(intDiv(recv_ts_ns, 1000000000)) + INTERVAL 30 DAY
SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS quant_hft.market_snapshots_mv
TO quant_hft.market_snapshots_fact AS
SELECT
    instrument_id,
    exchange_id,
    trading_day,
    action_day,
    update_time,
    update_millisec,
    last_price,
    bid_price_1,
    ask_price_1,
    bid_volume_1,
    ask_volume_1,
    volume,
    settlement_price,
    average_price_raw,
    average_price_norm,
    is_valid_settlement,
    exchange_ts_ns,
    recv_ts_ns
FROM quant_hft.market_snapshots_kafka;
