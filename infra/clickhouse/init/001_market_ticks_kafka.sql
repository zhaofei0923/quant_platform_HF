CREATE DATABASE IF NOT EXISTS quant_hft;

CREATE TABLE IF NOT EXISTS quant_hft.market_ticks_kafka (
    topic String,
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
    exchange_ts_ns Int64,
    recv_ts_ns Int64,
    published_ts_ns Int64
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:9092',
    kafka_topic_list = 'market.ticks.v1',
    kafka_group_name = 'quant_hft_ticks_v1',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1,
    kafka_handle_error_mode = 'stream';

CREATE TABLE IF NOT EXISTS quant_hft.market_ticks (
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
    exchange_ts_ns Int64,
    recv_ts_ns Int64,
    published_ts_ns Int64,
    event_time DateTime64(3, 'UTC')
)
ENGINE = MergeTree
PARTITION BY toYYYYMMDD(event_time)
ORDER BY (instrument_id, recv_ts_ns)
TTL toDateTime(event_time) + INTERVAL 180 DAY DELETE;

CREATE MATERIALIZED VIEW IF NOT EXISTS quant_hft.market_ticks_mv
TO quant_hft.market_ticks
AS
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
    exchange_ts_ns,
    recv_ts_ns,
    published_ts_ns,
    toDateTime64(recv_ts_ns / 1000000000.0, 3, 'UTC') AS event_time
FROM quant_hft.market_ticks_kafka;
