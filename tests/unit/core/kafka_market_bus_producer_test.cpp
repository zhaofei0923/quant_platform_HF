#include <string>

#include <gtest/gtest.h>

#include "quant_hft/core/kafka_market_bus_producer.h"

namespace quant_hft {

TEST(KafkaMarketBusProducerTest, SerializeMarketSnapshotJsonEscapesFields) {
    MarketSnapshot snapshot;
    snapshot.instrument_id = "SHFE.\"ag2406\"";
    snapshot.exchange_id = "SHFE";
    snapshot.trading_day = "20260212";
    snapshot.exchange_ts_ns = 123;
    snapshot.recv_ts_ns = 456;

    const std::string payload = KafkaMarketBusProducer::SerializeMarketSnapshotJson(snapshot);
    EXPECT_NE(payload.find("\"instrument_id\":\"SHFE.\\\"ag2406\\\"\""), std::string::npos);
    EXPECT_NE(payload.find("\"exchange_ts_ns\":123"), std::string::npos);
    EXPECT_NE(payload.find("\"recv_ts_ns\":456"), std::string::npos);
}

TEST(KafkaMarketBusProducerTest, PublishSucceedsWithCustomProducerCommand) {
    KafkaConnectionConfig config;
    config.brokers = "127.0.0.1:9092";
    config.market_topic = "quant_hft.market.snapshots.v1";
    config.producer_command_template = "cat >/dev/null";
    KafkaMarketBusProducer producer(config);

    MarketSnapshot snapshot;
    snapshot.instrument_id = "SHFE.ag2406";
    snapshot.exchange_ts_ns = 1;
    snapshot.recv_ts_ns = 2;

    std::string error;
    EXPECT_TRUE(producer.PublishMarketSnapshot(snapshot, &error)) << error;
}

TEST(KafkaMarketBusProducerTest, PublishFailsWhenCommandReturnsNonZero) {
    KafkaConnectionConfig config;
    config.brokers = "127.0.0.1:9092";
    config.market_topic = "quant_hft.market.snapshots.v1";
    config.producer_command_template = "false";
    KafkaMarketBusProducer producer(config);

    MarketSnapshot snapshot;
    snapshot.instrument_id = "SHFE.ag2406";
    snapshot.exchange_ts_ns = 1;
    snapshot.recv_ts_ns = 2;

    std::string error;
    EXPECT_FALSE(producer.PublishMarketSnapshot(snapshot, &error));
    EXPECT_NE(error.find("failed"), std::string::npos);
}

TEST(KafkaMarketBusProducerTest, PublishRejectsUnsafeTopic) {
    KafkaConnectionConfig config;
    config.brokers = "127.0.0.1:9092";
    config.market_topic = "quant_hft.market;drop";
    config.producer_command_template = "cat >/dev/null";
    KafkaMarketBusProducer producer(config);

    MarketSnapshot snapshot;
    snapshot.instrument_id = "SHFE.ag2406";
    snapshot.exchange_ts_ns = 1;
    snapshot.recv_ts_ns = 2;

    std::string error;
    EXPECT_FALSE(producer.PublishMarketSnapshot(snapshot, &error));
    EXPECT_NE(error.find("invalid"), std::string::npos);
}

}  // namespace quant_hft
