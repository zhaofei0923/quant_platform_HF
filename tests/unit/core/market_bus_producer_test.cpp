#include <filesystem>
#include <fstream>
#include <string>

#include <gtest/gtest.h>

#include "quant_hft/core/market_bus_producer.h"

namespace quant_hft {

TEST(MarketBusProducerTest, DisabledProducerNoops) {
    MarketBusProducer producer(/*bootstrap_servers=*/"", /*topic=*/"market.ticks.v1");
    MarketSnapshot snapshot;
    snapshot.instrument_id = "SHFE.ag2406";
    snapshot.last_price = 4500.0;

    const auto result = producer.PublishTick(snapshot);
    EXPECT_TRUE(result.ok);
    EXPECT_EQ(result.reason, "disabled");
    EXPECT_EQ(producer.PublishedCount(), 0U);
    EXPECT_EQ(producer.FailedCount(), 0U);
}

TEST(MarketBusProducerTest, PublishWritesSpoolLine) {
    const auto tmp_root =
        std::filesystem::temp_directory_path() /
        ("quant_hft_market_bus_test_" + std::to_string(NowEpochNanos()));
    std::filesystem::create_directories(tmp_root);

    MarketBusProducer producer("127.0.0.1:9092", "market.ticks.v1", tmp_root.string());
    MarketSnapshot snapshot;
    snapshot.instrument_id = "SHFE.ag2406";
    snapshot.exchange_id = "SHFE";
    snapshot.trading_day = "20260212";
    snapshot.last_price = 4501.0;
    snapshot.bid_price_1 = 4500.0;
    snapshot.ask_price_1 = 4502.0;
    snapshot.bid_volume_1 = 5;
    snapshot.ask_volume_1 = 6;
    snapshot.volume = 7;
    snapshot.exchange_ts_ns = 123;
    snapshot.recv_ts_ns = 456;

    const auto result = producer.PublishTick(snapshot);
    EXPECT_TRUE(result.ok);
    EXPECT_EQ(result.reason, "ok");
    EXPECT_EQ(producer.PublishedCount(), 1U);
    EXPECT_EQ(producer.FailedCount(), 0U);

    const auto spool_file = tmp_root / "market.ticks.v1.jsonl";
    std::ifstream in(spool_file);
    ASSERT_TRUE(in.is_open());
    std::string line;
    std::getline(in, line);
    EXPECT_NE(line.find("\"instrument_id\":\"SHFE.ag2406\""), std::string::npos);
    EXPECT_NE(line.find("\"topic\":\"market.ticks.v1\""), std::string::npos);

    std::error_code ec;
    std::filesystem::remove_all(tmp_root, ec);
}

}  // namespace quant_hft
