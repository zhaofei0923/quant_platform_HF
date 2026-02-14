#include <chrono>
#include <cstdio>
#include <fstream>
#include <vector>

#include <gtest/gtest.h>

#include "quant_hft/services/bar_aggregator.h"

namespace quant_hft {
namespace {

MarketSnapshot MakeSnapshot(const std::string& instrument_id,
                            const std::string& trading_day,
                            const std::string& action_day,
                            const std::string& update_time,
                            std::int32_t update_millisec,
                            double last_price,
                            std::int64_t volume,
                            EpochNanos ts_ns = 0) {
    MarketSnapshot snapshot;
    snapshot.instrument_id = instrument_id;
    snapshot.trading_day = trading_day;
    snapshot.action_day = action_day;
    snapshot.update_time = update_time;
    snapshot.update_millisec = update_millisec;
    snapshot.last_price = last_price;
    snapshot.volume = volume;
    snapshot.recv_ts_ns = ts_ns;
    return snapshot;
}

TEST(BarAggregatorTest, EmitsClosedMinuteBarWhenMinuteRolls) {
    BarAggregator aggregator;

    EXPECT_TRUE(aggregator.OnMarketSnapshot(
                            MakeSnapshot("SHFE.ag2406",
                                         "20260211",
                                         "20260211",
                                         "09:00:01",
                                         100,
                                         10.0,
                                         100))
                    .empty());
    EXPECT_TRUE(aggregator.OnMarketSnapshot(
                            MakeSnapshot("SHFE.ag2406",
                                         "20260211",
                                         "20260211",
                                         "09:00:45",
                                         200,
                                         12.0,
                                         108))
                    .empty());

    const auto bars = aggregator.OnMarketSnapshot(
        MakeSnapshot("SHFE.ag2406", "20260211", "20260211", "09:01:02", 50, 11.0, 120));
    ASSERT_EQ(bars.size(), 1U);
    EXPECT_EQ(bars[0].instrument_id, "SHFE.ag2406");
    EXPECT_EQ(bars[0].exchange_id, "SHFE");
    EXPECT_EQ(bars[0].trading_day, "20260211");
    EXPECT_EQ(bars[0].action_day, "20260211");
    EXPECT_EQ(bars[0].minute, "20260211 09:00");
    EXPECT_DOUBLE_EQ(bars[0].open, 10.0);
    EXPECT_DOUBLE_EQ(bars[0].high, 12.0);
    EXPECT_DOUBLE_EQ(bars[0].low, 10.0);
    EXPECT_DOUBLE_EQ(bars[0].close, 12.0);
    EXPECT_EQ(bars[0].volume, 8);

    const auto flush = aggregator.Flush();
    ASSERT_EQ(flush.size(), 1U);
    EXPECT_EQ(flush[0].minute, "20260211 09:01");
    EXPECT_DOUBLE_EQ(flush[0].open, 11.0);
    EXPECT_DOUBLE_EQ(flush[0].close, 11.0);
    EXPECT_EQ(flush[0].volume, 0);
}

TEST(BarAggregatorTest, FiltersNonTradingSessionByDefault) {
    BarAggregator aggregator;

    const auto bars = aggregator.OnMarketSnapshot(
        MakeSnapshot("SHFE.ag2406", "20260211", "20260211", "03:10:01", 0, 10.0, 1));
    EXPECT_TRUE(bars.empty());
    EXPECT_TRUE(aggregator.Flush().empty());
}

TEST(BarAggregatorTest, NightSessionUsesTradingDayAndKeepsActionDay) {
    BarAggregator aggregator;

    EXPECT_TRUE(aggregator.OnMarketSnapshot(
                            MakeSnapshot("DCE.i2409",
                                         "20260212",
                                         "20260211",
                                         "21:01:01",
                                         0,
                                         100.0,
                                         200))
                    .empty());
    const auto bars = aggregator.OnMarketSnapshot(
        MakeSnapshot("DCE.i2409", "20260212", "20260211", "21:02:01", 0, 101.0, 205));
    ASSERT_EQ(bars.size(), 1U);
    EXPECT_EQ(bars[0].minute, "20260212 21:01");
    EXPECT_EQ(bars[0].trading_day, "20260212");
    EXPECT_EQ(bars[0].action_day, "20260211");
}

TEST(BarAggregatorTest, TradingSessionMatcherHandlesDayAndNightWindowsByExchange) {
    BarAggregator aggregator;

    EXPECT_TRUE(aggregator.IsInTradingSession("SHFE", "09:00:00"));
    EXPECT_TRUE(aggregator.IsInTradingSession("SHFE", "14:59:59"));
    EXPECT_TRUE(aggregator.IsInTradingSession("SHFE", "21:00:00"));
    EXPECT_TRUE(aggregator.IsInTradingSession("SHFE", "00:59:59"));
    EXPECT_FALSE(aggregator.IsInTradingSession("SHFE", "03:10:00"));
    EXPECT_FALSE(aggregator.IsInTradingSession("CFFEX", "21:10:00"));
    EXPECT_FALSE(aggregator.IsInTradingSession("DCE", "23:10:00"));
}

TEST(BarAggregatorTest, ResolveTimestampUsesExchangeTimeInBacktestMode) {
    BarAggregatorConfig config;
    config.is_backtest_mode = true;
    BarAggregator aggregator(config);

    MarketSnapshot snapshot =
        MakeSnapshot("SHFE.ag2406", "20260211", "20260211", "09:00:01", 0, 10.0, 1, 100);
    snapshot.exchange_ts_ns = 200;

    EXPECT_TRUE(aggregator.OnMarketSnapshot(snapshot).empty());
    const auto bars = aggregator.Flush();
    ASSERT_EQ(bars.size(), 1U);
    EXPECT_EQ(bars[0].ts_ns, 200);
}

TEST(BarAggregatorTest, ResetInstrumentClearsActiveBucket) {
    BarAggregator aggregator;

    EXPECT_TRUE(aggregator.OnMarketSnapshot(
                            MakeSnapshot("SHFE.ag2406",
                                         "20260211",
                                         "20260211",
                                         "09:00:01",
                                         0,
                                         10.0,
                                         100))
                    .empty());

    aggregator.ResetInstrument("SHFE.ag2406");

    EXPECT_TRUE(aggregator.OnMarketSnapshot(
                            MakeSnapshot("SHFE.ag2406",
                                         "20260211",
                                         "20260211",
                                         "09:01:01",
                                         0,
                                         11.0,
                                         101))
                    .empty());
    const auto bars = aggregator.Flush();
    ASSERT_EQ(bars.size(), 1U);
    EXPECT_EQ(bars[0].minute, "20260211 09:01");
}

TEST(BarAggregatorTest, SessionConfigSupportsProductSpecificRules) {
    const auto now = std::chrono::steady_clock::now().time_since_epoch().count();
    const auto config_path =
        std::string("/tmp/trading_sessions_test_") + std::to_string(now) + ".yaml";
    {
        std::ofstream out(config_path);
        ASSERT_TRUE(out.is_open());
        out << "sessions:\n"
            << "  - exchange: CFFEX\n"
            << "    product: IF\n"
            << "    day: \"09:30-15:00\"\n"
            << "    night: null\n"
            << "  - exchange: CFFEX\n"
            << "    product: T\n"
            << "    day: \"09:30-15:15\"\n"
            << "    night: null\n";
    }

    BarAggregatorConfig config;
    config.trading_sessions_config_path = config_path;
    config.use_default_session_fallback = false;
    BarAggregator aggregator(config);

    const auto if_snapshot =
        MakeSnapshot("CFFEX.IF2503", "20260211", "20260211", "15:10:00", 0, 4300.0, 100);
    const auto tb_snapshot =
        MakeSnapshot("CFFEX.T2506", "20260211", "20260211", "15:10:00", 0, 101.0, 100);

    EXPECT_FALSE(aggregator.ShouldProcessSnapshot(if_snapshot));
    EXPECT_TRUE(aggregator.ShouldProcessSnapshot(tb_snapshot));

    std::remove(config_path.c_str());
}

TEST(BarAggregatorTest, SessionConfigPrefixMatchesInstrumentSymbolAfterDot) {
    const auto now = std::chrono::steady_clock::now().time_since_epoch().count();
    const auto config_path =
        std::string("/tmp/trading_sessions_prefix_test_") + std::to_string(now) + ".yaml";
    {
        std::ofstream out(config_path);
        ASSERT_TRUE(out.is_open());
        out << "sessions:\n"
            << "  - exchange: SHFE\n"
            << "    instrument_prefix: \"ag\"\n"
            << "    day: \"09:00-15:00\"\n"
            << "    night: \"21:00-02:30\"\n";
    }

    BarAggregatorConfig config;
    config.trading_sessions_config_path = config_path;
    config.use_default_session_fallback = false;
    BarAggregator aggregator(config);

    const auto ag_snapshot =
        MakeSnapshot("SHFE.ag2406", "20260211", "20260211", "01:30:00", 0, 6000.0, 100);
    const auto rb_snapshot =
        MakeSnapshot("SHFE.rb2406", "20260211", "20260211", "01:30:00", 0, 3500.0, 100);

    EXPECT_TRUE(aggregator.ShouldProcessSnapshot(ag_snapshot));
    EXPECT_FALSE(aggregator.ShouldProcessSnapshot(rb_snapshot));

    std::remove(config_path.c_str());
}

TEST(BarAggregatorTest, DefaultCffexSessionKeepsTreasuryAndFiltersEquityAfter1500) {
    BarAggregatorConfig config;
    config.trading_sessions_config_path =
        "/tmp/nonexistent_trading_sessions_for_default_rule_test.yaml";
    config.use_default_session_fallback = true;
    BarAggregator aggregator(config);

    const auto treasury_snapshot =
        MakeSnapshot("CFFEX.T2406", "20260211", "20260211", "15:05:00", 0, 101.0, 100);
    const auto equity_snapshot =
        MakeSnapshot("CFFEX.IF2406", "20260211", "20260211", "15:05:00", 0, 4300.0, 100);

    EXPECT_TRUE(aggregator.ShouldProcessSnapshot(treasury_snapshot));
    EXPECT_FALSE(aggregator.ShouldProcessSnapshot(equity_snapshot));
}

TEST(BarAggregatorTest, AggregatesOneMinuteBarsToHigherTimeframe) {
    std::vector<BarSnapshot> one_minute;
    one_minute.push_back(
        BarSnapshot{"SHFE.ag2406", "SHFE", "20260211", "20260211", "20260211 09:00", 10, 12, 9, 11, 5});
    one_minute.push_back(
        BarSnapshot{"SHFE.ag2406", "SHFE", "20260211", "20260211", "20260211 09:01", 11, 13, 10, 12, 7});
    one_minute.push_back(
        BarSnapshot{"SHFE.ag2406", "SHFE", "20260211", "20260211", "20260211 09:02", 12, 14, 11, 13, 6});

    const auto bars = BarAggregator::AggregateFromOneMinute(one_minute, 2);
    ASSERT_EQ(bars.size(), 2U);
    EXPECT_EQ(bars[0].minute, "20260211 09:00");
    EXPECT_DOUBLE_EQ(bars[0].open, 10.0);
    EXPECT_DOUBLE_EQ(bars[0].high, 13.0);
    EXPECT_DOUBLE_EQ(bars[0].low, 9.0);
    EXPECT_DOUBLE_EQ(bars[0].close, 12.0);
    EXPECT_EQ(bars[0].volume, 12);

    EXPECT_EQ(bars[1].minute, "20260211 09:02");
    EXPECT_DOUBLE_EQ(bars[1].open, 12.0);
    EXPECT_DOUBLE_EQ(bars[1].close, 13.0);
    EXPECT_EQ(bars[1].volume, 6);
}

}  // namespace
}  // namespace quant_hft
