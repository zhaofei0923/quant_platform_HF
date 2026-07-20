#include "quant_hft/services/canonical_warmup_history.h"

#include <gtest/gtest.h>

#include <chrono>
#include <filesystem>
#include <fstream>
#include <string>

namespace quant_hft {
namespace {

namespace fs = std::filesystem;

fs::path MakeTempDir(const std::string& suffix) {
    const auto nonce = std::chrono::steady_clock::now().time_since_epoch().count();
    const fs::path path = fs::temp_directory_path() /
                          ("quant_hft_canonical_warmup_" + suffix + "_" + std::to_string(nonce));
    fs::create_directories(path);
    return path;
}

std::string Header() {
    return "instrument_id,exchange_id,trading_day,action_day,minute,open,high,low,close,"
           "analysis_open,analysis_high,analysis_low,analysis_close,analysis_price_offset,"
           "volume,ts_ns,period_end_ts_ns,finalized_ts_ns,expected_source_bars,"
           "observed_source_bars,is_complete,is_session_endpoint,strategy_eligible,"
           "volume_complete,has_conflict,is_recovery_replay\n";
}

std::string Row(const std::string& instrument_id, std::int64_t ts_ns, double close,
                int expected = 5, int observed = 5, int complete = 1, int endpoint = 0,
                int eligible = 1, int volume_complete = 1, int conflict = 0, int replay = 0) {
    return instrument_id + ",DCE,20260721,20260720,20260721 21:05,100,105,95," +
           std::to_string(close) + ",100,105,95," + std::to_string(close) + ",0,10," +
           std::to_string(ts_ns) + "," + std::to_string(ts_ns + 10) + "," +
           std::to_string(ts_ns + 20) + "," + std::to_string(expected) + "," +
           std::to_string(observed) + "," + std::to_string(complete) + "," +
           std::to_string(endpoint) + "," + std::to_string(eligible) + "," +
           std::to_string(volume_complete) + "," + std::to_string(conflict) + "," +
           std::to_string(replay) + "\n";
}

void WriteBars(const fs::path& root, const std::string& trading_day, const std::string& payload) {
    const fs::path path =
        root / ("trading_day=" + trading_day) / "varieties" / "c" / "market" / "bars_5m.csv";
    fs::create_directories(path.parent_path());
    std::ofstream output(path, std::ios::binary | std::ios::trunc);
    output << Header() << payload;
}

std::string LegacyHeader() {
    return "instrument_id,exchange_id,trading_day,action_day,minute,open,high,low,close,"
           "analysis_open,analysis_high,analysis_low,analysis_close,analysis_price_offset,"
           "volume,ts_ns\n";
}

std::string LegacyRow(const std::string& minute, double open, double high, double low, double close,
                      std::int64_t volume, std::int64_t ts_ns) {
    return "c2609,DCE,20260721,20260720," + minute + "," + std::to_string(open) + "," +
           std::to_string(high) + "," + std::to_string(low) + "," + std::to_string(close) + "," +
           std::to_string(open) + "," + std::to_string(high) + "," + std::to_string(low) + "," +
           std::to_string(close) + ",0," + std::to_string(volume) + "," + std::to_string(ts_ns) +
           "\n";
}

void WriteLegacyBars(const fs::path& root, const std::string& trading_day, int timeframe_minutes,
                     const std::string& payload) {
    const fs::path path = root / ("trading_day=" + trading_day) / "varieties" / "c" / "market" /
                          ("bars_" + std::to_string(timeframe_minutes) + "m.csv");
    fs::create_directories(path.parent_path());
    std::ofstream output(path, std::ios::binary | std::ios::trunc);
    output << LegacyHeader() << payload;
}

TEST(CanonicalWarmupHistoryTest, LoadsRecentStrictCanonicalRowsAcrossTradingDays) {
    const fs::path root = MakeTempDir("strict");
    WriteBars(root, "20260720",
              Row("c2609", 100, 101) + Row("c2609", 200, 102) + Row("c2611", 250, 103) +
                  Row("c2609", 300, 104, 5, 4, 0));
    WriteBars(root, "20260721",
              Row("c2609", 400, 103) + Row("c2609", 500, 104) +
                  Row("c2609", 600, 105, 5, 5, 1, 0, 1, 1, 1) + Row("c2609", 1'100, 105));

    CanonicalWarmupHistoryOptions options;
    options.market_data_root = root.string();
    options.product_id = "c";
    options.instrument_id = "c2609";
    options.limit = 3;
    options.max_ts_ns = 1'000;
    CanonicalWarmupHistoryResult result;
    std::string error;

    ASSERT_TRUE(LoadCanonicalWarmupHistory(options, &result, &error)) << error;
    ASSERT_EQ(result.states.size(), 3U);
    EXPECT_EQ(result.states[0].ts_ns, 200);
    EXPECT_EQ(result.states[1].ts_ns, 400);
    EXPECT_EQ(result.states[2].ts_ns, 500);
    EXPECT_TRUE(result.states[0].has_bar);
    EXPECT_EQ(result.states[0].timeframe_minutes, 5);
    EXPECT_EQ(result.files_scanned, 2U);
    EXPECT_GT(result.rows_rejected, 0U);

    fs::remove_all(root);
}

TEST(CanonicalWarmupHistoryTest, ExcludesBothSidesOfTimestampConflict) {
    const fs::path root = MakeTempDir("conflict");
    WriteBars(root, "20260720", Row("c2609", 100, 101) + Row("c2609", 200, 102));
    WriteBars(root, "20260721", Row("c2609", 100, 104) + Row("c2609", 300, 103));

    CanonicalWarmupHistoryOptions options;
    options.market_data_root = root.string();
    options.product_id = "c";
    options.instrument_id = "c2609";
    options.max_ts_ns = 1'000;
    CanonicalWarmupHistoryResult result;
    std::string error;

    ASSERT_TRUE(LoadCanonicalWarmupHistory(options, &result, &error)) << error;
    ASSERT_EQ(result.states.size(), 2U);
    EXPECT_EQ(result.states[0].ts_ns, 200);
    EXPECT_EQ(result.states[1].ts_ns, 300);
    EXPECT_EQ(result.conflicting_rows, 1U);

    fs::remove_all(root);
}

TEST(CanonicalWarmupHistoryTest, ValidatesLegacyFiveMinuteBarAgainstExactOneMinuteSources) {
    const fs::path root = MakeTempDir("legacy_valid");
    WriteLegacyBars(root, "20260721", 1,
                    LegacyRow("20260721 21:00", 100, 102, 99, 101, 10, 100) +
                        LegacyRow("20260721 21:01", 101, 103, 100, 102, 11, 200) +
                        LegacyRow("20260721 21:02", 102, 104, 101, 103, 12, 300) +
                        LegacyRow("20260721 21:03", 103, 105, 102, 104, 13, 400) +
                        LegacyRow("20260721 21:04", 104, 106, 103, 105, 14, 500));
    WriteLegacyBars(root, "20260721", 5, LegacyRow("20260721 21:00", 100, 106, 99, 105, 60, 500));

    CanonicalWarmupHistoryOptions options;
    options.market_data_root = root.string();
    options.product_id = "c";
    options.instrument_id = "c2609";
    options.max_ts_ns = 1'000;
    CanonicalWarmupHistoryResult result;
    std::string error;

    ASSERT_TRUE(LoadCanonicalWarmupHistory(options, &result, &error)) << error;
    ASSERT_EQ(result.states.size(), 1U);
    EXPECT_EQ(result.states[0].ts_ns, 500);
    EXPECT_DOUBLE_EQ(result.states[0].bar_volume, 60.0);
    EXPECT_EQ(result.legacy_rows_validated, 1U);

    fs::remove_all(root);
}

TEST(CanonicalWarmupHistoryTest, RejectsLegacyBarWithMissingOrConflictingSourceMinute) {
    const fs::path root = MakeTempDir("legacy_conflict");
    WriteLegacyBars(root, "20260721", 1,
                    LegacyRow("20260721 21:00", 100, 102, 99, 101, 10, 100) +
                        LegacyRow("20260721 21:01", 101, 103, 100, 102, 11, 200) +
                        LegacyRow("20260721 21:02", 102, 104, 101, 103, 12, 300) +
                        LegacyRow("20260721 21:03", 103, 105, 102, 104, 13, 400) +
                        LegacyRow("20260721 21:03", 103, 106, 102, 105, 13, 401) +
                        LegacyRow("20260721 21:04", 104, 106, 103, 105, 14, 500));
    WriteLegacyBars(root, "20260721", 5, LegacyRow("20260721 21:00", 100, 106, 99, 105, 60, 500));

    CanonicalWarmupHistoryOptions options;
    options.market_data_root = root.string();
    options.product_id = "c";
    options.instrument_id = "c2609";
    options.max_ts_ns = 1'000;
    CanonicalWarmupHistoryResult result;
    std::string error;

    ASSERT_TRUE(LoadCanonicalWarmupHistory(options, &result, &error)) << error;
    EXPECT_TRUE(result.states.empty());
    EXPECT_EQ(result.legacy_rows_validated, 0U);
    EXPECT_EQ(result.rows_rejected, 1U);

    fs::remove_all(root);
}

TEST(CanonicalWarmupHistoryTest, MissingRootSafelyReturnsNoWarmup) {
    CanonicalWarmupHistoryOptions options;
    options.market_data_root =
        (fs::temp_directory_path() / "quant_hft_canonical_warmup_missing").string();
    options.product_id = "c";
    options.instrument_id = "c2609";
    CanonicalWarmupHistoryResult result;
    std::string error;

    ASSERT_TRUE(LoadCanonicalWarmupHistory(options, &result, &error)) << error;
    EXPECT_TRUE(result.states.empty());
    EXPECT_EQ(result.files_scanned, 0U);
}

}  // namespace
}  // namespace quant_hft
