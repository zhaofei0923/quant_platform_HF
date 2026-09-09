#include "quant_hft/monitoring/dashboard_snapshot.h"

#include <gtest/gtest.h>
#include <sys/stat.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <limits>
#include <string>
#include <thread>

#include "quant_hft/core/simple_json.h"

namespace quant_hft {
namespace {

using Json = simple_json::Value;

std::int64_t NowMs() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::system_clock::now().time_since_epoch())
        .count();
}

Json Parse(const std::string& text) {
    Json value;
    std::string error;
    EXPECT_TRUE(simple_json::ParseStrict(text, &value, &error)) << error;
    return value;
}

const Json& Field(const Json& value, const char* key) { return value.object_value.at(key); }

class DashboardSnapshotTest : public ::testing::Test {
   protected:
    void SetUp() override {
        char pattern[] = "/tmp/quant-dashboard-snapshot-XXXXXX";
        const auto* result = ::mkdtemp(pattern);
        ASSERT_NE(result, nullptr);
        directory_ = result;
        output_ = directory_ + "/input/private.json";
        ASSERT_TRUE(writer_.Start(output_, identity_, &error_)) << error_;
    }

    void TearDown() override {
        writer_.Stop();
        // The path is the exact private directory returned by mkdtemp in SetUp.
        if (!directory_.empty()) std::filesystem::remove_all(directory_);
    }

    TradingAccountSnapshot Account() {
        TradingAccountSnapshot snapshot;
        snapshot.account_id = "cash-account";  // CTP AccountID can differ from InvestorID.
        snapshot.investor_id = identity_.account_id;
        snapshot.balance = 1234.5;
        snapshot.available = 1000;
        snapshot.curr_margin = 234.5;
        snapshot.position_profit = -17.25;
        snapshot.commission = 2.5;
        snapshot.trading_day = "20260907";
        snapshot.ts_ns = (NowMs() - 1000) * 1000000;
        snapshot.source = "ctp";
        return snapshot;
    }

    QueryResult<InvestorPositionSnapshot> Positions() {
        QueryResult<InvestorPositionSnapshot> result;
        result.metadata.account_id = identity_.account_id;
        result.metadata.source = "ctp";
        result.metadata.trading_day = "20260907";
        result.metadata.generation = 7;
        result.metadata.complete = true;
        result.metadata.success = true;
        result.metadata.full_account = true;
        InvestorPositionSnapshot row;
        row.account_id = identity_.account_id;
        row.investor_id = identity_.account_id;
        row.instrument_id = "hc2610";
        row.exchange_id = "SHFE";
        row.posi_direction = "2";
        row.hedge_flag = "1";
        row.position_date = "1";
        row.position = 3;
        row.today_position = 3;
        row.long_frozen = 1;
        row.open_volume = 3;
        row.open_cost = 97500;
        row.use_margin = 13000;
        row.position_profit = -60;
        row.source = "ctp";
        row.ts_ns = (NowMs() - 1000) * 1000000;
        result.rows.push_back(row);
        row.position_date = "2";
        row.today_position = 0;
        row.yd_position = 2;
        row.position = 2;
        row.posi_direction = "3";
        result.rows.push_back(row);
        return result;
    }

    MarketSnapshot Market(const std::string& instrument = "hc2610",
                          EpochNanos recv_ts_ns = 0) {
        MarketSnapshot snapshot;
        snapshot.instrument_id = instrument;
        snapshot.exchange_id = "SHFE";
        snapshot.trading_day = "20260907";
        snapshot.last_price = 3498.0;
        snapshot.bid_price_1 = 3497.0;
        snapshot.ask_price_1 = 3499.0;
        snapshot.bid_volume_1 = 5;
        snapshot.ask_volume_1 = 7;
        snapshot.volume = 12345;
        snapshot.open_interest = 6789;
        snapshot.recv_ts_ns = recv_ts_ns > 0 ? recv_ts_ns : (NowMs() - 1000) * 1000000;
        snapshot.exchange_ts_ns = snapshot.recv_ts_ns - 1000000;
        return snapshot;
    }

    StrategyRiskSnapshot Risk(EpochNanos as_of_ns = 0) {
        StrategyRiskSnapshot row;
        row.account_id = identity_.account_id;
        row.strategy_id = "hc_outer";
        row.owner_strategy_id = "kama_candidate_hc";
        row.instrument_id = "hc2610";
        row.net = 3;
        row.avg_open = 3460.0;
        row.initial_stop = 3420.0;
        row.trailing_stop = 3450.0;
        row.effective_stop = 3450.0;
        row.stop_kind = StrategyStopKind::kTrailing;
        row.take_profit = 3540.0;
        row.as_of_ns = as_of_ns > 0 ? as_of_ns : (NowMs() - 2000) * 1000000;
        return row;
    }

    Json Snapshot() { return Parse(writer_.RenderSnapshot(NowMs())); }

    Json ReadPublished() {
        std::ifstream input(output_);
        return Parse(std::string((std::istreambuf_iterator<char>(input)), {}));
    }

    RuntimeIdentity identity_{"simnow", "9999", "test-investor", "dashboard-test"};
    DashboardSnapshotWriter writer_;
    std::string directory_;
    std::string output_;
    std::string error_;
};

TEST_F(DashboardSnapshotTest, InitialSnapshotIsMissingAndPrivate) {
    const auto root = ReadPublished();
    EXPECT_EQ(Field(root, "schema_version").number_value, 2);
    EXPECT_EQ(Field(Field(root, "identity"), "instance_id").string_value, "dashboard-test");
    EXPECT_EQ(Field(Field(root, "account"), "quality").string_value, "missing");
    EXPECT_TRUE(Field(Field(root, "account"), "data").IsNull());
    EXPECT_TRUE(Field(Field(root, "positions"), "data").IsNull());
    EXPECT_TRUE(Field(Field(root, "market_quotes"), "data").IsNull());
    EXPECT_TRUE(Field(Field(root, "strategy_risk"), "data").IsNull());
    struct stat info {};
    ASSERT_EQ(::stat(output_.c_str(), &info), 0);
    EXPECT_EQ(info.st_mode & 0777U, 0640U);
    ASSERT_EQ(::stat((directory_ + "/input").c_str(), &info), 0);
    EXPECT_EQ(info.st_mode & 0777U, 0750U);
}

TEST_F(DashboardSnapshotTest, MarketQuotesUsePerRowTimeAndRejectOlderOrInvalidTicks) {
    const EpochNanos newest = (NowMs() - 1000) * 1000000;
    writer_.CaptureMarket(Market("hc2610", newest));
    auto root = Snapshot();
    const auto& block = Field(root, "market_quotes");
    ASSERT_EQ(Field(block, "quality").string_value, "ok");
    ASSERT_EQ(Field(block, "data").array_value.size(), 1U);
    const auto& row = Field(block, "data").array_value.front();
    EXPECT_EQ(Field(row, "instrument_id").string_value, "hc2610");
    EXPECT_DOUBLE_EQ(Field(row, "last_price").number_value, 3498.0);
    EXPECT_EQ(Field(row, "as_of_ms").number_value, newest / 1000000);

    auto older = Market("hc2610", newest - 1000000);
    older.last_price = 1.0;
    writer_.CaptureMarket(older);
    auto invalid = Market("hc2610", newest + 1000000);
    invalid.last_price = std::numeric_limits<double>::max();
    writer_.CaptureMarket(invalid);
    root = Snapshot();
    EXPECT_DOUBLE_EQ(Field(Field(root, "market_quotes"), "data")
                         .array_value.front()
                         .object_value.at("last_price")
                         .number_value,
                     3498.0);
    EXPECT_GE(writer_.dropped_updates(), 1U);
    EXPECT_GE(Field(Field(root, "market_quotes"), "dropped_updates").number_value, 2);
}

TEST_F(DashboardSnapshotTest, NewTradingDayClearsOldMarketRowsAndUnknownBookPricesAreNull) {
    writer_.CaptureMarket(Market("hc2610"));
    auto next = Market("rb2610", NowMs() * 1000000);
    next.trading_day = "20260908";
    next.bid_price_1 = std::numeric_limits<double>::max();
    next.ask_price_1 = 1e100;
    writer_.CaptureMarket(next);
    const auto root = Snapshot();
    const auto& rows = Field(Field(root, "market_quotes"), "data").array_value;
    ASSERT_EQ(rows.size(), 1U);
    EXPECT_EQ(Field(rows.front(), "instrument_id").string_value, "rb2610");
    EXPECT_TRUE(Field(rows.front(), "bid_price_1").IsNull());
    EXPECT_TRUE(Field(rows.front(), "ask_price_1").IsNull());
}

TEST_F(DashboardSnapshotTest, StrategyRiskKeepsOwnerEffectiveStopAndCalculationTimestamp) {
    const EpochNanos calculated_at = (NowMs() - 2000) * 1000000;
    const EpochNanos observed_at = NowMs() * 1000000;
    writer_.CaptureStrategyRisk({Risk(calculated_at)}, observed_at, "20260907");
    const auto root = Snapshot();
    const auto& block = Field(root, "strategy_risk");
    ASSERT_EQ(Field(block, "quality").string_value, "ok");
    ASSERT_EQ(Field(block, "data").array_value.size(), 1U);
    const auto& row = Field(block, "data").array_value.front();
    EXPECT_EQ(Field(row, "strategy_id").string_value, "hc_outer");
    EXPECT_EQ(Field(row, "owner_strategy_id").string_value, "kama_candidate_hc");
    EXPECT_DOUBLE_EQ(Field(row, "effective_stop").number_value, 3450.0);
    EXPECT_EQ(Field(row, "stop_kind").string_value, "trailing");
    EXPECT_EQ(Field(row, "as_of_ms").number_value, calculated_at / 1000000);
}

TEST_F(DashboardSnapshotTest, StrategyRiskRequiresAnAccountBoundTradingDay) {
    writer_.CaptureStrategyRisk({Risk()}, NowMs() * 1000000, "");
    const auto root = Snapshot();
    const auto& block = Field(root, "strategy_risk");
    EXPECT_EQ(Field(block, "quality").string_value, "failed");
    EXPECT_EQ(Field(block, "error_code").string_value, "invalid_fields");
    EXPECT_EQ(Field(block, "dropped_updates").number_value, 1);
    EXPECT_TRUE(Field(block, "data").IsNull());
}

TEST_F(DashboardSnapshotTest, StrategyRiskRejectsForeignBatchAndCompleteEmptyClearsRows) {
    const EpochNanos observed_at = NowMs() * 1000000;
    writer_.CaptureStrategyRisk({Risk()}, observed_at, "20260907");
    auto foreign = Risk();
    foreign.account_id = "foreign";
    foreign.net = 99;
    writer_.CaptureStrategyRisk({foreign}, observed_at + 1, "20260907");
    auto root = Snapshot();
    EXPECT_EQ(Field(Field(root, "strategy_risk"), "error_code").string_value,
              "identity_mismatch");
    EXPECT_EQ(Field(Field(root, "strategy_risk"), "data").array_value.front().object_value.at("net")
                  .number_value,
              3);

    writer_.CaptureStrategyRisk({}, observed_at + 2, "20260907");
    root = Snapshot();
    EXPECT_EQ(Field(Field(root, "strategy_risk"), "quality").string_value, "ok");
    EXPECT_TRUE(Field(Field(root, "strategy_risk"), "data").array_value.empty());
}

TEST_F(DashboardSnapshotTest, MarketAndRiskCachesRejectCapacityOverflowWithoutPartialReplacement) {
    const EpochNanos now_ns = NowMs() * 1000000;
    for (std::size_t i = 0; i < DashboardSnapshotWriter::kMaxMarketQuotes; ++i) {
        writer_.CaptureMarket(Market("hc" + std::to_string(i), now_ns));
    }
    writer_.CaptureMarket(Market("overflow", now_ns));
    auto root = Snapshot();
    EXPECT_EQ(Field(Field(root, "market_quotes"), "error_code").string_value,
              "market_capacity_exceeded");
    EXPECT_EQ(Field(Field(root, "market_quotes"), "data").array_value.size(),
              DashboardSnapshotWriter::kMaxMarketQuotes);
    EXPECT_EQ(Field(Field(root, "market_quotes"), "dropped_updates").number_value, 1);

    std::vector<StrategyRiskSnapshot> risks(DashboardSnapshotWriter::kMaxStrategyRiskRows + 1,
                                            Risk());
    for (std::size_t i = 0; i < risks.size(); ++i) {
        risks[i].instrument_id = "hc" + std::to_string(i);
    }
    writer_.CaptureStrategyRisk(risks, now_ns, "20260907");
    root = Snapshot();
    EXPECT_EQ(Field(Field(root, "strategy_risk"), "error_code").string_value,
              "strategy_risk_capacity_exceeded");
    EXPECT_EQ(Field(Field(root, "strategy_risk"), "dropped_updates").number_value, 1);
    EXPECT_TRUE(Field(Field(root, "strategy_risk"), "data").IsNull());
}

TEST_F(DashboardSnapshotTest, ZeroBalanceRemainsConfirmedAndTimestampDoesNotRefresh) {
    auto snapshot = Account();
    snapshot.balance = 0;
    snapshot.available = 0;
    writer_.CaptureAccount(snapshot);
    const auto root = Parse(writer_.RenderSnapshot(NowMs() + 30000));
    const auto& account = Field(root, "account");
    EXPECT_EQ(Field(account, "quality").string_value, "ok");
    EXPECT_EQ(Field(Field(account, "data"), "balance").number_value, 0);
    EXPECT_EQ(Field(account, "as_of_ms").number_value, snapshot.ts_ns / 1000000);
    EXPECT_EQ(Field(Field(account, "data"), "position_profit").number_value, -17.25);
}

TEST_F(DashboardSnapshotTest, IncompleteOrFailedBatchPreservesOldPositionsThenCompleteEmptyClears) {
    auto batch = Positions();
    writer_.CapturePositions(batch);
    auto root = Snapshot();
    const double original_time = Field(Field(root, "positions"), "as_of_ms").number_value;
    ASSERT_EQ(Field(Field(root, "positions"), "data").array_value.size(), 2U);
    for (int mode = 0; mode < 3; ++mode) {
        auto incomplete = batch;
        incomplete.rows.clear();
        incomplete.metadata.complete = mode != 0;
        incomplete.metadata.success = mode != 1;
        incomplete.metadata.full_account = mode != 2;
        writer_.CapturePositions(incomplete);
        root = Snapshot();
        const auto& positions = Field(root, "positions");
        EXPECT_EQ(Field(positions, "quality").string_value, "failed");
        EXPECT_EQ(Field(positions, "as_of_ms").number_value, original_time);
        EXPECT_EQ(Field(positions, "data").array_value.size(), 2U);
    }
    batch.rows.clear();
    writer_.CapturePositions(batch);
    root = Snapshot();
    EXPECT_EQ(Field(Field(root, "positions"), "quality").string_value, "ok");
    EXPECT_TRUE(Field(Field(root, "positions"), "data").array_value.empty());
}

TEST_F(DashboardSnapshotTest, RetainsLongShortDateHedgeAndFrozenRowsSeparately) {
    writer_.CapturePositions(Positions());
    const auto root = Snapshot();
    const auto& rows = Field(Field(root, "positions"), "data").array_value;
    ASSERT_EQ(rows.size(), 2U);
    EXPECT_EQ(Field(rows[0], "posi_direction").string_value, "2");
    EXPECT_EQ(Field(rows[0], "position_date").string_value, "1");
    EXPECT_EQ(Field(rows[0], "long_frozen").number_value, 1);
    EXPECT_EQ(Field(rows[1], "posi_direction").string_value, "3");
    EXPECT_EQ(Field(rows[1], "position_date").string_value, "2");
    EXPECT_EQ(Field(rows[1], "yd_position").number_value, 2);
    EXPECT_EQ(Field(rows[1], "hedge_flag").string_value, "1");
}

TEST_F(DashboardSnapshotTest, IdentityFailureNeverPublishesForeignRowsOrAccount) {
    writer_.CaptureAccount(Account());
    writer_.CapturePositions(Positions());
    auto foreign_account = Account();
    foreign_account.investor_id = "foreign-investor";
    foreign_account.balance = 9999999;
    writer_.CaptureAccount(foreign_account);
    auto foreign_positions = Positions();
    foreign_positions.rows[1].investor_id = "foreign-investor";
    foreign_positions.rows[0].position = 999;
    writer_.CapturePositions(foreign_positions);
    const auto root = Snapshot();
    const auto& account = Field(root, "account");
    const auto& positions = Field(root, "positions");
    EXPECT_EQ(Field(account, "error_code").string_value, "identity_mismatch");
    EXPECT_EQ(Field(Field(account, "data"), "balance").number_value, 1234.5);
    EXPECT_EQ(Field(positions, "error_code").string_value, "identity_mismatch");
    EXPECT_EQ(Field(Field(positions, "data").array_value[0], "position").number_value, 3);
    EXPECT_EQ(writer_.RenderSnapshot(NowMs()).find("foreign-investor"), std::string::npos);
}

TEST_F(DashboardSnapshotTest, NonFiniteOrOversizedDataFailsWithoutPartialReplacement) {
    auto account = Account();
    writer_.CaptureAccount(account);
    account.balance = std::numeric_limits<double>::quiet_NaN();
    writer_.CaptureAccount(account);
    auto batch = Positions();
    writer_.CapturePositions(batch);
    batch.rows[1].use_margin = std::numeric_limits<double>::infinity();
    writer_.CapturePositions(batch);
    auto root = Snapshot();
    EXPECT_EQ(Field(Field(root, "account"), "error_code").string_value, "invalid_fields");
    EXPECT_EQ(Field(Field(root, "positions"), "error_code").string_value, "invalid_fields");
    EXPECT_EQ(
        Field(Field(Field(root, "positions"), "data").array_value[1], "use_margin").number_value,
        13000);
    batch = Positions();
    batch.rows.resize(DashboardSnapshotWriter::kMaxPositions + 1, batch.rows[0]);
    writer_.CapturePositions(batch);
    root = Snapshot();
    EXPECT_EQ(Field(Field(root, "positions"), "error_code").string_value,
              "position_capacity_exceeded");
    EXPECT_EQ(Field(Field(root, "positions"), "data").array_value.size(), 2U);
}

TEST_F(DashboardSnapshotTest, QueryFailuresRetainDataAndGenerationWhileOldResultsAreRejected) {
    auto account = Account();
    auto batch = Positions();
    writer_.CaptureAccount(account);
    writer_.CapturePositions(batch);
    writer_.MarkAccountQueryFailed();
    writer_.MarkPositionQueryFailed();
    auto root = Snapshot();
    EXPECT_EQ(Field(Field(root, "account"), "quality").string_value, "failed");
    EXPECT_EQ(Field(Field(root, "positions"), "generation").number_value, 7);
    account.ts_ns -= 1000000000;
    writer_.CaptureAccount(account);
    batch.metadata.generation = 6;
    writer_.CapturePositions(batch);
    root = Snapshot();
    EXPECT_EQ(Field(Field(root, "account"), "error_code").string_value, "out_of_order");
    EXPECT_EQ(Field(Field(root, "positions"), "error_code").string_value, "out_of_order");
}

TEST_F(DashboardSnapshotTest, LockAndManifestPreventConcurrentOrDifferentInstanceWriters) {
    DashboardSnapshotWriter second;
    EXPECT_FALSE(second.Start(output_, identity_, &error_));
    writer_.Stop();
    auto foreign = identity_;
    foreign.instance = "another-instance";
    EXPECT_FALSE(second.Start(output_, foreign, &error_));
    EXPECT_NE(error_.find("identity mismatch"), std::string::npos);
    ASSERT_TRUE(second.Start(output_, identity_, &error_)) << error_;
    second.Stop();
}

TEST_F(DashboardSnapshotTest, RestartPublishesMissingInsteadOfReusingOldCashAndHoldings) {
    writer_.CaptureAccount(Account());
    writer_.CapturePositions(Positions());
    ASSERT_TRUE(writer_.PublishNow());
    writer_.Stop();
    ASSERT_TRUE(writer_.Start(output_, identity_, &error_)) << error_;
    const auto root = ReadPublished();
    EXPECT_TRUE(Field(Field(root, "account"), "data").IsNull());
    EXPECT_TRUE(Field(Field(root, "positions"), "data").IsNull());
}

TEST_F(DashboardSnapshotTest, FailedOutputNeverEscapesToCallbacksOrErasesLastPublishedFile) {
    auto account = Account();
    writer_.CaptureAccount(account);
    ASSERT_TRUE(writer_.PublishNow());
    std::filesystem::rename(directory_ + "/input", directory_ + "/moved");
    EXPECT_FALSE(writer_.PublishNow());
    EXPECT_GE(writer_.write_failures(), 1U);
    account.balance = 25;
    EXPECT_NO_THROW(writer_.CaptureAccount(account));
    EXPECT_NO_THROW(writer_.CapturePositions(Positions()));
    std::filesystem::rename(directory_ + "/moved", directory_ + "/input");
    const auto root = ReadPublished();
    EXPECT_EQ(Field(Field(Field(root, "account"), "data"), "balance").number_value, 1234.5);
    ASSERT_TRUE(writer_.PublishNow());
}

TEST_F(DashboardSnapshotTest, ConcurrentSerializationAndCaptureKeepValidWholeDocuments) {
    const auto account = Account();
    const auto positions = Positions();
    const auto market = Market();
    const auto risk = Risk();
    const EpochNanos observed_at = NowMs() * 1000000;
    std::atomic<bool> done{false};
    std::thread producer([&] {
        for (int count = 0; count < 10000; ++count) {
            writer_.CaptureAccount(account);
            writer_.CapturePositions(positions);
            writer_.CaptureMarket(market);
            writer_.CaptureStrategyRisk({risk}, observed_at, "20260907");
        }
        done.store(true);
    });
    do {
        const auto root = Snapshot();
        const auto& rows = Field(Field(root, "positions"), "data");
        EXPECT_TRUE(rows.IsNull() || rows.array_value.size() == 2U);
        const auto& quotes = Field(Field(root, "market_quotes"), "data");
        EXPECT_TRUE(quotes.IsNull() || quotes.array_value.size() == 1U);
        const auto& risks = Field(Field(root, "strategy_risk"), "data");
        EXPECT_TRUE(risks.IsNull() || risks.array_value.size() == 1U);
    } while (!done.load());
    producer.join();
    EXPECT_TRUE(writer_.PublishNow());
    EXPECT_TRUE(ReadPublished().IsObject());
}

TEST(DashboardSnapshotDisabledTest, InactiveWriterDoesNotCreateFilesOrAcceptData) {
    DashboardSnapshotWriter writer;
    writer.CaptureAccount(TradingAccountSnapshot{});
    writer.CapturePositions(QueryResult<InvestorPositionSnapshot>{});
    EXPECT_FALSE(writer.PublishNow());
    EXPECT_EQ(writer.dropped_updates(), 0U);
}

}  // namespace
}  // namespace quant_hft
