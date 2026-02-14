#include <chrono>
#include <filesystem>
#include <fstream>
#include <string>

#include <gtest/gtest.h>

#include "quant_hft/core/local_wal_regulatory_sink.h"
#include "quant_hft/core/wal_replay_loader.h"
#include "quant_hft/services/in_memory_portfolio_ledger.h"
#include "quant_hft/services/order_state_machine.h"

namespace quant_hft {

namespace {

std::filesystem::path NewTempWalPath(const std::string& tag) {
    const auto now = std::chrono::steady_clock::now().time_since_epoch().count();
    return std::filesystem::temp_directory_path() /
           ("quant_hft_" + tag + "_" + std::to_string(now) + ".wal");
}

OrderEvent BuildEvent(const std::string& client_order_id,
                      OrderStatus status,
                      int total_volume,
                      int filled_volume,
                      double avg_fill_price,
                      EpochNanos ts_ns) {
    OrderEvent event;
    event.account_id = "a1";
    event.client_order_id = client_order_id;
    event.exchange_order_id = "ex-" + client_order_id;
    event.instrument_id = "SHFE.ag2406";
    event.status = status;
    event.total_volume = total_volume;
    event.filled_volume = filled_volume;
    event.avg_fill_price = avg_fill_price;
    event.reason = "";
    event.ts_ns = ts_ns;
    event.trace_id = "trace";
    return event;
}

}  // namespace

TEST(WalReplayLoaderTest, RebuildsOrderStateAndLedgerFromWal) {
    const auto wal_path = NewTempWalPath("replay");

    {
        LocalWalRegulatorySink sink(wal_path.string());
        sink.AppendOrderEvent(
            BuildEvent("ord-1", OrderStatus::kAccepted, 2, 0, 0.0, 1));
        sink.AppendTradeEvent(
            BuildEvent("ord-1", OrderStatus::kPartiallyFilled, 2, 1, 4500.0, 2));
        sink.AppendTradeEvent(
            BuildEvent("ord-1", OrderStatus::kFilled, 2, 2, 4510.0, 3));
        sink.Flush();
    }

    OrderStateMachine order_state_machine;
    InMemoryPortfolioLedger ledger;
    WalReplayLoader loader;
    const auto stats = loader.Replay(wal_path.string(), &order_state_machine, &ledger);

    EXPECT_EQ(stats.lines_total, 3);
    EXPECT_EQ(stats.events_loaded, 3);
    EXPECT_EQ(stats.parse_errors, 0);
    EXPECT_EQ(stats.state_rejected, 0);
    EXPECT_EQ(stats.ledger_applied, 3);

    const auto snapshot = order_state_machine.GetOrderSnapshot("ord-1");
    EXPECT_EQ(snapshot.status, OrderStatus::kFilled);
    EXPECT_EQ(snapshot.filled_volume, 2);
    EXPECT_TRUE(snapshot.is_terminal);

    const auto position =
        ledger.GetPositionSnapshot("a1", "SHFE.ag2406", PositionDirection::kLong);
    EXPECT_EQ(position.volume, 2);
    EXPECT_NEAR(position.avg_price, 4505.0, 1e-6);

    std::filesystem::remove(wal_path);
}

TEST(WalReplayLoaderTest, SupportsLegacyWalWithoutExtendedFields) {
    const auto wal_path = NewTempWalPath("legacy");
    {
        std::ofstream out(wal_path);
        out << "{\"seq\":1,\"kind\":\"order\",\"ts_ns\":10,"
               "\"account_id\":\"a1\",\"client_order_id\":\"ord-old\","
               "\"instrument_id\":\"SHFE.ag2406\",\"status\":1,"
               "\"filled_volume\":0}\n";
    }

    OrderStateMachine order_state_machine;
    InMemoryPortfolioLedger ledger;
    WalReplayLoader loader;
    const auto stats = loader.Replay(wal_path.string(), &order_state_machine, &ledger);

    EXPECT_EQ(stats.lines_total, 1);
    EXPECT_EQ(stats.events_loaded, 1);
    EXPECT_EQ(stats.ignored_lines, 0);
    EXPECT_EQ(stats.parse_errors, 0);
    EXPECT_EQ(stats.state_rejected, 0);

    const auto snapshot = order_state_machine.GetOrderSnapshot("ord-old");
    EXPECT_EQ(snapshot.status, OrderStatus::kAccepted);

    std::filesystem::remove(wal_path);
}

TEST(WalReplayLoaderTest, IgnoresRolloverLinesWithoutParseErrors) {
    const auto wal_path = NewTempWalPath("rollover");
    {
        std::ofstream out(wal_path);
        out << "{\"seq\":1,\"kind\":\"rollover\",\"ts_ns\":10,"
               "\"symbol\":\"rb\",\"action\":\"carry\","
               "\"from_instrument\":\"rb2305\",\"to_instrument\":\"rb2310\"}\n";
        out << "{\"seq\":2,\"kind\":\"order\",\"ts_ns\":11,"
               "\"account_id\":\"a1\",\"client_order_id\":\"ord-new\","
               "\"instrument_id\":\"SHFE.ag2406\",\"status\":1,"
               "\"filled_volume\":0}\n";
    }

    OrderStateMachine order_state_machine;
    InMemoryPortfolioLedger ledger;
    WalReplayLoader loader;
    const auto stats = loader.Replay(wal_path.string(), &order_state_machine, &ledger);

    EXPECT_EQ(stats.lines_total, 2);
    EXPECT_EQ(stats.events_loaded, 1);
    EXPECT_EQ(stats.ignored_lines, 1);
    EXPECT_EQ(stats.parse_errors, 0);

    const auto snapshot = order_state_machine.GetOrderSnapshot("ord-new");
    EXPECT_EQ(snapshot.status, OrderStatus::kAccepted);

    std::filesystem::remove(wal_path);
}

}  // namespace quant_hft
