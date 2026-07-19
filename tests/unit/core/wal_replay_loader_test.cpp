#include "quant_hft/core/wal_replay_loader.h"

#include <gtest/gtest.h>

#include <chrono>
#include <filesystem>
#include <fstream>
#include <string>

#include "quant_hft/core/local_wal_regulatory_sink.h"
#include "quant_hft/services/in_memory_portfolio_ledger.h"
#include "quant_hft/services/order_state_machine.h"

namespace quant_hft {

namespace {

std::filesystem::path NewTempWalPath(const std::string& tag) {
    const auto now = std::chrono::steady_clock::now().time_since_epoch().count();
    return std::filesystem::temp_directory_path() /
           ("quant_hft_" + tag + "_" + std::to_string(now) + ".wal");
}

OrderEvent BuildEvent(const std::string& client_order_id, OrderStatus status, int total_volume,
                      int filled_volume, double avg_fill_price, EpochNanos ts_ns) {
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

CtpOrderSubmitMapping BuildMapping() {
    CtpOrderSubmitMapping mapping;
    mapping.account_id = "a1";
    mapping.strategy_id = "strat-1";
    mapping.trace_id = "trace-map";
    mapping.client_order_id = "kama_candidate_hc-open-hc2610-abc";
    mapping.instrument_id = "SHFE.hc2610";
    mapping.exchange_id = "SHFE";
    mapping.side = Side::kBuy;
    mapping.offset = OffsetFlag::kOpen;
    mapping.volume = 1;
    mapping.price = 3200.0;
    mapping.order_ref = "1";
    mapping.front_id = 2;
    mapping.session_id = 3;
    mapping.request_id = 4;
    mapping.submit_ts_ns = 100;
    return mapping;
}

}  // namespace

TEST(WalReplayLoaderTest, RebuildsOrderStateAndLedgerFromWal) {
    const auto wal_path = NewTempWalPath("replay");

    {
        LocalWalRegulatorySink sink(wal_path.string());
        sink.AppendOrderEvent(BuildEvent("ord-1", OrderStatus::kAccepted, 2, 0, 0.0, 1));
        sink.AppendOrderEvent(BuildEvent("ord-1", OrderStatus::kPartiallyFilled, 2, 1, 4500.0, 2));
        sink.AppendOrderEvent(BuildEvent("ord-1", OrderStatus::kFilled, 2, 2, 4510.0, 3));
        sink.AppendTradeEvent(BuildEvent("ord-1", OrderStatus::kFilled, 2, 2, 4510.0, 3));
        sink.Flush();
    }

    OrderStateMachine order_state_machine;
    InMemoryPortfolioLedger ledger;
    WalReplayLoader loader;
    const auto stats = loader.Replay(wal_path.string(), &order_state_machine, &ledger);

    EXPECT_EQ(stats.lines_total, 4);
    EXPECT_EQ(stats.events_loaded, 3);
    EXPECT_EQ(stats.ignored_lines, 1);
    EXPECT_EQ(stats.parse_errors, 0);
    EXPECT_EQ(stats.state_rejected, 0);
    EXPECT_EQ(stats.ledger_applied, 3);

    const auto snapshot = order_state_machine.GetOrderSnapshot("ord-1");
    EXPECT_EQ(snapshot.status, OrderStatus::kFilled);
    EXPECT_EQ(snapshot.filled_volume, 2);
    EXPECT_TRUE(snapshot.is_terminal);

    const auto position = ledger.GetPositionSnapshot("a1", "SHFE.ag2406", PositionDirection::kLong);
    EXPECT_EQ(position.volume, 2);
    EXPECT_NEAR(position.avg_price, 4510.0, 1e-6);

    std::filesystem::remove(wal_path);
}

TEST(WalReplayLoaderTest, ReplaysLegacyTradeKindWithoutEventType) {
    const auto wal_path = NewTempWalPath("legacy_trade");
    {
        std::ofstream out(wal_path);
        out << "{\"seq\":1,\"kind\":\"trade\",\"ts_ns\":10,"
               "\"account_id\":\"a1\",\"client_order_id\":\"ord-legacy-trade\","
               "\"instrument_id\":\"SHFE.ag2406\",\"status\":3,"
               "\"total_volume\":1,\"filled_volume\":1,\"avg_fill_price\":4500.0}\n";
    }

    OrderStateMachine order_state_machine;
    InMemoryPortfolioLedger ledger;
    WalReplayLoader loader;
    const auto stats = loader.Replay(wal_path.string(), &order_state_machine, &ledger);

    EXPECT_EQ(stats.lines_total, 1);
    EXPECT_EQ(stats.events_loaded, 1);
    EXPECT_EQ(stats.ignored_lines, 0);
    EXPECT_EQ(stats.parse_errors, 0);

    const auto snapshot = order_state_machine.GetOrderSnapshot("ord-legacy-trade");
    EXPECT_EQ(snapshot.status, OrderStatus::kFilled);

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

TEST(WalReplayLoaderTest, ReplaysCtpSubmitMappingForOrphanOrderResolution) {
    const auto wal_path = NewTempWalPath("ctp_mapping");
    {
        LocalWalRegulatorySink sink(wal_path.string());
        ASSERT_TRUE(sink.AppendCtpOrderSubmitMapping(BuildMapping()));
        sink.Flush();
    }

    OrderStateMachine order_state_machine;
    InMemoryPortfolioLedger ledger;
    CtpOrderMappingStore mapping_store;
    WalReplayLoader loader;
    const auto stats =
        loader.Replay(wal_path.string(), &order_state_machine, &ledger, &mapping_store);

    EXPECT_EQ(stats.lines_total, 1);
    EXPECT_EQ(stats.events_loaded, 0);
    EXPECT_EQ(stats.ignored_lines, 1);
    EXPECT_EQ(stats.parse_errors, 0);
    EXPECT_EQ(stats.submit_mappings_loaded, 1);

    OrderEvent orphan;
    orphan.account_id = "a1";
    orphan.client_order_id = "1";
    orphan.order_ref = "1";
    orphan.front_id = 2;
    orphan.session_id = 3;
    orphan.status = OrderStatus::kFilled;
    orphan.filled_volume = 1;
    ASSERT_TRUE(mapping_store.EnrichOrderEvent(&orphan));
    EXPECT_EQ(orphan.client_order_id, "kama_candidate_hc-open-hc2610-abc");
    EXPECT_EQ(orphan.strategy_id, "strat-1");
    EXPECT_EQ(orphan.trace_id, "trace-map");
    EXPECT_EQ(orphan.instrument_id, "SHFE.hc2610");

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

TEST(CtpOrderMappingStoreTest, SameOrderRefAcrossTradingDaysDoesNotCrossAttribute) {
    CtpOrderMappingStore store;
    CtpOrderSubmitMapping first;
    first.account_id = "acc";
    first.client_order_id = "client-day-1";
    first.order_ref = "42";
    first.trading_day = "20260718";
    first.instrument_id = "SHFE.ag2608";
    first.exchange_id = "SHFE";
    first.side = Side::kBuy;
    first.offset = OffsetFlag::kOpen;
    store.Upsert(first);
    auto second = first;
    second.client_order_id = "client-day-2";
    second.trading_day = "20260719";
    store.Upsert(second);

    OrderEvent event;
    event.account_id = "acc";
    event.order_ref = "42";
    event.trading_day = "20260718";
    event.instrument_id = "SHFE.ag2608";
    event.exchange_id = "SHFE";
    event.side = Side::kBuy;
    event.offset = OffsetFlag::kOpen;
    ASSERT_TRUE(store.EnrichOrderEvent(&event));
    EXPECT_EQ(event.client_order_id, "client-day-1");

    event.client_order_id.clear();
    event.trading_day.clear();
    EXPECT_FALSE(store.EnrichOrderEvent(&event));
}

TEST(CtpOrderMappingStoreTest, ExchangeOrderIdentityResolvesTradeWithoutOrderRef) {
    CtpOrderMappingStore store;
    CtpOrderSubmitMapping mapping;
    mapping.account_id = "acc";
    mapping.client_order_id = "client-1";
    mapping.order_ref = "88";
    mapping.trading_day = "20260719";
    mapping.instrument_id = "DCE.i2609";
    mapping.exchange_id = "DCE";
    mapping.side = Side::kSell;
    mapping.offset = OffsetFlag::kOpen;
    store.Upsert(mapping);

    OrderEvent order;
    order.account_id = "acc";
    order.client_order_id = "client-1";
    order.exchange_order_id = "SYS000088";
    order.trading_day = "20260719";
    order.exchange_id = "DCE";
    ASSERT_TRUE(store.EnrichOrderEvent(&order));

    OrderEvent trade;
    trade.account_id = "acc";
    trade.exchange_order_id = "SYS000088";
    trade.trading_day = "20260719";
    trade.exchange_id = "DCE";
    ASSERT_TRUE(store.EnrichOrderEvent(&trade));
    EXPECT_EQ(trade.client_order_id, "client-1");
    EXPECT_EQ(trade.strategy_id, mapping.strategy_id);
    EXPECT_EQ(trade.instrument_id, "DCE.i2609");
}

}  // namespace quant_hft
