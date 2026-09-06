#include "quant_hft/core/wal_replay_loader.h"

#include <gtest/gtest.h>
#include <sys/wait.h>
#include <unistd.h>

#include <chrono>
#include <filesystem>
#include <fstream>
#include <limits>
#include <string>

#include "quant_hft/core/local_wal_regulatory_sink.h"
#include "quant_hft/core/wal_format.h"
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

TEST(WalReplayLoaderTest, DurableReceiptSurvivesAbruptProcessExitAndKeepsStreamOnReopen) {
    const auto path = NewTempWalPath("crash_receipt");
    const auto child = ::fork();
    ASSERT_GE(child, 0);
    if (child == 0) {
        LocalWalRegulatorySink sink(path.string());
        const auto receipt =
            sink.CommitOrderEvent(BuildEvent("first", OrderStatus::kFilled, 1, 1, 10, 1));
        ::_exit(receipt.durable && !receipt.stream_id.empty() ? 0 : 1);
    }
    int status = 0;
    ASSERT_EQ(::waitpid(child, &status, 0), child);
    ASSERT_TRUE(WIFEXITED(status));
    ASSERT_EQ(WEXITSTATUS(status), 0);
    const auto initial = ValidateWalFile(path.string());
    ASSERT_TRUE(initial.valid) << initial.error;
    EXPECT_EQ(initial.records, 1U);
    EXPECT_GT(std::filesystem::file_size(path), 0U);
    {
        LocalWalRegulatorySink sink(path.string());
        const auto next =
            sink.CommitOrderEvent(BuildEvent("second", OrderStatus::kFilled, 1, 1, 12, 2));
        ASSERT_TRUE(next.durable) << next.error;
        EXPECT_EQ(next.sequence, initial.next_sequence);
        EXPECT_EQ(next.stream_id, initial.stream_id);
        EXPECT_EQ(next.first_sequence, initial.first_sequence);
    }
    EXPECT_TRUE(ValidateWalFile(path.string()).valid);
    std::filesystem::remove(path);
}

TEST(WalReplayLoaderTest, PreservesPricePrecisionAndRejectsNonFiniteRecordsBeforeWrite) {
    const auto path = NewTempWalPath("precision");
    constexpr double kPrice = 123456.78901234567;
    {
        LocalWalRegulatorySink sink(path.string());
        ASSERT_TRUE(
            sink.CommitOrderEvent(BuildEvent("precise", OrderStatus::kFilled, 1, 1, kPrice, 1)));
        const auto size = std::filesystem::file_size(path);
        const auto invalid = sink.CommitOrderEvent(BuildEvent(
            "invalid", OrderStatus::kFilled, 1, 1, std::numeric_limits<double>::infinity(), 2));
        EXPECT_FALSE(invalid.durable);
        EXPECT_EQ(std::filesystem::file_size(path), size);
    }
    std::ifstream input(path);
    std::string line;
    ASSERT_TRUE(static_cast<bool>(std::getline(input, line)));
    const std::string key = "\"avg_fill_price\":";
    const auto pos = line.find(key);
    ASSERT_NE(pos, std::string::npos);
    EXPECT_EQ(std::stod(line.substr(pos + key.size())), kPrice);
    EXPECT_TRUE(ValidateWalFile(path.string()).valid);
    std::filesystem::remove(path);
}

TEST(WalReplayLoaderTest, ValidatedVisitorIncludesMappingsTradesAndTheirExactReceipts) {
    const auto path = NewTempWalPath("visit_receipts");
    std::vector<WalReceipt> receipts;
    {
        LocalWalRegulatorySink sink(path.string());
        receipts.push_back(sink.CommitCtpOrderSubmitMapping(BuildMapping()));
        auto event = BuildEvent("fill", OrderStatus::kFilled, 1, 1, 123.456789, 1);
        event.broker_id = "9999";
        event.reason = std::string("control:") + '\x01';
        receipts.push_back(sink.CommitOrderEvent(event));
        receipts.push_back(sink.CommitTradeEvent(event));
    }
    std::vector<WalReplayRecord> records;
    const auto result = WalReplayLoader().VisitValidated(path.string(), [&](const auto& record) {
        records.push_back(record);
        return true;
    });
    ASSERT_TRUE(result.completed) << result.error;
    ASSERT_EQ(records.size(), 3U);
    EXPECT_TRUE(records[0].mapping);
    EXPECT_EQ(records[2].event_type, "trade_fill");
    ASSERT_TRUE(records[1].event);
    EXPECT_EQ(records[1].event->broker_id, "9999");
    EXPECT_EQ(records[1].event->reason.back(), '\x01');
    for (std::size_t i = 0; i < records.size(); ++i) {
        EXPECT_TRUE(records[i].receipt.durable);
        EXPECT_EQ(records[i].receipt.sequence, receipts[i].sequence);
        EXPECT_EQ(records[i].receipt.checksum, receipts[i].checksum);
        EXPECT_EQ(records[i].receipt.stream_id, receipts[i].stream_id);
    }
    int visits = 0;
    const auto bounded = WalReplayLoader().VisitValidated(
        path.string(),
        [&](const auto&) {
            ++visits;
            return true;
        },
        10);
    EXPECT_FALSE(bounded.completed);
    EXPECT_EQ(visits, 0);
    {
        std::ofstream out(path, std::ios::app);
        out << "{\"seq\":3";
    }
    const auto damaged = WalReplayLoader().VisitValidated(path.string(), [&](const auto&) {
        ++visits;
        return true;
    });
    EXPECT_FALSE(damaged.completed);
    EXPECT_EQ(visits, 0);
    std::filesystem::remove(path);
}

TEST(WalReplayLoaderTest, RejectsChecksumCorruptionBeforeApplyingAnyProjection) {
    const auto path = NewTempWalPath("checksum");
    {
        LocalWalRegulatorySink sink(path.string());
        ASSERT_TRUE(sink.AppendOrderEvent(BuildEvent("good", OrderStatus::kFilled, 1, 1, 10, 1)));
        ASSERT_TRUE(sink.AppendOrderEvent(BuildEvent("bad", OrderStatus::kFilled, 1, 1, 11, 2)));
    }
    std::ifstream input(path);
    std::string bytes((std::istreambuf_iterator<char>(input)), std::istreambuf_iterator<char>());
    input.close();
    const auto pos = bytes.find("\"avg_fill_price\":11");
    ASSERT_NE(pos, std::string::npos);
    bytes.replace(pos, std::string("\"avg_fill_price\":11").size(), "\"avg_fill_price\":99");
    {
        std::ofstream out(path, std::ios::trunc);
        out << bytes;
    }
    OrderStateMachine orders;
    InMemoryPortfolioLedger ledger;
    const auto stats = WalReplayLoader().Replay(path.string(), &orders, &ledger);
    EXPECT_FALSE(stats.integrity_ok);
    EXPECT_EQ(stats.ledger_applied, 0U);
    EXPECT_EQ(stats.events_loaded, 0U);
    {
        LocalWalRegulatorySink sink(path.string());
        EXPECT_FALSE(sink.CommitOrderEvent(BuildEvent("no", OrderStatus::kFilled, 1, 1, 1, 3)));
    }
    std::filesystem::remove(path);
}

TEST(WalReplayLoaderTest, IncompleteTailRequiresExplicitRepairAndIsNeverSilentlyAppended) {
    const auto path = NewTempWalPath("tail");
    {
        LocalWalRegulatorySink sink(path.string());
        ASSERT_TRUE(sink.AppendOrderEvent(BuildEvent("good", OrderStatus::kFilled, 1, 1, 10, 1)));
    }
    {
        std::ofstream out(path, std::ios::app);
        out << "{\"seq\":1";
    }
    const auto size = std::filesystem::file_size(path);
    const auto validated = ValidateWalFile(path.string());
    EXPECT_FALSE(validated.valid);
    EXPECT_TRUE(validated.incomplete_tail);
    {
        LocalWalRegulatorySink sink(path.string());
        EXPECT_FALSE(
            sink.AppendOrderEvent(BuildEvent("must-not-append", OrderStatus::kFilled, 1, 1, 1, 2)));
    }
    EXPECT_EQ(std::filesystem::file_size(path), size);
    std::filesystem::remove(path);
}

TEST(WalReplayLoaderTest, RejectsConcurrentWriterAndLegacySequenceGap) {
    const auto path = NewTempWalPath("writer_lock");
    {
        LocalWalRegulatorySink first(path.string());
        ASSERT_TRUE(
            first.AppendOrderEvent(BuildEvent("first", OrderStatus::kAccepted, 1, 0, 0, 1)));
        LocalWalRegulatorySink second(path.string());
        EXPECT_FALSE(
            second.AppendOrderEvent(BuildEvent("second", OrderStatus::kAccepted, 1, 0, 0, 2)));
        EXPECT_NE(second.LastError().find("writer"), std::string::npos);
    }
    {
        std::ofstream out(path, std::ios::trunc);
        out << "{\"seq\":1,\"kind\":\"rollover\"}\n{\"seq\":3,\"kind\":\"rollover\"}\n";
    }
    EXPECT_FALSE(ValidateWalFile(path.string()).valid);
    std::filesystem::remove(path);
}

TEST(WalReplayLoaderTest, ContinuesLegacyStreamWithVersionedReceiptAndStableFirstSequence) {
    const auto path = NewTempWalPath("migration");
    {
        std::ofstream out(path);
        out << "{\"seq\":7,\"kind\":\"rollover\"}\n";
    }
    WalReceipt receipt;
    {
        LocalWalRegulatorySink sink(path.string());
        receipt = sink.CommitOrderEvent(BuildEvent("new", OrderStatus::kAccepted, 1, 0, 0, 1));
        ASSERT_TRUE(receipt) << receipt.error;
        EXPECT_EQ(receipt.sequence, 8U);
        EXPECT_EQ(receipt.first_sequence, 8U);
    }
    const auto result = ValidateWalFile(path.string());
    ASSERT_TRUE(result.valid) << result.error;
    EXPECT_EQ(result.legacy_records, 1U);
    EXPECT_EQ(result.stream_id, receipt.stream_id);
    {
        LocalWalRegulatorySink sink(path.string());
        const auto next =
            sink.CommitOrderEvent(BuildEvent("next", OrderStatus::kAccepted, 1, 0, 0, 2));
        EXPECT_TRUE(next);
        EXPECT_EQ(next.stream_id, receipt.stream_id);
        EXPECT_EQ(next.first_sequence, 8U);
        EXPECT_EQ(next.sequence, 9U);
    }
    std::filesystem::remove(path);
}

}  // namespace quant_hft
