#include "quant_hft/services/pending_exit_store.h"

#include <gtest/gtest.h>

#include <chrono>
#include <filesystem>
#include <fstream>
#include <thread>
#include <vector>

namespace quant_hft {
namespace {

std::filesystem::path MakeWalPath(const std::string& suffix) {
    const auto token = std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
    return std::filesystem::temp_directory_path() /
           ("quant_hft_pending_exit_" + suffix + "_" + token) / "pending_exit_v2.jsonl";
}

PendingExit MakePending(SignalType signal_type = SignalType::kStopLoss) {
    PendingExit pending;
    pending.account_id = "acc-1";
    pending.strategy_id = "kama";
    pending.instrument_id = "SHFE.hc2610";
    pending.position_side = PositionDirection::kLong;
    pending.signal_type = signal_type;
    pending.trace_id = "trace-1";
    pending.trigger_ts_ns = 1780000000000000001LL;
    return pending;
}

}  // namespace

TEST(PendingExitStoreTest, RequiresRecoveryBeforeMutation) {
    const auto path = MakeWalPath("require_recover");
    PendingExitStore store(path.string());
    std::string error;
    EXPECT_EQ(store.Upsert(MakePending(), &error), PendingExitUpsertResult::kFailed);
    EXPECT_FALSE(error.empty());
    std::filesystem::remove_all(path.parent_path());
}

TEST(PendingExitStoreTest, NeverPersistsOpenSignalsForLaterChasing) {
    const auto path = MakeWalPath("open_rejected");
    PendingExitStore store(path.string());
    ASSERT_TRUE(store.Recover());
    std::string error;
    EXPECT_EQ(store.Upsert(MakePending(SignalType::kOpen), &error),
              PendingExitUpsertResult::kFailed);
    EXPECT_NE(error.find("close-like"), std::string::npos);
    EXPECT_EQ(store.Size(), 0U);
    EXPECT_FALSE(std::filesystem::exists(path));
    std::filesystem::remove_all(path.parent_path());
}

TEST(PendingExitStoreTest, PersistsAndRecoversHighestPriorityForIdempotencyKey) {
    const auto path = MakeWalPath("priority");
    {
        PendingExitStore store(path.string());
        ASSERT_TRUE(store.Recover());
        EXPECT_EQ(store.Upsert(MakePending(SignalType::kClose)),
                  PendingExitUpsertResult::kInserted);
        EXPECT_EQ(store.Upsert(MakePending(SignalType::kTakeProfit)),
                  PendingExitUpsertResult::kPriorityRaised);
        EXPECT_EQ(store.Upsert(MakePending(SignalType::kClose)),
                  PendingExitUpsertResult::kAlreadyPending);
        auto forced = MakePending(SignalType::kForceClose);
        forced.trace_id = "force-trace";
        EXPECT_EQ(store.Upsert(forced), PendingExitUpsertResult::kPriorityRaised);
        EXPECT_EQ(store.Size(), 1U);
    }

    PendingExitStore recovered(path.string());
    std::string error;
    ASSERT_TRUE(recovered.Recover(&error)) << error;
    const auto value = recovered.Get(PendingExitStore::MakeKey(MakePending()));
    ASSERT_TRUE(value.has_value());
    EXPECT_EQ(value->signal_type, SignalType::kForceClose);
    EXPECT_EQ(value->trace_id, "force-trace");
    EXPECT_EQ(value->trigger_ts_ns, 1780000000000000001LL);
    std::filesystem::remove_all(path.parent_path());
}

TEST(PendingExitStoreTest, RemoveIsDurableAndIdempotent) {
    const auto path = MakeWalPath("remove");
    const auto pending = MakePending();
    {
        PendingExitStore store(path.string());
        ASSERT_TRUE(store.Recover());
        ASSERT_EQ(store.Upsert(pending), PendingExitUpsertResult::kInserted);
        ASSERT_TRUE(store.RemoveAfterBrokerFlat(PendingExitStore::MakeKey(pending), 0,
                                                1780000000000000100LL));
        EXPECT_TRUE(store.RemoveAfterBrokerFlat(PendingExitStore::MakeKey(pending), 0,
                                                1780000000000000200LL));
    }
    PendingExitStore recovered(path.string());
    ASSERT_TRUE(recovered.Recover());
    EXPECT_EQ(recovered.Size(), 0U);
    std::filesystem::remove_all(path.parent_path());
}

TEST(PendingExitStoreTest, RefusesRemovalUntilBrokerAuthoritativelyReportsFlat) {
    const auto path = MakeWalPath("not_flat");
    const auto pending = MakePending();
    {
        PendingExitStore store(path.string());
        ASSERT_TRUE(store.Recover());
        ASSERT_EQ(store.Upsert(pending), PendingExitUpsertResult::kInserted);
        std::string error;
        EXPECT_FALSE(store.RemoveAfterBrokerFlat(PendingExitStore::MakeKey(pending), 1,
                                                 1780000000000000100LL, &error));
        EXPECT_NE(error.find("broker position is flat"), std::string::npos);
        EXPECT_EQ(store.Size(), 1U);
    }
    PendingExitStore recovered(path.string());
    ASSERT_TRUE(recovered.Recover());
    EXPECT_EQ(recovered.Size(), 1U);
    std::filesystem::remove_all(path.parent_path());
}

TEST(PendingExitStoreTest, PositionSideIsPartOfStableKey) {
    const auto path = MakeWalPath("side");
    PendingExitStore store(path.string());
    ASSERT_TRUE(store.Recover());
    auto long_exit = MakePending();
    auto short_exit = MakePending();
    short_exit.position_side = PositionDirection::kShort;
    ASSERT_EQ(store.Upsert(long_exit), PendingExitUpsertResult::kInserted);
    ASSERT_EQ(store.Upsert(short_exit), PendingExitUpsertResult::kInserted);
    EXPECT_EQ(store.Size(), 2U);
    std::filesystem::remove_all(path.parent_path());
}

TEST(PendingExitStoreTest, EscapedIdentityAndTraceRoundTrip) {
    const auto path = MakeWalPath("escaping");
    auto pending = MakePending();
    pending.strategy_id = "kama/\"night\"";
    pending.trace_id = "line1\nline2\\tail";
    {
        PendingExitStore store(path.string());
        ASSERT_TRUE(store.Recover());
        ASSERT_EQ(store.Upsert(pending), PendingExitUpsertResult::kInserted);
    }
    PendingExitStore recovered(path.string());
    ASSERT_TRUE(recovered.Recover());
    const auto value = recovered.Get(PendingExitStore::MakeKey(pending));
    ASSERT_TRUE(value.has_value());
    EXPECT_EQ(value->trace_id, pending.trace_id);
    std::filesystem::remove_all(path.parent_path());
}

TEST(PendingExitStoreTest, IgnoresOnlyTornFinalRecordDuringRecovery) {
    const auto path = MakeWalPath("torn_tail");
    {
        PendingExitStore store(path.string());
        ASSERT_TRUE(store.Recover());
        ASSERT_EQ(store.Upsert(MakePending()), PendingExitUpsertResult::kInserted);
    }
    {
        std::ofstream output(path, std::ios::app);
        output << "{\"schema_version\":2";
    }
    PendingExitStore recovered(path.string());
    ASSERT_TRUE(recovered.Recover());
    EXPECT_EQ(recovered.Size(), 1U);
    auto next = MakePending();
    next.strategy_id = "other-instance";
    ASSERT_EQ(recovered.Upsert(next), PendingExitUpsertResult::kInserted);
    PendingExitStore second_restart(path.string());
    ASSERT_TRUE(second_restart.Recover());
    EXPECT_EQ(second_restart.Size(), 2U);
    std::filesystem::remove_all(path.parent_path());
}

TEST(PendingExitStoreTest, RejectsCorruptCompleteRecord) {
    const auto path = MakeWalPath("corrupt");
    std::filesystem::create_directories(path.parent_path());
    {
        std::ofstream output(path);
        output << "{not-json}\n";
    }
    PendingExitStore store(path.string());
    std::string error;
    EXPECT_FALSE(store.Recover(&error));
    EXPECT_NE(error.find("line 1"), std::string::npos);
    EXPECT_FALSE(store.IsRecovered());
    std::filesystem::remove_all(path.parent_path());
}

TEST(PendingExitStoreTest, RecoversComponentAndSeparatesHedgeFromSpeculation) {
    const auto path = MakeWalPath("component_hedge");
    auto spec = MakePending();
    spec.component_id = "trend/\"alpha\"";
    auto hedge = spec;
    hedge.hedge_flag = HedgeFlag::kHedge;
    hedge.component_id = "trend/beta";
    {
        PendingExitStore store(path.string());
        ASSERT_TRUE(store.Recover());
        ASSERT_EQ(store.Upsert(spec), PendingExitUpsertResult::kInserted);
        ASSERT_EQ(store.Upsert(hedge), PendingExitUpsertResult::kInserted);
    }
    PendingExitStore restarted(path.string());
    ASSERT_TRUE(restarted.Recover());
    ASSERT_EQ(restarted.Size(), 2U);
    EXPECT_EQ(restarted.Get(PendingExitStore::MakeKey(spec))->component_id, spec.component_id);
    EXPECT_EQ(restarted.Get(PendingExitStore::MakeKey(hedge))->component_id, hedge.component_id);
    EXPECT_TRUE(restarted.RemoveAfterStrategyFlat(PendingExitStore::MakeKey(spec), 0, 0, true, 1));
    EXPECT_TRUE(restarted.Get(PendingExitStore::MakeKey(hedge)).has_value());
    std::filesystem::remove_all(path.parent_path());
}

TEST(PendingExitStoreTest, LegacyV2PendingExitUsesSpeculationAndKeepsIdentity) {
    const auto path = MakeWalPath("legacy_v2");
    std::filesystem::create_directories(path.parent_path());
    {
        std::ofstream output(path);
        output
            << R"({"schema_version":2,"namespace":"__pending_exit_v2","op":"upsert","account_id":"acc-1","strategy_id":"kama","instrument_id":"SHFE.hc2610","position_side":"long","signal_type":"stop_loss","trace_id":"legacy","trigger_ts_ns":"1"})"
            << '\n';
    }
    PendingExitStore store(path.string());
    ASSERT_TRUE(store.Recover());
    const auto value = store.Get(PendingExitStore::MakeKey(MakePending()));
    ASSERT_TRUE(value);
    EXPECT_EQ(value->trace_id, "legacy");
    EXPECT_EQ(value->hedge_flag, HedgeFlag::kSpeculation);
    EXPECT_TRUE(value->component_id.empty());
    ASSERT_TRUE(store.RemoveAfterStrategyFlat(PendingExitStore::MakeKey(*value), 0, 0, true, 2));
    PendingExitStore restarted(path.string());
    ASSERT_TRUE(restarted.Recover());
    EXPECT_EQ(restarted.Size(), 0U);
    std::filesystem::remove_all(path.parent_path());
}

TEST(PendingExitStoreTest, ConcurrentDuplicateCandidatesRemainSingleIntent) {
    const auto path = MakeWalPath("concurrent");
    PendingExitStore store(path.string());
    ASSERT_TRUE(store.Recover());
    std::vector<std::thread> writers;
    for (int i = 0; i < 8; ++i) {
        writers.emplace_back([&store, i]() {
            auto pending = MakePending(i == 7 ? SignalType::kForceClose : SignalType::kStopLoss);
            pending.trace_id = "trace-" + std::to_string(i);
            store.Upsert(pending);
        });
    }
    for (auto& writer : writers) {
        writer.join();
    }
    ASSERT_EQ(store.Size(), 1U);
    ASSERT_EQ(store.List().size(), 1U);
    EXPECT_EQ(store.List()[0].signal_type, SignalType::kForceClose);
    std::filesystem::remove_all(path.parent_path());
}

}  // namespace quant_hft

namespace quant_hft {
TEST(PendingExitStoreTest, InstanceExitCompletesWithoutFlatteningOtherInstances) {
    const auto path = MakeWalPath("owned_flat");
    PendingExitStore store(path.string());
    ASSERT_TRUE(store.Recover());
    auto a = MakePending();
    a.strategy_id = "A";
    auto b = MakePending();
    b.strategy_id = "B";
    ASSERT_EQ(store.Upsert(a), PendingExitUpsertResult::kInserted);
    ASSERT_EQ(store.Upsert(b), PendingExitUpsertResult::kInserted);
    auto key = PendingExitStore::MakeKey(a);
    EXPECT_FALSE(store.RemoveAfterStrategyFlat(key, 0, 1, true, 1));
    EXPECT_FALSE(store.RemoveAfterStrategyFlat(key, 0, 0, false, 1));
    ASSERT_TRUE(store.RemoveAfterStrategyFlat(key, 0, 0, true, 1));
    EXPECT_FALSE(store.Get(key));
    EXPECT_TRUE(store.Get(PendingExitStore::MakeKey(b)));
    PendingExitStore restarted(path.string());
    ASSERT_TRUE(restarted.Recover());
    EXPECT_EQ(restarted.Size(), 1U);
    std::filesystem::remove_all(path.parent_path());
}
}  // namespace quant_hft
