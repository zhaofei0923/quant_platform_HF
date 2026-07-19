#include "quant_hft/services/dominant_contract_coordinator.h"

#include <gtest/gtest.h>

#include <chrono>
#include <filesystem>
#include <fstream>
#include <sstream>
#include <string>

namespace quant_hft {
namespace {

constexpr EpochNanos kNow = 10'000'000'000LL;

MarketSnapshot Tick(const std::string& instrument_id, std::int64_t open_interest,
                    EpochNanos recv_ts_ns = kNow) {
    MarketSnapshot tick;
    tick.instrument_id = instrument_id;
    tick.exchange_id = "DCE";
    tick.trading_day = "20260719";
    tick.last_price = 2000.0;
    tick.bid_price_1 = 1999.0;
    tick.ask_price_1 = 2001.0;
    tick.open_interest = open_interest;
    tick.volume = open_interest / 2;
    tick.exchange_ts_ns = recv_ts_ns;
    tick.recv_ts_ns = recv_ts_ns;
    return tick;
}

DominantContractCoordinator MakeCoordinator() {
    DominantContractCoordinatorConfig config;
    config.min_lead_ratio = 0.15;
    config.min_lead_windows = 3;
    config.min_hold_ms = 0;
    config.max_tick_age_ms = 6'000;
    config.min_warmup_bars = 30;
    config.require_complete_baseline = true;
    return DominantContractCoordinator(config);
}

void Seed(DominantContractCoordinator* coordinator, bool flat = true) {
    ASSERT_NE(coordinator, nullptr);
    std::string error;
    ASSERT_TRUE(coordinator->RegisterProduct("c", "20260719", "c2607", {"c2607", "c2609"}, kNow - 1,
                                             &error))
        << error;
    coordinator->UpdateBaselineSnapshot("c", Tick("c2607", 1000));
    coordinator->UpdateBaselineSnapshot("c", Tick("c2609", 900));
    coordinator->UpdateLiveSnapshot(Tick("c2607", 1000));
    coordinator->UpdateLiveSnapshot(Tick("c2609", 900));
    DominantContractBrokerState broker;
    broker.truth_complete = true;
    if (!flat) {
        broker.position = 2;
        broker.held_instrument_ids.insert("c2607");
    }
    coordinator->UpdateBrokerState("c", broker);
    EXPECT_EQ(coordinator->Evaluate("c", kNow, true).reason, "current_remains_dominant");
    EXPECT_EQ(coordinator->GetStatus("c").phase, DominantContractPhase::kReady);
}

TEST(DominantContractCoordinatorTest, RequiresThreeLeadWindowsThenFreezesOpens) {
    auto coordinator = MakeCoordinator();
    Seed(&coordinator);
    coordinator.UpdateLiveSnapshot(Tick("c2609", 1200));

    EXPECT_EQ(coordinator.Evaluate("c", kNow + 1, true).action, DominantContractAction::kNone);
    EXPECT_EQ(coordinator.Evaluate("c", kNow + 2, true).action, DominantContractAction::kNone);
    const auto decision = coordinator.Evaluate("c", kNow + 3, true);
    EXPECT_EQ(decision.action, DominantContractAction::kBeginSwitch);
    EXPECT_EQ(decision.candidate_instrument_id, "c2609");
    EXPECT_EQ(coordinator.GetStatus("c").phase, DominantContractPhase::kPendingFlat);

    SignalIntent open;
    open.instrument_id = "c2607";
    open.product_id = "c";
    open.contract_generation = 1;
    open.signal_type = SignalType::kOpen;
    open.offset = OffsetFlag::kOpen;
    EXPECT_FALSE(coordinator.ValidateSignal(open, 0).allowed);
}

TEST(DominantContractCoordinatorTest, EnforcesLeadBoundaryAndFifteenMinuteHold) {
    DominantContractCoordinatorConfig config;
    config.min_lead_ratio = 0.15;
    config.min_lead_windows = 3;
    config.min_hold_ms = 900'000;
    config.max_tick_age_ms = 6'000;
    config.require_complete_baseline = true;
    DominantContractCoordinator coordinator(config);
    std::string error;
    ASSERT_TRUE(
        coordinator.RegisterProduct("c", "20260719", "c2607", {"c2607", "c2609"}, kNow, &error));
    coordinator.UpdateBaselineSnapshot("c", Tick("c2607", 1000));
    coordinator.UpdateBaselineSnapshot("c", Tick("c2609", 900));
    DominantContractBrokerState broker;
    broker.truth_complete = true;
    coordinator.UpdateBrokerState("c", broker);
    coordinator.UpdateLiveSnapshot(Tick("c2607", 1000));
    coordinator.UpdateLiveSnapshot(Tick("c2609", 1149));
    EXPECT_EQ(coordinator.Evaluate("c", kNow + 1, true).reason, "candidate_lead_below_threshold");

    coordinator.UpdateLiveSnapshot(Tick("c2609", 1150, kNow + 2));
    EXPECT_EQ(coordinator.Evaluate("c", kNow + 2, true).reason, "candidate_confirmation_pending");
    EXPECT_EQ(coordinator.Evaluate("c", kNow + 3, true).reason, "candidate_confirmation_pending");
    EXPECT_EQ(coordinator.Evaluate("c", kNow + 4, true).reason, "minimum_hold_not_elapsed");

    const EpochNanos after_hold = kNow + 900'000'000'000LL;
    coordinator.UpdateLiveSnapshot(Tick("c2607", 1000, after_hold));
    coordinator.UpdateLiveSnapshot(Tick("c2609", 1150, after_hold));
    EXPECT_EQ(coordinator.Evaluate("c", after_hold + 1, true).action,
              DominantContractAction::kBeginSwitch);
}

TEST(DominantContractCoordinatorTest, KeepsCloseAndCancelsOnlyOpenOrders) {
    auto coordinator = MakeCoordinator();
    Seed(&coordinator);
    coordinator.UpdateLiveSnapshot(Tick("c2609", 1200));
    DominantContractBrokerState broker;
    broker.truth_complete = true;
    broker.position = 2;
    broker.active_open_orders = 1;
    broker.active_close_orders = 1;
    broker.held_instrument_ids.insert("c2607");
    coordinator.UpdateBrokerState("c", broker);

    (void)coordinator.Evaluate("c", kNow + 1, true);
    (void)coordinator.Evaluate("c", kNow + 2, true);
    const auto decision = coordinator.Evaluate("c", kNow + 3, true);
    EXPECT_EQ(decision.action, DominantContractAction::kCancelOpenOrders);

    SignalIntent close;
    close.instrument_id = "c2607";
    close.product_id = "c";
    close.contract_generation = 1;
    close.signal_type = SignalType::kStopLoss;
    close.offset = OffsetFlag::kClose;
    EXPECT_TRUE(coordinator.ValidateSignal(close, 2).allowed);
}

TEST(DominantContractCoordinatorTest, GenerationBarrierAndWarmupAreIdempotent) {
    auto coordinator = MakeCoordinator();
    Seed(&coordinator);
    coordinator.UpdateLiveSnapshot(Tick("c2609", 1200));
    (void)coordinator.Evaluate("c", kNow + 1, true);
    (void)coordinator.Evaluate("c", kNow + 2, true);
    const auto decision = coordinator.Evaluate("c", kNow + 3, true);
    ASSERT_EQ(decision.action, DominantContractAction::kBeginSwitch);

    std::uint64_t generation = 0;
    std::string error;
    ASSERT_TRUE(coordinator.BeginSwitch("c", "c2607", "c2609", kNow + 4, &generation, &error))
        << error;
    EXPECT_EQ(generation, 2U);
    ASSERT_TRUE(coordinator.CommitSwitch("c", "c2609", 29, 30, kNow + 5, &error)) << error;
    EXPECT_EQ(coordinator.GetStatus("c").phase, DominantContractPhase::kWarming);
    EXPECT_TRUE(coordinator.RecordWarmupBar("c", "c2609", "c2609|5|20260719|10:00", kNow + 6));
    EXPECT_FALSE(coordinator.RecordWarmupBar("c", "c2609", "c2609|5|20260719|10:00", kNow + 7));
    EXPECT_EQ(coordinator.GetStatus("c").phase, DominantContractPhase::kReady);

    SignalIntent stale_open;
    stale_open.instrument_id = "c2609";
    stale_open.product_id = "c";
    stale_open.contract_generation = 1;
    stale_open.signal_type = SignalType::kOpen;
    stale_open.offset = OffsetFlag::kOpen;
    EXPECT_FALSE(coordinator.ValidateSignal(stale_open, 0).allowed);
    EXPECT_EQ(coordinator.GetStatus("c").generation_rejections, 1U);

    stale_open.contract_generation = generation;
    EXPECT_TRUE(coordinator.ValidateSignal(stale_open, 0).allowed);

    SignalIntent stale_close = stale_open;
    stale_close.instrument_id = "c2607";
    stale_close.contract_generation = 1;
    stale_close.signal_type = SignalType::kStopLoss;
    stale_close.offset = OffsetFlag::kClose;
    const auto close_validation = coordinator.ValidateSignal(stale_close, 1);
    EXPECT_FALSE(close_validation.allowed);
    EXPECT_TRUE(close_validation.persist_pending_exit);
}

TEST(DominantContractCoordinatorTest, RecoveredContractRequiresGenerationBarrierAndWarmup) {
    auto coordinator = MakeCoordinator();
    Seed(&coordinator, false);

    std::uint64_t generation = 0;
    std::string error;
    ASSERT_TRUE(coordinator.BeginRecoveryWarmup("c", "c2607", kNow + 1, &generation, &error))
        << error;
    EXPECT_EQ(generation, 2U);
    EXPECT_EQ(coordinator.GetStatus("c").phase, DominantContractPhase::kDraining);
    ASSERT_TRUE(coordinator.CommitRecoveryWarmup("c", "c2607", 29, 30, kNow + 2, &error)) << error;
    EXPECT_EQ(coordinator.GetStatus("c").phase, DominantContractPhase::kWarming);
    EXPECT_TRUE(coordinator.RecordWarmupBar("c", "c2607", "c2607|5|20260719|10:00", kNow + 3));
    EXPECT_EQ(coordinator.GetStatus("c").phase, DominantContractPhase::kReady);
}

TEST(DominantContractCoordinatorTest, MissingBaselineOrStaleTheoreticalBestCannotSwitch) {
    auto coordinator = MakeCoordinator();
    std::string error;
    ASSERT_TRUE(coordinator.RegisterProduct("c", "20260719", "", {"c2607", "c2609", "c2611"}, kNow,
                                            &error));
    DominantContractBrokerState broker;
    broker.truth_complete = true;
    coordinator.UpdateBrokerState("c", broker);
    coordinator.UpdateBaselineSnapshot("c", Tick("c2607", 1000));
    coordinator.UpdateBaselineSnapshot("c", Tick("c2609", 1100));
    coordinator.UpdateLiveSnapshot(Tick("c2607", 1000));
    coordinator.UpdateLiveSnapshot(Tick("c2609", 1100));
    EXPECT_EQ(coordinator.Evaluate("c", kNow + 1, true).reason, "candidate_baseline_incomplete");

    coordinator.UpdateBaselineSnapshot("c", Tick("c2611", 1500, kNow - 7'000'000'000LL));
    coordinator.UpdateLiveSnapshot(Tick("c2611", 1500, kNow - 7'000'000'000LL));
    EXPECT_EQ(coordinator.Evaluate("c", kNow + 1, true).reason, "best_candidate_market_not_fresh");
}

TEST(DominantContractCoordinatorTest, QueryBaselineDoesNotSubstituteForFreshLiveTick) {
    auto coordinator = MakeCoordinator();
    std::string error;
    ASSERT_TRUE(coordinator.RegisterProduct("c", "20260719", "", {"c2607", "c2609"}, kNow, &error));
    DominantContractBrokerState broker;
    broker.truth_complete = true;
    coordinator.UpdateBrokerState("c", broker);
    coordinator.UpdateBaselineSnapshot("c", Tick("c2607", 1000));
    coordinator.UpdateBaselineSnapshot("c", Tick("c2609", 1200));

    EXPECT_EQ(coordinator.Evaluate("c", kNow + 1, true).reason, "best_candidate_market_not_fresh");
}

TEST(DominantContractCoordinatorTest, TradingDayRefreshInvalidatesGenerationAndBaseline) {
    auto coordinator = MakeCoordinator();
    Seed(&coordinator);

    std::string error;
    ASSERT_TRUE(
        coordinator.RefreshTradingDay("c", "20260720", {"c2609", "c2611"}, kNow + 1, &error))
        << error;
    const auto status = coordinator.GetStatus("c");
    EXPECT_EQ(status.trading_day, "20260720");
    EXPECT_EQ(status.phase, DominantContractPhase::kPendingFlat);
    EXPECT_EQ(status.generation, 2U);
    EXPECT_EQ(status.baseline_count, 0U);
    EXPECT_EQ(status.eligible_count, 2U);
    EXPECT_TRUE(coordinator.IsCandidateInstrument("c2609"));
    EXPECT_TRUE(coordinator.IsCandidateInstrument("c2611"));
    EXPECT_TRUE(coordinator.ProductForInstrument("c2607").has_value());

    SignalIntent queued_open;
    queued_open.instrument_id = "c2607";
    queued_open.product_id = "c";
    queued_open.contract_generation = 1;
    queued_open.signal_type = SignalType::kOpen;
    queued_open.offset = OffsetFlag::kOpen;
    const auto validation = coordinator.ValidateSignal(queued_open, 0);
    EXPECT_FALSE(validation.allowed);
    EXPECT_EQ(validation.reason, "stale_contract_generation");
    EXPECT_EQ(coordinator.Evaluate("c", kNow + 2, true).reason, "candidate_baseline_incomplete");
}

TEST(DominantContractCoordinatorTest, MultipleHeldContractsBlockProductOpens) {
    auto coordinator = MakeCoordinator();
    Seed(&coordinator);
    DominantContractBrokerState broker;
    broker.truth_complete = true;
    broker.position = 3;
    broker.held_instrument_ids = {"c2607", "c2609"};
    coordinator.UpdateBrokerState("c", broker);

    SignalIntent open;
    open.instrument_id = "c2607";
    open.product_id = "c";
    open.contract_generation = 1;
    open.signal_type = SignalType::kOpen;
    open.offset = OffsetFlag::kOpen;
    const auto validation = coordinator.ValidateSignal(open, 0);
    EXPECT_FALSE(validation.allowed);
    EXPECT_EQ(validation.reason, "broker_identity_unresolved");
    EXPECT_EQ(coordinator.Evaluate("c", kNow + 1, true).reason, "broker_identity_unresolved");

    broker.position = 1;
    broker.held_instrument_ids = {"c2609"};
    coordinator.UpdateBrokerState("c", broker);
    EXPECT_EQ(coordinator.ValidateSignal(open, 0).reason, "broker_identity_unresolved");
}

TEST(DominantContractCoordinatorTest, PersistsStructuredStatusAtomically) {
    auto coordinator = MakeCoordinator();
    Seed(&coordinator);
    const auto token = std::chrono::steady_clock::now().time_since_epoch().count();
    const auto path = std::filesystem::temp_directory_path() /
                      ("dominant_contract_status_" + std::to_string(token) + ".json");
    std::string error;
    ASSERT_TRUE(coordinator.PersistStatusAtomically("c", path.string(), &error)) << error;
    std::ifstream input(path);
    std::ostringstream payload;
    payload << input.rdbuf();
    EXPECT_NE(payload.str().find("\"schema_version\": 2"), std::string::npos);
    EXPECT_NE(payload.str().find("\"current_instrument_id\": \"c2607\""), std::string::npos);
    EXPECT_NE(payload.str().find("\"phase_started_ts_ns\""), std::string::npos);
    std::filesystem::remove(path);
}

}  // namespace
}  // namespace quant_hft
