#include <gtest/gtest.h>

#include <chrono>
#include <cstdio>
#include <vector>

#include "../backtest/tick_partition_fixture.h"
#include "quant_hft/apps/backtest_replay_support.h"
#include "quant_hft/core/ctp_config_loader.h"
#include "quant_hft/core/runtime_semantics_loader.h"
#include "quant_hft/risk/risk_manager.h"
#include "quant_hft/services/order_manager.h"
#include "quant_hft/strategy/atomic_factory.h"

namespace quant_hft::apps {
namespace {

BarSnapshot MinuteBar(int minute) {
    BarSnapshot bar;
    bar.instrument_id = "DCE.c2605";
    bar.exchange_id = "DCE";
    bar.trading_day = "20260515";
    bar.action_day = bar.trading_day;
    char key[32];
    std::snprintf(key, sizeof(key), "20260515 09:%02d", minute);
    bar.minute = key;
    bar.open = bar.analysis_open = 100 + minute;
    bar.high = bar.analysis_high = 102 + minute;
    bar.low = bar.analysis_low = 99 + minute;
    bar.close = bar.analysis_close = 101 + minute;
    bar.volume = 10;
    bar.ts_ns = detail::ReplayMinuteStartEpochNs(bar.minute) + 59'000'000'000LL;
    return bar;
}

detail::ReplayBarTickContext Timing(const BarSnapshot& bar) {
    detail::ReplayBarTickContext context;
    context.initialized = true;
    context.first_tick.instrument_id = context.last_tick.instrument_id = bar.instrument_id;
    context.first_tick.ts_ns = context.last_tick.ts_ns = bar.ts_ns;
    context.first_tick.last_price = context.last_tick.last_price = bar.close;
    return context;
}

TEST(ReplayMarketParityTest, AdditionalTimeframeCannotContaminateFiveMinuteDetector) {
    detail::ReplayTimeframeFanout replay({1, 5});
    TimeframeStateFanout single({5});
    std::size_t compared = 0;
    for (int minute = 0; minute < 30; ++minute) {
        const auto bar = MinuteBar(minute);
        const auto actual = replay.OnOneMinuteBar(bar, Timing(bar));
        const auto expected = single.OnOneMinuteBar(bar);
        for (const auto& a : actual) {
            if (a.timeframe_minutes != 5) {
                continue;
            }
            ASSERT_EQ(expected.size(), 1U);
            const auto& e = expected.front();
            EXPECT_EQ(a.state.market_state_bars_seen, e.state.market_state_bars_seen);
            EXPECT_EQ(a.state.market_regime, e.state.market_regime);
            EXPECT_EQ(a.kama, e.kama);
            EXPECT_EQ(a.atr, e.atr);
            EXPECT_EQ(a.adx, e.adx);
            EXPECT_EQ(a.bar.minute, e.bar.minute);
            EXPECT_EQ(a.strategy_eligible, e.strategy_eligible);
            ++compared;
        }
    }
    EXPECT_EQ(compared, 6U);
}

TEST(ReplayMarketParityTest, MissingMinuteCannotAdvanceStrategyOrDetector) {
    detail::ReplayTimeframeFanout replay({5});
    std::vector<detail::ReplayAggregatedBar> emitted;
    for (int minute : {0, 1, 3, 4}) {
        const auto bar = MinuteBar(minute);
        const auto batch = replay.OnOneMinuteBar(bar, Timing(bar));
        emitted.insert(emitted.end(), batch.begin(), batch.end());
    }
    ASSERT_EQ(emitted.size(), 1U);
    EXPECT_EQ(emitted.front().bar.observed_source_bars, 4);
    EXPECT_FALSE(emitted.front().bar.is_complete);
    EXPECT_FALSE(emitted.front().strategy_eligible);
    EXPECT_FALSE(emitted.front().state.has_bar);
    EXPECT_EQ(emitted.front().state.market_state_bars_seen, 0U);
}

TEST(ReplayMarketParityTest, ShutdownPartialBucketIsAuditOnly) {
    detail::ReplayTimeframeFanout replay({5});
    const auto bar = MinuteBar(0);
    EXPECT_TRUE(replay.OnOneMinuteBar(bar, Timing(bar)).empty());
    const auto emitted = replay.Flush();
    ASSERT_EQ(emitted.size(), 1U);
    EXPECT_FALSE(emitted.front().strategy_eligible);
    EXPECT_FALSE(emitted.front().state.has_bar);
    EXPECT_EQ(emitted.front().state.market_state_bars_seen, 0U);
}

}  // namespace
}  // namespace quant_hft::apps

namespace quant_hft::backtest {
TEST(ReplayMarketParityTest, ParameterProfileDoesNotChangeBacktestBehaviorMode) {
    struct Capture {
        AtomicParams params;
        std::string run_type;
        RunMode run_mode{RunMode::kLive};
        int state_calls{0};
    };
    class Probe : public ISubStrategy {
       public:
        explicit Probe(std::shared_ptr<Capture> capture) : capture_(std::move(capture)) {}
        void Init(const AtomicParams& params) override { capture_->params = params; }
        std::string GetId() const override { return "parameter_probe"; }
        void Reset() override {}
        std::vector<SignalIntent> OnState(const StateSnapshot7D&,
                                          const AtomicStrategyContext& ctx) override {
            capture_->run_type = ctx.run_type;
            capture_->run_mode = ctx.run_mode;
            ++capture_->state_calls;
            return {};
        }

       private:
        std::shared_ptr<Capture> capture_;
    };
    auto capture = std::make_shared<Capture>();
    AtomicFactory factory;
    std::string error;
    ASSERT_TRUE(factory.Register(
        "ParameterProbe", [capture] { return std::make_unique<Probe>(capture); }, &error))
        << error;
    CompositeStrategyDefinition definition;
    definition.run_type = "backtest";
    definition.market_state_mode = false;
    SubStrategyDefinition sub;
    sub.id = "parameter_probe";
    sub.type = "ParameterProbe";
    sub.params["marker"] = "base";
    sub.overrides.backtest_params["marker"] = "backtest";
    sub.overrides.sim_params["marker"] = "sim";
    definition.sub_strategies.push_back(sub);
    CompositeStrategy composite(definition, &factory);
    StrategyContext context;
    context.strategy_id = "profile_test";
    context.metadata["run_type"] = "backtest";
    context.metadata["parameter_profile"] = "sim";
    context.metadata["timestamp_basis"] = "utc";
    composite.Initialize(context);
    EXPECT_EQ(capture->params.at("marker"), "sim");
    StateSnapshot7D state;
    state.instrument_id = "DCE.c2605";
    state.has_bar = true;
    state.timeframe_minutes = 1;
    state.ts_ns =
        detail::ReplayMinuteStartEpochNs("20260515 09:02") - 8LL * 60 * 60 * 1'000'000'000LL;
    (void)composite.OnState(state);
    EXPECT_EQ(capture->state_calls, 1);
    EXPECT_EQ(capture->run_type, "backtest");
    EXPECT_EQ(capture->run_mode, RunMode::kBacktest);
}

TEST(ReplayMarketParityTest, ResearchTransformCheckpointPreservesOffsetsAndRejectsRawRestore) {
    MarketBarPipelineConfig config;
    config.timeframes = {1};
    config.bar_aggregator.is_backtest_mode = true;
    config.analysis_transform = std::make_shared<ProductSeriesAdjuster>(true);
    MarketBarPipeline continuous(config);
    auto make = [](const std::string& instrument, int minute, double price) {
        MarketSnapshot tick;
        tick.instrument_id = instrument;
        tick.exchange_id = "DCE";
        tick.trading_day = "20260515";
        tick.action_day = "20260515";
        tick.update_time = "09:0" + std::to_string(minute) + ":10";
        tick.last_price = price;
        tick.volume = 100 + minute;
        tick.exchange_ts_ns =
            detail::ReplayMinuteStartEpochNs("20260515 09:0" + std::to_string(minute)) +
            10'000'000'000LL;
        tick.recv_ts_ns = tick.exchange_ts_ns;
        return tick;
    };
    auto first = make("DCE.c2605", 0, 100);
    (void)continuous.OnTick(first);
    (void)continuous.AdvanceWatermark(first.recv_ts_ns + 60'000'000'000LL);
    MarketBarPipeline::PersistenceState checkpoint;
    std::string error;
    ASSERT_TRUE(continuous.SaveState(&checkpoint, &error)) << error;
    config.analysis_transform = std::make_shared<ProductSeriesAdjuster>(true);
    MarketBarPipeline restored(config);
    ASSERT_TRUE(restored.LoadState(checkpoint, &error)) << error;
    auto second = make("DCE.c2609", 2, 120);
    (void)continuous.OnTick(second);
    (void)restored.OnTick(second);
    auto lhs = continuous.AdvanceWatermark(second.recv_ts_ns + 60'000'000'000LL);
    auto rhs = restored.AdvanceWatermark(second.recv_ts_ns + 60'000'000'000LL);
    ASSERT_EQ(lhs.timeframe_emissions.size(), 1U);
    ASSERT_EQ(rhs.timeframe_emissions.size(), 1U);
    EXPECT_DOUBLE_EQ(lhs.timeframe_emissions[0].bar.analysis_close, 100);
    EXPECT_DOUBLE_EQ(rhs.timeframe_emissions[0].bar.analysis_close, 100);
    EXPECT_DOUBLE_EQ(rhs.timeframe_emissions[0].bar.close, 120);
    config.analysis_transform.reset();
    MarketBarPipeline raw(config);
    EXPECT_FALSE(raw.LoadState(checkpoint, &error));
}
}  // namespace quant_hft::backtest

namespace quant_hft::backtest {
namespace {
struct ProbeCapture {
    std::vector<std::string> calls;
    StrategyContext context;
    int resets{0};
    int volume{1};
    bool opposite_second{false};
    std::function<void()> first_state_action;
};
class ParityProbe final : public ILiveStrategy {
   public:
    explicit ParityProbe(std::shared_ptr<ProbeCapture> capture) : capture_(std::move(capture)) {}
    void Initialize(const StrategyContext& ctx) override { capture_->context = ctx; }
    std::vector<SignalIntent> OnState(const StateSnapshot7D& state) override {
        capture_->calls.push_back("state");
        if (opened_) return {};
        opened_ = true;
        if (capture_->first_state_action) capture_->first_state_action();
        SignalIntent intent;
        intent.instrument_id = state.instrument_id;
        intent.strategy_id = capture_->context.strategy_id;
        intent.volume = capture_->volume;
        intent.ts_ns = state.ts_ns;
        return {intent};
    }
    std::vector<SignalIntent> OnMarketTick(const MarketSnapshot& tick) override {
        capture_->calls.push_back("tick");
        if (capture_->opposite_second && opened_ && !secondary_sent_) {
            secondary_sent_ = true;
            SignalIntent intent;
            intent.instrument_id = tick.instrument_id;
            intent.strategy_id = capture_->context.strategy_id;
            intent.volume = capture_->volume;
            intent.side = Side::kSell;
            intent.ts_ns = tick.exchange_ts_ns;
            return {intent};
        }
        return {};
    }
    void OnOrderEvent(const OrderEvent&) override {}
    std::vector<SignalIntent> OnTimer(EpochNanos) override { return {}; }
    void Shutdown() override {}
    bool ResetForContractSwitch(const ContractSwitchContext&, std::string*) override {
        ++capture_->resets;
        opened_ = false;
        secondary_sent_ = false;
        return true;
    }

   private:
    std::shared_ptr<ProbeCapture> capture_;
    bool opened_{false};
    bool secondary_sent_{false};
};
}  // namespace
TEST(ReplayMarketParityTest, DefaultRuntimeUsesUtcCompleteBarsTickOrderAndSimParameters) {
    const auto dir = std::filesystem::temp_directory_path() /
                     ("quant_hft_parity_e2e_" +
                      std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
    std::filesystem::remove_all(dir);
    std::filesystem::create_directories(dir / "_manifest");
    std::string error;
    std::vector<Tick> ticks;
    const auto base = detail::ReplayMinuteStartEpochNs("20260515 09:00");
    for (int second = 0; second < 240; ++second) {
        Tick tick;
        tick.symbol = "DCE.c2605";
        tick.exchange = "DCE";
        tick.ts_ns = base + static_cast<EpochNanos>(second) * 1'000'000'000LL;
        tick.last_price = 100;
        tick.bid_price1 = 99;
        tick.ask_price1 = 101;
        tick.bid_volume1 = tick.ask_volume1 = 10;
        tick.volume = 100 + second;
        tick.open_interest = 1000;
        ticks.push_back(tick);
        if (second == 120)
            ticks.push_back(tick);  // Same timestamp cannot fill a newly emitted order.
    }
    const auto partition = dir / "ticks.parquet";
    ASSERT_TRUE(test::WriteTickPartitionFixture(partition, ticks, &error)) << error;
    std::ofstream(partition.string() + ".meta", std::ios::app)
        << "schema_version=v3\nsource_csv_fingerprint=parity-test\n";
    const auto manifest = dir / "_manifest/partitions.jsonl";
    std::ofstream(manifest) << "{\"file_path\":\"ticks.parquet\",\"source\":\"c\",\"trading_day\":"
                               "\"20260515\",\"instrument_id\":\"DCE.c2605\",\"min_ts_ns\":"
                            << ticks.front().ts_ns << ",\"max_ts_ns\":" << ticks.back().ts_ns
                            << ",\"row_count\":241}\n";
    const auto runtime = dir / "runtime.yaml";
    std::ofstream(runtime)
        << "ctp:\n  dominant_contract_switch_mode: flat_only\n  "
           "dominant_contract_recheck_interval_ms: 60000\n  dominant_contract_warmup_bars: 1\n  "
           "execution_algo: direct\n  execution_mode: direct\n  execution_price_mode: "
           "marketable_limit\n  cancel_after_ms: 2000\n  "
           "market_bar_allowed_lateness_ms: 0\n  market_bar_poll_interval_ms: 1000\n";
    const auto capture = std::make_shared<ProbeCapture>();
    ASSERT_TRUE(StrategyRegistry::Instance().RegisterFactory(
        "parity_probe", [capture] { return std::make_unique<ParityProbe>(capture); }, &error))
        << error;
    BacktestCliSpec spec;
    spec.engine_mode = "parquet";
    spec.dataset_root = dir.string();
    spec.dataset_manifest = manifest.string();
    spec.online_runtime_config_path = runtime.string();
    spec.start_date = spec.end_date = "20260515";
    spec.symbols = {"DCE.c2605"};
    spec.strategy_factory = "parity_probe";
    spec.run_id = "parity-probe";
    BacktestCliResult result;
    ASSERT_TRUE(RunBacktestSpec(spec, &result, &error)) << error;
    EXPECT_EQ(capture->context.metadata.at("run_type"), "backtest");
    EXPECT_EQ(capture->context.metadata.at("parameter_profile"), "sim");
    EXPECT_EQ(result.event_clock, "virtual_utc");
    EXPECT_EQ(result.replay.first_ts_ns, base - 8LL * 60 * 60 * 1'000'000'000LL);
    EXPECT_EQ(capture->resets, 1);
    auto state = std::find(capture->calls.begin(), capture->calls.end(), "state");
    ASSERT_NE(state, capture->calls.end());
    ASSERT_NE(std::next(state), capture->calls.end());
    EXPECT_EQ(*std::next(state), "tick");
    ASSERT_EQ(result.trades.size(), 1U);
    EXPECT_EQ(result.trades[0].offset, "Open");
    EXPECT_DOUBLE_EQ(result.trades[0].price, 101);
    EXPECT_GT(result.trades[0].timestamp_ns, result.trades[0].signal_ts_ns);
    EXPECT_EQ(result.trades[0].timestamp_ns,
              base - 8LL * 60 * 60 * 1'000'000'000LL + 121'000'000'000LL);
    EXPECT_LE(result.replay.buffered_input_rows_high_water, 4096);
    EXPECT_FALSE(result.market_checkpoint_json.empty());
    ASSERT_EQ(result.contract_statuses.size(), 1U);
    EXPECT_EQ(result.contract_statuses.front().phase, DominantContractPhase::kReady);
    // An explicitly UTC-encoded copy must produce identical strategy and fill time semantics.
    for (auto& tick : ticks) tick.ts_ns -= 8LL * 60 * 60 * 1'000'000'000LL;
    ASSERT_TRUE(test::WriteTickPartitionFixture(partition, ticks, &error)) << error;
    std::ofstream(partition.string() + ".meta", std::ios::app)
        << "schema_version=v3\nsource_csv_fingerprint=utc-parity-test\n";
    std::ofstream(manifest) << "{\"file_path\":\"ticks.parquet\",\"source\":\"c\",\"trading_day\":"
                               "\"20260515\",\"instrument_id\":\"DCE.c2605\",\"min_ts_ns\":"
                            << ticks.front().ts_ns << ",\"max_ts_ns\":" << ticks.back().ts_ns
                            << ",\"row_count\":241}\n";
    spec.input_timestamp_basis = "utc";
    BacktestCliResult utc_result;
    ASSERT_TRUE(RunBacktestSpec(spec, &utc_result, &error)) << error;
    EXPECT_EQ(utc_result.replay.first_ts_ns, result.replay.first_ts_ns);
    ASSERT_EQ(utc_result.trades.size(), result.trades.size());
    EXPECT_EQ(utc_result.trades[0].timestamp_ns, result.trades[0].timestamp_ns);
    EXPECT_EQ(utc_result.trades[0].action_day, "20260515");
    EXPECT_DOUBLE_EQ(utc_result.final_equity, result.final_equity);
    EXPECT_NE(utc_result.input_signature, result.input_signature);
    spec.symbols.push_back("DCE.c2609");
    EXPECT_FALSE(RunBacktestSpec(spec, &utc_result, &error));
    EXPECT_NE(error.find("candidate coverage missing requested instrument"), std::string::npos);
    std::filesystem::remove_all(dir);
}

TEST(ReplayMarketParityTest,
     RuntimeConfigIgnoresConnectionSecretsAndRejectsEnvironmentForDecisions) {
    const auto path = std::filesystem::temp_directory_path() /
                      ("quant_hft_runtime_semantics.yaml_" +
                       std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
    std::ofstream(path) << "ctp:\n  password: ${DO_NOT_RESOLVE}\n  market_bar_poll_interval_ms: "
                           "120\n  dominant_contract_recheck_interval_ms: 60000\n";
    RuntimeSemanticsConfig config;
    std::string error;
    ASSERT_TRUE(LoadRuntimeSemanticsConfig(path.string(), &config, &error)) << error;
    EXPECT_EQ(config.market_bar_poll_interval_ms, 120);
    EXPECT_EQ(config.execution_price_mode, "signal_limit");
    EXPECT_EQ(config.cancel_after_ms, 0);
    EXPECT_EQ(RenderRuntimeSemanticsJson(config).find("password"), std::string::npos);
    EXPECT_EQ(RenderRuntimeSemanticsJson(config).find("DO_NOT_RESOLVE"), std::string::npos);
    std::ofstream(path) << "ctp:\n  execution_mode: SLICED\n  execution_price_mode: market\n";
    ASSERT_TRUE(LoadRuntimeSemanticsConfig(path.string(), &config, &error)) << error;
    EXPECT_EQ(config.execution_mode, "sliced");
    EXPECT_EQ(config.execution_algo, "sliced");
    EXPECT_EQ(config.execution_price_mode, "marketable_limit");
    std::ofstream(path) << "ctp:\n  market_bar_poll_interval_ms: ${POLL_MS}\n";
    EXPECT_FALSE(LoadRuntimeSemanticsConfig(path.string(), &config, &error));
    std::filesystem::remove(path);
}
TEST(ReplayMarketParityTest, SharedRuntimeDefaultsMatchLiveLoaderAndRejectDecisionDrift) {
    RuntimeSemanticsConfig shared;
    CtpFileConfig live;
    std::string error;
    EXPECT_TRUE(ValidateRuntimeSemanticsAgainstCtpConfig(shared, live, &error)) << error;
    live.execution.cancel_after_ms = 2000;
    EXPECT_FALSE(ValidateRuntimeSemanticsAgainstCtpConfig(shared, live, &error));
    EXPECT_NE(error.find("cancel_after_ms"), std::string::npos);
    shared.cancel_after_ms = 2000;
    EXPECT_TRUE(ValidateRuntimeSemanticsAgainstCtpConfig(shared, live, &error)) << error;
    live.market_bar.allowed_lateness_ms = 700;
    EXPECT_FALSE(ValidateRuntimeSemanticsAgainstCtpConfig(shared, live, &error));
    EXPECT_NE(error.find("market_bar_allowed_lateness_ms"), std::string::npos);
    shared.market_bar_allowed_lateness_ms = 700;
    live.dominant_contract_min_hold_ms += 1;
    EXPECT_FALSE(ValidateRuntimeSemanticsAgainstCtpConfig(shared, live, &error));
    EXPECT_NE(error.find("dominant_contract_min_hold_ms"), std::string::npos);
    shared.dominant_contract_min_hold_ms = live.dominant_contract_min_hold_ms;
    shared.risk_rule_groups = "group_a, group_b";
    live.risk.rules.resize(2);
    live.risk.rules[0].rule_group = "group_a";
    live.risk.rules[1].rule_group = "group_b";
    EXPECT_TRUE(ValidateRuntimeSemanticsAgainstCtpConfig(shared, live, &error)) << error;
    live.risk.rules[1].rule_group = "other";
    EXPECT_FALSE(ValidateRuntimeSemanticsAgainstCtpConfig(shared, live, &error));
    EXPECT_NE(error.find("risk_rule_groups"), std::string::npos);
}

namespace {
void CompareReplayRiskAgainstLive(int volume, double max_notional, bool expected_allowed,
                                  bool mutate_rules = false, bool opposite_second = false) {
    const auto dir = std::filesystem::temp_directory_path() /
                     ("quant_hft_risk_parity_" + std::to_string(volume) + "_" +
                      std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
    std::filesystem::create_directories(dir / "_manifest");
    std::vector<Tick> ticks;
    const auto base = detail::ReplayMinuteStartEpochNs("20260515 09:00");
    for (int second = 0; second < 180; ++second) {
        Tick tick;
        tick.symbol = "DCE.c2605";
        tick.exchange = "DCE";
        tick.ts_ns = base + second * 1'000'000'000LL;
        tick.last_price = 100;
        tick.bid_price1 = 99;
        tick.ask_price1 = 101;
        tick.bid_volume1 = tick.ask_volume1 = 1000;
        tick.volume = 100 + second;
        tick.open_interest = 1000;
        ticks.push_back(tick);
    }
    std::string error;
    const auto partition = dir / "ticks.parquet";
    ASSERT_TRUE(test::WriteTickPartitionFixture(partition, ticks, &error)) << error;
    std::ofstream(partition.string() + ".meta", std::ios::app)
        << "schema_version=v3\nsource_csv_fingerprint=risk-parity\n";
    const auto manifest = dir / "_manifest/partitions.jsonl";
    std::ofstream(manifest) << "{\"file_path\":\"ticks.parquet\",\"source\":\"c\",\"trading_day\":"
                               "\"20260515\",\"instrument_id\":\"DCE.c2605\",\"min_ts_ns\":"
                            << ticks.front().ts_ns << ",\"max_ts_ns\":" << ticks.back().ts_ns
                            << ",\"row_count\":180}\n";
    const auto rules = dir / "risk.yaml";
    std::ofstream(rules) << "global:\n  max_order_volume: 100\n  self_trade_prevention: true\n";
    const auto runtime = dir / "runtime.yaml";
    std::ofstream(runtime)
        << "ctp:\n  dominant_contract_switch_mode: flat_only\n"
           "  dominant_contract_recheck_interval_ms: 60000\n  dominant_contract_warmup_bars: 1\n"
           "  execution_price_mode: marketable_limit\n  market_bar_allowed_lateness_ms: 0\n"
           "  market_bar_poll_interval_ms: 1000\n  risk_default_max_order_volume: 200\n"
           "  risk_default_max_order_notional: "
        << max_notional << "\n  risk_rule_file_path: " << rules.string() << "\n";
    auto capture = std::make_shared<ProbeCapture>();
    capture->volume = volume;
    capture->opposite_second = opposite_second;
    if (mutate_rules)
        capture->first_state_action =
            [rules] {
                std::ofstream(rules)
                    << "global:\n  max_order_volume: 102\n  self_trade_prevention: true\n";
            };
    const std::string factory = "risk_parity_probe_" + std::to_string(volume);
    ASSERT_TRUE(StrategyRegistry::Instance().RegisterFactory(
        factory, [capture] { return std::make_unique<ParityProbe>(capture); }, &error))
        << error;
    BacktestCliSpec spec;
    spec.engine_mode = "parquet";
    spec.dataset_root = dir.string();
    spec.dataset_manifest = manifest.string();
    spec.online_runtime_config_path = runtime.string();
    spec.start_date = spec.end_date = "20260515";
    spec.symbols = {"DCE.c2605"};
    spec.strategy_factory = factory;
    spec.initial_equity = 1'000'000;
    auto orders = std::make_shared<OrderManager>();
    auto live = CreateRiskManager(orders, nullptr);
    RiskManagerConfig config;
    config.rule_file_path = rules.string();
    config.enable_dynamic_reload = false;
    config.default_max_order_volume = 200;
    config.default_max_order_notional = max_notional;
    ASSERT_TRUE(live->Initialize(config));
    OrderIntent order;
    order.account_id = spec.account_id;
    order.instrument_id = "DCE.c2605";
    order.strategy_id = "risk_case";
    order.price = 101;
    order.volume = volume;
    OrderContext context;
    context.account_id = spec.account_id;
    context.instrument_id = order.instrument_id;
    context.strategy_id = order.strategy_id;
    context.current_price = 101;
    context.contract_multiplier = 10;
    const auto decision = live->CheckOrder(order, context);
    EXPECT_EQ(decision.allowed, expected_allowed);
    BacktestCliResult result;
    if (mutate_rules) {
        EXPECT_FALSE(RunBacktestSpec(spec, &result, &error));
        EXPECT_NE(error.find("snapshot changed during replay"), std::string::npos);
        std::filesystem::remove_all(dir);
        return;
    }
    ASSERT_TRUE(RunBacktestSpec(spec, &result, &error)) << error;
    EXPECT_EQ(!result.trades.empty(), decision.allowed);
    if (opposite_second) EXPECT_EQ(result.trades.size(), 1U);
    if (decision.allowed && !result.trades.empty()) EXPECT_EQ(result.trades.front().volume, volume);
    std::filesystem::remove_all(dir);
}
}  // namespace
TEST(ReplayMarketParityTest, DefaultRiskFileOverridesCtpVolumeLimitExactlyAsLive) {
    CompareReplayRiskAgainstLive(101, 1'000'000, false);
}
TEST(ReplayMarketParityTest, OrderNotionalUsesSharedLiveUnitsWithoutExtraMultiplier) {
    CompareReplayRiskAgainstLive(3, 2000, true);
}

TEST(ReplayMarketParityTest, SharedRiskSeesPendingOppositeOrderForSelfTradePrevention) {
    CompareReplayRiskAgainstLive(1, 1'000'000, true, false, true);
}
TEST(ReplayMarketParityTest, RuleMutationDuringReplayCannotSilentlyChangeSnapshotSemantics) {
    CompareReplayRiskAgainstLive(2, 1'000'000, true, true);
}
TEST(ReplayMarketParityTest, EnabledSimBudgetFailsBeforeInputAndRuleContentChangesSignature) {
    const auto dir = std::filesystem::temp_directory_path() /
                     ("quant_hft_risk_input_" +
                      std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
    std::filesystem::create_directories(dir);
    const auto rules = dir / "risk.yaml";
    const auto runtime = dir / "runtime.yaml";
    std::ofstream(rules) << "global:\n  max_order_volume: 100\n";
    std::ofstream(runtime) << "ctp:\n  dominant_contract_switch_mode: flat_only\n"
                              "  risk_sim_subaccount_enabled: true\n  risk_rule_file_path: "
                           << rules.string() << "\n";
    BacktestCliSpec spec;
    spec.online_runtime_config_path = runtime.string();
    RuntimeSemanticsConfig before, after;
    std::string error;
    ASSERT_TRUE(LoadRuntimeSemanticsConfig(runtime.string(), &before, &error));
    const auto signature = BuildInputSignature(spec);
    const auto mtime = std::filesystem::last_write_time(rules);
    std::ofstream(rules) << "global:\n  max_order_volume: 101\n";
    std::filesystem::last_write_time(rules, mtime);
    ASSERT_TRUE(LoadRuntimeSemanticsConfig(runtime.string(), &after, &error));
    EXPECT_EQ(before.source_content_fingerprint, after.source_content_fingerprint);
    EXPECT_NE(before.risk_rule_content_fingerprint, after.risk_rule_content_fingerprint);
    EXPECT_NE(signature, BuildInputSignature(spec));
    BacktestCliResult result;
    EXPECT_FALSE(RunBacktestSpec(spec, &result, &error));
    EXPECT_NE(error.find("enabled risk_sim_subaccount"), std::string::npos);
    std::filesystem::remove_all(dir);
}
}  // namespace quant_hft::backtest
