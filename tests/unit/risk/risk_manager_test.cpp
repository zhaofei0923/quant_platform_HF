#include "quant_hft/risk/risk_manager.h"

#include <gtest/gtest.h>

#include <filesystem>
#include <fstream>
#include <memory>
#include <string>
#include <thread>
#include <unordered_set>
#include <vector>

#include "quant_hft/services/order_manager.h"

namespace quant_hft {
namespace {

TEST(RiskManagerTest, CommittedTradeStatisticsRestoreAndDeduplicateByDay) {
    auto manager = CreateRiskManager(nullptr, nullptr);
    Trade first;
    first.trade_id = "canonical-1";
    first.trading_day = "20260907";
    first.profit = -40;
    first.commission = 2;
    first.valuation_complete = true;
    std::string error;
    ASSERT_TRUE(manager->OnCommittedTrade(first.trade_id, first, &error));
    ASSERT_TRUE(manager->OnCommittedTrade(first.trade_id, first, &error));
    EXPECT_DOUBLE_EQ(manager->GetTradeStatistics().loss, 40);
    EXPECT_DOUBLE_EQ(manager->GetTradeStatistics().commission, 2);
    auto restored = CreateRiskManager(nullptr, nullptr);
    ASSERT_TRUE(restored->RestoreTradeStatistics("20260907", {first, first}, &error));
    ASSERT_TRUE(restored->OnCommittedTrade(first.trade_id, first, &error));
    EXPECT_EQ(restored->GetTradeStatistics().trade_count, 1U);
    EXPECT_DOUBLE_EQ(restored->GetTradeStatistics().loss, 40);
    Trade tomorrow = first;
    tomorrow.trade_id = "canonical-2";
    tomorrow.trading_day = "20260908";
    tomorrow.profit = -5;
    ASSERT_TRUE(restored->OnCommittedTrade(tomorrow.trade_id, tomorrow, &error));
    ASSERT_TRUE(restored->OnCommittedTrade(first.trade_id, first, &error));
    EXPECT_DOUBLE_EQ(restored->GetTradeStatistics().loss, 5);
    EXPECT_EQ(restored->GetTradeStatistics().trade_count, 1U);
}

class FakeTradingDomainStore final : public ITradingDomainStore {
   public:
    bool UpsertOrder(const Order& order, std::string* error) override {
        (void)order;
        (void)error;
        return true;
    }

    bool AppendTrade(const Trade& trade, std::string* error) override {
        (void)trade;
        (void)error;
        return true;
    }

    bool UpsertPosition(const Position& position, std::string* error) override {
        (void)position;
        (void)error;
        return true;
    }

    bool UpsertAccount(const Account& account, std::string* error) override {
        (void)account;
        (void)error;
        return true;
    }

    bool AppendRiskEvent(const RiskEventRecord& risk_event, std::string* error) override {
        (void)error;
        risk_events.push_back(risk_event);
        return true;
    }

    bool MarkProcessedOrderEvent(const ProcessedOrderEventRecord& event,
                                 std::string* error) override {
        (void)error;
        processed.insert(event.event_key);
        return true;
    }

    bool ExistsProcessedOrderEvent(const std::string& event_key, bool* exists,
                                   std::string* error) const override {
        (void)error;
        if (exists != nullptr) {
            *exists = processed.find(event_key) != processed.end();
        }
        return true;
    }

    bool InsertPositionDetailFromTrade(const Trade& trade, std::string* error) override {
        (void)trade;
        (void)error;
        return true;
    }

    bool ClosePositionDetailFifo(const Trade& trade, std::string* error) override {
        (void)trade;
        (void)error;
        return true;
    }

    bool LoadPositionSummary(const std::string& account_id, const std::string& strategy_id,
                             std::vector<Position>* out, std::string* error) const override {
        (void)account_id;
        (void)strategy_id;
        (void)error;
        if (out != nullptr) {
            out->clear();
        }
        return true;
    }

    bool UpdateOrderCancelRetry(const std::string& client_order_id, std::int32_t cancel_retry_count,
                                EpochNanos last_cancel_ts_ns, std::string* error) override {
        (void)client_order_id;
        (void)cancel_retry_count;
        (void)last_cancel_ts_ns;
        (void)error;
        return true;
    }

    std::vector<RiskEventRecord> risk_events;
    mutable std::unordered_set<std::string> processed;
};

OrderIntent BuildIntent(const std::string& order_id, Side side = Side::kBuy, double price = 4000.0,
                        int volume = 1) {
    OrderIntent intent;
    intent.account_id = "acc1";
    intent.strategy_id = "trend_001";
    intent.instrument_id = "SHFE.ag2406";
    intent.client_order_id = order_id;
    intent.side = side;
    intent.offset = OffsetFlag::kOpen;
    intent.type = OrderType::kLimit;
    intent.price = price;
    intent.volume = volume;
    return intent;
}

OrderContext BuildContext() {
    OrderContext context;
    context.account_id = "acc1";
    context.strategy_id = "trend_001";
    context.instrument_id = "SHFE.ag2406";
    context.current_price = 4000.0;
    context.contract_multiplier = 10.0;
    return context;
}

TEST(RiskManagerTest, CheckOrderMaxVolumeExceededRejects) {
    auto store = std::make_shared<FakeTradingDomainStore>();
    auto order_manager = std::make_shared<OrderManager>(store);
    auto risk_manager = CreateRiskManager(order_manager, store);

    RiskManagerConfig config;
    config.enable_dynamic_reload = false;
    config.rule_file_path.clear();
    config.default_max_order_volume = 2;
    ASSERT_TRUE(risk_manager->Initialize(config));

    auto result =
        risk_manager->CheckOrder(BuildIntent("ord-a", Side::kBuy, 4000.0, 3), BuildContext());
    EXPECT_FALSE(result.allowed);
    EXPECT_EQ(result.violated_rule, RiskRuleType::MAX_ORDER_VOLUME);
}

TEST(RiskManagerTest, RuntimeMaxVolumeGuardStillAppliesWithRuleFile) {
    namespace fs = std::filesystem;
    const auto rule_path = fs::temp_directory_path() / "quant_hft_runtime_volume_guard.yaml";
    {
        std::ofstream out(rule_path);
        out << "global:\n  max_order_volume: 100\n";
    }

    auto risk_manager = CreateRiskManager(nullptr, nullptr);
    RiskManagerConfig config;
    config.enable_dynamic_reload = false;
    config.rule_file_path = rule_path.string();
    config.default_max_order_volume = 5;
    ASSERT_TRUE(risk_manager->Initialize(config));

    const auto result =
        risk_manager->CheckOrder(BuildIntent("ord-runtime-limit", Side::kBuy, 4000.0, 6),
                                 BuildContext());
    EXPECT_FALSE(result.allowed);
    EXPECT_EQ(result.violated_rule, RiskRuleType::MAX_ORDER_VOLUME);
    fs::remove(rule_path);
}

TEST(RiskManagerTest, PositionNotionalGuardUsesProjectedOpenExposureAndAllowsCloses) {
    auto risk_manager = CreateRiskManager(nullptr, nullptr);
    RiskManagerConfig config;
    config.enable_dynamic_reload = false;
    config.rule_file_path.clear();
    config.default_max_order_volume = 100;
    config.default_max_order_notional = 250000.0;
    config.default_max_position_notional = 250000.0;
    ASSERT_TRUE(risk_manager->Initialize(config));

    auto context = BuildContext();
    context.current_price = 5000.0;
    context.current_position = 4.0;
    auto projected_over_limit = BuildIntent("ord-position", Side::kBuy, 5000.0, 2);
    auto result = risk_manager->CheckOrder(projected_over_limit, context);
    EXPECT_FALSE(result.allowed);
    EXPECT_EQ(result.violated_rule, RiskRuleType::MAX_POSITION_NOTIONAL);

    context.current_position = 6.0;
    auto reduce_only = BuildIntent("ord-close", Side::kSell, 5000.0, 2);
    reduce_only.offset = OffsetFlag::kCloseToday;
    EXPECT_TRUE(risk_manager->CheckOrder(reduce_only, context).allowed);
}

TEST(RiskManagerTest, PositionVolumeGuardChecksProjectedOpensButAllowsCloses) {
    auto risk_manager = CreateRiskManager(nullptr, nullptr);
    RiskManagerConfig config;
    config.enable_dynamic_reload = false;
    config.rule_file_path.clear();
    ASSERT_TRUE(risk_manager->Initialize(config));

    RiskRule position_rule;
    position_rule.rule_id = "risk.test.max_position_per_instrument";
    position_rule.type = RiskRuleType::MAX_POSITION_PER_INSTRUMENT;
    position_rule.strategy_id = "trend_001";
    position_rule.threshold = 5.0;
    ASSERT_TRUE(risk_manager->ReloadRules({position_rule}));

    auto context = BuildContext();
    context.current_position = 4.0;
    const auto open_result =
        risk_manager->CheckOrder(BuildIntent("ord-add", Side::kBuy, 4000.0, 2), context);
    EXPECT_FALSE(open_result.allowed);
    EXPECT_EQ(open_result.violated_rule, RiskRuleType::MAX_POSITION_PER_INSTRUMENT);

    context.current_position = 6.0;
    auto close_intent = BuildIntent("ord-reduce", Side::kSell, 4000.0, 2);
    close_intent.offset = OffsetFlag::kCloseYesterday;
    EXPECT_TRUE(risk_manager->CheckOrder(close_intent, context).allowed);
}

TEST(RiskManagerTest, CheckOrderSelfTradePreventionCrossPriceRejects) {
    auto store = std::make_shared<FakeTradingDomainStore>();
    auto order_manager = std::make_shared<OrderManager>(store);

    auto existing_intent = BuildIntent("resting-sell", Side::kSell, 4000.0, 1);
    (void)order_manager->CreateOrder(existing_intent);

    auto risk_manager = CreateRiskManager(order_manager, store);
    RiskManagerConfig config;
    config.enable_dynamic_reload = false;
    config.rule_file_path.clear();
    ASSERT_TRUE(risk_manager->Initialize(config));

    auto buy_intent = BuildIntent("incoming-buy", Side::kBuy, 4001.0, 1);
    auto result = risk_manager->CheckOrder(buy_intent, BuildContext());
    EXPECT_FALSE(result.allowed);
    EXPECT_EQ(result.violated_rule, RiskRuleType::SELF_TRADE_PREVENTION);
}

TEST(RiskManagerTest, SelfTradePreventionCoversEveryOpenCloseCombination) {
    for (const auto resting_offset : {OffsetFlag::kOpen, OffsetFlag::kCloseToday}) {
        for (const auto incoming_offset : {OffsetFlag::kOpen, OffsetFlag::kCloseYesterday}) {
            auto store = std::make_shared<FakeTradingDomainStore>();
            auto orders = std::make_shared<OrderManager>(store);
            auto resting = BuildIntent("resting", Side::kSell, 4000.0);
            resting.offset = resting_offset;
            orders->CreateOrder(resting);
            auto risk = CreateRiskManager(orders, store);
            RiskManagerConfig config;
            config.enable_dynamic_reload = false;
            config.rule_file_path.clear();
            ASSERT_TRUE(risk->Initialize(config));
            auto incoming = BuildIntent("incoming", Side::kBuy, 4000.0);
            incoming.offset = incoming_offset;
            const auto result = risk->CheckOrder(incoming, BuildContext());
            EXPECT_FALSE(result.allowed);
            EXPECT_EQ(result.violated_rule, RiskRuleType::SELF_TRADE_PREVENTION);
        }
    }
}

TEST(RiskManagerTest, CheckOrderSelfTradePreventionRejectsAcrossStrategiesInSameAccount) {
    auto store = std::make_shared<FakeTradingDomainStore>();
    auto order_manager = std::make_shared<OrderManager>(store);

    auto existing_intent = BuildIntent("resting-cross-strategy", Side::kSell, 4000.0, 1);
    existing_intent.strategy_id = "mean_reversion_001";
    (void)order_manager->CreateOrder(existing_intent);

    auto risk_manager = CreateRiskManager(order_manager, store);
    RiskManagerConfig config;
    config.enable_dynamic_reload = false;
    config.rule_file_path.clear();
    ASSERT_TRUE(risk_manager->Initialize(config));

    auto buy_intent = BuildIntent("incoming-cross-strategy", Side::kBuy, 4001.0, 1);
    buy_intent.strategy_id = "trend_001";
    auto context = BuildContext();
    context.strategy_id = buy_intent.strategy_id;
    const auto result = risk_manager->CheckOrder(buy_intent, context);

    EXPECT_FALSE(result.allowed);
    EXPECT_EQ(result.violated_rule, RiskRuleType::SELF_TRADE_PREVENTION);
}

TEST(RiskManagerTest, CheckOrderOrderRateExceededRejects) {
    auto store = std::make_shared<FakeTradingDomainStore>();
    auto order_manager = std::make_shared<OrderManager>(store);
    auto risk_manager = CreateRiskManager(order_manager, store);

    RiskManagerConfig config;
    config.enable_dynamic_reload = false;
    config.rule_file_path.clear();
    config.default_max_order_rate = 1;
    ASSERT_TRUE(risk_manager->Initialize(config));

    auto context = BuildContext();
    EXPECT_TRUE(risk_manager->CheckOrder(BuildIntent("ord-r1"), context).allowed);
    EXPECT_FALSE(risk_manager->CheckOrder(BuildIntent("ord-r2"), context).allowed);
}

TEST(RiskManagerTest, CheckOrderSimSubaccountCapitalExceededRejects) {
    auto store = std::make_shared<FakeTradingDomainStore>();
    auto order_manager = std::make_shared<OrderManager>(store);
    auto risk_manager = CreateRiskManager(order_manager, store);

    RiskManagerConfig config;
    config.enable_dynamic_reload = false;
    config.rule_file_path.clear();
    config.sim_subaccount_enabled = true;
    config.sim_subaccount_id = "acc1";
    config.sim_subaccount_initial_equity = 200000.0;
    config.sim_subaccount_max_margin = 200000.0;
    config.sim_subaccount_order_margin_rate = 0.1;
    config.sim_subaccount_contract_multiplier = 10.0;
    ASSERT_TRUE(risk_manager->Initialize(config));

    auto context = BuildContext();
    context.current_margin = 199000.0;
    context.contract_multiplier = 0.0;
    const auto result =
        risk_manager->CheckOrder(BuildIntent("ord-subaccount", Side::kBuy, 1000.0, 2), context);

    EXPECT_FALSE(result.allowed);
    EXPECT_EQ(result.violated_rule, RiskRuleType::SIM_SUBACCOUNT_CAPITAL);
    ASSERT_TRUE(result.limit_value.has_value());
    ASSERT_TRUE(result.current_value.has_value());
    EXPECT_DOUBLE_EQ(result.limit_value.value(), 200000.0);
    EXPECT_DOUBLE_EQ(result.current_value.value(), 201000.0);
}

TEST(RiskManagerTest, CheckOrderSimSubaccountUsesContextContractMultiplier) {
    auto store = std::make_shared<FakeTradingDomainStore>();
    auto order_manager = std::make_shared<OrderManager>(store);
    auto risk_manager = CreateRiskManager(order_manager, store);

    RiskManagerConfig config;
    config.enable_dynamic_reload = false;
    config.rule_file_path.clear();
    config.sim_subaccount_enabled = true;
    config.sim_subaccount_id = "acc1";
    config.sim_subaccount_initial_equity = 950.0;
    config.sim_subaccount_max_margin = 950.0;
    config.sim_subaccount_order_margin_rate = 0.1;
    config.sim_subaccount_contract_multiplier = 1.0;
    ASSERT_TRUE(risk_manager->Initialize(config));

    auto context = BuildContext();
    context.contract_multiplier = 10.0;
    const auto result = risk_manager->CheckOrder(
        BuildIntent("ord-product-multiplier", Side::kBuy, 1000.0, 1), context);

    EXPECT_FALSE(result.allowed);
    EXPECT_EQ(result.violated_rule, RiskRuleType::SIM_SUBACCOUNT_CAPITAL);
    ASSERT_TRUE(result.current_value.has_value());
    EXPECT_DOUBLE_EQ(result.current_value.value(), 1000.0);
}

TEST(RiskManagerTest, CheckCancelCancelRateExceededRejects) {
    auto store = std::make_shared<FakeTradingDomainStore>();
    auto order_manager = std::make_shared<OrderManager>(store);
    auto risk_manager = CreateRiskManager(order_manager, store);

    RiskManagerConfig config;
    config.enable_dynamic_reload = false;
    config.rule_file_path.clear();
    config.default_max_cancel_rate = 1;
    ASSERT_TRUE(risk_manager->Initialize(config));

    auto context = BuildContext();
    EXPECT_TRUE(risk_manager->CheckCancel("ord-c1", context).allowed);
    EXPECT_FALSE(risk_manager->CheckCancel("ord-c2", context).allowed);
}

TEST(RiskManagerTest, CheckCancelDailyCancelCountExceededRejects) {
    auto store = std::make_shared<FakeTradingDomainStore>();
    auto order_manager = std::make_shared<OrderManager>(store);
    auto risk_manager = CreateRiskManager(order_manager, store);

    RiskManagerConfig config;
    config.enable_dynamic_reload = false;
    config.rule_file_path.clear();
    config.default_max_daily_cancel_count = 1;
    ASSERT_TRUE(risk_manager->Initialize(config));

    auto context = BuildContext();
    EXPECT_TRUE(risk_manager->CheckCancel("ord-daily-c1", context).allowed);
    const auto rejected = risk_manager->CheckCancel("ord-daily-c2", context);
    EXPECT_FALSE(rejected.allowed);
    EXPECT_EQ(rejected.violated_rule, RiskRuleType::MAX_DAILY_CANCEL_COUNT);

    risk_manager->ResetDailyStats();
    EXPECT_TRUE(risk_manager->CheckCancel("ord-daily-c3", context).allowed);
}

TEST(RiskManagerTest, RiskRuleLoadFromYamlSuccess) {
    namespace fs = std::filesystem;
    std::string rule_path = "configs/risk_rules.yaml";
    if (!fs::exists(rule_path)) {
        rule_path = "../configs/risk_rules.yaml";
    }
    std::string error;
    const auto rules = LoadRiskRulesFromYaml(rule_path, &error);
    EXPECT_FALSE(rules.empty()) << error;
}

TEST(RiskManagerTest, RiskManagerReloadRulesDynamicUpdate) {
    auto store = std::make_shared<FakeTradingDomainStore>();
    auto order_manager = std::make_shared<OrderManager>(store);
    auto risk_manager = CreateRiskManager(order_manager, store);

    RiskManagerConfig config;
    config.enable_dynamic_reload = false;
    config.rule_file_path.clear();
    ASSERT_TRUE(risk_manager->Initialize(config));

    std::vector<RiskRule> rules;
    RiskRule max_volume_rule;
    max_volume_rule.rule_id = "risk.test.max_volume";
    max_volume_rule.type = RiskRuleType::MAX_ORDER_VOLUME;
    max_volume_rule.strategy_id = "trend_001";
    max_volume_rule.threshold = 1.0;
    max_volume_rule.priority = 1;
    rules.push_back(max_volume_rule);
    ASSERT_TRUE(risk_manager->ReloadRules(rules));

    const auto result =
        risk_manager->CheckOrder(BuildIntent("ord-reload", Side::kBuy, 4000.0, 2), BuildContext());
    EXPECT_FALSE(result.allowed);
}

TEST(RiskManagerTest, InitializeStopsExistingDynamicReloadThread) {
    namespace fs = std::filesystem;
    const auto rule_path = fs::temp_directory_path() / "quant_hft_risk_reload_thread.yaml";
    {
        std::ofstream out(rule_path);
        out << "# intentionally empty: manager should fall back to defaults\n";
    }

    auto store = std::make_shared<FakeTradingDomainStore>();
    auto order_manager = std::make_shared<OrderManager>(store);
    auto risk_manager = CreateRiskManager(order_manager, store);

    RiskManagerConfig config;
    config.enable_dynamic_reload = true;
    config.reload_interval_seconds = 1;
    config.rule_file_path = rule_path.string();

    ASSERT_TRUE(risk_manager->Initialize(config));
    ASSERT_TRUE(risk_manager->Initialize(config));
    EXPECT_FALSE(risk_manager->GetActiveRules().empty());

    fs::remove(rule_path);
}

TEST(RiskManagerTest, VirtualClockRefillsTheSameRateLimiterWithoutWallClockSleep) {
    auto now = std::chrono::steady_clock::time_point{};
    auto manager = CreateRiskManager(nullptr, nullptr);
    RiskManagerConfig config;
    config.rule_file_path.clear();
    config.enable_dynamic_reload = false;
    config.default_max_order_rate = 2;
    config.default_max_cancel_rate = 0;
    config.monotonic_now = [&] { return now; };
    ASSERT_TRUE(manager->Initialize(config));
    auto intent = BuildIntent("clock", Side::kBuy, 4000, 1);
    EXPECT_TRUE(manager->CheckOrder(intent, BuildContext()).allowed);
    EXPECT_TRUE(manager->CheckOrder(intent, BuildContext()).allowed);
    EXPECT_FALSE(manager->CheckOrder(intent, BuildContext()).allowed);
    now += std::chrono::milliseconds(500);
    EXPECT_TRUE(manager->CheckOrder(intent, BuildContext()).allowed);
    EXPECT_FALSE(manager->CheckOrder(intent, BuildContext()).allowed);
}

TEST(RiskManagerTest, DestructionInterruptsLongReloadInterval) {
    const auto path =
        std::filesystem::temp_directory_path() /
        ("quant_hft_risk_stop_" +
         std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()) + ".yaml");
    std::ofstream(path) << "global:\n  max_order_volume: 100\n";
    auto manager = CreateRiskManager(nullptr, nullptr);
    RiskManagerConfig config;
    config.rule_file_path = path.string();
    config.reload_interval_seconds = 60;
    ASSERT_TRUE(manager->Initialize(config));
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    const auto start = std::chrono::steady_clock::now();
    manager.reset();
    EXPECT_LT(std::chrono::steady_clock::now() - start, std::chrono::seconds(1));
    std::filesystem::remove(path);
}

}  // namespace
}  // namespace quant_hft
