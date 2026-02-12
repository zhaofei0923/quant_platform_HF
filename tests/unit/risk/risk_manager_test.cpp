#include <memory>
#include <string>
#include <filesystem>
#include <unordered_set>
#include <vector>

#include <gtest/gtest.h>

#include "quant_hft/risk/risk_manager.h"
#include "quant_hft/services/order_manager.h"

namespace quant_hft {
namespace {

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

    bool MarkProcessedOrderEvent(const ProcessedOrderEventRecord& event, std::string* error) override {
        (void)error;
        processed.insert(event.event_key);
        return true;
    }

    bool ExistsProcessedOrderEvent(const std::string& event_key,
                                   bool* exists,
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

    bool LoadPositionSummary(const std::string& account_id,
                             const std::string& strategy_id,
                             std::vector<Position>* out,
                             std::string* error) const override {
        (void)account_id;
        (void)strategy_id;
        (void)error;
        if (out != nullptr) {
            out->clear();
        }
        return true;
    }

    bool UpdateOrderCancelRetry(const std::string& client_order_id,
                                std::int32_t cancel_retry_count,
                                EpochNanos last_cancel_ts_ns,
                                std::string* error) override {
        (void)client_order_id;
        (void)cancel_retry_count;
        (void)last_cancel_ts_ns;
        (void)error;
        return true;
    }

    std::vector<RiskEventRecord> risk_events;
    mutable std::unordered_set<std::string> processed;
};

OrderIntent BuildIntent(const std::string& order_id,
                        Side side = Side::kBuy,
                        double price = 4000.0,
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

    auto result = risk_manager->CheckOrder(BuildIntent("ord-a", Side::kBuy, 4000.0, 3), BuildContext());
    EXPECT_FALSE(result.allowed);
    EXPECT_EQ(result.violated_rule, RiskRuleType::MAX_ORDER_VOLUME);
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

    const auto result = risk_manager->CheckOrder(BuildIntent("ord-reload", Side::kBuy, 4000.0, 2),
                                                 BuildContext());
    EXPECT_FALSE(result.allowed);
}

}  // namespace
}  // namespace quant_hft
