#include "quant_hft/strategy/live_strategy.h"
#include "quant_hft/strategy/strategy_registry.h"

#include <atomic>
#include <memory>
#include <string>
#include <vector>

#include <gtest/gtest.h>

namespace quant_hft {
namespace {

std::string UniqueFactoryName() {
    static std::atomic<int> seq{0};
    return "strategy_registry_test_factory_" + std::to_string(seq.fetch_add(1));
}

class TestLiveStrategy final : public ILiveStrategy {
public:
    void Initialize(const StrategyContext& ctx) override { context_ = ctx; }

    std::vector<SignalIntent> OnState(const StateSnapshot7D& state) override {
        (void)state;
        return {};
    }

    void OnOrderEvent(const OrderEvent& event) override { (void)event; }

    std::vector<SignalIntent> OnTimer(EpochNanos now_ns) override {
        (void)now_ns;
        return {};
    }

    void Shutdown() override {}

private:
    StrategyContext context_;
};

}  // namespace

TEST(StrategyRegistryTest, RegistersAndCreatesFactory) {
    std::string error;
    const auto factory_name = UniqueFactoryName();
    ASSERT_TRUE(StrategyRegistry::Instance().RegisterFactory(
        factory_name,
        []() { return std::make_unique<TestLiveStrategy>(); },
        &error))
        << error;

    auto strategy = StrategyRegistry::Instance().Create(factory_name);
    ASSERT_NE(strategy, nullptr);
}

TEST(StrategyRegistryTest, RejectsDuplicateFactoryRegistration) {
    std::string error;
    const auto factory_name = UniqueFactoryName();
    ASSERT_TRUE(StrategyRegistry::Instance().RegisterFactory(
        factory_name,
        []() { return std::make_unique<TestLiveStrategy>(); },
        &error))
        << error;

    error.clear();
    EXPECT_FALSE(StrategyRegistry::Instance().RegisterFactory(
        factory_name,
        []() { return std::make_unique<TestLiveStrategy>(); },
        &error));
    EXPECT_NE(error.find("already"), std::string::npos);
}

TEST(StrategyRegistryTest, ReturnsNullptrForUnknownFactory) {
    auto strategy = StrategyRegistry::Instance().Create("strategy_registry_test_missing");
    EXPECT_EQ(strategy, nullptr);
}

}  // namespace quant_hft
