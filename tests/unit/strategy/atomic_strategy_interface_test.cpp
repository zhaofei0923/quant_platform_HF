#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "quant_hft/strategy/atomic_strategy.h"

namespace quant_hft {
namespace {

class DummyOpeningStrategy final : public IOpeningStrategy, public IAtomicOrderAware {
   public:
    void Init(const AtomicParams& params) override { params_ = params; }

    std::string GetId() const override { return "dummy_opening"; }

    void Reset() override {
        params_.clear();
        on_order_event_calls_ = 0;
    }

    std::vector<SignalIntent> OnState(const StateSnapshot7D& state,
                                      const AtomicStrategyContext& ctx) override {
        (void)state;
        (void)ctx;
        return {};
    }

    void OnOrderEvent(const OrderEvent& event, const AtomicStrategyContext& ctx) override {
        (void)event;
        (void)ctx;
        ++on_order_event_calls_;
    }

    int on_order_event_calls() const { return on_order_event_calls_; }

   private:
    AtomicParams params_;
    int on_order_event_calls_{0};
};

TEST(AtomicStrategyInterfaceTest, ContextCarriesAccountPositionAndAverageOpenPrice) {
    AtomicStrategyContext ctx;
    ctx.account_id = "sim-account";
    ctx.net_positions["SHFE.rb2405"] = 2;
    ctx.avg_open_prices["SHFE.rb2405"] = 3500.5;

    EXPECT_EQ(ctx.account_id, "sim-account");
    ASSERT_EQ(ctx.net_positions.count("SHFE.rb2405"), 1U);
    EXPECT_EQ(ctx.net_positions["SHFE.rb2405"], 2);
    ASSERT_EQ(ctx.avg_open_prices.count("SHFE.rb2405"), 1U);
    EXPECT_DOUBLE_EQ(ctx.avg_open_prices["SHFE.rb2405"], 3500.5);
}

TEST(AtomicStrategyInterfaceTest, OrderAwareMixinCanBeInvokedWithoutAffectingBaseInterface) {
    DummyOpeningStrategy strategy;
    strategy.Init({});

    OrderEvent event;
    AtomicStrategyContext ctx;
    strategy.OnOrderEvent(event, ctx);
    EXPECT_EQ(strategy.on_order_event_calls(), 1);

    strategy.Reset();
    EXPECT_EQ(strategy.on_order_event_calls(), 0);
    EXPECT_EQ(strategy.GetId(), "dummy_opening");
}

}  // namespace
}  // namespace quant_hft
