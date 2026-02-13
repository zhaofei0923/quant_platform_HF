#include <stdexcept>

#include <gtest/gtest.h>

#include "quant_hft/strategy/base_strategy.h"

namespace quant_hft {

namespace {

class TestStrategy : public Strategy {
public:
    using Strategy::Buy;
    using Strategy::CancelOrder;
    using Strategy::Sell;
};

}  // namespace

TEST(BaseStrategyTest, BuySellThrowWhenNotBound) {
    TestStrategy strategy;

    EXPECT_THROW(strategy.Buy("rb2405", 3500.0, 1), std::runtime_error);
    EXPECT_THROW(strategy.Sell("rb2405", 3501.0, 1), std::runtime_error);
    EXPECT_THROW(strategy.CancelOrder("ord-1"), std::runtime_error);
}

}  // namespace quant_hft
