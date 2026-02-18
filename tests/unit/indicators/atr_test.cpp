#include "quant_hft/indicators/atr.h"

#include <gtest/gtest.h>

#include <limits>
#include <stdexcept>

namespace quant_hft {
namespace {

constexpr double kTolerance = 1e-8;

TEST(ATRTest, ThrowsWhenPeriodIsNotPositive) {
    EXPECT_THROW((void)ATR(0), std::invalid_argument);
    EXPECT_THROW((void)ATR(-2), std::invalid_argument);
}

TEST(ATRTest, ComputesWilderAtrFromTrueRange) {
    ATR atr(3);

    atr.Update(10.0, 9.0, 9.5);
    EXPECT_FALSE(atr.IsReady());

    atr.Update(10.5, 9.2, 10.2);
    EXPECT_FALSE(atr.IsReady());

    atr.Update(10.8, 10.0, 10.7);
    ASSERT_TRUE(atr.IsReady());
    ASSERT_TRUE(atr.Value().has_value());
    EXPECT_NEAR(*atr.Value(), 1.0333333333333339, kTolerance);

    atr.Update(11.0, 10.4, 10.9);
    ASSERT_TRUE(atr.Value().has_value());
    EXPECT_NEAR(*atr.Value(), 0.8888888888888892, kTolerance);

    atr.Update(11.2, 10.7, 11.1);
    ASSERT_TRUE(atr.Value().has_value());
    EXPECT_NEAR(*atr.Value(), 0.7592592592592595, kTolerance);
}

TEST(ATRTest, IgnoresNonFiniteInputs) {
    ATR atr(3);
    atr.Update(10.0, 9.0, 9.5);
    atr.Update(10.5, 9.2, 10.2);
    atr.Update(10.8, 10.0, 10.7);
    ASSERT_TRUE(atr.Value().has_value());

    atr.Update(std::numeric_limits<double>::quiet_NaN(), 10.0, 10.7);
    EXPECT_NEAR(*atr.Value(), 1.0333333333333339, kTolerance);

    atr.Update(11.0, std::numeric_limits<double>::infinity(), 10.9);
    EXPECT_NEAR(*atr.Value(), 1.0333333333333339, kTolerance);
}

TEST(ATRTest, ResetClearsState) {
    ATR atr(2);
    atr.Update(10.0, 9.0, 9.5);
    atr.Update(10.5, 9.5, 10.0);
    ASSERT_TRUE(atr.IsReady());

    atr.Reset();

    EXPECT_FALSE(atr.IsReady());
    EXPECT_FALSE(atr.Value().has_value());
}

}  // namespace
}  // namespace quant_hft
