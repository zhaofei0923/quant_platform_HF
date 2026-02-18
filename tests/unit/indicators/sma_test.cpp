#include "quant_hft/indicators/sma.h"

#include <gtest/gtest.h>

#include <limits>
#include <stdexcept>

namespace quant_hft {
namespace {

constexpr double kTolerance = 1e-8;

TEST(SMATest, ThrowsWhenPeriodIsNotPositive) {
    EXPECT_THROW((void)SMA(0), std::invalid_argument);
    EXPECT_THROW((void)SMA(-3), std::invalid_argument);
}

TEST(SMATest, ComputesSlidingAverageAndReadyState) {
    SMA sma(3);

    EXPECT_FALSE(sma.IsReady());
    EXPECT_FALSE(sma.Value().has_value());

    sma.Update(0.0, 0.0, 1.0);
    EXPECT_FALSE(sma.IsReady());

    sma.Update(0.0, 0.0, 2.0);
    EXPECT_FALSE(sma.IsReady());

    sma.Update(0.0, 0.0, 3.0);
    ASSERT_TRUE(sma.IsReady());
    ASSERT_TRUE(sma.Value().has_value());
    EXPECT_NEAR(*sma.Value(), 2.0, kTolerance);

    sma.Update(0.0, 0.0, 4.0);
    ASSERT_TRUE(sma.Value().has_value());
    EXPECT_NEAR(*sma.Value(), 3.0, kTolerance);
}

TEST(SMATest, IgnoresNonFiniteInputs) {
    SMA sma(3);
    sma.Update(0.0, 0.0, 1.0);
    sma.Update(0.0, 0.0, 2.0);
    sma.Update(0.0, 0.0, 3.0);
    ASSERT_TRUE(sma.Value().has_value());

    sma.Update(0.0, 0.0, std::numeric_limits<double>::quiet_NaN());
    EXPECT_NEAR(*sma.Value(), 2.0, kTolerance);

    sma.Update(0.0, 0.0, std::numeric_limits<double>::infinity());
    EXPECT_NEAR(*sma.Value(), 2.0, kTolerance);
}

TEST(SMATest, ResetClearsState) {
    SMA sma(2);
    sma.Update(0.0, 0.0, 10.0);
    sma.Update(0.0, 0.0, 12.0);

    ASSERT_TRUE(sma.IsReady());
    ASSERT_TRUE(sma.Value().has_value());

    sma.Reset();

    EXPECT_FALSE(sma.IsReady());
    EXPECT_FALSE(sma.Value().has_value());
}

}  // namespace
}  // namespace quant_hft
