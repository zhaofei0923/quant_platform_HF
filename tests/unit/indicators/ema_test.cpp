#include "quant_hft/indicators/ema.h"

#include <gtest/gtest.h>

#include <limits>
#include <stdexcept>

namespace quant_hft {
namespace {

constexpr double kTolerance = 1e-8;

TEST(EMATest, ThrowsWhenPeriodIsNotPositive) {
    EXPECT_THROW((void)EMA(0), std::invalid_argument);
    EXPECT_THROW((void)EMA(-5), std::invalid_argument);
}

TEST(EMATest, UsesSmaSeedAndExponentialUpdate) {
    EMA ema(3);

    ema.Update(0.0, 0.0, 9.5);
    EXPECT_FALSE(ema.IsReady());

    ema.Update(0.0, 0.0, 10.2);
    EXPECT_FALSE(ema.IsReady());

    ema.Update(0.0, 0.0, 10.7);
    ASSERT_TRUE(ema.IsReady());
    ASSERT_TRUE(ema.Value().has_value());
    EXPECT_NEAR(*ema.Value(), 10.133333333333333, kTolerance);

    ema.Update(0.0, 0.0, 10.9);
    ASSERT_TRUE(ema.Value().has_value());
    EXPECT_NEAR(*ema.Value(), 10.516666666666666, kTolerance);

    ema.Update(0.0, 0.0, 11.1);
    ASSERT_TRUE(ema.Value().has_value());
    EXPECT_NEAR(*ema.Value(), 10.808333333333334, kTolerance);
}

TEST(EMATest, IgnoresNonFiniteInputs) {
    EMA ema(3);
    ema.Update(0.0, 0.0, 1.0);
    ema.Update(0.0, 0.0, 2.0);
    ema.Update(0.0, 0.0, 3.0);
    ASSERT_TRUE(ema.Value().has_value());

    ema.Update(0.0, 0.0, std::numeric_limits<double>::quiet_NaN());
    EXPECT_NEAR(*ema.Value(), 2.0, kTolerance);

    ema.Update(0.0, 0.0, std::numeric_limits<double>::infinity());
    EXPECT_NEAR(*ema.Value(), 2.0, kTolerance);
}

TEST(EMATest, ResetClearsState) {
    EMA ema(2);
    ema.Update(0.0, 0.0, 10.0);
    ema.Update(0.0, 0.0, 12.0);
    ASSERT_TRUE(ema.IsReady());

    ema.Reset();

    EXPECT_FALSE(ema.IsReady());
    EXPECT_FALSE(ema.Value().has_value());
}

}  // namespace
}  // namespace quant_hft
