#include "quant_hft/indicators/kama.h"

#include <gtest/gtest.h>

#include <cmath>
#include <limits>
#include <stdexcept>

namespace quant_hft {
namespace {

constexpr double kTolerance = 1e-8;

TEST(KAMATest, ThrowsWhenAnyPeriodIsNotPositive) {
    EXPECT_THROW((void)KAMA(0, 2, 30), std::invalid_argument);
    EXPECT_THROW((void)KAMA(10, 0, 30), std::invalid_argument);
    EXPECT_THROW((void)KAMA(10, 2, 0), std::invalid_argument);
}

TEST(KAMATest, UsesSmaSeedAndAdaptiveSmoothing) {
    KAMA kama(3, 2, 5);

    kama.Update(0.0, 0.0, 9.5);
    EXPECT_FALSE(kama.IsReady());

    kama.Update(0.0, 0.0, 10.2);
    EXPECT_FALSE(kama.IsReady());

    kama.Update(0.0, 0.0, 10.7);
    EXPECT_FALSE(kama.IsReady());

    kama.Update(0.0, 0.0, 10.9);
    ASSERT_TRUE(kama.IsReady());
    ASSERT_TRUE(kama.Value().has_value());
    ASSERT_TRUE(kama.EfficiencyRatio().has_value());
    EXPECT_NEAR(*kama.Value(), 10.325, kTolerance);
    EXPECT_NEAR(*kama.EfficiencyRatio(), 1.0, kTolerance);

    kama.Update(0.0, 0.0, 11.1);
    ASSERT_TRUE(kama.Value().has_value());
    ASSERT_TRUE(kama.EfficiencyRatio().has_value());
    EXPECT_NEAR(*kama.Value(), 10.669444444444444, kTolerance);
    EXPECT_NEAR(*kama.EfficiencyRatio(), 1.0, kTolerance);

    kama.Update(0.0, 0.0, 11.4);
    ASSERT_TRUE(kama.Value().has_value());
    ASSERT_TRUE(kama.EfficiencyRatio().has_value());
    EXPECT_NEAR(*kama.Value(), 10.994135802469136, kTolerance);
    EXPECT_NEAR(*kama.EfficiencyRatio(), 1.0, kTolerance);
}

TEST(KAMATest, HandlesZeroEfficiencyRatioSequence) {
    KAMA kama(3, 2, 5);

    kama.Update(0.0, 0.0, 10.0);
    kama.Update(0.0, 0.0, 11.0);
    kama.Update(0.0, 0.0, 10.0);
    EXPECT_FALSE(kama.EfficiencyRatio().has_value());
    kama.Update(0.0, 0.0, 11.0);
    ASSERT_TRUE(kama.IsReady());
    ASSERT_TRUE(kama.Value().has_value());
    ASSERT_TRUE(kama.EfficiencyRatio().has_value());
    EXPECT_NEAR(*kama.EfficiencyRatio(), 1.0 / 3.0, kTolerance);

    const double before = *kama.Value();
    kama.Update(0.0, 0.0, 10.0);
    ASSERT_TRUE(kama.Value().has_value());
    ASSERT_TRUE(kama.EfficiencyRatio().has_value());
    EXPECT_NEAR(*kama.EfficiencyRatio(), 1.0 / 3.0, kTolerance);
    EXPECT_LT(std::abs(*kama.Value() - before), 0.5);
}

TEST(KAMATest, IgnoresNonFiniteInputsAndSupportsReset) {
    KAMA kama(3, 2, 5);
    kama.Update(0.0, 0.0, 9.5);
    kama.Update(0.0, 0.0, 10.2);
    kama.Update(0.0, 0.0, 10.7);
    kama.Update(0.0, 0.0, 10.9);
    ASSERT_TRUE(kama.Value().has_value());

    kama.Update(0.0, 0.0, std::numeric_limits<double>::quiet_NaN());
    EXPECT_NEAR(*kama.Value(), 10.325, kTolerance);
    ASSERT_TRUE(kama.EfficiencyRatio().has_value());
    EXPECT_NEAR(*kama.EfficiencyRatio(), 1.0, kTolerance);

    kama.Reset();
    EXPECT_FALSE(kama.IsReady());
    EXPECT_FALSE(kama.Value().has_value());
    EXPECT_FALSE(kama.EfficiencyRatio().has_value());
}

}  // namespace
}  // namespace quant_hft
