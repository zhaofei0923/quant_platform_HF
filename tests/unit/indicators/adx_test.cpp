#include "quant_hft/indicators/adx.h"

#include <gtest/gtest.h>

#include <limits>
#include <stdexcept>

#include "test_data_adx_reference.h"

namespace quant_hft {
namespace {

constexpr double kTolerance = 1e-6;

TEST(ADXTest, ThrowsWhenPeriodIsNotPositive) {
    EXPECT_THROW((void)ADX(0), std::invalid_argument);
    EXPECT_THROW((void)ADX(-4), std::invalid_argument);
}

TEST(ADXTest, MatchesReferenceValues) {
    ADX adx(test_data::kAdxPeriod);

    for (std::size_t i = 0; i < test_data::kAdxClose.size(); ++i) {
        adx.Update(test_data::kAdxHigh[i], test_data::kAdxLow[i], test_data::kAdxClose[i]);

        if (test_data::kExpectedPlusDi[i] >= 0.0) {
            ASSERT_TRUE(adx.PlusDI().has_value());
            ASSERT_TRUE(adx.MinusDI().has_value());
            EXPECT_NEAR(*adx.PlusDI(), test_data::kExpectedPlusDi[i], kTolerance);
            EXPECT_NEAR(*adx.MinusDI(), test_data::kExpectedMinusDi[i], kTolerance);
        } else {
            EXPECT_FALSE(adx.PlusDI().has_value());
            EXPECT_FALSE(adx.MinusDI().has_value());
        }

        if (test_data::kExpectedAdx[i] >= 0.0) {
            ASSERT_TRUE(adx.IsReady());
            ASSERT_TRUE(adx.Value().has_value());
            EXPECT_NEAR(*adx.Value(), test_data::kExpectedAdx[i], kTolerance);
        } else {
            EXPECT_FALSE(adx.Value().has_value());
        }
    }
}

TEST(ADXTest, HandlesTrendingAndFlatMarkets) {
    ADX uptrend(3);
    uptrend.Update(10.0, 9.0, 9.5);
    uptrend.Update(11.0, 10.0, 10.5);
    uptrend.Update(12.0, 11.0, 11.5);
    uptrend.Update(13.0, 12.0, 12.5);
    uptrend.Update(14.0, 13.0, 13.5);
    ASSERT_TRUE(uptrend.PlusDI().has_value());
    ASSERT_TRUE(uptrend.MinusDI().has_value());
    EXPECT_GT(*uptrend.PlusDI(), *uptrend.MinusDI());

    ADX flat(3);
    for (int i = 0; i < 8; ++i) {
        flat.Update(10.0, 10.0, 10.0);
    }
    ASSERT_TRUE(flat.IsReady());
    ASSERT_TRUE(flat.Value().has_value());
    EXPECT_NEAR(*flat.Value(), 0.0, kTolerance);
}

TEST(ADXTest, IgnoresNonFiniteInputsAndSupportsReset) {
    ADX adx(3);
    adx.Update(10.0, 9.0, 9.5);
    adx.Update(10.5, 9.2, 10.2);
    adx.Update(10.8, 10.0, 10.7);
    ASSERT_TRUE(adx.PlusDI().has_value());

    const double plus_di_before = *adx.PlusDI();
    adx.Update(std::numeric_limits<double>::quiet_NaN(), 10.0, 10.7);
    ASSERT_TRUE(adx.PlusDI().has_value());
    EXPECT_NEAR(*adx.PlusDI(), plus_di_before, kTolerance);

    adx.Reset();
    EXPECT_FALSE(adx.IsReady());
    EXPECT_FALSE(adx.Value().has_value());
    EXPECT_FALSE(adx.PlusDI().has_value());
    EXPECT_FALSE(adx.MinusDI().has_value());
}

}  // namespace
}  // namespace quant_hft
