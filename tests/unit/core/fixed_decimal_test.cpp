#include <gtest/gtest.h>

#include "quant_hft/core/fixed_decimal.h"

namespace quant_hft {

TEST(FixedDecimalTest, ToScaledSupportsHalfUpDownAndUp) {
    EXPECT_EQ(FixedDecimal::ToScaled(1.234L, 2, FixedRoundingMode::kHalfUp), 123);
    EXPECT_EQ(FixedDecimal::ToScaled(1.235L, 2, FixedRoundingMode::kHalfUp), 124);
    EXPECT_EQ(FixedDecimal::ToScaled(1.239L, 2, FixedRoundingMode::kDown), 123);
    EXPECT_EQ(FixedDecimal::ToScaled(1.231L, 2, FixedRoundingMode::kUp), 124);
    EXPECT_EQ(FixedDecimal::ToScaled(-1.235L, 2, FixedRoundingMode::kHalfUp), -124);
}

TEST(FixedDecimalTest, RescaleKeepsSemanticValueWithConfiguredRounding) {
    const std::int64_t scaled_4 = 12345;  // 1.2345
    EXPECT_EQ(FixedDecimal::Rescale(scaled_4, 4, 2, FixedRoundingMode::kHalfUp), 123);
    EXPECT_EQ(FixedDecimal::Rescale(scaled_4, 4, 2, FixedRoundingMode::kUp), 124);
    EXPECT_EQ(FixedDecimal::Rescale(scaled_4, 4, 2, FixedRoundingMode::kDown), 123);
}

TEST(FixedDecimalTest, ToLongDoubleRestoresScaledValue) {
    constexpr std::int64_t scaled = 987654;
    const auto restored = FixedDecimal::ToLongDouble(scaled, 3);
    EXPECT_NEAR(static_cast<double>(restored), 987.654, 1e-9);
}

}  // namespace quant_hft
