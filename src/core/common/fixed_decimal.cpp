#include "quant_hft/core/fixed_decimal.h"

#include <algorithm>
#include <cmath>
#include <limits>

namespace quant_hft {
namespace {

std::int64_t Pow10(int scale) {
    if (scale <= 0) {
        return 1;
    }
    std::int64_t value = 1;
    for (int i = 0; i < scale; ++i) {
        if (value > std::numeric_limits<std::int64_t>::max() / 10) {
            return std::numeric_limits<std::int64_t>::max();
        }
        value *= 10;
    }
    return value;
}

std::int64_t ClampToInt64(long double value) {
    constexpr long double kMax = static_cast<long double>(std::numeric_limits<std::int64_t>::max());
    constexpr long double kMin = static_cast<long double>(std::numeric_limits<std::int64_t>::min());
    if (value >= kMax) {
        return std::numeric_limits<std::int64_t>::max();
    }
    if (value <= kMin) {
        return std::numeric_limits<std::int64_t>::min();
    }
    return static_cast<std::int64_t>(value);
}

std::int64_t RoundWithMode(long double value, FixedRoundingMode mode) {
    switch (mode) {
        case FixedRoundingMode::kDown:
            return ClampToInt64(std::floor(value));
        case FixedRoundingMode::kUp:
            return ClampToInt64(std::ceil(value));
        case FixedRoundingMode::kHalfUp:
        default:
            if (value >= 0) {
                return ClampToInt64(std::floor(value + 0.5L));
            }
            return ClampToInt64(std::ceil(value - 0.5L));
    }
}

}  // namespace

std::int64_t FixedDecimal::ToScaled(long double value, int scale, FixedRoundingMode mode) {
    const int safe_scale = std::max(0, scale);
    const auto factor = static_cast<long double>(Pow10(safe_scale));
    return RoundWithMode(value * factor, mode);
}

std::int64_t FixedDecimal::Rescale(std::int64_t scaled_value,
                                   int from_scale,
                                   int to_scale,
                                   FixedRoundingMode mode) {
    const int safe_from = std::max(0, from_scale);
    const int safe_to = std::max(0, to_scale);
    if (safe_from == safe_to) {
        return scaled_value;
    }
    const auto value = ToLongDouble(scaled_value, safe_from);
    return ToScaled(value, safe_to, mode);
}

long double FixedDecimal::ToLongDouble(std::int64_t scaled_value, int scale) {
    const int safe_scale = std::max(0, scale);
    const auto divisor = static_cast<long double>(Pow10(safe_scale));
    if (divisor <= 0.0L) {
        return 0.0L;
    }
    return static_cast<long double>(scaled_value) / divisor;
}

}  // namespace quant_hft
