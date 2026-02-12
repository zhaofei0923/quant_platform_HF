#pragma once

#include <cstdint>

namespace quant_hft {

enum class FixedRoundingMode {
    kHalfUp = 0,
    kDown = 1,
    kUp = 2,
};

class FixedDecimal {
public:
    static std::int64_t ToScaled(long double value, int scale, FixedRoundingMode mode);
    static std::int64_t Rescale(std::int64_t scaled_value,
                                int from_scale,
                                int to_scale,
                                FixedRoundingMode mode);
    static long double ToLongDouble(std::int64_t scaled_value, int scale);
};

}  // namespace quant_hft
