#pragma once

#include <array>

namespace quant_hft::test_data {

constexpr int kAdxPeriod = 3;

constexpr std::array<double, 12> kAdxHigh{
    10.0,
    10.5,
    10.8,
    11.0,
    11.2,
    11.5,
    11.8,
    11.7,
    11.3,
    11.0,
    10.9,
    10.8,
};

constexpr std::array<double, 12> kAdxLow{
    9.0,
    9.2,
    10.0,
    10.4,
    10.7,
    10.9,
    11.2,
    11.0,
    10.8,
    10.5,
    10.3,
    10.1,
};

constexpr std::array<double, 12> kAdxClose{
    9.5,
    10.2,
    10.7,
    10.9,
    11.1,
    11.4,
    11.6,
    11.1,
    10.9,
    10.7,
    10.5,
    10.2,
};

// Reference values generated offline from TA-Lib style Wilder smoothing.
constexpr std::array<double, 12> kExpectedPlusDi{
    -1.0,
    -1.0,
    25.80645161290324,
    27.49999999999999,
    30.243902439024353,
    35.83916083916085,
    40.06134969325154,
    26.325337633541643,
    19.25259821626006,
    13.722451338359306,
    9.045574179249215,
    5.666112600972376,
};

constexpr std::array<double, 12> kExpectedMinusDi{
    -1.0,
    -1.0,
    0.0,
    0.0,
    0.0,
    0.0,
    0.0,
    9.79641201370688,
    17.91110783518826,
    30.000788042764448,
    31.13658401440639,
    30.178231997158285,
};

constexpr std::array<double, 12> kExpectedAdx{
    -1.0,
    -1.0,
    -1.0,
    -1.0,
    100.0,
    100.0,
    100.0,
    81.91964285714292,
    55.81632147107169,
    49.6210141538395,
    51.406396462236735,
    57.06589781548314,
};

}  // namespace quant_hft::test_data
