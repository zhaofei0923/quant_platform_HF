#include "quant_hft/services/market_state_detector.h"

#include <gtest/gtest.h>

#include <limits>
#include <stdexcept>

namespace quant_hft {
namespace {

TEST(MarketStateDetectorTest, ThrowsWhenConfigIsInvalid) {
    MarketStateDetectorConfig config;
    config.adx_period = 0;
    EXPECT_THROW((void)MarketStateDetector(config), std::invalid_argument);
}

TEST(MarketStateDetectorTest, ReturnsUnknownUntilIndicatorsReady) {
    MarketStateDetectorConfig config;
    config.adx_period = 3;
    config.atr_period = 3;
    config.kama_er_period = 3;
    config.min_bars_for_flat = 1;
    MarketStateDetector detector(config);

    detector.Update(100.5, 99.5, 100.0);
    detector.Update(101.5, 100.5, 101.0);
    detector.Update(102.5, 101.5, 102.0);
    detector.Update(103.5, 102.5, 103.0);
    EXPECT_EQ(detector.GetRegime(), MarketRegime::kUnknown);

    detector.Update(104.5, 103.5, 104.0);
    EXPECT_NE(detector.GetRegime(), MarketRegime::kUnknown);
    ASSERT_TRUE(detector.GetADX().has_value());
}

TEST(MarketStateDetectorTest, DetectsFlatRegimeWhenAtrRatioIsTiny) {
    MarketStateDetectorConfig config;
    config.adx_period = 3;
    config.atr_period = 3;
    config.kama_er_period = 3;
    config.atr_flat_ratio = 0.001;
    config.min_bars_for_flat = 3;
    MarketStateDetector detector(config);

    for (int i = 0; i < 10; ++i) {
        detector.Update(100.0, 100.0, 100.0);
    }

    EXPECT_EQ(detector.GetRegime(), MarketRegime::kFlat);
    ASSERT_TRUE(detector.GetATRRatio().has_value());
    EXPECT_NEAR(*detector.GetATRRatio(), 0.0, 1e-12);
}

TEST(MarketStateDetectorTest, DetectsStrongTrendAndRanging) {
    MarketStateDetectorConfig strong_cfg;
    strong_cfg.adx_period = 3;
    strong_cfg.atr_period = 3;
    strong_cfg.kama_er_period = 3;
    strong_cfg.adx_strong_threshold = 30.0;
    strong_cfg.adx_weak_lower = 20.0;
    strong_cfg.adx_weak_upper = 30.0;
    strong_cfg.kama_er_strong = 0.6;
    strong_cfg.kama_er_weak_lower = 0.3;
    strong_cfg.atr_flat_ratio = 0.001;
    strong_cfg.min_bars_for_flat = 1;

    MarketStateDetector strong_detector(strong_cfg);
    for (int i = 0; i < 12; ++i) {
        const double close = 100.0 + static_cast<double>(i);
        strong_detector.Update(close + 1.0, close - 1.0, close);
    }
    EXPECT_EQ(strong_detector.GetRegime(), MarketRegime::kStrongTrend);
    ASSERT_TRUE(strong_detector.GetKAMAER().has_value());
    EXPECT_GT(*strong_detector.GetKAMAER(), strong_cfg.kama_er_strong);

    MarketStateDetector ranging_detector(strong_cfg);
    const double closes[] = {100.0, 102.0, 99.0, 103.0, 98.0, 102.0,
                             99.5,  101.5, 98.5, 102.5, 99.0, 101.0};
    for (double close : closes) {
        ranging_detector.Update(close + 2.0, close - 2.0, close);
    }
    EXPECT_EQ(ranging_detector.GetRegime(), MarketRegime::kRanging);
}

TEST(MarketStateDetectorTest, IgnoresNonFiniteInputAndSupportsReset) {
    MarketStateDetectorConfig config;
    config.adx_period = 3;
    config.atr_period = 3;
    config.kama_er_period = 3;
    config.min_bars_for_flat = 1;

    MarketStateDetector detector(config);
    detector.Update(101.0, 99.0, 100.0);
    detector.Update(102.0, 100.0, 101.0);
    detector.Update(103.0, 101.0, 102.0);
    detector.Update(104.0, 102.0, 103.0);
    detector.Update(105.0, 103.0, 104.0);

    const MarketRegime before = detector.GetRegime();
    const auto adx_before = detector.GetADX();
    detector.Update(std::numeric_limits<double>::quiet_NaN(), 100.0, 100.0);
    EXPECT_EQ(detector.GetRegime(), before);
    EXPECT_EQ(detector.GetADX(), adx_before);

    detector.Reset();
    EXPECT_EQ(detector.GetRegime(), MarketRegime::kUnknown);
    EXPECT_FALSE(detector.GetADX().has_value());
    EXPECT_FALSE(detector.GetKAMA().has_value());
    EXPECT_FALSE(detector.GetATR().has_value());
    EXPECT_FALSE(detector.GetKAMAER().has_value());
    EXPECT_FALSE(detector.GetATRRatio().has_value());
}

TEST(MarketStateDetectorTest, ExposesKamaAndAtrWhenReady) {
    MarketStateDetectorConfig config;
    config.adx_period = 3;
    config.atr_period = 3;
    config.kama_er_period = 3;
    config.min_bars_for_flat = 1;

    MarketStateDetector detector(config);
    EXPECT_FALSE(detector.GetKAMA().has_value());
    EXPECT_FALSE(detector.GetATR().has_value());

    for (int i = 0; i < 8; ++i) {
        const double close = 100.0 + static_cast<double>(i);
        detector.Update(close + 1.0, close - 1.0, close);
    }

    ASSERT_TRUE(detector.GetKAMA().has_value());
    ASSERT_TRUE(detector.GetATR().has_value());
    EXPECT_GT(*detector.GetKAMA(), 0.0);
    EXPECT_GT(*detector.GetATR(), 0.0);

    detector.Reset();
    EXPECT_FALSE(detector.GetKAMA().has_value());
    EXPECT_FALSE(detector.GetATR().has_value());
}

}  // namespace
}  // namespace quant_hft
