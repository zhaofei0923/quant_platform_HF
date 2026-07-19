#include "quant_hft/services/trading_session_calendar.h"

#include <gtest/gtest.h>

namespace quant_hft {
namespace {

TEST(TradingSessionCalendarTest, SeparatesMarketEndpointFromOrderSession) {
    TradingSessionCalendar calendar;

    EXPECT_TRUE(calendar.IsOrderTime("SHFE", "hc2609", "14:59:59"));
    EXPECT_TRUE(calendar.IsMarketDataTime("SHFE", "hc2609", "15:00:00"));
    EXPECT_FALSE(calendar.IsOrderTime("SHFE", "hc2609", "15:00:00"));
    EXPECT_FALSE(calendar.IsMarketDataTime("SHFE", "hc2609", "15:00:01"));
}

TEST(TradingSessionCalendarTest, HandlesCrossMidnightSessionRemainingTime) {
    TradingSessionCalendar calendar;

    EXPECT_TRUE(calendar.IsOrderTime("SHFE", "au2608", "23:30:00"));
    EXPECT_EQ(calendar.RemainingSessionMillis("SHFE", "au2608", "23:30:00"), 3LL * 60 * 60 * 1000);
    EXPECT_TRUE(calendar.IsOrderTime("SHFE", "au2608", "02:29:59"));
    EXPECT_EQ(calendar.RemainingSessionMillis("SHFE", "au2608", "02:29:59"), 1000);
}

TEST(TradingSessionCalendarTest, RequiresFreshTickWhenHolidayLayerUnavailable) {
    TradingSessionCalendar calendar;
    calendar.SetTradingDayOpenResolver(
        [](const std::string&) -> std::optional<bool> { return std::nullopt; });
    // 2026-07-01 14:59:00 Asia/Shanghai.
    constexpr EpochNanos kNowNs = 1'782'889'140'000'000'000LL;

    const auto blocked =
        calendar.EvaluateOrderTime("SHFE", "hc2609", "20260701", kNowNs, 30'000, false);
    EXPECT_FALSE(blocked.open_allowed);
    EXPECT_EQ(blocked.reason, "calendar_unknown_without_fresh_tick");
}

TEST(TradingSessionCalendarTest, AppliesOpenSessionEndGuard) {
    TradingSessionCalendar calendar;
    // Use RemainingSessionMillis directly to avoid making the test depend on a wall-clock epoch.
    EXPECT_EQ(calendar.RemainingSessionMillis("DCE", "c2609", "14:59:30"), 30'000);
    EXPECT_EQ(calendar.RemainingSessionMillis("DCE", "c2609", "15:00:00"), 0);
}

}  // namespace
}  // namespace quant_hft
