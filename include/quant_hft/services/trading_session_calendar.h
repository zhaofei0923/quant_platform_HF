#pragma once

#include <cstdint>
#include <functional>
#include <optional>
#include <string>

#include "quant_hft/contracts/types.h"
#include "quant_hft/services/bar_aggregator.h"

namespace quant_hft {

struct TradingSessionCalendarConfig {
    std::string trading_sessions_config_path{"configs/trading_sessions.yaml"};
    bool use_default_session_fallback{true};
    int timezone_offset_hours{8};
};

struct TradingSessionDecision {
    bool in_session{false};
    bool is_session_endpoint{false};
    bool open_allowed{false};
    std::int64_t remaining_session_ms{0};
    std::string reason;
};

// Shared session semantics for market data and execution.  Intraday rules are sourced from the
// same parser as BarAggregator; an optional trading-day resolver can layer an authoritative
// exchange calendar on top without coupling this service to a particular database client.
class TradingSessionCalendar {
   public:
    using TradingDayOpenResolver =
        std::function<std::optional<bool>(const std::string& trading_day)>;

    explicit TradingSessionCalendar(TradingSessionCalendarConfig config = {});

    void SetTradingDayOpenResolver(TradingDayOpenResolver resolver);

    bool IsMarketDataTime(const std::string& exchange_id, const std::string& instrument_id,
                          const std::string& update_time) const;
    bool IsOrderTime(const std::string& exchange_id, const std::string& instrument_id,
                     const std::string& update_time) const;

    TradingSessionDecision EvaluateOrderTime(const std::string& exchange_id,
                                             const std::string& instrument_id,
                                             const std::string& trading_day, EpochNanos now_ns,
                                             std::int64_t open_guard_ms,
                                             bool has_fresh_session_tick) const;

    std::int64_t RemainingSessionMillis(const std::string& exchange_id,
                                        const std::string& instrument_id,
                                        const std::string& update_time,
                                        std::int32_t update_millisec = 0) const;

    std::string TimeOfDay(EpochNanos ts_ns) const;

   private:
    static bool ParseMinute(const std::string& text, int* minute_of_day, int* second);
    static bool ParseSessionKey(const std::string& key, int* start_minute, int* end_minute);

    TradingSessionCalendarConfig config_;
    BarAggregator delegate_;
    TradingDayOpenResolver trading_day_open_resolver_;
};

}  // namespace quant_hft
