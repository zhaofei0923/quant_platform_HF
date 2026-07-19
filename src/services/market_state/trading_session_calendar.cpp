#include "quant_hft/services/trading_session_calendar.h"

#include <algorithm>
#include <chrono>
#include <cstdio>
#include <ctime>

namespace quant_hft {

TradingSessionCalendar::TradingSessionCalendar(TradingSessionCalendarConfig config)
    : config_(std::move(config)),
      delegate_(BarAggregatorConfig{true, false, config_.trading_sessions_config_path,
                                    config_.use_default_session_fallback}) {}

void TradingSessionCalendar::SetTradingDayOpenResolver(TradingDayOpenResolver resolver) {
    trading_day_open_resolver_ = std::move(resolver);
}

bool TradingSessionCalendar::IsMarketDataTime(const std::string& exchange_id,
                                              const std::string& instrument_id,
                                              const std::string& update_time) const {
    MarketSnapshot snapshot;
    snapshot.exchange_id = exchange_id;
    snapshot.instrument_id = instrument_id;
    snapshot.trading_day = "19700101";
    snapshot.action_day = "19700101";
    snapshot.update_time = update_time;
    snapshot.last_price = 1.0;
    if (delegate_.ShouldProcessSnapshot(snapshot)) {
        return true;
    }

    int minute = 0;
    int second = 0;
    return ParseMinute(update_time, &minute, &second) && second == 0 &&
           delegate_.IsSessionEndMinute(exchange_id, instrument_id, update_time);
}

bool TradingSessionCalendar::IsOrderTime(const std::string& exchange_id,
                                         const std::string& instrument_id,
                                         const std::string& update_time) const {
    // ResolveSessionKey only returns a key for the half-open session interval.  In contrast,
    // ShouldProcessSnapshot deliberately accepts an exact endpoint tick so the market pipeline
    // can preserve the closing-auction Bar; that endpoint must never be order-eligible.
    return !delegate_.ResolveSessionKey(exchange_id, instrument_id, update_time).empty();
}

TradingSessionDecision TradingSessionCalendar::EvaluateOrderTime(
    const std::string& exchange_id, const std::string& instrument_id,
    const std::string& trading_day, EpochNanos now_ns, std::int64_t open_guard_ms,
    bool has_fresh_session_tick) const {
    TradingSessionDecision decision;
    const std::string update_time = TimeOfDay(now_ns);
    decision.in_session = IsOrderTime(exchange_id, instrument_id, update_time);
    decision.is_session_endpoint =
        !decision.in_session && IsMarketDataTime(exchange_id, instrument_id, update_time);
    if (!decision.in_session) {
        decision.reason = decision.is_session_endpoint ? "session_endpoint" : "outside_session";
        return decision;
    }

    if (trading_day_open_resolver_) {
        const std::optional<bool> open = trading_day_open_resolver_(trading_day);
        if (open.has_value() && !*open) {
            decision.reason = "trading_day_closed";
            return decision;
        }
        if (!open.has_value() && !has_fresh_session_tick) {
            decision.reason = "calendar_unknown_without_fresh_tick";
            return decision;
        }
    } else if (!has_fresh_session_tick) {
        decision.reason = "fresh_session_tick_required";
        return decision;
    }

    int ignored_minute = 0;
    int second = 0;
    (void)ParseMinute(update_time, &ignored_minute, &second);
    const auto dot = update_time.find('.');
    std::int32_t millisec = 0;
    if (dot != std::string::npos) {
        try {
            millisec = std::stoi(update_time.substr(dot + 1, 3));
        } catch (...) {
            millisec = 0;
        }
    }
    decision.remaining_session_ms =
        RemainingSessionMillis(exchange_id, instrument_id, update_time, millisec);
    if (decision.remaining_session_ms <= std::max<std::int64_t>(0, open_guard_ms)) {
        decision.reason = "session_end_guard";
        return decision;
    }
    decision.open_allowed = true;
    decision.reason = "allowed";
    return decision;
}

std::int64_t TradingSessionCalendar::RemainingSessionMillis(const std::string& exchange_id,
                                                            const std::string& instrument_id,
                                                            const std::string& update_time,
                                                            std::int32_t update_millisec) const {
    int current_minute = 0;
    int second = 0;
    if (!ParseMinute(update_time, &current_minute, &second) ||
        !IsOrderTime(exchange_id, instrument_id, update_time)) {
        return 0;
    }
    const std::string key = delegate_.ResolveSessionKey(exchange_id, instrument_id, update_time);
    int start_minute = 0;
    int end_minute = 0;
    if (!ParseSessionKey(key, &start_minute, &end_minute)) {
        return 0;
    }
    int remaining_minutes = 0;
    if (start_minute < end_minute) {
        remaining_minutes = end_minute - current_minute;
    } else if (current_minute >= start_minute) {
        remaining_minutes = (24 * 60 - current_minute) + end_minute;
    } else {
        remaining_minutes = end_minute - current_minute;
    }
    const std::int64_t elapsed_ms =
        static_cast<std::int64_t>(second) * 1000 + std::clamp(update_millisec, 0, 999);
    return std::max<std::int64_t>(
        0, static_cast<std::int64_t>(remaining_minutes) * 60'000 - elapsed_ms);
}

std::string TradingSessionCalendar::TimeOfDay(EpochNanos ts_ns) const {
    constexpr std::int64_t kNanosPerSecond = 1'000'000'000LL;
    std::time_t seconds = static_cast<std::time_t>(ts_ns / kNanosPerSecond);
    seconds += static_cast<std::time_t>(config_.timezone_offset_hours) * 60 * 60;
    std::tm tm{};
#if defined(_WIN32)
    gmtime_s(&tm, &seconds);
#else
    gmtime_r(&seconds, &tm);
#endif
    const int millisec = static_cast<int>((ts_ns % kNanosPerSecond) / 1'000'000);
    char buffer[20];
    std::snprintf(buffer, sizeof(buffer), "%02d:%02d:%02d.%03d", tm.tm_hour, tm.tm_min, tm.tm_sec,
                  std::max(0, millisec));
    return std::string(buffer);
}

bool TradingSessionCalendar::ParseMinute(const std::string& text, int* minute_of_day, int* second) {
    if (minute_of_day == nullptr || second == nullptr || text.size() < 5 || text[2] != ':') {
        return false;
    }
    int hour = 0;
    int minute = 0;
    int parsed_second = 0;
    if (std::sscanf(text.c_str(), "%2d:%2d:%2d", &hour, &minute, &parsed_second) < 2 || hour < 0 ||
        hour > 23 || minute < 0 || minute > 59 || parsed_second < 0 || parsed_second > 59) {
        return false;
    }
    *minute_of_day = hour * 60 + minute;
    *second = parsed_second;
    return true;
}

bool TradingSessionCalendar::ParseSessionKey(const std::string& key, int* start_minute,
                                             int* end_minute) {
    if (start_minute == nullptr || end_minute == nullptr) {
        return false;
    }
    const auto separator = key.find('-');
    if (separator == std::string::npos) {
        return false;
    }
    try {
        *start_minute = std::stoi(key.substr(0, separator));
        *end_minute = std::stoi(key.substr(separator + 1));
    } catch (...) {
        return false;
    }
    return *start_minute >= 0 && *start_minute < 24 * 60 && *end_minute >= 0 &&
           *end_minute < 24 * 60;
}

}  // namespace quant_hft
