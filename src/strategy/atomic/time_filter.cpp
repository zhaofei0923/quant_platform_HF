#include "quant_hft/strategy/atomic/time_filter.h"

#include <algorithm>
#include <stdexcept>

#include "atomic_param_parsing.h"
#include "quant_hft/strategy/atomic_factory.h"

namespace quant_hft {

void TimeFilter::Init(const AtomicParams& params) {
    id_ = atomic_internal::GetString(params, "id", "TimeFilter");
    start_hour_ = atomic_internal::GetInt(params, "start_hour", 0);
    end_hour_ = atomic_internal::GetInt(params, "end_hour", 0);
    const std::string timezone = atomic_internal::GetString(params, "timezone", "UTC");

    if (id_.empty()) {
        throw std::invalid_argument("TimeFilter id must not be empty");
    }
    if (start_hour_ < 0 || start_hour_ > 23 || end_hour_ < 0 || end_hour_ > 23) {
        throw std::invalid_argument("TimeFilter hours must be in [0, 23]");
    }

    if (timezone == "UTC") {
        timezone_offset_hours_ = 0;
    } else if (timezone == "Asia/Shanghai") {
        timezone_offset_hours_ = 8;
    } else {
        throw std::invalid_argument("TimeFilter only supports UTC and Asia/Shanghai");
    }
}

std::string TimeFilter::GetId() const { return id_; }

void TimeFilter::Reset() {}

bool TimeFilter::AllowOpening(EpochNanos now_ns) {
    constexpr std::int64_t kNanosPerSecond = 1000000000LL;
    constexpr std::int64_t kSecondsPerDay = 24LL * 60LL * 60LL;
    constexpr std::int64_t kSecondsPerHour = 60LL * 60LL;

    const std::int64_t utc_seconds = now_ns / kNanosPerSecond;
    const std::int64_t local_seconds =
        utc_seconds + static_cast<std::int64_t>(timezone_offset_hours_) * kSecondsPerHour;
    const std::int64_t seconds_into_day =
        (local_seconds % kSecondsPerDay + kSecondsPerDay) % kSecondsPerDay;
    const int hour = static_cast<int>(seconds_into_day / kSecondsPerHour);

    if (start_hour_ == end_hour_) {
        return true;
    }

    bool restricted = false;
    if (start_hour_ < end_hour_) {
        restricted = (hour >= start_hour_ && hour < end_hour_);
    } else {
        restricted = (hour >= start_hour_ || hour < end_hour_);
    }
    return !restricted;
}

QUANT_HFT_REGISTER_ATOMIC_STRATEGY("TimeFilter", TimeFilter);

}  // namespace quant_hft
