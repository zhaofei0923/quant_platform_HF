#pragma once

#include <chrono>
#include <ctime>
#include <iomanip>
#include <sstream>
#include <stdexcept>
#include <string>

#include "quant_hft/contracts/types.h"

namespace quant_hft {

class Timestamp {
public:
    Timestamp() = default;
    explicit Timestamp(EpochNanos ns) : ns_(ns) {}

    static Timestamp FromSql(const std::string& text) {
        std::tm tm = {};
        tm.tm_isdst = 0;

        std::istringstream iss_datetime(text);
        iss_datetime >> std::get_time(&tm, "%Y-%m-%d %H:%M:%S");
        if (iss_datetime.fail()) {
            std::istringstream iss_date(text);
            iss_date >> std::get_time(&tm, "%Y-%m-%d");
            if (iss_date.fail()) {
                throw std::runtime_error("invalid timestamp format: " + text);
            }
            tm.tm_hour = 0;
            tm.tm_min = 0;
            tm.tm_sec = 0;
        }

        const std::time_t seconds = timegm(&tm);
        if (seconds < 0) {
            throw std::runtime_error("invalid timestamp value: " + text);
        }
        return Timestamp(static_cast<EpochNanos>(seconds) * 1'000'000'000);
    }

    static Timestamp Now() {
        const auto now = std::chrono::time_point_cast<std::chrono::nanoseconds>(
            std::chrono::system_clock::now());
        return Timestamp(now.time_since_epoch().count());
    }

    std::string ToSql() const {
        const auto seconds = static_cast<std::time_t>(ns_ / 1'000'000'000);
        std::tm tm = *gmtime(&seconds);
        std::ostringstream oss;
        oss << std::put_time(&tm, "%Y-%m-%d %H:%M:%S");
        return oss.str();
    }

    EpochNanos ToEpochNanos() const { return ns_; }

    bool operator>(const Timestamp& other) const { return ns_ > other.ns_; }
    bool operator<(const Timestamp& other) const { return ns_ < other.ns_; }
    bool operator>=(const Timestamp& other) const { return ns_ >= other.ns_; }
    bool operator<=(const Timestamp& other) const { return ns_ <= other.ns_; }
    bool operator==(const Timestamp& other) const { return ns_ == other.ns_; }
    bool operator!=(const Timestamp& other) const { return ns_ != other.ns_; }

private:
    EpochNanos ns_{0};
};

}  // namespace quant_hft
