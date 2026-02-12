#pragma once

#include <cstdint>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "quant_hft/contracts/types.h"

namespace quant_hft {

struct BarSnapshot {
    std::string instrument_id;
    std::string exchange_id;
    std::string trading_day;
    std::string action_day;
    std::string minute;
    double open{0.0};
    double high{0.0};
    double low{0.0};
    double close{0.0};
    std::int64_t volume{0};
    EpochNanos ts_ns{0};
};

struct BarAggregatorConfig {
    bool filter_non_trading_ticks{true};
};

class BarAggregator {
public:
    explicit BarAggregator(BarAggregatorConfig config = {});

    bool ShouldProcessSnapshot(const MarketSnapshot& snapshot) const;
    std::vector<BarSnapshot> OnMarketSnapshot(const MarketSnapshot& snapshot);
    std::vector<BarSnapshot> Flush();
    static std::vector<BarSnapshot> AggregateFromOneMinute(
        const std::vector<BarSnapshot>& one_minute_bars,
        std::int32_t timeframe_minutes);

    static bool IsTradingSessionTime(const std::string& update_time);

private:
    struct MinuteBucket {
        bool initialized{false};
        std::string minute_key;
        std::int64_t last_cumulative_volume{0};
        BarSnapshot bar;
    };

    static bool ParseHhmm(const std::string& update_time, int* hhmm);
    static std::string ResolveExchangeId(const MarketSnapshot& snapshot);
    static std::string ResolveTradingDay(const MarketSnapshot& snapshot);
    static std::string ResolveActionDay(const MarketSnapshot& snapshot);
    static std::string BuildMinuteKey(const std::string& trading_day, const std::string& update_time);
    static EpochNanos ResolveTimestamp(const MarketSnapshot& snapshot);

    void ResetBucketLocked(MinuteBucket* bucket,
                           const MarketSnapshot& snapshot,
                           const std::string& exchange_id,
                           const std::string& trading_day,
                           const std::string& action_day,
                           const std::string& minute_key);

    BarAggregatorConfig config_;
    mutable std::mutex mutex_;
    std::unordered_map<std::string, MinuteBucket> buckets_;
};

}  // namespace quant_hft
