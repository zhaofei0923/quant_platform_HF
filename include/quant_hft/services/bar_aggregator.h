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
    bool is_backtest_mode{false};
    std::string trading_sessions_config_path{"configs/trading_sessions.yaml"};
    bool use_default_session_fallback{true};
};

class BarAggregator {
public:
    struct SessionInterval {
        int start_minute{0};
        int end_minute{0};
    };

    struct SessionRule {
        std::string instrument_prefix;
        std::string product;
        std::vector<SessionInterval> intervals;
    };

    explicit BarAggregator(BarAggregatorConfig config = {});

    bool ShouldProcessSnapshot(const MarketSnapshot& snapshot) const;
    std::vector<BarSnapshot> OnMarketSnapshot(const MarketSnapshot& snapshot);
    std::vector<BarSnapshot> Flush();
    void ResetInstrument(const std::string& instrument_id);
    std::string InferExchangeId(const std::string& instrument_id) const;
    bool IsInTradingSession(const std::string& exchange_id, const std::string& update_time) const;
    static std::vector<BarSnapshot> AggregateFromOneMinute(
        const std::vector<BarSnapshot>& one_minute_bars,
        std::int32_t timeframe_minutes);

private:
    struct MinuteBucket {
        bool initialized{false};
        std::string minute_key;
        std::int64_t last_cumulative_volume{0};
        BarSnapshot bar;
    };

    static bool ParseMinuteOfDay(const std::string& update_time, int* minute_of_day);
    static std::string ResolveExchangeId(const MarketSnapshot& snapshot);
    static std::string ResolveProductCode(const MarketSnapshot& snapshot);
    static std::string ResolveTradingDay(const MarketSnapshot& snapshot);
    static std::string ResolveActionDay(const MarketSnapshot& snapshot);
    static std::string BuildMinuteKey(const std::string& trading_day, const std::string& update_time);
    EpochNanos ResolveTimestamp(const MarketSnapshot& snapshot) const;
    bool IsInTradingSession(const std::string& exchange_id,
                            const std::string& instrument_id,
                            const std::string& product,
                            const std::string& update_time) const;
    void LoadTradingSessions();

    void ResetBucketLocked(MinuteBucket* bucket,
                           const MarketSnapshot& snapshot,
                           const std::string& exchange_id,
                           const std::string& trading_day,
                           const std::string& action_day,
                           const std::string& minute_key);

    BarAggregatorConfig config_;
    mutable std::mutex mutex_;
    std::unordered_map<std::string, MinuteBucket> buckets_;
    std::unordered_map<std::string, std::vector<SessionRule>> session_rules_by_exchange_;
};

}  // namespace quant_hft
