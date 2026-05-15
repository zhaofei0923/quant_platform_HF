#pragma once

#include <cstdint>
#include <string>
#include <unordered_map>
#include <vector>

#include "quant_hft/contracts/types.h"
#include "quant_hft/services/bar_aggregator.h"
#include "quant_hft/services/market_state_detector.h"

namespace quant_hft {

struct TimeframeStateEmission {
    std::int32_t timeframe_minutes{1};
    BarSnapshot bar;
    StateSnapshot7D state;
};

class TimeframeStateFanout {
   public:
    using PersistenceState = std::unordered_map<std::string, std::string>;

    explicit TimeframeStateFanout(std::vector<std::int32_t> timeframes,
                                  MarketStateDetectorConfig detector_config = {});

    std::vector<TimeframeStateEmission> OnOneMinuteBar(const BarSnapshot& bar);
    std::vector<TimeframeStateEmission> Flush();
    void ResetInstrument(const std::string& instrument_id);

    bool SaveState(PersistenceState* out, std::string* error) const;
    bool LoadState(const PersistenceState& state, std::string* error);

    const std::vector<std::int32_t>& timeframes() const noexcept { return timeframes_; }

   private:
    struct Bucket {
        bool initialized{false};
        std::int32_t timeframe_minutes{1};
        std::string bucket_minute;
        BarSnapshot bar;
    };

    static bool ParseMinuteValue(const std::string& minute_key, std::string* trading_day,
                                 int* minute_of_day);
    static std::string FormatMinuteValue(const std::string& trading_day, int minute_of_day);
    static std::string BuildKey(const std::string& instrument_id, std::int32_t timeframe_minutes);
    static std::string BuildBucketMinute(const std::string& minute_key,
                                         std::int32_t timeframe_minutes);
    static void MergeBarIntoBucket(const BarSnapshot& bar, Bucket* bucket);
    TimeframeStateEmission BuildEmission(const BarSnapshot& bar, std::int32_t timeframe_minutes);
    MarketStateDetector& DetectorFor(const std::string& key);

    std::vector<std::int32_t> timeframes_;
    MarketStateDetectorConfig detector_config_;
    std::unordered_map<std::string, Bucket> buckets_;
    std::unordered_map<std::string, MarketStateDetector> detectors_;
};

}  // namespace quant_hft
