#pragma once

#include <cstdint>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "quant_hft/contracts/types.h"
#include "quant_hft/services/bar_aggregator.h"
#include "quant_hft/services/market_state_detector.h"

namespace quant_hft {

struct TimeframeStateEmission {
    std::int32_t timeframe_minutes{1};
    BarSnapshot bar;
    StateSnapshot7D state;
    // False means the bar is retained for audit/recording only. Incomplete,
    // conflicting, and closing-auction endpoint buckets never advance market
    // state indicators or strategy logic.
    bool strategy_eligible{true};
};

class TimeframeStateFanout {
   public:
    using PersistenceState = std::unordered_map<std::string, std::string>;

    explicit TimeframeStateFanout(
        std::vector<std::int32_t> timeframes, MarketStateDetectorConfig detector_config = {},
        MarketStateDetectorConfigByProduct detector_config_by_product = {});

    std::vector<TimeframeStateEmission> OnOneMinuteBar(const BarSnapshot& bar);
    // Finalize buckets whose period_end_ts_ns is no later than the supplied
    // event-time watermark. Missing source minutes produce non-tradable bars.
    std::vector<TimeframeStateEmission> AdvanceWatermark(EpochNanos watermark_ts_ns);
    std::vector<TimeframeStateEmission> Flush();
    // Production-safe shutdown API: pending buckets are discarded rather than
    // published as complete strategy inputs.
    void DiscardPending();
    // Flush only the pending buckets for a specific instrument. Unlike Flush(),
    // other instruments' in-progress buckets are preserved. Use this at session
    // boundaries to emit a single instrument's section-end bar without prematurely
    // flushing incomplete buckets for other instruments.
    std::vector<TimeframeStateEmission> FlushInstrument(const std::string& instrument_id);
    void ResetInstrument(const std::string& instrument_id);
    void ResetInstrumentBuckets(const std::string& instrument_id);

    bool SaveState(PersistenceState* out, std::string* error) const;
    bool LoadState(const PersistenceState& state, std::string* error);

    const std::vector<std::int32_t>& timeframes() const noexcept { return timeframes_; }

   private:
    struct Bucket {
        bool initialized{false};
        std::int32_t timeframe_minutes{1};
        std::string bucket_minute;
        EpochNanos period_end_ts_ns{0};
        bool conflicting_duplicate{false};
        bool has_incomplete_source{false};
        bool has_non_tradable_source{false};
        std::unordered_map<std::string, std::string> source_fingerprint_by_minute;
        BarSnapshot bar;
    };

    static bool ParseMinuteValue(const std::string& minute_key, std::string* trading_day,
                                 int* minute_of_day);
    static std::string FormatMinuteValue(const std::string& trading_day, int minute_of_day);
    static std::string BuildKey(const std::string& instrument_id, std::int32_t timeframe_minutes);
    static std::string BuildBucketMinute(const std::string& minute_key,
                                         std::int32_t timeframe_minutes);
    static EpochNanos ResolveBucketPeriodEnd(const BarSnapshot& bar,
                                             std::int32_t timeframe_minutes);
    static std::string BarFingerprint(const BarSnapshot& bar);
    static bool IsTerminalSlot(const std::string& minute_key, std::int32_t timeframe_minutes);
    static void MergeBarIntoBucket(const BarSnapshot& bar, Bucket* bucket);
    TimeframeStateEmission BuildEmission(const BarSnapshot& bar, std::int32_t timeframe_minutes,
                                         bool strategy_eligible);
    TimeframeStateEmission FinalizeBucket(Bucket* bucket);
    MarketStateDetector& DetectorFor(const std::string& instrument_id,
                                     std::int32_t timeframe_minutes);

    std::vector<std::int32_t> timeframes_;
    MarketStateDetectorConfig detector_config_;
    MarketStateDetectorConfigByProduct detector_config_by_product_;
    std::unordered_map<std::string, Bucket> buckets_;
    std::unordered_map<std::string, MarketStateDetector> detectors_;
    std::unordered_set<std::string> finalized_bucket_keys_;
};

}  // namespace quant_hft
