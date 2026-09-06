#pragma once

#include <cstdint>
#include <deque>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "quant_hft/contracts/types.h"
#include "quant_hft/services/bar_aggregator.h"
#include "quant_hft/services/market_state_detector.h"
#include "quant_hft/services/timeframe_state_fanout.h"

namespace quant_hft {

// Optional analysis-only transformation. Raw execution prices are never changed. A
// checkpoint includes both identity and state; Clone enables transactional restoration.
class IBarAnalysisTransform {
   public:
    using PersistenceState = std::unordered_map<std::string, std::string>;
    virtual ~IBarAnalysisTransform() = default;
    virtual std::string Identity() const = 0;
    virtual BarSnapshot Apply(const BarSnapshot& raw) = 0;
    virtual std::unique_ptr<IBarAnalysisTransform> Clone() const = 0;
    virtual bool SaveState(PersistenceState* out, std::string* error) const = 0;
    virtual bool LoadState(const PersistenceState& state, std::string* error) = 0;
};

struct MarketBarPipelineConfig {
    BarAggregatorConfig bar_aggregator;
    std::vector<std::int32_t> timeframes{5};
    MarketStateDetectorConfig detector;
    MarketStateDetectorConfigByProduct detector_by_product;
    std::shared_ptr<IBarAnalysisTransform> analysis_transform;
    std::int64_t tick_fingerprint_retention_ms{10'000};
    std::int32_t complete_five_minute_bars_to_reenable{2};
    std::size_t recent_complete_state_limit{128};
};

struct MarketBarPipelineResult {
    std::vector<BarSnapshot> one_minute_bars;
    std::vector<TimeframeStateEmission> timeframe_emissions;
    std::vector<std::string> critical_conflicts;
    bool duplicate_tick{false};
    bool late_tick{false};
    bool recovery_replay{false};
};

// The single production/backtest market-to-strategy pipeline.  It owns all pending 1m/5m
// state, detector warmup, exact-payload tick deduplication, canonical Bar idempotency and
// late-tick recovery suppression.  Callers must record the raw tick before invoking OnTick;
// duplicate and late ticks are intentionally retained in that raw evidence stream.
class MarketBarPipeline {
   public:
    using PersistenceState = std::unordered_map<std::string, std::string>;

    explicit MarketBarPipeline(MarketBarPipelineConfig config = {});

    MarketBarPipelineResult OnTick(const MarketSnapshot& snapshot);
    MarketBarPipelineResult AdvanceWatermark(EpochNanos now_ns);

    // Restores the v2 checkpoint before replaying the raw tail.  Replay may rebuild canonical
    // bars and indicators, but every replay emission is marked non-tradable.
    bool Recover(const PersistenceState& checkpoint, const std::vector<MarketSnapshot>& raw_tail,
                 MarketBarPipelineResult* result, std::string* error);

    // Advances only the safe watermark and returns an in-memory checkpoint containing every
    // still-pending bucket.  It never force-publishes a partial bar.
    bool PrepareShutdown(EpochNanos now_ns, PersistenceState* checkpoint,
                         MarketBarPipelineResult* result, std::string* error);

    bool SaveState(PersistenceState* out, std::string* error) const;
    bool LoadState(const PersistenceState& state, std::string* error);
    bool SaveCheckpointAtomically(const std::string& path, std::string* error) const;
    bool LoadCheckpointFile(const std::string& path, std::string* error);

    // Drops pre-gap market state and requires complete, conflict-free recovery bars.
    void MarkGap(const std::string& instrument_id);
    bool IsOpeningSuppressed(const std::string& instrument_id) const;
    std::vector<std::string> SuppressedInstruments() const;
    void ResetInstrument(const std::string& instrument_id, bool preserve_detector_state = true);
    std::vector<StateSnapshot7D> RecentCompleteStates(const std::string& instrument_id,
                                                      std::int32_t timeframe_minutes = 5,
                                                      std::size_t limit = 30) const;

   private:
    struct RecoveryState {
        std::int32_t consecutive_complete_five_minute_bars{0};
    };

    static std::string TickFingerprint(const MarketSnapshot& snapshot);
    static std::string BarKey(const BarSnapshot& bar, std::int32_t timeframe_minutes);
    static std::string BarFingerprint(const BarSnapshot& bar);
    static std::string EscapeCheckpointValue(const std::string& value);
    static bool UnescapeCheckpointValue(const std::string& value, std::string* out);

    void ResetInstrumentLocked(const std::string& instrument_id, bool preserve_detector_state);
    void PruneTickFingerprintsLocked(EpochNanos reference_ts_ns);
    MarketBarPipelineResult ProcessOneMinuteBarsLocked(std::vector<BarSnapshot> bars,
                                                       bool recovery_replay);
    bool AppendCanonicalOneMinuteLocked(const BarSnapshot& bar, MarketBarPipelineResult* result);
    void AppendCanonicalEmissionLocked(TimeframeStateEmission emission,
                                       MarketBarPipelineResult* result);
    void UpdateLateRecoveryLocked(const TimeframeStateEmission& emission);
    void MergeResult(MarketBarPipelineResult source, MarketBarPipelineResult* destination) const;
    bool SaveStateLocked(PersistenceState* out, std::string* error) const;
    bool LoadStateLocked(const PersistenceState& state, std::string* error);

    MarketBarPipelineConfig config_;
    mutable std::mutex mutex_;
    BarAggregator bar_aggregator_;
    TimeframeStateFanout timeframe_fanout_;
    EpochNanos last_watermark_ns_{0};
    bool replaying_{false};
    std::unordered_map<std::string, EpochNanos> tick_fingerprint_seen_ts_;
    std::unordered_map<std::string, std::string> canonical_bar_fingerprints_;
    std::unordered_map<std::string, RecoveryState> recovery_by_instrument_;
    std::unordered_map<std::string, std::deque<StateSnapshot7D>> recent_complete_states_;
};

}  // namespace quant_hft
