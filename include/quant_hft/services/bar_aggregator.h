#pragma once

#include <cstdint>
#include <map>
#include <mutex>
#include <string>
#include <unordered_map>
#include <unordered_set>
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
    double analysis_open{0.0};
    double analysis_high{0.0};
    double analysis_low{0.0};
    double analysis_close{0.0};
    double analysis_price_offset{0.0};
    std::int64_t volume{0};
    EpochNanos ts_ns{0};
    // Event-time finality metadata. Existing callers may leave these fields at
    // their defaults; live aggregation always fills them before emission.
    EpochNanos period_end_ts_ns{0};
    EpochNanos finalized_ts_ns{0};
    std::int32_t expected_source_bars{1};
    std::int32_t observed_source_bars{1};
    bool is_complete{true};
    bool is_session_endpoint{false};
    bool strategy_eligible{true};
    // Additional integrity flags are append-only.  They distinguish a structurally complete
    // bar that is temporarily suppressed from trading from genuinely incomplete market data.
    bool volume_complete{true};
    bool has_conflict{false};
    bool is_recovery_replay{false};
};

struct BarAggregatorConfig {
    bool filter_non_trading_ticks{true};
    bool is_backtest_mode{false};
    std::string trading_sessions_config_path{"configs/trading_sessions.yaml"};
    bool use_default_session_fallback{true};
    std::int32_t allowed_lateness_ms{3500};
};

class BarAggregator {
   public:
    using PersistenceState = std::unordered_map<std::string, std::string>;
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
    // Finalize all minute buckets whose event-time period end is no later than
    // (now_ts_ns - allowed_lateness). This is the production-safe timer API.
    std::vector<BarSnapshot> AdvanceWatermark(EpochNanos now_ts_ns);
    std::vector<BarSnapshot> FlushFinished(
        const std::unordered_map<std::string, EpochNanos>& instrument_last_tick_ts_ns,
        EpochNanos cutoff_ts_ns);
    std::vector<BarSnapshot> FlushSessionEndBars(
        const std::unordered_map<std::string, EpochNanos>& instrument_last_tick_ts_ns,
        EpochNanos cutoff_ts_ns);
    std::vector<BarSnapshot> Flush();
    // Production shutdown must persist or discard pending buckets instead of
    // publishing them as completed market data. Flush() remains for backtest
    // end-of-input compatibility.
    void DiscardPending();
    bool SaveState(PersistenceState* out, std::string* error) const;
    bool LoadState(const PersistenceState& state, std::string* error);
    bool IsFinalizedSnapshot(const MarketSnapshot& snapshot) const;
    void ResetInstrument(const std::string& instrument_id);
    std::string InferExchangeId(const std::string& instrument_id) const;
    bool IsInTradingSession(const std::string& exchange_id, const std::string& update_time) const;
    bool IsSessionEndMinute(const std::string& exchange_id, const std::string& instrument_id,
                            const std::string& update_time) const;
    std::string ResolveSessionKey(const std::string& exchange_id, const std::string& instrument_id,
                                  const std::string& update_time) const;
    int ResolveSessionOrder(const std::string& exchange_id, const std::string& instrument_id,
                            const std::string& update_time) const;
    static std::vector<BarSnapshot> AggregateFromOneMinute(
        const std::vector<BarSnapshot>& one_minute_bars, std::int32_t timeframe_minutes);

   private:
    struct MinuteBucket {
        bool initialized{false};
        std::string minute_key;
        std::int64_t first_cumulative_volume{0};
        std::int64_t max_cumulative_volume{0};
        EpochNanos first_event_ts_ns{0};
        EpochNanos last_event_ts_ns{0};
        std::uint64_t first_arrival_seq{0};
        std::uint64_t last_arrival_seq{0};
        EpochNanos period_start_ts_ns{0};
        EpochNanos period_end_ts_ns{0};
        BarSnapshot bar;
    };

    struct InstrumentVolumeState {
        std::string trading_day;
        bool initialized{false};
        bool baseline_complete{false};
        std::int64_t cumulative_volume{0};
    };

    static bool ParseMinuteOfDay(const std::string& update_time, int* minute_of_day);
    std::string ResolveExchangeId(const MarketSnapshot& snapshot) const;
    static std::string ResolveProductCode(const MarketSnapshot& snapshot);
    static std::string ResolveTradingDay(const MarketSnapshot& snapshot);
    static std::string ResolveActionDay(const MarketSnapshot& snapshot);
    static std::string BuildMinuteKey(const std::string& trading_day,
                                      const std::string& update_time);
    static EpochNanos ResolveEventTimestamp(const MarketSnapshot& snapshot);
    static EpochNanos ResolvePhysicalMinuteStart(const MarketSnapshot& snapshot);
    EpochNanos ResolveTimestamp(const MarketSnapshot& snapshot) const;
    bool IsInTradingSession(const std::string& exchange_id, const std::string& instrument_id,
                            const std::string& product, const std::string& update_time) const;
    bool ResolveSessionInterval(const std::string& exchange_id, const std::string& instrument_id,
                                const std::string& product, const std::string& update_time,
                                SessionInterval* interval) const;
    bool IsExactSessionEndTime(const std::string& exchange_id, const std::string& instrument_id,
                               const std::string& product, const std::string& update_time) const;
    bool IsSessionEndMinuteKey(const std::string& exchange_id, const std::string& instrument_id,
                               const std::string& minute_key) const;
    void LoadTradingSessions();

    void ResetBucketLocked(MinuteBucket* bucket, const MarketSnapshot& snapshot,
                           const std::string& exchange_id, const std::string& trading_day,
                           const std::string& action_day, const std::string& minute_key,
                           EpochNanos event_ts_ns, EpochNanos period_start_ts_ns,
                           bool is_session_endpoint, std::uint64_t arrival_seq);
    std::vector<BarSnapshot> FinalizeReadyLocked(EpochNanos watermark_ts_ns,
                                                 EpochNanos finalized_ts_ns,
                                                 const std::string* instrument_filter = nullptr);
    BarSnapshot FinalizeBucketLocked(const std::string& instrument_id, MinuteBucket* bucket,
                                     EpochNanos finalized_ts_ns);
    void PruneClosedBoundaryMinutesLocked(const std::string& instrument_id,
                                          const std::string& trading_day);
    void EraseClosedBoundaryMinutesLocked(const std::string& instrument_id);

    BarAggregatorConfig config_;
    mutable std::mutex mutex_;
    std::unordered_map<std::string, std::map<EpochNanos, MinuteBucket>> buckets_;
    std::unordered_map<std::string, InstrumentVolumeState> volume_states_;
    std::unordered_map<std::string, EpochNanos> max_event_ts_by_instrument_;
    std::unordered_set<std::string> finalized_minute_keys_;
    std::uint64_t next_arrival_seq_{1};
    std::unordered_set<std::string> closed_session_boundary_minutes_;
    std::unordered_map<std::string, std::vector<SessionRule>> session_rules_by_exchange_;
};

}  // namespace quant_hft
