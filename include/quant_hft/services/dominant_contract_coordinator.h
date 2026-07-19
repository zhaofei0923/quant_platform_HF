#pragma once

#include <cstdint>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "quant_hft/contracts/types.h"

namespace quant_hft {

enum class DominantContractPhase : std::uint8_t {
    kSelecting = 0,
    kReady = 1,
    kPendingFlat = 2,
    kDraining = 3,
    kWarming = 4,
    kFault = 5,
};

const char* DominantContractPhaseName(DominantContractPhase phase);

struct DominantContractCoordinatorConfig {
    double min_lead_ratio{0.15};
    std::int32_t min_lead_windows{3};
    std::int64_t min_hold_ms{900'000};
    std::int64_t max_tick_age_ms{6'000};
    std::int32_t min_warmup_bars{30};
    bool require_complete_baseline{true};
};

struct DominantContractBrokerState {
    bool truth_complete{false};
    bool has_unmapped_position_or_order{false};
    std::int32_t position{0};
    std::int32_t frozen{0};
    std::size_t active_open_orders{0};
    std::size_t active_close_orders{0};
    std::unordered_set<std::string> held_instrument_ids;
};

struct DominantContractStatus {
    std::string trading_day;
    std::string product_id;
    std::string current_instrument_id;
    std::string candidate_instrument_id;
    DominantContractPhase phase{DominantContractPhase::kSelecting};
    std::uint64_t generation{1};
    std::string selection_metric{"open_interest"};
    double current_metric{0.0};
    double candidate_metric{0.0};
    double lead_ratio{0.0};
    std::int32_t lead_windows{0};
    std::size_t eligible_count{0};
    std::size_t baseline_count{0};
    std::int32_t broker_position{0};
    std::int32_t broker_frozen{0};
    std::size_t active_open_orders{0};
    std::size_t active_close_orders{0};
    std::int32_t warmup_observed_bars{0};
    std::int32_t warmup_required_bars{30};
    std::uint64_t generation_rejections{0};
    EpochNanos selected_at_ns{0};
    EpochNanos phase_started_ts_ns{0};
    EpochNanos updated_at_ns{0};
    std::string last_error;
};

enum class DominantContractAction : std::uint8_t {
    kNone = 0,
    kSelectInitial = 1,
    kEnterPendingFlat = 2,
    kCancelOpenOrders = 3,
    kBeginSwitch = 4,
};

struct DominantContractDecision {
    DominantContractAction action{DominantContractAction::kNone};
    std::string product_id;
    std::string previous_instrument_id;
    std::string candidate_instrument_id;
    std::uint64_t generation{0};
    std::string reason;
};

struct ContractSignalValidation {
    bool allowed{false};
    bool persist_pending_exit{false};
    std::string reason;
};

// Thread-safe product-level state machine for safe flat-only dominant-contract switching.
// It deliberately has no CTP or strategy dependencies; callers provide authoritative broker
// snapshots and execute returned actions only after their own callback barriers complete.
class DominantContractCoordinator {
   public:
    explicit DominantContractCoordinator(DominantContractCoordinatorConfig config = {});

    bool RegisterProduct(const std::string& product_id, const std::string& trading_day,
                         const std::string& current_instrument_id,
                         const std::vector<std::string>& eligible_instruments, EpochNanos now_ns,
                         std::string* error = nullptr);
    bool ReplaceEligibleInstruments(const std::string& product_id,
                                    const std::vector<std::string>& eligible_instruments,
                                    std::string* error = nullptr);
    bool RefreshTradingDay(const std::string& product_id, const std::string& trading_day,
                           const std::vector<std::string>& eligible_instruments, EpochNanos now_ns,
                           std::string* error = nullptr);
    void UpdateBaselineSnapshot(const std::string& product_id, const MarketSnapshot& snapshot);
    void UpdateLiveSnapshot(const MarketSnapshot& snapshot);
    void UpdateBrokerState(const std::string& product_id,
                           const DominantContractBrokerState& broker_state);

    DominantContractDecision Evaluate(const std::string& product_id, EpochNanos now_ns,
                                      bool selection_session_open);
    bool CommitInitialSelection(const std::string& product_id, const std::string& instrument_id,
                                EpochNanos now_ns, std::string* error = nullptr);
    bool BeginRecoveryWarmup(const std::string& product_id, const std::string& expected_current,
                             EpochNanos now_ns, std::uint64_t* generation,
                             std::string* error = nullptr);
    bool CommitRecoveryWarmup(const std::string& product_id, const std::string& instrument_id,
                              std::int32_t replayed_warmup_bars, std::int32_t required_warmup_bars,
                              EpochNanos now_ns, std::string* error = nullptr);
    bool BeginSwitch(const std::string& product_id, const std::string& expected_current,
                     const std::string& expected_candidate, EpochNanos now_ns,
                     std::uint64_t* generation, std::string* error = nullptr);
    bool CommitSwitch(const std::string& product_id, const std::string& instrument_id,
                      std::int32_t replayed_warmup_bars, std::int32_t required_warmup_bars,
                      EpochNanos now_ns, std::string* error = nullptr);
    bool AbortBeforeStrategyReset(const std::string& product_id, const std::string& reason,
                                  EpochNanos now_ns);
    bool RecordWarmupBar(const std::string& product_id, const std::string& instrument_id,
                         const std::string& canonical_bar_key, EpochNanos now_ns);
    void MarkFault(const std::string& product_id, const std::string& reason, EpochNanos now_ns);

    bool CanDispatchToStrategy(const std::string& instrument_id) const;
    bool IsCandidateInstrument(const std::string& instrument_id) const;
    ContractSignalValidation ValidateSignal(const SignalIntent& signal,
                                            std::int32_t broker_close_volume);
    std::optional<std::uint64_t> GenerationForInstrument(const std::string& instrument_id) const;
    std::optional<std::string> ProductForInstrument(const std::string& instrument_id) const;
    DominantContractStatus GetStatus(const std::string& product_id) const;
    std::vector<DominantContractStatus> GetAllStatuses() const;

    bool PersistStatusAtomically(const std::string& product_id, const std::string& output_path,
                                 std::string* error = nullptr) const;

   private:
    struct ProductState {
        DominantContractStatus status;
        std::unordered_set<std::string> eligible_instruments;
        std::unordered_map<std::string, MarketSnapshot> baseline_snapshots;
        std::unordered_map<std::string, MarketSnapshot> live_snapshots;
        DominantContractBrokerState broker;
        std::unordered_set<std::string> warmup_bar_keys;
    };

    static bool IsNewerSnapshot(const MarketSnapshot& candidate, const MarketSnapshot& current);
    bool IsFreshExecutableSnapshot(const MarketSnapshot& snapshot, EpochNanos now_ns) const;
    const MarketSnapshot* BestSnapshotLocked(const ProductState& state, std::string* metric) const;
    const MarketSnapshot* LatestSnapshotLocked(const ProductState& state,
                                               const std::string& instrument_id) const;
    void RebuildInstrumentIndexLocked();

    DominantContractCoordinatorConfig config_;
    mutable std::mutex mutex_;
    std::unordered_map<std::string, ProductState> products_;
    std::unordered_map<std::string, std::string> product_by_instrument_;
};

}  // namespace quant_hft
