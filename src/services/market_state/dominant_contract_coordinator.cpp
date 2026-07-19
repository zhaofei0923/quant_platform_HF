#include "quant_hft/services/dominant_contract_coordinator.h"

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <cmath>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <sstream>

#if !defined(_WIN32)
#include <fcntl.h>
#include <unistd.h>
#endif

namespace quant_hft {
namespace {

constexpr EpochNanos kNanosPerMillisecond = 1'000'000;

void SetError(std::string* error, const std::string& value) {
    if (error != nullptr) {
        *error = value;
    }
}

std::string JsonEscape(const std::string& value) {
    std::ostringstream out;
    for (const unsigned char ch : value) {
        switch (ch) {
            case '\\':
                out << "\\\\";
                break;
            case '"':
                out << "\\\"";
                break;
            case '\n':
                out << "\\n";
                break;
            case '\r':
                out << "\\r";
                break;
            case '\t':
                out << "\\t";
                break;
            default:
                if (ch < 0x20) {
                    out << "\\u" << std::hex << std::setw(4) << std::setfill('0')
                        << static_cast<int>(ch) << std::dec;
                } else {
                    out << static_cast<char>(ch);
                }
        }
    }
    return out.str();
}

#if !defined(_WIN32)
bool FsyncPath(const std::filesystem::path& path, bool directory, std::string* error) {
    const int flags = directory ? (O_RDONLY | O_DIRECTORY) : O_RDONLY;
    const int fd = ::open(path.c_str(), flags);
    if (fd < 0) {
        SetError(error, "open for fsync failed: " + std::string(std::strerror(errno)));
        return false;
    }
    const int rc = ::fsync(fd);
    const int saved_errno = errno;
    ::close(fd);
    if (rc != 0) {
        SetError(error, "fsync failed: " + std::string(std::strerror(saved_errno)));
        return false;
    }
    return true;
}
#endif

}  // namespace

const char* DominantContractPhaseName(DominantContractPhase phase) {
    switch (phase) {
        case DominantContractPhase::kSelecting:
            return "selecting";
        case DominantContractPhase::kReady:
            return "ready";
        case DominantContractPhase::kPendingFlat:
            return "pending_flat";
        case DominantContractPhase::kDraining:
            return "draining";
        case DominantContractPhase::kWarming:
            return "warming";
        case DominantContractPhase::kFault:
            return "fault";
    }
    return "fault";
}

DominantContractCoordinator::DominantContractCoordinator(DominantContractCoordinatorConfig config)
    : config_(std::move(config)) {
    config_.min_lead_ratio = std::max(0.0, config_.min_lead_ratio);
    config_.min_lead_windows = std::max(1, config_.min_lead_windows);
    config_.min_hold_ms = std::max<std::int64_t>(0, config_.min_hold_ms);
    config_.max_tick_age_ms = std::max<std::int64_t>(1, config_.max_tick_age_ms);
    config_.min_warmup_bars = std::max(1, config_.min_warmup_bars);
}

bool DominantContractCoordinator::RegisterProduct(
    const std::string& product_id, const std::string& trading_day,
    const std::string& current_instrument_id, const std::vector<std::string>& eligible_instruments,
    EpochNanos now_ns, std::string* error) {
    if (product_id.empty() || trading_day.empty() || eligible_instruments.empty()) {
        SetError(error, "product_id, trading_day and eligible instruments are required");
        return false;
    }
    std::unordered_set<std::string> unique;
    for (const auto& instrument_id : eligible_instruments) {
        if (instrument_id.empty() || !unique.insert(instrument_id).second) {
            SetError(error, "eligible instrument ids must be non-empty and unique");
            return false;
        }
    }
    std::lock_guard<std::mutex> lock(mutex_);
    ProductState state;
    state.status.trading_day = trading_day;
    state.status.product_id = product_id;
    state.status.current_instrument_id = current_instrument_id;
    // A non-empty current instrument at registration time is broker-recovered state, not a
    // trusted dominant selection. Keep it close-only until fresh candidate data confirms that
    // it is still dominant (or the flat-only switch transaction completes).
    state.status.phase = current_instrument_id.empty() ? DominantContractPhase::kSelecting
                                                       : DominantContractPhase::kPendingFlat;
    state.status.generation = 1;
    state.status.eligible_count = unique.size();
    state.status.warmup_required_bars = config_.min_warmup_bars;
    state.status.selected_at_ns = now_ns;
    state.status.phase_started_ts_ns = now_ns;
    state.status.updated_at_ns = now_ns;
    state.eligible_instruments = std::move(unique);
    products_[product_id] = std::move(state);
    RebuildInstrumentIndexLocked();
    return true;
}

bool DominantContractCoordinator::ReplaceEligibleInstruments(
    const std::string& product_id, const std::vector<std::string>& eligible_instruments,
    std::string* error) {
    std::unordered_set<std::string> unique;
    for (const auto& instrument_id : eligible_instruments) {
        if (instrument_id.empty() || !unique.insert(instrument_id).second) {
            SetError(error, "eligible instrument ids must be non-empty and unique");
            return false;
        }
    }
    if (unique.empty()) {
        SetError(error, "eligible instruments must not be empty");
        return false;
    }
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = products_.find(product_id);
    if (it == products_.end()) {
        SetError(error, "product is not registered");
        return false;
    }
    it->second.eligible_instruments = std::move(unique);
    it->second.status.eligible_count = it->second.eligible_instruments.size();
    for (auto snapshot_it = it->second.baseline_snapshots.begin();
         snapshot_it != it->second.baseline_snapshots.end();) {
        if (it->second.eligible_instruments.count(snapshot_it->first) == 0U) {
            snapshot_it = it->second.baseline_snapshots.erase(snapshot_it);
        } else {
            ++snapshot_it;
        }
    }
    it->second.status.baseline_count = it->second.baseline_snapshots.size();
    RebuildInstrumentIndexLocked();
    return true;
}

bool DominantContractCoordinator::RefreshTradingDay(
    const std::string& product_id, const std::string& trading_day,
    const std::vector<std::string>& eligible_instruments, EpochNanos now_ns, std::string* error) {
    if (trading_day.empty() || eligible_instruments.empty()) {
        SetError(error, "trading day and eligible instruments are required");
        return false;
    }
    std::unordered_set<std::string> unique;
    for (const auto& instrument_id : eligible_instruments) {
        if (instrument_id.empty() || !unique.insert(instrument_id).second) {
            SetError(error, "eligible instrument ids must be non-empty and unique");
            return false;
        }
    }
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = products_.find(product_id);
    if (it == products_.end()) {
        SetError(error, "product is not registered");
        return false;
    }
    auto& state = it->second;
    auto& status = state.status;
    if (status.phase == DominantContractPhase::kDraining ||
        status.phase == DominantContractPhase::kWarming) {
        SetError(error, "trading day changed during contract switch");
        return false;
    }
    state.eligible_instruments = std::move(unique);
    state.baseline_snapshots.clear();
    state.live_snapshots.clear();
    state.warmup_bar_keys.clear();
    status.trading_day = trading_day;
    status.eligible_count = state.eligible_instruments.size();
    status.baseline_count = 0;
    status.candidate_instrument_id.clear();
    status.lead_windows = 0;
    status.lead_ratio = 0.0;
    status.current_metric = 0.0;
    status.candidate_metric = 0.0;
    status.generation_rejections = 0;
    status.phase = status.current_instrument_id.empty() ? DominantContractPhase::kSelecting
                                                        : DominantContractPhase::kPendingFlat;
    status.phase_started_ts_ns = now_ns;
    status.updated_at_ns = now_ns;
    status.last_error.clear();
    ++status.generation;
    RebuildInstrumentIndexLocked();
    return true;
}

void DominantContractCoordinator::UpdateBaselineSnapshot(const std::string& product_id,
                                                         const MarketSnapshot& snapshot) {
    if (snapshot.instrument_id.empty()) {
        return;
    }
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = products_.find(product_id);
    if (it == products_.end() ||
        it->second.eligible_instruments.count(snapshot.instrument_id) == 0U ||
        (!snapshot.trading_day.empty() && snapshot.trading_day != it->second.status.trading_day)) {
        return;
    }
    auto& stored = it->second.baseline_snapshots[snapshot.instrument_id];
    if (stored.instrument_id.empty() || IsNewerSnapshot(snapshot, stored)) {
        stored = snapshot;
    }
    it->second.status.baseline_count = it->second.baseline_snapshots.size();
}

void DominantContractCoordinator::UpdateLiveSnapshot(const MarketSnapshot& snapshot) {
    if (snapshot.instrument_id.empty()) {
        return;
    }
    std::lock_guard<std::mutex> lock(mutex_);
    const auto product_it = product_by_instrument_.find(snapshot.instrument_id);
    if (product_it == product_by_instrument_.end()) {
        return;
    }
    auto state_it = products_.find(product_it->second);
    if (state_it == products_.end()) {
        return;
    }
    auto& stored = state_it->second.live_snapshots[snapshot.instrument_id];
    if (stored.instrument_id.empty() || IsNewerSnapshot(snapshot, stored)) {
        stored = snapshot;
    }
}

void DominantContractCoordinator::UpdateBrokerState(
    const std::string& product_id, const DominantContractBrokerState& broker_state) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = products_.find(product_id);
    if (it == products_.end()) {
        return;
    }
    it->second.broker = broker_state;
    const auto& current_instrument_id = it->second.status.current_instrument_id;
    const bool held_outside_current = !current_instrument_id.empty() &&
                                      std::any_of(it->second.broker.held_instrument_ids.begin(),
                                                  it->second.broker.held_instrument_ids.end(),
                                                  [&](const std::string& instrument_id) {
                                                      return instrument_id != current_instrument_id;
                                                  });
    if (it->second.broker.held_instrument_ids.size() > 1U || held_outside_current) {
        it->second.broker.has_unmapped_position_or_order = true;
    }
    auto& status = it->second.status;
    status.broker_position = broker_state.position;
    status.broker_frozen = broker_state.frozen;
    status.active_open_orders = broker_state.active_open_orders;
    status.active_close_orders = broker_state.active_close_orders;
    RebuildInstrumentIndexLocked();
}

DominantContractDecision DominantContractCoordinator::Evaluate(const std::string& product_id,
                                                               EpochNanos now_ns,
                                                               bool selection_session_open) {
    std::lock_guard<std::mutex> lock(mutex_);
    DominantContractDecision decision;
    decision.product_id = product_id;
    auto it = products_.find(product_id);
    if (it == products_.end()) {
        decision.reason = "product_not_registered";
        return decision;
    }
    auto& state = it->second;
    auto& status = state.status;
    status.updated_at_ns = now_ns;
    status.baseline_count = state.baseline_snapshots.size();
    status.eligible_count = state.eligible_instruments.size();
    decision.previous_instrument_id = status.current_instrument_id;
    decision.generation = status.generation;

    if (status.phase == DominantContractPhase::kFault ||
        status.phase == DominantContractPhase::kDraining ||
        status.phase == DominantContractPhase::kWarming) {
        decision.reason = "phase_" + std::string(DominantContractPhaseName(status.phase));
        return decision;
    }
    if (!selection_session_open) {
        decision.reason = "selection_session_closed";
        return decision;
    }
    if (!state.broker.truth_complete || state.broker.has_unmapped_position_or_order) {
        decision.reason = state.broker.has_unmapped_position_or_order ? "broker_identity_unresolved"
                                                                      : "broker_truth_incomplete";
        return decision;
    }
    if (config_.require_complete_baseline &&
        state.baseline_snapshots.size() != state.eligible_instruments.size()) {
        decision.reason = "candidate_baseline_incomplete";
        return decision;
    }

    std::string metric;
    const MarketSnapshot* best = BestSnapshotLocked(state, &metric);
    if (best == nullptr) {
        decision.reason = "candidate_snapshot_missing";
        return decision;
    }
    status.selection_metric = metric;
    status.candidate_metric = metric == "open_interest" ? best->open_interest : best->volume;
    decision.candidate_instrument_id = best->instrument_id;

    const auto best_live_it = state.live_snapshots.find(best->instrument_id);
    if (best_live_it == state.live_snapshots.end() ||
        !IsFreshExecutableSnapshot(best_live_it->second, now_ns)) {
        decision.reason = "best_candidate_market_not_fresh";
        return decision;
    }
    if (status.current_instrument_id.empty()) {
        status.candidate_instrument_id = best->instrument_id;
        decision.action = DominantContractAction::kSelectInitial;
        decision.reason = "initial_candidate_ready";
        return decision;
    }
    if (best->instrument_id == status.current_instrument_id) {
        status.candidate_instrument_id.clear();
        status.lead_windows = 0;
        status.lead_ratio = 0.0;
        if (status.phase == DominantContractPhase::kPendingFlat) {
            status.phase = DominantContractPhase::kReady;
            status.phase_started_ts_ns = now_ns;
        }
        decision.reason = "current_remains_dominant";
        return decision;
    }

    const MarketSnapshot* current = LatestSnapshotLocked(state, status.current_instrument_id);
    const auto current_live_it = state.live_snapshots.find(status.current_instrument_id);
    if (current == nullptr || current_live_it == state.live_snapshots.end() ||
        !IsFreshExecutableSnapshot(current_live_it->second, now_ns)) {
        decision.reason = "current_market_not_fresh";
        return decision;
    }
    const double current_metric =
        metric == "open_interest" ? current->open_interest : current->volume;
    const double candidate_metric = metric == "open_interest" ? best->open_interest : best->volume;
    status.current_metric = current_metric;
    status.candidate_metric = candidate_metric;
    status.lead_ratio = current_metric > 0.0 ? (candidate_metric - current_metric) / current_metric
                                             : (candidate_metric > 0.0 ? 1.0 : 0.0);
    if (status.lead_ratio < config_.min_lead_ratio) {
        status.candidate_instrument_id.clear();
        status.lead_windows = 0;
        if (status.phase == DominantContractPhase::kPendingFlat) {
            status.phase = DominantContractPhase::kReady;
            status.phase_started_ts_ns = now_ns;
        }
        decision.reason = "candidate_lead_below_threshold";
        return decision;
    }
    if (status.candidate_instrument_id != best->instrument_id) {
        status.candidate_instrument_id = best->instrument_id;
        status.lead_windows = 1;
    } else if (status.lead_windows < config_.min_lead_windows) {
        ++status.lead_windows;
    }
    if (status.lead_windows < config_.min_lead_windows) {
        decision.reason = "candidate_confirmation_pending";
        return decision;
    }
    const EpochNanos min_hold_ns = config_.min_hold_ms * kNanosPerMillisecond;
    if (status.selected_at_ns > 0 && now_ns >= status.selected_at_ns &&
        now_ns - status.selected_at_ns < min_hold_ns) {
        decision.reason = "minimum_hold_not_elapsed";
        return decision;
    }

    const bool non_flat = state.broker.position > 0 || state.broker.frozen > 0 ||
                          state.broker.active_close_orders > 0;
    if (non_flat || state.broker.active_open_orders > 0) {
        if (status.phase != DominantContractPhase::kPendingFlat) {
            status.phase = DominantContractPhase::kPendingFlat;
            status.phase_started_ts_ns = now_ns;
        }
        decision.action = state.broker.active_open_orders > 0
                              ? DominantContractAction::kCancelOpenOrders
                              : DominantContractAction::kEnterPendingFlat;
        decision.reason =
            state.broker.active_open_orders > 0 ? "active_open_orders" : "broker_not_flat";
        return decision;
    }

    // Freeze product-level opens before the caller starts the final broker query and
    // metadata/fee barriers. This closes the race where a new open could be submitted after
    // the flat check but before BeginSwitch increments the generation.
    if (status.phase != DominantContractPhase::kPendingFlat) {
        status.phase = DominantContractPhase::kPendingFlat;
        status.phase_started_ts_ns = now_ns;
    }
    decision.action = DominantContractAction::kBeginSwitch;
    decision.reason = "candidate_confirmed_and_broker_flat";
    return decision;
}

bool DominantContractCoordinator::CommitInitialSelection(const std::string& product_id,
                                                         const std::string& instrument_id,
                                                         EpochNanos now_ns, std::string* error) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = products_.find(product_id);
    if (it == products_.end() || it->second.eligible_instruments.count(instrument_id) == 0U) {
        SetError(error, "initial selection is not an eligible instrument");
        return false;
    }
    auto& status = it->second.status;
    if (!status.current_instrument_id.empty() ||
        status.phase != DominantContractPhase::kSelecting) {
        SetError(error, "initial selection is no longer pending");
        return false;
    }
    status.current_instrument_id = instrument_id;
    status.candidate_instrument_id.clear();
    status.phase = DominantContractPhase::kWarming;
    status.phase_started_ts_ns = now_ns;
    status.selected_at_ns = now_ns;
    status.updated_at_ns = now_ns;
    status.warmup_observed_bars = 0;
    status.warmup_required_bars = config_.min_warmup_bars;
    status.last_error.clear();
    RebuildInstrumentIndexLocked();
    return true;
}

bool DominantContractCoordinator::BeginRecoveryWarmup(const std::string& product_id,
                                                      const std::string& expected_current,
                                                      EpochNanos now_ns, std::uint64_t* generation,
                                                      std::string* error) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = products_.find(product_id);
    if (it == products_.end()) {
        SetError(error, "product is not registered");
        return false;
    }
    auto& state = it->second;
    auto& status = state.status;
    if (expected_current.empty() || status.current_instrument_id != expected_current ||
        (status.phase != DominantContractPhase::kPendingFlat &&
         status.phase != DominantContractPhase::kReady)) {
        SetError(error, "recovered dominant contract identity changed");
        return false;
    }
    if (!state.broker.truth_complete || state.broker.has_unmapped_position_or_order) {
        SetError(error, "broker truth is not complete for recovery warmup");
        return false;
    }
    status.phase = DominantContractPhase::kDraining;
    status.phase_started_ts_ns = now_ns;
    status.updated_at_ns = now_ns;
    status.candidate_instrument_id.clear();
    state.warmup_bar_keys.clear();
    ++status.generation;
    if (generation != nullptr) {
        *generation = status.generation;
    }
    return true;
}

bool DominantContractCoordinator::CommitRecoveryWarmup(const std::string& product_id,
                                                       const std::string& instrument_id,
                                                       std::int32_t replayed_warmup_bars,
                                                       std::int32_t required_warmup_bars,
                                                       EpochNanos now_ns, std::string* error) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = products_.find(product_id);
    if (it == products_.end()) {
        SetError(error, "product is not registered");
        return false;
    }
    auto& status = it->second.status;
    if (status.phase != DominantContractPhase::kDraining ||
        status.current_instrument_id != instrument_id) {
        SetError(error, "recovery warmup is not draining the requested contract");
        return false;
    }
    status.warmup_required_bars =
        std::max(config_.min_warmup_bars, std::max(1, required_warmup_bars));
    status.warmup_observed_bars = std::max(0, replayed_warmup_bars);
    status.phase = status.warmup_observed_bars >= status.warmup_required_bars
                       ? DominantContractPhase::kReady
                       : DominantContractPhase::kWarming;
    status.phase_started_ts_ns = now_ns;
    status.updated_at_ns = now_ns;
    status.last_error.clear();
    RebuildInstrumentIndexLocked();
    return true;
}

bool DominantContractCoordinator::BeginSwitch(const std::string& product_id,
                                              const std::string& expected_current,
                                              const std::string& expected_candidate,
                                              EpochNanos now_ns, std::uint64_t* generation,
                                              std::string* error) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = products_.find(product_id);
    if (it == products_.end()) {
        SetError(error, "product is not registered");
        return false;
    }
    auto& state = it->second;
    auto& status = state.status;
    if (status.current_instrument_id != expected_current ||
        status.candidate_instrument_id != expected_candidate ||
        state.eligible_instruments.count(expected_candidate) == 0U) {
        SetError(error, "dominant switch identity changed");
        return false;
    }
    if (!state.broker.truth_complete || state.broker.has_unmapped_position_or_order ||
        state.broker.position > 0 || state.broker.frozen > 0 ||
        state.broker.active_open_orders > 0 || state.broker.active_close_orders > 0) {
        SetError(error, "broker is not authoritatively flat");
        return false;
    }
    status.phase = DominantContractPhase::kDraining;
    status.phase_started_ts_ns = now_ns;
    ++status.generation;
    status.updated_at_ns = now_ns;
    state.warmup_bar_keys.clear();
    if (generation != nullptr) {
        *generation = status.generation;
    }
    return true;
}

bool DominantContractCoordinator::CommitSwitch(const std::string& product_id,
                                               const std::string& instrument_id,
                                               std::int32_t replayed_warmup_bars,
                                               std::int32_t required_warmup_bars, EpochNanos now_ns,
                                               std::string* error) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = products_.find(product_id);
    if (it == products_.end()) {
        SetError(error, "product is not registered");
        return false;
    }
    auto& status = it->second.status;
    if (status.phase != DominantContractPhase::kDraining ||
        status.candidate_instrument_id != instrument_id) {
        SetError(error, "dominant switch is not draining the requested candidate");
        return false;
    }
    status.current_instrument_id = instrument_id;
    status.selected_at_ns = now_ns;
    status.updated_at_ns = now_ns;
    status.warmup_required_bars =
        std::max(config_.min_warmup_bars, std::max(1, required_warmup_bars));
    status.warmup_observed_bars = std::max(0, replayed_warmup_bars);
    status.phase = status.warmup_observed_bars >= status.warmup_required_bars
                       ? DominantContractPhase::kReady
                       : DominantContractPhase::kWarming;
    status.phase_started_ts_ns = now_ns;
    status.last_error.clear();
    if (status.phase == DominantContractPhase::kReady) {
        status.candidate_instrument_id.clear();
        status.lead_windows = 0;
    }
    RebuildInstrumentIndexLocked();
    return true;
}

bool DominantContractCoordinator::AbortBeforeStrategyReset(const std::string& product_id,
                                                           const std::string& reason,
                                                           EpochNanos now_ns) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = products_.find(product_id);
    if (it == products_.end() || it->second.status.phase != DominantContractPhase::kDraining) {
        return false;
    }
    auto& status = it->second.status;
    ++status.generation;
    status.phase = DominantContractPhase::kReady;
    status.phase_started_ts_ns = now_ns;
    status.last_error = reason;
    status.updated_at_ns = now_ns;
    return true;
}

bool DominantContractCoordinator::RecordWarmupBar(const std::string& product_id,
                                                  const std::string& instrument_id,
                                                  const std::string& canonical_bar_key,
                                                  EpochNanos now_ns) {
    if (canonical_bar_key.empty()) {
        return false;
    }
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = products_.find(product_id);
    if (it == products_.end()) {
        return false;
    }
    auto& state = it->second;
    auto& status = state.status;
    if (status.phase != DominantContractPhase::kWarming ||
        status.current_instrument_id != instrument_id) {
        return false;
    }
    if (!state.warmup_bar_keys.insert(canonical_bar_key).second) {
        return false;
    }
    ++status.warmup_observed_bars;
    status.updated_at_ns = now_ns;
    if (status.warmup_observed_bars >= status.warmup_required_bars) {
        status.phase = DominantContractPhase::kReady;
        status.phase_started_ts_ns = now_ns;
        status.candidate_instrument_id.clear();
        status.lead_windows = 0;
        status.last_error.clear();
    }
    return true;
}

void DominantContractCoordinator::MarkFault(const std::string& product_id,
                                            const std::string& reason, EpochNanos now_ns) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = products_.find(product_id);
    if (it == products_.end()) {
        return;
    }
    it->second.status.phase = DominantContractPhase::kFault;
    it->second.status.phase_started_ts_ns = now_ns;
    it->second.status.last_error = reason;
    it->second.status.updated_at_ns = now_ns;
}

bool DominantContractCoordinator::CanDispatchToStrategy(const std::string& instrument_id) const {
    std::lock_guard<std::mutex> lock(mutex_);
    const auto product_it = product_by_instrument_.find(instrument_id);
    if (product_it == product_by_instrument_.end()) {
        return false;
    }
    const auto state_it = products_.find(product_it->second);
    if (state_it == products_.end() ||
        state_it->second.status.phase == DominantContractPhase::kDraining ||
        state_it->second.status.phase == DominantContractPhase::kFault ||
        state_it->second.status.phase == DominantContractPhase::kSelecting) {
        return false;
    }
    const auto& state = state_it->second;
    return state.status.current_instrument_id == instrument_id ||
           state.broker.held_instrument_ids.count(instrument_id) > 0U;
}

bool DominantContractCoordinator::IsCandidateInstrument(const std::string& instrument_id) const {
    std::lock_guard<std::mutex> lock(mutex_);
    return product_by_instrument_.count(instrument_id) > 0U;
}

ContractSignalValidation DominantContractCoordinator::ValidateSignal(
    const SignalIntent& signal, std::int32_t broker_close_volume) {
    std::lock_guard<std::mutex> lock(mutex_);
    ContractSignalValidation validation;
    std::string product_id = signal.product_id;
    if (product_id.empty()) {
        const auto product_it = product_by_instrument_.find(signal.instrument_id);
        if (product_it != product_by_instrument_.end()) {
            product_id = product_it->second;
        }
    }
    auto state_it = products_.find(product_id);
    if (state_it == products_.end()) {
        validation.reason = "dominant_product_unknown";
        return validation;
    }
    auto& state = state_it->second;
    auto& status = state.status;
    const bool close_like =
        signal.signal_type != SignalType::kOpen || signal.offset != OffsetFlag::kOpen;
    if (!close_like &&
        (!state.broker.truth_complete || state.broker.has_unmapped_position_or_order)) {
        validation.reason = state.broker.has_unmapped_position_or_order
                                ? "broker_identity_unresolved"
                                : "broker_truth_incomplete";
        return validation;
    }
    if (signal.contract_generation == 0 || signal.contract_generation != status.generation) {
        ++status.generation_rejections;
        validation.reason = "stale_contract_generation";
        validation.persist_pending_exit = close_like && broker_close_volume > 0;
        return validation;
    }
    if (close_like) {
        validation.allowed = broker_close_volume > 0;
        validation.reason = validation.allowed ? "close_allowed" : "broker_position_flat";
        return validation;
    }
    if (status.phase != DominantContractPhase::kReady) {
        validation.reason =
            "dominant_phase_" + std::string(DominantContractPhaseName(status.phase));
        return validation;
    }
    if (status.current_instrument_id != signal.instrument_id) {
        validation.reason = "non_active_dominant_contract";
        return validation;
    }
    validation.allowed = true;
    validation.reason = "open_allowed";
    return validation;
}

std::optional<std::uint64_t> DominantContractCoordinator::GenerationForInstrument(
    const std::string& instrument_id) const {
    std::lock_guard<std::mutex> lock(mutex_);
    const auto product_it = product_by_instrument_.find(instrument_id);
    if (product_it == product_by_instrument_.end()) {
        return std::nullopt;
    }
    const auto state_it = products_.find(product_it->second);
    if (state_it == products_.end()) {
        return std::nullopt;
    }
    return state_it->second.status.generation;
}

std::optional<std::string> DominantContractCoordinator::ProductForInstrument(
    const std::string& instrument_id) const {
    std::lock_guard<std::mutex> lock(mutex_);
    const auto it = product_by_instrument_.find(instrument_id);
    return it == product_by_instrument_.end() ? std::nullopt
                                              : std::optional<std::string>(it->second);
}

DominantContractStatus DominantContractCoordinator::GetStatus(const std::string& product_id) const {
    std::lock_guard<std::mutex> lock(mutex_);
    const auto it = products_.find(product_id);
    return it == products_.end() ? DominantContractStatus{} : it->second.status;
}

std::vector<DominantContractStatus> DominantContractCoordinator::GetAllStatuses() const {
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<DominantContractStatus> statuses;
    statuses.reserve(products_.size());
    for (const auto& [product_id, state] : products_) {
        (void)product_id;
        statuses.push_back(state.status);
    }
    std::sort(statuses.begin(), statuses.end(),
              [](const auto& lhs, const auto& rhs) { return lhs.product_id < rhs.product_id; });
    return statuses;
}

bool DominantContractCoordinator::PersistStatusAtomically(const std::string& product_id,
                                                          const std::string& output_path,
                                                          std::string* error) const {
    if (output_path.empty()) {
        SetError(error, "dominant contract status path is empty");
        return false;
    }
    const DominantContractStatus status = GetStatus(product_id);
    if (status.product_id.empty()) {
        SetError(error, "dominant contract product is not registered");
        return false;
    }
    std::ostringstream payload;
    payload << std::setprecision(17);
    payload << "{\n"
            << "  \"schema_version\": 2,\n"
            << "  \"trading_day\": \"" << JsonEscape(status.trading_day) << "\",\n"
            << "  \"product_id\": \"" << JsonEscape(status.product_id) << "\",\n"
            << "  \"current_instrument_id\": \"" << JsonEscape(status.current_instrument_id)
            << "\",\n"
            << "  \"candidate_instrument_id\": \"" << JsonEscape(status.candidate_instrument_id)
            << "\",\n"
            << "  \"phase\": \"" << DominantContractPhaseName(status.phase) << "\",\n"
            << "  \"generation\": " << status.generation << ",\n"
            << "  \"selection_metric\": \"" << JsonEscape(status.selection_metric) << "\",\n"
            << "  \"current_metric\": " << status.current_metric << ",\n"
            << "  \"candidate_metric\": " << status.candidate_metric << ",\n"
            << "  \"lead_ratio\": " << status.lead_ratio << ",\n"
            << "  \"lead_windows\": " << status.lead_windows << ",\n"
            << "  \"eligible_count\": " << status.eligible_count << ",\n"
            << "  \"baseline_count\": " << status.baseline_count << ",\n"
            << "  \"broker_position\": " << status.broker_position << ",\n"
            << "  \"broker_frozen\": " << status.broker_frozen << ",\n"
            << "  \"active_open_orders\": " << status.active_open_orders << ",\n"
            << "  \"active_close_orders\": " << status.active_close_orders << ",\n"
            << "  \"warmup_observed_bars\": " << status.warmup_observed_bars << ",\n"
            << "  \"warmup_required_bars\": " << status.warmup_required_bars << ",\n"
            << "  \"generation_rejections\": " << status.generation_rejections << ",\n"
            << "  \"selected_at_ns\": " << status.selected_at_ns << ",\n"
            << "  \"phase_started_ts_ns\": " << status.phase_started_ts_ns << ",\n"
            << "  \"updated_at_ns\": " << status.updated_at_ns << ",\n"
            << "  \"last_error\": \"" << JsonEscape(status.last_error) << "\"\n"
            << "}\n";

    const std::filesystem::path output(output_path);
    std::error_code ec;
    if (!output.parent_path().empty()) {
        std::filesystem::create_directories(output.parent_path(), ec);
        if (ec) {
            SetError(error, "failed to create dominant status directory: " + ec.message());
            return false;
        }
    }
    const auto suffix = std::chrono::duration_cast<std::chrono::nanoseconds>(
                            std::chrono::system_clock::now().time_since_epoch())
                            .count();
    const std::filesystem::path temporary = output.string() + ".tmp." + std::to_string(suffix);
    {
        std::ofstream stream(temporary, std::ios::out | std::ios::trunc | std::ios::binary);
        if (!stream.is_open()) {
            SetError(error, "failed to open dominant status temp file");
            return false;
        }
        stream << payload.str();
        stream.flush();
        if (!stream.good()) {
            stream.close();
            std::filesystem::remove(temporary, ec);
            SetError(error, "failed to flush dominant status temp file");
            return false;
        }
    }
#if !defined(_WIN32)
    if (!FsyncPath(temporary, false, error)) {
        std::filesystem::remove(temporary, ec);
        return false;
    }
#endif
    std::filesystem::rename(temporary, output, ec);
    if (ec) {
        std::filesystem::remove(temporary, ec);
        SetError(error, "failed to publish dominant status: " + ec.message());
        return false;
    }
#if !defined(_WIN32)
    const auto parent =
        output.parent_path().empty() ? std::filesystem::path(".") : output.parent_path();
    if (!FsyncPath(parent, true, error)) {
        return false;
    }
#endif
    return true;
}

bool DominantContractCoordinator::IsNewerSnapshot(const MarketSnapshot& candidate,
                                                  const MarketSnapshot& current) {
    const EpochNanos candidate_event =
        candidate.exchange_ts_ns > 0 ? candidate.exchange_ts_ns : candidate.recv_ts_ns;
    const EpochNanos current_event =
        current.exchange_ts_ns > 0 ? current.exchange_ts_ns : current.recv_ts_ns;
    return candidate_event > current_event ||
           (candidate_event == current_event && candidate.recv_ts_ns >= current.recv_ts_ns);
}

bool DominantContractCoordinator::IsFreshExecutableSnapshot(const MarketSnapshot& snapshot,
                                                            EpochNanos now_ns) const {
    if (snapshot.recv_ts_ns <= 0 || now_ns < snapshot.recv_ts_ns ||
        now_ns - snapshot.recv_ts_ns > config_.max_tick_age_ms * kNanosPerMillisecond) {
        return false;
    }
    return std::isfinite(snapshot.last_price) && snapshot.last_price > 0.0 &&
           std::isfinite(snapshot.bid_price_1) && snapshot.bid_price_1 > 0.0 &&
           std::isfinite(snapshot.ask_price_1) && snapshot.ask_price_1 > 0.0;
}

const MarketSnapshot* DominantContractCoordinator::BestSnapshotLocked(const ProductState& state,
                                                                      std::string* metric) const {
    const bool has_positive_open_interest =
        std::any_of(state.eligible_instruments.begin(), state.eligible_instruments.end(),
                    [&](const std::string& instrument_id) {
                        const MarketSnapshot* snapshot = LatestSnapshotLocked(state, instrument_id);
                        return snapshot != nullptr && snapshot->open_interest > 0;
                    });
    if (metric != nullptr) {
        *metric = has_positive_open_interest ? "open_interest" : "volume";
    }
    const MarketSnapshot* best = nullptr;
    for (const auto& instrument_id : state.eligible_instruments) {
        const MarketSnapshot* candidate = LatestSnapshotLocked(state, instrument_id);
        if (candidate == nullptr) {
            return nullptr;
        }
        if (best == nullptr) {
            best = candidate;
            continue;
        }
        const std::int64_t candidate_value =
            has_positive_open_interest ? candidate->open_interest : candidate->volume;
        const std::int64_t best_value =
            has_positive_open_interest ? best->open_interest : best->volume;
        if (candidate_value > best_value ||
            (candidate_value == best_value && candidate->instrument_id < best->instrument_id)) {
            best = candidate;
        }
    }
    return best;
}

const MarketSnapshot* DominantContractCoordinator::LatestSnapshotLocked(
    const ProductState& state, const std::string& instrument_id) const {
    const auto live_it = state.live_snapshots.find(instrument_id);
    const auto baseline_it = state.baseline_snapshots.find(instrument_id);
    if (live_it == state.live_snapshots.end()) {
        return baseline_it == state.baseline_snapshots.end() ? nullptr : &baseline_it->second;
    }
    if (baseline_it == state.baseline_snapshots.end()) {
        return &live_it->second;
    }
    return IsNewerSnapshot(live_it->second, baseline_it->second) ? &live_it->second
                                                                 : &baseline_it->second;
}

void DominantContractCoordinator::RebuildInstrumentIndexLocked() {
    product_by_instrument_.clear();
    for (const auto& [product_id, state] : products_) {
        for (const auto& instrument_id : state.eligible_instruments) {
            product_by_instrument_[instrument_id] = product_id;
        }
        if (!state.status.current_instrument_id.empty()) {
            product_by_instrument_[state.status.current_instrument_id] = product_id;
        }
        for (const auto& instrument_id : state.broker.held_instrument_ids) {
            product_by_instrument_[instrument_id] = product_id;
        }
    }
}

}  // namespace quant_hft
