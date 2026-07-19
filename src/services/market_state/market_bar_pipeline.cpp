#include "quant_hft/services/market_bar_pipeline.h"

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <sstream>
#include <utility>

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

const std::string* RequireValue(const MarketBarPipeline::PersistenceState& state,
                                const std::string& key, std::string* error) {
    const auto it = state.find(key);
    if (it == state.end()) {
        SetError(error, "missing market bar pipeline state key: " + key);
        return nullptr;
    }
    return &it->second;
}

template <typename Integer>
bool ParseInteger(const MarketBarPipeline::PersistenceState& state, const std::string& key,
                  Integer* out, std::string* error) {
    const std::string* value = RequireValue(state, key, error);
    if (value == nullptr || out == nullptr) {
        return false;
    }
    try {
        std::size_t consumed = 0;
        const long long parsed = std::stoll(*value, &consumed);
        if (consumed != value->size()) {
            throw std::invalid_argument("trailing characters");
        }
        *out = static_cast<Integer>(parsed);
        return true;
    } catch (...) {
        SetError(error, "invalid integer market bar pipeline state key: " + key);
        return false;
    }
}

bool ParseDouble(const MarketBarPipeline::PersistenceState& state, const std::string& key,
                 double* out, std::string* error) {
    const std::string* value = RequireValue(state, key, error);
    if (value == nullptr || out == nullptr) {
        return false;
    }
    try {
        std::size_t consumed = 0;
        *out = std::stod(*value, &consumed);
        if (consumed != value->size()) {
            throw std::invalid_argument("trailing characters");
        }
        return true;
    } catch (...) {
        SetError(error, "invalid double market bar pipeline state key: " + key);
        return false;
    }
}

std::string FormatDouble(double value) {
    std::ostringstream stream;
    stream << std::setprecision(17) << value;
    return stream.str();
}

std::string ErrnoMessage(const std::string& action) { return action + ": " + std::strerror(errno); }

#if !defined(_WIN32)
bool FsyncPath(const std::filesystem::path& path, bool directory, std::string* error) {
    const int flags = directory ? (O_RDONLY | O_DIRECTORY) : O_RDONLY;
    const int fd = ::open(path.c_str(), flags);
    if (fd < 0) {
        SetError(error, ErrnoMessage("open for fsync failed: " + path.string()));
        return false;
    }
    const int rc = ::fsync(fd);
    const int saved_errno = errno;
    ::close(fd);
    if (rc != 0) {
        errno = saved_errno;
        SetError(error, ErrnoMessage("fsync failed: " + path.string()));
        return false;
    }
    return true;
}
#endif

}  // namespace

MarketBarPipeline::MarketBarPipeline(MarketBarPipelineConfig config)
    : config_(std::move(config)),
      bar_aggregator_(config_.bar_aggregator),
      timeframe_fanout_(config_.timeframes, config_.detector, config_.detector_by_product) {}

MarketBarPipelineResult MarketBarPipeline::OnTick(const MarketSnapshot& snapshot) {
    std::lock_guard<std::mutex> lock(mutex_);
    MarketBarPipelineResult result;
    result.recovery_replay = replaying_;

    const EpochNanos reference_ts =
        snapshot.recv_ts_ns > 0
            ? snapshot.recv_ts_ns
            : (snapshot.exchange_ts_ns > 0 ? snapshot.exchange_ts_ns : NowEpochNanos());
    PruneTickFingerprintsLocked(reference_ts);
    const std::string fingerprint = TickFingerprint(snapshot);
    const auto duplicate_it = tick_fingerprint_seen_ts_.find(fingerprint);
    const EpochNanos retention_ns =
        std::max<std::int64_t>(0, config_.tick_fingerprint_retention_ms) * kNanosPerMillisecond;
    if (duplicate_it != tick_fingerprint_seen_ts_.end() &&
        (retention_ns == 0 || reference_ts - duplicate_it->second <= retention_ns)) {
        duplicate_it->second = std::max(duplicate_it->second, reference_ts);
        result.duplicate_tick = true;
        return result;
    }
    tick_fingerprint_seen_ts_[fingerprint] = reference_ts;

    if (bar_aggregator_.IsFinalizedSnapshot(snapshot)) {
        result.late_tick = true;
        recovery_by_instrument_[snapshot.instrument_id] = RecoveryState{};
    }

    MarketBarPipelineResult processed =
        ProcessOneMinuteBarsLocked(bar_aggregator_.OnMarketSnapshot(snapshot), replaying_);
    processed.late_tick = result.late_tick;
    processed.recovery_replay = replaying_;
    return processed;
}

MarketBarPipelineResult MarketBarPipeline::AdvanceWatermark(EpochNanos now_ns) {
    std::lock_guard<std::mutex> lock(mutex_);
    MarketBarPipelineResult result =
        ProcessOneMinuteBarsLocked(bar_aggregator_.AdvanceWatermark(now_ns), replaying_);
    const EpochNanos lateness_ns =
        static_cast<EpochNanos>(std::max(0, config_.bar_aggregator.allowed_lateness_ms)) *
        kNanosPerMillisecond;
    const EpochNanos watermark_ns = now_ns > lateness_ns ? now_ns - lateness_ns : 0;
    last_watermark_ns_ = std::max(last_watermark_ns_, watermark_ns);
    for (auto emission : timeframe_fanout_.AdvanceWatermark(watermark_ns)) {
        if (replaying_) {
            emission.bar.is_recovery_replay = true;
            emission.bar.strategy_eligible = false;
            emission.strategy_eligible = false;
            emission.state.has_bar = false;
        }
        AppendCanonicalEmissionLocked(std::move(emission), &result);
    }
    result.recovery_replay = replaying_;
    return result;
}

bool MarketBarPipeline::Recover(const PersistenceState& checkpoint,
                                const std::vector<MarketSnapshot>& raw_tail,
                                MarketBarPipelineResult* result, std::string* error) {
    if (result == nullptr) {
        SetError(error, "market bar recovery result is null");
        return false;
    }
    *result = {};
    std::unique_lock<std::mutex> lock(mutex_);
    if (!LoadStateLocked(checkpoint, error)) {
        return false;
    }
    replaying_ = true;
    lock.unlock();
    for (const auto& snapshot : raw_tail) {
        MergeResult(OnTick(snapshot), result);
    }
    lock.lock();
    replaying_ = false;
    result->recovery_replay = true;
    return true;
}

bool MarketBarPipeline::PrepareShutdown(EpochNanos now_ns, PersistenceState* checkpoint,
                                        MarketBarPipelineResult* result, std::string* error) {
    if (checkpoint == nullptr || result == nullptr) {
        SetError(error, "market bar shutdown checkpoint/result is null");
        return false;
    }
    std::lock_guard<std::mutex> lock(mutex_);
    *result = ProcessOneMinuteBarsLocked(bar_aggregator_.AdvanceWatermark(now_ns), false);
    const EpochNanos lateness_ns =
        static_cast<EpochNanos>(std::max(0, config_.bar_aggregator.allowed_lateness_ms)) *
        kNanosPerMillisecond;
    const EpochNanos watermark_ns = now_ns > lateness_ns ? now_ns - lateness_ns : 0;
    last_watermark_ns_ = std::max(last_watermark_ns_, watermark_ns);
    for (auto emission : timeframe_fanout_.AdvanceWatermark(watermark_ns)) {
        AppendCanonicalEmissionLocked(std::move(emission), result);
    }
    return SaveStateLocked(checkpoint, error);
}

bool MarketBarPipeline::SaveState(PersistenceState* out, std::string* error) const {
    std::lock_guard<std::mutex> lock(mutex_);
    return SaveStateLocked(out, error);
}

bool MarketBarPipeline::LoadState(const PersistenceState& state, std::string* error) {
    std::lock_guard<std::mutex> lock(mutex_);
    return LoadStateLocked(state, error);
}

bool MarketBarPipeline::SaveCheckpointAtomically(const std::string& path,
                                                 std::string* error) const {
    if (path.empty()) {
        SetError(error, "market bar checkpoint path is empty");
        return false;
    }
    PersistenceState state;
    if (!SaveState(&state, error)) {
        return false;
    }
    std::vector<std::pair<std::string, std::string>> entries(state.begin(), state.end());
    std::sort(entries.begin(), entries.end());
    std::ostringstream payload;
    for (const auto& [key, value] : entries) {
        payload << EscapeCheckpointValue(key) << '=' << EscapeCheckpointValue(value) << '\n';
    }

    const std::filesystem::path output(path);
    std::error_code ec;
    if (!output.parent_path().empty()) {
        std::filesystem::create_directories(output.parent_path(), ec);
        if (ec) {
            SetError(error, "failed to create checkpoint directory: " + ec.message());
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
            SetError(error, "failed to open market bar checkpoint temp file");
            return false;
        }
        stream << payload.str();
        stream.flush();
        if (!stream.good()) {
            stream.close();
            std::filesystem::remove(temporary, ec);
            SetError(error, "failed to flush market bar checkpoint temp file");
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
        SetError(error, "failed to atomically publish market bar checkpoint: " + ec.message());
        return false;
    }
#if !defined(_WIN32)
    const std::filesystem::path parent =
        output.parent_path().empty() ? std::filesystem::path(".") : output.parent_path();
    if (!FsyncPath(parent, true, error)) {
        return false;
    }
#endif
    return true;
}

bool MarketBarPipeline::LoadCheckpointFile(const std::string& path, std::string* error) {
    std::ifstream stream(path, std::ios::in | std::ios::binary);
    if (!stream.is_open()) {
        SetError(error, "failed to open market bar checkpoint: " + path);
        return false;
    }
    PersistenceState state;
    std::string line;
    while (std::getline(stream, line)) {
        if (line.empty()) {
            continue;
        }
        const std::size_t separator = line.find('=');
        if (separator == std::string::npos) {
            SetError(error, "malformed market bar checkpoint line");
            return false;
        }
        std::string key;
        std::string value;
        if (!UnescapeCheckpointValue(line.substr(0, separator), &key) ||
            !UnescapeCheckpointValue(line.substr(separator + 1), &value) || key.empty()) {
            SetError(error, "invalid market bar checkpoint escape sequence");
            return false;
        }
        if (!state.emplace(std::move(key), std::move(value)).second) {
            SetError(error, "duplicate market bar checkpoint key");
            return false;
        }
    }
    if (!stream.eof()) {
        SetError(error, "failed while reading market bar checkpoint");
        return false;
    }
    return LoadState(state, error);
}

bool MarketBarPipeline::IsOpeningSuppressed(const std::string& instrument_id) const {
    std::lock_guard<std::mutex> lock(mutex_);
    return recovery_by_instrument_.find(instrument_id) != recovery_by_instrument_.end();
}

std::vector<std::string> MarketBarPipeline::SuppressedInstruments() const {
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<std::string> instruments;
    instruments.reserve(recovery_by_instrument_.size());
    for (const auto& [instrument_id, state] : recovery_by_instrument_) {
        (void)state;
        instruments.push_back(instrument_id);
    }
    std::sort(instruments.begin(), instruments.end());
    return instruments;
}

void MarketBarPipeline::ResetInstrument(const std::string& instrument_id,
                                        bool preserve_detector_state) {
    if (instrument_id.empty()) {
        return;
    }
    std::lock_guard<std::mutex> lock(mutex_);
    bar_aggregator_.ResetInstrument(instrument_id);
    if (preserve_detector_state) {
        timeframe_fanout_.ResetInstrumentBuckets(instrument_id);
    } else {
        timeframe_fanout_.ResetInstrument(instrument_id);
    }
    const std::string prefix = instrument_id + "|";
    for (auto it = canonical_bar_fingerprints_.begin(); it != canonical_bar_fingerprints_.end();) {
        if (it->first.rfind(prefix, 0) == 0) {
            it = canonical_bar_fingerprints_.erase(it);
        } else {
            ++it;
        }
    }
    for (auto it = tick_fingerprint_seen_ts_.begin(); it != tick_fingerprint_seen_ts_.end();) {
        if (it->first.rfind(prefix, 0) == 0) {
            it = tick_fingerprint_seen_ts_.erase(it);
        } else {
            ++it;
        }
    }
    for (auto it = recent_complete_states_.begin(); it != recent_complete_states_.end();) {
        if (it->first.rfind(prefix, 0) == 0) {
            it = recent_complete_states_.erase(it);
        } else {
            ++it;
        }
    }
    recovery_by_instrument_.erase(instrument_id);
}

std::vector<StateSnapshot7D> MarketBarPipeline::RecentCompleteStates(
    const std::string& instrument_id, std::int32_t timeframe_minutes, std::size_t limit) const {
    std::lock_guard<std::mutex> lock(mutex_);
    const std::string key = instrument_id + "|" + std::to_string(timeframe_minutes);
    const auto it = recent_complete_states_.find(key);
    if (it == recent_complete_states_.end() || limit == 0) {
        return {};
    }
    const std::size_t offset = it->second.size() > limit ? it->second.size() - limit : 0;
    return std::vector<StateSnapshot7D>(it->second.begin() + static_cast<std::ptrdiff_t>(offset),
                                        it->second.end());
}

std::string MarketBarPipeline::TickFingerprint(const MarketSnapshot& snapshot) {
    std::ostringstream out;
    out.precision(17);
    out << snapshot.instrument_id << '|' << snapshot.exchange_id << '|' << snapshot.trading_day
        << '|' << snapshot.action_day << '|' << snapshot.update_time << '|'
        << snapshot.update_millisec << '|' << snapshot.last_price << '|' << snapshot.bid_price_1
        << '|' << snapshot.ask_price_1 << '|' << snapshot.bid_volume_1 << '|'
        << snapshot.ask_volume_1 << '|' << snapshot.volume << '|' << snapshot.open_interest << '|'
        << snapshot.settlement_price << '|' << snapshot.average_price_raw << '|'
        << snapshot.exchange_ts_ns;
    return out.str();
}

std::string MarketBarPipeline::BarKey(const BarSnapshot& bar, std::int32_t timeframe_minutes) {
    return bar.instrument_id + "|" + std::to_string(timeframe_minutes) + "|" + bar.trading_day +
           "|" + bar.minute;
}

std::string MarketBarPipeline::BarFingerprint(const BarSnapshot& bar) {
    std::ostringstream out;
    out.precision(17);
    out << bar.instrument_id << '|' << bar.exchange_id << '|' << bar.trading_day << '|'
        << bar.action_day << '|' << bar.minute << '|' << bar.open << '|' << bar.high << '|'
        << bar.low << '|' << bar.close << '|' << bar.analysis_open << '|' << bar.analysis_high
        << '|' << bar.analysis_low << '|' << bar.analysis_close << '|' << bar.analysis_price_offset
        << '|' << bar.volume << '|' << bar.ts_ns << '|' << bar.period_end_ts_ns << '|'
        << bar.expected_source_bars << '|' << bar.observed_source_bars << '|' << bar.is_complete
        << '|' << bar.is_session_endpoint << '|' << bar.volume_complete << '|' << bar.has_conflict;
    return out.str();
}

std::string MarketBarPipeline::EscapeCheckpointValue(const std::string& value) {
    std::ostringstream out;
    out << std::uppercase << std::hex;
    for (unsigned char ch : value) {
        if ((ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || (ch >= '0' && ch <= '9') ||
            ch == '.' || ch == '_' || ch == '-' || ch == '|') {
            out << static_cast<char>(ch);
        } else {
            out << '%' << std::setw(2) << std::setfill('0') << static_cast<int>(ch);
        }
    }
    return out.str();
}

bool MarketBarPipeline::UnescapeCheckpointValue(const std::string& value, std::string* out) {
    if (out == nullptr) {
        return false;
    }
    out->clear();
    for (std::size_t index = 0; index < value.size(); ++index) {
        if (value[index] != '%') {
            out->push_back(value[index]);
            continue;
        }
        if (index + 2 >= value.size()) {
            return false;
        }
        try {
            std::size_t consumed = 0;
            const int parsed = std::stoi(value.substr(index + 1, 2), &consumed, 16);
            if (consumed != 2) {
                return false;
            }
            out->push_back(static_cast<char>(parsed));
        } catch (...) {
            return false;
        }
        index += 2;
    }
    return true;
}

void MarketBarPipeline::PruneTickFingerprintsLocked(EpochNanos reference_ts_ns) {
    const EpochNanos retention_ns =
        std::max<std::int64_t>(0, config_.tick_fingerprint_retention_ms) * kNanosPerMillisecond;
    if (retention_ns <= 0) {
        return;
    }
    for (auto it = tick_fingerprint_seen_ts_.begin(); it != tick_fingerprint_seen_ts_.end();) {
        if (reference_ts_ns > it->second && reference_ts_ns - it->second > retention_ns) {
            it = tick_fingerprint_seen_ts_.erase(it);
        } else {
            ++it;
        }
    }
}

MarketBarPipelineResult MarketBarPipeline::ProcessOneMinuteBarsLocked(std::vector<BarSnapshot> bars,
                                                                      bool recovery_replay) {
    MarketBarPipelineResult result;
    result.recovery_replay = recovery_replay;
    for (auto bar : bars) {
        if (recovery_replay) {
            bar.is_recovery_replay = true;
            bar.strategy_eligible = false;
        }
        if (recovery_by_instrument_.find(bar.instrument_id) != recovery_by_instrument_.end()) {
            bar.strategy_eligible = false;
        }
        if (!AppendCanonicalOneMinuteLocked(bar, &result)) {
            continue;
        }
        for (auto emission : timeframe_fanout_.OnOneMinuteBar(bar)) {
            if (recovery_replay) {
                emission.bar.is_recovery_replay = true;
                emission.bar.strategy_eligible = false;
                emission.strategy_eligible = false;
                emission.state.has_bar = false;
            }
            AppendCanonicalEmissionLocked(std::move(emission), &result);
        }
    }
    return result;
}

bool MarketBarPipeline::AppendCanonicalOneMinuteLocked(const BarSnapshot& bar,
                                                       MarketBarPipelineResult* result) {
    if (result == nullptr) {
        return false;
    }
    const std::string key = BarKey(bar, 1);
    const std::string fingerprint = BarFingerprint(bar);
    const auto it = canonical_bar_fingerprints_.find(key);
    if (it != canonical_bar_fingerprints_.end()) {
        if (it->second != fingerprint) {
            result->critical_conflicts.push_back(key);
            recovery_by_instrument_[bar.instrument_id] = RecoveryState{};
        }
        return false;
    }
    canonical_bar_fingerprints_[key] = fingerprint;
    result->one_minute_bars.push_back(bar);
    return true;
}

void MarketBarPipeline::AppendCanonicalEmissionLocked(TimeframeStateEmission emission,
                                                      MarketBarPipelineResult* result) {
    if (result == nullptr || emission.bar.instrument_id.empty()) {
        return;
    }
    UpdateLateRecoveryLocked(emission);
    const std::string key = BarKey(emission.bar, emission.timeframe_minutes);
    const std::string fingerprint = BarFingerprint(emission.bar);
    const auto it = canonical_bar_fingerprints_.find(key);
    if (it != canonical_bar_fingerprints_.end()) {
        if (it->second != fingerprint) {
            result->critical_conflicts.push_back(key);
            recovery_by_instrument_[emission.bar.instrument_id] = RecoveryState{};
        }
        return;
    }
    canonical_bar_fingerprints_[key] = fingerprint;
    if (emission.strategy_eligible && emission.bar.strategy_eligible && emission.bar.is_complete &&
        !emission.bar.is_session_endpoint && !emission.bar.is_recovery_replay &&
        !emission.bar.has_conflict && emission.state.has_bar) {
        auto& recent = recent_complete_states_[emission.bar.instrument_id + "|" +
                                               std::to_string(emission.timeframe_minutes)];
        recent.push_back(emission.state);
        const std::size_t limit = std::max<std::size_t>(1, config_.recent_complete_state_limit);
        while (recent.size() > limit) {
            recent.pop_front();
        }
    }
    result->timeframe_emissions.push_back(std::move(emission));
}

void MarketBarPipeline::UpdateLateRecoveryLocked(const TimeframeStateEmission& emission) {
    if (emission.timeframe_minutes != 5 || emission.bar.is_session_endpoint ||
        emission.bar.is_recovery_replay) {
        return;
    }
    auto it = recovery_by_instrument_.find(emission.bar.instrument_id);
    if (it == recovery_by_instrument_.end()) {
        return;
    }
    const bool structurally_complete =
        emission.bar.is_complete && !emission.bar.has_conflict &&
        emission.bar.expected_source_bars == emission.bar.observed_source_bars;
    if (!structurally_complete) {
        it->second.consecutive_complete_five_minute_bars = 0;
        return;
    }
    ++it->second.consecutive_complete_five_minute_bars;
    if (it->second.consecutive_complete_five_minute_bars >=
        std::max(1, config_.complete_five_minute_bars_to_reenable)) {
        recovery_by_instrument_.erase(it);
    }
}

void MarketBarPipeline::MergeResult(MarketBarPipelineResult source,
                                    MarketBarPipelineResult* destination) const {
    if (destination == nullptr) {
        return;
    }
    destination->one_minute_bars.insert(destination->one_minute_bars.end(),
                                        std::make_move_iterator(source.one_minute_bars.begin()),
                                        std::make_move_iterator(source.one_minute_bars.end()));
    destination->timeframe_emissions.insert(
        destination->timeframe_emissions.end(),
        std::make_move_iterator(source.timeframe_emissions.begin()),
        std::make_move_iterator(source.timeframe_emissions.end()));
    destination->critical_conflicts.insert(
        destination->critical_conflicts.end(),
        std::make_move_iterator(source.critical_conflicts.begin()),
        std::make_move_iterator(source.critical_conflicts.end()));
    destination->duplicate_tick = destination->duplicate_tick || source.duplicate_tick;
    destination->late_tick = destination->late_tick || source.late_tick;
    destination->recovery_replay = destination->recovery_replay || source.recovery_replay;
}

bool MarketBarPipeline::SaveStateLocked(PersistenceState* out, std::string* error) const {
    if (out == nullptr) {
        SetError(error, "market bar pipeline state output is null");
        return false;
    }
    BarAggregator::PersistenceState aggregator_state;
    TimeframeStateFanout::PersistenceState fanout_state;
    if (!bar_aggregator_.SaveState(&aggregator_state, error) ||
        !timeframe_fanout_.SaveState(&fanout_state, error)) {
        return false;
    }
    out->clear();
    (*out)["version"] = "2";
    (*out)["last_watermark_ns"] = std::to_string(last_watermark_ns_);
    for (const auto& [key, value] : aggregator_state) {
        (*out)["aggregator." + key] = value;
    }
    for (const auto& [key, value] : fanout_state) {
        (*out)["fanout." + key] = value;
    }

    (*out)["tick_fingerprints.count"] = std::to_string(tick_fingerprint_seen_ts_.size());
    std::size_t fingerprint_index = 0;
    for (const auto& [fingerprint, ts_ns] : tick_fingerprint_seen_ts_) {
        const std::string prefix = "tick_fingerprints." + std::to_string(fingerprint_index++);
        (*out)[prefix + ".value"] = fingerprint;
        (*out)[prefix + ".ts_ns"] = std::to_string(ts_ns);
    }

    (*out)["canonical_bars.count"] = std::to_string(canonical_bar_fingerprints_.size());
    std::size_t canonical_index = 0;
    for (const auto& [key, fingerprint] : canonical_bar_fingerprints_) {
        const std::string prefix = "canonical_bars." + std::to_string(canonical_index++);
        (*out)[prefix + ".key"] = key;
        (*out)[prefix + ".fingerprint"] = fingerprint;
    }

    (*out)["recovery.count"] = std::to_string(recovery_by_instrument_.size());
    std::size_t recovery_index = 0;
    for (const auto& [instrument_id, recovery] : recovery_by_instrument_) {
        const std::string prefix = "recovery." + std::to_string(recovery_index++);
        (*out)[prefix + ".instrument_id"] = instrument_id;
        (*out)[prefix + ".complete_five_minute_bars"] =
            std::to_string(recovery.consecutive_complete_five_minute_bars);
    }

    std::size_t recent_count = 0;
    for (const auto& [key, states] : recent_complete_states_) {
        (void)key;
        recent_count += states.size();
    }
    (*out)["recent_states.count"] = std::to_string(recent_count);
    std::size_t recent_index = 0;
    for (const auto& [key, states] : recent_complete_states_) {
        (void)key;
        for (const auto& state : states) {
            const std::string prefix = "recent_states." + std::to_string(recent_index++);
            (*out)[prefix + ".instrument_id"] = state.instrument_id;
            (*out)[prefix + ".timeframe_minutes"] = std::to_string(state.timeframe_minutes);
            (*out)[prefix + ".bar_open"] = FormatDouble(state.bar_open);
            (*out)[prefix + ".bar_high"] = FormatDouble(state.bar_high);
            (*out)[prefix + ".bar_low"] = FormatDouble(state.bar_low);
            (*out)[prefix + ".bar_close"] = FormatDouble(state.bar_close);
            (*out)[prefix + ".analysis_bar_open"] = FormatDouble(state.analysis_bar_open);
            (*out)[prefix + ".analysis_bar_high"] = FormatDouble(state.analysis_bar_high);
            (*out)[prefix + ".analysis_bar_low"] = FormatDouble(state.analysis_bar_low);
            (*out)[prefix + ".analysis_bar_close"] = FormatDouble(state.analysis_bar_close);
            (*out)[prefix + ".analysis_price_offset"] = FormatDouble(state.analysis_price_offset);
            (*out)[prefix + ".bar_volume"] = FormatDouble(state.bar_volume);
            (*out)[prefix + ".market_regime"] =
                std::to_string(static_cast<int>(state.market_regime));
            (*out)[prefix + ".market_state_adx"] = FormatDouble(state.market_state_adx);
            (*out)[prefix + ".market_state_kama_er"] = FormatDouble(state.market_state_kama_er);
            (*out)[prefix + ".market_state_atr_ratio"] = FormatDouble(state.market_state_atr_ratio);
            (*out)[prefix + ".market_state_bars_seen"] =
                std::to_string(state.market_state_bars_seen);
            (*out)[prefix + ".market_state_decision_reason"] = state.market_state_decision_reason;
            (*out)[prefix + ".ts_ns"] = std::to_string(state.ts_ns);
        }
    }
    return true;
}

bool MarketBarPipeline::LoadStateLocked(const PersistenceState& state, std::string* error) {
    const std::string* version = RequireValue(state, "version", error);
    if (version == nullptr || *version != "2") {
        SetError(error, "unsupported market bar pipeline checkpoint version");
        return false;
    }
    EpochNanos loaded_watermark = 0;
    if (!ParseInteger(state, "last_watermark_ns", &loaded_watermark, error)) {
        return false;
    }
    BarAggregator::PersistenceState aggregator_state;
    TimeframeStateFanout::PersistenceState fanout_state;
    for (const auto& [key, value] : state) {
        if (key.rfind("aggregator.", 0) == 0) {
            aggregator_state[key.substr(std::string("aggregator.").size())] = value;
        } else if (key.rfind("fanout.", 0) == 0) {
            fanout_state[key.substr(std::string("fanout.").size())] = value;
        }
    }
    std::unordered_map<std::string, EpochNanos> loaded_tick_fingerprints;
    std::int64_t fingerprint_count = 0;
    if (!ParseInteger(state, "tick_fingerprints.count", &fingerprint_count, error) ||
        fingerprint_count < 0) {
        return false;
    }
    for (std::int64_t index = 0; index < fingerprint_count; ++index) {
        const std::string prefix = "tick_fingerprints." + std::to_string(index);
        const std::string* fingerprint = RequireValue(state, prefix + ".value", error);
        EpochNanos ts_ns = 0;
        if (fingerprint == nullptr || !ParseInteger(state, prefix + ".ts_ns", &ts_ns, error)) {
            return false;
        }
        loaded_tick_fingerprints[*fingerprint] = ts_ns;
    }

    std::unordered_map<std::string, std::string> loaded_canonical;
    std::int64_t canonical_count = 0;
    if (!ParseInteger(state, "canonical_bars.count", &canonical_count, error) ||
        canonical_count < 0) {
        return false;
    }
    for (std::int64_t index = 0; index < canonical_count; ++index) {
        const std::string prefix = "canonical_bars." + std::to_string(index);
        const std::string* key = RequireValue(state, prefix + ".key", error);
        const std::string* fingerprint = RequireValue(state, prefix + ".fingerprint", error);
        if (key == nullptr || key->empty() || fingerprint == nullptr) {
            return false;
        }
        loaded_canonical[*key] = *fingerprint;
    }

    std::unordered_map<std::string, RecoveryState> loaded_recovery;
    std::int64_t recovery_count = 0;
    if (!ParseInteger(state, "recovery.count", &recovery_count, error) || recovery_count < 0) {
        return false;
    }
    for (std::int64_t index = 0; index < recovery_count; ++index) {
        const std::string prefix = "recovery." + std::to_string(index);
        const std::string* instrument_id = RequireValue(state, prefix + ".instrument_id", error);
        RecoveryState recovery;
        if (instrument_id == nullptr || instrument_id->empty() ||
            !ParseInteger(state, prefix + ".complete_five_minute_bars",
                          &recovery.consecutive_complete_five_minute_bars, error)) {
            return false;
        }
        loaded_recovery[*instrument_id] = recovery;
    }

    std::unordered_map<std::string, std::deque<StateSnapshot7D>> loaded_recent;
    std::int64_t recent_count = 0;
    const auto recent_count_it = state.find("recent_states.count");
    if (recent_count_it != state.end()) {
        if (!ParseInteger(state, "recent_states.count", &recent_count, error) || recent_count < 0) {
            return false;
        }
        for (std::int64_t index = 0; index < recent_count; ++index) {
            const std::string prefix = "recent_states." + std::to_string(index);
            const std::string* instrument_id =
                RequireValue(state, prefix + ".instrument_id", error);
            const std::string* decision_reason =
                RequireValue(state, prefix + ".market_state_decision_reason", error);
            StateSnapshot7D recent;
            int regime = 0;
            if (instrument_id == nullptr || instrument_id->empty() || decision_reason == nullptr ||
                !ParseInteger(state, prefix + ".timeframe_minutes", &recent.timeframe_minutes,
                              error) ||
                !ParseDouble(state, prefix + ".bar_open", &recent.bar_open, error) ||
                !ParseDouble(state, prefix + ".bar_high", &recent.bar_high, error) ||
                !ParseDouble(state, prefix + ".bar_low", &recent.bar_low, error) ||
                !ParseDouble(state, prefix + ".bar_close", &recent.bar_close, error) ||
                !ParseDouble(state, prefix + ".analysis_bar_open", &recent.analysis_bar_open,
                             error) ||
                !ParseDouble(state, prefix + ".analysis_bar_high", &recent.analysis_bar_high,
                             error) ||
                !ParseDouble(state, prefix + ".analysis_bar_low", &recent.analysis_bar_low,
                             error) ||
                !ParseDouble(state, prefix + ".analysis_bar_close", &recent.analysis_bar_close,
                             error) ||
                !ParseDouble(state, prefix + ".analysis_price_offset",
                             &recent.analysis_price_offset, error) ||
                !ParseDouble(state, prefix + ".bar_volume", &recent.bar_volume, error) ||
                !ParseInteger(state, prefix + ".market_regime", &regime, error) ||
                !ParseDouble(state, prefix + ".market_state_adx", &recent.market_state_adx,
                             error) ||
                !ParseDouble(state, prefix + ".market_state_kama_er", &recent.market_state_kama_er,
                             error) ||
                !ParseDouble(state, prefix + ".market_state_atr_ratio",
                             &recent.market_state_atr_ratio, error) ||
                !ParseInteger(state, prefix + ".market_state_bars_seen",
                              &recent.market_state_bars_seen, error) ||
                !ParseInteger(state, prefix + ".ts_ns", &recent.ts_ns, error)) {
                return false;
            }
            if (regime < static_cast<int>(MarketRegime::kUnknown) ||
                regime > static_cast<int>(MarketRegime::kFlat)) {
                SetError(error, "invalid recent state market regime");
                return false;
            }
            recent.instrument_id = *instrument_id;
            recent.market_regime = static_cast<MarketRegime>(regime);
            recent.market_state_decision_reason = *decision_reason;
            recent.has_bar = true;
            auto& rows = loaded_recent[recent.instrument_id + "|" +
                                       std::to_string(recent.timeframe_minutes)];
            rows.push_back(std::move(recent));
            const std::size_t limit = std::max<std::size_t>(1, config_.recent_complete_state_limit);
            while (rows.size() > limit) {
                rows.pop_front();
            }
        }
    }

    // Validate all nested state before mutating the live pipeline.  This prevents a valid
    // aggregator section followed by a corrupt fanout section from producing a half-restore.
    BarAggregator validated_aggregator(config_.bar_aggregator);
    TimeframeStateFanout validated_fanout(config_.timeframes, config_.detector,
                                          config_.detector_by_product);
    if (!validated_aggregator.LoadState(aggregator_state, error) ||
        !validated_fanout.LoadState(fanout_state, error)) {
        return false;
    }
    if (!bar_aggregator_.LoadState(aggregator_state, error) ||
        !timeframe_fanout_.LoadState(fanout_state, error)) {
        SetError(error, "validated market bar checkpoint failed to install");
        return false;
    }

    last_watermark_ns_ = loaded_watermark;
    tick_fingerprint_seen_ts_ = std::move(loaded_tick_fingerprints);
    canonical_bar_fingerprints_ = std::move(loaded_canonical);
    recovery_by_instrument_ = std::move(loaded_recovery);
    recent_complete_states_ = std::move(loaded_recent);
    replaying_ = false;
    return true;
}

}  // namespace quant_hft
