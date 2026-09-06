#pragma once

#include <algorithm>
#include <map>
#include <mutex>
#include <stdexcept>
#include <string>
#include <vector>

#include "quant_hft/services/timeframe_state_fanout.h"
#include "quant_hft/strategy/live_strategy.h"

namespace quant_hft {

// Bounded recovery evidence; it never resets strategy/risk state itself. A generation
// prevents a late recovery acknowledgement from clearing a newer delivery gap.
class MarketGapRecovery {
   public:
    struct Pending {
        MarketGapContext context;
        std::vector<StateSnapshot7D> states;
        MarketWarmupRequirements required;
    };
    void MarkGap(const std::string& instrument, EpochNanos after_ns) {
        if (instrument.empty()) return;
        std::lock_guard<std::mutex> lock(mutex_);
        auto& gap = gaps_[instrument];
        gap = {};
        gap.generation = ++generation_;
        gap.after_ns = after_ns;
    }
    bool Suppresses(const std::string& instrument) const {
        std::lock_guard<std::mutex> lock(mutex_);
        return gaps_.count(instrument) != 0;
    }
    bool Observe(const TimeframeStateEmission& emission) {
        std::lock_guard<std::mutex> lock(mutex_);
        const auto found = gaps_.find(emission.bar.instrument_id);
        if (found == gaps_.end()) return false;
        auto& gap = found->second;
        const auto& bar = emission.bar;
        if (bar.is_session_endpoint || bar.is_recovery_replay) return true;
        auto& states = gap.states[emission.timeframe_minutes];
        if (!bar.is_complete || !bar.volume_complete || bar.has_conflict ||
            bar.expected_source_bars != bar.observed_source_bars) {
            states.clear();
            return true;
        }
        const auto start = bar.period_end_ts_ns -
                           static_cast<EpochNanos>(emission.timeframe_minutes) * 60'000'000'000LL;
        if (!emission.strategy_eligible || !bar.strategy_eligible || !emission.state.has_bar ||
            start < gap.after_ns || emission.state.ts_ns <= gap.after_ns)
            return true;
        states.emplace(emission.state.ts_ns, emission.state);
        const auto required = gap.required.find(emission.timeframe_minutes);
        const std::size_t limit = required == gap.required.end()
                                      ? 4096U
                                      : static_cast<std::size_t>(std::max(2, required->second));
        while (states.size() > std::min<std::size_t>(4096U, limit)) states.erase(states.begin());
        return true;
    }
    std::vector<Pending> Snapshot() const {
        std::lock_guard<std::mutex> lock(mutex_);
        std::vector<Pending> result;
        for (const auto& item : gaps_) {
            Pending pending;
            pending.context = {item.first, item.second.generation};
            pending.required = item.second.required;
            for (const auto& frame : item.second.states)
                for (const auto& state : frame.second) pending.states.push_back(state.second);
            result.push_back(std::move(pending));
        }
        return result;
    }
    void SetRequirements(const MarketGapContext& context, MarketWarmupRequirements required) {
        std::lock_guard<std::mutex> lock(mutex_);
        const auto found = gaps_.find(context.instrument_id);
        if (found != gaps_.end() && found->second.generation == context.generation)
            found->second.required = std::move(required);
    }
    bool Complete(const MarketGapContext& context) {
        std::lock_guard<std::mutex> lock(mutex_);
        const auto found = gaps_.find(context.instrument_id);
        if (found == gaps_.end() || found->second.generation != context.generation) return false;
        gaps_.erase(found);
        return true;
    }
    void SaveState(StrategyState* out) const {
        std::lock_guard<std::mutex> lock(mutex_);
        (*out)["market_gap.count"] = std::to_string(gaps_.size());
        std::size_t index = 0;
        for (const auto& gap : gaps_) {
            const auto prefix = "market_gap." + std::to_string(index++);
            (*out)[prefix + ".instrument"] = gap.first;
            (*out)[prefix + ".after_ns"] = std::to_string(gap.second.after_ns);
        }
    }
    bool LoadState(const StrategyState& state, std::string* error) {
        const auto count = state.find("market_gap.count");
        if (count == state.end()) return true;
        try {
            const auto size = std::stoull(count->second);
            if (size > 4096) throw std::runtime_error("too many market gap records");
            std::map<std::string, EpochNanos> loaded;
            for (std::size_t i = 0; i < size; ++i) {
                const auto prefix = "market_gap." + std::to_string(i);
                const auto instrument = state.at(prefix + ".instrument");
                const auto after = std::stoll(state.at(prefix + ".after_ns"));
                if (instrument.empty() || after < 0 || !loaded.emplace(instrument, after).second)
                    throw std::runtime_error("invalid market gap record");
            }
            for (const auto& gap : loaded) MarkGap(gap.first, gap.second);
            return true;
        } catch (const std::exception& ex) {
            if (error != nullptr) *error = ex.what();
            return false;
        }
    }

   private:
    struct Gap {
        std::uint64_t generation{0};
        EpochNanos after_ns{0};
        MarketWarmupRequirements required;
        std::map<std::int32_t, std::map<EpochNanos, StateSnapshot7D>> states;
    };
    mutable std::mutex mutex_;
    std::uint64_t generation_{0};
    std::map<std::string, Gap> gaps_;
};

}  // namespace quant_hft
