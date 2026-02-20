#include "quant_hft/strategy/signal_merger.h"

#include <algorithm>
#include <unordered_map>

namespace quant_hft {
namespace {

int SignalPriority(SignalType signal_type) {
    switch (signal_type) {
        case SignalType::kForceClose:
            return 0;
        case SignalType::kStopLoss:
            return 1;
        case SignalType::kTakeProfit:
            return 2;
        case SignalType::kClose:
            return 3;
        case SignalType::kOpen:
        default:
            return 4;
    }
}

bool IsPreferredSignal(const SignalIntent& lhs, const SignalIntent& rhs) {
    const int lhs_priority = SignalPriority(lhs.signal_type);
    const int rhs_priority = SignalPriority(rhs.signal_type);
    if (lhs_priority != rhs_priority) {
        return lhs_priority < rhs_priority;
    }
    if (lhs.volume != rhs.volume) {
        return lhs.volume > rhs.volume;
    }
    if (lhs.ts_ns != rhs.ts_ns) {
        return lhs.ts_ns > rhs.ts_ns;
    }
    return lhs.trace_id < rhs.trace_id;
}

}  // namespace

std::vector<SignalIntent> PrioritySignalMerger::Merge(
    const std::vector<SignalIntent>& signals) const {
    if (signals.empty()) {
        return {};
    }

    std::unordered_map<std::string, SignalIntent> best_by_instrument;
    for (const SignalIntent& signal : signals) {
        if (signal.instrument_id.empty()) {
            continue;
        }
        const auto it = best_by_instrument.find(signal.instrument_id);
        if (it == best_by_instrument.end() || IsPreferredSignal(signal, it->second)) {
            best_by_instrument[signal.instrument_id] = signal;
        }
    }

    std::vector<SignalIntent> merged;
    merged.reserve(best_by_instrument.size());
    for (const auto& item : best_by_instrument) {
        merged.push_back(item.second);
    }
    std::sort(merged.begin(), merged.end(), [](const SignalIntent& lhs, const SignalIntent& rhs) {
        return lhs.instrument_id < rhs.instrument_id;
    });
    return merged;
}

std::unique_ptr<ISignalMerger> CreateSignalMerger(SignalMergeRule rule, std::string* error) {
    switch (rule) {
        case SignalMergeRule::kPriority:
            return std::make_unique<PrioritySignalMerger>();
        default:
            if (error != nullptr) {
                *error = "unsupported SignalMergeRule";
            }
            return nullptr;
    }
}

std::unique_ptr<ISignalMerger> CreateSignalMerger(const std::string& rule, std::string* error) {
    if (rule == "kPriority" || rule == "priority") {
        return std::make_unique<PrioritySignalMerger>();
    }
    if (error != nullptr) {
        *error = "unsupported signal merger rule: " + rule;
    }
    return nullptr;
}

}  // namespace quant_hft
