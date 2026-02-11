#include "quant_hft/services/basic_risk_engine.h"

#include <cmath>
#include <ctime>
#include <utility>

namespace quant_hft {

BasicRiskEngine::BasicRiskEngine(BasicRiskLimits limits) : limits_(limits) {}

BasicRiskEngine::BasicRiskEngine(BasicRiskLimits limits, std::vector<BasicRiskRule> rules)
    : limits_(std::move(limits)), rules_(std::move(rules)) {}

RiskDecision BasicRiskEngine::PreCheck(const OrderIntent& intent) const {
    const auto decision_ts = NowEpochNanos();
    const auto* matched_rule = MatchRule(intent);
    const std::int32_t max_volume = matched_rule == nullptr ? limits_.max_order_volume
                                                            : matched_rule->max_order_volume;
    const double max_notional = matched_rule == nullptr ? limits_.max_order_notional
                                                        : matched_rule->max_order_notional;
    const std::string rule_group = matched_rule == nullptr ? limits_.rule_group
                                                           : matched_rule->rule_group;
    const std::string rule_version = matched_rule == nullptr ? limits_.rule_version
                                                             : matched_rule->rule_version;

    auto build_decision = [&](RiskAction action,
                              const std::string& rule_id,
                              const std::string& reason) -> RiskDecision {
        RiskDecision decision;
        decision.action = action;
        decision.rule_id = rule_id;
        decision.rule_group = rule_group.empty() ? "default" : rule_group;
        decision.rule_version = rule_version.empty() ? "v1" : rule_version;
        decision.reason = reason;
        decision.decision_ts_ns = decision_ts;
        return decision;
    };

    const std::string rule_prefix =
        matched_rule == nullptr ? "risk.default" : matched_rule->rule_id;
    if (intent.volume <= 0) {
        return build_decision(RiskAction::kReject,
                              rule_prefix + ".non_positive_volume",
                              "volume must be positive");
    }
    if (intent.volume > max_volume) {
        return build_decision(RiskAction::kReject,
                              rule_prefix + ".max_order_volume",
                              "volume exceeds max order volume");
    }
    const auto notional = std::fabs(intent.price) * static_cast<double>(intent.volume);
    if (notional > max_notional) {
        return build_decision(RiskAction::kReject,
                              rule_prefix + ".max_order_notional",
                              "notional exceeds max per-order notional");
    }
    return build_decision(RiskAction::kAllow, rule_prefix + ".allow", "pass");
}

const BasicRiskRule* BasicRiskEngine::MatchRule(const OrderIntent& intent) const {
    const int hhmm = ToUtcHhmm(intent.ts_ns == 0 ? NowEpochNanos() : intent.ts_ns);
    const BasicRiskRule* selected = nullptr;
    int best_score = -1;

    for (const auto& rule : rules_) {
        int score = 0;
        if (!rule.account_id.empty()) {
            if (rule.account_id != intent.account_id) {
                continue;
            }
            score += 4;
        }
        if (!rule.instrument_id.empty()) {
            if (rule.instrument_id != intent.instrument_id) {
                continue;
            }
            score += 2;
        }
        if (!MatchesTimeWindow(hhmm, rule.window_start_hhmm, rule.window_end_hhmm)) {
            continue;
        }
        score += 1;

        if (score > best_score) {
            best_score = score;
            selected = &rule;
        }
    }
    return selected;
}

int BasicRiskEngine::ToUtcHhmm(EpochNanos ts_ns) {
    const std::time_t seconds = static_cast<std::time_t>(ts_ns / 1'000'000'000);
    std::tm tm_utc{};
#if defined(_WIN32)
    gmtime_s(&tm_utc, &seconds);
#else
    gmtime_r(&seconds, &tm_utc);
#endif
    return tm_utc.tm_hour * 100 + tm_utc.tm_min;
}

bool BasicRiskEngine::MatchesTimeWindow(int hhmm, int start_hhmm, int end_hhmm) {
    if (start_hhmm == end_hhmm) {
        return true;
    }
    if (start_hhmm < end_hhmm) {
        return hhmm >= start_hhmm && hhmm <= end_hhmm;
    }
    return hhmm >= start_hhmm || hhmm <= end_hhmm;
}

}  // namespace quant_hft
