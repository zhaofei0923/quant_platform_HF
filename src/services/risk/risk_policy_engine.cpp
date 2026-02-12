#include "quant_hft/services/risk_policy_engine.h"

#include <cmath>
#include <ctime>
#include <utility>

namespace quant_hft {

RiskPolicyEngine::RiskPolicyEngine(RiskPolicyDefaults defaults) : defaults_(std::move(defaults)) {}

RiskPolicyEngine::RiskPolicyEngine(RiskPolicyDefaults defaults,
                                   std::vector<RiskPolicyRule> rules)
    : defaults_(std::move(defaults)), rules_(std::move(rules)) {}

RiskDecision RiskPolicyEngine::PreCheck(const OrderIntent& intent,
                                        const RiskContext& context) const {
    const auto decision_ts = NowEpochNanos();
    const int hhmm = context.session_hhmm > 0
                         ? context.session_hhmm
                         : ToUtcHhmm(intent.ts_ns == 0 ? decision_ts : intent.ts_ns);
    const auto* matched = MatchRule(intent, context, hhmm);

    const std::int32_t max_order_volume = matched != nullptr && matched->max_order_volume > 0
                                              ? matched->max_order_volume
                                              : defaults_.max_order_volume;
    const double max_order_notional = matched != nullptr && matched->max_order_notional > 0.0
                                          ? matched->max_order_notional
                                          : defaults_.max_order_notional;
    const std::int32_t max_active_orders = matched != nullptr && matched->max_active_orders > 0
                                               ? matched->max_active_orders
                                               : defaults_.max_active_orders;
    const double max_position_notional =
        matched != nullptr && matched->max_position_notional > 0.0
            ? matched->max_position_notional
            : defaults_.max_position_notional;
    const std::int32_t max_cancel_count = matched != nullptr && matched->max_cancel_count > 0
                                              ? matched->max_cancel_count
                                              : defaults_.max_cancel_count;
    const double max_cancel_ratio = matched != nullptr && matched->max_cancel_ratio > 0.0
                                        ? matched->max_cancel_ratio
                                        : defaults_.max_cancel_ratio;

    const std::string policy_id = matched == nullptr || matched->policy_id.empty()
                                      ? defaults_.policy_id
                                      : matched->policy_id;
    const std::string policy_scope = matched == nullptr || matched->policy_scope.empty()
                                         ? defaults_.policy_scope
                                         : matched->policy_scope;
    const std::string decision_tags = matched == nullptr || matched->decision_tags.empty()
                                          ? defaults_.decision_tags
                                          : matched->decision_tags;
    const std::string rule_group = matched == nullptr || matched->rule_group.empty()
                                       ? defaults_.rule_group
                                       : matched->rule_group;
    const std::string rule_version = matched == nullptr || matched->rule_version.empty()
                                         ? defaults_.rule_version
                                         : matched->rule_version;

    auto build = [&](RiskAction action,
                     const std::string& rule_id_suffix,
                     const std::string& reason,
                     double observed_value,
                     double threshold_value) -> RiskDecision {
        RiskDecision decision;
        decision.action = action;
        decision.rule_id = policy_id + "." + rule_id_suffix;
        decision.rule_group = rule_group.empty() ? "default" : rule_group;
        decision.rule_version = rule_version.empty() ? "v1" : rule_version;
        decision.policy_id = policy_id;
        decision.policy_scope = policy_scope.empty() ? "global" : policy_scope;
        decision.observed_value = observed_value;
        decision.threshold_value = threshold_value;
        decision.decision_tags = decision_tags;
        decision.reason = reason;
        decision.decision_ts_ns = decision_ts;
        return decision;
    };

    if (intent.volume <= 0) {
        return build(RiskAction::kReject,
                     "non_positive_volume",
                     "volume must be positive",
                     static_cast<double>(intent.volume),
                     0.0);
    }
    if (max_order_volume > 0 && intent.volume > max_order_volume) {
        return build(RiskAction::kReject,
                     "max_order_volume",
                     "volume exceeds max order volume",
                     static_cast<double>(intent.volume),
                     static_cast<double>(max_order_volume));
    }

    const double order_notional = std::fabs(intent.price) * static_cast<double>(intent.volume);
    if (max_order_notional > 0.0 && order_notional > max_order_notional) {
        return build(RiskAction::kReject,
                     "max_order_notional",
                     "notional exceeds max per-order notional",
                     order_notional,
                     max_order_notional);
    }

    if (max_active_orders > 0 && context.active_order_count > max_active_orders) {
        return build(RiskAction::kReject,
                     "max_active_orders",
                     "active order count exceeds policy",
                     static_cast<double>(context.active_order_count),
                     static_cast<double>(max_active_orders));
    }

    if (max_position_notional > 0.0 &&
        std::fabs(context.account_position_notional) > max_position_notional) {
        return build(RiskAction::kReject,
                     "max_position_notional",
                     "account position notional exceeds policy",
                     std::fabs(context.account_position_notional),
                     max_position_notional);
    }

    if (max_cancel_count > 0 && context.cancel_count > max_cancel_count) {
        return build(RiskAction::kReject,
                     "max_cancel_count",
                     "cancel count exceeds policy",
                     static_cast<double>(context.cancel_count),
                     static_cast<double>(max_cancel_count));
    }

    const double cancel_ratio = context.submit_count > 0
                                    ? static_cast<double>(context.cancel_count) /
                                          static_cast<double>(context.submit_count)
                                    : 0.0;
    if (max_cancel_ratio > 0.0 && cancel_ratio > max_cancel_ratio) {
        return build(RiskAction::kReject,
                     "max_cancel_ratio",
                     "cancel ratio exceeds policy",
                     cancel_ratio,
                     max_cancel_ratio);
    }

    return build(RiskAction::kAllow, "allow", "pass", 0.0, 0.0);
}

bool RiskPolicyEngine::ReloadPolicies(const std::vector<RiskPolicyDefinition>& policies,
                                      std::string* error) {
    std::vector<RiskPolicyRule> reloaded;
    reloaded.reserve(policies.size());
    for (const auto& policy : policies) {
        if (policy.policy_id.empty()) {
            if (error != nullptr) {
                *error = "policy_id is required";
            }
            return false;
        }
        if (policy.max_order_volume < 0 || policy.max_order_notional < 0.0 ||
            policy.max_active_orders < 0 || policy.max_position_notional < 0.0 ||
            policy.max_cancel_count < 0 || policy.max_cancel_ratio < 0.0) {
            if (error != nullptr) {
                *error = "policy thresholds must be non-negative";
            }
            return false;
        }
        RiskPolicyRule rule;
        rule.policy_id = policy.policy_id;
        rule.policy_scope = policy.policy_scope.empty() ? "global" : policy.policy_scope;
        rule.account_id = policy.account_id;
        rule.instrument_id = policy.instrument_id;
        rule.exchange_id = policy.exchange_id;
        rule.window_start_hhmm = policy.window_start_hhmm;
        rule.window_end_hhmm = policy.window_end_hhmm;
        rule.max_order_volume = policy.max_order_volume;
        rule.max_order_notional = policy.max_order_notional;
        rule.max_active_orders = policy.max_active_orders;
        rule.max_position_notional = policy.max_position_notional;
        rule.max_cancel_count = policy.max_cancel_count;
        rule.max_cancel_ratio = policy.max_cancel_ratio;
        rule.decision_tags = policy.decision_tags;
        rule.rule_group = policy.rule_group;
        rule.rule_version = policy.rule_version;
        reloaded.push_back(std::move(rule));
    }
    rules_ = std::move(reloaded);
    if (error != nullptr) {
        error->clear();
    }
    return true;
}

double RiskPolicyEngine::EvaluateExposure(const RiskContext& context) const {
    return std::fabs(context.account_position_notional) +
           std::fabs(context.account_cross_gross_notional) +
           std::fabs(context.account_cross_net_notional);
}

const RiskPolicyRule* RiskPolicyEngine::MatchRule(const OrderIntent& intent,
                                                  const RiskContext& context,
                                                  int hhmm) const {
    const RiskPolicyRule* selected = nullptr;
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
        if (!rule.exchange_id.empty()) {
            if (rule.exchange_id != context.exchange_id) {
                continue;
            }
            score += 1;
        }
        if (!MatchesTimeWindow(hhmm, rule.window_start_hhmm, rule.window_end_hhmm)) {
            continue;
        }
        if (rule.window_start_hhmm != 0 || rule.window_end_hhmm != 2359) {
            score += 1;
        }

        if (score > best_score) {
            best_score = score;
            selected = &rule;
        }
    }
    return selected;
}

int RiskPolicyEngine::ToUtcHhmm(EpochNanos ts_ns) {
    const std::time_t seconds = static_cast<std::time_t>(ts_ns / 1'000'000'000);
    std::tm tm_utc{};
#if defined(_WIN32)
    gmtime_s(&tm_utc, &seconds);
#else
    gmtime_r(&seconds, &tm_utc);
#endif
    return tm_utc.tm_hour * 100 + tm_utc.tm_min;
}

bool RiskPolicyEngine::MatchesTimeWindow(int hhmm, int start_hhmm, int end_hhmm) {
    if (start_hhmm == end_hhmm) {
        return true;
    }
    if (start_hhmm < end_hhmm) {
        return hhmm >= start_hhmm && hhmm <= end_hhmm;
    }
    return hhmm >= start_hhmm || hhmm <= end_hhmm;
}

}  // namespace quant_hft
