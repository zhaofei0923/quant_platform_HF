#pragma once

#include <string>
#include <vector>

#include "quant_hft/interfaces/risk_engine.h"

namespace quant_hft {

struct RiskPolicyDefaults {
    std::int32_t max_order_volume{200};
    double max_order_notional{1'000'000.0};
    std::int32_t max_active_orders{0};
    double max_position_notional{0.0};
    std::int32_t max_cancel_count{0};
    double max_cancel_ratio{0.0};
    std::string policy_id{"policy.global"};
    std::string policy_scope{"global"};
    std::string decision_tags;
    std::string rule_group{"default"};
    std::string rule_version{"v1"};
};

struct RiskPolicyRule {
    std::string policy_id;
    std::string policy_scope;
    std::string account_id;
    std::string instrument_id;
    std::string exchange_id;
    int window_start_hhmm{0};
    int window_end_hhmm{2359};
    std::int32_t max_order_volume{0};
    double max_order_notional{0.0};
    std::int32_t max_active_orders{0};
    double max_position_notional{0.0};
    std::int32_t max_cancel_count{0};
    double max_cancel_ratio{0.0};
    std::string decision_tags;
    std::string rule_group;
    std::string rule_version{"v1"};
};

class RiskPolicyEngine : public IRiskEngine {
public:
    explicit RiskPolicyEngine(RiskPolicyDefaults defaults);
    RiskPolicyEngine(RiskPolicyDefaults defaults, std::vector<RiskPolicyRule> rules);

    RiskDecision PreCheck(const OrderIntent& intent,
                          const RiskContext& context) const override;
    bool ReloadPolicies(const std::vector<RiskPolicyDefinition>& policies,
                        std::string* error) override;
    double EvaluateExposure(const RiskContext& context) const override;

private:
    const RiskPolicyRule* MatchRule(const OrderIntent& intent,
                                    const RiskContext& context,
                                    int hhmm) const;
    static int ToUtcHhmm(EpochNanos ts_ns);
    static bool MatchesTimeWindow(int hhmm, int start_hhmm, int end_hhmm);

    RiskPolicyDefaults defaults_;
    std::vector<RiskPolicyRule> rules_;
};

}  // namespace quant_hft
