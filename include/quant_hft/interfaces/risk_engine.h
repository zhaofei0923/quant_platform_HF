#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "quant_hft/contracts/types.h"

namespace quant_hft {

struct RiskContext {
    std::string account_id;
    std::string instrument_id;
    std::string exchange_id;
    double account_position_notional{0.0};
    // Cross-account exposure snapshots for the same account family/book.
    double account_cross_gross_notional{0.0};
    double account_cross_net_notional{0.0};
    std::int32_t active_order_count{0};
    std::int32_t cancel_count{0};
    std::int32_t submit_count{0};
    // Optional override of session clock in hhmm format.
    // When <= 0, risk engines should derive time from intent.ts_ns.
    int session_hhmm{-1};
};

struct RiskPolicyDefinition {
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

class IRiskEngine {
public:
    virtual ~IRiskEngine() = default;
    virtual RiskDecision PreCheck(const OrderIntent& intent,
                                  const RiskContext& context) const = 0;
    virtual bool ReloadPolicies(const std::vector<RiskPolicyDefinition>& policies,
                                std::string* error) = 0;
    virtual double EvaluateExposure(const RiskContext& context) const = 0;
};

}  // namespace quant_hft
