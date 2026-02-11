#pragma once

#include <string>
#include <vector>

#include "quant_hft/interfaces/risk_engine.h"

namespace quant_hft {

struct BasicRiskLimits {
    std::int32_t max_order_volume{200};
    double max_order_notional{1'000'000.0};
    std::string rule_group{"default"};
    std::string rule_version{"v1"};
};

struct BasicRiskRule {
    std::string rule_id;
    std::string rule_group;
    std::string rule_version{"v1"};
    std::string account_id;
    std::string instrument_id;
    int window_start_hhmm{0};
    int window_end_hhmm{2359};
    std::int32_t max_order_volume{200};
    double max_order_notional{1'000'000.0};
};

class BasicRiskEngine : public IRiskEngine {
public:
    explicit BasicRiskEngine(BasicRiskLimits limits);
    BasicRiskEngine(BasicRiskLimits limits, std::vector<BasicRiskRule> rules);

    RiskDecision PreCheck(const OrderIntent& intent) const;
    RiskDecision PreCheck(const OrderIntent& intent,
                          const RiskContext& context) const override;

private:
    const BasicRiskRule* MatchRule(const OrderIntent& intent) const;
    static int ToUtcHhmm(EpochNanos ts_ns);
    static bool MatchesTimeWindow(int hhmm, int start_hhmm, int end_hhmm);

    BasicRiskLimits limits_;
    std::vector<BasicRiskRule> rules_;
};

}  // namespace quant_hft
