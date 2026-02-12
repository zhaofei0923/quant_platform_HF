#include "quant_hft/risk/risk_rule_registry.h"

#include <algorithm>
#include <cmath>

#include "quant_hft/services/order_manager.h"

namespace quant_hft {
namespace {

bool IsOpenOrder(const OrderIntent& intent) {
    return intent.offset == OffsetFlag::kOpen;
}

bool IsCrossingPrice(const OrderIntent& intent, const Order& resting) {
    if (intent.side == resting.side) {
        return false;
    }
    if (intent.type != OrderType::kLimit || resting.order_type != OrderType::kLimit) {
        return false;
    }
    if (intent.side == Side::kBuy) {
        return intent.price >= resting.price;
    }
    return intent.price <= resting.price;
}

RiskCheckResult AllowResult() {
    RiskCheckResult result;
    result.allowed = true;
    return result;
}

}  // namespace

void RegisterDefaultRiskRules(RiskRuleExecutor* executor,
                              const std::shared_ptr<OrderManager>& order_manager,
                              bool enable_self_trade_prevention,
                              const std::function<bool(const std::string&, double, int)>& consume_rate_token) {
    if (executor == nullptr) {
        return;
    }

    executor->RegisterRule(
        RiskRuleType::MAX_LOSS_PER_ORDER,
        [](const RiskRule& rule, const OrderIntent& intent, const OrderContext& context) {
            const double mark_price = context.current_price > 0.0 ? context.current_price : intent.price;
            const double multiplier = context.contract_multiplier > 0.0 ? context.contract_multiplier : 1.0;
            const double estimated_loss =
                std::fabs(intent.price - mark_price) * static_cast<double>(intent.volume) * multiplier;
            if (rule.threshold > 0.0 && estimated_loss > rule.threshold) {
                RiskCheckResult result;
                result.allowed = false;
                result.violated_rule = RiskRuleType::MAX_LOSS_PER_ORDER;
                result.reason = "单笔预估亏损超过上限";
                result.limit_value = rule.threshold;
                result.current_value = estimated_loss;
                return result;
            }
            return AllowResult();
        });

    executor->RegisterRule(
        RiskRuleType::MAX_ORDER_VOLUME,
        [](const RiskRule& rule, const OrderIntent& intent, const OrderContext&) {
            if (rule.threshold > 0.0 && static_cast<double>(intent.volume) > rule.threshold) {
                RiskCheckResult result;
                result.allowed = false;
                result.violated_rule = RiskRuleType::MAX_ORDER_VOLUME;
                result.reason = "单笔报单手数超过上限";
                result.limit_value = rule.threshold;
                result.current_value = static_cast<double>(intent.volume);
                return result;
            }
            return AllowResult();
        });

    executor->RegisterRule(
        RiskRuleType::MAX_ORDER_RATE,
        [consume_rate_token](const RiskRule& rule, const OrderIntent&, const OrderContext& context) {
            if (rule.threshold <= 0.0) {
                return AllowResult();
            }
            const auto key = context.strategy_id.empty() ? "__global__" : context.strategy_id;
            const bool pass = consume_rate_token(key, rule.threshold, 0);
            if (!pass) {
                RiskCheckResult result;
                result.allowed = false;
                result.violated_rule = RiskRuleType::MAX_ORDER_RATE;
                result.reason = "报单频率超限";
                result.limit_value = rule.threshold;
                return result;
            }
            return AllowResult();
        });

    executor->RegisterRule(
        RiskRuleType::MAX_CANCEL_RATE,
        [consume_rate_token](const RiskRule& rule, const OrderIntent&, const OrderContext& context) {
            if (rule.threshold <= 0.0) {
                return AllowResult();
            }
            const auto key = context.strategy_id.empty() ? "__global__" : context.strategy_id;
            const bool pass = consume_rate_token(key, rule.threshold, 1);
            if (!pass) {
                RiskCheckResult result;
                result.allowed = false;
                result.violated_rule = RiskRuleType::MAX_CANCEL_RATE;
                result.reason = "撤单频率超限";
                result.limit_value = rule.threshold;
                return result;
            }
            return AllowResult();
        });

    executor->RegisterRule(
        RiskRuleType::MAX_POSITION_PER_INSTRUMENT,
        [](const RiskRule& rule, const OrderIntent&, const OrderContext& context) {
            if (rule.threshold > 0.0 && std::fabs(context.current_position) > rule.threshold) {
                RiskCheckResult result;
                result.allowed = false;
                result.violated_rule = RiskRuleType::MAX_POSITION_PER_INSTRUMENT;
                result.reason = "单合约持仓超过上限";
                result.limit_value = rule.threshold;
                result.current_value = std::fabs(context.current_position);
                return result;
            }
            return AllowResult();
        });

    executor->RegisterRule(
        RiskRuleType::DAILY_LOSS_LIMIT,
        [](const RiskRule& rule, const OrderIntent&, const OrderContext& context) {
            const double daily_loss = context.today_pnl < 0.0 ? std::fabs(context.today_pnl) : 0.0;
            if (rule.threshold > 0.0 && daily_loss > rule.threshold) {
                RiskCheckResult result;
                result.allowed = false;
                result.violated_rule = RiskRuleType::DAILY_LOSS_LIMIT;
                result.reason = "当日亏损超过上限";
                result.limit_value = rule.threshold;
                result.current_value = daily_loss;
                return result;
            }
            return AllowResult();
        });

    executor->RegisterRule(
        RiskRuleType::SELF_TRADE_PREVENTION,
        [order_manager, enable_self_trade_prevention](const RiskRule& rule,
                                                       const OrderIntent& intent,
                                                       const OrderContext& context) {
            if (!enable_self_trade_prevention || rule.threshold <= 0.0) {
                return AllowResult();
            }
            if (!IsOpenOrder(intent)) {
                return AllowResult();
            }
            if (order_manager == nullptr) {
                return AllowResult();
            }

            const auto active_orders =
                order_manager->GetActiveOrdersByStrategy(context.strategy_id, context.instrument_id);
            for (const auto& order : active_orders) {
                if (order.symbol != context.instrument_id) {
                    continue;
                }
                if (order.offset != OffsetFlag::kOpen) {
                    continue;
                }
                if (!IsCrossingPrice(intent, order)) {
                    continue;
                }
                if (intent.side == Side::kBuy) {
                    RiskCheckResult result;
                    result.allowed = false;
                    result.violated_rule = RiskRuleType::SELF_TRADE_PREVENTION;
                    result.reason = "可能自成交：买入价≥已有卖出挂单价";
                    return result;
                }
                RiskCheckResult result;
                result.allowed = false;
                result.violated_rule = RiskRuleType::SELF_TRADE_PREVENTION;
                result.reason = "可能自成交：卖出价≤已有买入挂单价";
                return result;
            }
            return AllowResult();
        });
}

}  // namespace quant_hft
