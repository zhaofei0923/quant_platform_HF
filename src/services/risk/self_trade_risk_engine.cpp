#include "quant_hft/services/self_trade_risk_engine.h"

#include <algorithm>

namespace quant_hft {

SelfTradeRiskEngine::SelfTradeRiskEngine(SelfTradeRiskConfig config)
    : config_(std::move(config)), strict_mode_(config_.strict_mode) {}

RiskDecision SelfTradeRiskEngine::PreCheck(const OrderIntent& intent) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!config_.enabled) {
        return BuildDecision(RiskAction::kAllow, "self_trade_check_disabled", 0.0, 0.0);
    }

    const ActiveOrder* crossing_order = nullptr;
    for (const auto& [client_order_id, resting] : active_orders_) {
        (void)client_order_id;
        if (resting.remaining_volume <= 0) {
            continue;
        }
        if (resting.account_id != intent.account_id ||
            resting.instrument_id != intent.instrument_id) {
            continue;
        }
        if (!IsCrossing(intent, resting)) {
            continue;
        }
        crossing_order = &resting;
        break;
    }

    if (crossing_order == nullptr) {
        return BuildDecision(RiskAction::kAllow, "self_trade_check_pass", 0.0, 0.0);
    }

    ++conflict_hits_;
    const bool enforce_strict = strict_mode_ || config_.strict_mode_trigger_hits <= 0 ||
                                conflict_hits_ >= config_.strict_mode_trigger_hits;
    if (enforce_strict) {
        strict_mode_ = true;
        return BuildDecision(RiskAction::kReject,
                             "self_trade_blocked_crossing_order",
                             intent.price,
                             crossing_order->price);
    }

    return BuildDecision(
        RiskAction::kAllow, "self_trade_warn_only_threshold_not_reached", intent.price, crossing_order->price);
}

void SelfTradeRiskEngine::RecordAcceptedOrder(const OrderIntent& intent) {
    if (intent.client_order_id.empty() || intent.account_id.empty() ||
        intent.instrument_id.empty() || intent.volume <= 0) {
        return;
    }
    ActiveOrder order;
    order.account_id = intent.account_id;
    order.instrument_id = intent.instrument_id;
    order.side = intent.side;
    order.price = intent.price;
    order.remaining_volume = intent.volume;
    order.last_filled_volume = 0;

    std::lock_guard<std::mutex> lock(mutex_);
    active_orders_[intent.client_order_id] = std::move(order);
}

void SelfTradeRiskEngine::OnOrderEvent(const OrderEvent& event) {
    if (event.client_order_id.empty()) {
        return;
    }

    std::lock_guard<std::mutex> lock(mutex_);
    auto it = active_orders_.find(event.client_order_id);
    if (it == active_orders_.end()) {
        return;
    }

    auto& active = it->second;
    if (event.total_volume > 0) {
        active.remaining_volume = std::max(0, event.total_volume - event.filled_volume);
    } else if (event.filled_volume > active.last_filled_volume) {
        const auto delta = event.filled_volume - active.last_filled_volume;
        active.remaining_volume = std::max(0, active.remaining_volume - delta);
    }
    active.last_filled_volume = std::max(active.last_filled_volume, event.filled_volume);

    if (IsTerminalStatus(event.status) || active.remaining_volume == 0) {
        active_orders_.erase(it);
    }
}

bool SelfTradeRiskEngine::strict_mode() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return strict_mode_;
}

int SelfTradeRiskEngine::conflict_hits() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return conflict_hits_;
}

bool SelfTradeRiskEngine::IsTerminalStatus(OrderStatus status) {
    return status == OrderStatus::kFilled || status == OrderStatus::kCanceled ||
           status == OrderStatus::kRejected;
}

bool SelfTradeRiskEngine::IsCrossing(const OrderIntent& intent, const ActiveOrder& resting) {
    if (intent.side == resting.side) {
        return false;
    }
    if (intent.side == Side::kBuy) {
        return intent.price >= resting.price;
    }
    return intent.price <= resting.price;
}

RiskDecision SelfTradeRiskEngine::BuildDecision(RiskAction action,
                                                const std::string& reason,
                                                double observed_value,
                                                double threshold_value) {
    RiskDecision decision;
    decision.action = action;
    decision.rule_id = "policy.self_trade.cross";
    decision.rule_group = "self_trade";
    decision.rule_version = "v1";
    decision.policy_id = "policy.self_trade";
    decision.policy_scope = "account_instrument";
    decision.decision_tags = "risk,self_trade";
    decision.reason = reason;
    decision.observed_value = observed_value;
    decision.threshold_value = threshold_value;
    decision.decision_ts_ns = NowEpochNanos();
    return decision;
}

}  // namespace quant_hft
