#include "quant_hft/services/in_memory_order_gateway.h"

#include <utility>

namespace quant_hft {

bool InMemoryOrderGateway::PlaceOrder(const OrderIntent& intent) {
    std::lock_guard<std::mutex> lock(mutex_);
    active_orders_[intent.client_order_id] = intent;

    if (callback_) {
        OrderEvent event;
        event.account_id = intent.account_id;
        event.client_order_id = intent.client_order_id;
        event.exchange_order_id = "sim-" + intent.client_order_id;
        event.instrument_id = intent.instrument_id;
        event.status = OrderStatus::kAccepted;
        event.total_volume = intent.volume;
        event.filled_volume = 0;
        event.ts_ns = NowEpochNanos();
        event.trace_id = intent.trace_id;
        callback_(event);
    }
    return true;
}

bool InMemoryOrderGateway::CancelOrder(const std::string& client_order_id,
                                       const std::string& trace_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    const auto it = active_orders_.find(client_order_id);
    if (it == active_orders_.end()) {
        return false;
    }

    const auto order = it->second;
    active_orders_.erase(it);

    if (callback_) {
        OrderEvent event;
        event.account_id = order.account_id;
        event.client_order_id = order.client_order_id;
        event.exchange_order_id = "sim-" + order.client_order_id;
        event.instrument_id = order.instrument_id;
        event.status = OrderStatus::kCanceled;
        event.total_volume = order.volume;
        event.filled_volume = 0;
        event.reason = "canceled by request";
        event.ts_ns = NowEpochNanos();
        event.trace_id = trace_id;
        callback_(event);
    }

    return true;
}

void InMemoryOrderGateway::RegisterOrderEventCallback(OrderEventCallback callback) {
    std::lock_guard<std::mutex> lock(mutex_);
    callback_ = std::move(callback);
}

}  // namespace quant_hft
