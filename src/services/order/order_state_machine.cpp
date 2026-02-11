#include "quant_hft/services/order_state_machine.h"

namespace quant_hft {

bool OrderStateMachine::OnOrderIntent(const OrderIntent& intent) {
    if (intent.client_order_id.empty() || intent.volume <= 0) {
        return false;
    }

    std::lock_guard<std::mutex> lock(mutex_);
    if (orders_.find(intent.client_order_id) != orders_.end()) {
        return false;
    }

    ManagedOrderSnapshot snapshot;
    snapshot.client_order_id = intent.client_order_id;
    snapshot.account_id = intent.account_id;
    snapshot.instrument_id = intent.instrument_id;
    snapshot.status = OrderStatus::kNew;
    snapshot.total_volume = intent.volume;
    snapshot.filled_volume = 0;
    snapshot.last_update_ts_ns = intent.ts_ns;
    snapshot.is_terminal = false;
    snapshot.message = "intent accepted";

    orders_.emplace(intent.client_order_id, std::move(snapshot));
    return true;
}

bool OrderStateMachine::OnOrderEvent(const OrderEvent& event) {
    if (event.client_order_id.empty()) {
        return false;
    }

    std::lock_guard<std::mutex> lock(mutex_);
    const auto it = orders_.find(event.client_order_id);
    if (it == orders_.end()) {
        return false;
    }

    auto& order = it->second;
    const bool is_duplicate = order.status == event.status &&
                              order.filled_volume == event.filled_volume;
    if (is_duplicate) {
        return true;
    }

    if (order.is_terminal) {
        return false;
    }

    const auto next_total =
        event.total_volume > 0 ? event.total_volume : order.total_volume;
    if (event.filled_volume < order.filled_volume) {
        return false;
    }
    if (next_total > 0 && event.filled_volume > next_total) {
        return false;
    }
    if (!IsTransitionAllowed(order.status, event.status)) {
        return false;
    }
    if (event.status == OrderStatus::kFilled && next_total > 0 &&
        event.filled_volume != next_total) {
        return false;
    }
    if (event.status == OrderStatus::kPartiallyFilled && next_total > 0 &&
        event.filled_volume >= next_total) {
        return false;
    }

    order.status = event.status;
    order.total_volume = next_total;
    order.filled_volume = event.filled_volume;
    order.last_update_ts_ns = event.ts_ns;
    order.is_terminal = IsTerminalStatus(event.status);
    order.message = event.reason;
    if (!event.account_id.empty()) {
        order.account_id = event.account_id;
    }
    if (!event.instrument_id.empty()) {
        order.instrument_id = event.instrument_id;
    }
    return true;
}

bool OrderStateMachine::RecoverFromOrderEvent(const OrderEvent& event) {
    if (event.client_order_id.empty()) {
        return false;
    }

    std::lock_guard<std::mutex> lock(mutex_);
    const auto it = orders_.find(event.client_order_id);
    if (it == orders_.end()) {
        ManagedOrderSnapshot snapshot;
        snapshot.client_order_id = event.client_order_id;
        snapshot.account_id = event.account_id;
        snapshot.instrument_id = event.instrument_id;
        snapshot.status = event.status;
        snapshot.total_volume =
            event.total_volume > 0 ? event.total_volume : event.filled_volume;
        snapshot.filled_volume = event.filled_volume;
        snapshot.last_update_ts_ns = event.ts_ns;
        snapshot.is_terminal = IsTerminalStatus(event.status);
        snapshot.message = "recovered from wal";
        orders_.emplace(event.client_order_id, std::move(snapshot));
        return true;
    }

    auto& order = it->second;
    if (order.status == event.status && order.filled_volume == event.filled_volume) {
        return true;
    }

    if (order.is_terminal) {
        return false;
    }

    const auto next_total =
        event.total_volume > 0 ? event.total_volume : order.total_volume;
    if (event.filled_volume < order.filled_volume) {
        return false;
    }
    if (next_total > 0 && event.filled_volume > next_total) {
        return false;
    }
    if (!IsTransitionAllowed(order.status, event.status)) {
        return false;
    }
    if (event.status == OrderStatus::kFilled && next_total > 0 &&
        event.filled_volume != next_total) {
        return false;
    }
    if (event.status == OrderStatus::kPartiallyFilled && next_total > 0 &&
        event.filled_volume >= next_total) {
        return false;
    }

    order.status = event.status;
    order.total_volume = next_total;
    order.filled_volume = event.filled_volume;
    order.last_update_ts_ns = event.ts_ns;
    order.is_terminal = IsTerminalStatus(event.status);
    order.message = "recovered from wal";
    if (!event.account_id.empty()) {
        order.account_id = event.account_id;
    }
    if (!event.instrument_id.empty()) {
        order.instrument_id = event.instrument_id;
    }
    return true;
}

bool OrderStateMachine::HasOrder(const std::string& client_order_id) const {
    std::lock_guard<std::mutex> lock(mutex_);
    return orders_.find(client_order_id) != orders_.end();
}

ManagedOrderSnapshot OrderStateMachine::GetOrderSnapshot(
    const std::string& client_order_id) const {
    std::lock_guard<std::mutex> lock(mutex_);
    const auto it = orders_.find(client_order_id);
    if (it == orders_.end()) {
        ManagedOrderSnapshot empty;
        empty.client_order_id = client_order_id;
        empty.message = "order not found";
        return empty;
    }
    return it->second;
}

std::size_t OrderStateMachine::ActiveOrderCount() const {
    std::lock_guard<std::mutex> lock(mutex_);
    std::size_t active = 0;
    for (const auto& item : orders_) {
        if (!item.second.is_terminal) {
            ++active;
        }
    }
    return active;
}

bool OrderStateMachine::IsTerminalStatus(OrderStatus status) {
    return status == OrderStatus::kFilled ||
           status == OrderStatus::kCanceled ||
           status == OrderStatus::kRejected;
}

bool OrderStateMachine::IsTransitionAllowed(OrderStatus from, OrderStatus to) {
    if (from == to) {
        return true;
    }

    switch (from) {
        case OrderStatus::kNew:
            return to == OrderStatus::kAccepted ||
                   to == OrderStatus::kPartiallyFilled ||
                   to == OrderStatus::kFilled ||
                   to == OrderStatus::kCanceled ||
                   to == OrderStatus::kRejected;
        case OrderStatus::kAccepted:
            return to == OrderStatus::kPartiallyFilled ||
                   to == OrderStatus::kFilled ||
                   to == OrderStatus::kCanceled ||
                   to == OrderStatus::kRejected;
        case OrderStatus::kPartiallyFilled:
            return to == OrderStatus::kPartiallyFilled ||
                   to == OrderStatus::kFilled ||
                   to == OrderStatus::kCanceled;
        case OrderStatus::kFilled:
        case OrderStatus::kCanceled:
        case OrderStatus::kRejected:
            return false;
    }
    return false;
}

}  // namespace quant_hft
