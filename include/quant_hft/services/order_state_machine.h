#pragma once

#include <cstdint>
#include <mutex>
#include <string>
#include <unordered_map>

#include "quant_hft/contracts/types.h"

namespace quant_hft {

struct ManagedOrderSnapshot {
    std::string client_order_id;
    std::string account_id;
    std::string instrument_id;
    OrderStatus status{OrderStatus::kNew};
    std::int32_t total_volume{0};
    std::int32_t filled_volume{0};
    EpochNanos last_update_ts_ns{0};
    bool is_terminal{false};
    std::string message;
};

class OrderStateMachine {
public:
    bool OnOrderIntent(const OrderIntent& intent);
    bool OnOrderEvent(const OrderEvent& event);
    // Recovery path for WAL replay: allows bootstrapping missing orders.
    bool RecoverFromOrderEvent(const OrderEvent& event);

    bool HasOrder(const std::string& client_order_id) const;
    ManagedOrderSnapshot GetOrderSnapshot(const std::string& client_order_id) const;
    std::size_t ActiveOrderCount() const;

private:
    static bool IsTerminalStatus(OrderStatus status);
    static bool IsTransitionAllowed(OrderStatus from, OrderStatus to);

    mutable std::mutex mutex_;
    std::unordered_map<std::string, ManagedOrderSnapshot> orders_;
};

}  // namespace quant_hft
