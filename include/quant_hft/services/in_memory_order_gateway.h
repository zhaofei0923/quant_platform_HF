#pragma once

#include <mutex>
#include <string>
#include <unordered_map>

#include "quant_hft/interfaces/order_gateway.h"

namespace quant_hft {

class InMemoryOrderGateway : public IOrderGateway {
public:
    bool PlaceOrder(const OrderIntent& intent) override;
    bool CancelOrder(const std::string& client_order_id,
                     const std::string& trace_id) override;
    void RegisterOrderEventCallback(OrderEventCallback callback) override;

private:
    mutable std::mutex mutex_;
    std::unordered_map<std::string, OrderIntent> active_orders_;
    OrderEventCallback callback_;
};

}  // namespace quant_hft
