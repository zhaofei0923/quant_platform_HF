#pragma once

#include <functional>
#include <string>

#include "quant_hft/contracts/types.h"

namespace quant_hft {

class IOrderGateway {
public:
    using OrderEventCallback = std::function<void(const OrderEvent&)>;

    virtual ~IOrderGateway() = default;
    virtual bool PlaceOrder(const OrderIntent& intent) = 0;
    virtual bool CancelOrder(const std::string& client_order_id,
                             const std::string& trace_id) = 0;
    virtual void RegisterOrderEventCallback(OrderEventCallback callback) = 0;
};

}  // namespace quant_hft
