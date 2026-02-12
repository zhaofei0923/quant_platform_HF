#pragma once

#include <cstddef>
#include <deque>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "quant_hft/contracts/types.h"
#include "quant_hft/interfaces/trading_domain_store.h"
#include "quant_hft/services/order_state_machine.h"

namespace quant_hft {

class OrderManager {
public:
    explicit OrderManager(std::shared_ptr<ITradingDomainStore> domain_store = nullptr,
                          std::size_t processed_event_cache_size = 10000);

    Order CreateOrder(const OrderIntent& intent);
    bool OnOrderEvent(const OrderEvent& event, Order* out_order, std::string* error);
    bool OnTradeEvent(const OrderEvent& event, Trade* out_trade, std::string* error);

    std::optional<Order> GetOrder(const std::string& client_order_id) const;
    std::vector<Order> GetActiveOrders() const;
    std::vector<Order> GetActiveOrdersByStrategy(
        const std::string& strategy_id,
        const std::string& instrument_id = "") const;

    bool IsOrderProcessed(const std::string& order_ref, int front_id, int session_id) const;

    static std::string BuildOrderEventKey(const OrderEvent& event);
    static std::string BuildTradeEventKey(const OrderEvent& event);

private:
    bool IsEventProcessed(const std::string& event_key, std::string* error) const;
    void MarkEventProcessed(const std::string& event_key,
                            const OrderEvent& event,
                            std::int32_t event_type,
                            std::string* error);
    static std::string ResolveOrderId(const OrderEvent& event);

    mutable std::mutex mutex_;
    std::shared_ptr<ITradingDomainStore> domain_store_;
    OrderStateMachine state_machine_;
    std::unordered_map<std::string, Order> orders_;
    std::unordered_set<std::string> processed_events_;
    std::deque<std::string> processed_order_;
    std::size_t processed_event_cache_size_{10000};
};

}  // namespace quant_hft
