#include "quant_hft/services/order_manager.h"

#include <algorithm>
#include <utility>

namespace quant_hft {

namespace {

bool IsTradeEvent(const OrderEvent& event) {
    return !event.trade_id.empty() || event.event_source == "OnRtnTrade" ||
           event.event_source == "OnRspQryTrade";
}

}  // namespace

OrderManager::OrderManager(std::shared_ptr<ITradingDomainStore> domain_store,
                           std::size_t processed_event_cache_size)
    : domain_store_(std::move(domain_store)),
      processed_event_cache_size_(std::max<std::size_t>(1000, processed_event_cache_size)) {}

Order OrderManager::CreateOrder(const OrderIntent& intent) {
    Order order;
    order.order_id = intent.client_order_id;
    order.account_id = intent.account_id;
    order.strategy_id = intent.strategy_id;
    order.symbol = intent.instrument_id;
    order.side = intent.side;
    order.offset = intent.offset;
    order.order_type = intent.type;
    order.price = intent.price;
    order.quantity = intent.volume;
    order.status = OrderStatus::kNew;
    order.created_at_ns = intent.ts_ns > 0 ? intent.ts_ns : NowEpochNanos();
    order.updated_at_ns = order.created_at_ns;
    order.message = "created";

    state_machine_.OnOrderIntent(intent);
    {
        std::lock_guard<std::mutex> lock(mutex_);
        orders_[order.order_id] = order;
    }
    if (domain_store_ != nullptr) {
        std::string ignored_error;
        (void)domain_store_->UpsertOrder(order, &ignored_error);
    }
    return order;
}

bool OrderManager::OnOrderEvent(const OrderEvent& event, Order* out_order, std::string* error) {
    const auto event_key = BuildOrderEventKey(event);
    if (event_key.empty()) {
        if (error != nullptr) {
            *error = "empty order event key";
        }
        return false;
    }
    if (IsEventProcessed(event_key, error)) {
        if (out_order != nullptr) {
            const auto existing = GetOrder(ResolveOrderId(event));
            if (existing.has_value()) {
                *out_order = *existing;
            }
        }
        return true;
    }

    bool applied = state_machine_.OnOrderEvent(event);
    if (!applied) {
        applied = state_machine_.RecoverFromOrderEvent(event);
    }
    if (!applied) {
        if (error != nullptr) {
            *error = "order state transition rejected";
        }
        return false;
    }

    const auto order_id = ResolveOrderId(event);
    Order order;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = orders_.find(order_id);
        if (it == orders_.end()) {
            order.order_id = order_id;
            order.account_id = event.account_id;
            order.strategy_id = event.strategy_id;
            order.symbol = event.instrument_id;
            order.exchange = event.exchange_id;
            order.side = event.side;
            order.offset = event.offset;
            order.order_type = OrderType::kLimit;
            order.price = event.avg_fill_price;
            order.quantity = event.total_volume;
            order.created_at_ns = event.ts_ns > 0 ? event.ts_ns : NowEpochNanos();
            order.updated_at_ns = order.created_at_ns;
            orders_[order_id] = order;
            it = orders_.find(order_id);
        }
        order = it->second;
        order.status = event.status;
        if (event.total_volume > 0) {
            order.quantity = event.total_volume;
        }
        order.filled_quantity = event.filled_volume;
        order.avg_fill_price = event.avg_fill_price;
        order.updated_at_ns = event.ts_ns > 0 ? event.ts_ns : NowEpochNanos();
        order.message = event.reason.empty() ? event.status_msg : event.reason;
        it->second = order;
    }

    if (domain_store_ != nullptr) {
        std::string store_error;
        if (!domain_store_->UpsertOrder(order, &store_error) && error != nullptr &&
            error->empty()) {
            *error = store_error;
        }
    }
    MarkEventProcessed(event_key, event, 0, error);

    if (out_order != nullptr) {
        *out_order = order;
    }
    return true;
}

bool OrderManager::OnTradeEvent(const OrderEvent& event, Trade* out_trade, std::string* error) {
    if (!IsTradeEvent(event)) {
        if (error != nullptr) {
            *error = "not a trade event";
        }
        return false;
    }
    const auto event_key = BuildTradeEventKey(event);
    if (event_key.empty()) {
        if (error != nullptr) {
            *error = "empty trade event key";
        }
        return false;
    }
    if (IsEventProcessed(event_key, error)) {
        return true;
    }

    Trade trade;
    trade.trade_id = event.trade_id.empty() ? event_key : event.trade_id;
        trade.order_id = ResolveOrderId(event);
        trade.account_id = event.account_id;
    trade.strategy_id = event.strategy_id;
    {
        const auto order = GetOrder(trade.order_id);
        if (order.has_value()) {
            trade.strategy_id = order->strategy_id;
        }
    }
    trade.symbol = event.instrument_id;
    trade.exchange = event.exchange_id;
    trade.side = event.side;
    trade.offset = event.offset;
    trade.price = event.avg_fill_price;
    trade.quantity = event.total_volume > 0 ? event.total_volume : event.filled_volume;
    trade.trade_ts_ns = event.ts_ns > 0 ? event.ts_ns : NowEpochNanos();
    trade.commission = 0.0;
    trade.profit = 0.0;

    if (domain_store_ != nullptr) {
        std::string store_error;
        if (!domain_store_->AppendTrade(trade, &store_error)) {
            if (error != nullptr) {
                *error = store_error;
            }
            return false;
        }
    }
    MarkEventProcessed(event_key, event, 1, error);
    if (out_trade != nullptr) {
        *out_trade = trade;
    }
    return true;
}

std::optional<Order> OrderManager::GetOrder(const std::string& client_order_id) const {
    if (client_order_id.empty()) {
        return std::nullopt;
    }
    std::lock_guard<std::mutex> lock(mutex_);
    const auto it = orders_.find(client_order_id);
    if (it == orders_.end()) {
        return std::nullopt;
    }
    return it->second;
}

std::vector<Order> OrderManager::GetActiveOrders() const {
    std::vector<Order> out;
    std::lock_guard<std::mutex> lock(mutex_);
    out.reserve(orders_.size());
    for (const auto& [order_id, order] : orders_) {
        (void)order_id;
        if (order.status == OrderStatus::kFilled || order.status == OrderStatus::kCanceled ||
            order.status == OrderStatus::kRejected) {
            continue;
        }
        out.push_back(order);
    }
    return out;
}

std::vector<Order> OrderManager::GetActiveOrdersByStrategy(
    const std::string& strategy_id,
    const std::string& instrument_id) const {
    if (strategy_id.empty()) {
        return {};
    }

    std::vector<Order> out;
    std::lock_guard<std::mutex> lock(mutex_);
    out.reserve(orders_.size());
    for (const auto& [order_id, order] : orders_) {
        (void)order_id;
        if (order.strategy_id != strategy_id) {
            continue;
        }
        if (!instrument_id.empty() && order.symbol != instrument_id) {
            continue;
        }
        if (order.status == OrderStatus::kFilled || order.status == OrderStatus::kCanceled ||
            order.status == OrderStatus::kRejected) {
            continue;
        }
        out.push_back(order);
    }
    return out;
}

bool OrderManager::IsOrderProcessed(const std::string& order_ref,
                                    int front_id,
                                    int session_id) const {
    if (order_ref.empty()) {
        return false;
    }
    const auto prefix = order_ref + "|" + std::to_string(front_id) + "|" +
                        std::to_string(session_id) + "|";
    std::lock_guard<std::mutex> lock(mutex_);
    for (const auto& key : processed_events_) {
        if (key.rfind(prefix, 0) == 0U) {
            return true;
        }
    }
    return false;
}

std::string OrderManager::BuildOrderEventKey(const OrderEvent& event) {
    if (event.order_ref.empty()) {
        return "";
    }
    return event.order_ref + "|" + std::to_string(event.front_id) + "|" +
           std::to_string(event.session_id) + "|" +
           std::to_string(static_cast<int>(event.status)) + "|" +
           std::to_string(event.filled_volume) + "|" + event.event_source + "|" +
           std::to_string(event.exchange_ts_ns);
}

std::string OrderManager::BuildTradeEventKey(const OrderEvent& event) {
    if (!event.trade_id.empty()) {
        return "trade_id|" + event.trade_id;
    }
    if (event.order_ref.empty()) {
        return "";
    }
    return event.order_ref + "|" + std::to_string(event.front_id) + "|" +
           std::to_string(event.session_id) + "|trade|" + event.event_source + "|" +
           std::to_string(event.exchange_ts_ns) + "|" + std::to_string(event.filled_volume);
}

bool OrderManager::IsEventProcessed(const std::string& event_key, std::string* error) const {
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (processed_events_.find(event_key) != processed_events_.end()) {
            return true;
        }
    }
    if (domain_store_ == nullptr) {
        return false;
    }
    bool exists = false;
    std::string store_error;
    if (!domain_store_->ExistsProcessedOrderEvent(event_key, &exists, &store_error)) {
        if (error != nullptr) {
            *error = store_error;
        }
        return false;
    }
    return exists;
}

void OrderManager::MarkEventProcessed(const std::string& event_key,
                                      const OrderEvent& event,
                                      std::int32_t event_type,
                                      std::string* error) {
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (processed_events_.insert(event_key).second) {
            processed_order_.push_back(event_key);
            while (processed_order_.size() > processed_event_cache_size_) {
                processed_events_.erase(processed_order_.front());
                processed_order_.pop_front();
            }
        }
    }
    if (domain_store_ != nullptr) {
        ProcessedOrderEventRecord record;
        record.event_key = event_key;
        record.order_ref = event.order_ref;
        record.front_id = event.front_id;
        record.session_id = event.session_id;
        record.event_type = event_type;
        record.trade_id = event.trade_id;
        record.event_source = event.event_source;
        record.processed_ts_ns = event.ts_ns > 0 ? event.ts_ns : NowEpochNanos();
        std::string store_error;
        if (!domain_store_->MarkProcessedOrderEvent(record, &store_error) && error != nullptr &&
            error->empty()) {
            *error = store_error;
        }
    }
}

std::string OrderManager::ResolveOrderId(const OrderEvent& event) {
    if (!event.client_order_id.empty()) {
        return event.client_order_id;
    }
    return event.order_ref;
}

}  // namespace quant_hft
