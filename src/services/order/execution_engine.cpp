#include "quant_hft/services/execution_engine.h"

#include <algorithm>
#include <chrono>
#include <stdexcept>
#include <thread>
#include <utility>

namespace quant_hft {

namespace {

bool IsTerminalStatus(OrderStatus status) {
    return status == OrderStatus::kCanceled || status == OrderStatus::kFilled ||
           status == OrderStatus::kRejected;
}

}  // namespace

ExecutionEngine::ExecutionEngine(std::shared_ptr<CTPTraderAdapter> adapter,
                                 std::shared_ptr<FlowController> flow_controller,
                                 std::shared_ptr<CircuitBreakerManager> breaker_manager,
                                 std::shared_ptr<OrderManager> order_manager,
                                 std::shared_ptr<PositionManager> position_manager,
                                 std::shared_ptr<ITradingDomainStore> domain_store,
                                 int acquire_timeout_ms,
                                 int cancel_retry_max,
                                 int cancel_retry_base_ms,
                                 int cancel_retry_max_delay_ms,
                                 int cancel_wait_ack_timeout_ms)
    : adapter_(std::move(adapter)),
      flow_controller_(std::move(flow_controller)),
      breaker_manager_(std::move(breaker_manager)),
      order_manager_(std::move(order_manager)),
      position_manager_(std::move(position_manager)),
      domain_store_(std::move(domain_store)),
      acquire_timeout_ms_(std::max(0, acquire_timeout_ms)),
      cancel_retry_max_(std::max(1, cancel_retry_max)),
      cancel_retry_base_ms_(std::max(1, cancel_retry_base_ms)),
      cancel_retry_max_delay_ms_(std::max(cancel_retry_base_ms_, cancel_retry_max_delay_ms)),
      cancel_wait_ack_timeout_ms_(std::max(1, cancel_wait_ack_timeout_ms)) {}

std::future<OrderResult> ExecutionEngine::PlaceOrderAsync(const OrderIntent& intent) {
    return std::async(std::launch::async, [this, intent]() {
        OrderResult result;
        result.client_order_id = intent.client_order_id;
        if (adapter_ == nullptr || flow_controller_ == nullptr || breaker_manager_ == nullptr) {
            result.message = "execution engine dependencies are null";
            return result;
        }
        if (intent.account_id.empty() || intent.strategy_id.empty()) {
            result.message = "order intent account_id/strategy_id required";
            return result;
        }
        if (risk_manager_ != nullptr) {
            const auto context = BuildOrderContext(intent);
            const auto risk_result = risk_manager_->CheckOrder(intent, context);
            if (!risk_result.allowed) {
                result.message = "risk reject: " + risk_result.reason;
                return result;
            }
        }
        default_account_id_ = intent.account_id;
        default_strategy_id_ = intent.strategy_id;
        if (!AllowByBreaker(intent.strategy_id, intent.account_id)) {
            result.message = "blocked by circuit breaker";
            return result;
        }
        if (!AcquireFlowPermit(Operation{
                intent.account_id,
                OperationType::kOrderInsert,
                intent.instrument_id,
            })) {
            RecordBreakerFailure(intent.strategy_id, intent.account_id);
            result.message = "flow control rejected order insert";
            return result;
        }

        const auto order_ref = adapter_->PlaceOrderWithRef(intent);
        if (order_ref.empty()) {
            RecordBreakerFailure(intent.strategy_id, intent.account_id);
            result.message = "ctp place order failed";
            return result;
        }

        OrderIntent order_intent = intent;
        order_intent.client_order_id = order_ref;
        if (order_manager_ != nullptr) {
            (void)order_manager_->CreateOrder(order_intent);
        }
        RecordBreakerSuccess(intent.strategy_id, intent.account_id);
        result.success = true;
        result.client_order_id = order_ref;
        result.message = "submitted";
        return result;
    });
}

std::future<bool> ExecutionEngine::CancelOrderAsync(const std::string& client_order_id) {
    return std::async(std::launch::async, [this, client_order_id]() {
        if (adapter_ == nullptr || flow_controller_ == nullptr || breaker_manager_ == nullptr) {
            return false;
        }
        if (client_order_id.empty()) {
            return false;
        }
        if (risk_manager_ != nullptr) {
            const auto context = BuildCancelContext(client_order_id);
            const auto risk_result = risk_manager_->CheckCancel(client_order_id, context);
            if (!risk_result.allowed) {
                return false;
            }
        }
        const auto account_id =
            default_account_id_.empty() ? adapter_->GetLastUserSession().investor_id
                                        : default_account_id_;
        if (!AllowByBreaker(default_strategy_id_, account_id)) {
            return false;
        }
        if (order_manager_ != nullptr) {
            const auto existing = order_manager_->GetOrder(client_order_id);
            if (!existing.has_value()) {
                return false;
            }
            if (IsTerminalStatus(existing->status)) {
                RecordBreakerSuccess(default_strategy_id_, account_id);
                return true;
            }
        }

        int delay_ms = std::max(1, cancel_retry_base_ms_);
        for (int attempt = 1; attempt <= std::max(1, cancel_retry_max_); ++attempt) {
            if (!AcquireFlowPermit(Operation{
                    account_id,
                    OperationType::kOrderCancel,
                    "",
                })) {
                std::this_thread::sleep_for(std::chrono::milliseconds(delay_ms));
                delay_ms = std::min(cancel_retry_max_delay_ms_, delay_ms * 2);
                continue;
            }

            const bool submitted = adapter_->CancelOrder(client_order_id, client_order_id);
            if (domain_store_ != nullptr) {
                std::string ignored_error;
                (void)domain_store_->UpdateOrderCancelRetry(
                    client_order_id, attempt, NowEpochNanos(), &ignored_error);
            }
            if (submitted) {
                if (order_manager_ == nullptr) {
                    RecordBreakerSuccess(default_strategy_id_, account_id);
                    return true;
                }
                const auto deadline =
                    std::chrono::steady_clock::now() +
                    std::chrono::milliseconds(std::max(1, cancel_wait_ack_timeout_ms_));
                while (std::chrono::steady_clock::now() < deadline) {
                    const auto order = order_manager_->GetOrder(client_order_id);
                    if (order.has_value() && IsTerminalStatus(order->status)) {
                        RecordBreakerSuccess(default_strategy_id_, account_id);
                        return true;
                    }
                    std::this_thread::sleep_for(std::chrono::milliseconds(10));
                }
            }

            if (attempt < cancel_retry_max_) {
                std::this_thread::sleep_for(std::chrono::milliseconds(delay_ms));
                delay_ms = std::min(cancel_retry_max_delay_ms_, delay_ms * 2);
            }
        }

        RecordBreakerFailure(default_strategy_id_, account_id);
        return false;
    });
}

std::future<TradingAccountSnapshot> ExecutionEngine::QueryTradingAccountAsync() {
    return std::async(std::launch::async, [this]() {
        if (adapter_ == nullptr || flow_controller_ == nullptr) {
            throw std::runtime_error("query dependencies are null");
        }
        const auto account_id =
            default_account_id_.empty() ? adapter_->GetLastUserSession().investor_id
                                        : default_account_id_;
        if (!AcquireFlowPermit(Operation{
                account_id,
                OperationType::kQuery,
                "",
            })) {
            throw std::runtime_error("query flow control rejected");
        }
        const auto before = adapter_->GetLastTradingAccountSnapshot().ts_ns;
        if (adapter_->EnqueueTradingAccountQuery() <= 0) {
            throw std::runtime_error("failed to enqueue trading account query");
        }
        const auto deadline =
            std::chrono::steady_clock::now() + std::chrono::milliseconds(3000);
        while (std::chrono::steady_clock::now() < deadline) {
            const auto snapshot = adapter_->GetLastTradingAccountSnapshot();
            if (snapshot.ts_ns > before || (before == 0 && snapshot.ts_ns > 0)) {
                return snapshot;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        throw std::runtime_error("query trading account timeout");
    });
}

std::future<std::vector<InvestorPositionSnapshot>> ExecutionEngine::QueryInvestorPositionAsync(
    const std::string& instrument_id) {
    return std::async(std::launch::async, [this, instrument_id]() {
        if (adapter_ == nullptr || flow_controller_ == nullptr) {
            throw std::runtime_error("query dependencies are null");
        }
        const auto account_id =
            default_account_id_.empty() ? adapter_->GetLastUserSession().investor_id
                                        : default_account_id_;
        if (!AcquireFlowPermit(Operation{
                account_id,
                OperationType::kQuery,
                instrument_id,
            })) {
            throw std::runtime_error("query flow control rejected");
        }
        std::size_t before_size = adapter_->GetLastInvestorPositionSnapshots().size();
        if (adapter_->EnqueueInvestorPositionQuery() <= 0) {
            throw std::runtime_error("failed to enqueue investor position query");
        }
        const auto deadline =
            std::chrono::steady_clock::now() + std::chrono::milliseconds(3000);
        while (std::chrono::steady_clock::now() < deadline) {
            auto snapshots = adapter_->GetLastInvestorPositionSnapshots();
            if (snapshots.size() != before_size || !snapshots.empty()) {
                return snapshots;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        throw std::runtime_error("query investor position timeout");
    });
}

void ExecutionEngine::RegisterOrderCallback(OrderCallback cb) {
    order_callback_ = std::move(cb);
}

void ExecutionEngine::SetRiskManager(std::shared_ptr<RiskManager> risk_manager) {
    risk_manager_ = std::move(risk_manager);
}

std::string ExecutionEngine::GetTradingDay() const {
    if (adapter_ == nullptr) {
        return "";
    }
    return adapter_->GetLastTradingAccountSnapshot().trading_day;
}

void ExecutionEngine::HandleOrderEvent(const OrderEvent& event) {
    if (order_manager_ == nullptr) {
        return;
    }
    Order order;
    std::string ignored_error;
    if (!order_manager_->OnOrderEvent(event, &order, &ignored_error)) {
        return;
    }

    if (!event.trade_id.empty() || event.event_source == "OnRtnTrade" ||
        event.event_source == "OnRspQryTrade") {
        Trade trade;
        if (order_manager_->OnTradeEvent(event, &trade, &ignored_error)) {
            if (position_manager_ != nullptr) {
                (void)position_manager_->UpdatePosition(trade, &ignored_error);
            }
            if (risk_manager_ != nullptr) {
                risk_manager_->OnTrade(trade);
            }
        }
    }

    if (order_callback_) {
        order_callback_(order);
    }
}

std::vector<Order> ExecutionEngine::GetActiveOrders() const {
    if (order_manager_ == nullptr) {
        return {};
    }
    return order_manager_->GetActiveOrders();
}

bool ExecutionEngine::PlaceOrder(const OrderIntent& intent) {
    return PlaceOrderAsync(intent).get().success;
}

bool ExecutionEngine::CancelOrder(const std::string& account_id,
                                  const std::string& strategy_id,
                                  const std::string& client_order_id,
                                  const std::string& trace_id,
                                  const std::string& instrument_id) {
    (void)trace_id;
    (void)instrument_id;
    if (!account_id.empty()) {
        default_account_id_ = account_id;
    }
    if (!strategy_id.empty()) {
        default_strategy_id_ = strategy_id;
    }
    return CancelOrderAsync(client_order_id).get();
}

bool ExecutionEngine::QueryTradingAccount(int request_id, const std::string& account_id) {
    (void)request_id;
    if (!account_id.empty()) {
        default_account_id_ = account_id;
    }
    try {
        (void)QueryTradingAccountAsync().get();
        return true;
    } catch (...) {
        return false;
    }
}

bool ExecutionEngine::QueryInvestorPosition(int request_id, const std::string& account_id) {
    (void)request_id;
    if (!account_id.empty()) {
        default_account_id_ = account_id;
    }
    try {
        (void)QueryInvestorPositionAsync().get();
        return true;
    } catch (...) {
        return false;
    }
}

bool ExecutionEngine::QueryInstrument(int request_id, const std::string& account_id) {
    (void)account_id;
    if (adapter_ == nullptr || flow_controller_ == nullptr) {
        return false;
    }
    if (!AcquireFlowPermit(Operation{
            default_account_id_,
            OperationType::kQuery,
            "",
        })) {
        return false;
    }
    return adapter_->EnqueueInstrumentQuery(request_id);
}

bool ExecutionEngine::QueryBrokerTradingParams(int request_id, const std::string& account_id) {
    (void)account_id;
    if (adapter_ == nullptr || flow_controller_ == nullptr) {
        return false;
    }
    if (!AcquireFlowPermit(Operation{
            default_account_id_,
            OperationType::kQuery,
            "",
        })) {
        return false;
    }
    return adapter_->EnqueueBrokerTradingParamsQuery(request_id);
}

bool ExecutionEngine::AllowByBreaker(const std::string& strategy_id, const std::string& account_id) {
    if (!breaker_manager_->Allow(BreakerScope::kStrategy, strategy_id)) {
        return false;
    }
    if (!breaker_manager_->Allow(BreakerScope::kAccount, account_id)) {
        return false;
    }
    if (!breaker_manager_->Allow(BreakerScope::kSystem, "__system__")) {
        return false;
    }
    return true;
}

void ExecutionEngine::RecordBreakerSuccess(const std::string& strategy_id,
                                           const std::string& account_id) {
    breaker_manager_->RecordSuccess(BreakerScope::kStrategy, strategy_id);
    breaker_manager_->RecordSuccess(BreakerScope::kAccount, account_id);
    breaker_manager_->RecordSuccess(BreakerScope::kSystem, "__system__");
}

void ExecutionEngine::RecordBreakerFailure(const std::string& strategy_id,
                                           const std::string& account_id) {
    breaker_manager_->RecordFailure(BreakerScope::kStrategy, strategy_id);
    breaker_manager_->RecordFailure(BreakerScope::kAccount, account_id);
    breaker_manager_->RecordFailure(BreakerScope::kSystem, "__system__");
}

bool ExecutionEngine::AcquireFlowPermit(const Operation& operation) {
    return flow_controller_->Acquire(operation, acquire_timeout_ms_).allowed;
}

OrderContext ExecutionEngine::BuildOrderContext(const OrderIntent& intent) const {
    OrderContext context;
    context.account_id = intent.account_id;
    context.strategy_id = intent.strategy_id;
    context.instrument_id = intent.instrument_id;
    context.current_price = intent.price;

    if (adapter_ != nullptr) {
        const auto account = adapter_->GetLastTradingAccountSnapshot();
        context.current_margin = account.curr_margin;
        context.available_fund = account.available;
        context.today_pnl = account.close_profit + account.position_profit;
        context.today_commission = account.commission;
    }

    if (position_manager_ != nullptr && !intent.account_id.empty()) {
        const auto positions = position_manager_->GetCurrentPositions(intent.account_id);
        for (const auto& position : positions) {
            if (position.symbol != intent.instrument_id) {
                continue;
            }
            context.current_position +=
                static_cast<double>(position.long_qty - position.short_qty);
            context.current_margin += position.margin;
        }
    }
    return context;
}

OrderContext ExecutionEngine::BuildCancelContext(const std::string& client_order_id) const {
    OrderContext context;
    context.account_id = default_account_id_;
    context.strategy_id = default_strategy_id_;
    if (order_manager_ != nullptr) {
        const auto order = order_manager_->GetOrder(client_order_id);
        if (order.has_value()) {
            context.account_id = order->account_id;
            context.strategy_id = order->strategy_id;
            context.instrument_id = order->symbol;
            context.current_price = order->price;
        }
    }
    if (adapter_ != nullptr) {
        const auto account = adapter_->GetLastTradingAccountSnapshot();
        context.current_margin = account.curr_margin;
        context.available_fund = account.available;
        context.today_pnl = account.close_profit + account.position_profit;
        context.today_commission = account.commission;
    }
    return context;
}

}  // namespace quant_hft
