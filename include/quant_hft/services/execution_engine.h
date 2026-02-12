#pragma once

#include <future>
#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "quant_hft/contracts/types.h"
#include "quant_hft/core/circuit_breaker.h"
#include "quant_hft/core/ctp_trader_adapter.h"
#include "quant_hft/core/flow_controller.h"
#include "quant_hft/interfaces/trading_domain_store.h"
#include "quant_hft/risk/risk_manager.h"
#include "quant_hft/services/order_manager.h"
#include "quant_hft/services/position_manager.h"

namespace quant_hft {

struct OrderResult {
    bool success{false};
    std::string client_order_id;
    std::string message;
};

class ExecutionEngine {
public:
    ExecutionEngine(std::shared_ptr<CTPTraderAdapter> adapter,
                    std::shared_ptr<FlowController> flow_controller,
                    std::shared_ptr<CircuitBreakerManager> breaker_manager,
                    std::shared_ptr<OrderManager> order_manager = nullptr,
                    std::shared_ptr<PositionManager> position_manager = nullptr,
                    std::shared_ptr<ITradingDomainStore> domain_store = nullptr,
                    int acquire_timeout_ms = 1000,
                    int cancel_retry_max = 3,
                    int cancel_retry_base_ms = 1000,
                    int cancel_retry_max_delay_ms = 5000,
                    int cancel_wait_ack_timeout_ms = 1200);

    std::future<OrderResult> PlaceOrderAsync(const OrderIntent& intent);
    std::future<bool> CancelOrderAsync(const std::string& client_order_id);
    void SetRiskManager(std::shared_ptr<RiskManager> risk_manager);
    std::future<TradingAccountSnapshot> QueryTradingAccountAsync();
    std::future<std::vector<InvestorPositionSnapshot>> QueryInvestorPositionAsync(
        const std::string& instrument_id = "");
    using OrderCallback = std::function<void(const Order&)>;
    void RegisterOrderCallback(OrderCallback cb);
    std::string GetTradingDay() const;
    void HandleOrderEvent(const OrderEvent& event);
    std::vector<Order> GetActiveOrders() const;

    [[deprecated("use PlaceOrderAsync")]]
    bool PlaceOrder(const OrderIntent& intent);
    [[deprecated("use CancelOrderAsync")]]
    bool CancelOrder(const std::string& account_id,
                     const std::string& strategy_id,
                     const std::string& client_order_id,
                     const std::string& trace_id,
                     const std::string& instrument_id = "");

    [[deprecated("use QueryTradingAccountAsync")]]
    bool QueryTradingAccount(int request_id, const std::string& account_id);
    [[deprecated("use QueryInvestorPositionAsync")]]
    bool QueryInvestorPosition(int request_id, const std::string& account_id);
    [[deprecated("use CTPTraderAdapter::EnqueueInstrumentQuery()")]]
    bool QueryInstrument(int request_id, const std::string& account_id);
    [[deprecated("use CTPTraderAdapter::EnqueueBrokerTradingParamsQuery()")]]
    bool QueryBrokerTradingParams(int request_id, const std::string& account_id);

private:
    bool AllowByBreaker(const std::string& strategy_id, const std::string& account_id);
    void RecordBreakerSuccess(const std::string& strategy_id, const std::string& account_id);
    void RecordBreakerFailure(const std::string& strategy_id, const std::string& account_id);
    bool AcquireFlowPermit(const Operation& operation);
    OrderContext BuildOrderContext(const OrderIntent& intent) const;
    OrderContext BuildCancelContext(const std::string& client_order_id) const;

    std::shared_ptr<CTPTraderAdapter> adapter_;
    std::shared_ptr<FlowController> flow_controller_;
    std::shared_ptr<CircuitBreakerManager> breaker_manager_;
    std::shared_ptr<OrderManager> order_manager_;
    std::shared_ptr<PositionManager> position_manager_;
    std::shared_ptr<ITradingDomainStore> domain_store_;
    std::shared_ptr<RiskManager> risk_manager_;
    OrderCallback order_callback_;
    std::string default_account_id_;
    std::string default_strategy_id_;
    int acquire_timeout_ms_{1000};
    int cancel_retry_max_{3};
    int cancel_retry_base_ms_{1000};
    int cancel_retry_max_delay_ms_{5000};
    int cancel_wait_ack_timeout_ms_{1200};
};

}  // namespace quant_hft
