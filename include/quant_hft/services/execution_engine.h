#pragma once

#include <memory>
#include <string>

#include "quant_hft/contracts/types.h"
#include "quant_hft/core/circuit_breaker.h"
#include "quant_hft/core/ctp_trader_adapter.h"
#include "quant_hft/core/flow_controller.h"

namespace quant_hft {

class ExecutionEngine {
public:
    ExecutionEngine(std::shared_ptr<CTPTraderAdapter> adapter,
                    std::shared_ptr<FlowController> flow_controller,
                    std::shared_ptr<CircuitBreakerManager> breaker_manager,
                    int acquire_timeout_ms = 1000);

    bool PlaceOrder(const OrderIntent& intent);
    bool CancelOrder(const std::string& account_id,
                     const std::string& strategy_id,
                     const std::string& client_order_id,
                     const std::string& trace_id,
                     const std::string& instrument_id = "");

    bool QueryTradingAccount(int request_id, const std::string& account_id);
    bool QueryInvestorPosition(int request_id, const std::string& account_id);
    bool QueryInstrument(int request_id, const std::string& account_id);
    bool QueryBrokerTradingParams(int request_id, const std::string& account_id);

private:
    bool AllowByBreaker(const std::string& strategy_id, const std::string& account_id);
    void RecordBreakerSuccess(const std::string& strategy_id, const std::string& account_id);
    void RecordBreakerFailure(const std::string& strategy_id, const std::string& account_id);
    bool AcquireFlowPermit(const Operation& operation);

    std::shared_ptr<CTPTraderAdapter> adapter_;
    std::shared_ptr<FlowController> flow_controller_;
    std::shared_ptr<CircuitBreakerManager> breaker_manager_;
    int acquire_timeout_ms_{1000};
};

}  // namespace quant_hft
