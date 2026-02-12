#include "quant_hft/services/execution_engine.h"

#include <algorithm>

namespace quant_hft {

ExecutionEngine::ExecutionEngine(std::shared_ptr<CTPTraderAdapter> adapter,
                                 std::shared_ptr<FlowController> flow_controller,
                                 std::shared_ptr<CircuitBreakerManager> breaker_manager,
                                 int acquire_timeout_ms)
    : adapter_(std::move(adapter)),
      flow_controller_(std::move(flow_controller)),
      breaker_manager_(std::move(breaker_manager)),
      acquire_timeout_ms_(std::max(0, acquire_timeout_ms)) {}

bool ExecutionEngine::PlaceOrder(const OrderIntent& intent) {
    if (adapter_ == nullptr || flow_controller_ == nullptr || breaker_manager_ == nullptr) {
        return false;
    }
    if (!AllowByBreaker(intent.strategy_id, intent.account_id)) {
        return false;
    }
    if (!AcquireFlowPermit(Operation{
            intent.account_id,
            OperationType::kOrderInsert,
            intent.instrument_id,
        })) {
        RecordBreakerFailure(intent.strategy_id, intent.account_id);
        return false;
    }
    if (!adapter_->PlaceOrder(intent)) {
        RecordBreakerFailure(intent.strategy_id, intent.account_id);
        return false;
    }
    RecordBreakerSuccess(intent.strategy_id, intent.account_id);
    return true;
}

bool ExecutionEngine::CancelOrder(const std::string& account_id,
                                  const std::string& strategy_id,
                                  const std::string& client_order_id,
                                  const std::string& trace_id,
                                  const std::string& instrument_id) {
    if (adapter_ == nullptr || flow_controller_ == nullptr || breaker_manager_ == nullptr) {
        return false;
    }
    if (!AllowByBreaker(strategy_id, account_id)) {
        return false;
    }
    if (!AcquireFlowPermit(Operation{
            account_id,
            OperationType::kOrderCancel,
            instrument_id,
        })) {
        RecordBreakerFailure(strategy_id, account_id);
        return false;
    }
    if (!adapter_->CancelOrder(client_order_id, trace_id)) {
        RecordBreakerFailure(strategy_id, account_id);
        return false;
    }
    RecordBreakerSuccess(strategy_id, account_id);
    return true;
}

bool ExecutionEngine::QueryTradingAccount(int request_id, const std::string& account_id) {
    if (adapter_ == nullptr || flow_controller_ == nullptr) {
        return false;
    }
    if (!AcquireFlowPermit(Operation{
            account_id,
            OperationType::kQuery,
            "",
        })) {
        return false;
    }
    return adapter_->EnqueueTradingAccountQuery(request_id);
}

bool ExecutionEngine::QueryInvestorPosition(int request_id, const std::string& account_id) {
    if (adapter_ == nullptr || flow_controller_ == nullptr) {
        return false;
    }
    if (!AcquireFlowPermit(Operation{
            account_id,
            OperationType::kQuery,
            "",
        })) {
        return false;
    }
    return adapter_->EnqueueInvestorPositionQuery(request_id);
}

bool ExecutionEngine::QueryInstrument(int request_id, const std::string& account_id) {
    if (adapter_ == nullptr || flow_controller_ == nullptr) {
        return false;
    }
    if (!AcquireFlowPermit(Operation{
            account_id,
            OperationType::kQuery,
            "",
        })) {
        return false;
    }
    return adapter_->EnqueueInstrumentQuery(request_id);
}

bool ExecutionEngine::QueryBrokerTradingParams(int request_id, const std::string& account_id) {
    if (adapter_ == nullptr || flow_controller_ == nullptr) {
        return false;
    }
    if (!AcquireFlowPermit(Operation{
            account_id,
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

}  // namespace quant_hft
