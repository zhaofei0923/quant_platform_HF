#pragma once

#include <string>
#include <vector>

#include "quant_hft/contracts/types.h"

namespace quant_hft {

enum class SubmissionOutcome { kNotSubmitted, kSubmitted, kUnknown };

struct SubmissionResult {
    SubmissionOutcome outcome{SubmissionOutcome::kNotSubmitted};
    std::string client_order_id;
    std::string error;
};

// Services depend on these execution capabilities, not a CTP session implementation.
class IExecutionGateway {
   public:
    virtual ~IExecutionGateway() = default;
    virtual SubmissionResult SubmitOrder(const OrderIntent& intent) = 0;
    virtual bool CancelOrder(const std::string& client_order_id, const std::string& trace_id) = 0;
    virtual std::string GetDefaultAccountId() const = 0;
    virtual TradingAccountSnapshot GetLastTradingAccountSnapshot() const = 0;
    virtual std::vector<InvestorPositionSnapshot> GetLastInvestorPositionSnapshots() const = 0;
    virtual std::uint64_t GetInvestorPositionSnapshotGeneration() const noexcept = 0;
    virtual int EnqueueTradingAccountQuery() = 0;
    virtual int EnqueueInvestorPositionQuery() = 0;
    virtual bool EnqueueInstrumentQuery(int request_id) = 0;
    virtual bool EnqueueBrokerTradingParamsQuery(int request_id) = 0;
};
}  // namespace quant_hft
