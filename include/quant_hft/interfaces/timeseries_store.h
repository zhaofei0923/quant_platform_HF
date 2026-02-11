#pragma once

#include <string>
#include <vector>

#include "quant_hft/contracts/types.h"

namespace quant_hft {

struct RiskDecisionRow {
    OrderIntent intent;
    RiskDecision decision;
    EpochNanos ts_ns{0};
};

class ITimeseriesStore {
public:
    virtual ~ITimeseriesStore() = default;

    virtual void AppendMarketSnapshot(const MarketSnapshot& snapshot) = 0;
    virtual void AppendOrderEvent(const OrderEvent& event) = 0;
    virtual void AppendRiskDecision(const OrderIntent& intent,
                                    const RiskDecision& decision) = 0;

    virtual std::vector<MarketSnapshot> GetMarketSnapshots(
        const std::string& instrument_id) const = 0;
    virtual std::vector<OrderEvent> GetOrderEvents(
        const std::string& client_order_id) const = 0;
    virtual std::vector<RiskDecisionRow> GetRiskDecisionRows() const = 0;
};

}  // namespace quant_hft
