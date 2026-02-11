#pragma once

#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "quant_hft/interfaces/timeseries_store.h"

namespace quant_hft {

class TimescaleEventStore : public ITimeseriesStore {
public:
    void AppendMarketSnapshot(const MarketSnapshot& snapshot) override;
    void AppendOrderEvent(const OrderEvent& event) override;
    void AppendRiskDecision(const OrderIntent& intent,
                            const RiskDecision& decision) override;

    std::vector<MarketSnapshot> GetMarketSnapshots(
        const std::string& instrument_id) const override;
    std::vector<OrderEvent> GetOrderEvents(
        const std::string& client_order_id) const override;
    std::vector<RiskDecisionRow> GetRiskDecisionRows() const override;

private:
    mutable std::mutex mutex_;
    std::unordered_map<std::string, std::vector<MarketSnapshot>> market_by_instrument_;
    std::unordered_map<std::string, std::vector<OrderEvent>> order_by_client_id_;
    std::vector<RiskDecisionRow> risk_rows_;
};

}  // namespace quant_hft
