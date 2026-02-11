#include "quant_hft/core/timescale_event_store.h"

#include <utility>

namespace quant_hft {

void TimescaleEventStore::AppendMarketSnapshot(const MarketSnapshot& snapshot) {
    if (snapshot.instrument_id.empty()) {
        return;
    }
    std::lock_guard<std::mutex> lock(mutex_);
    market_by_instrument_[snapshot.instrument_id].push_back(snapshot);
}

void TimescaleEventStore::AppendOrderEvent(const OrderEvent& event) {
    if (event.client_order_id.empty()) {
        return;
    }
    std::lock_guard<std::mutex> lock(mutex_);
    order_by_client_id_[event.client_order_id].push_back(event);
}

void TimescaleEventStore::AppendRiskDecision(const OrderIntent& intent,
                                             const RiskDecision& decision) {
    std::lock_guard<std::mutex> lock(mutex_);
    RiskDecisionRow row;
    row.intent = intent;
    row.decision = decision;
    row.ts_ns = intent.ts_ns > 0 ? intent.ts_ns : NowEpochNanos();
    risk_rows_.push_back(std::move(row));
}

std::vector<MarketSnapshot> TimescaleEventStore::GetMarketSnapshots(
    const std::string& instrument_id) const {
    std::lock_guard<std::mutex> lock(mutex_);
    const auto it = market_by_instrument_.find(instrument_id);
    if (it == market_by_instrument_.end()) {
        return {};
    }
    return it->second;
}

std::vector<OrderEvent> TimescaleEventStore::GetOrderEvents(
    const std::string& client_order_id) const {
    std::lock_guard<std::mutex> lock(mutex_);
    const auto it = order_by_client_id_.find(client_order_id);
    if (it == order_by_client_id_.end()) {
        return {};
    }
    return it->second;
}

std::vector<RiskDecisionRow> TimescaleEventStore::GetRiskDecisionRows() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return risk_rows_;
}

}  // namespace quant_hft
