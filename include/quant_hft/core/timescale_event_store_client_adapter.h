#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "quant_hft/core/storage_retry_policy.h"
#include "quant_hft/core/timescale_sql_client.h"
#include "quant_hft/interfaces/timeseries_store.h"

namespace quant_hft {

class TimescaleEventStoreClientAdapter : public ITimeseriesStore {
public:
    TimescaleEventStoreClientAdapter(std::shared_ptr<ITimescaleSqlClient> client,
                                     StorageRetryPolicy retry_policy);

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
    bool InsertWithRetry(const std::string& table,
                         const std::unordered_map<std::string, std::string>& row) const;

    static std::string ToString(std::int32_t value);
    static std::string ToString(std::int64_t value);
    static std::string ToString(double value);
    static std::string SideToString(Side side);
    static bool ParseSide(const std::string& text, Side* out);
    static std::string OffsetToString(OffsetFlag offset);
    static bool ParseOffset(const std::string& text, OffsetFlag* out);
    static std::string RiskActionToString(RiskAction action);
    static bool ParseRiskAction(const std::string& text, RiskAction* out);
    static std::string OrderStatusToString(OrderStatus status);
    static bool ParseOrderStatus(const std::string& text, OrderStatus* out);
    static std::string GetOrEmpty(const std::unordered_map<std::string, std::string>& row,
                                  const std::string& key);
    static bool ParseInt32(const std::unordered_map<std::string, std::string>& row,
                           const std::string& key,
                           std::int32_t* out);
    static bool ParseInt64(const std::unordered_map<std::string, std::string>& row,
                           const std::string& key,
                           std::int64_t* out);
    static bool ParseDouble(const std::unordered_map<std::string, std::string>& row,
                            const std::string& key,
                            double* out);

    std::shared_ptr<ITimescaleSqlClient> client_;
    StorageRetryPolicy retry_policy_;
};

}  // namespace quant_hft
