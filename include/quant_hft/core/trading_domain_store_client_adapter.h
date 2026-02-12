#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "quant_hft/core/storage_retry_policy.h"
#include "quant_hft/core/timescale_sql_client.h"
#include "quant_hft/interfaces/trading_domain_store.h"

namespace quant_hft {

class TradingDomainStoreClientAdapter : public ITradingDomainStore {
public:
    TradingDomainStoreClientAdapter(std::shared_ptr<ITimescaleSqlClient> client,
                                    StorageRetryPolicy retry_policy,
                                    std::string schema = "trading_core");

    bool UpsertOrder(const Order& order, std::string* error) override;
    bool AppendTrade(const Trade& trade, std::string* error) override;
    bool UpsertPosition(const Position& position, std::string* error) override;
    bool UpsertAccount(const Account& account, std::string* error) override;
    bool AppendRiskEvent(const RiskEventRecord& risk_event, std::string* error) override;
    bool MarkProcessedOrderEvent(const ProcessedOrderEventRecord& event,
                                 std::string* error) override;
    bool ExistsProcessedOrderEvent(const std::string& event_key,
                                   bool* exists,
                                   std::string* error) const override;
    bool InsertPositionDetailFromTrade(const Trade& trade, std::string* error) override;
    bool ClosePositionDetailFifo(const Trade& trade, std::string* error) override;
    bool LoadPositionSummary(const std::string& account_id,
                             const std::string& strategy_id,
                             std::vector<Position>* out,
                             std::string* error) const override;
    bool UpdateOrderCancelRetry(const std::string& client_order_id,
                                std::int32_t cancel_retry_count,
                                EpochNanos last_cancel_ts_ns,
                                std::string* error) override;

private:
    bool InsertWithRetry(const std::string& table,
                         const std::unordered_map<std::string, std::string>& row,
                         std::string* error) const;
    static std::string ToString(std::int32_t value);
    static std::string ToString(std::int64_t value);
    static std::string ToString(double value);
    static std::string ToTimestamp(EpochNanos ts_ns);
    std::string TableName(const std::string& table) const;

    std::shared_ptr<ITimescaleSqlClient> client_;
    StorageRetryPolicy retry_policy_;
    std::string schema_;
};

}  // namespace quant_hft
