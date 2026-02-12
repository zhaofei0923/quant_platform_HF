#pragma once

#include <memory>
#include <string>
#include <unordered_map>

#include "quant_hft/core/storage_retry_policy.h"
#include "quant_hft/core/timescale_sql_client.h"
#include "quant_hft/interfaces/trading_ledger_store.h"

namespace quant_hft {

class TradingLedgerStoreClientAdapter : public ITradingLedgerStore {
public:
    TradingLedgerStoreClientAdapter(std::shared_ptr<ITimescaleSqlClient> client,
                                    StorageRetryPolicy retry_policy,
                                    std::string schema);

    bool AppendOrderEvent(const OrderEvent& event, std::string* error) override;
    bool AppendTradeEvent(const OrderEvent& event, std::string* error) override;
    bool AppendAccountSnapshot(const TradingAccountSnapshot& snapshot,
                               std::string* error) override;
    bool AppendPositionSnapshot(const InvestorPositionSnapshot& snapshot,
                                std::string* error) override;
    bool UpsertReplayOffset(const std::string& stream_name,
                            std::int64_t last_seq,
                            std::int64_t updated_ts_ns,
                            std::string* error) override;

private:
    bool InsertWithRetry(const std::string& table,
                         const std::unordered_map<std::string, std::string>& row,
                         std::string* error) const;
    bool IsDuplicateKeyError(const std::string& error) const;
    std::string TableName(const std::string& table) const;
    static std::string ToString(std::int32_t value);
    static std::string ToString(std::int64_t value);
    static std::string ToString(double value);
    static std::string BuildTradeDate(std::int64_t ts_ns);
    static std::string BuildIdempotencyKey(const OrderEvent& event);

    std::shared_ptr<ITimescaleSqlClient> client_;
    StorageRetryPolicy retry_policy_;
    std::string schema_;
};

}  // namespace quant_hft
