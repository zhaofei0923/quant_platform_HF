#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "quant_hft/core/storage_retry_policy.h"
#include "quant_hft/core/timescale_sql_client.h"
#include "quant_hft/interfaces/settlement_store.h"

namespace quant_hft {

class SettlementStoreClientAdapter : public ISettlementStore {
public:
    SettlementStoreClientAdapter(std::shared_ptr<ITimescaleSqlClient> client,
                                 StorageRetryPolicy retry_policy,
                                 std::string trading_schema,
                                 std::string ops_schema);

    bool GetRun(const std::string& trading_day,
                SettlementRunRecord* out,
                std::string* error) const override;
    bool UpsertRun(const SettlementRunRecord& run, std::string* error) override;
    bool AppendSummary(const SettlementSummaryRecord& summary, std::string* error) override;
    bool AppendDetail(const SettlementDetailRecord& detail, std::string* error) override;
    bool AppendPrice(const SettlementPriceRecord& price, std::string* error) override;
    bool AppendReconcileDiff(const SettlementReconcileDiffRecord& diff,
                             std::string* error) override;

private:
    bool InsertWithRetry(const std::string& table,
                         const std::unordered_map<std::string, std::string>& row,
                         std::string* error) const;
    bool UpsertWithRetry(const std::string& table,
                         const std::unordered_map<std::string, std::string>& row,
                         const std::vector<std::string>& conflict_keys,
                         const std::vector<std::string>& update_keys,
                         std::string* error) const;
    bool IsDuplicateKeyError(const std::string& error) const;
    bool IsUpsertUnsupportedError(const std::string& error) const;
    std::string TableName(const std::string& schema, const std::string& table) const;
    static std::string ToString(std::int32_t value);
    static std::string ToString(std::int64_t value);
    static std::string ToString(double value);
    static std::string ToTimestamp(EpochNanos ts_ns);

    std::shared_ptr<ITimescaleSqlClient> client_;
    StorageRetryPolicy retry_policy_;
    std::string trading_schema_;
    std::string ops_schema_;
};

}  // namespace quant_hft
