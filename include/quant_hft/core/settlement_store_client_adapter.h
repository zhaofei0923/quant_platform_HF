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

    bool BeginTransaction(std::string* error) override;
    bool CommitTransaction(std::string* error) override;
    bool RollbackTransaction(std::string* error) override;

    bool GetRun(const std::string& trading_day,
                SettlementRunRecord* out,
                std::string* error) const override;
    bool UpsertRun(const SettlementRunRecord& run, std::string* error) override;
    bool AppendSummary(const SettlementSummaryRecord& summary, std::string* error) override;
    bool AppendDetail(const SettlementDetailRecord& detail, std::string* error) override;
    bool AppendPrice(const SettlementPriceRecord& price, std::string* error) override;
    bool AppendReconcileDiff(const SettlementReconcileDiffRecord& diff,
                             std::string* error) override;
    bool LoadOpenPositions(const std::string& account_id,
                           std::vector<SettlementOpenPositionRecord>* out,
                           std::string* error) const override;
    bool LoadInstruments(const std::vector<std::string>& instrument_ids,
                         std::unordered_map<std::string, SettlementInstrumentRecord>* out,
                         std::string* error) const override;
    bool UpdatePositionAfterSettlement(const SettlementOpenPositionRecord& position,
                                       std::string* error) override;
    bool RolloverPositionDetail(const std::string& account_id, std::string* error) override;
    bool RolloverPositionSummary(const std::string& account_id, std::string* error) override;
    bool LoadAccountFunds(const std::string& account_id,
                          const std::string& trading_day,
                          SettlementAccountFundsRecord* out,
                          std::string* error) const override;
    bool SumDeposit(const std::string& account_id,
                    const std::string& trading_day,
                    double* out,
                    std::string* error) const override;
    bool SumWithdraw(const std::string& account_id,
                     const std::string& trading_day,
                     double* out,
                     std::string* error) const override;
    bool SumCommission(const std::string& account_id,
                       const std::string& trading_day,
                       double* out,
                       std::string* error) const override;
    bool SumCloseProfit(const std::string& account_id,
                        const std::string& trading_day,
                        double* out,
                        std::string* error) const override;
    bool UpsertAccountFunds(const SettlementAccountFundsRecord& funds,
                            std::string* error) override;
    bool LoadPositionSummary(const std::string& account_id,
                             std::vector<SettlementPositionSummaryRecord>* out,
                             std::string* error) const override;
    bool LoadOrderKeysByDay(const std::string& account_id,
                            const std::string& trading_day,
                            std::vector<SettlementOrderKey>* out,
                            std::string* error) const override;
    bool LoadTradeIdsByDay(const std::string& account_id,
                           const std::string& trading_day,
                           std::vector<std::string>* out,
                           std::string* error) const override;
    bool UpsertSystemConfig(const std::string& key,
                            const std::string& value,
                            std::string* error) override;

private:
    bool SumTradeField(const std::string& account_id,
                       const std::string& trading_day,
                       const std::string& field_name,
                       double* out,
                       std::string* error) const;
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
    bool in_transaction_{false};
};

}  // namespace quant_hft
