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
    bool BindRuntimeIdentity(const std::string& environment, const std::string& broker,
                             const std::string& account, const std::string& instance,
                             std::string* error) override;
    bool AdvanceTradingDay(const std::string& account, const std::string& broker,
                           const std::string& day, std::string* error) override;

    bool TransferStrategyCapital(const std::string& account, const std::string& transfer_id,
                                 const std::string& from, const std::string& to, double amount,
                                 const std::string& reason, std::string* error) override;
    bool AppendCapitalReconciliation(const CapitalReconciliationSnapshot& snapshot,
                                     std::string* error) override;
    bool ReserveStrategyOpen(const StrategyOpenReservationRequest& request,
                             std::string* error) override;
    bool ReserveStrategyClose(const OrderIntent& intent, std::string* error) override;
    bool ObserveStrategyOrderEvent(const OrderEvent& event, std::string* error) override;
    bool LoadStrategyCloseReserved(const std::string& account, const std::string& strategy,
                                   const std::string& instrument, Side side, std::int32_t* out,
                                   std::string* error) const override;
    bool LoadStrategyCloseReserved(const std::string& account, const std::string& strategy,
                                   const std::string& instrument, Side side, HedgeFlag hedge_flag,
                                   std::int32_t* out, std::string* error) const;
    bool ConfigureIndependentStrategyBooks(
        const std::string& account, const std::unordered_map<std::string, double>& allocations,
        std::string* error) override;
    bool LoadBrokerPositionSummary(const std::string& account, std::vector<Position>* out,
                                   std::string* error) const override;
    bool LoadStrategyBook(const std::string& account, const std::string& strategy,
                          StrategyCapitalSnapshot* capital, std::vector<Position>* positions,
                          std::string* error) const override;
    bool LoadStrategyCapital(const std::string& account, const std::string& strategy,
                             StrategyCapitalSnapshot* out, std::string* error) const override;
    bool ApplyTrade(const TradeApplyRequest& request, TradeApplyResult* result,
                    std::string* error) override;
    bool InstallPositionBaseline(const PositionBaseline& baseline, std::string* error) override;
    bool AcknowledgeReceipt(const WalReceipt& receipt, std::string* error) override;
    bool LoadWatermark(const std::string& stream, DomainWatermark* out,
                       std::string* error) const override;
    bool LoadPendingOutbox(const std::string& account, std::vector<TradeOutboxRecord>* out,
                           std::string* error) const override;
    bool AcknowledgeOutbox(const std::string& outbox_id, std::string* error) override;
    bool LoadPendingOutboxForConsumer(const std::string& consumer, const std::string& account,
                                      std::vector<TradeOutboxRecord>* out,
                                      std::string* error) const override;
    bool AcknowledgeOutboxForConsumer(const std::string& consumer, const std::string& id,
                                      std::string* error) override;
    bool LoadTradeHistory(const std::string& account, const std::string& trading_day,
                          std::vector<TradeOutboxRecord>* out, std::string* error) const override;

    bool UpsertOrder(const Order& order, std::string* error) override;
    bool AppendTrade(const Trade& trade, std::string* error) override;
    bool UpsertPosition(const Position& position, std::string* error) override;
    bool UpsertAccount(const Account& account, std::string* error) override;
    bool AppendRiskEvent(const RiskEventRecord& risk_event, std::string* error) override;
    bool MarkProcessedOrderEvent(const ProcessedOrderEventRecord& event,
                                 std::string* error) override;
    bool ExistsProcessedOrderEvent(const std::string& event_key, bool* exists,
                                   std::string* error) const override;
    bool InsertPositionDetailFromTrade(const Trade& trade, std::string* error) override;
    bool ClosePositionDetailFifo(const Trade& trade, std::string* error) override;
    bool LoadPositionSummary(const std::string& account_id, const std::string& strategy_id,
                             std::vector<Position>* out, std::string* error) const override;
    bool UpdateOrderCancelRetry(const std::string& client_order_id, std::int32_t cancel_retry_count,
                                EpochNanos last_cancel_ts_ns, std::string* error) override;

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
