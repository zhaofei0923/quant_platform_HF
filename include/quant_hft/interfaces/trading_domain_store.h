#pragma once

#include <string>
#include <vector>

#include "quant_hft/contracts/types.h"
#include "quant_hft/contracts/wal_receipt.h"

namespace quant_hft {

enum class TradeApplyStatus { kApplied, kDuplicate, kCoveredByBaseline, kConflict, kFailed };

enum class GenericClosePriority { kUnspecified, kTodayFirst, kYesterdayFirst };

enum class TradeFeeModel { kExplicitCommission, kMoneyPlusVolumeV1 };
struct TradeFeeRate {
    double by_money{0.0};
    double by_volume{0.0};
};

struct TradeAccountingPolicy {
    GenericClosePriority generic_close_priority{GenericClosePriority::kUnspecified};
    std::string close_rule_source;
    std::string close_rule_version;
    // This explicit valuation basis is opening-lot price, not a claim about the
    // broker's mark-to-settlement CloseProfitByDate convention.
    bool valuation_inputs_verified{false};
    double contract_multiplier{0.0};
    double commission{0.0};
    std::string valuation_source;
    // Only an explicitly verified fee-date convention may reuse CloseAllocation.
    // The domain computes mixed-bucket fees after allocation inside ApplyTrade.
    TradeFeeModel fee_model{TradeFeeModel::kExplicitCommission};
    TradeFeeRate open_fee;
    TradeFeeRate close_fee;
    TradeFeeRate close_today_fee;
    std::string fee_date_basis;
    std::string fee_allocation_source;
    std::string fee_allocation_version;
};

struct TradeApplyRequest {
    Trade trade;
    WalReceipt receipt;
    // Explicitly enabled only for deterministic simulation/unit tests, never live recovery.
    bool allow_ephemeral{false};
    bool historical{false};
    TradeAccountingPolicy accounting_policy;
    // Enforced only after raw-identity duplicate detection, before any new fact is committed.
    bool require_verified_accounting{false};
};

struct TradeApplyResult {
    TradeApplyStatus status{TradeApplyStatus::kFailed};
    std::string identity_key;
    std::string outbox_id;
    Position position;
    CloseAllocation close_allocation;
    std::string error;
    std::uint64_t commit_sequence{0};
    std::string close_rule_source;
    std::string close_rule_version;
};

struct PositionBaseline {
    std::string baseline_id;
    std::string account_id;
    std::string trading_day;
    std::string broker_id;
    // Caller must finish and reconcile the broker position/trade queries before installation.
    bool complete{false};
    std::vector<Position> positions;
    std::vector<Trade> covered_trades;
};

struct TradeOutboxRecord {
    std::string event_kind{"trade"};
    std::string outbox_id;
    std::string identity_key;
    std::string account_id;
    std::string strategy_id;
    std::string instrument_id;
    std::uint64_t position_version{0};
    Trade trade;
    Position position;
    std::uint64_t commit_sequence{0};
    CloseAllocation close_allocation;
    std::string close_rule_source;
    std::string close_rule_version;
    std::string valuation_source;
    std::string fee_model;
    std::string fee_date_basis;
    std::string fee_allocation_source;
    std::string fee_allocation_version;
};

struct DomainWatermark {
    std::string stream_id;
    // First unapplied sequence; gaps never advance this value.
    std::uint64_t next_sequence{0};
    bool initialized{false};
};

struct RiskEventRecord {
    std::string account_id;
    std::string strategy_id;
    std::string instrument_id;
    std::string order_ref;
    std::string rule_id;
    std::int32_t event_type{0};
    std::int32_t event_level{0};
    std::string event_desc;
    std::string tags_json;
    std::string details_json;
    EpochNanos event_ts_ns{0};
};

struct ProcessedOrderEventRecord {
    std::string event_key;
    std::string order_ref;
    std::int32_t front_id{0};
    std::int32_t session_id{0};
    std::int32_t event_type{0};
    std::string trade_id;
    std::string event_source;
    EpochNanos processed_ts_ns{0};
};

class ITradingDomainStore {
   public:
    virtual ~ITradingDomainStore() = default;
    virtual bool AdvanceTradingDay(const std::string&, const std::string&, const std::string&,
                                   std::string* error) {
        if (error != nullptr) *error = "atomic trading day advancement unsupported";
        return false;
    }
    virtual bool BindRuntimeIdentity(const std::string&, const std::string&, const std::string&,
                                     const std::string&, std::string* error) {
        if (error != nullptr) *error = "runtime identity binding unsupported";
        return false;
    }

    virtual bool ApplyTrade(const TradeApplyRequest&, TradeApplyResult*, std::string* error) {
        if (error != nullptr) *error = "atomic ApplyTrade unsupported";
        return false;
    }
    virtual bool InstallPositionBaseline(const PositionBaseline&, std::string* error) {
        if (error != nullptr) *error = "position baseline unsupported";
        return false;
    }
    virtual bool AcknowledgeReceipt(const WalReceipt&, std::string* error) {
        if (error != nullptr) *error = "receipt acknowledgement unsupported";
        return false;
    }
    virtual bool LoadWatermark(const std::string&, DomainWatermark*, std::string* error) const {
        if (error != nullptr) *error = "domain watermark unsupported";
        return false;
    }
    virtual bool LoadPendingOutbox(const std::string&, std::vector<TradeOutboxRecord>*,
                                   std::string* error) const {
        if (error != nullptr) *error = "trade outbox unsupported";
        return false;
    }
    virtual bool AcknowledgeOutbox(const std::string&, std::string* error) {
        if (error != nullptr) *error = "trade outbox acknowledgement unsupported";
        return false;
    }
    // Each consumer must be idempotent on outbox_id; delivery is at least once.
    virtual bool LoadPendingOutboxForConsumer(const std::string&, const std::string&,
                                              std::vector<TradeOutboxRecord>*,
                                              std::string* error) const {
        if (error != nullptr) *error = "consumer outbox unsupported";
        return false;
    }
    virtual bool AcknowledgeOutboxForConsumer(const std::string&, const std::string&,
                                              std::string* error) {
        if (error != nullptr) *error = "consumer outbox acknowledgement unsupported";
        return false;
    }
    virtual bool LoadTradeHistory(const std::string&, const std::string&,
                                  std::vector<TradeOutboxRecord>*, std::string* error) const {
        if (error != nullptr) *error = "trade history unsupported";
        return false;
    }

    virtual bool UpsertOrder(const Order& order, std::string* error) = 0;
    virtual bool AppendTrade(const Trade& trade, std::string* error) = 0;
    virtual bool UpsertPosition(const Position& position, std::string* error) = 0;
    virtual bool UpsertAccount(const Account& account, std::string* error) = 0;
    virtual bool AppendRiskEvent(const RiskEventRecord& risk_event, std::string* error) = 0;
    virtual bool MarkProcessedOrderEvent(const ProcessedOrderEventRecord& event,
                                         std::string* error) = 0;
    virtual bool ExistsProcessedOrderEvent(const std::string& event_key, bool* exists,
                                           std::string* error) const = 0;
    virtual bool InsertPositionDetailFromTrade(const Trade& trade, std::string* error) = 0;
    virtual bool ClosePositionDetailFifo(const Trade& trade, std::string* error) = 0;
    virtual bool LoadPositionSummary(const std::string& account_id, const std::string& strategy_id,
                                     std::vector<Position>* out, std::string* error) const = 0;
    virtual bool UpdateOrderCancelRetry(const std::string& client_order_id,
                                        std::int32_t cancel_retry_count,
                                        EpochNanos last_cancel_ts_ns, std::string* error) = 0;
};

}  // namespace quant_hft
