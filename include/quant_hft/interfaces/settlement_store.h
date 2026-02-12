#pragma once

#include <cstdint>
#include <string>
#include <unordered_map>
#include <vector>

#include "quant_hft/contracts/types.h"

namespace quant_hft {

struct SettlementRunRecord {
    std::string trading_day;
    std::string status;
    bool force_run{false};
    EpochNanos heartbeat_ts_ns{0};
    EpochNanos started_ts_ns{0};
    EpochNanos completed_ts_ns{0};
    std::string error_code;
    std::string error_msg;
    std::string evidence_path;
};

struct SettlementSummaryRecord {
    std::string trading_day;
    std::string account_id;
    double pre_balance{0.0};
    double deposit{0.0};
    double withdraw{0.0};
    double commission{0.0};
    double close_profit{0.0};
    double position_profit{0.0};
    double balance{0.0};
    double curr_margin{0.0};
    double available{0.0};
    double risk_degree{0.0};
    EpochNanos created_ts_ns{0};
};

struct SettlementDetailRecord {
    std::string trading_day;
    std::int64_t settlement_id{0};
    std::int64_t position_id{0};
    std::string instrument_id;
    std::int32_t volume{0};
    double settlement_price{0.0};
    double profit{0.0};
    EpochNanos created_ts_ns{0};
};

struct SettlementPriceRecord {
    std::string trading_day;
    std::string instrument_id;
    std::string exchange_id;
    std::string source;
    bool has_settlement_price{true};
    double settlement_price{0.0};
    bool is_final{false};
    EpochNanos created_ts_ns{0};
};

struct SettlementOpenPositionRecord {
    std::int64_t position_id{0};
    std::string account_id;
    std::string strategy_id;
    std::string instrument_id;
    std::string exchange_id;
    std::string open_date;
    double open_price{0.0};
    std::int32_t volume{0};
    bool is_today{false};
    std::string position_date;
    std::int32_t close_volume{0};
    std::int32_t position_status{1};
    double accumulated_mtm{0.0};
    std::string last_settlement_date;
    double last_settlement_price{0.0};
    double last_settlement_profit{0.0};
    EpochNanos update_ts_ns{0};
};

struct SettlementInstrumentRecord {
    std::string instrument_id;
    std::int32_t contract_multiplier{1};
    double long_margin_rate{0.0};
    double short_margin_rate{0.0};
};

struct SettlementAccountFundsRecord {
    bool exists{false};
    std::string account_id;
    std::string trading_day;
    double pre_balance{0.0};
    double deposit{0.0};
    double withdraw{0.0};
    double frozen_commission{0.0};
    double frozen_margin{0.0};
    double available{0.0};
    double curr_margin{0.0};
    double commission{0.0};
    double close_profit{0.0};
    double position_profit{0.0};
    double balance{0.0};
    double risk_degree{0.0};
    double pre_settlement_balance{0.0};
    double floating_profit{0.0};
    EpochNanos update_ts_ns{0};
};

struct SettlementPositionSummaryRecord {
    std::string account_id;
    std::string strategy_id;
    std::string instrument_id;
    std::string exchange_id;
    std::int32_t long_volume{0};
    std::int32_t short_volume{0};
    std::int32_t long_today_volume{0};
    std::int32_t short_today_volume{0};
    std::int32_t long_yd_volume{0};
    std::int32_t short_yd_volume{0};
};

struct SettlementOrderKey {
    std::string order_ref;
    std::int32_t front_id{0};
    std::int32_t session_id{0};
};

struct SettlementReconcileDiffRecord {
    std::string trading_day;
    std::string account_id;
    std::string diff_type;
    std::string key_ref;
    double local_value{0.0};
    double ctp_value{0.0};
    double delta_value{0.0};
    std::string diagnose_hint;
    std::string raw_payload;
    EpochNanos created_ts_ns{0};
};

class ISettlementStore {
public:
    virtual ~ISettlementStore() = default;

    virtual bool BeginTransaction(std::string* error) = 0;
    virtual bool CommitTransaction(std::string* error) = 0;
    virtual bool RollbackTransaction(std::string* error) = 0;

    virtual bool GetRun(const std::string& trading_day,
                        SettlementRunRecord* out,
                        std::string* error) const = 0;
    virtual bool UpsertRun(const SettlementRunRecord& run, std::string* error) = 0;
    virtual bool AppendSummary(const SettlementSummaryRecord& summary, std::string* error) = 0;
    virtual bool AppendDetail(const SettlementDetailRecord& detail, std::string* error) = 0;
    virtual bool AppendPrice(const SettlementPriceRecord& price, std::string* error) = 0;
    virtual bool AppendReconcileDiff(const SettlementReconcileDiffRecord& diff,
                                     std::string* error) = 0;
    virtual bool LoadOpenPositions(const std::string& account_id,
                                   std::vector<SettlementOpenPositionRecord>* out,
                                   std::string* error) const = 0;
    virtual bool LoadInstruments(const std::vector<std::string>& instrument_ids,
                                 std::unordered_map<std::string, SettlementInstrumentRecord>* out,
                                 std::string* error) const = 0;
    virtual bool UpdatePositionAfterSettlement(const SettlementOpenPositionRecord& position,
                                               std::string* error) = 0;
    virtual bool RolloverPositionDetail(const std::string& account_id, std::string* error) = 0;
    virtual bool RolloverPositionSummary(const std::string& account_id, std::string* error) = 0;
    virtual bool LoadAccountFunds(const std::string& account_id,
                                  const std::string& trading_day,
                                  SettlementAccountFundsRecord* out,
                                  std::string* error) const = 0;
    virtual bool SumDeposit(const std::string& account_id,
                            const std::string& trading_day,
                            double* out,
                            std::string* error) const = 0;
    virtual bool SumWithdraw(const std::string& account_id,
                             const std::string& trading_day,
                             double* out,
                             std::string* error) const = 0;
    virtual bool SumCommission(const std::string& account_id,
                               const std::string& trading_day,
                               double* out,
                               std::string* error) const = 0;
    virtual bool SumCloseProfit(const std::string& account_id,
                                const std::string& trading_day,
                                double* out,
                                std::string* error) const = 0;
    virtual bool UpsertAccountFunds(const SettlementAccountFundsRecord& funds,
                                    std::string* error) = 0;
    virtual bool LoadPositionSummary(const std::string& account_id,
                                     std::vector<SettlementPositionSummaryRecord>* out,
                                     std::string* error) const = 0;
    virtual bool LoadOrderKeysByDay(const std::string& account_id,
                                    const std::string& trading_day,
                                    std::vector<SettlementOrderKey>* out,
                                    std::string* error) const = 0;
    virtual bool LoadTradeIdsByDay(const std::string& account_id,
                                   const std::string& trading_day,
                                   std::vector<std::string>* out,
                                   std::string* error) const = 0;
    virtual bool UpsertSystemConfig(const std::string& key,
                                    const std::string& value,
                                    std::string* error) = 0;
};

}  // namespace quant_hft
