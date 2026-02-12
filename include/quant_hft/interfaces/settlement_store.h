#pragma once

#include <cstdint>
#include <string>

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
    double settlement_price{0.0};
    bool is_final{false};
    EpochNanos created_ts_ns{0};
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

    virtual bool GetRun(const std::string& trading_day,
                        SettlementRunRecord* out,
                        std::string* error) const = 0;
    virtual bool UpsertRun(const SettlementRunRecord& run, std::string* error) = 0;
    virtual bool AppendSummary(const SettlementSummaryRecord& summary, std::string* error) = 0;
    virtual bool AppendDetail(const SettlementDetailRecord& detail, std::string* error) = 0;
    virtual bool AppendPrice(const SettlementPriceRecord& price, std::string* error) = 0;
    virtual bool AppendReconcileDiff(const SettlementReconcileDiffRecord& diff,
                                     std::string* error) = 0;
};

}  // namespace quant_hft
