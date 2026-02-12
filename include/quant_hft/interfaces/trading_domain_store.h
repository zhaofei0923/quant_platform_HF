#pragma once

#include <string>
#include <vector>

#include "quant_hft/contracts/types.h"

namespace quant_hft {

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

    virtual bool UpsertOrder(const Order& order, std::string* error) = 0;
    virtual bool AppendTrade(const Trade& trade, std::string* error) = 0;
    virtual bool UpsertPosition(const Position& position, std::string* error) = 0;
    virtual bool UpsertAccount(const Account& account, std::string* error) = 0;
    virtual bool AppendRiskEvent(const RiskEventRecord& risk_event, std::string* error) = 0;
    virtual bool MarkProcessedOrderEvent(const ProcessedOrderEventRecord& event,
                                         std::string* error) = 0;
    virtual bool ExistsProcessedOrderEvent(const std::string& event_key,
                                           bool* exists,
                                           std::string* error) const = 0;
    virtual bool InsertPositionDetailFromTrade(const Trade& trade, std::string* error) = 0;
    virtual bool ClosePositionDetailFifo(const Trade& trade, std::string* error) = 0;
    virtual bool LoadPositionSummary(const std::string& account_id,
                                     const std::string& strategy_id,
                                     std::vector<Position>* out,
                                     std::string* error) const = 0;
    virtual bool UpdateOrderCancelRetry(const std::string& client_order_id,
                                        std::int32_t cancel_retry_count,
                                        EpochNanos last_cancel_ts_ns,
                                        std::string* error) = 0;
};

}  // namespace quant_hft
