#pragma once

#include <string>

#include "quant_hft/contracts/types.h"

namespace quant_hft {

struct RiskEventRecord {
    std::string account_id;
    std::string strategy_id;
    std::string instrument_id;
    std::string order_ref;
    std::int32_t event_type{0};
    std::int32_t event_level{0};
    std::string event_desc;
    EpochNanos event_ts_ns{0};
};

class ITradingDomainStore {
public:
    virtual ~ITradingDomainStore() = default;

    virtual bool UpsertOrder(const Order& order, std::string* error) = 0;
    virtual bool AppendTrade(const Trade& trade, std::string* error) = 0;
    virtual bool UpsertPosition(const Position& position, std::string* error) = 0;
    virtual bool UpsertAccount(const Account& account, std::string* error) = 0;
    virtual bool AppendRiskEvent(const RiskEventRecord& risk_event, std::string* error) = 0;
};

}  // namespace quant_hft

