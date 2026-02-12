#pragma once

#include <cstdint>
#include <string>

#include "quant_hft/contracts/types.h"

namespace quant_hft {

class ITradingLedgerStore {
public:
    virtual ~ITradingLedgerStore() = default;

    virtual bool AppendOrderEvent(const OrderEvent& event, std::string* error) = 0;
    virtual bool AppendTradeEvent(const OrderEvent& event, std::string* error) = 0;
    virtual bool AppendAccountSnapshot(const TradingAccountSnapshot& snapshot,
                                       std::string* error) = 0;
    virtual bool AppendPositionSnapshot(const InvestorPositionSnapshot& snapshot,
                                        std::string* error) = 0;
    virtual bool UpsertReplayOffset(const std::string& stream_name,
                                    std::int64_t last_seq,
                                    std::int64_t updated_ts_ns,
                                    std::string* error) = 0;
};

}  // namespace quant_hft
