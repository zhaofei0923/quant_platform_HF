#pragma once

#include <cstdint>

namespace quant_hft {

enum class EventType {
    kUnknown = 0,
    kFrontConnected,
    kFrontDisconnected,
    kRspUserLogin,
    kRspAuthenticate,
    kRspSettlementInfoConfirm,
    kRspQuery,
    kRspError,
    kRtnOrder,
    kRtnTrade,
    kRtnDepthMarketData,
};

enum class EventPriority {
    kHigh = 0,
    kNormal = 1,
    kLow = 2,
};

struct EventEnvelope {
    EventType type{EventType::kUnknown};
    EventPriority priority{EventPriority::kNormal};
    std::int64_t ts_ns{0};
};

}  // namespace quant_hft
