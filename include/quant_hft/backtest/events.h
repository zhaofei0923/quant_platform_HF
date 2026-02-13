#pragma once

#include <variant>

#include "quant_hft/common/timestamp.h"
#include "quant_hft/contracts/types.h"

namespace quant_hft::backtest {

enum class EventType {
    kMarket,
    kSignal,
    kOrder,
    kFill,
};

struct Event {
    EventType type{EventType::kMarket};
    Timestamp time;
    std::variant<Tick, OrderIntent, Order, Trade> data;
};

}  // namespace quant_hft::backtest
