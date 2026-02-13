#pragma once

#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "quant_hft/common/timestamp.h"
#include "quant_hft/models/bar.h"
#include "quant_hft/models/tick.h"

namespace quant_hft {

class DataFeed {
public:
    virtual ~DataFeed() = default;

    virtual void Subscribe(const std::vector<std::string>& symbols,
                           std::function<void(const Tick&)> on_tick,
                           std::function<void(const Bar&)> on_bar = nullptr) = 0;

    virtual std::vector<Bar> GetHistoryBars(const std::string& symbol,
                                            const Timestamp& start,
                                            const Timestamp& end,
                                            const std::string& timeframe) = 0;

    virtual std::vector<Tick> GetHistoryTicks(const std::string& symbol,
                                              const Timestamp& start,
                                              const Timestamp& end) = 0;

    virtual void Run() = 0;
    virtual void Stop() = 0;
    virtual Timestamp CurrentTime() const = 0;
    virtual bool IsLive() const = 0;
};

}  // namespace quant_hft
