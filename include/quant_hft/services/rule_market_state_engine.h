#pragma once

#include <deque>
#include <mutex>
#include <string>
#include <unordered_map>

#include "quant_hft/interfaces/market_state_engine.h"

namespace quant_hft {

class RuleMarketStateEngine : public IMarketStateEngine {
public:
    explicit RuleMarketStateEngine(std::size_t lookback_window = 64);

    void OnMarketSnapshot(const MarketSnapshot& snapshot) override;
    StateSnapshot7D GetCurrentState(const std::string& instrument_id) const override;
    void RegisterStateCallback(StateCallback callback) override;

private:
    struct InstrumentBuffer {
        std::deque<double> prices;
        std::deque<std::int64_t> volumes;
        StateSnapshot7D latest;
    };

    StateSnapshot7D BuildState(const std::string& instrument_id,
                               const InstrumentBuffer& buffer,
                               const MarketSnapshot& snapshot) const;

    std::size_t lookback_window_;
    mutable std::mutex mutex_;
    std::unordered_map<std::string, InstrumentBuffer> buffers_;
    StateCallback callback_;
};

}  // namespace quant_hft
