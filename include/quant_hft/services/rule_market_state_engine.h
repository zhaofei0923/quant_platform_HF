#pragma once

#include <deque>
#include <mutex>
#include <memory>
#include <string>
#include <unordered_map>

#include "quant_hft/interfaces/market_state_engine.h"
#include "quant_hft/services/market_state_detector.h"

namespace quant_hft {

class RuleMarketStateEngine : public IMarketStateEngine {
public:
    explicit RuleMarketStateEngine(std::size_t lookback_window = 64,
                                   MarketStateDetectorConfig detector_config = {});

    void OnMarketSnapshot(const MarketSnapshot& snapshot) override;
    StateSnapshot7D GetCurrentState(const std::string& instrument_id) const override;
    void RegisterStateCallback(StateCallback callback) override;

private:
    struct InstrumentBuffer {
        std::deque<double> prices;
        std::deque<std::int64_t> volumes;
        std::unique_ptr<MarketStateDetector> detector;
        EpochNanos last_detector_ts_ns{0};
        StateSnapshot7D latest;
    };

    StateSnapshot7D BuildState(const std::string& instrument_id,
                               const InstrumentBuffer& buffer,
                               const MarketSnapshot& snapshot) const;

    std::size_t lookback_window_;
    MarketStateDetectorConfig detector_config_;
    mutable std::mutex mutex_;
    std::unordered_map<std::string, InstrumentBuffer> buffers_;
    StateCallback callback_;
};

}  // namespace quant_hft
