#pragma once

#include <cstdint>
#include <mutex>
#include <string>

#include "quant_hft/contracts/types.h"

namespace quant_hft {

class MarketBusProducer {
public:
    struct PublishResult {
        bool ok{false};
        std::string reason;
    };

    MarketBusProducer(std::string bootstrap_servers,
                      std::string topic,
                      std::string spool_dir = "runtime/market_bus_spool");

    PublishResult PublishTick(const MarketSnapshot& snapshot);
    bool Enabled() const;
    std::uint64_t PublishedCount() const;
    std::uint64_t FailedCount() const;

private:
    std::string SpoolPathForTopic() const;

    mutable std::mutex mutex_;
    std::string bootstrap_servers_;
    std::string topic_;
    std::string spool_dir_;
    std::uint64_t published_count_{0};
    std::uint64_t failed_count_{0};
};

}  // namespace quant_hft
