#pragma once

#include <mutex>
#include <string>

#include "quant_hft/core/storage_connection_config.h"
#include "quant_hft/interfaces/market_bus_producer.h"

namespace quant_hft {

class KafkaMarketBusProducer : public IMarketBusProducer {
public:
    explicit KafkaMarketBusProducer(KafkaConnectionConfig config);

    bool PublishMarketSnapshot(const MarketSnapshot& snapshot, std::string* error) override;
    bool Flush(std::string* error) override;

    static std::string SerializeMarketSnapshotJson(const MarketSnapshot& snapshot);

private:
    static bool IsSafeKafkaName(const std::string& text);
    static std::string ReplaceAll(std::string text,
                                  const std::string& pattern,
                                  const std::string& replacement);
    std::string BuildProducerCommand(std::string* error) const;

    KafkaConnectionConfig config_;
    mutable std::mutex publish_mutex_;
};

}  // namespace quant_hft
