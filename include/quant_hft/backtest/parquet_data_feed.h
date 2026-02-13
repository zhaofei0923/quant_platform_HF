#pragma once

#include <cstddef>
#include <string>
#include <vector>

#include "quant_hft/common/timestamp.h"
#include "quant_hft/contracts/types.h"

namespace quant_hft {

struct ParquetPartitionMeta {
    std::string file_path;
    std::string trading_day;
    std::string instrument_id;
    EpochNanos min_ts_ns{0};
    EpochNanos max_ts_ns{0};
    std::size_t row_count{0};
};

class ParquetDataFeed {
public:
    explicit ParquetDataFeed(std::string parquet_root = "");

    void SetParquetRoot(const std::string& parquet_root);

    bool RegisterPartition(const ParquetPartitionMeta& partition);

    std::vector<ParquetPartitionMeta> DiscoverFromDirectory(const std::string& root_path) const;

    std::vector<ParquetPartitionMeta> QueryPartitions(EpochNanos start_ts_ns,
                                                      EpochNanos end_ts_ns,
                                                      const std::string& instrument_id = "") const;

    std::vector<Tick> LoadTicks(const std::string& symbol,
                                const Timestamp& start,
                                const Timestamp& end) const;

    std::size_t PartitionCount() const noexcept;

private:
    std::string parquet_root_;
    std::vector<ParquetPartitionMeta> partitions_;
};

}  // namespace quant_hft
