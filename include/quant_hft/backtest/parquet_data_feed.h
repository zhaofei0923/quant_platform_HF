#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

#include "quant_hft/common/timestamp.h"
#include "quant_hft/contracts/types.h"

namespace quant_hft {

struct ParquetPartitionMeta {
    std::string file_path;
    std::string source;
    std::string trading_day;
    std::string instrument_id;
    EpochNanos min_ts_ns{0};
    EpochNanos max_ts_ns{0};
    std::size_t row_count{0};
    std::string schema_version{"v2"};
    std::string source_csv_fingerprint;
};

struct ParquetScanMetrics {
    std::int64_t scan_rows{0};
    std::int64_t scan_row_groups{0};
    std::int64_t io_bytes{0};
    bool early_stop_hit{false};
};

class ParquetDataFeed {
   public:
    explicit ParquetDataFeed(std::string parquet_root = "");

    void SetParquetRoot(const std::string& parquet_root);

    bool RegisterPartition(const ParquetPartitionMeta& partition);

    bool LoadManifestJsonl(const std::string& manifest_path, std::string* error = nullptr);

    std::vector<ParquetPartitionMeta> DiscoverFromDirectory(const std::string& root_path) const;

    std::vector<ParquetPartitionMeta> QueryPartitions(EpochNanos start_ts_ns, EpochNanos end_ts_ns,
                                                      const std::string& instrument_id = "") const;

    std::vector<ParquetPartitionMeta> QueryPartitions(
        EpochNanos start_ts_ns, EpochNanos end_ts_ns,
        const std::vector<std::string>& instrument_ids, const std::string& source = "") const;

    bool LoadPartitionTicks(const ParquetPartitionMeta& partition, const Timestamp& start,
                            const Timestamp& end, const std::vector<std::string>& projected_columns,
                            std::vector<Tick>* out, ParquetScanMetrics* metrics,
                            std::int64_t max_ticks = -1, std::string* error = nullptr) const;

    std::vector<Tick> LoadTicks(const std::string& symbol, const Timestamp& start,
                                const Timestamp& end) const;

    std::size_t PartitionCount() const noexcept;

   private:
    std::string parquet_root_;
    std::vector<ParquetPartitionMeta> partitions_;
};

}  // namespace quant_hft
