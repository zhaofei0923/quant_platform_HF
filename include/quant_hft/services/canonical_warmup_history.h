#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

#include "quant_hft/contracts/types.h"

namespace quant_hft {

struct CanonicalWarmupHistoryOptions {
    std::string market_data_root;
    std::string product_id;
    std::string instrument_id;
    std::int32_t timeframe_minutes{5};
    std::size_t limit{128};
    EpochNanos max_ts_ns{0};
};

struct CanonicalWarmupHistoryResult {
    std::vector<StateSnapshot7D> states;
    std::size_t files_scanned{0};
    std::size_t rows_scanned{0};
    std::size_t rows_accepted{0};
    std::size_t rows_rejected{0};
    std::size_t conflicting_rows{0};
    std::size_t legacy_rows_validated{0};
};

// Loads only canonical, strategy-eligible bars from product-partitioned live CSV files. Legacy
// five-minute rows are accepted only when they can be reproduced exactly from five consecutive
// one-minute rows in the same partition. The returned states are ordered by event timestamp and
// capped to the most recent `limit` rows. Rejected or conflicting rows are never allowed to
// contribute to a strategy warmup barrier.
bool LoadCanonicalWarmupHistory(const CanonicalWarmupHistoryOptions& options,
                                CanonicalWarmupHistoryResult* result, std::string* error);

}  // namespace quant_hft
