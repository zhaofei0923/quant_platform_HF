#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

#include "quant_hft/contracts/types.h"

namespace quant_hft {

struct SubStrategyIndicatorTraceRow {
    std::string instrument_id;
    EpochNanos ts_ns{0};
    std::string strategy_id;
    std::string strategy_type;
    double bar_open{0.0};
    double bar_high{0.0};
    double bar_low{0.0};
    double bar_close{0.0};
    double bar_volume{0.0};
    std::optional<double> kama;
    std::optional<double> atr;
    std::optional<double> adx;
    std::optional<double> er;
    MarketRegime market_regime{MarketRegime::kUnknown};
};

class SubStrategyIndicatorTraceParquetWriter {
   public:
    SubStrategyIndicatorTraceParquetWriter() = default;
    ~SubStrategyIndicatorTraceParquetWriter() = default;

    bool Open(const std::string& output_path, std::string* error);
    bool Append(const SubStrategyIndicatorTraceRow& row, std::string* error);
    bool Close(std::string* error);

    std::int64_t rows_written() const noexcept { return rows_written_; }
    const std::string& output_path() const noexcept { return output_path_; }
    bool is_open() const noexcept { return is_open_; }

   private:
    bool is_open_{false};
    std::int64_t rows_written_{0};
    std::string output_path_;

#if QUANT_HFT_ENABLE_ARROW_PARQUET
    std::vector<SubStrategyIndicatorTraceRow> rows_;
#endif
};

}  // namespace quant_hft
