#pragma once

#include <cstdint>
#include <fstream>
#include <string>

#include "quant_hft/backtest/sub_strategy_indicator_trace_parquet_writer.h"

namespace quant_hft {

class SubStrategyIndicatorTraceCsvWriter {
   public:
    SubStrategyIndicatorTraceCsvWriter() = default;
    ~SubStrategyIndicatorTraceCsvWriter() = default;

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
    std::ofstream out_;
};

}  // namespace quant_hft
