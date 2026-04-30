#pragma once

#include <cstdint>
#include <fstream>
#include <mutex>
#include <string>

#include "quant_hft/core/ctp_config.h"
#include "quant_hft/services/bar_aggregator.h"

namespace quant_hft {

class MarketDataCsvRecorder {
   public:
    MarketDataCsvRecorder() = default;
    ~MarketDataCsvRecorder();

    MarketDataCsvRecorder(const MarketDataCsvRecorder&) = delete;
    MarketDataCsvRecorder& operator=(const MarketDataCsvRecorder&) = delete;

    bool Open(MarketDataRecordingConfig config, std::string* error);
    bool AppendTick(const MarketSnapshot& snapshot, std::string* error);
    bool AppendBar(const BarSnapshot& bar, std::string* error);
    bool Close(std::string* error);

    bool enabled() const noexcept { return config_.enabled; }
    bool is_open() const noexcept { return is_open_; }
    std::int64_t ticks_written() const noexcept { return ticks_written_; }
    std::int64_t bars_written() const noexcept { return bars_written_; }
    const std::string& tick_path() const noexcept { return tick_path_; }
    const std::string& bar_path() const noexcept { return bar_path_; }
    const std::string& output_dir() const noexcept { return output_dir_; }

   private:
    bool is_open_{false};
    std::int64_t ticks_written_{0};
    std::int64_t bars_written_{0};
    std::string output_dir_;
    std::string tick_path_;
    std::string bar_path_;
    MarketDataRecordingConfig config_;
    mutable std::mutex mutex_;
    std::ofstream tick_out_;
    std::ofstream bar_out_;
};

}  // namespace quant_hft