#pragma once

#include <cstdint>
#include <fstream>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

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
    void SetAllowedInstrumentIds(const std::vector<std::string>& instrument_ids);
    void ClearAllowedInstrumentIds();
    bool AppendTick(const MarketSnapshot& snapshot, std::string* error);
    bool AppendBar(const BarSnapshot& bar, std::string* error);
    bool AppendTimeframeBar(const BarSnapshot& bar, std::int32_t timeframe_minutes,
                            std::string* error);
    bool Close(std::string* error);

    bool enabled() const noexcept { return config_.enabled; }
    bool is_open() const noexcept { return is_open_; }
    std::int64_t ticks_written() const noexcept { return ticks_written_; }
    std::int64_t bars_written() const noexcept { return bars_written_; }
    const std::string& tick_path() const noexcept { return tick_path_; }
    const std::string& bar_path() const noexcept { return bar_path_; }
    const std::string& output_dir() const noexcept { return output_dir_; }

   private:
    struct ProductStreams {
        std::string tick_path;
        std::string bar_path;
        std::ofstream tick_out;
        std::ofstream bar_out;
        std::unordered_map<std::int32_t, std::string> timeframe_bar_paths;
        std::unordered_map<std::int32_t, std::unique_ptr<std::ofstream>> timeframe_bar_outs;
    };

    ProductStreams* EnsureProductStreams(const std::string& instrument_id, std::string* error);
    std::ofstream* EnsureProductTimeframeBarStream(ProductStreams* streams,
                                                   const std::string& instrument_id,
                                                   std::int32_t timeframe_minutes,
                                                   std::string* error);
    std::ofstream* EnsureGlobalTimeframeBarStream(std::int32_t timeframe_minutes,
                                                  std::string* error);
    bool ShouldRecordInstrumentLocked(const std::string& instrument_id) const;

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
    std::unordered_map<std::int32_t, std::string> timeframe_bar_paths_;
    std::unordered_map<std::int32_t, std::unique_ptr<std::ofstream>> timeframe_bar_outs_;
    std::unordered_map<std::string, std::unique_ptr<ProductStreams>> product_streams_;
    bool restrict_to_allowed_instruments_{false};
    std::unordered_set<std::string> allowed_instrument_ids_;
};

}  // namespace quant_hft
