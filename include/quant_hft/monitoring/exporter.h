#pragma once

#include <atomic>
#include <memory>
#include <string>
#include <thread>

#if QUANT_HFT_WITH_METRICS
#include <prometheus/exposer.h>
#endif

namespace quant_hft {

class MetricsExporter {
public:
    MetricsExporter() = default;
    ~MetricsExporter();

    bool Start(int port, std::string* error);
    void Stop();
    bool IsRunning() const;

private:
    std::atomic<bool> stop_requested_{false};
    std::atomic<bool> running_{false};
    std::thread worker_;

#if QUANT_HFT_WITH_METRICS
    std::unique_ptr<prometheus::Exposer> exposer_;
#endif
};

}  // namespace quant_hft
