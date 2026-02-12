#include "quant_hft/monitoring/exporter.h"

#include <chrono>
#include <exception>

#include "quant_hft/monitoring/metric_registry.h"

namespace quant_hft {

MetricsExporter::~MetricsExporter() {
    Stop();
}

bool MetricsExporter::Start(int port, std::string* error) {
    if (running_.load()) {
        return true;
    }
#if !QUANT_HFT_WITH_METRICS
    (void)port;
    if (error != nullptr) {
        *error = "metrics support not enabled at build time";
    }
    return false;
#else
    stop_requested_.store(false);
    running_.store(false);

    worker_ = std::thread([this, port]() {
        try {
            auto exposer = std::make_unique<prometheus::Exposer>(
                "0.0.0.0:" + std::to_string(port));
            exposer->RegisterCollectable(MetricRegistry::Instance().GetPrometheusRegistry());
            exposer_ = std::move(exposer);
            running_.store(true);
            while (!stop_requested_.load()) {
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
            }
            exposer_.reset();
            running_.store(false);
        } catch (...) {
            running_.store(false);
        }
    });

    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (std::chrono::steady_clock::now() < deadline) {
        if (running_.load()) {
            if (error != nullptr) {
                error->clear();
            }
            return true;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }

    stop_requested_.store(true);
    if (worker_.joinable()) {
        worker_.join();
    }
    if (error != nullptr) {
        *error = "metrics exporter failed to start";
    }
    return false;
#endif
}

void MetricsExporter::Stop() {
    stop_requested_.store(true);
    if (worker_.joinable()) {
        worker_.join();
    }
    running_.store(false);
}

bool MetricsExporter::IsRunning() const {
    return running_.load();
}

}  // namespace quant_hft
