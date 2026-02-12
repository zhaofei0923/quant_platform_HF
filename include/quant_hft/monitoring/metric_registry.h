#pragma once

#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#if QUANT_HFT_WITH_METRICS
#include <prometheus/registry.h>
#endif

namespace quant_hft {

using MetricLabels = std::map<std::string, std::string>;

class MonitoringCounter {
public:
    explicit MonitoringCounter(std::function<void(double)> fn = nullptr);
    void Increment(double value = 1.0) const;

private:
    std::function<void(double)> fn_;
};

class MonitoringGauge {
public:
    explicit MonitoringGauge(std::function<void(double)> fn = nullptr);
    void Set(double value) const;

private:
    std::function<void(double)> fn_;
};

class MonitoringHistogram {
public:
    explicit MonitoringHistogram(std::function<void(double)> fn = nullptr);
    void Observe(double value) const;

private:
    std::function<void(double)> fn_;
};

class MetricRegistry {
public:
    static MetricRegistry& Instance();

    std::shared_ptr<MonitoringCounter> BuildCounter(const std::string& name,
                                                    const std::string& help,
                                                    const MetricLabels& labels = {});

    std::shared_ptr<MonitoringGauge> BuildGauge(const std::string& name,
                                                const std::string& help,
                                                const MetricLabels& labels = {});

    std::shared_ptr<MonitoringHistogram> BuildHistogram(
        const std::string& name,
        const std::string& help,
        const std::vector<double>& buckets,
        const MetricLabels& labels = {});

#if QUANT_HFT_WITH_METRICS
    std::shared_ptr<prometheus::Registry> GetPrometheusRegistry() const;
#endif

private:
    MetricRegistry();

    static std::string BuildMetricKey(const std::string& name, const MetricLabels& labels);

    mutable std::mutex mutex_;

#if QUANT_HFT_WITH_METRICS
    std::shared_ptr<prometheus::Registry> registry_;
    std::unordered_map<std::string, void*> counter_families_;
    std::unordered_map<std::string, void*> gauge_families_;
    std::unordered_map<std::string, void*> histogram_families_;
    std::unordered_map<std::string, void*> counters_;
    std::unordered_map<std::string, void*> gauges_;
    std::unordered_map<std::string, void*> histograms_;
#endif
};

}  // namespace quant_hft
