#include "quant_hft/monitoring/metric_registry.h"

#include <algorithm>

#if QUANT_HFT_WITH_METRICS
#include <prometheus/counter.h>
#include <prometheus/gauge.h>
#include <prometheus/histogram.h>
#endif

namespace quant_hft {

MonitoringCounter::MonitoringCounter(std::function<void(double)> fn)
    : fn_(std::move(fn)) {}

void MonitoringCounter::Increment(double value) const {
    if (fn_) {
        fn_(value);
    }
}

MonitoringGauge::MonitoringGauge(std::function<void(double)> fn)
    : fn_(std::move(fn)) {}

void MonitoringGauge::Set(double value) const {
    if (fn_) {
        fn_(value);
    }
}

MonitoringHistogram::MonitoringHistogram(std::function<void(double)> fn)
    : fn_(std::move(fn)) {}

void MonitoringHistogram::Observe(double value) const {
    if (fn_) {
        fn_(value);
    }
}

MetricRegistry& MetricRegistry::Instance() {
    static MetricRegistry instance;
    return instance;
}

MetricRegistry::MetricRegistry() {
#if QUANT_HFT_WITH_METRICS
    registry_ = std::make_shared<prometheus::Registry>();
#endif
}

std::string MetricRegistry::BuildMetricKey(const std::string& name, const MetricLabels& labels) {
    std::string key = name;
    for (const auto& [label_key, label_value] : labels) {
        key += "|" + label_key + "=" + label_value;
    }
    return key;
}

std::shared_ptr<MonitoringCounter> MetricRegistry::BuildCounter(const std::string& name,
                                                                const std::string& help,
                                                                const MetricLabels& labels) {
#if !QUANT_HFT_WITH_METRICS
    (void)name;
    (void)help;
    (void)labels;
    return std::make_shared<MonitoringCounter>();
#else
    std::lock_guard<std::mutex> lock(mutex_);
    const std::string metric_key = BuildMetricKey(name, labels);

    auto metric_it = counters_.find(metric_key);
    if (metric_it != counters_.end()) {
        auto* metric = reinterpret_cast<prometheus::Counter*>(metric_it->second);
        return std::make_shared<MonitoringCounter>([metric](double value) { metric->Increment(value); });
    }

    prometheus::Family<prometheus::Counter>* family = nullptr;
    auto family_it = counter_families_.find(name);
    if (family_it == counter_families_.end()) {
        family = &prometheus::BuildCounter().Name(name).Help(help).Register(*registry_);
        counter_families_[name] = family;
    } else {
        family = reinterpret_cast<prometheus::Family<prometheus::Counter>*>(family_it->second);
    }

    auto& metric = family->Add(labels);
    counters_[metric_key] = &metric;
    return std::make_shared<MonitoringCounter>([&metric](double value) { metric.Increment(value); });
#endif
}

std::shared_ptr<MonitoringGauge> MetricRegistry::BuildGauge(const std::string& name,
                                                            const std::string& help,
                                                            const MetricLabels& labels) {
#if !QUANT_HFT_WITH_METRICS
    (void)name;
    (void)help;
    (void)labels;
    return std::make_shared<MonitoringGauge>();
#else
    std::lock_guard<std::mutex> lock(mutex_);
    const std::string metric_key = BuildMetricKey(name, labels);

    auto metric_it = gauges_.find(metric_key);
    if (metric_it != gauges_.end()) {
        auto* metric = reinterpret_cast<prometheus::Gauge*>(metric_it->second);
        return std::make_shared<MonitoringGauge>([metric](double value) { metric->Set(value); });
    }

    prometheus::Family<prometheus::Gauge>* family = nullptr;
    auto family_it = gauge_families_.find(name);
    if (family_it == gauge_families_.end()) {
        family = &prometheus::BuildGauge().Name(name).Help(help).Register(*registry_);
        gauge_families_[name] = family;
    } else {
        family = reinterpret_cast<prometheus::Family<prometheus::Gauge>*>(family_it->second);
    }

    auto& metric = family->Add(labels);
    gauges_[metric_key] = &metric;
    return std::make_shared<MonitoringGauge>([&metric](double value) { metric.Set(value); });
#endif
}

std::shared_ptr<MonitoringHistogram> MetricRegistry::BuildHistogram(
    const std::string& name,
    const std::string& help,
    const std::vector<double>& buckets,
    const MetricLabels& labels) {
#if !QUANT_HFT_WITH_METRICS
    (void)name;
    (void)help;
    (void)buckets;
    (void)labels;
    return std::make_shared<MonitoringHistogram>();
#else
    std::lock_guard<std::mutex> lock(mutex_);
    const std::string metric_key = BuildMetricKey(name, labels);

    auto metric_it = histograms_.find(metric_key);
    if (metric_it != histograms_.end()) {
        auto* metric = reinterpret_cast<prometheus::Histogram*>(metric_it->second);
        return std::make_shared<MonitoringHistogram>([metric](double value) { metric->Observe(value); });
    }

    prometheus::Family<prometheus::Histogram>* family = nullptr;
    auto family_it = histogram_families_.find(name);
    if (family_it == histogram_families_.end()) {
        family = &prometheus::BuildHistogram().Name(name).Help(help).Register(*registry_);
        histogram_families_[name] = family;
    } else {
        family = reinterpret_cast<prometheus::Family<prometheus::Histogram>*>(family_it->second);
    }

    auto& metric = family->Add(labels, buckets);
    histograms_[metric_key] = &metric;
    return std::make_shared<MonitoringHistogram>([&metric](double value) { metric.Observe(value); });
#endif
}

#if QUANT_HFT_WITH_METRICS
std::shared_ptr<prometheus::Registry> MetricRegistry::GetPrometheusRegistry() const {
    return registry_;
}
#endif

}  // namespace quant_hft
