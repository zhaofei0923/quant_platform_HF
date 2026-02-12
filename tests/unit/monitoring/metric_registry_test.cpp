#include <gtest/gtest.h>

#include "quant_hft/monitoring/metric_registry.h"

#if QUANT_HFT_WITH_METRICS
#include <prometheus/metric_type.h>
#endif

namespace quant_hft {
namespace {

TEST(MetricRegistryTest, CounterIncrementValueMatches) {
    auto counter = MetricRegistry::Instance().BuildCounter(
        "quant_hft_test_counter_total", "test counter", {{"scope", "unit"}});
    ASSERT_NE(counter, nullptr);
    counter->Increment();
    counter->Increment(2.0);

#if QUANT_HFT_WITH_METRICS
    const auto collected = MetricRegistry::Instance().GetPrometheusRegistry()->Collect();
    bool found = false;
    for (const auto& family : collected) {
        if (family.name != "quant_hft_test_counter_total" ||
            family.type != prometheus::MetricType::Counter) {
            continue;
        }
        ASSERT_FALSE(family.metric.empty());
        EXPECT_DOUBLE_EQ(family.metric.front().counter.value, 3.0);
        found = true;
        break;
    }
    EXPECT_TRUE(found);
#else
    SUCCEED();
#endif
}

}  // namespace
}  // namespace quant_hft
