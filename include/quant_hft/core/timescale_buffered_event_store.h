#pragma once

#include <condition_variable>
#include <cstddef>
#include <deque>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "quant_hft/core/storage_retry_policy.h"
#include "quant_hft/core/timescale_event_store_client_adapter.h"
#include "quant_hft/interfaces/timeseries_store.h"

namespace quant_hft {

struct TimescaleBufferedStoreOptions {
    std::size_t batch_size{128};
    int flush_interval_ms{50};
    std::string schema{"public"};
};

class TimescaleBufferedEventStore : public ITimeseriesStore {
public:
    TimescaleBufferedEventStore(std::shared_ptr<ITimescaleSqlClient> client,
                                StorageRetryPolicy retry_policy,
                                TimescaleBufferedStoreOptions options);
    ~TimescaleBufferedEventStore() override;

    TimescaleBufferedEventStore(const TimescaleBufferedEventStore&) = delete;
    TimescaleBufferedEventStore& operator=(const TimescaleBufferedEventStore&) = delete;

    void AppendMarketSnapshot(const MarketSnapshot& snapshot) override;
    void AppendOrderEvent(const OrderEvent& event) override;
    void AppendRiskDecision(const OrderIntent& intent,
                            const RiskDecision& decision) override;

    std::vector<MarketSnapshot> GetMarketSnapshots(
        const std::string& instrument_id) const override;
    std::vector<OrderEvent> GetOrderEvents(
        const std::string& client_order_id) const override;
    std::vector<RiskDecisionRow> GetRiskDecisionRows() const override;

    void Flush() const;

private:
    enum class RecordKind {
        kMarket,
        kOrder,
        kRisk,
    };

    struct BufferedRecord {
        RecordKind kind{RecordKind::kMarket};
        MarketSnapshot market;
        OrderEvent order;
        OrderIntent intent;
        RiskDecision decision;
    };

    void RunWorker();
    void Enqueue(BufferedRecord record);
    void Stop();

    mutable std::mutex mutex_;
    mutable std::condition_variable cv_;
    mutable std::condition_variable drained_cv_;
    mutable std::deque<BufferedRecord> queue_;
    mutable std::size_t in_flight_{0};
    bool stop_{false};

    TimescaleBufferedStoreOptions options_;
    TimescaleEventStoreClientAdapter adapter_;
    std::thread worker_;
};

}  // namespace quant_hft
