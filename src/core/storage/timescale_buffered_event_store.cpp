#include "quant_hft/core/timescale_buffered_event_store.h"

#include <algorithm>
#include <chrono>
#include <utility>

namespace quant_hft {

TimescaleBufferedEventStore::TimescaleBufferedEventStore(
    std::shared_ptr<ITimescaleSqlClient> client,
    StorageRetryPolicy retry_policy,
    TimescaleBufferedStoreOptions options)
    : options_(std::move(options)),
      adapter_(std::move(client), retry_policy, options_.schema),
      worker_(&TimescaleBufferedEventStore::RunWorker, this) {
    if (options_.batch_size == 0) {
        options_.batch_size = 1;
    }
    if (options_.flush_interval_ms <= 0) {
        options_.flush_interval_ms = 1;
    }
    if (options_.schema.empty()) {
        options_.schema = "public";
    }
}

TimescaleBufferedEventStore::~TimescaleBufferedEventStore() { Stop(); }

void TimescaleBufferedEventStore::AppendMarketSnapshot(const MarketSnapshot& snapshot) {
    BufferedRecord record;
    record.kind = RecordKind::kMarket;
    record.market = snapshot;
    Enqueue(std::move(record));
}

void TimescaleBufferedEventStore::AppendOrderEvent(const OrderEvent& event) {
    BufferedRecord record;
    record.kind = RecordKind::kOrder;
    record.order = event;
    Enqueue(std::move(record));
}

void TimescaleBufferedEventStore::AppendRiskDecision(const OrderIntent& intent,
                                                     const RiskDecision& decision) {
    BufferedRecord record;
    record.kind = RecordKind::kRisk;
    record.intent = intent;
    record.decision = decision;
    Enqueue(std::move(record));
}

std::vector<MarketSnapshot> TimescaleBufferedEventStore::GetMarketSnapshots(
    const std::string& instrument_id) const {
    Flush();
    return adapter_.GetMarketSnapshots(instrument_id);
}

std::vector<OrderEvent> TimescaleBufferedEventStore::GetOrderEvents(
    const std::string& client_order_id) const {
    Flush();
    return adapter_.GetOrderEvents(client_order_id);
}

std::vector<RiskDecisionRow> TimescaleBufferedEventStore::GetRiskDecisionRows() const {
    Flush();
    return adapter_.GetRiskDecisionRows();
}

void TimescaleBufferedEventStore::Flush() const {
    {
        std::lock_guard<std::mutex> lock(mutex_);
        cv_.notify_one();
    }

    std::unique_lock<std::mutex> lock(mutex_);
    drained_cv_.wait(lock, [&] { return queue_.empty() && in_flight_ == 0; });
}

void TimescaleBufferedEventStore::RunWorker() {
    while (true) {
        std::vector<BufferedRecord> batch;
        {
            std::unique_lock<std::mutex> lock(mutex_);
            cv_.wait_for(lock,
                         std::chrono::milliseconds(options_.flush_interval_ms),
                         [&] { return stop_ || !queue_.empty(); });

            if (stop_ && queue_.empty() && in_flight_ == 0) {
                break;
            }
            if (queue_.empty()) {
                continue;
            }

            const auto count = std::min<std::size_t>(queue_.size(), options_.batch_size);
            batch.reserve(count);
            for (std::size_t i = 0; i < count; ++i) {
                batch.push_back(std::move(queue_.front()));
                queue_.pop_front();
            }
            in_flight_ += batch.size();
        }

        for (const auto& record : batch) {
            switch (record.kind) {
                case RecordKind::kMarket:
                    adapter_.AppendMarketSnapshot(record.market);
                    break;
                case RecordKind::kOrder:
                    adapter_.AppendOrderEvent(record.order);
                    break;
                case RecordKind::kRisk:
                    adapter_.AppendRiskDecision(record.intent, record.decision);
                    break;
            }
        }

        {
            std::lock_guard<std::mutex> lock(mutex_);
            in_flight_ -= batch.size();
            if (queue_.empty() && in_flight_ == 0) {
                drained_cv_.notify_all();
            }
        }
    }

    {
        std::lock_guard<std::mutex> lock(mutex_);
        drained_cv_.notify_all();
    }
}

void TimescaleBufferedEventStore::Enqueue(BufferedRecord record) {
    {
        std::lock_guard<std::mutex> lock(mutex_);
        queue_.push_back(std::move(record));
        if (queue_.size() >= options_.batch_size) {
            cv_.notify_one();
            return;
        }
    }
}

void TimescaleBufferedEventStore::Stop() {
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (stop_) {
            return;
        }
        stop_ = true;
        cv_.notify_one();
    }

    if (worker_.joinable()) {
        worker_.join();
    }
}

}  // namespace quant_hft
