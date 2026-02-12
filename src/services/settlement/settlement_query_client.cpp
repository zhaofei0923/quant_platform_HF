#include "quant_hft/services/settlement_query_client.h"

#include <algorithm>
#include <chrono>
#include <thread>

namespace quant_hft {

SettlementQueryClient::SettlementQueryClient(std::shared_ptr<CTPTraderAdapter> trader,
                                             std::shared_ptr<FlowController> flow_controller,
                                             SettlementQueryClientConfig config)
    : trader_(std::move(trader)),
      flow_controller_(std::move(flow_controller)),
      config_(std::move(config)) {
    config_.retry_max = std::max(1, config_.retry_max);
    config_.backoff_initial_ms = std::max(1, config_.backoff_initial_ms);
    config_.backoff_max_ms = std::max(config_.backoff_initial_ms, config_.backoff_max_ms);
    config_.acquire_timeout_ms = std::max(1, config_.acquire_timeout_ms);
    if (trader_ != nullptr) {
        trader_->RegisterOrderEventCallback([this](const OrderEvent& event) {
            if (event.event_source != "OnRspQryOrder" && event.event_source != "OnRspQryTrade") {
                return;
            }
            std::lock_guard<std::mutex> lock(backfill_mutex_);
            backfill_events_.push_back(event);
        });
    }
}

bool SettlementQueryClient::QueryTradingAccountWithRetry(int request_id_seed, std::string* error) {
    return QueryWithRetry(
        "trading_account",
        request_id_seed,
        [this](int request_id) { return trader_->EnqueueTradingAccountQuery(request_id); },
        error);
}

bool SettlementQueryClient::QueryInvestorPositionWithRetry(int request_id_seed,
                                                           std::string* error) {
    return QueryWithRetry(
        "investor_position",
        request_id_seed,
        [this](int request_id) { return trader_->EnqueueInvestorPositionQuery(request_id); },
        error);
}

bool SettlementQueryClient::QueryInstrumentWithRetry(int request_id_seed, std::string* error) {
    return QueryWithRetry(
        "instrument",
        request_id_seed,
        [this](int request_id) { return trader_->EnqueueInstrumentQuery(request_id); },
        error);
}

bool SettlementQueryClient::QueryOrderTradeBackfill(std::vector<OrderEvent>* out_events,
                                                    std::string* error) {
    {
        std::lock_guard<std::mutex> lock(backfill_mutex_);
        backfill_events_.clear();
    }

    std::string order_error;
    if (!QueryWithRetry(
            "order_backfill",
            30,
            [this](int request_id) { return trader_->EnqueueOrderQuery(request_id); },
            &order_error)) {
        if (error != nullptr) {
            *error = order_error;
        }
        return false;
    }

    std::string trade_error;
    if (!QueryWithRetry(
            "trade_backfill",
            40,
            [this](int request_id) { return trader_->EnqueueTradeQuery(request_id); },
            &trade_error)) {
        if (error != nullptr) {
            *error = trade_error;
        }
        return false;
    }

    // Wait briefly for asynchronous callback dispatcher to flush query responses.
    std::size_t previous_size = 0;
    int stable_rounds = 0;
    constexpr int kSleepMs = 20;
    constexpr int kMaxWaitMs = 500;
    const int max_rounds = kMaxWaitMs / kSleepMs;
    for (int round = 0; round < max_rounds; ++round) {
        std::this_thread::sleep_for(std::chrono::milliseconds(kSleepMs));
        std::size_t current_size = 0;
        {
            std::lock_guard<std::mutex> lock(backfill_mutex_);
            current_size = backfill_events_.size();
        }
        if (current_size == previous_size) {
            ++stable_rounds;
            if (stable_rounds >= 3) {
                break;
            }
        } else {
            stable_rounds = 0;
            previous_size = current_size;
        }
    }

    if (out_events != nullptr) {
        std::lock_guard<std::mutex> lock(backfill_mutex_);
        *out_events = backfill_events_;
    }
    return true;
}

bool SettlementQueryClient::GetLastTradingAccountSnapshot(TradingAccountSnapshot* out_snapshot,
                                                          std::string* error) {
    if (out_snapshot == nullptr) {
        if (error != nullptr) {
            *error = "output trading account snapshot is null";
        }
        return false;
    }
    if (!QueryTradingAccountWithRetry(1, error)) {
        return false;
    }
    if (trader_ == nullptr) {
        if (error != nullptr) {
            *error = "trader adapter is null";
        }
        return false;
    }
    *out_snapshot = trader_->GetLastTradingAccountSnapshot();
    return true;
}

bool SettlementQueryClient::GetLastInvestorPositionSnapshots(
    std::vector<InvestorPositionSnapshot>* out_snapshots,
    std::string* error) {
    if (out_snapshots == nullptr) {
        if (error != nullptr) {
            *error = "output investor position snapshots is null";
        }
        return false;
    }
    if (!QueryInvestorPositionWithRetry(10, error)) {
        return false;
    }
    if (trader_ == nullptr) {
        if (error != nullptr) {
            *error = "trader adapter is null";
        }
        return false;
    }
    *out_snapshots = trader_->GetLastInvestorPositionSnapshots();
    return true;
}

bool SettlementQueryClient::QueryWithRetry(const std::string& name,
                                           int request_id_seed,
                                           const std::function<bool(int)>& sender,
                                           std::string* error) {
    if (trader_ == nullptr || flow_controller_ == nullptr) {
        if (error != nullptr) {
            *error = "settlement query client dependencies are null";
        }
        return false;
    }
    if (!sender) {
        if (error != nullptr) {
            *error = "query sender callback is empty";
        }
        return false;
    }

    std::string last_error;
    int backoff_ms = config_.backoff_initial_ms;
    for (int attempt = 1; attempt <= config_.retry_max; ++attempt) {
        std::string permit_error;
        if (!AcquireQueryPermit(&permit_error)) {
            last_error = "query[" + name + "] flow control rejected: " + permit_error;
        } else {
            const int request_id = request_id_seed + attempt - 1;
            if (sender(request_id)) {
                return true;
            }
            last_error = "query[" + name + "] request enqueue failed at attempt=" +
                         std::to_string(attempt);
        }

        if (attempt < config_.retry_max) {
            std::this_thread::sleep_for(std::chrono::milliseconds(backoff_ms));
            backoff_ms = std::min(config_.backoff_max_ms, backoff_ms * 2);
        }
    }
    if (error != nullptr) {
        *error = last_error.empty() ? "query failed" : last_error;
    }
    return false;
}

bool SettlementQueryClient::AcquireQueryPermit(std::string* error) const {
    Operation op;
    op.account_id = config_.account_id;
    op.type = OperationType::kSettlementQuery;
    op.instrument_id = "";
    const auto result = flow_controller_->Acquire(op, config_.acquire_timeout_ms);
    if (result.allowed) {
        return true;
    }
    if (error != nullptr) {
        *error = result.reason.empty() ? "rate_limited" : result.reason;
    }
    return false;
}

}  // namespace quant_hft
