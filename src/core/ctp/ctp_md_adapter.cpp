#include "quant_hft/core/ctp_md_adapter.h"

#include <string>
#include <utility>

#include "quant_hft/core/structured_log.h"

namespace quant_hft {

CTPMdAdapter::CTPMdAdapter(std::size_t query_qps_limit,
                           std::size_t dispatcher_workers,
                           std::size_t python_queue_size)
    : gateway_(query_qps_limit),
      dispatcher_(dispatcher_workers),
      python_dispatcher_(python_queue_size) {
    python_dispatcher_.Start();
    gateway_.RegisterMarketDataCallback([this](const MarketSnapshot& snapshot) {
        MarketSnapshot copied = snapshot;
        if (!dispatcher_.Post(
                [this, copied]() {
                    TickCallback callback;
                    {
                        std::lock_guard<std::mutex> lock(mutex_);
                        callback = user_tick_callback_;
                    }
                    if (!callback) {
                        return;
                    }
                    if (!python_dispatcher_.Post([callback, copied]() { callback(copied); }, false)) {
                        const auto stats = python_dispatcher_.GetStats();
                        EmitStructuredLog(
                            nullptr,
                            "ctp_md_adapter",
                            "warn",
                            "python_callback_dropped",
                            {{"is_critical", "false"},
                             {"queue_depth", std::to_string(stats.pending)},
                             {"queue_capacity", std::to_string(stats.max_queue_size)},
                             {"dropped_total", std::to_string(stats.dropped)}});
                    }
                },
                EventPriority::kHigh)) {
            const auto stats = dispatcher_.GetStats();
            EmitStructuredLog(nullptr,
                              "ctp_md_adapter",
                              "error",
                              "dispatcher_queue_full",
                              {{"priority", "high"},
                               {"queue_depth", std::to_string(stats.pending_high)},
                               {"dropped_total", std::to_string(stats.dropped_total)}});
        }
    });
}

CTPMdAdapter::~CTPMdAdapter() {
    Disconnect();
    python_dispatcher_.Stop();
}

bool CTPMdAdapter::Connect(const MarketDataConnectConfig& config) {
    Disconnect();
    dispatcher_.Start();
    if (!gateway_.Connect(config)) {
        std::lock_guard<std::mutex> lock(mutex_);
        state_ = MdSessionState::kDisconnected;
        dispatcher_.Stop();
        return false;
    }
    std::lock_guard<std::mutex> lock(mutex_);
    state_ = MdSessionState::kConnected;
    state_ = MdSessionState::kLoggedIn;
    state_ = MdSessionState::kReady;
    return true;
}

void CTPMdAdapter::Disconnect() {
    gateway_.Disconnect();
    dispatcher_.Stop();
    std::lock_guard<std::mutex> lock(mutex_);
    state_ = MdSessionState::kDisconnected;
}

bool CTPMdAdapter::Subscribe(const std::vector<std::string>& instrument_ids) {
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (state_ != MdSessionState::kReady) {
            return false;
        }
    }
    return gateway_.Subscribe(instrument_ids);
}

bool CTPMdAdapter::Unsubscribe(const std::vector<std::string>& instrument_ids) {
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (state_ != MdSessionState::kReady) {
            return false;
        }
    }
    return gateway_.Unsubscribe(instrument_ids);
}

bool CTPMdAdapter::IsReady() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return state_ == MdSessionState::kReady;
}

MdSessionState CTPMdAdapter::SessionState() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return state_;
}

void CTPMdAdapter::RegisterTickCallback(TickCallback callback) {
    std::lock_guard<std::mutex> lock(mutex_);
    user_tick_callback_ = std::move(callback);
}

std::string CTPMdAdapter::GetLastConnectDiagnostic() const {
    return gateway_.GetLastConnectDiagnostic();
}

}  // namespace quant_hft
