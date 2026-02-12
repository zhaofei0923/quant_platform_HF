#include "quant_hft/core/ctp_md_adapter.h"

#include <utility>

namespace quant_hft {

CTPMdAdapter::CTPMdAdapter(std::size_t query_qps_limit, std::size_t dispatcher_workers)
    : gateway_(query_qps_limit),
      dispatcher_(dispatcher_workers) {
    gateway_.RegisterMarketDataCallback([this](const MarketSnapshot& snapshot) {
        MarketSnapshot copied = snapshot;
        dispatcher_.Post(
            [this, copied]() {
                TickCallback callback;
                {
                    std::lock_guard<std::mutex> lock(mutex_);
                    callback = user_tick_callback_;
                }
                if (callback) {
                    callback(copied);
                }
            },
            EventPriority::kHigh);
    });
}

CTPMdAdapter::~CTPMdAdapter() {
    Disconnect();
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
