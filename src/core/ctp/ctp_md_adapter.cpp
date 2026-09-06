#include "quant_hft/core/ctp_md_adapter.h"

#include <string>
#include <utility>

#include "quant_hft/core/structured_log.h"

namespace quant_hft {

CTPMdAdapter::CTPMdAdapter(std::size_t query_qps_limit, std::size_t dispatcher_workers,
                           std::size_t callback_queue_size)
    : gateway_(std::make_shared<CtpGatewayAdapter>(query_qps_limit)),
      dispatcher_(dispatcher_workers),
      callback_dispatcher_(callback_queue_size) {
    InitializeGatewayCallbacks();
}

CTPMdAdapter::CTPMdAdapter(std::shared_ptr<CtpGatewayAdapter> gateway,
                           std::size_t dispatcher_workers, std::size_t callback_queue_size)
    : gateway_(std::move(gateway)),
      owns_gateway_(false),
      dispatcher_(dispatcher_workers),
      callback_dispatcher_(callback_queue_size) {
    if (gateway_ == nullptr) {
        gateway_ = std::make_shared<CtpGatewayAdapter>();
        owns_gateway_ = true;
    }
    InitializeGatewayCallbacks();
}

void CTPMdAdapter::InitializeGatewayCallbacks() {
    callback_dispatcher_.Start();
    connection_listener_token_ = gateway_->AddConnectionStateListener([this](bool healthy) {
        std::lock_guard<std::mutex> lock(mutex_);
        state_ = healthy ? MdSessionState::kReady : MdSessionState::kDisconnected;
    });
    gateway_->RegisterMarketDataCallback(
        [this](const MarketSnapshot& snapshot) { (void)SubmitSnapshot(snapshot); });
}

bool CTPMdAdapter::SubmitSnapshot(const MarketSnapshot& snapshot) {
    TickCallback callback;
    std::function<void(const MarketSnapshot&, const std::string&)> gap;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        callback = user_tick_callback_;
        gap = gap_callback_;
    }
    if (!callback) return false;
    if (callback_dispatcher_.Post(
            [callback, gap, snapshot]() {
                try {
                    callback(snapshot);
                } catch (...) {
                    if (gap) gap(snapshot, "market_consumer_failed");
                }
            },
            false))
        return true;
    if (gap) gap(snapshot, "market_queue_full");
    EmitStructuredLog(nullptr, "ctp_md_adapter", "error", "market_delivery_gap",
                      {{"instrument_id", snapshot.instrument_id}});
    return false;
}

CTPMdAdapter::~CTPMdAdapter() {
    Disconnect();
    if (gateway_ != nullptr) {
        gateway_->RemoveConnectionStateListener(connection_listener_token_);
    }
    StopEventDelivery();
}

bool CTPMdAdapter::Connect(const MarketDataConnectConfig& config) {
    Disconnect();
    dispatcher_.Start();
    if (!gateway_->IsHealthy() && !gateway_->Connect(config)) {
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
    if (owns_gateway_ && gateway_ != nullptr) {
        gateway_->Disconnect();
    }
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
    return gateway_->Subscribe(instrument_ids);
}

bool CTPMdAdapter::Unsubscribe(const std::vector<std::string>& instrument_ids) {
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (state_ != MdSessionState::kReady) {
            return false;
        }
    }
    return gateway_->Unsubscribe(instrument_ids);
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

void CTPMdAdapter::RegisterGapCallback(
    std::function<void(const MarketSnapshot&, const std::string&)> callback) {
    std::lock_guard<std::mutex> lock(mutex_);
    gap_callback_ = std::move(callback);
}

void CTPMdAdapter::StopEventDelivery() {
    {
        std::lock_guard<std::mutex> lock(mutex_);
        user_tick_callback_ = nullptr;
    }
    callback_dispatcher_.Stop();
    std::lock_guard<std::mutex> lock(mutex_);
    gap_callback_ = nullptr;
}

std::string CTPMdAdapter::GetLastConnectDiagnostic() const {
    return gateway_->GetLastConnectDiagnostic();
}

void CTPMdAdapter::UpdateInstrumentMetadata(const std::vector<InstrumentMetaSnapshot>& snapshots) {
    gateway_->UpdateInstrumentMetadata(snapshots);
}

}  // namespace quant_hft
