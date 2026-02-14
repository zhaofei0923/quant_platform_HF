#pragma once

#include <cstddef>
#include <functional>
#include <mutex>
#include <string>
#include <vector>

#include "quant_hft/contracts/types.h"
#include "quant_hft/core/ctp_gateway_adapter.h"
#include "quant_hft/core/event_dispatcher.h"
#include "quant_hft/core/python_callback_dispatcher.h"
#include "quant_hft/interfaces/market_data_gateway.h"

namespace quant_hft {

enum class MdSessionState {
    kDisconnected = 0,
    kConnected = 1,
    kLoggedIn = 2,
    kReady = 3,
};

class CTPMdAdapter {
public:
    using TickCallback = IMarketDataGateway::MarketDataCallback;

    explicit CTPMdAdapter(std::size_t query_qps_limit = 10,
                          std::size_t dispatcher_workers = 1,
                          std::size_t python_queue_size = 5000);
    ~CTPMdAdapter();

    CTPMdAdapter(const CTPMdAdapter&) = delete;
    CTPMdAdapter& operator=(const CTPMdAdapter&) = delete;

    bool Connect(const MarketDataConnectConfig& config);
    void Disconnect();
    bool Subscribe(const std::vector<std::string>& instrument_ids);
    bool Unsubscribe(const std::vector<std::string>& instrument_ids);
    bool IsReady() const;
    MdSessionState SessionState() const;
    void RegisterTickCallback(TickCallback callback);
    std::string GetLastConnectDiagnostic() const;

private:
    mutable std::mutex mutex_;
    CtpGatewayAdapter gateway_;
    EventDispatcher dispatcher_;
    PythonCallbackDispatcher python_dispatcher_;
    TickCallback user_tick_callback_;
    MdSessionState state_{MdSessionState::kDisconnected};
};

}  // namespace quant_hft
