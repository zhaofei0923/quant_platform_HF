#pragma once

#include <functional>
#include <string>
#include <vector>

#include "quant_hft/contracts/types.h"

namespace quant_hft {

struct MarketDataConnectConfig {
    std::string market_front_address;
    std::string trader_front_address;
    std::string flow_path;
    std::string broker_id;
    std::string user_id;
    std::string investor_id;
    std::string password;
    std::string app_id;
    std::string auth_code;
    bool is_production_mode{false};
    bool enable_real_api{false};
    bool enable_terminal_auth{true};
    int connect_timeout_ms{10'000};
    int reconnect_max_attempts{8};
    int reconnect_initial_backoff_ms{500};
    int reconnect_max_backoff_ms{8'000};
};

class IMarketDataGateway {
public:
    using MarketDataCallback = std::function<void(const MarketSnapshot&)>;

    virtual ~IMarketDataGateway() = default;
    virtual bool Connect(const MarketDataConnectConfig& config) = 0;
    virtual void Disconnect() = 0;
    virtual bool Subscribe(const std::vector<std::string>& instrument_ids) = 0;
    virtual bool Unsubscribe(const std::vector<std::string>& instrument_ids) = 0;
    virtual void RegisterMarketDataCallback(MarketDataCallback callback) = 0;
    virtual bool IsHealthy() const = 0;
};

}  // namespace quant_hft
