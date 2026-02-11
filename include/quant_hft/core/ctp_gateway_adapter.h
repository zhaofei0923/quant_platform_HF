#pragma once

#include <cstdint>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "quant_hft/core/ctp_config.h"
#include "quant_hft/core/query_scheduler.h"
#include "quant_hft/interfaces/market_data_gateway.h"
#include "quant_hft/interfaces/order_gateway.h"

namespace quant_hft {

struct CtpUserSessionInfo {
    std::string investor_id;
    std::string login_time;
    std::string last_login_time;
    std::string reserve_info;
};

// Phase-1 adapter skeleton for CTP v6.7.11.
class CtpGatewayAdapter : public IMarketDataGateway, public IOrderGateway {
public:
    explicit CtpGatewayAdapter(std::size_t query_qps_limit = 10);
    ~CtpGatewayAdapter() override;

    bool Connect(const MarketDataConnectConfig& config) override;
    void Disconnect() override;
    bool Subscribe(const std::vector<std::string>& instrument_ids) override;
    bool Unsubscribe(const std::vector<std::string>& instrument_ids) override;
    void RegisterMarketDataCallback(MarketDataCallback callback) override;
    bool IsHealthy() const override;

    bool PlaceOrder(const OrderIntent& intent) override;
    bool CancelOrder(const std::string& client_order_id,
                     const std::string& trace_id) override;
    void RegisterOrderEventCallback(OrderEventCallback callback) override;

    // v6.7.11 query entry (ReqQryUserSession) through scheduler.
    bool EnqueueUserSessionQuery(int request_id);

    CtpUserSessionInfo GetLastUserSession() const;
    void UpdateOffsetApplySrc(char apply_src);
    char GetOffsetApplySrc() const;
    std::string GetLastConnectDiagnostic() const;

private:
    friend class CtpMdSpi;
    friend class CtpTdSpi;

    struct OrderMeta {
        std::string order_ref;
        std::string instrument_id;
        int front_id{0};
        int session_id{0};
    };

    struct RealApiState;

    bool ConnectSimulated();
    bool ConnectRealApi();
    bool ConnectRealApiWithFrontPair(const CtpRuntimeConfig& runtime,
                                     bool was_connected,
                                     const CtpFrontPair& front_pair,
                                     std::string* failure_detail);
    void StartReconnectWorker();
    void StopReconnectWorker();
    void RequestReconnect();
    void HandleConnectionLoss();
    void ReconnectWorkerLoop();
    void TryMarkHealthyFromState();
    bool ReplayMarketDataSubscriptions();
    void DisconnectRealApi();
    int NextRequestIdLocked();
    std::string NextOrderRefLocked();

    bool connected_{false};
    bool healthy_{false};
    CtpRuntimeConfig runtime_config_;

    mutable std::mutex mutex_;
    std::unordered_set<std::string> subscriptions_;
    std::unordered_map<std::string, OrderMeta> client_order_meta_;
    std::unordered_map<std::string, std::string> order_ref_to_client_id_;
    MarketDataCallback market_data_callback_;
    OrderEventCallback order_event_callback_;

    QueryScheduler query_scheduler_;
    CtpUserSessionInfo user_session_;
    char offset_apply_src_{'0'};
    int front_id_{0};
    int session_id_{0};
    int request_id_seq_{0};
    std::uint64_t order_ref_seq_{0};
    std::string last_connect_diagnostic_;

    std::condition_variable reconnect_cv_;
    std::thread reconnect_thread_;
    bool reconnect_stop_{false};
    bool reconnect_requested_{false};
    bool reconnect_in_progress_{false};

    std::unique_ptr<RealApiState> real_api_;
};

}  // namespace quant_hft
