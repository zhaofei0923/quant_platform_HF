#pragma once

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "quant_hft/contracts/types.h"
#include "quant_hft/core/callback_dispatcher.h"
#include "quant_hft/core/ctp_gateway_adapter.h"
#include "quant_hft/core/event_dispatcher.h"
#include "quant_hft/interfaces/market_data_gateway.h"
#include "quant_hft/interfaces/order_gateway.h"

namespace quant_hft {

enum class TraderSessionState {
    kDisconnected = 0,
    kConnected = 1,
    kAuthenticated = 2,
    kLoggedIn = 3,
    kSettlementConfirmed = 4,
    kReady = 5,
};

class CTPTraderAdapter {
   public:
    using OrderEventCallback = IOrderGateway::OrderEventCallback;
    using TradingAccountSnapshotCallback = CtpGatewayAdapter::TradingAccountSnapshotCallback;
    using InvestorPositionSnapshotCallback = CtpGatewayAdapter::InvestorPositionSnapshotCallback;
    using InstrumentMetaSnapshotCallback = CtpGatewayAdapter::InstrumentMetaSnapshotCallback;
    using BrokerTradingParamsSnapshotCallback =
        CtpGatewayAdapter::BrokerTradingParamsSnapshotCallback;

    explicit CTPTraderAdapter(std::size_t query_qps_limit = 10, std::size_t dispatcher_workers = 1,
                              std::size_t callback_queue_size = 5000,
                              std::int64_t callback_critical_wait_ms = 10);
    explicit CTPTraderAdapter(std::shared_ptr<CtpGatewayAdapter> gateway,
                              std::size_t dispatcher_workers = 1,
                              std::size_t callback_queue_size = 5000,
                              std::int64_t callback_critical_wait_ms = 10);
    ~CTPTraderAdapter();

    CTPTraderAdapter(const CTPTraderAdapter&) = delete;
    CTPTraderAdapter& operator=(const CTPTraderAdapter&) = delete;

    bool Connect(const MarketDataConnectConfig& config);
    void Disconnect();
    bool IsReady() const;
    TraderSessionState SessionState() const;
    bool ConfirmSettlement();
    bool PlaceOrder(const OrderIntent& intent);
    std::string PlaceOrderWithRef(const OrderIntent& intent);
    bool CancelOrder(const std::string& client_order_id, const std::string& trace_id);

    std::future<std::pair<int, std::string>> LoginAsync(const std::string& broker_id,
                                                        const std::string& user_id,
                                                        const std::string& password,
                                                        int timeout_ms = 5000);
    bool RecoverOrdersAndTrades(int timeout_ms = 10000);

    bool EnqueueUserSessionQuery(int request_id);
    int EnqueueUserSessionQuery();
    bool EnqueueTradingAccountQuery(int request_id);
    int EnqueueTradingAccountQuery();
    bool EnqueueInvestorPositionQuery(int request_id);
    int EnqueueInvestorPositionQuery();
    bool EnqueueInstrumentQuery(int request_id);
    int EnqueueInstrumentQuery();
    bool EnqueueInstrumentMarginRateQuery(int request_id, const std::string& instrument_id);
    int EnqueueInstrumentMarginRateQuery(const std::string& instrument_id);
    bool EnqueueInstrumentCommissionRateQuery(int request_id, const std::string& instrument_id);
    int EnqueueInstrumentCommissionRateQuery(const std::string& instrument_id);
    bool EnqueueBrokerTradingParamsQuery(int request_id);
    int EnqueueBrokerTradingParamsQuery();
    bool EnqueueOrderQuery(int request_id);
    int EnqueueOrderQuery();
    bool EnqueueTradeQuery(int request_id);
    int EnqueueTradeQuery();

    void RegisterOrderEventCallback(OrderEventCallback callback);
    void RegisterTradingAccountSnapshotCallback(TradingAccountSnapshotCallback callback);
    void RegisterInvestorPositionSnapshotCallback(InvestorPositionSnapshotCallback callback);
    void RegisterInstrumentMetaSnapshotCallback(InstrumentMetaSnapshotCallback callback);
    void RegisterBrokerTradingParamsSnapshotCallback(BrokerTradingParamsSnapshotCallback callback);
    void SetCircuitBreaker(std::function<void(bool)> callback);

    CtpUserSessionInfo GetLastUserSession() const;
    TradingAccountSnapshot GetLastTradingAccountSnapshot() const;
    std::vector<InvestorPositionSnapshot> GetLastInvestorPositionSnapshots() const;
    std::string GetLastConnectDiagnostic() const;
    std::string BuildOrderRef(const std::string& strategy_id) const;

   private:
    static constexpr int kMaxReconnectAttempts = 10;
    static constexpr int kBaseReconnectDelayMs = 1000;

    int AllocateRequestId();
    void StorePromise(int request_id, const std::shared_ptr<std::promise<void>>& promise);
    void ResolvePromise(int request_id);
    void RejectPromise(int request_id, const std::string& error_msg);
    void ResolveSettlementPromise(int request_id);
    void RejectSettlementPromise(int request_id, const std::string& error_msg);
    void ResolveLoginPromise(int request_id, int error_code, const std::string& error_msg);
    void ScheduleReconnect();
    void OnReconnectTimer();
    void ResetReconnectState();

    mutable std::mutex mutex_;
    std::shared_ptr<CtpGatewayAdapter> gateway_;
    EventDispatcher dispatcher_;
    CallbackDispatcher callback_dispatcher_;
    OrderEventCallback user_order_event_callback_;
    TradingAccountSnapshotCallback user_trading_account_callback_;
    InvestorPositionSnapshotCallback user_investor_position_callback_;
    InstrumentMetaSnapshotCallback user_instrument_meta_callback_;
    BrokerTradingParamsSnapshotCallback user_broker_trading_params_callback_;
    MarketDataConnectConfig last_connect_config_;
    bool has_connect_config_{false};
    TraderSessionState state_{TraderSessionState::kDisconnected};
    bool settlement_confirm_required_{true};
    bool settlement_confirmed_{false};
    std::atomic<bool> need_reconnect_{false};
    std::atomic<int> reconnect_attempts_{0};
    std::chrono::steady_clock::time_point last_reconnect_time_{};
    std::atomic<int> next_request_id_{1};
    mutable std::mutex promise_map_mutex_;
    std::unordered_map<int, std::shared_ptr<std::promise<void>>> query_promises_;
    std::unordered_map<int, std::shared_ptr<std::promise<void>>> settlement_promises_;
    std::unordered_map<int, std::shared_ptr<std::promise<std::pair<int, std::string>>>>
        login_promises_;
    mutable std::uint64_t order_ref_seq_{0};
    std::function<void(bool)> circuit_breaker_callback_;
};

}  // namespace quant_hft
