#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
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

std::string TraderSessionStateToString(TraderSessionState state);

struct CtpTraderReadinessSnapshot {
    bool ready{false};
    TraderSessionState state{TraderSessionState::kDisconnected};
    bool gateway_healthy{false};
    bool settlement_confirmed{false};
    bool need_reconnect{false};
    int reconnect_attempts{0};
    std::string last_reconnect_stage;
    std::string last_connect_diagnostic;
};

class CTPTraderAdapter {
   public:
    using OrderEventCallback = IOrderGateway::OrderEventCallback;
    using OrderSubmitMappingCallback = CtpGatewayAdapter::OrderSubmitMappingCallback;
    using TradingAccountSnapshotCallback = CtpGatewayAdapter::TradingAccountSnapshotCallback;
    using InvestorPositionSnapshotCallback = CtpGatewayAdapter::InvestorPositionSnapshotCallback;
    using InstrumentMetaSnapshotCallback = CtpGatewayAdapter::InstrumentMetaSnapshotCallback;
    using BrokerTradingParamsSnapshotCallback =
        CtpGatewayAdapter::BrokerTradingParamsSnapshotCallback;
    using InstrumentMarginRateSnapshotCallback =
        CtpGatewayAdapter::InstrumentMarginRateSnapshotCallback;
    using InstrumentCommissionRateSnapshotCallback =
        CtpGatewayAdapter::InstrumentCommissionRateSnapshotCallback;
    using InstrumentOrderCommRateSnapshotCallback =
        CtpGatewayAdapter::InstrumentOrderCommRateSnapshotCallback;

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
    CtpTraderReadinessSnapshot GetReadinessSnapshot() const;
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
    bool EnqueueInstrumentQuery(int request_id, const std::string& instrument_id);
    int EnqueueInstrumentQuery();
    int EnqueueInstrumentQuery(const std::string& instrument_id);
    bool EnqueueInstrumentMarginRateQuery(int request_id, const std::string& instrument_id);
    int EnqueueInstrumentMarginRateQuery(const std::string& instrument_id);
    bool EnqueueInstrumentCommissionRateQuery(int request_id, const std::string& instrument_id);
    int EnqueueInstrumentCommissionRateQuery(const std::string& instrument_id);
    bool EnqueueInstrumentOrderCommRateQuery(int request_id, const std::string& instrument_id);
    int EnqueueInstrumentOrderCommRateQuery(const std::string& instrument_id);
    bool EnqueueBrokerTradingParamsQuery(int request_id);
    int EnqueueBrokerTradingParamsQuery();
    bool EnqueueOrderQuery(int request_id);
    int EnqueueOrderQuery();
    bool EnqueueTradeQuery(int request_id);
    int EnqueueTradeQuery();

    void RegisterOrderEventCallback(OrderEventCallback callback);
    void RegisterOrderSubmitMappingCallback(OrderSubmitMappingCallback callback);
    void RegisterTradingAccountSnapshotCallback(TradingAccountSnapshotCallback callback);
    void RegisterInvestorPositionSnapshotCallback(InvestorPositionSnapshotCallback callback);
    void RegisterInstrumentMetaSnapshotCallback(InstrumentMetaSnapshotCallback callback);
    void RegisterBrokerTradingParamsSnapshotCallback(BrokerTradingParamsSnapshotCallback callback);
    void RegisterInstrumentMarginRateSnapshotCallback(
        InstrumentMarginRateSnapshotCallback callback);
    void RegisterInstrumentCommissionRateSnapshotCallback(
        InstrumentCommissionRateSnapshotCallback callback);
    void RegisterInstrumentOrderCommRateSnapshotCallback(
        InstrumentOrderCommRateSnapshotCallback callback);
    void SetCircuitBreaker(std::function<void(bool)> callback);

    CtpUserSessionInfo GetLastUserSession() const;
    TradingAccountSnapshot GetLastTradingAccountSnapshot() const;
    std::vector<InvestorPositionSnapshot> GetLastInvestorPositionSnapshots() const;
    std::string GetLastConnectDiagnostic() const;
    std::string BuildOrderRef(const std::string& strategy_id) const;

   private:
    int AllocateRequestId();
    void StorePromise(int request_id, const std::shared_ptr<std::promise<void>>& promise);
    void ResolvePromise(int request_id);
    void RejectPromise(int request_id, const std::string& error_msg);
    void ResolveSettlementPromise(int request_id);
    void RejectSettlementPromise(int request_id, const std::string& error_msg);
    void ResolveLoginPromise(int request_id, int error_code, const std::string& error_msg);
    void RejectAllPromises(const std::string& error_msg);
    void ScheduleReconnect();
    void OnReconnectTimer(std::uint64_t generation);
    void ResetReconnectState();
    void StopReconnectWorker();
    void ReconnectWorkerLoop();
    void JoinLoginTimeoutThreads();
    bool IsGenerationCurrent(std::uint64_t generation) const;
    void UnregisterGatewayCallbacks();
    void SetReconnectStageLocked(const std::string& stage);
    void SetReconnectStage(const std::string& stage);

    mutable std::mutex mutex_;
    std::shared_ptr<CtpGatewayAdapter> gateway_;
    EventDispatcher dispatcher_;
    CallbackDispatcher callback_dispatcher_;
    OrderEventCallback user_order_event_callback_;
    OrderSubmitMappingCallback user_order_submit_mapping_callback_;
    TradingAccountSnapshotCallback user_trading_account_callback_;
    InvestorPositionSnapshotCallback user_investor_position_callback_;
    InstrumentMetaSnapshotCallback user_instrument_meta_callback_;
    BrokerTradingParamsSnapshotCallback user_broker_trading_params_callback_;
    InstrumentMarginRateSnapshotCallback user_instrument_margin_rate_callback_;
    InstrumentCommissionRateSnapshotCallback user_instrument_commission_rate_callback_;
    InstrumentOrderCommRateSnapshotCallback user_instrument_order_comm_rate_callback_;
    MarketDataConnectConfig last_connect_config_;
    bool has_connect_config_{false};
    TraderSessionState state_{TraderSessionState::kDisconnected};
    bool settlement_confirm_required_{true};
    bool settlement_confirmed_{false};
    std::string last_reconnect_stage_{"disconnected"};
    std::atomic<bool> need_reconnect_{false};
    std::atomic<int> reconnect_attempts_{0};
    std::chrono::steady_clock::time_point last_reconnect_time_{};
    std::atomic<std::uint64_t> lifecycle_generation_{1};
    mutable std::mutex reconnect_mutex_;
    std::condition_variable reconnect_cv_;
    std::thread reconnect_thread_;
    bool reconnect_stop_{false};
    bool reconnect_scheduled_{false};
    std::chrono::steady_clock::time_point reconnect_deadline_{};
    std::uint64_t reconnect_generation_{0};
    std::atomic<int> next_request_id_{1};
    mutable std::mutex promise_map_mutex_;
    std::unordered_map<int, std::shared_ptr<std::promise<void>>> query_promises_;
    std::unordered_map<int, std::shared_ptr<std::promise<void>>> settlement_promises_;
    std::unordered_map<int, std::shared_ptr<std::promise<std::pair<int, std::string>>>>
        login_promises_;
    mutable std::mutex login_timeout_threads_mutex_;
    std::vector<std::thread> login_timeout_threads_;
    mutable std::uint64_t order_ref_seq_{0};
    std::function<void(bool)> circuit_breaker_callback_;
};

}  // namespace quant_hft
