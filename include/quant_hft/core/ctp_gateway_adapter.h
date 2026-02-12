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
    using ConnectionStateCallback = std::function<void(bool healthy)>;
    using LoginResponseCallback =
        std::function<void(int request_id, int error_code, const std::string& error_msg)>;
    using QueryCompleteCallback =
        std::function<void(int request_id, const std::string& query_name, bool success)>;
    using SettlementConfirmCallback =
        std::function<void(int request_id, int error_code, const std::string& error_msg)>;
    using TradingAccountSnapshotCallback =
        std::function<void(const TradingAccountSnapshot&)>;
    using InvestorPositionSnapshotCallback =
        std::function<void(const std::vector<InvestorPositionSnapshot>&)>;
    using InstrumentMetaSnapshotCallback =
        std::function<void(const std::vector<InstrumentMetaSnapshot>&)>;
    using BrokerTradingParamsSnapshotCallback =
        std::function<void(const BrokerTradingParamsSnapshot&)>;

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
    virtual bool EnqueueUserSessionQuery(int request_id);
    virtual bool EnqueueTradingAccountQuery(int request_id);
    virtual bool EnqueueInvestorPositionQuery(int request_id);
    virtual bool EnqueueInstrumentQuery(int request_id);
    virtual bool EnqueueInstrumentMarginRateQuery(int request_id, const std::string& instrument_id);
    virtual bool EnqueueInstrumentCommissionRateQuery(int request_id, const std::string& instrument_id);
    virtual bool EnqueueBrokerTradingParamsQuery(int request_id);
    virtual bool EnqueueOrderQuery(int request_id);
    virtual bool EnqueueTradeQuery(int request_id);

    virtual bool RequestUserLogin(int request_id,
                                  const std::string& broker_id,
                                  const std::string& user_id,
                                  const std::string& password);
    virtual bool RequestSettlementInfoConfirm(int request_id);

    virtual void RegisterConnectionStateCallback(ConnectionStateCallback callback);
    virtual void RegisterLoginResponseCallback(LoginResponseCallback callback);
    virtual void RegisterQueryCompleteCallback(QueryCompleteCallback callback);
    virtual void RegisterSettlementConfirmCallback(SettlementConfirmCallback callback);

    void RegisterTradingAccountSnapshotCallback(TradingAccountSnapshotCallback callback);
    void RegisterInvestorPositionSnapshotCallback(InvestorPositionSnapshotCallback callback);
    void RegisterInstrumentMetaSnapshotCallback(InstrumentMetaSnapshotCallback callback);
    void RegisterBrokerTradingParamsSnapshotCallback(
        BrokerTradingParamsSnapshotCallback callback);

    CtpUserSessionInfo GetLastUserSession() const;
    TradingAccountSnapshot GetLastTradingAccountSnapshot() const;
    std::vector<InvestorPositionSnapshot> GetLastInvestorPositionSnapshots() const;
    std::vector<InstrumentMetaSnapshot> GetLastInstrumentMetaSnapshots() const;
    BrokerTradingParamsSnapshot GetLastBrokerTradingParamsSnapshot() const;
    void UpdateOffsetApplySrc(char apply_src);
    char GetOffsetApplySrc() const;
    std::string GetLastConnectDiagnostic() const;
    static void NormalizeMarketSnapshot(MarketSnapshot* snapshot);

private:
    friend class CtpMdSpi;
    friend class CtpTdSpi;

    struct OrderMeta {
        std::string order_ref;
        std::string instrument_id;
        Side side{Side::kBuy};
        OffsetFlag offset{OffsetFlag::kOpen};
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
    bool ExecuteTdQueryWithRetry(const std::function<int()>& request_fn) const;
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
    TradingAccountSnapshot trading_account_snapshot_;
    std::vector<InvestorPositionSnapshot> investor_position_snapshots_;
    std::vector<InstrumentMetaSnapshot> instrument_meta_snapshots_;
    BrokerTradingParamsSnapshot broker_trading_params_snapshot_;

    TradingAccountSnapshotCallback trading_account_snapshot_callback_;
    InvestorPositionSnapshotCallback investor_position_snapshot_callback_;
    InstrumentMetaSnapshotCallback instrument_meta_snapshot_callback_;
    BrokerTradingParamsSnapshotCallback broker_trading_params_snapshot_callback_;
    ConnectionStateCallback connection_state_callback_;
    LoginResponseCallback login_response_callback_;
    QueryCompleteCallback query_complete_callback_;
    SettlementConfirmCallback settlement_confirm_callback_;

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
