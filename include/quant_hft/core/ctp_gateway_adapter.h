#pragma once

#include <algorithm>
#include <condition_variable>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <type_traits>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "quant_hft/core/ctp_config.h"
#include "quant_hft/core/query_batch_collector.h"
#include "quant_hft/core/query_scheduler.h"
#include "quant_hft/interfaces/market_data_gateway.h"
#include "quant_hft/interfaces/order_gateway.h"

namespace quant_hft {

struct CtpUserSessionInfo {
    std::string investor_id;
    std::string login_time;
    std::string last_login_time;
    std::string reserve_info;
    std::string trading_day;
    std::string max_order_ref;
    int front_id{0};
    int session_id{0};
};

// Phase-1 adapter skeleton for CTP v6.7.11.
class CtpGatewayAdapter : public IMarketDataGateway, public IOrderGateway {
   public:
    using ConnectionStateCallback = std::function<void(bool healthy)>;
    using ConnectionListenerToken = std::uint64_t;
    using OrderSubmitMappingCallback = std::function<void(const CtpOrderSubmitMapping&)>;
    using OrderSubmitPrepareCallback =
        std::function<bool(const CtpOrderSubmitMapping&, std::string* error)>;
    using LoginResponseCallback =
        std::function<void(int request_id, int error_code, const std::string& error_msg)>;
    using QueryCompleteCallback =
        std::function<void(int request_id, const std::string& query_name, bool success)>;
    using SettlementConfirmCallback =
        std::function<void(int request_id, int error_code, const std::string& error_msg)>;
    using TradingAccountSnapshotCallback = std::function<void(const TradingAccountSnapshot&)>;
    using TradingAccountQueryStartCallback = std::function<void(int, std::uint64_t)>;
    using TradingAccountQueryCallback =
        std::function<void(const QueryResult<TradingAccountSnapshot>&)>;
    using InvestorPositionSnapshotCallback =
        std::function<void(const std::vector<InvestorPositionSnapshot>&)>;
    using InvestorPositionQueryCallback =
        std::function<void(const QueryResult<InvestorPositionSnapshot>&)>;
    using InstrumentMetaQueryCallback =
        std::function<void(const QueryResult<InstrumentMetaSnapshot>&)>;
    using InstrumentCommissionRateQueryCallback =
        std::function<void(const QueryResult<InstrumentCommissionRateSnapshot>&)>;
    using InstrumentOrderCommRateQueryCallback =
        std::function<void(const QueryResult<InstrumentOrderCommRateSnapshot>&)>;
    using InstrumentMetaSnapshotCallback =
        std::function<void(const std::vector<InstrumentMetaSnapshot>&)>;
    using DepthMarketSnapshotCallback = std::function<void(const std::vector<MarketSnapshot>&)>;
    using BrokerTradingParamsSnapshotCallback =
        std::function<void(const BrokerTradingParamsSnapshot&)>;
    using InstrumentMarginRateSnapshotCallback =
        std::function<void(const std::vector<InstrumentMarginRateSnapshot>&)>;
    using InstrumentCommissionRateSnapshotCallback =
        std::function<void(const std::vector<InstrumentCommissionRateSnapshot>&)>;
    using InstrumentOrderCommRateSnapshotCallback =
        std::function<void(const std::vector<InstrumentOrderCommRateSnapshot>&)>;

    explicit CtpGatewayAdapter(std::size_t query_qps_limit = 10);
    void CompleteScheduledQuery(int request_id, std::uint64_t generation);
    bool IsQueryActive(int request_id, std::uint64_t generation) const;
    bool RecordQueryResponse(int request_id, std::uint64_t generation, bool success);
    std::uint64_t GetQueryGeneration() const;
    void PollQueries();
    ~CtpGatewayAdapter() override;

    bool Connect(const MarketDataConnectConfig& config) override;
    void Disconnect() override;
    bool Subscribe(const std::vector<std::string>& instrument_ids) override;
    bool Unsubscribe(const std::vector<std::string>& instrument_ids) override;
    void RegisterMarketDataCallback(MarketDataCallback callback) override;
    bool IsHealthy() const override;

    bool PlaceOrder(const OrderIntent& intent) override;
    bool CancelOrder(const std::string& client_order_id, const std::string& trace_id) override;
    void RegisterOrderEventCallback(OrderEventCallback callback) override;
    virtual void RegisterOrderSubmitMappingCallback(OrderSubmitMappingCallback callback);
    virtual void RegisterOrderSubmitPrepareCallback(OrderSubmitPrepareCallback callback);

    // v6.7.11 query entry (ReqQryUserSession) through scheduler.
    virtual bool EnqueueUserSessionQuery(int request_id);
    virtual bool EnqueueTradingAccountQuery(int request_id);
    virtual bool EnqueueInvestorPositionQuery(int request_id);
    virtual bool EnqueueInstrumentQuery(int request_id);
    virtual bool EnqueueInstrumentQuery(int request_id, const std::string& instrument_id);
    virtual bool EnqueueDepthMarketDataQuery(int request_id);
    virtual bool EnqueueInstrumentMarginRateQuery(int request_id, const std::string& instrument_id);
    virtual bool EnqueueInstrumentCommissionRateQuery(int request_id,
                                                      const std::string& instrument_id);
    virtual bool EnqueueInstrumentOrderCommRateQuery(int request_id,
                                                     const std::string& instrument_id);
    virtual bool EnqueueBrokerTradingParamsQuery(int request_id);
    virtual bool EnqueueOrderQuery(int request_id);
    virtual bool EnqueueTradeQuery(int request_id);

    virtual bool RequestUserLogin(int request_id, const std::string& broker_id,
                                  const std::string& user_id, const std::string& password);
    virtual bool RequestSettlementInfoConfirm(int request_id);

    virtual void RegisterConnectionStateCallback(ConnectionStateCallback callback);
    virtual ConnectionListenerToken AddConnectionStateListener(ConnectionStateCallback callback);
    virtual void RemoveConnectionStateListener(ConnectionListenerToken token);
    virtual void RegisterLoginResponseCallback(LoginResponseCallback callback);
    virtual void RegisterQueryCompleteCallback(QueryCompleteCallback callback);
    virtual void RegisterSettlementConfirmCallback(SettlementConfirmCallback callback);

    void RegisterTradingAccountSnapshotCallback(TradingAccountSnapshotCallback callback);
    void RegisterInvestorPositionSnapshotCallback(InvestorPositionSnapshotCallback callback);
    void RegisterInvestorPositionQueryCallback(InvestorPositionQueryCallback callback);
    void RegisterTradingAccountQueryStartCallback(TradingAccountQueryStartCallback callback);
    void RegisterTradingAccountQueryCallback(TradingAccountQueryCallback callback);
    void RegisterInstrumentMetaQueryCallback(InstrumentMetaQueryCallback callback);
    void RegisterInstrumentCommissionRateQueryCallback(
        InstrumentCommissionRateQueryCallback callback);
    void RegisterInstrumentOrderCommRateQueryCallback(
        InstrumentOrderCommRateQueryCallback callback);
    void RegisterInstrumentMetaSnapshotCallback(InstrumentMetaSnapshotCallback callback);
    void RegisterDepthMarketSnapshotCallback(DepthMarketSnapshotCallback callback);
    void RegisterBrokerTradingParamsSnapshotCallback(BrokerTradingParamsSnapshotCallback callback);
    void RegisterInstrumentMarginRateSnapshotCallback(
        InstrumentMarginRateSnapshotCallback callback);
    void RegisterInstrumentCommissionRateSnapshotCallback(
        InstrumentCommissionRateSnapshotCallback callback);
    void RegisterInstrumentOrderCommRateSnapshotCallback(
        InstrumentOrderCommRateSnapshotCallback callback);

    CtpUserSessionInfo GetLastUserSession() const;
    TradingAccountSnapshot GetLastTradingAccountSnapshot() const;
    std::vector<InvestorPositionSnapshot> GetLastInvestorPositionSnapshots() const;
    std::vector<InstrumentMetaSnapshot> GetLastInstrumentMetaSnapshots() const;
    std::vector<MarketSnapshot> GetLastDepthMarketSnapshots() const;
    BrokerTradingParamsSnapshot GetLastBrokerTradingParamsSnapshot() const;
    std::vector<InstrumentMarginRateSnapshot> GetLastInstrumentMarginRateSnapshots() const;
    std::vector<InstrumentCommissionRateSnapshot> GetLastInstrumentCommissionRateSnapshots() const;
    std::vector<InstrumentOrderCommRateSnapshot> GetLastInstrumentOrderCommRateSnapshots() const;
    void UpdateOffsetApplySrc(char apply_src);
    char GetOffsetApplySrc() const;
    std::string GetLastConnectDiagnostic() const;
    std::uint64_t GetSessionGeneration() const;
    std::uint64_t GetDuplicateTradesSuppressed() const;
    static void NormalizeMarketSnapshot(MarketSnapshot* snapshot);
    static EpochNanos ParseMarketExchangeTimestamp(const std::string& action_day,
                                                   const std::string& update_time,
                                                   std::int32_t update_millisec);
    void UpdateInstrumentMetadata(const std::vector<InstrumentMetaSnapshot>& snapshots);

   private:
    friend class CtpMdSpi;
    friend class CtpTdSpi;
    friend class CtpCallbackScope;
    friend class CtpGatewayAdapterTestPeer;

    struct OrderMeta {
        std::string order_ref;
        std::string strategy_id;
        std::string component_id;
        std::string instrument_id;
        Side side{Side::kBuy};
        OffsetFlag offset{OffsetFlag::kOpen};
        int front_id{0};
        int session_id{0};
        std::int32_t total_volume{0};
        std::int32_t cumulative_filled_volume{0};
        std::string trading_day;
        bool terminal{false};
    };

    struct RealApiState;

    bool ConnectSimulated();
    bool ConnectRealApi();
    bool ConnectRealApiWithFrontPair(const CtpRuntimeConfig& runtime, bool was_connected,
                                     const CtpFrontPair& front_pair, std::string* failure_detail);
    void StartReconnectWorker();
    void StopReconnectWorker();
    void RequestReconnect();
    void HandleConnectionLoss();
    void ReconnectWorkerLoop();
    void TryMarkHealthyFromState();
    bool ReplayMarketDataSubscriptions();
    void DisconnectRealApi();
    void PublishInvestorPositionQueryResponse(int request_id, std::uint64_t generation,
                                              const InvestorPositionSnapshot* row, int error_code,
                                              const std::string& error, bool last);
    void FailQuery(int request_id, std::uint64_t generation, const std::string& name,
                   const std::string& error);
    template <typename Row, typename Cache, typename Callback>
    void PublishSnapshotQueryResponse(QueryBatchCollector<Row>& collector, int request_id,
                                      std::uint64_t generation, const Row* row, int error_code,
                                      const std::string& error, bool last, Cache& cache,
                                      Callback& callback_slot) {
        auto result = collector.Accept(request_id, generation, row, error_code, error, last);
        if (!result) return;
        Callback callback;
        std::function<void(const QueryResult<Row>&)> query_callback;
        QueryCompleteCallback complete;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (generation != query_generation_) return;
            if constexpr (std::is_same_v<Cache, std::vector<Row>>) {
                if (result->metadata.success) {
                    if (result->metadata.instrument_id.empty()) {
                        cache = result->rows;
                    } else {
                        const auto& instrument = result->metadata.instrument_id;
                        cache.erase(std::remove_if(cache.begin(), cache.end(),
                                                   [&](const Row& value) {
                                                       return value.instrument_id == instrument;
                                                   }),
                                    cache.end());
                        cache.insert(cache.end(), result->rows.begin(), result->rows.end());
                    }
                }
            } else {
                // Scalar account/parameter APIs cannot represent an empty or multi-currency batch.
                result->metadata.success = result->metadata.success && result->rows.size() == 1;
                if (result->metadata.success) cache = result->rows.front();
            }
            if (result->metadata.success) callback = callback_slot;
            if constexpr (std::is_same_v<Row, TradingAccountSnapshot>)
                query_callback = trading_account_query_callback_;
            if constexpr (std::is_same_v<Row, InstrumentMetaSnapshot>)
                query_callback = instrument_meta_query_callback_;
            if constexpr (std::is_same_v<Row, InstrumentCommissionRateSnapshot>)
                query_callback = instrument_commission_rate_query_callback_;
            if constexpr (std::is_same_v<Row, InstrumentOrderCommRateSnapshot>)
                query_callback = instrument_order_comm_rate_query_callback_;
            complete = query_complete_callback_;
        }
        if (query_callback) query_callback(*result);
        if (callback) {
            if constexpr (std::is_same_v<Cache, std::vector<Row>>)
                callback(result->rows);
            else
                callback(result->rows.front());
        }
        if (complete) complete(request_id, result->metadata.query_name, result->metadata.success);
    }
    bool ExecuteTdQueryWithRetry(const std::function<int()>& request_fn) const;
    int NextRequestIdLocked();
    std::string NextOrderRefLocked();

    bool connected_{false};
    bool healthy_{false};
    bool desired_connected_{false};
    CtpRuntimeConfig runtime_config_;

    mutable std::mutex mutex_;
    std::unordered_set<std::string> subscriptions_;
    std::unordered_map<std::string, OrderMeta> client_order_meta_;
    std::unordered_map<std::string, std::string> order_ref_to_client_id_;
    std::unordered_set<std::string> seen_trade_keys_;
    std::uint64_t duplicate_trades_suppressed_{0};
    std::uint64_t session_generation_{0};
    MarketDataCallback market_data_callback_;
    OrderEventCallback order_event_callback_;
    OrderSubmitMappingCallback order_submit_mapping_callback_;
    OrderSubmitPrepareCallback order_submit_prepare_callback_;

    QueryScheduler query_scheduler_;
    std::uint64_t query_generation_{0};
    QueryBatchCollector<InvestorPositionSnapshot> investor_position_queries_;
    QueryBatchCollector<TradingAccountSnapshot> trading_account_queries_;
    QueryBatchCollector<InstrumentMetaSnapshot> instrument_meta_queries_;
    QueryBatchCollector<MarketSnapshot> depth_market_queries_;
    QueryBatchCollector<BrokerTradingParamsSnapshot> broker_trading_params_queries_;
    QueryBatchCollector<InstrumentMarginRateSnapshot> instrument_margin_rate_queries_;
    QueryBatchCollector<InstrumentCommissionRateSnapshot> instrument_commission_rate_queries_;
    QueryBatchCollector<InstrumentOrderCommRateSnapshot> instrument_order_comm_rate_queries_;
    CtpUserSessionInfo user_session_;
    TradingAccountSnapshot trading_account_snapshot_;
    std::vector<InvestorPositionSnapshot> investor_position_snapshots_;
    std::vector<InstrumentMetaSnapshot> instrument_meta_snapshots_;
    std::vector<MarketSnapshot> depth_market_snapshots_;
    BrokerTradingParamsSnapshot broker_trading_params_snapshot_;
    std::vector<InstrumentMarginRateSnapshot> instrument_margin_rate_snapshots_;
    std::vector<InstrumentCommissionRateSnapshot> instrument_commission_rate_snapshots_;
    std::vector<InstrumentOrderCommRateSnapshot> instrument_order_comm_rate_snapshots_;

    TradingAccountSnapshotCallback trading_account_snapshot_callback_;
    TradingAccountQueryStartCallback trading_account_query_start_callback_;
    TradingAccountQueryCallback trading_account_query_callback_;
    InvestorPositionSnapshotCallback investor_position_snapshot_callback_;
    InvestorPositionQueryCallback investor_position_query_callback_;
    InstrumentMetaQueryCallback instrument_meta_query_callback_;
    InstrumentCommissionRateQueryCallback instrument_commission_rate_query_callback_;
    InstrumentOrderCommRateQueryCallback instrument_order_comm_rate_query_callback_;
    InstrumentMetaSnapshotCallback instrument_meta_snapshot_callback_;
    DepthMarketSnapshotCallback depth_market_snapshot_callback_;
    BrokerTradingParamsSnapshotCallback broker_trading_params_snapshot_callback_;
    InstrumentMarginRateSnapshotCallback instrument_margin_rate_snapshot_callback_;
    InstrumentCommissionRateSnapshotCallback instrument_commission_rate_snapshot_callback_;
    InstrumentOrderCommRateSnapshotCallback instrument_order_comm_rate_snapshot_callback_;
    ConnectionStateCallback connection_state_callback_;
    std::unordered_map<ConnectionListenerToken, ConnectionStateCallback>
        connection_state_listeners_;
    ConnectionListenerToken next_connection_listener_token_{1};
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
