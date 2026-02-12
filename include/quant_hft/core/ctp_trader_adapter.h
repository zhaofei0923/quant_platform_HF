#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "quant_hft/contracts/types.h"
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
    using TradingAccountSnapshotCallback =
        CtpGatewayAdapter::TradingAccountSnapshotCallback;
    using InvestorPositionSnapshotCallback =
        CtpGatewayAdapter::InvestorPositionSnapshotCallback;
    using InstrumentMetaSnapshotCallback =
        CtpGatewayAdapter::InstrumentMetaSnapshotCallback;
    using BrokerTradingParamsSnapshotCallback =
        CtpGatewayAdapter::BrokerTradingParamsSnapshotCallback;

    explicit CTPTraderAdapter(std::size_t query_qps_limit = 10,
                              std::size_t dispatcher_workers = 1);
    ~CTPTraderAdapter();

    CTPTraderAdapter(const CTPTraderAdapter&) = delete;
    CTPTraderAdapter& operator=(const CTPTraderAdapter&) = delete;

    bool Connect(const MarketDataConnectConfig& config);
    void Disconnect();
    bool IsReady() const;
    TraderSessionState SessionState() const;
    bool ConfirmSettlement();
    bool PlaceOrder(const OrderIntent& intent);
    bool CancelOrder(const std::string& client_order_id, const std::string& trace_id);

    bool EnqueueUserSessionQuery(int request_id);
    bool EnqueueTradingAccountQuery(int request_id);
    bool EnqueueInvestorPositionQuery(int request_id);
    bool EnqueueInstrumentQuery(int request_id);
    bool EnqueueInstrumentMarginRateQuery(int request_id, const std::string& instrument_id);
    bool EnqueueInstrumentCommissionRateQuery(int request_id, const std::string& instrument_id);
    bool EnqueueBrokerTradingParamsQuery(int request_id);
    bool EnqueueOrderQuery(int request_id);
    bool EnqueueTradeQuery(int request_id);

    void RegisterOrderEventCallback(OrderEventCallback callback);
    void RegisterTradingAccountSnapshotCallback(TradingAccountSnapshotCallback callback);
    void RegisterInvestorPositionSnapshotCallback(InvestorPositionSnapshotCallback callback);
    void RegisterInstrumentMetaSnapshotCallback(InstrumentMetaSnapshotCallback callback);
    void RegisterBrokerTradingParamsSnapshotCallback(BrokerTradingParamsSnapshotCallback callback);

    CtpUserSessionInfo GetLastUserSession() const;
    std::string GetLastConnectDiagnostic() const;
    std::string BuildOrderRef(const std::string& strategy_id) const;

private:
    mutable std::mutex mutex_;
    CtpGatewayAdapter gateway_;
    EventDispatcher dispatcher_;
    OrderEventCallback user_order_event_callback_;
    TradingAccountSnapshotCallback user_trading_account_callback_;
    InvestorPositionSnapshotCallback user_investor_position_callback_;
    InstrumentMetaSnapshotCallback user_instrument_meta_callback_;
    BrokerTradingParamsSnapshotCallback user_broker_trading_params_callback_;
    TraderSessionState state_{TraderSessionState::kDisconnected};
    bool settlement_confirm_required_{true};
    bool settlement_confirmed_{false};
    mutable std::uint64_t order_ref_seq_{0};
};

}  // namespace quant_hft
