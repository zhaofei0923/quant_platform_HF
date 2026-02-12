#include "quant_hft/core/ctp_trader_adapter.h"

#include <utility>

namespace quant_hft {

namespace {

constexpr std::int64_t kNanosPerMilli = 1'000'000;

}  // namespace

CTPTraderAdapter::CTPTraderAdapter(std::size_t query_qps_limit,
                                   std::size_t dispatcher_workers)
    : gateway_(query_qps_limit),
      dispatcher_(dispatcher_workers) {
    gateway_.RegisterOrderEventCallback([this](const OrderEvent& event) {
        OrderEvent copied = event;
        dispatcher_.Post(
            [this, copied]() {
                OrderEventCallback callback;
                {
                    std::lock_guard<std::mutex> lock(mutex_);
                    callback = user_order_event_callback_;
                }
                if (callback) {
                    callback(copied);
                }
            },
            EventPriority::kHigh);
    });
    gateway_.RegisterTradingAccountSnapshotCallback([this](const TradingAccountSnapshot& snapshot) {
        TradingAccountSnapshot copied = snapshot;
        dispatcher_.Post(
            [this, copied]() {
                TradingAccountSnapshotCallback callback;
                {
                    std::lock_guard<std::mutex> lock(mutex_);
                    callback = user_trading_account_callback_;
                }
                if (callback) {
                    callback(copied);
                }
            },
            EventPriority::kNormal);
    });
    gateway_.RegisterInvestorPositionSnapshotCallback(
        [this](const std::vector<InvestorPositionSnapshot>& snapshots) {
            auto copied = snapshots;
            dispatcher_.Post(
                [this, copied = std::move(copied)]() {
                    InvestorPositionSnapshotCallback callback;
                    {
                        std::lock_guard<std::mutex> lock(mutex_);
                        callback = user_investor_position_callback_;
                    }
                    if (callback) {
                        callback(copied);
                    }
                },
                EventPriority::kNormal);
        });
    gateway_.RegisterInstrumentMetaSnapshotCallback(
        [this](const std::vector<InstrumentMetaSnapshot>& snapshots) {
            auto copied = snapshots;
            dispatcher_.Post(
                [this, copied = std::move(copied)]() {
                    InstrumentMetaSnapshotCallback callback;
                    {
                        std::lock_guard<std::mutex> lock(mutex_);
                        callback = user_instrument_meta_callback_;
                    }
                    if (callback) {
                        callback(copied);
                    }
                },
                EventPriority::kNormal);
        });
    gateway_.RegisterBrokerTradingParamsSnapshotCallback(
        [this](const BrokerTradingParamsSnapshot& snapshot) {
            BrokerTradingParamsSnapshot copied = snapshot;
            dispatcher_.Post(
                [this, copied]() {
                    BrokerTradingParamsSnapshotCallback callback;
                    {
                        std::lock_guard<std::mutex> lock(mutex_);
                        callback = user_broker_trading_params_callback_;
                    }
                    if (callback) {
                        callback(copied);
                    }
                },
                EventPriority::kNormal);
        });
}

CTPTraderAdapter::~CTPTraderAdapter() {
    Disconnect();
}

bool CTPTraderAdapter::Connect(const MarketDataConnectConfig& config) {
    Disconnect();
    {
        std::lock_guard<std::mutex> lock(mutex_);
        settlement_confirm_required_ = config.settlement_confirm_required;
        settlement_confirmed_ = false;
        state_ = TraderSessionState::kDisconnected;
    }
    dispatcher_.Start();
    if (!gateway_.Connect(config)) {
        std::lock_guard<std::mutex> lock(mutex_);
        state_ = TraderSessionState::kDisconnected;
        dispatcher_.Stop();
        return false;
    }
    {
        std::lock_guard<std::mutex> lock(mutex_);
        state_ = TraderSessionState::kConnected;
        state_ = TraderSessionState::kAuthenticated;
        state_ = TraderSessionState::kLoggedIn;
        if (!settlement_confirm_required_) {
            settlement_confirmed_ = true;
            state_ = TraderSessionState::kReady;
        }
    }
    return true;
}

void CTPTraderAdapter::Disconnect() {
    gateway_.Disconnect();
    dispatcher_.Stop();
    std::lock_guard<std::mutex> lock(mutex_);
    settlement_confirmed_ = false;
    state_ = TraderSessionState::kDisconnected;
}

bool CTPTraderAdapter::IsReady() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return state_ == TraderSessionState::kReady;
}

TraderSessionState CTPTraderAdapter::SessionState() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return state_;
}

bool CTPTraderAdapter::ConfirmSettlement() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (state_ < TraderSessionState::kLoggedIn) {
        return false;
    }
    settlement_confirmed_ = true;
    state_ = TraderSessionState::kSettlementConfirmed;
    state_ = TraderSessionState::kReady;
    return true;
}

bool CTPTraderAdapter::PlaceOrder(const OrderIntent& intent) {
    OrderIntent request = intent;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (state_ != TraderSessionState::kReady || !settlement_confirmed_) {
            return false;
        }
        if (request.strategy_id.empty()) {
            return false;
        }
        if (request.client_order_id.empty()) {
            const auto unix_ms = NowEpochNanos() / kNanosPerMilli;
            ++order_ref_seq_;
            request.client_order_id = request.strategy_id + "_" + std::to_string(unix_ms) + "_" +
                                      std::to_string(order_ref_seq_);
        }
    }
    return gateway_.PlaceOrder(request);
}

bool CTPTraderAdapter::CancelOrder(const std::string& client_order_id,
                                   const std::string& trace_id) {
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (state_ != TraderSessionState::kReady || !settlement_confirmed_) {
            return false;
        }
    }
    return gateway_.CancelOrder(client_order_id, trace_id);
}

bool CTPTraderAdapter::EnqueueUserSessionQuery(int request_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (state_ < TraderSessionState::kLoggedIn) {
        return false;
    }
    return gateway_.EnqueueUserSessionQuery(request_id);
}

bool CTPTraderAdapter::EnqueueTradingAccountQuery(int request_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (state_ < TraderSessionState::kLoggedIn) {
        return false;
    }
    return gateway_.EnqueueTradingAccountQuery(request_id);
}

bool CTPTraderAdapter::EnqueueInvestorPositionQuery(int request_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (state_ < TraderSessionState::kLoggedIn) {
        return false;
    }
    return gateway_.EnqueueInvestorPositionQuery(request_id);
}

bool CTPTraderAdapter::EnqueueInstrumentQuery(int request_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (state_ < TraderSessionState::kLoggedIn) {
        return false;
    }
    return gateway_.EnqueueInstrumentQuery(request_id);
}

bool CTPTraderAdapter::EnqueueInstrumentMarginRateQuery(int request_id,
                                                        const std::string& instrument_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (state_ < TraderSessionState::kLoggedIn) {
        return false;
    }
    return gateway_.EnqueueInstrumentMarginRateQuery(request_id, instrument_id);
}

bool CTPTraderAdapter::EnqueueInstrumentCommissionRateQuery(int request_id,
                                                            const std::string& instrument_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (state_ < TraderSessionState::kLoggedIn) {
        return false;
    }
    return gateway_.EnqueueInstrumentCommissionRateQuery(request_id, instrument_id);
}

bool CTPTraderAdapter::EnqueueBrokerTradingParamsQuery(int request_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (state_ < TraderSessionState::kLoggedIn) {
        return false;
    }
    return gateway_.EnqueueBrokerTradingParamsQuery(request_id);
}

bool CTPTraderAdapter::EnqueueOrderQuery(int request_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (state_ < TraderSessionState::kLoggedIn) {
        return false;
    }
    return gateway_.EnqueueOrderQuery(request_id);
}

bool CTPTraderAdapter::EnqueueTradeQuery(int request_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (state_ < TraderSessionState::kLoggedIn) {
        return false;
    }
    return gateway_.EnqueueTradeQuery(request_id);
}

void CTPTraderAdapter::RegisterOrderEventCallback(OrderEventCallback callback) {
    std::lock_guard<std::mutex> lock(mutex_);
    user_order_event_callback_ = std::move(callback);
}

void CTPTraderAdapter::RegisterTradingAccountSnapshotCallback(
    TradingAccountSnapshotCallback callback) {
    std::lock_guard<std::mutex> lock(mutex_);
    user_trading_account_callback_ = std::move(callback);
}

void CTPTraderAdapter::RegisterInvestorPositionSnapshotCallback(
    InvestorPositionSnapshotCallback callback) {
    std::lock_guard<std::mutex> lock(mutex_);
    user_investor_position_callback_ = std::move(callback);
}

void CTPTraderAdapter::RegisterInstrumentMetaSnapshotCallback(
    InstrumentMetaSnapshotCallback callback) {
    std::lock_guard<std::mutex> lock(mutex_);
    user_instrument_meta_callback_ = std::move(callback);
}

void CTPTraderAdapter::RegisterBrokerTradingParamsSnapshotCallback(
    BrokerTradingParamsSnapshotCallback callback) {
    std::lock_guard<std::mutex> lock(mutex_);
    user_broker_trading_params_callback_ = std::move(callback);
}

CtpUserSessionInfo CTPTraderAdapter::GetLastUserSession() const {
    return gateway_.GetLastUserSession();
}

std::string CTPTraderAdapter::GetLastConnectDiagnostic() const {
    return gateway_.GetLastConnectDiagnostic();
}

std::string CTPTraderAdapter::BuildOrderRef(const std::string& strategy_id) const {
    std::lock_guard<std::mutex> lock(mutex_);
    const auto unix_ms = NowEpochNanos() / kNanosPerMilli;
    ++order_ref_seq_;
    return strategy_id + "_" + std::to_string(unix_ms) + "_" + std::to_string(order_ref_seq_);
}

}  // namespace quant_hft
