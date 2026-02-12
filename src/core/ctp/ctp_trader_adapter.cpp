#include "quant_hft/core/ctp_trader_adapter.h"

#include <algorithm>
#include <chrono>
#include <stdexcept>
#include <thread>
#include <utility>

#include "quant_hft/monitoring/metric_registry.h"

namespace quant_hft {

namespace {

constexpr std::int64_t kNanosPerMilli = 1'000'000;

std::string BuildOrderRefString(const std::string& strategy_id, std::uint64_t seq) {
    const auto unix_ms = NowEpochNanos() / kNanosPerMilli;
    return strategy_id + "_" + std::to_string(unix_ms) + "_" + std::to_string(seq);
}

std::shared_ptr<MonitoringGauge> CtpConnectedGauge() {
    static auto metric = MetricRegistry::Instance().BuildGauge(
        "quant_hft_ctp_connected", "CTP connected state gauge");
    return metric;
}

}  // namespace

CTPTraderAdapter::CTPTraderAdapter(std::size_t query_qps_limit, std::size_t dispatcher_workers)
    : CTPTraderAdapter(std::make_shared<CtpGatewayAdapter>(query_qps_limit), dispatcher_workers) {}

CTPTraderAdapter::CTPTraderAdapter(std::shared_ptr<CtpGatewayAdapter> gateway,
                                   std::size_t dispatcher_workers)
    : gateway_(std::move(gateway)),
      dispatcher_(dispatcher_workers) {
    if (gateway_ == nullptr) {
        gateway_ = std::make_shared<CtpGatewayAdapter>(10);
    }

    gateway_->RegisterOrderEventCallback([this](const OrderEvent& event) {
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

    gateway_->RegisterTradingAccountSnapshotCallback([this](const TradingAccountSnapshot& snapshot) {
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

    gateway_->RegisterInvestorPositionSnapshotCallback(
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

    gateway_->RegisterInstrumentMetaSnapshotCallback(
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

    gateway_->RegisterBrokerTradingParamsSnapshotCallback(
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

    gateway_->RegisterConnectionStateCallback([this](bool healthy) {
        CtpConnectedGauge()->Set(healthy ? 1.0 : 0.0);
        if (!healthy) {
            bool should_reconnect = false;
            {
                std::lock_guard<std::mutex> lock(mutex_);
                if (state_ != TraderSessionState::kDisconnected && has_connect_config_) {
                    should_reconnect = true;
                }
                state_ = TraderSessionState::kDisconnected;
                settlement_confirmed_ = false;
            }
            if (!should_reconnect) {
                return;
            }
            need_reconnect_.store(true, std::memory_order_relaxed);
            reconnect_attempts_.store(0, std::memory_order_relaxed);
            ScheduleReconnect();
            return;
        }
        if (need_reconnect_.load(std::memory_order_relaxed)) {
            ScheduleReconnect();
        }
    });

    gateway_->RegisterLoginResponseCallback([this](int request_id,
                                                   int error_code,
                                                   const std::string& error_msg) {
        if (error_code == 0) {
            std::lock_guard<std::mutex> lock(mutex_);
            state_ = TraderSessionState::kLoggedIn;
            if (!settlement_confirm_required_) {
                settlement_confirmed_ = true;
                state_ = TraderSessionState::kReady;
            }
        }
        ResolveLoginPromise(request_id, error_code, error_msg);
    });

    gateway_->RegisterQueryCompleteCallback(
        [this](int request_id, const std::string&, bool success) {
            if (success) {
                ResolvePromise(request_id);
            } else {
                RejectPromise(request_id, "query failed");
            }
        });

    gateway_->RegisterSettlementConfirmCallback([this](int request_id,
                                                       int error_code,
                                                       const std::string& error_msg) {
        if (error_code == 0) {
            std::lock_guard<std::mutex> lock(mutex_);
            settlement_confirmed_ = true;
            state_ = TraderSessionState::kSettlementConfirmed;
        } else {
            std::lock_guard<std::mutex> lock(mutex_);
            settlement_confirmed_ = false;
        }
        if (error_code == 0) {
            ResolveSettlementPromise(request_id);
        } else {
            RejectSettlementPromise(request_id, error_msg.empty() ? "confirm settlement failed"
                                                                  : error_msg);
        }
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
        last_connect_config_ = config;
        has_connect_config_ = true;
    }
    next_request_id_.store(1, std::memory_order_relaxed);
    need_reconnect_.store(false, std::memory_order_relaxed);
    reconnect_attempts_.store(0, std::memory_order_relaxed);
    {
        std::lock_guard<std::mutex> lock(promise_map_mutex_);
        query_promises_.clear();
        settlement_promises_.clear();
        login_promises_.clear();
    }

    dispatcher_.Start();
    if (!gateway_->Connect(config)) {
        std::lock_guard<std::mutex> lock(mutex_);
        state_ = TraderSessionState::kDisconnected;
        dispatcher_.Stop();
        CtpConnectedGauge()->Set(0.0);
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
    CtpConnectedGauge()->Set(1.0);
    return true;
}

void CTPTraderAdapter::Disconnect() {
    need_reconnect_.store(false, std::memory_order_relaxed);
    {
        std::lock_guard<std::mutex> lock(mutex_);
        has_connect_config_ = false;
        settlement_confirmed_ = false;
        state_ = TraderSessionState::kDisconnected;
    }
    gateway_->Disconnect();
    CtpConnectedGauge()->Set(0.0);
    dispatcher_.Stop();
    {
        std::lock_guard<std::mutex> lock(promise_map_mutex_);
        query_promises_.clear();
        settlement_promises_.clear();
        login_promises_.clear();
    }
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
    bool needs_confirm = true;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (state_ < TraderSessionState::kLoggedIn) {
            return false;
        }
        needs_confirm = settlement_confirm_required_;
        if (!needs_confirm) {
            settlement_confirmed_ = true;
            state_ = TraderSessionState::kReady;
            return true;
        }
    }

    const int request_id = AllocateRequestId();
    auto promise = std::make_shared<std::promise<void>>();
    {
        std::lock_guard<std::mutex> lock(promise_map_mutex_);
        settlement_promises_[request_id] = promise;
    }
    if (!gateway_->RequestSettlementInfoConfirm(request_id)) {
        RejectSettlementPromise(request_id, "ReqSettlementInfoConfirm failed to submit");
        return false;
    }

    auto future = promise->get_future();
    if (future.wait_for(std::chrono::seconds(3)) != std::future_status::ready) {
        RejectSettlementPromise(request_id, "confirm settlement timeout");
        return false;
    }
    try {
        future.get();
    } catch (...) {
        return false;
    }

    std::lock_guard<std::mutex> lock(mutex_);
    state_ = TraderSessionState::kReady;
    return settlement_confirmed_;
}

bool CTPTraderAdapter::PlaceOrder(const OrderIntent& intent) {
    return !PlaceOrderWithRef(intent).empty();
}

std::string CTPTraderAdapter::PlaceOrderWithRef(const OrderIntent& intent) {
    OrderIntent request = intent;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (state_ != TraderSessionState::kReady || !settlement_confirmed_) {
            return "";
        }
        if (request.strategy_id.empty()) {
            return "";
        }
        if (request.client_order_id.empty()) {
            ++order_ref_seq_;
            request.client_order_id = BuildOrderRefString(request.strategy_id, order_ref_seq_);
        }
    }
    if (!gateway_->PlaceOrder(request)) {
        return "";
    }
    return request.client_order_id;
}

bool CTPTraderAdapter::CancelOrder(const std::string& client_order_id,
                                   const std::string& trace_id) {
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (state_ != TraderSessionState::kReady || !settlement_confirmed_) {
            return false;
        }
    }
    return gateway_->CancelOrder(client_order_id, trace_id);
}

std::future<std::pair<int, std::string>> CTPTraderAdapter::LoginAsync(const std::string& broker_id,
                                                                       const std::string& user_id,
                                                                       const std::string& password,
                                                                       int timeout_ms) {
    auto promise = std::make_shared<std::promise<std::pair<int, std::string>>>();
    auto future = promise->get_future();

    const int request_id = AllocateRequestId();
    {
        std::lock_guard<std::mutex> lock(promise_map_mutex_);
        login_promises_[request_id] = promise;
    }

    if (!gateway_->RequestUserLogin(request_id, broker_id, user_id, password)) {
        ResolveLoginPromise(request_id, -2, "ReqUserLogin failed");
        return future;
    }

    std::thread([this, request_id, timeout_ms]() {
        std::this_thread::sleep_for(std::chrono::milliseconds(std::max(1, timeout_ms)));
        std::shared_ptr<std::promise<std::pair<int, std::string>>> timed_out;
        {
            std::lock_guard<std::mutex> lock(promise_map_mutex_);
            const auto it = login_promises_.find(request_id);
            if (it == login_promises_.end()) {
                return;
            }
            timed_out = it->second;
            login_promises_.erase(it);
        }
        if (timed_out) {
            try {
                timed_out->set_value({-1, "Login timeout"});
            } catch (...) {
            }
        }
    }).detach();

    return future;
}

bool CTPTraderAdapter::RecoverOrdersAndTrades(int timeout_ms) {
    const auto timeout = std::chrono::milliseconds(std::max(1, timeout_ms));

    const int order_request_id = AllocateRequestId();
    auto order_promise = std::make_shared<std::promise<void>>();
    StorePromise(order_request_id, order_promise);
    if (!EnqueueOrderQuery(order_request_id)) {
        RejectPromise(order_request_id, "ReqQryOrder failed to submit");
        return false;
    }
    auto order_future = order_promise->get_future();
    if (order_future.wait_for(timeout) != std::future_status::ready) {
        RejectPromise(order_request_id, "ReqQryOrder timeout");
        return false;
    }
    try {
        order_future.get();
    } catch (...) {
        return false;
    }

    const int trade_request_id = AllocateRequestId();
    auto trade_promise = std::make_shared<std::promise<void>>();
    StorePromise(trade_request_id, trade_promise);
    if (!EnqueueTradeQuery(trade_request_id)) {
        RejectPromise(trade_request_id, "ReqQryTrade failed to submit");
        return false;
    }
    auto trade_future = trade_promise->get_future();
    if (trade_future.wait_for(timeout) != std::future_status::ready) {
        RejectPromise(trade_request_id, "ReqQryTrade timeout");
        return false;
    }
    try {
        trade_future.get();
    } catch (...) {
        return false;
    }

    return true;
}

bool CTPTraderAdapter::EnqueueUserSessionQuery(int request_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (state_ < TraderSessionState::kLoggedIn) {
        return false;
    }
    return gateway_->EnqueueUserSessionQuery(request_id);
}

int CTPTraderAdapter::EnqueueUserSessionQuery() {
    const int request_id = AllocateRequestId();
    return EnqueueUserSessionQuery(request_id) ? request_id : -1;
}

bool CTPTraderAdapter::EnqueueTradingAccountQuery(int request_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (state_ < TraderSessionState::kLoggedIn) {
        return false;
    }
    return gateway_->EnqueueTradingAccountQuery(request_id);
}

int CTPTraderAdapter::EnqueueTradingAccountQuery() {
    const int request_id = AllocateRequestId();
    return EnqueueTradingAccountQuery(request_id) ? request_id : -1;
}

bool CTPTraderAdapter::EnqueueInvestorPositionQuery(int request_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (state_ < TraderSessionState::kLoggedIn) {
        return false;
    }
    return gateway_->EnqueueInvestorPositionQuery(request_id);
}

int CTPTraderAdapter::EnqueueInvestorPositionQuery() {
    const int request_id = AllocateRequestId();
    return EnqueueInvestorPositionQuery(request_id) ? request_id : -1;
}

bool CTPTraderAdapter::EnqueueInstrumentQuery(int request_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (state_ < TraderSessionState::kLoggedIn) {
        return false;
    }
    return gateway_->EnqueueInstrumentQuery(request_id);
}

int CTPTraderAdapter::EnqueueInstrumentQuery() {
    const int request_id = AllocateRequestId();
    return EnqueueInstrumentQuery(request_id) ? request_id : -1;
}

bool CTPTraderAdapter::EnqueueInstrumentMarginRateQuery(int request_id,
                                                        const std::string& instrument_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (state_ < TraderSessionState::kLoggedIn) {
        return false;
    }
    return gateway_->EnqueueInstrumentMarginRateQuery(request_id, instrument_id);
}

int CTPTraderAdapter::EnqueueInstrumentMarginRateQuery(const std::string& instrument_id) {
    const int request_id = AllocateRequestId();
    return EnqueueInstrumentMarginRateQuery(request_id, instrument_id) ? request_id : -1;
}

bool CTPTraderAdapter::EnqueueInstrumentCommissionRateQuery(int request_id,
                                                            const std::string& instrument_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (state_ < TraderSessionState::kLoggedIn) {
        return false;
    }
    return gateway_->EnqueueInstrumentCommissionRateQuery(request_id, instrument_id);
}

int CTPTraderAdapter::EnqueueInstrumentCommissionRateQuery(const std::string& instrument_id) {
    const int request_id = AllocateRequestId();
    return EnqueueInstrumentCommissionRateQuery(request_id, instrument_id) ? request_id : -1;
}

bool CTPTraderAdapter::EnqueueBrokerTradingParamsQuery(int request_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (state_ < TraderSessionState::kLoggedIn) {
        return false;
    }
    return gateway_->EnqueueBrokerTradingParamsQuery(request_id);
}

int CTPTraderAdapter::EnqueueBrokerTradingParamsQuery() {
    const int request_id = AllocateRequestId();
    return EnqueueBrokerTradingParamsQuery(request_id) ? request_id : -1;
}

bool CTPTraderAdapter::EnqueueOrderQuery(int request_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (state_ < TraderSessionState::kLoggedIn) {
        return false;
    }
    return gateway_->EnqueueOrderQuery(request_id);
}

int CTPTraderAdapter::EnqueueOrderQuery() {
    const int request_id = AllocateRequestId();
    return EnqueueOrderQuery(request_id) ? request_id : -1;
}

bool CTPTraderAdapter::EnqueueTradeQuery(int request_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (state_ < TraderSessionState::kLoggedIn) {
        return false;
    }
    return gateway_->EnqueueTradeQuery(request_id);
}

int CTPTraderAdapter::EnqueueTradeQuery() {
    const int request_id = AllocateRequestId();
    return EnqueueTradeQuery(request_id) ? request_id : -1;
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
    return gateway_->GetLastUserSession();
}

TradingAccountSnapshot CTPTraderAdapter::GetLastTradingAccountSnapshot() const {
    return gateway_->GetLastTradingAccountSnapshot();
}

std::vector<InvestorPositionSnapshot> CTPTraderAdapter::GetLastInvestorPositionSnapshots() const {
    return gateway_->GetLastInvestorPositionSnapshots();
}

std::string CTPTraderAdapter::GetLastConnectDiagnostic() const {
    return gateway_->GetLastConnectDiagnostic();
}

std::string CTPTraderAdapter::BuildOrderRef(const std::string& strategy_id) const {
    std::lock_guard<std::mutex> lock(mutex_);
    ++order_ref_seq_;
    return BuildOrderRefString(strategy_id, order_ref_seq_);
}

int CTPTraderAdapter::AllocateRequestId() {
    return next_request_id_.fetch_add(1, std::memory_order_relaxed);
}

void CTPTraderAdapter::StorePromise(int request_id,
                                    const std::shared_ptr<std::promise<void>>& promise) {
    std::lock_guard<std::mutex> lock(promise_map_mutex_);
    query_promises_[request_id] = promise;
}

void CTPTraderAdapter::ResolvePromise(int request_id) {
    std::shared_ptr<std::promise<void>> promise;
    {
        std::lock_guard<std::mutex> lock(promise_map_mutex_);
        const auto it = query_promises_.find(request_id);
        if (it == query_promises_.end()) {
            return;
        }
        promise = it->second;
        query_promises_.erase(it);
    }
    try {
        promise->set_value();
    } catch (...) {
    }
}

void CTPTraderAdapter::RejectPromise(int request_id, const std::string& error_msg) {
    std::shared_ptr<std::promise<void>> promise;
    {
        std::lock_guard<std::mutex> lock(promise_map_mutex_);
        const auto it = query_promises_.find(request_id);
        if (it == query_promises_.end()) {
            return;
        }
        promise = it->second;
        query_promises_.erase(it);
    }
    try {
        promise->set_exception(
            std::make_exception_ptr(std::runtime_error(error_msg.empty() ? "query failed"
                                                                         : error_msg)));
    } catch (...) {
    }
}

void CTPTraderAdapter::ResolveSettlementPromise(int request_id) {
    std::shared_ptr<std::promise<void>> promise;
    {
        std::lock_guard<std::mutex> lock(promise_map_mutex_);
        const auto it = settlement_promises_.find(request_id);
        if (it == settlement_promises_.end()) {
            return;
        }
        promise = it->second;
        settlement_promises_.erase(it);
    }
    try {
        promise->set_value();
    } catch (...) {
    }
}

void CTPTraderAdapter::RejectSettlementPromise(int request_id, const std::string& error_msg) {
    std::shared_ptr<std::promise<void>> promise;
    {
        std::lock_guard<std::mutex> lock(promise_map_mutex_);
        const auto it = settlement_promises_.find(request_id);
        if (it == settlement_promises_.end()) {
            return;
        }
        promise = it->second;
        settlement_promises_.erase(it);
    }
    try {
        promise->set_exception(
            std::make_exception_ptr(std::runtime_error(error_msg.empty() ? "settlement confirm failed"
                                                                         : error_msg)));
    } catch (...) {
    }
}

void CTPTraderAdapter::ResolveLoginPromise(int request_id,
                                           int error_code,
                                           const std::string& error_msg) {
    std::shared_ptr<std::promise<std::pair<int, std::string>>> promise;
    {
        std::lock_guard<std::mutex> lock(promise_map_mutex_);
        const auto it = login_promises_.find(request_id);
        if (it == login_promises_.end()) {
            return;
        }
        promise = it->second;
        login_promises_.erase(it);
    }
    try {
        promise->set_value({error_code, error_msg});
    } catch (...) {
    }
}

void CTPTraderAdapter::ScheduleReconnect() {
    if (!need_reconnect_.load(std::memory_order_relaxed)) {
        return;
    }
    const int attempt = reconnect_attempts_.fetch_add(1, std::memory_order_relaxed);
    if (attempt >= kMaxReconnectAttempts) {
        need_reconnect_.store(false, std::memory_order_relaxed);
        return;
    }
    const int shift = std::min(attempt, 10);
    const int delay_ms = std::min(30000, kBaseReconnectDelayMs * (1 << shift));
    std::thread([this, delay_ms]() {
        std::this_thread::sleep_for(std::chrono::milliseconds(delay_ms));
        OnReconnectTimer();
    }).detach();
}

void CTPTraderAdapter::OnReconnectTimer() {
    if (!need_reconnect_.load(std::memory_order_relaxed)) {
        return;
    }
    last_reconnect_time_ = std::chrono::steady_clock::now();
    if (!gateway_->IsHealthy()) {
        ScheduleReconnect();
        return;
    }

    MarketDataConnectConfig cfg;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (!has_connect_config_) {
            need_reconnect_.store(false, std::memory_order_relaxed);
            return;
        }
        cfg = last_connect_config_;
    }

    auto login_future = LoginAsync(cfg.broker_id, cfg.user_id, cfg.password, 5000);
    if (login_future.wait_for(std::chrono::seconds(6)) != std::future_status::ready) {
        ScheduleReconnect();
        return;
    }
    const auto login_result = login_future.get();
    if (login_result.first != 0) {
        ScheduleReconnect();
        return;
    }

    if (!ConfirmSettlement()) {
        ScheduleReconnect();
        return;
    }
    if (!RecoverOrdersAndTrades()) {
        ScheduleReconnect();
        return;
    }
    {
        std::lock_guard<std::mutex> lock(mutex_);
        state_ = TraderSessionState::kReady;
    }
    ResetReconnectState();
}

void CTPTraderAdapter::ResetReconnectState() {
    need_reconnect_.store(false, std::memory_order_relaxed);
    reconnect_attempts_.store(0, std::memory_order_relaxed);
    last_reconnect_time_ = std::chrono::steady_clock::now();
}

}  // namespace quant_hft
