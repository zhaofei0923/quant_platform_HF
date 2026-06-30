#include "quant_hft/core/ctp_trader_adapter.h"

#include <algorithm>
#include <chrono>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>

#include "quant_hft/core/structured_log.h"
#include "quant_hft/monitoring/metric_registry.h"

namespace quant_hft {

namespace {

constexpr std::int64_t kNanosPerMilli = 1'000'000;
constexpr int kInitialAdapterRequestId = 1000;
constexpr auto kSettlementConfirmTimeout = std::chrono::seconds(30);
constexpr auto kSettlementTimeoutAccountProbe = std::chrono::seconds(12);

std::string BuildOrderRefString(const std::string& strategy_id, std::uint64_t seq) {
    const auto unix_ms = NowEpochNanos() / kNanosPerMilli;
    return strategy_id + "_" + std::to_string(unix_ms) + "_" + std::to_string(seq);
}

std::string TraderSessionStateToString(TraderSessionState state) {
    switch (state) {
        case TraderSessionState::kDisconnected:
            return "disconnected";
        case TraderSessionState::kConnected:
            return "connected";
        case TraderSessionState::kAuthenticated:
            return "authenticated";
        case TraderSessionState::kLoggedIn:
            return "logged_in";
        case TraderSessionState::kSettlementConfirmed:
            return "settlement_confirmed";
        case TraderSessionState::kReady:
            return "ready";
    }
    return "unknown";
}

void EmitOrderSubmitRejectedDiagnostic(const OrderIntent& intent, const std::string& reason,
                                       TraderSessionState state, bool settlement_confirmed) {
    EmitStructuredLog(nullptr, "ctp_trader_adapter", "warn", "ctp_order_submit_rejected",
                      {{"strategy_id", intent.strategy_id},
                       {"instrument_id", intent.instrument_id},
                       {"side", intent.side == Side::kBuy ? "buy" : "sell"},
                       {"offset", std::to_string(static_cast<int>(intent.offset))},
                       {"volume", std::to_string(intent.volume)},
                       {"client_order_id", intent.client_order_id},
                       {"trace_id", intent.trace_id},
                       {"reason", reason},
                       {"session_state", TraderSessionStateToString(state)},
                       {"settlement_confirmed", settlement_confirmed ? "true" : "false"}});
}

std::shared_ptr<MonitoringGauge> CtpConnectedGauge() {
    static auto metric = MetricRegistry::Instance().BuildGauge("quant_hft_ctp_connected",
                                                               "CTP connected state gauge");
    return metric;
}

}  // namespace

CTPTraderAdapter::CTPTraderAdapter(std::size_t query_qps_limit, std::size_t dispatcher_workers,
                                   std::size_t callback_queue_size,
                                   std::int64_t callback_critical_wait_ms)
    : CTPTraderAdapter(std::make_shared<CtpGatewayAdapter>(query_qps_limit), dispatcher_workers,
                       callback_queue_size, callback_critical_wait_ms) {}

CTPTraderAdapter::CTPTraderAdapter(std::shared_ptr<CtpGatewayAdapter> gateway,
                                   std::size_t dispatcher_workers, std::size_t callback_queue_size,
                                   std::int64_t callback_critical_wait_ms)
    : gateway_(std::move(gateway)),
      dispatcher_(dispatcher_workers),
      callback_dispatcher_(callback_queue_size, callback_critical_wait_ms) {
    if (gateway_ == nullptr) {
        gateway_ = std::make_shared<CtpGatewayAdapter>(10);
    }
    callback_dispatcher_.Start();

    gateway_->RegisterOrderEventCallback([this](const OrderEvent& event) {
        OrderEvent copied = event;
        if (!dispatcher_.Post(
                [this, copied]() {
                    OrderEventCallback callback;
                    std::function<void(bool)> breaker;
                    {
                        std::lock_guard<std::mutex> lock(mutex_);
                        callback = user_order_event_callback_;
                        breaker = circuit_breaker_callback_;
                    }
                    if (!callback) {
                        return;
                    }
                    if (!callback_dispatcher_.Post([callback, copied]() { callback(copied); },
                                                   true)) {
                        const auto stats = callback_dispatcher_.GetStats();
                        EmitStructuredLog(
                            nullptr, "ctp_trader_adapter", "error", "callback_dispatch_failed",
                            {{"is_critical", "true"},
                             {"queue_depth", std::to_string(stats.pending)},
                             {"queue_capacity", std::to_string(stats.max_queue_size)},
                             {"dropped_total", std::to_string(stats.dropped)},
                             {"critical_timeout_total", std::to_string(stats.critical_timeout)}});
                        if (breaker) {
                            breaker(true);
                        }
                    }
                },
                EventPriority::kHigh)) {
            const auto stats = dispatcher_.GetStats();
            EmitStructuredLog(nullptr, "ctp_trader_adapter", "error", "dispatcher_queue_full",
                              {{"priority", "high"},
                               {"queue_depth", std::to_string(stats.pending_high)},
                               {"dropped_total", std::to_string(stats.dropped_total)}});
        }
    });

    gateway_->RegisterOrderSubmitMappingCallback([this](const CtpOrderSubmitMapping& mapping) {
        OrderSubmitMappingCallback callback;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            callback = user_order_submit_mapping_callback_;
        }
        if (callback) {
            callback(mapping);
        }
    });

    gateway_->RegisterTradingAccountSnapshotCallback(
        [this](const TradingAccountSnapshot& snapshot) {
            TradingAccountSnapshot copied = snapshot;
            if (!dispatcher_.Post(
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
                    EventPriority::kNormal)) {
                const auto stats = dispatcher_.GetStats();
                EmitStructuredLog(nullptr, "ctp_trader_adapter", "warn", "dispatcher_queue_full",
                                  {{"priority", "normal"},
                                   {"queue_depth", std::to_string(stats.pending_normal)},
                                   {"dropped_total", std::to_string(stats.dropped_total)}});
            }
        });

    gateway_->RegisterInvestorPositionSnapshotCallback(
        [this](const std::vector<InvestorPositionSnapshot>& snapshots) {
            auto copied = snapshots;
            if (!dispatcher_.Post(
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
                    EventPriority::kNormal)) {
                const auto stats = dispatcher_.GetStats();
                EmitStructuredLog(nullptr, "ctp_trader_adapter", "warn", "dispatcher_queue_full",
                                  {{"priority", "normal"},
                                   {"queue_depth", std::to_string(stats.pending_normal)},
                                   {"dropped_total", std::to_string(stats.dropped_total)}});
            }
        });

    gateway_->RegisterInstrumentMetaSnapshotCallback(
        [this](const std::vector<InstrumentMetaSnapshot>& snapshots) {
            auto copied = snapshots;
            if (!dispatcher_.Post(
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
                    EventPriority::kNormal)) {
                const auto stats = dispatcher_.GetStats();
                EmitStructuredLog(nullptr, "ctp_trader_adapter", "warn", "dispatcher_queue_full",
                                  {{"priority", "normal"},
                                   {"queue_depth", std::to_string(stats.pending_normal)},
                                   {"dropped_total", std::to_string(stats.dropped_total)}});
            }
        });

    gateway_->RegisterBrokerTradingParamsSnapshotCallback(
        [this](const BrokerTradingParamsSnapshot& snapshot) {
            BrokerTradingParamsSnapshot copied = snapshot;
            if (!dispatcher_.Post(
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
                    EventPriority::kNormal)) {
                const auto stats = dispatcher_.GetStats();
                EmitStructuredLog(nullptr, "ctp_trader_adapter", "warn", "dispatcher_queue_full",
                                  {{"priority", "normal"},
                                   {"queue_depth", std::to_string(stats.pending_normal)},
                                   {"dropped_total", std::to_string(stats.dropped_total)}});
            }
        });

    gateway_->RegisterInstrumentMarginRateSnapshotCallback(
        [this](const std::vector<InstrumentMarginRateSnapshot>& snapshots) {
            auto copied = snapshots;
            if (!dispatcher_.Post(
                    [this, copied = std::move(copied)]() {
                        InstrumentMarginRateSnapshotCallback callback;
                        {
                            std::lock_guard<std::mutex> lock(mutex_);
                            callback = user_instrument_margin_rate_callback_;
                        }
                        if (callback) {
                            callback(copied);
                        }
                    },
                    EventPriority::kNormal)) {
                const auto stats = dispatcher_.GetStats();
                EmitStructuredLog(nullptr, "ctp_trader_adapter", "warn", "dispatcher_queue_full",
                                  {{"priority", "normal"},
                                   {"queue_depth", std::to_string(stats.pending_normal)},
                                   {"dropped_total", std::to_string(stats.dropped_total)}});
            }
        });

    gateway_->RegisterInstrumentCommissionRateSnapshotCallback(
        [this](const std::vector<InstrumentCommissionRateSnapshot>& snapshots) {
            auto copied = snapshots;
            if (!dispatcher_.Post(
                    [this, copied = std::move(copied)]() {
                        InstrumentCommissionRateSnapshotCallback callback;
                        {
                            std::lock_guard<std::mutex> lock(mutex_);
                            callback = user_instrument_commission_rate_callback_;
                        }
                        if (callback) {
                            callback(copied);
                        }
                    },
                    EventPriority::kNormal)) {
                const auto stats = dispatcher_.GetStats();
                EmitStructuredLog(nullptr, "ctp_trader_adapter", "warn", "dispatcher_queue_full",
                                  {{"priority", "normal"},
                                   {"queue_depth", std::to_string(stats.pending_normal)},
                                   {"dropped_total", std::to_string(stats.dropped_total)}});
            }
        });

    gateway_->RegisterInstrumentOrderCommRateSnapshotCallback(
        [this](const std::vector<InstrumentOrderCommRateSnapshot>& snapshots) {
            auto copied = snapshots;
            if (!dispatcher_.Post(
                    [this, copied = std::move(copied)]() {
                        InstrumentOrderCommRateSnapshotCallback callback;
                        {
                            std::lock_guard<std::mutex> lock(mutex_);
                            callback = user_instrument_order_comm_rate_callback_;
                        }
                        if (callback) {
                            callback(copied);
                        }
                    },
                    EventPriority::kNormal)) {
                const auto stats = dispatcher_.GetStats();
                EmitStructuredLog(nullptr, "ctp_trader_adapter", "warn", "dispatcher_queue_full",
                                  {{"priority", "normal"},
                                   {"queue_depth", std::to_string(stats.pending_normal)},
                                   {"dropped_total", std::to_string(stats.dropped_total)}});
            }
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

    gateway_->RegisterLoginResponseCallback(
        [this](int request_id, int error_code, const std::string& error_msg) {
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

    gateway_->RegisterSettlementConfirmCallback(
        [this](int request_id, int error_code, const std::string& error_msg) {
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
                RejectSettlementPromise(
                    request_id, error_msg.empty() ? "confirm settlement failed" : error_msg);
            }
        });
}

CTPTraderAdapter::~CTPTraderAdapter() {
    Disconnect();
    UnregisterGatewayCallbacks();
    JoinLoginTimeoutThreads();
    callback_dispatcher_.Stop();
}

bool CTPTraderAdapter::Connect(const MarketDataConnectConfig& config) {
    Disconnect();
    lifecycle_generation_.fetch_add(1, std::memory_order_acq_rel);
    {
        std::lock_guard<std::mutex> lock(mutex_);
        settlement_confirm_required_ = config.settlement_confirm_required;
        settlement_confirmed_ = false;
        state_ = TraderSessionState::kDisconnected;
        last_connect_config_ = config;
        has_connect_config_ = true;
    }
    next_request_id_.store(kInitialAdapterRequestId, std::memory_order_relaxed);
    need_reconnect_.store(false, std::memory_order_relaxed);
    reconnect_attempts_.store(0, std::memory_order_relaxed);
    {
        RejectAllPromises("adapter reconnecting");
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
    lifecycle_generation_.fetch_add(1, std::memory_order_acq_rel);
    StopReconnectWorker();
    {
        std::lock_guard<std::mutex> lock(mutex_);
        has_connect_config_ = false;
        settlement_confirmed_ = false;
        state_ = TraderSessionState::kDisconnected;
    }
    gateway_->Disconnect();
    CtpConnectedGauge()->Set(0.0);
    dispatcher_.Stop();
    RejectAllPromises("adapter disconnected");
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
        EmitStructuredLog(nullptr, "ctp_trader_adapter", "error", "settlement_confirm_error",
                          {{"error", "ReqSettlementInfoConfirm failed to submit"}});
        RejectSettlementPromise(request_id, "ReqSettlementInfoConfirm failed to submit");
        return false;
    }

    auto future = promise->get_future();
    if (future.wait_for(kSettlementConfirmTimeout) != std::future_status::ready) {
        EmitStructuredLog(nullptr, "ctp_trader_adapter", "error", "settlement_confirm_error",
                          {{"error", "confirm settlement timeout"}});
        const auto before_snapshot = gateway_->GetLastTradingAccountSnapshot();
        const int account_request_id = AllocateRequestId();
        if (gateway_->EnqueueTradingAccountQuery(account_request_id)) {
            const auto deadline = std::chrono::steady_clock::now() + kSettlementTimeoutAccountProbe;
            while (std::chrono::steady_clock::now() < deadline) {
                const auto snapshot = gateway_->GetLastTradingAccountSnapshot();
                if (snapshot.ts_ns > 0 && snapshot.ts_ns != before_snapshot.ts_ns) {
                    ResolveSettlementPromise(request_id);
                    std::lock_guard<std::mutex> lock(mutex_);
                    settlement_confirmed_ = true;
                    state_ = TraderSessionState::kReady;
                    EmitStructuredLog(nullptr, "ctp_trader_adapter", "warn",
                                      "settlement_confirm_timeout_account_probe_ready");
                    return true;
                }
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
            }
        }
        RejectSettlementPromise(request_id, "confirm settlement timeout");
        return false;
    }
    try {
        future.get();
    } catch (const std::exception& ex) {
        EmitStructuredLog(nullptr, "ctp_trader_adapter", "error", "settlement_confirm_error",
                          {{"error", ex.what()}});
        return false;
    } catch (...) {
        EmitStructuredLog(nullptr, "ctp_trader_adapter", "error", "settlement_confirm_error",
                          {{"error", "unknown exception"}});
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
    TraderSessionState observed_state = TraderSessionState::kDisconnected;
    bool observed_settlement_confirmed = false;
    std::string reject_reason;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        observed_state = state_;
        observed_settlement_confirmed = settlement_confirmed_;
        if (state_ != TraderSessionState::kReady) {
            reject_reason = "trader_not_ready";
        } else if (!settlement_confirmed_) {
            reject_reason = "settlement_unconfirmed";
        } else if (request.strategy_id.empty()) {
            reject_reason = "missing_strategy_id";
        }
        if (reject_reason.empty() && request.client_order_id.empty()) {
            ++order_ref_seq_;
            request.client_order_id = BuildOrderRefString(request.strategy_id, order_ref_seq_);
        }
    }
    if (!reject_reason.empty()) {
        EmitOrderSubmitRejectedDiagnostic(request, reject_reason, observed_state,
                                          observed_settlement_confirmed);
        return "";
    }
    if (!gateway_->PlaceOrder(request)) {
        EmitOrderSubmitRejectedDiagnostic(request, "gateway_place_order_failed", observed_state,
                                          observed_settlement_confirmed);
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

    const auto generation = lifecycle_generation_.load(std::memory_order_acquire);
    std::thread timeout_thread([this, request_id, timeout_ms, generation]() {
        std::this_thread::sleep_for(std::chrono::milliseconds(std::max(1, timeout_ms)));
        if (!IsGenerationCurrent(generation)) {
            return;
        }
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
    });
    {
        std::lock_guard<std::mutex> lock(login_timeout_threads_mutex_);
        login_timeout_threads_.push_back(std::move(timeout_thread));
    }

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
    return EnqueueInstrumentQuery(request_id, "");
}

bool CTPTraderAdapter::EnqueueInstrumentQuery(int request_id, const std::string& instrument_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (state_ < TraderSessionState::kLoggedIn) {
        return false;
    }
    return gateway_->EnqueueInstrumentQuery(request_id, instrument_id);
}

int CTPTraderAdapter::EnqueueInstrumentQuery() {
    const int request_id = AllocateRequestId();
    return EnqueueInstrumentQuery(request_id) ? request_id : -1;
}

int CTPTraderAdapter::EnqueueInstrumentQuery(const std::string& instrument_id) {
    const int request_id = AllocateRequestId();
    return EnqueueInstrumentQuery(request_id, instrument_id) ? request_id : -1;
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

bool CTPTraderAdapter::EnqueueInstrumentOrderCommRateQuery(int request_id,
                                                           const std::string& instrument_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (state_ < TraderSessionState::kLoggedIn) {
        return false;
    }
    return gateway_->EnqueueInstrumentOrderCommRateQuery(request_id, instrument_id);
}

int CTPTraderAdapter::EnqueueInstrumentOrderCommRateQuery(const std::string& instrument_id) {
    const int request_id = AllocateRequestId();
    return EnqueueInstrumentOrderCommRateQuery(request_id, instrument_id) ? request_id : -1;
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

void CTPTraderAdapter::RegisterOrderSubmitMappingCallback(OrderSubmitMappingCallback callback) {
    std::lock_guard<std::mutex> lock(mutex_);
    user_order_submit_mapping_callback_ = std::move(callback);
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

void CTPTraderAdapter::RegisterInstrumentMarginRateSnapshotCallback(
    InstrumentMarginRateSnapshotCallback callback) {
    std::lock_guard<std::mutex> lock(mutex_);
    user_instrument_margin_rate_callback_ = std::move(callback);
}

void CTPTraderAdapter::RegisterInstrumentCommissionRateSnapshotCallback(
    InstrumentCommissionRateSnapshotCallback callback) {
    std::lock_guard<std::mutex> lock(mutex_);
    user_instrument_commission_rate_callback_ = std::move(callback);
}

void CTPTraderAdapter::RegisterInstrumentOrderCommRateSnapshotCallback(
    InstrumentOrderCommRateSnapshotCallback callback) {
    std::lock_guard<std::mutex> lock(mutex_);
    user_instrument_order_comm_rate_callback_ = std::move(callback);
}

void CTPTraderAdapter::SetCircuitBreaker(std::function<void(bool)> callback) {
    std::lock_guard<std::mutex> lock(mutex_);
    circuit_breaker_callback_ = std::move(callback);
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
        promise->set_exception(std::make_exception_ptr(
            std::runtime_error(error_msg.empty() ? "query failed" : error_msg)));
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
        promise->set_exception(std::make_exception_ptr(
            std::runtime_error(error_msg.empty() ? "settlement confirm failed" : error_msg)));
    } catch (...) {
    }
}

void CTPTraderAdapter::ResolveLoginPromise(int request_id, int error_code,
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

void CTPTraderAdapter::RejectAllPromises(const std::string& error_msg) {
    std::unordered_map<int, std::shared_ptr<std::promise<void>>> query_promises;
    std::unordered_map<int, std::shared_ptr<std::promise<void>>> settlement_promises;
    std::unordered_map<int, std::shared_ptr<std::promise<std::pair<int, std::string>>>>
        login_promises;
    {
        std::lock_guard<std::mutex> lock(promise_map_mutex_);
        query_promises.swap(query_promises_);
        settlement_promises.swap(settlement_promises_);
        login_promises.swap(login_promises_);
    }

    const auto exception = std::make_exception_ptr(
        std::runtime_error(error_msg.empty() ? "adapter stopped" : error_msg));
    for (auto& entry : query_promises) {
        auto& promise = entry.second;
        try {
            promise->set_exception(exception);
        } catch (...) {
        }
    }
    for (auto& entry : settlement_promises) {
        auto& promise = entry.second;
        try {
            promise->set_exception(exception);
        } catch (...) {
        }
    }
    for (auto& entry : login_promises) {
        auto& promise = entry.second;
        try {
            promise->set_value({-3, error_msg.empty() ? "adapter stopped" : error_msg});
        } catch (...) {
        }
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
    {
        std::lock_guard<std::mutex> lock(reconnect_mutex_);
        if (reconnect_stop_) {
            return;
        }
        if (!reconnect_thread_.joinable()) {
            reconnect_thread_ = std::thread(&CTPTraderAdapter::ReconnectWorkerLoop, this);
        }
        if (reconnect_scheduled_) {
            return;
        }
        reconnect_generation_ = lifecycle_generation_.load(std::memory_order_acquire);
        reconnect_deadline_ =
            std::chrono::steady_clock::now() + std::chrono::milliseconds(delay_ms);
        reconnect_scheduled_ = true;
    }
    reconnect_cv_.notify_one();
}

void CTPTraderAdapter::OnReconnectTimer(std::uint64_t generation) {
    if (!IsGenerationCurrent(generation) || !need_reconnect_.load(std::memory_order_relaxed)) {
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
    if (!IsGenerationCurrent(generation) || login_result.first != 0) {
        ScheduleReconnect();
        return;
    }

    if (!IsGenerationCurrent(generation) || !ConfirmSettlement()) {
        ScheduleReconnect();
        return;
    }
    if (!IsGenerationCurrent(generation) || !RecoverOrdersAndTrades()) {
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

void CTPTraderAdapter::StopReconnectWorker() {
    std::thread worker;
    {
        std::lock_guard<std::mutex> lock(reconnect_mutex_);
        reconnect_stop_ = true;
        reconnect_scheduled_ = false;
        worker = std::move(reconnect_thread_);
    }
    reconnect_cv_.notify_all();
    if (worker.joinable()) {
        worker.join();
    }
    {
        std::lock_guard<std::mutex> lock(reconnect_mutex_);
        reconnect_stop_ = false;
        reconnect_generation_ = lifecycle_generation_.load(std::memory_order_acquire);
    }
}

void CTPTraderAdapter::ReconnectWorkerLoop() {
    std::unique_lock<std::mutex> lock(reconnect_mutex_);
    while (true) {
        reconnect_cv_.wait(lock, [this]() { return reconnect_stop_ || reconnect_scheduled_; });
        if (reconnect_stop_) {
            return;
        }
        const auto deadline = reconnect_deadline_;
        const auto generation = reconnect_generation_;
        const bool interrupted = reconnect_cv_.wait_until(lock, deadline, [this, generation]() {
            return reconnect_stop_ || !reconnect_scheduled_ || reconnect_generation_ != generation;
        });
        if (reconnect_stop_) {
            return;
        }
        if (interrupted) {
            continue;
        }
        reconnect_scheduled_ = false;
        lock.unlock();
        OnReconnectTimer(generation);
        lock.lock();
    }
}

void CTPTraderAdapter::JoinLoginTimeoutThreads() {
    std::vector<std::thread> threads;
    {
        std::lock_guard<std::mutex> lock(login_timeout_threads_mutex_);
        threads.swap(login_timeout_threads_);
    }
    for (auto& thread : threads) {
        if (thread.joinable()) {
            thread.join();
        }
    }
}

bool CTPTraderAdapter::IsGenerationCurrent(std::uint64_t generation) const {
    return generation == lifecycle_generation_.load(std::memory_order_acquire);
}

void CTPTraderAdapter::UnregisterGatewayCallbacks() {
    if (gateway_ == nullptr) {
        return;
    }
    gateway_->RegisterOrderEventCallback(nullptr);
    gateway_->RegisterOrderSubmitMappingCallback(nullptr);
    gateway_->RegisterTradingAccountSnapshotCallback(nullptr);
    gateway_->RegisterInvestorPositionSnapshotCallback(nullptr);
    gateway_->RegisterInstrumentMetaSnapshotCallback(nullptr);
    gateway_->RegisterBrokerTradingParamsSnapshotCallback(nullptr);
    gateway_->RegisterInstrumentMarginRateSnapshotCallback(nullptr);
    gateway_->RegisterInstrumentCommissionRateSnapshotCallback(nullptr);
    gateway_->RegisterInstrumentOrderCommRateSnapshotCallback(nullptr);
    gateway_->RegisterConnectionStateCallback(nullptr);
    gateway_->RegisterLoginResponseCallback(nullptr);
    gateway_->RegisterQueryCompleteCallback(nullptr);
    gateway_->RegisterSettlementConfirmCallback(nullptr);
}

}  // namespace quant_hft
