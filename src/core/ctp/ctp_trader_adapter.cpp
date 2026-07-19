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

namespace {

constexpr std::int64_t kNanosPerMilli = 1'000'000;
constexpr int kInitialAdapterRequestId = 1000;
constexpr auto kSettlementConfirmTimeout = std::chrono::seconds(30);
constexpr auto kSettlementTimeoutAccountProbe = std::chrono::seconds(12);

std::string BuildOrderRefString(const std::string& strategy_id, std::uint64_t seq) {
    const auto unix_ms = NowEpochNanos() / kNanosPerMilli;
    return strategy_id + "_" + std::to_string(unix_ms) + "_" + std::to_string(seq);
}

std::string BoolString(bool value) { return value ? "true" : "false"; }

bool IsReconnectRecoveryStage(const std::string& stage) {
    return stage == "connection_lost" || stage == "gateway_healthy" || stage == "scheduled" ||
           stage == "gateway_not_healthy" || stage == "exhausted" || stage == "login_timeout" ||
           stage == "login_failed" || stage == "settlement_failed" || stage == "recover_failed" ||
           stage == "retry_after_exhaustion";
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
                       {"settlement_confirmed", BoolString(settlement_confirmed)}});
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
                       callback_queue_size, callback_critical_wait_ms) {
    owns_gateway_ = true;
}

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
    StartLoginTimeoutWorker();

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
    gateway_->RegisterOrderSubmitPrepareCallback(
        [this](const CtpOrderSubmitMapping& mapping, std::string* error) {
            OrderSubmitPrepareCallback callback;
            {
                std::lock_guard<std::mutex> lock(mutex_);
                callback = user_order_submit_prepare_callback_;
            }
            return callback == nullptr || callback(mapping, error);
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

    connection_listener_token_ = gateway_->AddConnectionStateListener([this](bool healthy) {
        CtpConnectedGauge()->Set(healthy ? 1.0 : 0.0);
        if (!healthy) {
            bool should_reconnect = false;
            TraderSessionState previous_state = TraderSessionState::kDisconnected;
            {
                std::lock_guard<std::mutex> lock(mutex_);
                previous_state = state_;
                if (state_ != TraderSessionState::kDisconnected && has_connect_config_) {
                    should_reconnect = true;
                }
                state_ = TraderSessionState::kDisconnected;
                settlement_confirmed_ = false;
                SetReconnectStageLocked("connection_lost");
            }
            EmitStructuredLog(nullptr, "ctp_trader_adapter", "warn", "ctp_trader_connection_lost",
                              {{"previous_state", TraderSessionStateToString(previous_state)},
                               {"will_reconnect", BoolString(should_reconnect)}});
            if (!should_reconnect) {
                return;
            }
            need_reconnect_.store(true, std::memory_order_relaxed);
            reconnect_attempts_.store(0, std::memory_order_relaxed);
            ScheduleReconnect();
            return;
        }
        bool should_restore = false;
        TraderSessionState previous_state = TraderSessionState::kDisconnected;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            previous_state = state_;
            should_restore = has_connect_config_ && state_ != TraderSessionState::kReady &&
                             IsReconnectRecoveryStage(last_reconnect_stage_);
            if (should_restore) {
                SetReconnectStageLocked("gateway_healthy");
            }
        }
        if (should_restore) {
            const bool was_reconnecting = need_reconnect_.exchange(true, std::memory_order_relaxed);
            if (!was_reconnecting) {
                reconnect_attempts_.store(0, std::memory_order_relaxed);
            }
            EmitStructuredLog(nullptr, "ctp_trader_adapter", "info",
                              "ctp_trader_gateway_healthy_restore_scheduled",
                              {{"previous_state", TraderSessionStateToString(previous_state)},
                               {"was_reconnecting", BoolString(was_reconnecting)}});
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
                    SetReconnectStageLocked("ready");
                } else {
                    SetReconnectStageLocked("logged_in");
                }
            }
            ResolveLoginPromise(request_id, error_code, error_msg);
        });

    gateway_->RegisterQueryCompleteCallback(
        [this](int request_id, const std::string&, bool success) {
            if (!dispatcher_.WaitUntilDrained(5'000)) {
                RejectPromise(request_id, "query callback drain timeout");
                return;
            }
            if (!callback_dispatcher_.Post(
                    [this, request_id, success]() {
                        if (success) {
                            ResolvePromise(request_id);
                        } else {
                            RejectPromise(request_id, "query failed");
                        }
                    },
                    true)) {
                RejectPromise(request_id, "query callback barrier enqueue failed");
            }
        });

    gateway_->RegisterSettlementConfirmCallback(
        [this](int request_id, int error_code, const std::string& error_msg) {
            if (error_code == 0) {
                std::lock_guard<std::mutex> lock(mutex_);
                settlement_confirmed_ = true;
                state_ = TraderSessionState::kSettlementConfirmed;
                SetReconnectStageLocked("settlement_confirmed");
            } else {
                std::lock_guard<std::mutex> lock(mutex_);
                settlement_confirmed_ = false;
                SetReconnectStageLocked("settlement_failed");
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
    StopLoginTimeoutWorker();
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
        SetReconnectStageLocked("connecting");
    }
    next_request_id_.store(kInitialAdapterRequestId, std::memory_order_relaxed);
    need_reconnect_.store(false, std::memory_order_relaxed);
    reconnect_attempts_.store(0, std::memory_order_relaxed);
    {
        RejectAllPromises("adapter reconnecting");
    }

    dispatcher_.Start();
    if (!gateway_->IsHealthy() && !gateway_->Connect(config)) {
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
            SetReconnectStageLocked("ready");
        } else {
            SetReconnectStageLocked("logged_in");
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
        SetReconnectStageLocked("disconnected");
    }
    if (owns_gateway_) {
        gateway_->Disconnect();
    }
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

CtpTraderReadinessSnapshot CTPTraderAdapter::GetReadinessSnapshot() const {
    CtpTraderReadinessSnapshot snapshot;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        snapshot.state = state_;
        snapshot.ready = state_ == TraderSessionState::kReady;
        snapshot.settlement_confirmed = settlement_confirmed_;
        snapshot.last_reconnect_stage = last_reconnect_stage_;
    }
    snapshot.gateway_healthy = gateway_ != nullptr && gateway_->IsHealthy();
    snapshot.need_reconnect = need_reconnect_.load(std::memory_order_relaxed);
    snapshot.reconnect_attempts = reconnect_attempts_.load(std::memory_order_relaxed);
    snapshot.last_connect_diagnostic =
        gateway_ == nullptr ? "" : gateway_->GetLastConnectDiagnostic();
    return snapshot;
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
                    SetReconnectStageLocked("ready");
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
    SetReconnectStageLocked("ready");
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

    // Install the deadline before invoking CTP: a simulated transport and, in practice, a very
    // fast callback can complete synchronously from RequestUserLogin.
    {
        std::lock_guard<std::mutex> lock(login_timeout_mutex_);
        login_deadlines_[request_id] = LoginDeadline{
            std::chrono::steady_clock::now() + std::chrono::milliseconds(std::max(1, timeout_ms)),
            lifecycle_generation_.load(std::memory_order_acquire)};
    }
    login_timeout_cv_.notify_one();

    if (!gateway_->RequestUserLogin(request_id, broker_id, user_id, password)) {
        ResolveLoginPromise(request_id, -2, "ReqUserLogin failed");
        return future;
    }

    return future;
}

bool CTPTraderAdapter::RecoverOrdersAndTrades(int timeout_ms) {
    return RecoverOrdersAndTradesReport(timeout_ms).ok();
}

CtpRecoveryReport CTPTraderAdapter::RecoverOrdersAndTradesReport(int timeout_ms) {
    CtpRecoveryReport report;
    report.generation = gateway_->GetSessionGeneration();
    report.trading_day = gateway_->GetLastUserSession().trading_day;
    const auto duplicate_count_before = gateway_->GetDuplicateTradesSuppressed();
    const auto started = std::chrono::steady_clock::now();
    const auto finish = [this, &report, started, duplicate_count_before]() {
        report.elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                                std::chrono::steady_clock::now() - started)
                                .count();
        const auto duplicate_count_after = gateway_->GetDuplicateTradesSuppressed();
        report.duplicate_trades_suppressed =
            duplicate_count_after >= duplicate_count_before
                ? static_cast<std::size_t>(duplicate_count_after - duplicate_count_before)
                : 0;
        if (report.error.empty() && gateway_->GetSessionGeneration() != report.generation) {
            report.error_stage = "generation_changed";
            report.error = "CTP session generation changed during recovery";
            report.callbacks_drained = false;
        }
        return report;
    };
    const auto timeout = std::chrono::milliseconds(std::max(1, timeout_ms));

    const int order_request_id = AllocateRequestId();
    auto order_promise = std::make_shared<std::promise<void>>();
    StorePromise(order_request_id, order_promise);
    if (!EnqueueOrderQuery(order_request_id)) {
        RejectPromise(order_request_id, "ReqQryOrder failed to submit");
        report.error_stage = "order_query_submit";
        report.error = "ReqQryOrder failed to submit";
        return finish();
    }
    auto order_future = order_promise->get_future();
    if (order_future.wait_for(timeout) != std::future_status::ready) {
        RejectPromise(order_request_id, "ReqQryOrder timeout");
        report.error_stage = "order_query_wait";
        report.error = "ReqQryOrder timeout";
        return finish();
    }
    try {
        order_future.get();
        report.order_query_complete = true;
    } catch (const std::exception& ex) {
        report.error_stage = "order_query_result";
        report.error = ex.what();
        return finish();
    }

    const int trade_request_id = AllocateRequestId();
    auto trade_promise = std::make_shared<std::promise<void>>();
    StorePromise(trade_request_id, trade_promise);
    if (!EnqueueTradeQuery(trade_request_id)) {
        RejectPromise(trade_request_id, "ReqQryTrade failed to submit");
        report.error_stage = "trade_query_submit";
        report.error = "ReqQryTrade failed to submit";
        return finish();
    }
    auto trade_future = trade_promise->get_future();
    if (trade_future.wait_for(timeout) != std::future_status::ready) {
        RejectPromise(trade_request_id, "ReqQryTrade timeout");
        report.error_stage = "trade_query_wait";
        report.error = "ReqQryTrade timeout";
        return finish();
    }
    try {
        trade_future.get();
        report.trade_query_complete = true;
    } catch (const std::exception& ex) {
        report.error_stage = "trade_query_result";
        report.error = ex.what();
        return finish();
    }
    report.callbacks_drained = true;
    return finish();
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

void CTPTraderAdapter::RegisterOrderSubmitPrepareCallback(OrderSubmitPrepareCallback callback) {
    std::lock_guard<std::mutex> lock(mutex_);
    user_order_submit_prepare_callback_ = std::move(callback);
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
    {
        std::lock_guard<std::mutex> lock(login_timeout_mutex_);
        login_deadlines_.erase(request_id);
    }
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
    {
        std::lock_guard<std::mutex> lock(login_timeout_mutex_);
        login_deadlines_.clear();
    }
    login_timeout_cv_.notify_all();

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

    bool has_config = false;
    int max_attempts = 1;
    int base_delay_ms = 1;
    int max_delay_ms = 1;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        has_config = has_connect_config_;
        if (has_config) {
            max_attempts = std::max(1, last_connect_config_.reconnect_max_attempts);
            base_delay_ms = std::max(1, last_connect_config_.reconnect_initial_backoff_ms);
            max_delay_ms = std::max(base_delay_ms, last_connect_config_.reconnect_max_backoff_ms);
        } else {
            SetReconnectStageLocked("no_connect_config");
        }
    }
    if (!has_config) {
        need_reconnect_.store(false, std::memory_order_relaxed);
        EmitStructuredLog(nullptr, "ctp_trader_adapter", "warn", "ctp_trader_reconnect_cancelled",
                          {{"stage", "no_connect_config"}});
        return;
    }

    int attempt_number = 0;
    int delay_ms = 0;
    std::uint64_t generation = 0;
    {
        std::lock_guard<std::mutex> lock(reconnect_mutex_);
        if (reconnect_stop_) {
            return;
        }
        if (reconnect_scheduled_) {
            return;
        }
        const int observed_attempts = reconnect_attempts_.load(std::memory_order_relaxed);
        if (observed_attempts >= max_attempts) {
            SetReconnectStage("exhausted");
            EmitStructuredLog(nullptr, "ctp_trader_adapter", "error",
                              "ctp_trader_reconnect_exhausted",
                              {{"attempts", std::to_string(observed_attempts)},
                               {"max_attempts", std::to_string(max_attempts)}});
            if (!reconnect_thread_.joinable()) {
                reconnect_thread_ = std::thread(&CTPTraderAdapter::ReconnectWorkerLoop, this);
            }
            reconnect_generation_ = lifecycle_generation_.load(std::memory_order_acquire);
            generation = reconnect_generation_;
            delay_ms = max_delay_ms;
            reconnect_deadline_ =
                std::chrono::steady_clock::now() + std::chrono::milliseconds(delay_ms);
            reconnect_scheduled_ = true;
            reconnect_exhaustion_cooldown_ = true;
            EmitStructuredLog(nullptr, "ctp_trader_adapter", "info",
                              "ctp_trader_reconnect_retry_after_exhaustion_scheduled",
                              {{"attempts", std::to_string(observed_attempts)},
                               {"max_attempts", std::to_string(max_attempts)},
                               {"delay_ms", std::to_string(delay_ms)},
                               {"generation", std::to_string(generation)}});
            reconnect_cv_.notify_one();
            return;
        }
        attempt_number = observed_attempts + 1;
        reconnect_attempts_.store(attempt_number, std::memory_order_relaxed);
        reconnect_exhaustion_cooldown_ = false;
        const int shift = std::min(attempt_number - 1, 10);
        delay_ms = std::min(max_delay_ms, base_delay_ms * (1 << shift));
        if (!reconnect_thread_.joinable()) {
            reconnect_thread_ = std::thread(&CTPTraderAdapter::ReconnectWorkerLoop, this);
        }
        reconnect_generation_ = lifecycle_generation_.load(std::memory_order_acquire);
        generation = reconnect_generation_;
        reconnect_deadline_ =
            std::chrono::steady_clock::now() + std::chrono::milliseconds(delay_ms);
        reconnect_scheduled_ = true;
    }
    SetReconnectStage("scheduled");
    EmitStructuredLog(nullptr, "ctp_trader_adapter", "info", "ctp_trader_reconnect_scheduled",
                      {{"attempt", std::to_string(attempt_number)},
                       {"max_attempts", std::to_string(max_attempts)},
                       {"delay_ms", std::to_string(delay_ms)},
                       {"generation", std::to_string(generation)}});
    reconnect_cv_.notify_one();
}

void CTPTraderAdapter::OnReconnectTimer(std::uint64_t generation) {
    if (!IsGenerationCurrent(generation) || !need_reconnect_.load(std::memory_order_relaxed)) {
        return;
    }
    last_reconnect_time_ = std::chrono::steady_clock::now();
    const int attempt_number = reconnect_attempts_.load(std::memory_order_relaxed);
    if (!gateway_->IsHealthy()) {
        SetReconnectStage("gateway_not_healthy");
        EmitStructuredLog(nullptr, "ctp_trader_adapter", "warn",
                          "ctp_trader_reconnect_gateway_not_healthy",
                          {{"attempt", std::to_string(attempt_number)}});
        ScheduleReconnect();
        return;
    }

    MarketDataConnectConfig cfg;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (!has_connect_config_) {
            need_reconnect_.store(false, std::memory_order_relaxed);
            SetReconnectStageLocked("no_connect_config");
            EmitStructuredLog(nullptr, "ctp_trader_adapter", "warn",
                              "ctp_trader_reconnect_cancelled", {{"stage", "no_connect_config"}});
            return;
        }
        cfg = last_connect_config_;
    }

    // Gateway health already means both physical channels have authenticated and logged in.
    // Reissuing ReqUserLogin here races the gateway-owned lifecycle and was a primary source of
    // repeated Login timeout incidents.
    {
        std::lock_guard<std::mutex> lock(mutex_);
        state_ = TraderSessionState::kLoggedIn;
        SetReconnectStageLocked("gateway_login_confirmed");
    }

    SetReconnectStage("settlement_started");
    if (!IsGenerationCurrent(generation) || !ConfirmSettlement()) {
        SetReconnectStage("settlement_failed");
        EmitStructuredLog(nullptr, "ctp_trader_adapter", "warn",
                          "ctp_trader_reconnect_settlement_failed",
                          {{"attempt", std::to_string(attempt_number)}});
        ScheduleReconnect();
        return;
    }
    SetReconnectStage("recover_started");
    if (!IsGenerationCurrent(generation) || !RecoverOrdersAndTrades()) {
        SetReconnectStage("recover_failed");
        EmitStructuredLog(nullptr, "ctp_trader_adapter", "warn",
                          "ctp_trader_reconnect_recover_failed",
                          {{"attempt", std::to_string(attempt_number)}});
        ScheduleReconnect();
        return;
    }
    {
        std::lock_guard<std::mutex> lock(mutex_);
        state_ = TraderSessionState::kReady;
        SetReconnectStageLocked("ready");
    }
    EmitStructuredLog(nullptr, "ctp_trader_adapter", "info", "ctp_trader_reconnect_ready",
                      {{"attempt", std::to_string(attempt_number)}});
    ResetReconnectState();
}

void CTPTraderAdapter::OnReconnectExhaustionCooldown(std::uint64_t generation) {
    if (!IsGenerationCurrent(generation) || !need_reconnect_.load(std::memory_order_relaxed)) {
        return;
    }
    reconnect_attempts_.store(0, std::memory_order_relaxed);
    SetReconnectStage("retry_after_exhaustion");
    EmitStructuredLog(nullptr, "ctp_trader_adapter", "info",
                      "ctp_trader_reconnect_retry_after_exhaustion",
                      {{"generation", std::to_string(generation)}});
    ScheduleReconnect();
}

void CTPTraderAdapter::ResetReconnectState() {
    need_reconnect_.store(false, std::memory_order_relaxed);
    reconnect_attempts_.store(0, std::memory_order_relaxed);
    last_reconnect_time_ = std::chrono::steady_clock::now();
    std::lock_guard<std::mutex> lock(reconnect_mutex_);
    reconnect_exhaustion_cooldown_ = false;
}

void CTPTraderAdapter::SetReconnectStageLocked(const std::string& stage) {
    last_reconnect_stage_ = stage;
}

void CTPTraderAdapter::SetReconnectStage(const std::string& stage) {
    std::lock_guard<std::mutex> lock(mutex_);
    SetReconnectStageLocked(stage);
}

void CTPTraderAdapter::StopReconnectWorker() {
    std::thread worker;
    {
        std::lock_guard<std::mutex> lock(reconnect_mutex_);
        reconnect_stop_ = true;
        reconnect_scheduled_ = false;
        reconnect_exhaustion_cooldown_ = false;
        worker = std::move(reconnect_thread_);
    }
    reconnect_cv_.notify_all();
    if (worker.joinable()) {
        worker.join();
    }
    {
        std::lock_guard<std::mutex> lock(reconnect_mutex_);
        reconnect_stop_ = false;
        reconnect_exhaustion_cooldown_ = false;
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
        const bool exhaustion_cooldown = reconnect_exhaustion_cooldown_;
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
        if (exhaustion_cooldown) {
            reconnect_exhaustion_cooldown_ = false;
        }
        lock.unlock();
        if (exhaustion_cooldown) {
            OnReconnectExhaustionCooldown(generation);
        } else {
            OnReconnectTimer(generation);
        }
        lock.lock();
    }
}

void CTPTraderAdapter::JoinLoginTimeoutThreads() { StopLoginTimeoutWorker(); }

void CTPTraderAdapter::StartLoginTimeoutWorker() {
    std::lock_guard<std::mutex> lock(login_timeout_mutex_);
    if (login_timeout_thread_.joinable()) {
        return;
    }
    login_timeout_stop_ = false;
    login_timeout_thread_ = std::thread(&CTPTraderAdapter::LoginTimeoutWorkerLoop, this);
}

void CTPTraderAdapter::StopLoginTimeoutWorker() {
    std::thread worker;
    {
        std::lock_guard<std::mutex> lock(login_timeout_mutex_);
        login_timeout_stop_ = true;
        login_deadlines_.clear();
        worker = std::move(login_timeout_thread_);
    }
    login_timeout_cv_.notify_all();
    if (worker.joinable()) {
        worker.join();
    }
}

void CTPTraderAdapter::LoginTimeoutWorkerLoop() {
    std::unique_lock<std::mutex> lock(login_timeout_mutex_);
    while (!login_timeout_stop_) {
        if (login_deadlines_.empty()) {
            login_timeout_cv_.wait(
                lock, [this]() { return login_timeout_stop_ || !login_deadlines_.empty(); });
            continue;
        }
        auto next = std::min_element(login_deadlines_.begin(), login_deadlines_.end(),
                                     [](const auto& lhs, const auto& rhs) {
                                         return lhs.second.deadline < rhs.second.deadline;
                                     });
        const auto deadline = next->second.deadline;
        if (login_timeout_cv_.wait_until(lock, deadline) != std::cv_status::timeout) {
            continue;
        }

        const auto now = std::chrono::steady_clock::now();
        std::vector<std::pair<int, std::uint64_t>> expired;
        for (auto it = login_deadlines_.begin(); it != login_deadlines_.end();) {
            if (it->second.deadline <= now) {
                expired.emplace_back(it->first, it->second.generation);
                it = login_deadlines_.erase(it);
            } else {
                ++it;
            }
        }
        lock.unlock();
        for (const auto& [request_id, generation] : expired) {
            if (!IsGenerationCurrent(generation)) {
                continue;
            }
            std::shared_ptr<std::promise<std::pair<int, std::string>>> timed_out;
            {
                std::lock_guard<std::mutex> promise_lock(promise_map_mutex_);
                const auto it = login_promises_.find(request_id);
                if (it != login_promises_.end()) {
                    timed_out = it->second;
                    login_promises_.erase(it);
                }
            }
            if (timed_out) {
                try {
                    timed_out->set_value({-1, "Login timeout"});
                } catch (...) {
                }
            }
        }
        lock.lock();
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
    gateway_->RegisterOrderSubmitPrepareCallback(nullptr);
    gateway_->RegisterTradingAccountSnapshotCallback(nullptr);
    gateway_->RegisterInvestorPositionSnapshotCallback(nullptr);
    gateway_->RegisterInstrumentMetaSnapshotCallback(nullptr);
    gateway_->RegisterBrokerTradingParamsSnapshotCallback(nullptr);
    gateway_->RegisterInstrumentMarginRateSnapshotCallback(nullptr);
    gateway_->RegisterInstrumentCommissionRateSnapshotCallback(nullptr);
    gateway_->RegisterInstrumentOrderCommRateSnapshotCallback(nullptr);
    gateway_->RemoveConnectionStateListener(connection_listener_token_);
    gateway_->RegisterLoginResponseCallback(nullptr);
    gateway_->RegisterQueryCompleteCallback(nullptr);
    gateway_->RegisterSettlementConfirmCallback(nullptr);
}

}  // namespace quant_hft
