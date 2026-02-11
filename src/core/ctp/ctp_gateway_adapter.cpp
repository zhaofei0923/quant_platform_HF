#include "quant_hft/core/ctp_gateway_adapter.h"

#include <algorithm>
#include <chrono>
#include <condition_variable>
#include <cstdio>
#include <cstring>
#include <functional>
#include <thread>
#include <utility>
#include <vector>

#if defined(QUANT_HFT_ENABLE_CTP_REAL_API) && QUANT_HFT_ENABLE_CTP_REAL_API
#define QUANT_HFT_HAS_REAL_CTP 1
#include "ThostFtdcMdApi.h"
#include "ThostFtdcTraderApi.h"
#include "ThostFtdcUserApiDataType.h"
#include "ThostFtdcUserApiStruct.h"
#else
#define QUANT_HFT_HAS_REAL_CTP 0
#endif

namespace quant_hft {

namespace {

constexpr int kDefaultConnectTimeoutMs = 10000;

#if QUANT_HFT_HAS_REAL_CTP
std::string SafeCtpString(const char* raw) {
    if (raw == nullptr) {
        return "";
    }
    return std::string(raw);
}
#endif

template <std::size_t N>
void CopyCtpField(char (&target)[N], const std::string& value) {
    std::snprintf(target, N, "%s", value.c_str());
}

#if QUANT_HFT_HAS_REAL_CTP
bool IsRspSuccess(CThostFtdcRspInfoField* rsp_info) {
    return rsp_info == nullptr || rsp_info->ErrorID == 0;
}

std::string FormatRspError(const std::string& stage, CThostFtdcRspInfoField* rsp_info) {
    if (rsp_info == nullptr) {
        return stage;
    }
    const auto error_msg = SafeCtpString(rsp_info->ErrorMsg);
    std::string detail = stage + " (ErrorID=" + std::to_string(rsp_info->ErrorID);
    if (!error_msg.empty()) {
        detail += ", ErrorMsg=" + error_msg;
    }
    detail += ")";
    return detail;
}

OrderStatus FromCtpOrderStatus(char status) {
    switch (status) {
        case THOST_FTDC_OST_AllTraded:
            return OrderStatus::kFilled;
        case THOST_FTDC_OST_PartTradedQueueing:
        case THOST_FTDC_OST_PartTradedNotQueueing:
            return OrderStatus::kPartiallyFilled;
        case THOST_FTDC_OST_Canceled:
            return OrderStatus::kCanceled;
        case THOST_FTDC_OST_NoTradeNotQueueing:
            return OrderStatus::kRejected;
        case THOST_FTDC_OST_NoTradeQueueing:
        case THOST_FTDC_OST_Unknown:
        case THOST_FTDC_OST_NotTouched:
        case THOST_FTDC_OST_Touched:
        default:
            return OrderStatus::kAccepted;
    }
}

bool IsTerminalStatus(OrderStatus status) {
    return status == OrderStatus::kFilled || status == OrderStatus::kCanceled ||
           status == OrderStatus::kRejected;
}

char ToCtpDirection(Side side) {
    return side == Side::kBuy ? THOST_FTDC_D_Buy : THOST_FTDC_D_Sell;
}

char ToCtpOffset(OffsetFlag offset) {
    switch (offset) {
        case OffsetFlag::kClose:
            return THOST_FTDC_OF_Close;
        case OffsetFlag::kCloseToday:
            return THOST_FTDC_OF_CloseToday;
        case OffsetFlag::kCloseYesterday:
            return THOST_FTDC_OF_CloseYesterday;
        case OffsetFlag::kOpen:
        default:
            return THOST_FTDC_OF_Open;
    }
}

char ToCtpOrderPriceType(OrderType type) {
    return type == OrderType::kMarket ? THOST_FTDC_OPT_AnyPrice : THOST_FTDC_OPT_LimitPrice;
}
#endif

}  // namespace

#if QUANT_HFT_HAS_REAL_CTP
class CtpMdSpi;
class CtpTdSpi;
#endif

struct CtpGatewayAdapter::RealApiState {
#if QUANT_HFT_HAS_REAL_CTP
    CThostFtdcMdApi* md_api{nullptr};
    CThostFtdcTraderApi* td_api{nullptr};
    CtpMdSpi* md_spi{nullptr};
    CtpTdSpi* td_spi{nullptr};
#endif
    std::mutex event_mutex;
    std::condition_variable event_cv;
    bool md_front_connected{false};
    bool td_front_connected{false};
    bool md_logged_in{false};
    bool td_logged_in{false};
    std::string last_error;
};

#if QUANT_HFT_HAS_REAL_CTP

class CtpMdSpi final : public CThostFtdcMdSpi {
public:
    CtpMdSpi(CtpGatewayAdapter* owner, CtpGatewayAdapter::RealApiState* state)
        : owner_(owner), state_(state) {}

    void OnFrontConnected() override {
        {
            std::lock_guard<std::mutex> event_lock(state_->event_mutex);
            state_->md_front_connected = true;
        }
        state_->event_cv.notify_all();

        CThostFtdcReqUserLoginField req{};
        int request_id = 0;
        {
            std::lock_guard<std::mutex> lock(owner_->mutex_);
            CopyCtpField(req.BrokerID, owner_->runtime_config_.broker_id);
            CopyCtpField(req.UserID, owner_->runtime_config_.user_id);
            CopyCtpField(req.Password, owner_->runtime_config_.password);
            request_id = owner_->NextRequestIdLocked();
        }

        if (state_->md_api->ReqUserLogin(&req, request_id) != 0) {
            SetError("Md ReqUserLogin failed");
        }
    }

    void OnFrontDisconnected(int) override {
        {
            std::lock_guard<std::mutex> event_lock(state_->event_mutex);
            state_->md_front_connected = false;
            state_->md_logged_in = false;
        }
        state_->event_cv.notify_all();
        owner_->HandleConnectionLoss();
    }

    void OnRspUserLogin(CThostFtdcRspUserLoginField*, CThostFtdcRspInfoField* p_rsp_info,
                        int, bool b_is_last) override {
        if (!b_is_last) {
            return;
        }
        if (!IsRspSuccess(p_rsp_info)) {
            SetError("Md login failed", p_rsp_info);
            return;
        }

        {
            std::lock_guard<std::mutex> event_lock(state_->event_mutex);
            state_->md_logged_in = true;
        }
        state_->event_cv.notify_all();
        owner_->TryMarkHealthyFromState();
        if (!owner_->ReplayMarketDataSubscriptions()) {
            owner_->HandleConnectionLoss();
        }
    }

    void OnRspError(CThostFtdcRspInfoField* p_rsp_info, int, bool) override {
        if (!IsRspSuccess(p_rsp_info)) {
            SetError("Md response error", p_rsp_info);
        }
    }

    void OnRtnDepthMarketData(CThostFtdcDepthMarketDataField* p_depth_market_data) override {
        if (p_depth_market_data == nullptr) {
            return;
        }

        MarketSnapshot snapshot;
        snapshot.instrument_id = SafeCtpString(p_depth_market_data->InstrumentID);
        snapshot.last_price = p_depth_market_data->LastPrice;
        snapshot.bid_price_1 = p_depth_market_data->BidPrice1;
        snapshot.ask_price_1 = p_depth_market_data->AskPrice1;
        snapshot.bid_volume_1 = p_depth_market_data->BidVolume1;
        snapshot.ask_volume_1 = p_depth_market_data->AskVolume1;
        snapshot.volume = p_depth_market_data->Volume;
        snapshot.recv_ts_ns = NowEpochNanos();
        std::function<void(const MarketSnapshot&)> callback;
        {
            std::lock_guard<std::mutex> lock(owner_->mutex_);
            callback = owner_->market_data_callback_;
        }
        if (callback) {
            callback(snapshot);
        }
    }

private:
    void SetError(const std::string& stage, CThostFtdcRspInfoField* rsp_info = nullptr) {
        const auto message = FormatRspError(stage, rsp_info);
        {
            std::lock_guard<std::mutex> event_lock(state_->event_mutex);
            state_->last_error = message;
        }
        owner_->HandleConnectionLoss();
        state_->event_cv.notify_all();
    }

    CtpGatewayAdapter* owner_;
    CtpGatewayAdapter::RealApiState* state_;
};

class CtpTdSpi final : public CThostFtdcTraderSpi {
public:
    CtpTdSpi(CtpGatewayAdapter* owner, CtpGatewayAdapter::RealApiState* state)
        : owner_(owner), state_(state) {}

    void OnFrontConnected() override {
        {
            std::lock_guard<std::mutex> event_lock(state_->event_mutex);
            state_->td_front_connected = true;
        }
        state_->event_cv.notify_all();

        CtpRuntimeConfig runtime;
        int request_id = 0;
        {
            std::lock_guard<std::mutex> lock(owner_->mutex_);
            runtime = owner_->runtime_config_;
            request_id = owner_->NextRequestIdLocked();
        }

        if (runtime.enable_terminal_auth && !runtime.auth_code.empty() &&
            !runtime.app_id.empty()) {
            CThostFtdcReqAuthenticateField auth_req{};
            CopyCtpField(auth_req.BrokerID, runtime.broker_id);
            CopyCtpField(auth_req.UserID, runtime.user_id);
            CopyCtpField(auth_req.AuthCode, runtime.auth_code);
            CopyCtpField(auth_req.AppID, runtime.app_id);
            if (state_->td_api->ReqAuthenticate(&auth_req, request_id) != 0) {
                SetError("Td ReqAuthenticate failed");
            }
            return;
        }

        SendUserLogin();
    }

    void OnFrontDisconnected(int) override {
        {
            std::lock_guard<std::mutex> event_lock(state_->event_mutex);
            state_->td_front_connected = false;
            state_->td_logged_in = false;
        }
        state_->event_cv.notify_all();
        owner_->HandleConnectionLoss();
    }

    void OnRspAuthenticate(CThostFtdcRspAuthenticateField*, CThostFtdcRspInfoField* p_rsp_info,
                           int, bool b_is_last) override {
        if (!b_is_last) {
            return;
        }
        if (!IsRspSuccess(p_rsp_info)) {
            SetError("Td authenticate failed", p_rsp_info);
            return;
        }
        SendUserLogin();
    }

    void OnRspUserLogin(CThostFtdcRspUserLoginField* p_rsp_user_login,
                        CThostFtdcRspInfoField* p_rsp_info,
                        int,
                        bool b_is_last) override {
        if (!b_is_last) {
            return;
        }
        if (!IsRspSuccess(p_rsp_info) || p_rsp_user_login == nullptr) {
            SetError("Td login failed", p_rsp_info);
            return;
        }

        {
            std::lock_guard<std::mutex> lock(owner_->mutex_);
            owner_->front_id_ = p_rsp_user_login->FrontID;
            owner_->session_id_ = p_rsp_user_login->SessionID;
            owner_->runtime_config_.last_login_time = SafeCtpString(p_rsp_user_login->LastLoginTime);
            owner_->runtime_config_.reserve_info = SafeCtpString(p_rsp_user_login->ReserveInfo);
            owner_->user_session_.investor_id = owner_->runtime_config_.investor_id;
            owner_->user_session_.login_time = SafeCtpString(p_rsp_user_login->LoginTime);
            owner_->user_session_.last_login_time = owner_->runtime_config_.last_login_time;
            owner_->user_session_.reserve_info = owner_->runtime_config_.reserve_info;
        }

        {
            std::lock_guard<std::mutex> event_lock(state_->event_mutex);
            state_->td_logged_in = true;
        }
        state_->event_cv.notify_all();
        owner_->TryMarkHealthyFromState();
    }

    void OnRspQryUserSession(CThostFtdcUserSessionField* p_user_session,
                             CThostFtdcRspInfoField* p_rsp_info,
                             int,
                             bool b_is_last) override {
        if (!b_is_last) {
            return;
        }
        if (!IsRspSuccess(p_rsp_info) || p_user_session == nullptr) {
            return;
        }

        std::lock_guard<std::mutex> lock(owner_->mutex_);
        owner_->user_session_.investor_id = owner_->runtime_config_.investor_id;
        owner_->user_session_.login_time = SafeCtpString(p_user_session->LoginTime);
    }

    void OnRtnOffsetSetting(CThostFtdcOffsetSettingField* p_offset_setting) override {
        if (p_offset_setting == nullptr) {
            return;
        }
        std::lock_guard<std::mutex> lock(owner_->mutex_);
        owner_->offset_apply_src_ = p_offset_setting->ApplySrc;
    }

    void OnRspQryOffsetSetting(CThostFtdcOffsetSettingField* p_offset_setting,
                               CThostFtdcRspInfoField* p_rsp_info,
                               int,
                               bool b_is_last) override {
        if (!b_is_last || !IsRspSuccess(p_rsp_info) || p_offset_setting == nullptr) {
            return;
        }
        std::lock_guard<std::mutex> lock(owner_->mutex_);
        owner_->offset_apply_src_ = p_offset_setting->ApplySrc;
    }

    void OnRtnOrder(CThostFtdcOrderField* p_order) override {
        if (p_order == nullptr) {
            return;
        }

        OrderEvent event;
        event.account_id = SafeCtpString(p_order->InvestorID);
        event.exchange_order_id = SafeCtpString(p_order->OrderSysID);
        event.instrument_id = SafeCtpString(p_order->InstrumentID);
        event.status = FromCtpOrderStatus(p_order->OrderStatus);
        event.total_volume = p_order->VolumeTotalOriginal;
        event.filled_volume = p_order->VolumeTraded;
        event.avg_fill_price = p_order->LimitPrice;
        event.reason = SafeCtpString(p_order->StatusMsg);
        event.ts_ns = NowEpochNanos();

        std::function<void(const OrderEvent&)> callback;
        {
            std::lock_guard<std::mutex> lock(owner_->mutex_);
            const auto order_ref = SafeCtpString(p_order->OrderRef);
            const auto it = owner_->order_ref_to_client_id_.find(order_ref);
            if (it != owner_->order_ref_to_client_id_.end()) {
                event.client_order_id = it->second;
                if (IsTerminalStatus(event.status)) {
                    owner_->client_order_meta_.erase(it->second);
                    owner_->order_ref_to_client_id_.erase(it);
                }
            } else {
                event.client_order_id = order_ref;
            }
            callback = owner_->order_event_callback_;
        }

        if (callback) {
            callback(event);
        }
    }

    void OnRtnTrade(CThostFtdcTradeField* p_trade) override {
        if (p_trade == nullptr) {
            return;
        }

        OrderEvent event;
        event.account_id = SafeCtpString(p_trade->InvestorID);
        event.exchange_order_id = SafeCtpString(p_trade->OrderSysID);
        event.instrument_id = SafeCtpString(p_trade->InstrumentID);
        event.status = OrderStatus::kFilled;
        event.total_volume = p_trade->Volume;
        event.filled_volume = p_trade->Volume;
        event.avg_fill_price = p_trade->Price;
        event.reason = "trade";
        event.ts_ns = NowEpochNanos();

        std::function<void(const OrderEvent&)> callback;
        {
            std::lock_guard<std::mutex> lock(owner_->mutex_);
            const auto order_ref = SafeCtpString(p_trade->OrderRef);
            const auto it = owner_->order_ref_to_client_id_.find(order_ref);
            event.client_order_id =
                it == owner_->order_ref_to_client_id_.end() ? order_ref : it->second;
            callback = owner_->order_event_callback_;
        }

        if (callback) {
            callback(event);
        }
    }

    void OnRspError(CThostFtdcRspInfoField* p_rsp_info, int, bool) override {
        if (!IsRspSuccess(p_rsp_info)) {
            SetError("Td response error", p_rsp_info);
        }
    }

private:
    void SendUserLogin() {
        CtpRuntimeConfig runtime;
        int request_id = 0;
        {
            std::lock_guard<std::mutex> lock(owner_->mutex_);
            runtime = owner_->runtime_config_;
            request_id = owner_->NextRequestIdLocked();
        }

        CThostFtdcReqUserLoginField login_req{};
        CopyCtpField(login_req.BrokerID, runtime.broker_id);
        CopyCtpField(login_req.UserID, runtime.user_id);
        CopyCtpField(login_req.Password, runtime.password);

        if (state_->td_api->ReqUserLogin(&login_req, request_id) != 0) {
            SetError("Td ReqUserLogin failed");
        }
    }

    void SetError(const std::string& stage, CThostFtdcRspInfoField* rsp_info = nullptr) {
        const auto message = FormatRspError(stage, rsp_info);
        {
            std::lock_guard<std::mutex> event_lock(state_->event_mutex);
            state_->last_error = message;
        }
        owner_->HandleConnectionLoss();
        state_->event_cv.notify_all();
    }

    CtpGatewayAdapter* owner_;
    CtpGatewayAdapter::RealApiState* state_;
};

#endif

CtpGatewayAdapter::CtpGatewayAdapter(std::size_t query_qps_limit)
    : query_scheduler_(query_qps_limit) {}

CtpGatewayAdapter::~CtpGatewayAdapter() {
    Disconnect();
    StopReconnectWorker();
}

bool CtpGatewayAdapter::Connect(const MarketDataConnectConfig& config) {
    CtpRuntimeConfig runtime;
    runtime.environment = config.is_production_mode ? CtpEnvironment::kProduction
                                                     : CtpEnvironment::kSimNow;
    runtime.is_production_mode = config.is_production_mode;
    runtime.enable_real_api = config.enable_real_api;
    runtime.enable_terminal_auth = config.enable_terminal_auth;
    runtime.connect_timeout_ms = config.connect_timeout_ms;
    runtime.reconnect_max_attempts = config.reconnect_max_attempts;
    runtime.reconnect_initial_backoff_ms = config.reconnect_initial_backoff_ms;
    runtime.reconnect_max_backoff_ms = config.reconnect_max_backoff_ms;
    runtime.md_front = config.market_front_address;
    runtime.td_front = config.trader_front_address;
    runtime.flow_path = config.flow_path;
    runtime.broker_id = config.broker_id;
    runtime.user_id = config.user_id;
    runtime.investor_id = config.investor_id.empty() ? config.user_id : config.investor_id;
    runtime.password = config.password;
    runtime.app_id = config.app_id;
    runtime.auth_code = config.auth_code;

    std::string error;
    if (!CtpConfigValidator::Validate(runtime, &error)) {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            last_connect_diagnostic_ = "ctp config validation failed: " + error;
        }
        Disconnect();
        return false;
    }

    Disconnect();

    {
        std::lock_guard<std::mutex> lock(mutex_);
        runtime_config_ = std::move(runtime);
        subscriptions_.clear();
        client_order_meta_.clear();
        order_ref_to_client_id_.clear();
        front_id_ = 0;
        session_id_ = 0;
        request_id_seq_ = 0;
        order_ref_seq_ = 0;
        last_connect_diagnostic_.clear();
        reconnect_requested_ = false;
        reconnect_in_progress_ = false;
    }

    if (runtime_config_.enable_real_api) {
        return ConnectRealApi();
    }
    return ConnectSimulated();
}

bool CtpGatewayAdapter::ConnectSimulated() {
    DisconnectRealApi();
    std::lock_guard<std::mutex> lock(mutex_);
    connected_ = true;
    healthy_ = true;
    last_connect_diagnostic_.clear();
    return true;
}

bool CtpGatewayAdapter::ConnectRealApi() {
#if !QUANT_HFT_HAS_REAL_CTP
    std::lock_guard<std::mutex> lock(mutex_);
    connected_ = false;
    healthy_ = false;
    last_connect_diagnostic_ = "real CTP API is not enabled at build time";
    return false;
#else
    CtpRuntimeConfig runtime;
    bool was_connected = false;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        runtime = runtime_config_;
        was_connected = connected_;
    }
    std::vector<std::string> failures;
    for (const auto& candidate : BuildCtpFrontCandidates(runtime.md_front, runtime.td_front)) {
        std::string failure_detail;
        if (ConnectRealApiWithFrontPair(runtime, was_connected, candidate, &failure_detail)) {
            std::lock_guard<std::mutex> lock(mutex_);
            runtime_config_.md_front = candidate.md_front;
            runtime_config_.td_front = candidate.td_front;
            last_connect_diagnostic_.clear();
            return true;
        }
        failures.push_back("md=" + candidate.md_front + " td=" + candidate.td_front +
                           " => " + failure_detail);
    }
    {
        std::lock_guard<std::mutex> lock(mutex_);
        last_connect_diagnostic_ = "all candidate fronts failed: ";
        for (std::size_t i = 0; i < failures.size(); ++i) {
            if (i > 0) {
                last_connect_diagnostic_ += " | ";
            }
            last_connect_diagnostic_ += failures[i];
        }
    }
    return false;
#endif
}

bool CtpGatewayAdapter::ConnectRealApiWithFrontPair(const CtpRuntimeConfig& runtime,
                                                    bool was_connected,
                                                    const CtpFrontPair& front_pair,
                                                    std::string* failure_detail) {
#if !QUANT_HFT_HAS_REAL_CTP
    (void)runtime;
    (void)was_connected;
    (void)front_pair;
    if (failure_detail != nullptr) {
        *failure_detail = "real CTP API disabled";
    }
    return false;
#else
    DisconnectRealApi();

    auto state = std::make_unique<RealApiState>();
    const std::string flow_path = runtime.flow_path.empty() ? "ctp_flow" : runtime.flow_path;

    state->md_api = CThostFtdcMdApi::CreateFtdcMdApi(
        flow_path.c_str(), false, false, runtime.is_production_mode);
    state->td_api = CThostFtdcTraderApi::CreateFtdcTraderApi(
        flow_path.c_str(), runtime.is_production_mode);

    if (state->md_api == nullptr || state->td_api == nullptr) {
        return false;
    }

    state->md_spi = new CtpMdSpi(this, state.get());
    state->td_spi = new CtpTdSpi(this, state.get());

    state->md_api->RegisterSpi(state->md_spi);
    state->td_api->RegisterSpi(state->td_spi);

    std::string md_front = front_pair.md_front;
    std::string td_front = front_pair.td_front;
    state->md_api->RegisterFront(const_cast<char*>(md_front.c_str()));
    state->td_api->RegisterFront(const_cast<char*>(td_front.c_str()));

    {
        std::lock_guard<std::mutex> lock(mutex_);
        real_api_ = std::move(state);
    }

    RealApiState* state_ptr = nullptr;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        state_ptr = real_api_.get();
    }

    state_ptr->md_api->Init();
    state_ptr->td_api->Init();

    const int timeout_ms = runtime.connect_timeout_ms > 0 ? runtime.connect_timeout_ms
                                                           : kDefaultConnectTimeoutMs;

    bool ready = false;
    {
        std::unique_lock<std::mutex> lock(state_ptr->event_mutex);
        ready = state_ptr->event_cv.wait_for(
            lock,
            std::chrono::milliseconds(timeout_ms),
            [state_ptr] {
                return (state_ptr->md_logged_in && state_ptr->td_logged_in) ||
                       !state_ptr->last_error.empty();
            });
    }

    bool ok = false;
    std::string state_error;
    bool md_logged_in = false;
    bool td_logged_in = false;
    {
        std::lock_guard<std::mutex> lock(state_ptr->event_mutex);
        ok = ready && state_ptr->last_error.empty() && state_ptr->md_logged_in &&
             state_ptr->td_logged_in;
        state_error = state_ptr->last_error;
        md_logged_in = state_ptr->md_logged_in;
        td_logged_in = state_ptr->td_logged_in;
    }

    {
        std::lock_guard<std::mutex> lock(mutex_);
        connected_ = ok ? true : was_connected;
        healthy_ = ok;
        reconnect_requested_ = false;
        reconnect_in_progress_ = false;
    }

    if (!ok) {
        if (failure_detail != nullptr) {
            if (!state_error.empty()) {
                *failure_detail = state_error;
            } else if (!ready) {
                *failure_detail = "connect timeout waiting md/td login";
            } else {
                *failure_detail =
                    "login not complete (md_logged_in=" +
                    std::string(md_logged_in ? "true" : "false") +
                    ", td_logged_in=" +
                    std::string(td_logged_in ? "true" : "false") + ")";
            }
        }
        DisconnectRealApi();
        return false;
    }

    StartReconnectWorker();
    if (!ReplayMarketDataSubscriptions()) {
        HandleConnectionLoss();
    }
    if (failure_detail != nullptr) {
        failure_detail->clear();
    }
    return true;
#endif
}

void CtpGatewayAdapter::StartReconnectWorker() {
    std::lock_guard<std::mutex> lock(mutex_);
    if (reconnect_thread_.joinable()) {
        return;
    }
    reconnect_stop_ = false;
    reconnect_requested_ = false;
    reconnect_in_progress_ = false;
    reconnect_thread_ = std::thread(&CtpGatewayAdapter::ReconnectWorkerLoop, this);
}

void CtpGatewayAdapter::StopReconnectWorker() {
    std::thread worker;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        reconnect_stop_ = true;
        reconnect_requested_ = true;
        worker = std::move(reconnect_thread_);
    }
    reconnect_cv_.notify_all();
    if (worker.joinable()) {
        worker.join();
    }
    {
        std::lock_guard<std::mutex> lock(mutex_);
        reconnect_stop_ = false;
        reconnect_requested_ = false;
        reconnect_in_progress_ = false;
    }
}

void CtpGatewayAdapter::RequestReconnect() {
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (reconnect_stop_ || !connected_ || !runtime_config_.enable_real_api) {
            return;
        }
        reconnect_requested_ = true;
    }
    reconnect_cv_.notify_one();
}

void CtpGatewayAdapter::HandleConnectionLoss() {
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (!connected_) {
            return;
        }
        healthy_ = false;
    }
    RequestReconnect();
}

void CtpGatewayAdapter::ReconnectWorkerLoop() {
#if !QUANT_HFT_HAS_REAL_CTP
    return;
#else
    while (true) {
        CtpRuntimeConfig runtime;
        {
            std::unique_lock<std::mutex> lock(mutex_);
            reconnect_cv_.wait(lock, [this]() {
                return reconnect_stop_ || reconnect_requested_;
            });
            if (reconnect_stop_) {
                return;
            }
            reconnect_requested_ = false;
            reconnect_in_progress_ = true;
            runtime = runtime_config_;
        }

        int attempt = 0;
        int backoff_ms = std::max(1, runtime.reconnect_initial_backoff_ms);
        const int max_backoff_ms = std::max(backoff_ms, runtime.reconnect_max_backoff_ms);
        const int max_attempts = std::max(1, runtime.reconnect_max_attempts);
        bool recovered = false;

        while (attempt < max_attempts) {
            {
                std::lock_guard<std::mutex> lock(mutex_);
                if (reconnect_stop_ || !connected_ || !runtime_config_.enable_real_api) {
                    reconnect_in_progress_ = false;
                    return;
                }
            }

            ++attempt;
            if (ConnectRealApi()) {
                recovered = true;
                break;
            }

            {
                std::lock_guard<std::mutex> lock(mutex_);
                healthy_ = false;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(backoff_ms));
            backoff_ms = std::min(max_backoff_ms, backoff_ms * 2);
        }

        {
            std::lock_guard<std::mutex> lock(mutex_);
            reconnect_in_progress_ = false;
            if (!recovered) {
                connected_ = false;
            }
        }
    }
#endif
}

void CtpGatewayAdapter::TryMarkHealthyFromState() {
#if !QUANT_HFT_HAS_REAL_CTP
    return;
#else
    bool healthy = false;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (!real_api_) {
            return;
        }
        std::lock_guard<std::mutex> event_lock(real_api_->event_mutex);
        healthy = real_api_->md_logged_in && real_api_->td_logged_in &&
                  real_api_->last_error.empty();
        if (healthy) {
            healthy_ = true;
            connected_ = true;
        }
    }
#endif
}

bool CtpGatewayAdapter::ReplayMarketDataSubscriptions() {
#if !QUANT_HFT_HAS_REAL_CTP
    return false;
#else
    std::vector<std::string> instrument_ids;
    CThostFtdcMdApi* md_api = nullptr;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (!runtime_config_.enable_real_api || !real_api_ || !real_api_->md_api) {
            return false;
        }
        md_api = real_api_->md_api;
        instrument_ids.assign(subscriptions_.begin(), subscriptions_.end());
    }

    if (instrument_ids.empty()) {
        return true;
    }

    std::vector<char*> ctp_ids;
    ctp_ids.reserve(instrument_ids.size());
    for (auto& instrument_id : instrument_ids) {
        ctp_ids.push_back(instrument_id.data());
    }
    return md_api->SubscribeMarketData(ctp_ids.data(), static_cast<int>(ctp_ids.size())) == 0;
#endif
}

void CtpGatewayAdapter::DisconnectRealApi() {
#if QUANT_HFT_HAS_REAL_CTP
    std::unique_ptr<RealApiState> state;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        state = std::move(real_api_);
    }

    if (!state) {
        return;
    }

    if (state->md_api != nullptr) {
        state->md_api->RegisterSpi(nullptr);
        state->md_api->Release();
        state->md_api = nullptr;
    }
    delete state->md_spi;
    state->md_spi = nullptr;

    if (state->td_api != nullptr) {
        state->td_api->RegisterSpi(nullptr);
        state->td_api->Release();
        state->td_api = nullptr;
    }
    delete state->td_spi;
    state->td_spi = nullptr;
#else
    std::lock_guard<std::mutex> lock(mutex_);
    real_api_.reset();
#endif
}

void CtpGatewayAdapter::Disconnect() {
    StopReconnectWorker();
    DisconnectRealApi();

    std::lock_guard<std::mutex> lock(mutex_);
    connected_ = false;
    healthy_ = false;
    subscriptions_.clear();
    client_order_meta_.clear();
    order_ref_to_client_id_.clear();
    reconnect_requested_ = false;
    reconnect_in_progress_ = false;
}

bool CtpGatewayAdapter::Subscribe(const std::vector<std::string>& instrument_ids) {
    bool use_real = false;

#if QUANT_HFT_HAS_REAL_CTP
    CThostFtdcMdApi* md_api = nullptr;
#endif

    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (!connected_ || (runtime_config_.enable_real_api && !healthy_)) {
            return false;
        }
        use_real = runtime_config_.enable_real_api;
#if QUANT_HFT_HAS_REAL_CTP
        if (use_real && real_api_) {
            md_api = real_api_->md_api;
        }
#endif
    }

    if (use_real) {
#if QUANT_HFT_HAS_REAL_CTP
        if (md_api == nullptr) {
            return false;
        }
        std::vector<std::string> ids = instrument_ids;
        std::vector<char*> ctp_ids;
        ctp_ids.reserve(ids.size());
        for (auto& id : ids) {
            ctp_ids.push_back(id.data());
        }
        if (!ctp_ids.empty() && md_api->SubscribeMarketData(ctp_ids.data(), static_cast<int>(ctp_ids.size())) != 0) {
            return false;
        }
#else
        return false;
#endif
    }

    std::lock_guard<std::mutex> lock(mutex_);
    for (const auto& id : instrument_ids) {
        subscriptions_.insert(id);
    }
    return true;
}

bool CtpGatewayAdapter::Unsubscribe(const std::vector<std::string>& instrument_ids) {
    bool use_real = false;

#if QUANT_HFT_HAS_REAL_CTP
    CThostFtdcMdApi* md_api = nullptr;
#endif

    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (!connected_ || (runtime_config_.enable_real_api && !healthy_)) {
            return false;
        }
        use_real = runtime_config_.enable_real_api;
#if QUANT_HFT_HAS_REAL_CTP
        if (use_real && real_api_) {
            md_api = real_api_->md_api;
        }
#endif
    }

    if (use_real) {
#if QUANT_HFT_HAS_REAL_CTP
        if (md_api == nullptr) {
            return false;
        }
        std::vector<std::string> ids = instrument_ids;
        std::vector<char*> ctp_ids;
        ctp_ids.reserve(ids.size());
        for (auto& id : ids) {
            ctp_ids.push_back(id.data());
        }
        if (!ctp_ids.empty() && md_api->UnSubscribeMarketData(ctp_ids.data(), static_cast<int>(ctp_ids.size())) != 0) {
            return false;
        }
#else
        return false;
#endif
    }

    std::lock_guard<std::mutex> lock(mutex_);
    for (const auto& id : instrument_ids) {
        subscriptions_.erase(id);
    }
    return true;
}

void CtpGatewayAdapter::RegisterMarketDataCallback(MarketDataCallback callback) {
    std::lock_guard<std::mutex> lock(mutex_);
    market_data_callback_ = std::move(callback);
}

bool CtpGatewayAdapter::IsHealthy() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return connected_ && healthy_;
}

bool CtpGatewayAdapter::PlaceOrder(const OrderIntent& intent) {
    bool use_real = false;
    std::function<void(const OrderEvent&)> callback;
    OrderEvent simulated_event;
    bool emit_simulated_event = false;

#if QUANT_HFT_HAS_REAL_CTP
    CThostFtdcTraderApi* td_api = nullptr;
#endif

    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (!connected_ || (runtime_config_.enable_real_api && !healthy_)) {
            return false;
        }

        use_real = runtime_config_.enable_real_api;

#if QUANT_HFT_HAS_REAL_CTP
        if (use_real && real_api_) {
            td_api = real_api_->td_api;
        }

        if (use_real) {
            if (td_api == nullptr) {
                return false;
            }

            CThostFtdcInputOrderField req{};
            CopyCtpField(req.BrokerID, runtime_config_.broker_id);
            CopyCtpField(req.InvestorID, runtime_config_.investor_id);
            CopyCtpField(req.UserID, runtime_config_.user_id);
            CopyCtpField(req.InstrumentID, intent.instrument_id);

            const std::string order_ref = NextOrderRefLocked();
            CopyCtpField(req.OrderRef, order_ref);

            req.OrderPriceType = ToCtpOrderPriceType(intent.type);
            req.Direction = ToCtpDirection(intent.side);
            req.CombOffsetFlag[0] = ToCtpOffset(intent.offset);
            req.CombHedgeFlag[0] = THOST_FTDC_HF_Speculation;
            req.LimitPrice = intent.price;
            req.VolumeTotalOriginal = intent.volume;
            req.TimeCondition = THOST_FTDC_TC_GFD;
            req.VolumeCondition = THOST_FTDC_VC_AV;
            req.ContingentCondition = THOST_FTDC_CC_Immediately;
            req.ForceCloseReason = THOST_FTDC_FCC_NotForceClose;
            req.MinVolume = 1;

            const int request_id = NextRequestIdLocked();
            if (td_api->ReqOrderInsert(&req, request_id) != 0) {
                return false;
            }

            OrderMeta meta;
            meta.order_ref = order_ref;
            meta.instrument_id = intent.instrument_id;
            meta.front_id = front_id_;
            meta.session_id = session_id_;
            client_order_meta_[intent.client_order_id] = meta;
            order_ref_to_client_id_[order_ref] = intent.client_order_id;
            return true;
        }
#else
        if (use_real) {
            return false;
        }
#endif

        callback = order_event_callback_;
        if (!callback) {
            return false;
        }

        simulated_event.account_id = intent.account_id;
        simulated_event.client_order_id = intent.client_order_id;
        simulated_event.exchange_order_id = "ctp-sim-" + intent.client_order_id;
        simulated_event.instrument_id = intent.instrument_id;
        simulated_event.status = OrderStatus::kAccepted;
        simulated_event.total_volume = intent.volume;
        simulated_event.ts_ns = NowEpochNanos();
        simulated_event.trace_id = intent.trace_id;
        emit_simulated_event = true;
    }

    if (emit_simulated_event && callback) {
        callback(simulated_event);
    }

    return true;
}

bool CtpGatewayAdapter::CancelOrder(const std::string& client_order_id,
                                    const std::string& trace_id) {
    bool use_real = false;
    std::function<void(const OrderEvent&)> callback;
    OrderEvent simulated_event;
    bool emit_simulated_event = false;

#if QUANT_HFT_HAS_REAL_CTP
    CThostFtdcTraderApi* td_api = nullptr;
#endif

    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (!connected_ || (runtime_config_.enable_real_api && !healthy_)) {
            return false;
        }

        use_real = runtime_config_.enable_real_api;

#if QUANT_HFT_HAS_REAL_CTP
        if (use_real && real_api_) {
            td_api = real_api_->td_api;
        }

        if (use_real) {
            if (td_api == nullptr) {
                return false;
            }

            const auto it = client_order_meta_.find(client_order_id);
            if (it == client_order_meta_.end()) {
                return false;
            }

            CThostFtdcInputOrderActionField req{};
            CopyCtpField(req.BrokerID, runtime_config_.broker_id);
            CopyCtpField(req.InvestorID, runtime_config_.investor_id);
            CopyCtpField(req.UserID, runtime_config_.user_id);
            CopyCtpField(req.InstrumentID, it->second.instrument_id);
            CopyCtpField(req.OrderRef, it->second.order_ref);
            req.FrontID = it->second.front_id;
            req.SessionID = it->second.session_id;
            req.ActionFlag = THOST_FTDC_AF_Delete;

            const int request_id = NextRequestIdLocked();
            if (td_api->ReqOrderAction(&req, request_id) != 0) {
                return false;
            }
            return true;
        }
#else
        if (use_real) {
            return false;
        }
#endif

        callback = order_event_callback_;
        if (!callback) {
            return false;
        }

        simulated_event.account_id = runtime_config_.user_id;
        simulated_event.client_order_id = client_order_id;
        simulated_event.exchange_order_id = "ctp-sim-" + client_order_id;
        simulated_event.status = OrderStatus::kCanceled;
        simulated_event.reason = "cancel request accepted";
        simulated_event.ts_ns = NowEpochNanos();
        simulated_event.trace_id = trace_id;
        emit_simulated_event = true;
    }

    if (emit_simulated_event && callback) {
        callback(simulated_event);
    }

    return true;
}

void CtpGatewayAdapter::RegisterOrderEventCallback(OrderEventCallback callback) {
    std::lock_guard<std::mutex> lock(mutex_);
    order_event_callback_ = std::move(callback);
}

bool CtpGatewayAdapter::EnqueueUserSessionQuery(int request_id) {
    bool query_ok = true;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (!connected_) {
            return false;
        }
    }

    QueryScheduler::QueryTask task;
    task.request_id = request_id;
    task.priority = QueryScheduler::Priority::kHigh;
    task.execute = [this, request_id, &query_ok]() {
        std::lock_guard<std::mutex> lock(mutex_);

        if (runtime_config_.enable_real_api) {
#if QUANT_HFT_HAS_REAL_CTP
            if (!healthy_ || !real_api_ || !real_api_->td_api) {
                query_ok = false;
                return;
            }
            CThostFtdcQryUserSessionField req{};
            CopyCtpField(req.BrokerID, runtime_config_.broker_id);
            CopyCtpField(req.UserID, runtime_config_.user_id);
            req.FrontID = front_id_;
            req.SessionID = session_id_;
            query_ok = real_api_->td_api->ReqQryUserSession(&req, request_id) == 0;
#else
            query_ok = false;
#endif
            return;
        }

        user_session_.investor_id = runtime_config_.investor_id;
        user_session_.login_time = "09:00:00";
        user_session_.last_login_time = runtime_config_.last_login_time;
        user_session_.reserve_info = runtime_config_.reserve_info;
    };

    if (!query_scheduler_.TrySchedule(std::move(task))) {
        return false;
    }
    return query_scheduler_.DrainOnce() > 0 && query_ok;
}

CtpUserSessionInfo CtpGatewayAdapter::GetLastUserSession() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return user_session_;
}

void CtpGatewayAdapter::UpdateOffsetApplySrc(char apply_src) {
    std::lock_guard<std::mutex> lock(mutex_);
    offset_apply_src_ = apply_src;
}

char CtpGatewayAdapter::GetOffsetApplySrc() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return offset_apply_src_;
}

std::string CtpGatewayAdapter::GetLastConnectDiagnostic() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return last_connect_diagnostic_;
}

int CtpGatewayAdapter::NextRequestIdLocked() {
    return ++request_id_seq_;
}

std::string CtpGatewayAdapter::NextOrderRefLocked() {
    return std::to_string(++order_ref_seq_);
}

}  // namespace quant_hft
