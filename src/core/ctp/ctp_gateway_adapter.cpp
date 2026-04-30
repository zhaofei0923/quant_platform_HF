#include "quant_hft/core/ctp_gateway_adapter.h"

#include <algorithm>
#include <cctype>
#include <chrono>
#include <cmath>
#include <condition_variable>
#include <cstdio>
#include <cstring>
#include <ctime>
#include <functional>
#include <string_view>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>

#include "quant_hft/monitoring/metric_registry.h"

#if defined(QUANT_HFT_ENABLE_CTP_REAL_API) && QUANT_HFT_ENABLE_CTP_REAL_API
#define QUANT_HFT_HAS_REAL_CTP 1
#include "ThostFtdcMdApi.h"
#include "ThostFtdcTraderApi.h"
#include "ThostFtdcUserApiDataType.h"
#include "ThostFtdcUserApiStruct.h"
#else
#define QUANT_HFT_HAS_REAL_CTP 0
#endif

#if QUANT_HFT_HAS_REAL_CTP
struct CThostFtdcOffsetSettingField;
#endif

namespace quant_hft {

namespace {

constexpr int kDefaultConnectTimeoutMs = 10000;

#if QUANT_HFT_HAS_REAL_CTP
std::shared_ptr<MonitoringCounter> CtpReconnectCounter() {
    static auto metric = MetricRegistry::Instance().BuildCounter("quant_hft_ctp_reconnect_total",
                                                                 "CTP reconnect attempt counter");
    return metric;
}
#endif

#if QUANT_HFT_HAS_REAL_CTP
std::string SafeCtpString(const char* raw) {
    if (raw == nullptr) {
        return "";
    }
    return std::string(raw);
}

template <typename Api, typename = void>
struct HasMdApiCreateWithProductionFlag : std::false_type {};

template <typename Api>
struct HasMdApiCreateWithProductionFlag<
    Api, std::void_t<decltype(Api::CreateFtdcMdApi(static_cast<const char*>(nullptr), false, false,
                                                   false))>> : std::true_type {};

template <typename Api, typename = void>
struct HasTraderApiCreateWithProductionFlag : std::false_type {};

template <typename Api>
struct HasTraderApiCreateWithProductionFlag<
    Api, std::void_t<decltype(Api::CreateFtdcTraderApi(static_cast<const char*>(nullptr), false))>>
    : std::true_type {};

template <typename Api, typename = void>
struct HasReqQryUserSession : std::false_type {};

template <typename Api>
struct HasReqQryUserSession<Api, std::void_t<decltype(std::declval<Api*>()->ReqQryUserSession(
                                     std::declval<CThostFtdcQryUserSessionField*>(), 0))>>
    : std::true_type {};

template <typename Response, typename = void>
struct HasLastLoginTimeField : std::false_type {};

template <typename Response>
struct HasLastLoginTimeField<Response,
                             std::void_t<decltype(std::declval<Response&>().LastLoginTime)>>
    : std::true_type {};

template <typename Response, typename = void>
struct HasReserveInfoField : std::false_type {};

template <typename Response>
struct HasReserveInfoField<Response, std::void_t<decltype(std::declval<Response&>().ReserveInfo)>>
    : std::true_type {};

template <typename Api>
CThostFtdcMdApi* CreateMdApiCompatImpl(const std::string& flow_path, bool is_production_mode) {
    if constexpr (HasMdApiCreateWithProductionFlag<Api>::value) {
        return Api::CreateFtdcMdApi(flow_path.c_str(), false, false, is_production_mode);
    }
    (void)is_production_mode;
    return Api::CreateFtdcMdApi(flow_path.c_str(), false, false);
}

inline CThostFtdcMdApi* CreateMdApiCompat(const std::string& flow_path, bool is_production_mode) {
    return CreateMdApiCompatImpl<CThostFtdcMdApi>(flow_path, is_production_mode);
}

template <typename Api>
CThostFtdcTraderApi* CreateTraderApiCompatImpl(const std::string& flow_path,
                                               bool is_production_mode) {
    if constexpr (HasTraderApiCreateWithProductionFlag<Api>::value) {
        return Api::CreateFtdcTraderApi(flow_path.c_str(), is_production_mode);
    }
    (void)is_production_mode;
    return Api::CreateFtdcTraderApi(flow_path.c_str());
}

inline CThostFtdcTraderApi* CreateTraderApiCompat(const std::string& flow_path,
                                                  bool is_production_mode) {
    return CreateTraderApiCompatImpl<CThostFtdcTraderApi>(flow_path, is_production_mode);
}

template <typename Response>
std::string ResolveLastLoginTimeImpl(const Response& response) {
    if constexpr (HasLastLoginTimeField<Response>::value) {
        return SafeCtpString(response.LastLoginTime);
    }
    return SafeCtpString(response.LoginTime);
}

inline std::string ResolveLastLoginTime(const CThostFtdcRspUserLoginField& response) {
    return ResolveLastLoginTimeImpl(response);
}

template <typename Response>
std::string ResolveReserveInfoImpl(const Response& response) {
    if constexpr (HasReserveInfoField<Response>::value) {
        return SafeCtpString(response.ReserveInfo);
    }
    return "";
}

inline std::string ResolveReserveInfo(const CThostFtdcRspUserLoginField& response) {
    return ResolveReserveInfoImpl(response);
}
#endif

template <std::size_t N>
void CopyCtpField(char (&target)[N], const std::string& value) {
    std::snprintf(target, N, "%s", value.c_str());
}

bool IsInvalidMarketPrice(double value) {
    return !std::isfinite(value) || std::fabs(value) >= 1e100;
}

#if QUANT_HFT_HAS_REAL_CTP
std::string DigitsOnly(const std::string& raw) {
    std::string out;
    out.reserve(raw.size());
    for (const char ch : raw) {
        if (ch >= '0' && ch <= '9') {
            out.push_back(ch);
        }
    }
    return out;
}

std::time_t PortableTimegm(std::tm* utc_tm) {
    if (utc_tm == nullptr) {
        return static_cast<std::time_t>(0);
    }
#if defined(_WIN32)
    return _mkgmtime(utc_tm);
#else
    return timegm(utc_tm);
#endif
}

EpochNanos ParseCtpDateTimeToEpochNanos(const std::string& ctp_date, const std::string& ctp_time) {
    const std::string digits_date = DigitsOnly(ctp_date);
    const std::string digits_time = DigitsOnly(ctp_time);
    if (digits_date.size() < 8 || digits_time.size() < 6) {
        return 0;
    }
    std::tm utc_tm{};
    utc_tm.tm_year = std::stoi(digits_date.substr(0, 4)) - 1900;
    utc_tm.tm_mon = std::stoi(digits_date.substr(4, 2)) - 1;
    utc_tm.tm_mday = std::stoi(digits_date.substr(6, 2));
    utc_tm.tm_hour = std::stoi(digits_time.substr(0, 2));
    utc_tm.tm_min = std::stoi(digits_time.substr(2, 2));
    utc_tm.tm_sec = std::stoi(digits_time.substr(4, 2));
    const auto utc_seconds = static_cast<std::int64_t>(PortableTimegm(&utc_tm));
    if (utc_seconds <= 0) {
        return 0;
    }
    // CTP timestamps are exchange local time (Asia/Shanghai, UTC+8).
    constexpr std::int64_t kUtcOffsetSeconds = 8LL * 3600LL;
    return (utc_seconds - kUtcOffsetSeconds) * 1'000'000'000LL;
}
#endif

#if QUANT_HFT_HAS_REAL_CTP
bool IsRspSuccess(CThostFtdcRspInfoField* rsp_info) {
    return rsp_info == nullptr || rsp_info->ErrorID == 0;
}

std::string LowerAscii(std::string value) {
    std::transform(value.begin(), value.end(), value.begin(),
                   [](unsigned char ch) { return static_cast<char>(std::tolower(ch)); });
    return value;
}

bool ContainsAnyToken(const std::string& text, std::initializer_list<std::string_view> tokens) {
    const auto normalized = LowerAscii(text);
    for (const auto token : tokens) {
        if (normalized.find(std::string(token)) != std::string::npos) {
            return true;
        }
    }
    return false;
}

bool IsRecoverableQueryError(CThostFtdcRspInfoField* rsp_info) {
    if (rsp_info == nullptr) {
        return false;
    }
    const auto message = SafeCtpString(rsp_info->ErrorMsg);
    return ContainsAnyToken(message, {
                                         "query not ready",
                                         "not ready",
                                         "flow control",
                                         "flowctrl",
                                         "throttle",
                                         "query pending",
                                         "查询未就绪",
                                         "流控",
                                         "未就绪",
                                     });
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

char ToCtpDirection(Side side) { return side == Side::kBuy ? THOST_FTDC_D_Buy : THOST_FTDC_D_Sell; }

Side FromCtpDirection(char direction) {
    return direction == THOST_FTDC_D_Sell ? Side::kSell : Side::kBuy;
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

OffsetFlag FromCtpOffset(char offset) {
    switch (offset) {
        case THOST_FTDC_OF_Close:
            return OffsetFlag::kClose;
        case THOST_FTDC_OF_CloseToday:
            return OffsetFlag::kCloseToday;
        case THOST_FTDC_OF_CloseYesterday:
            return OffsetFlag::kCloseYesterday;
        case THOST_FTDC_OF_Open:
        default:
            return OffsetFlag::kOpen;
    }
}

char ToCtpOrderPriceType(OrderType type) {
    return type == OrderType::kMarket ? THOST_FTDC_OPT_AnyPrice : THOST_FTDC_OPT_LimitPrice;
}

std::string BuildCtpTradeId(const std::string& exchange_id, Side side,
                            const std::string& trade_id) {
    if (trade_id.empty()) {
        return "";
    }
    const std::string direction = side == Side::kSell ? "SELL" : "BUY";
    if (exchange_id.empty()) {
        return direction + "|" + trade_id;
    }
    return exchange_id + "|" + direction + "|" + trade_id;
}
#endif

std::string InferExchangeIdFromInstrument(const std::string& instrument_id) {
    const auto dot_pos = instrument_id.find('.');
    if (dot_pos == std::string::npos || dot_pos == 0) {
        std::string product;
        for (const char ch : instrument_id) {
            if (std::isalpha(static_cast<unsigned char>(ch))) {
                product.push_back(static_cast<char>(std::tolower(static_cast<unsigned char>(ch))));
            } else if (!product.empty()) {
                break;
            }
        }
        if (product == "ag" || product == "al" || product == "ao" || product == "au" ||
            product == "br" || product == "bu" || product == "cu" || product == "fu" ||
            product == "hc" || product == "ni" || product == "pb" || product == "rb" ||
            product == "ru" || product == "sn" || product == "sp" || product == "ss" ||
            product == "wr" || product == "zn") {
            return "SHFE";
        }
        if (product == "a" || product == "b" || product == "bb" || product == "c" ||
            product == "cs" || product == "eb" || product == "eg" || product == "fb" ||
            product == "i" || product == "j" || product == "jd" || product == "jm" ||
            product == "l" || product == "lh" || product == "m" || product == "p" ||
            product == "pg" || product == "pp" || product == "rr" || product == "v" ||
            product == "y") {
            return "DCE";
        }
        if (product == "ap" || product == "cf" || product == "cj" || product == "cy" ||
            product == "fg" || product == "jr" || product == "lr" || product == "ma" ||
            product == "oi" || product == "pf" || product == "pk" || product == "pm" ||
            product == "ri" || product == "rm" || product == "rs" || product == "sa" ||
            product == "sf" || product == "sh" || product == "sm" || product == "sr" ||
            product == "ta" || product == "ur" || product == "wh" || product == "zc") {
            return "CZCE";
        }
        if (product == "bc" || product == "ec" || product == "lu" || product == "nr" ||
            product == "sc") {
            return "INE";
        }
        if (product == "if" || product == "ih" || product == "im" || product == "ic" ||
            product == "t" || product == "tf" || product == "tl" || product == "ts") {
            return "CFFEX";
        }
        if (product == "lc" || product == "si") {
            return "GFEX";
        }
        return "";
    }
    return instrument_id.substr(0, dot_pos);
}

void StampOrderEventTimestamps(OrderEvent* event) {
    if (event == nullptr) {
        return;
    }
    if (event->recv_ts_ns <= 0) {
        event->recv_ts_ns = NowEpochNanos();
    }
    if (event->exchange_ts_ns <= 0) {
        event->exchange_ts_ns = event->recv_ts_ns;
    }
    if (event->ts_ns <= 0) {
        event->ts_ns = event->recv_ts_ns;
    }
}

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

    void OnRspUserLogin(CThostFtdcRspUserLoginField*, CThostFtdcRspInfoField* p_rsp_info, int,
                        bool b_is_last) override {
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
        snapshot.exchange_id = SafeCtpString(p_depth_market_data->ExchangeID);
        snapshot.trading_day = SafeCtpString(p_depth_market_data->TradingDay);
        snapshot.action_day = SafeCtpString(p_depth_market_data->ActionDay);
        snapshot.update_time = SafeCtpString(p_depth_market_data->UpdateTime);
        snapshot.update_millisec = p_depth_market_data->UpdateMillisec;
        snapshot.last_price = p_depth_market_data->LastPrice;
        snapshot.bid_price_1 = p_depth_market_data->BidPrice1;
        snapshot.ask_price_1 = p_depth_market_data->AskPrice1;
        snapshot.bid_volume_1 = p_depth_market_data->BidVolume1;
        snapshot.ask_volume_1 = p_depth_market_data->AskVolume1;
        snapshot.volume = p_depth_market_data->Volume;
        snapshot.open_interest = static_cast<std::int64_t>(p_depth_market_data->OpenInterest);
        snapshot.settlement_price = p_depth_market_data->SettlementPrice;
        snapshot.average_price_raw = p_depth_market_data->AveragePrice;
        snapshot.recv_ts_ns = NowEpochNanos();
        CtpGatewayAdapter::NormalizeMarketSnapshot(&snapshot);
        std::function<void(const MarketSnapshot&)> callback;
        {
            std::lock_guard<std::mutex> lock(owner_->mutex_);
            for (const auto& meta : owner_->instrument_meta_snapshots_) {
                if (meta.instrument_id != snapshot.instrument_id) {
                    continue;
                }
                if (snapshot.exchange_id.empty() && !meta.exchange_id.empty()) {
                    snapshot.exchange_id = meta.exchange_id;
                }
                if (!IsInvalidMarketPrice(snapshot.average_price_raw) &&
                    snapshot.average_price_raw > 0.0 && meta.volume_multiple > 0 &&
                    snapshot.exchange_id != "CZCE") {
                    snapshot.average_price_norm =
                        snapshot.average_price_raw / static_cast<double>(meta.volume_multiple);
                }
                break;
            }
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

        if (runtime.enable_terminal_auth && !runtime.auth_code.empty() && !runtime.app_id.empty()) {
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

    void OnRspAuthenticate(CThostFtdcRspAuthenticateField*, CThostFtdcRspInfoField* p_rsp_info, int,
                           bool b_is_last) override {
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
                        CThostFtdcRspInfoField* p_rsp_info, int n_request_id,
                        bool b_is_last) override {
        if (!b_is_last) {
            return;
        }
        if (!IsRspSuccess(p_rsp_info) || p_rsp_user_login == nullptr) {
            CtpGatewayAdapter::LoginResponseCallback callback;
            {
                std::lock_guard<std::mutex> lock(owner_->mutex_);
                callback = owner_->login_response_callback_;
            }
            if (callback) {
                callback(n_request_id, p_rsp_info == nullptr ? -1 : p_rsp_info->ErrorID,
                         p_rsp_info == nullptr ? "Td login failed"
                                               : SafeCtpString(p_rsp_info->ErrorMsg));
            }
            SetError("Td login failed", p_rsp_info);
            return;
        }

        {
            std::lock_guard<std::mutex> lock(owner_->mutex_);
            owner_->front_id_ = p_rsp_user_login->FrontID;
            owner_->session_id_ = p_rsp_user_login->SessionID;
            owner_->runtime_config_.last_login_time = ResolveLastLoginTime(*p_rsp_user_login);
            owner_->runtime_config_.reserve_info = ResolveReserveInfo(*p_rsp_user_login);
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
        CtpGatewayAdapter::LoginResponseCallback callback;
        {
            std::lock_guard<std::mutex> lock(owner_->mutex_);
            callback = owner_->login_response_callback_;
        }
        if (callback) {
            callback(n_request_id, 0, "");
        }
        owner_->TryMarkHealthyFromState();
    }

    void OnRspQryUserSession(CThostFtdcUserSessionField* p_user_session,
                             CThostFtdcRspInfoField* p_rsp_info, int, bool b_is_last) {
        if (!b_is_last) {
            return;
        }
        owner_->CompleteScheduledQuery();
        if (!IsRspSuccess(p_rsp_info) || p_user_session == nullptr) {
            return;
        }

        std::lock_guard<std::mutex> lock(owner_->mutex_);
        owner_->user_session_.investor_id = owner_->runtime_config_.investor_id;
        owner_->user_session_.login_time = SafeCtpString(p_user_session->LoginTime);
    }

    void OnRspSettlementInfoConfirm(CThostFtdcSettlementInfoConfirmField*,
                                    CThostFtdcRspInfoField* p_rsp_info, int n_request_id,
                                    bool b_is_last) override {
        if (!b_is_last) {
            return;
        }
        CtpGatewayAdapter::SettlementConfirmCallback callback;
        {
            std::lock_guard<std::mutex> lock(owner_->mutex_);
            callback = owner_->settlement_confirm_callback_;
        }
        if (callback) {
            callback(n_request_id, p_rsp_info == nullptr ? 0 : p_rsp_info->ErrorID,
                     p_rsp_info == nullptr ? "" : SafeCtpString(p_rsp_info->ErrorMsg));
        }
    }

    void OnRtnOffsetSetting(CThostFtdcOffsetSettingField*) {}

    void OnRspQryOffsetSetting(CThostFtdcOffsetSettingField* p_offset_setting,
                               CThostFtdcRspInfoField* p_rsp_info, int, bool b_is_last) {
        (void)p_offset_setting;
        (void)p_rsp_info;
        (void)b_is_last;
    }

    void OnRspQryTradingAccount(CThostFtdcTradingAccountField* p_trading_account,
                                CThostFtdcRspInfoField* p_rsp_info, int, bool b_is_last) override {
        if (b_is_last) {
            owner_->CompleteScheduledQuery();
        }
        if (!IsRspSuccess(p_rsp_info)) {
            return;
        }

        TradingAccountSnapshot snapshot;
        bool has_snapshot = false;
        std::function<void(const TradingAccountSnapshot&)> callback;
        {
            std::lock_guard<std::mutex> lock(owner_->mutex_);
            if (p_trading_account != nullptr) {
                owner_->trading_account_snapshot_.account_id =
                    SafeCtpString(p_trading_account->AccountID);
                if (owner_->trading_account_snapshot_.account_id.empty()) {
                    owner_->trading_account_snapshot_.account_id = owner_->runtime_config_.user_id;
                }
                owner_->trading_account_snapshot_.investor_id = owner_->runtime_config_.investor_id;
                owner_->trading_account_snapshot_.balance = p_trading_account->Balance;
                owner_->trading_account_snapshot_.available = p_trading_account->Available;
                owner_->trading_account_snapshot_.curr_margin = p_trading_account->CurrMargin;
                owner_->trading_account_snapshot_.frozen_margin = p_trading_account->FrozenMargin;
                owner_->trading_account_snapshot_.frozen_cash = p_trading_account->FrozenCash;
                owner_->trading_account_snapshot_.frozen_commission =
                    p_trading_account->FrozenCommission;
                owner_->trading_account_snapshot_.commission = p_trading_account->Commission;
                owner_->trading_account_snapshot_.close_profit = p_trading_account->CloseProfit;
                owner_->trading_account_snapshot_.position_profit =
                    p_trading_account->PositionProfit;
                owner_->trading_account_snapshot_.trading_day =
                    SafeCtpString(p_trading_account->TradingDay);
                owner_->trading_account_snapshot_.ts_ns = NowEpochNanos();
                owner_->trading_account_snapshot_.source = "ctp";
            }

            if (!owner_->trading_account_snapshot_.account_id.empty()) {
                snapshot = owner_->trading_account_snapshot_;
                has_snapshot = true;
            }
            if (b_is_last) {
                callback = owner_->trading_account_snapshot_callback_;
            }
        }
        if (b_is_last && has_snapshot && callback) {
            callback(snapshot);
        }
    }

    void OnRspQryInvestorPosition(CThostFtdcInvestorPositionField* p_investor_position,
                                  CThostFtdcRspInfoField* p_rsp_info, int,
                                  bool b_is_last) override {
        if (b_is_last) {
            owner_->CompleteScheduledQuery();
        }
        if (!IsRspSuccess(p_rsp_info)) {
            return;
        }

        std::function<void(const std::vector<InvestorPositionSnapshot>&)> callback;
        std::vector<InvestorPositionSnapshot> snapshots;
        {
            std::lock_guard<std::mutex> lock(owner_->mutex_);
            if (p_investor_position != nullptr) {
                InvestorPositionSnapshot snapshot;
                snapshot.account_id = owner_->runtime_config_.user_id;
                snapshot.investor_id = owner_->runtime_config_.investor_id;
                snapshot.instrument_id = SafeCtpString(p_investor_position->InstrumentID);
                snapshot.exchange_id = SafeCtpString(p_investor_position->ExchangeID);
                snapshot.posi_direction = std::string(1, p_investor_position->PosiDirection);
                snapshot.hedge_flag = std::string(1, p_investor_position->HedgeFlag);
                snapshot.position_date = std::string(1, p_investor_position->PositionDate);
                snapshot.position = p_investor_position->Position;
                snapshot.today_position = p_investor_position->TodayPosition;
                snapshot.yd_position = p_investor_position->YdPosition;
                snapshot.long_frozen = p_investor_position->LongFrozen;
                snapshot.short_frozen = p_investor_position->ShortFrozen;
                snapshot.open_volume = p_investor_position->OpenVolume;
                snapshot.close_volume = p_investor_position->CloseVolume;
                snapshot.position_cost = p_investor_position->PositionCost;
                snapshot.open_cost = p_investor_position->OpenCost;
                snapshot.position_profit = p_investor_position->PositionProfit;
                snapshot.close_profit = p_investor_position->CloseProfit;
                snapshot.margin_rate_by_money = p_investor_position->MarginRateByMoney;
                snapshot.margin_rate_by_volume = p_investor_position->MarginRateByVolume;
                snapshot.use_margin = p_investor_position->UseMargin;
                snapshot.ts_ns = NowEpochNanos();
                snapshot.source = "ctp";
                owner_->investor_position_snapshots_.push_back(std::move(snapshot));
            }
            if (b_is_last) {
                snapshots = owner_->investor_position_snapshots_;
                callback = owner_->investor_position_snapshot_callback_;
            }
        }
        if (b_is_last && callback) {
            callback(snapshots);
        }
    }

    void OnRspQryInstrument(CThostFtdcInstrumentField* p_instrument,
                            CThostFtdcRspInfoField* p_rsp_info, int, bool b_is_last) override {
        if (b_is_last) {
            owner_->CompleteScheduledQuery();
        }
        if (!IsRspSuccess(p_rsp_info)) {
            return;
        }

        std::function<void(const std::vector<InstrumentMetaSnapshot>&)> callback;
        std::vector<InstrumentMetaSnapshot> snapshots;
        {
            std::lock_guard<std::mutex> lock(owner_->mutex_);
            if (p_instrument != nullptr) {
                InstrumentMetaSnapshot meta;
                meta.instrument_id = SafeCtpString(p_instrument->InstrumentID);
                meta.exchange_id = SafeCtpString(p_instrument->ExchangeID);
                meta.product_id = SafeCtpString(p_instrument->ProductID);
                meta.volume_multiple = p_instrument->VolumeMultiple;
                meta.price_tick = p_instrument->PriceTick;
                meta.max_margin_side_algorithm = p_instrument->MaxMarginSideAlgorithm != 0 &&
                                                 p_instrument->MaxMarginSideAlgorithm != '0';
                meta.ts_ns = NowEpochNanos();
                meta.source = "ctp";
                owner_->instrument_meta_snapshots_.erase(
                    std::remove_if(
                        owner_->instrument_meta_snapshots_.begin(),
                        owner_->instrument_meta_snapshots_.end(),
                        [&](const auto& row) { return row.instrument_id == meta.instrument_id; }),
                    owner_->instrument_meta_snapshots_.end());
                owner_->instrument_meta_snapshots_.push_back(std::move(meta));
            }
            if (b_is_last) {
                snapshots = owner_->instrument_meta_snapshots_;
                callback = owner_->instrument_meta_snapshot_callback_;
            }
        }
        if (b_is_last && callback) {
            callback(snapshots);
        }
    }

    void OnRspQryBrokerTradingParams(CThostFtdcBrokerTradingParamsField* p_broker_trading_params,
                                     CThostFtdcRspInfoField* p_rsp_info, int,
                                     bool b_is_last) override {
        if (b_is_last) {
            owner_->CompleteScheduledQuery();
        }
        if (!b_is_last || !IsRspSuccess(p_rsp_info) || p_broker_trading_params == nullptr) {
            return;
        }

        BrokerTradingParamsSnapshot snapshot;
        std::function<void(const BrokerTradingParamsSnapshot&)> callback;
        {
            std::lock_guard<std::mutex> lock(owner_->mutex_);
            owner_->broker_trading_params_snapshot_.account_id = owner_->runtime_config_.user_id;
            owner_->broker_trading_params_snapshot_.investor_id =
                owner_->runtime_config_.investor_id;
            owner_->broker_trading_params_snapshot_.margin_price_type =
                std::string(1, p_broker_trading_params->MarginPriceType);
            owner_->broker_trading_params_snapshot_.algorithm = "";
            owner_->broker_trading_params_snapshot_.ts_ns = NowEpochNanos();
            owner_->broker_trading_params_snapshot_.source = "ctp";
            snapshot = owner_->broker_trading_params_snapshot_;
            callback = owner_->broker_trading_params_snapshot_callback_;
        }
        if (callback) {
            callback(snapshot);
        }
    }

    void OnRspQryInstrumentMarginRate(CThostFtdcInstrumentMarginRateField* p_margin_rate,
                                      CThostFtdcRspInfoField* p_rsp_info, int,
                                      bool b_is_last) override {
        if (b_is_last) {
            owner_->CompleteScheduledQuery();
        }
        if (!IsRspSuccess(p_rsp_info)) {
            if (IsRecoverableQueryError(p_rsp_info)) {
                return;
            }
            return;
        }
        std::vector<InstrumentMarginRateSnapshot> snapshots;
        CtpGatewayAdapter::InstrumentMarginRateSnapshotCallback callback;
        {
            std::lock_guard<std::mutex> lock(owner_->mutex_);
            if (p_margin_rate != nullptr) {
                InstrumentMarginRateSnapshot snapshot;
                snapshot.account_id = SafeCtpString(p_margin_rate->InvestorID);
                snapshot.investor_id = SafeCtpString(p_margin_rate->InvestorID);
                snapshot.instrument_id = SafeCtpString(p_margin_rate->InstrumentID);
                snapshot.exchange_id = SafeCtpString(p_margin_rate->ExchangeID);
                snapshot.hedge_flag = std::string(1, p_margin_rate->HedgeFlag);
                snapshot.long_margin_ratio_by_money = p_margin_rate->LongMarginRatioByMoney;
                snapshot.long_margin_ratio_by_volume = p_margin_rate->LongMarginRatioByVolume;
                snapshot.short_margin_ratio_by_money = p_margin_rate->ShortMarginRatioByMoney;
                snapshot.short_margin_ratio_by_volume = p_margin_rate->ShortMarginRatioByVolume;
                snapshot.is_relative =
                    p_margin_rate->IsRelative != 0 && p_margin_rate->IsRelative != '0';
                snapshot.ts_ns = NowEpochNanos();
                snapshot.source = "ctp";
                auto& rows = owner_->instrument_margin_rate_snapshots_;
                rows.erase(std::remove_if(rows.begin(), rows.end(),
                                          [&](const auto& row) {
                                              return row.account_id == snapshot.account_id &&
                                                     row.instrument_id == snapshot.instrument_id &&
                                                     row.exchange_id == snapshot.exchange_id &&
                                                     row.hedge_flag == snapshot.hedge_flag;
                                          }),
                           rows.end());
                rows.push_back(std::move(snapshot));
            }
            if (b_is_last) {
                snapshots = owner_->instrument_margin_rate_snapshots_;
                callback = owner_->instrument_margin_rate_snapshot_callback_;
            }
        }
        if (b_is_last && callback) {
            callback(snapshots);
        }
    }

    void OnRspQryInstrumentCommissionRate(
        CThostFtdcInstrumentCommissionRateField* p_commission_rate,
        CThostFtdcRspInfoField* p_rsp_info, int, bool b_is_last) override {
        if (b_is_last) {
            owner_->CompleteScheduledQuery();
        }
        if (!IsRspSuccess(p_rsp_info)) {
            if (IsRecoverableQueryError(p_rsp_info)) {
                return;
            }
            return;
        }
        std::vector<InstrumentCommissionRateSnapshot> snapshots;
        CtpGatewayAdapter::InstrumentCommissionRateSnapshotCallback callback;
        {
            std::lock_guard<std::mutex> lock(owner_->mutex_);
            if (p_commission_rate != nullptr) {
                InstrumentCommissionRateSnapshot snapshot;
                snapshot.account_id = SafeCtpString(p_commission_rate->InvestorID);
                snapshot.investor_id = SafeCtpString(p_commission_rate->InvestorID);
                snapshot.instrument_id = SafeCtpString(p_commission_rate->InstrumentID);
                snapshot.exchange_id = SafeCtpString(p_commission_rate->ExchangeID);
                snapshot.open_ratio_by_money = p_commission_rate->OpenRatioByMoney;
                snapshot.open_ratio_by_volume = p_commission_rate->OpenRatioByVolume;
                snapshot.close_ratio_by_money = p_commission_rate->CloseRatioByMoney;
                snapshot.close_ratio_by_volume = p_commission_rate->CloseRatioByVolume;
                snapshot.close_today_ratio_by_money = p_commission_rate->CloseTodayRatioByMoney;
                snapshot.close_today_ratio_by_volume = p_commission_rate->CloseTodayRatioByVolume;
                snapshot.ts_ns = NowEpochNanos();
                snapshot.source = "ctp";
                auto& rows = owner_->instrument_commission_rate_snapshots_;
                rows.erase(std::remove_if(rows.begin(), rows.end(),
                                          [&](const auto& row) {
                                              return row.account_id == snapshot.account_id &&
                                                     row.instrument_id == snapshot.instrument_id &&
                                                     row.exchange_id == snapshot.exchange_id;
                                          }),
                           rows.end());
                rows.push_back(std::move(snapshot));
            }
            if (b_is_last) {
                snapshots = owner_->instrument_commission_rate_snapshots_;
                callback = owner_->instrument_commission_rate_snapshot_callback_;
            }
        }
        if (b_is_last && callback) {
            callback(snapshots);
        }
    }

    void OnRspQryInstrumentOrderCommRate(CThostFtdcInstrumentOrderCommRateField* p_order_comm_rate,
                                         CThostFtdcRspInfoField* p_rsp_info, int,
                                         bool b_is_last) override {
        if (b_is_last) {
            owner_->CompleteScheduledQuery();
        }
        if (!IsRspSuccess(p_rsp_info)) {
            if (IsRecoverableQueryError(p_rsp_info)) {
                return;
            }
            return;
        }
        std::vector<InstrumentOrderCommRateSnapshot> snapshots;
        CtpGatewayAdapter::InstrumentOrderCommRateSnapshotCallback callback;
        {
            std::lock_guard<std::mutex> lock(owner_->mutex_);
            if (p_order_comm_rate != nullptr) {
                InstrumentOrderCommRateSnapshot snapshot;
                snapshot.account_id = SafeCtpString(p_order_comm_rate->InvestorID);
                snapshot.investor_id = SafeCtpString(p_order_comm_rate->InvestorID);
                snapshot.instrument_id = SafeCtpString(p_order_comm_rate->InstrumentID);
                snapshot.exchange_id = SafeCtpString(p_order_comm_rate->ExchangeID);
                snapshot.hedge_flag = std::string(1, p_order_comm_rate->HedgeFlag);
                snapshot.order_comm_by_volume = p_order_comm_rate->OrderCommByVolume;
                snapshot.order_action_comm_by_volume = p_order_comm_rate->OrderActionCommByVolume;
                snapshot.ts_ns = NowEpochNanos();
                snapshot.source = "ctp";
                auto& rows = owner_->instrument_order_comm_rate_snapshots_;
                rows.erase(std::remove_if(rows.begin(), rows.end(),
                                          [&](const auto& row) {
                                              return row.account_id == snapshot.account_id &&
                                                     row.instrument_id == snapshot.instrument_id &&
                                                     row.exchange_id == snapshot.exchange_id &&
                                                     row.hedge_flag == snapshot.hedge_flag;
                                          }),
                           rows.end());
                rows.push_back(std::move(snapshot));
            }
            if (b_is_last) {
                snapshots = owner_->instrument_order_comm_rate_snapshots_;
                callback = owner_->instrument_order_comm_rate_snapshot_callback_;
            }
        }
        if (b_is_last && callback) {
            callback(snapshots);
        }
    }

    void OnRspQryOrder(CThostFtdcOrderField* p_order, CThostFtdcRspInfoField* p_rsp_info,
                       int n_request_id, bool b_is_last) override {
        if (b_is_last) {
            owner_->CompleteScheduledQuery();
        }
        const bool success = IsRspSuccess(p_rsp_info);
        if (b_is_last) {
            CtpGatewayAdapter::QueryCompleteCallback query_callback;
            {
                std::lock_guard<std::mutex> lock(owner_->mutex_);
                query_callback = owner_->query_complete_callback_;
            }
            if (query_callback) {
                query_callback(n_request_id, "order", success);
            }
        }
        if (!success || p_order == nullptr) {
            return;
        }

        OrderEvent event;
        event.account_id = SafeCtpString(p_order->InvestorID);
        event.exchange_order_id = SafeCtpString(p_order->OrderSysID);
        event.instrument_id = SafeCtpString(p_order->InstrumentID);
        event.exchange_id = SafeCtpString(p_order->ExchangeID);
        event.side = FromCtpDirection(p_order->Direction);
        event.offset = FromCtpOffset(p_order->CombOffsetFlag[0]);
        event.status = FromCtpOrderStatus(p_order->OrderStatus);
        event.total_volume = p_order->VolumeTotalOriginal;
        event.filled_volume = p_order->VolumeTraded;
        event.avg_fill_price = p_order->LimitPrice;
        event.reason = SafeCtpString(p_order->StatusMsg);
        event.status_msg = SafeCtpString(p_order->StatusMsg);
        event.order_submit_status = std::string(1, p_order->OrderSubmitStatus);
        event.order_ref = SafeCtpString(p_order->OrderRef);
        event.front_id = p_order->FrontID;
        event.session_id = p_order->SessionID;
        event.event_source = "OnRspQryOrder";
        event.ts_ns = NowEpochNanos();
        event.exchange_ts_ns = ParseCtpDateTimeToEpochNanos(SafeCtpString(p_order->InsertDate),
                                                            SafeCtpString(p_order->InsertTime));

        std::function<void(const OrderEvent&)> callback;
        {
            std::lock_guard<std::mutex> lock(owner_->mutex_);
            const auto order_ref = SafeCtpString(p_order->OrderRef);
            const auto it = owner_->order_ref_to_client_id_.find(order_ref);
            if (it != owner_->order_ref_to_client_id_.end()) {
                event.client_order_id = it->second;
            } else {
                event.client_order_id = order_ref;
            }
            const auto meta_it = owner_->client_order_meta_.find(event.client_order_id);
            if (meta_it != owner_->client_order_meta_.end()) {
                event.strategy_id = meta_it->second.strategy_id;
            }
            callback = owner_->order_event_callback_;
        }
        if (callback) {
            StampOrderEventTimestamps(&event);
            callback(event);
        }
    }

    void OnRspQryTrade(CThostFtdcTradeField* p_trade, CThostFtdcRspInfoField* p_rsp_info,
                       int n_request_id, bool b_is_last) override {
        if (b_is_last) {
            owner_->CompleteScheduledQuery();
        }
        const bool success = IsRspSuccess(p_rsp_info);
        if (b_is_last) {
            CtpGatewayAdapter::QueryCompleteCallback query_callback;
            {
                std::lock_guard<std::mutex> lock(owner_->mutex_);
                query_callback = owner_->query_complete_callback_;
            }
            if (query_callback) {
                query_callback(n_request_id, "trade", success);
            }
        }
        if (!success || p_trade == nullptr) {
            return;
        }

        OrderEvent event;
        event.account_id = SafeCtpString(p_trade->InvestorID);
        event.exchange_order_id = SafeCtpString(p_trade->OrderSysID);
        event.instrument_id = SafeCtpString(p_trade->InstrumentID);
        event.exchange_id = SafeCtpString(p_trade->ExchangeID);
        event.side = FromCtpDirection(p_trade->Direction);
        event.offset = FromCtpOffset(p_trade->OffsetFlag);
        event.status = OrderStatus::kFilled;
        event.last_trade_volume = p_trade->Volume;
        event.total_volume = p_trade->Volume;
        event.filled_volume = p_trade->Volume;
        event.avg_fill_price = p_trade->Price;
        event.reason = "trade_query";
        event.order_ref = SafeCtpString(p_trade->OrderRef);
        event.trade_id =
            BuildCtpTradeId(event.exchange_id, event.side, SafeCtpString(p_trade->TradeID));
        event.event_source = "OnRspQryTrade";
        event.ts_ns = NowEpochNanos();
        event.exchange_ts_ns = ParseCtpDateTimeToEpochNanos(SafeCtpString(p_trade->TradeDate),
                                                            SafeCtpString(p_trade->TradeTime));

        std::function<void(const OrderEvent&)> callback;
        {
            std::lock_guard<std::mutex> lock(owner_->mutex_);
            const auto order_ref = SafeCtpString(p_trade->OrderRef);
            const auto it = owner_->order_ref_to_client_id_.find(order_ref);
            event.client_order_id =
                it == owner_->order_ref_to_client_id_.end() ? order_ref : it->second;
            const auto meta_it = owner_->client_order_meta_.find(event.client_order_id);
            if (meta_it != owner_->client_order_meta_.end()) {
                auto& meta = meta_it->second;
                event.strategy_id = meta.strategy_id;
                meta.cumulative_filled_volume += std::max(0, p_trade->Volume);
                event.total_volume = meta.total_volume > 0 ? meta.total_volume : p_trade->Volume;
                event.filled_volume = meta.cumulative_filled_volume;
                event.status = event.filled_volume >= event.total_volume
                                   ? OrderStatus::kFilled
                                   : OrderStatus::kPartiallyFilled;
            }
            callback = owner_->order_event_callback_;
        }
        if (callback) {
            StampOrderEventTimestamps(&event);
            callback(event);
        }
    }

    void OnRspOrderInsert(CThostFtdcInputOrderField* p_input_order,
                          CThostFtdcRspInfoField* p_rsp_info, int, bool b_is_last) override {
        if (!b_is_last || IsRspSuccess(p_rsp_info)) {
            return;
        }

        OrderEvent event;
        event.account_id = p_input_order == nullptr ? owner_->runtime_config_.investor_id
                                                    : SafeCtpString(p_input_order->InvestorID);
        event.instrument_id =
            p_input_order == nullptr ? "" : SafeCtpString(p_input_order->InstrumentID);
        event.exchange_id =
            p_input_order == nullptr ? "" : SafeCtpString(p_input_order->ExchangeID);
        event.side =
            p_input_order == nullptr ? Side::kBuy : FromCtpDirection(p_input_order->Direction);
        event.offset = p_input_order == nullptr ? OffsetFlag::kOpen
                                                : FromCtpOffset(p_input_order->CombOffsetFlag[0]);
        event.status = OrderStatus::kRejected;
        event.total_volume = p_input_order == nullptr ? 0 : p_input_order->VolumeTotalOriginal;
        event.filled_volume = 0;
        event.avg_fill_price = p_input_order == nullptr ? 0.0 : p_input_order->LimitPrice;
        event.reason = FormatRspError("order_insert_rejected", p_rsp_info);
        event.status_msg = SafeCtpString(p_rsp_info->ErrorMsg);
        event.order_ref = p_input_order == nullptr ? "" : SafeCtpString(p_input_order->OrderRef);
        event.front_id = owner_->front_id_;
        event.session_id = owner_->session_id_;
        event.event_source = "OnRspOrderInsert";
        event.ts_ns = NowEpochNanos();
        EmitOrderEvent(std::move(event), true);
    }

    void OnErrRtnOrderInsert(CThostFtdcInputOrderField* p_input_order,
                             CThostFtdcRspInfoField* p_rsp_info) override {
        if (IsRspSuccess(p_rsp_info)) {
            return;
        }

        OrderEvent event;
        event.account_id = p_input_order == nullptr ? owner_->runtime_config_.investor_id
                                                    : SafeCtpString(p_input_order->InvestorID);
        event.instrument_id =
            p_input_order == nullptr ? "" : SafeCtpString(p_input_order->InstrumentID);
        event.exchange_id =
            p_input_order == nullptr ? "" : SafeCtpString(p_input_order->ExchangeID);
        event.side =
            p_input_order == nullptr ? Side::kBuy : FromCtpDirection(p_input_order->Direction);
        event.offset = p_input_order == nullptr ? OffsetFlag::kOpen
                                                : FromCtpOffset(p_input_order->CombOffsetFlag[0]);
        event.status = OrderStatus::kRejected;
        event.total_volume = p_input_order == nullptr ? 0 : p_input_order->VolumeTotalOriginal;
        event.filled_volume = 0;
        event.avg_fill_price = p_input_order == nullptr ? 0.0 : p_input_order->LimitPrice;
        event.reason = FormatRspError("order_insert_error", p_rsp_info);
        event.status_msg = SafeCtpString(p_rsp_info->ErrorMsg);
        event.order_ref = p_input_order == nullptr ? "" : SafeCtpString(p_input_order->OrderRef);
        event.front_id = owner_->front_id_;
        event.session_id = owner_->session_id_;
        event.event_source = "OnErrRtnOrderInsert";
        event.ts_ns = NowEpochNanos();
        EmitOrderEvent(std::move(event), true);
    }

    void OnRspOrderAction(CThostFtdcInputOrderActionField* p_input_order_action,
                          CThostFtdcRspInfoField* p_rsp_info, int, bool b_is_last) override {
        if (!b_is_last) {
            return;
        }

        OrderEvent event;
        event.account_id = owner_->runtime_config_.investor_id;
        event.instrument_id = p_input_order_action == nullptr
                                  ? ""
                                  : SafeCtpString(p_input_order_action->InstrumentID);
        event.exchange_id =
            p_input_order_action == nullptr ? "" : SafeCtpString(p_input_order_action->ExchangeID);
        event.status = IsRspSuccess(p_rsp_info) ? OrderStatus::kAccepted : OrderStatus::kRejected;
        event.order_ref =
            p_input_order_action == nullptr ? "" : SafeCtpString(p_input_order_action->OrderRef);
        event.front_id =
            p_input_order_action == nullptr ? owner_->front_id_ : p_input_order_action->FrontID;
        event.session_id =
            p_input_order_action == nullptr ? owner_->session_id_ : p_input_order_action->SessionID;
        event.reason = IsRspSuccess(p_rsp_info)
                           ? "cancel_request_accepted"
                           : FormatRspError("cancel_request_rejected", p_rsp_info);
        event.status_msg = p_rsp_info == nullptr ? "" : SafeCtpString(p_rsp_info->ErrorMsg);
        event.event_source = "OnRspOrderAction";
        event.ts_ns = NowEpochNanos();
        EmitOrderEvent(std::move(event), false);
    }

    void OnErrRtnOrderAction(CThostFtdcOrderActionField* p_order_action,
                             CThostFtdcRspInfoField* p_rsp_info) override {
        if (IsRspSuccess(p_rsp_info)) {
            return;
        }

        OrderEvent event;
        event.account_id = owner_->runtime_config_.investor_id;
        event.instrument_id =
            p_order_action == nullptr ? "" : SafeCtpString(p_order_action->InstrumentID);
        event.exchange_id =
            p_order_action == nullptr ? "" : SafeCtpString(p_order_action->ExchangeID);
        event.status = OrderStatus::kRejected;
        event.order_ref = p_order_action == nullptr ? "" : SafeCtpString(p_order_action->OrderRef);
        event.front_id = p_order_action == nullptr ? owner_->front_id_ : p_order_action->FrontID;
        event.session_id =
            p_order_action == nullptr ? owner_->session_id_ : p_order_action->SessionID;
        event.reason = FormatRspError("cancel_error", p_rsp_info);
        event.status_msg = SafeCtpString(p_rsp_info->ErrorMsg);
        event.event_source = "OnErrRtnOrderAction";
        event.ts_ns = NowEpochNanos();
        EmitOrderEvent(std::move(event), false);
    }

    void OnRtnOrder(CThostFtdcOrderField* p_order) override {
        if (p_order == nullptr) {
            return;
        }

        OrderEvent event;
        event.account_id = SafeCtpString(p_order->InvestorID);
        event.exchange_order_id = SafeCtpString(p_order->OrderSysID);
        event.instrument_id = SafeCtpString(p_order->InstrumentID);
        event.exchange_id = SafeCtpString(p_order->ExchangeID);
        event.side = FromCtpDirection(p_order->Direction);
        event.offset = FromCtpOffset(p_order->CombOffsetFlag[0]);
        event.status = FromCtpOrderStatus(p_order->OrderStatus);
        event.total_volume = p_order->VolumeTotalOriginal;
        event.filled_volume = p_order->VolumeTraded;
        event.avg_fill_price = p_order->LimitPrice;
        event.reason = SafeCtpString(p_order->StatusMsg);
        event.status_msg = SafeCtpString(p_order->StatusMsg);
        event.order_submit_status = std::string(1, p_order->OrderSubmitStatus);
        event.order_ref = SafeCtpString(p_order->OrderRef);
        event.front_id = p_order->FrontID;
        event.session_id = p_order->SessionID;
        event.event_source = "OnRtnOrder";
        event.ts_ns = NowEpochNanos();

        std::function<void(const OrderEvent&)> callback;
        {
            std::lock_guard<std::mutex> lock(owner_->mutex_);
            const auto order_ref = SafeCtpString(p_order->OrderRef);
            const auto it = owner_->order_ref_to_client_id_.find(order_ref);
            if (it != owner_->order_ref_to_client_id_.end()) {
                event.client_order_id = it->second;
                const auto meta_it = owner_->client_order_meta_.find(it->second);
                if (meta_it != owner_->client_order_meta_.end()) {
                    event.strategy_id = meta_it->second.strategy_id;
                }
                if (event.status == OrderStatus::kCanceled ||
                    event.status == OrderStatus::kRejected) {
                    owner_->client_order_meta_.erase(it->second);
                    owner_->order_ref_to_client_id_.erase(it);
                }
            } else {
                event.client_order_id = order_ref;
            }
            callback = owner_->order_event_callback_;
        }

        if (callback) {
            StampOrderEventTimestamps(&event);
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
        event.exchange_id = SafeCtpString(p_trade->ExchangeID);
        event.side = FromCtpDirection(p_trade->Direction);
        event.offset = FromCtpOffset(p_trade->OffsetFlag);
        event.status = OrderStatus::kFilled;
        event.last_trade_volume = p_trade->Volume;
        event.total_volume = p_trade->Volume;
        event.filled_volume = p_trade->Volume;
        event.avg_fill_price = p_trade->Price;
        event.reason = "trade";
        event.order_ref = SafeCtpString(p_trade->OrderRef);
        event.trade_id =
            BuildCtpTradeId(event.exchange_id, event.side, SafeCtpString(p_trade->TradeID));
        event.event_source = "OnRtnTrade";
        event.ts_ns = NowEpochNanos();

        std::function<void(const OrderEvent&)> callback;
        {
            std::lock_guard<std::mutex> lock(owner_->mutex_);
            const auto order_ref = SafeCtpString(p_trade->OrderRef);
            const auto it = owner_->order_ref_to_client_id_.find(order_ref);
            event.client_order_id =
                it == owner_->order_ref_to_client_id_.end() ? order_ref : it->second;
            const auto meta_it = owner_->client_order_meta_.find(event.client_order_id);
            if (meta_it != owner_->client_order_meta_.end()) {
                auto& meta = meta_it->second;
                event.strategy_id = meta.strategy_id;
                meta.cumulative_filled_volume += std::max(0, p_trade->Volume);
                event.total_volume = meta.total_volume > 0 ? meta.total_volume : p_trade->Volume;
                event.filled_volume = meta.cumulative_filled_volume;
                event.status = event.filled_volume >= event.total_volume
                                   ? OrderStatus::kFilled
                                   : OrderStatus::kPartiallyFilled;
                if (event.status == OrderStatus::kFilled) {
                    owner_->order_ref_to_client_id_.erase(meta.order_ref);
                    owner_->client_order_meta_.erase(event.client_order_id);
                }
            }
            callback = owner_->order_event_callback_;
        }

        if (callback) {
            StampOrderEventTimestamps(&event);
            callback(event);
        }
    }

    void OnRspError(CThostFtdcRspInfoField* p_rsp_info, int, bool) override {
        if (!IsRspSuccess(p_rsp_info)) {
            if (IsRecoverableQueryError(p_rsp_info)) {
                return;
            }
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

    void EmitOrderEvent(OrderEvent event, bool erase_terminal_mapping) {
        std::function<void(const OrderEvent&)> callback;
        {
            std::lock_guard<std::mutex> lock(owner_->mutex_);
            if (event.account_id.empty()) {
                event.account_id = owner_->runtime_config_.investor_id;
            }
            if (event.client_order_id.empty() && !event.order_ref.empty()) {
                const auto it = owner_->order_ref_to_client_id_.find(event.order_ref);
                if (it != owner_->order_ref_to_client_id_.end()) {
                    event.client_order_id = it->second;
                }
            }
            if (event.client_order_id.empty()) {
                event.client_order_id = event.order_ref;
            }
            const auto meta_it = owner_->client_order_meta_.find(event.client_order_id);
            if (meta_it != owner_->client_order_meta_.end()) {
                event.strategy_id = meta_it->second.strategy_id;
            }
            if (erase_terminal_mapping && !event.client_order_id.empty()) {
                owner_->client_order_meta_.erase(event.client_order_id);
                if (!event.order_ref.empty()) {
                    owner_->order_ref_to_client_id_.erase(event.order_ref);
                }
            }
            callback = owner_->order_event_callback_;
        }
        if (callback) {
            StampOrderEventTimestamps(&event);
            callback(event);
        }
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

void CtpGatewayAdapter::NormalizeMarketSnapshot(MarketSnapshot* snapshot) {
    if (snapshot == nullptr) {
        return;
    }

    if (snapshot->exchange_id.empty()) {
        snapshot->exchange_id = InferExchangeIdFromInstrument(snapshot->instrument_id);
    }
    if (snapshot->trading_day.empty()) {
        snapshot->trading_day = snapshot->action_day;
    }
    if (snapshot->action_day.empty()) {
        snapshot->action_day = snapshot->trading_day;
    }
    if (snapshot->update_millisec < 0) {
        snapshot->update_millisec = 0;
    }

    if (IsInvalidMarketPrice(snapshot->settlement_price) || snapshot->settlement_price <= 0.0) {
        snapshot->settlement_price = 0.0;
        snapshot->is_valid_settlement = false;
    } else {
        snapshot->is_valid_settlement = true;
    }

    if (IsInvalidMarketPrice(snapshot->average_price_raw) || snapshot->average_price_raw <= 0.0) {
        snapshot->average_price_norm = 0.0;
    } else {
        snapshot->average_price_norm = snapshot->average_price_raw;
    }
}

bool CtpGatewayAdapter::Connect(const MarketDataConnectConfig& config) {
    CtpRuntimeConfig runtime;
    runtime.environment =
        config.is_production_mode ? CtpEnvironment::kProduction : CtpEnvironment::kSimNow;
    runtime.is_production_mode = config.is_production_mode;
    runtime.enable_real_api = config.enable_real_api;
    runtime.enable_terminal_auth = config.enable_terminal_auth;
    runtime.connect_timeout_ms = config.connect_timeout_ms;
    runtime.reconnect_max_attempts = config.reconnect_max_attempts;
    runtime.reconnect_initial_backoff_ms = config.reconnect_initial_backoff_ms;
    runtime.reconnect_max_backoff_ms = config.reconnect_max_backoff_ms;
    runtime.query_retry_backoff_ms = config.query_retry_backoff_ms;
    runtime.recovery_quiet_period_ms = config.recovery_quiet_period_ms;
    runtime.settlement_confirm_required = config.settlement_confirm_required;
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
    ConnectionStateCallback callback;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        connected_ = true;
        healthy_ = true;
        last_connect_diagnostic_.clear();
        callback = connection_state_callback_;
    }
    if (callback) {
        callback(true);
    }
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
        failures.push_back("md=" + candidate.md_front + " td=" + candidate.td_front + " => " +
                           failure_detail);
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

    state->md_api = CreateMdApiCompat(flow_path, runtime.is_production_mode);
    state->td_api = CreateTraderApiCompat(flow_path, runtime.is_production_mode);

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

    const int timeout_ms =
        runtime.connect_timeout_ms > 0 ? runtime.connect_timeout_ms : kDefaultConnectTimeoutMs;

    bool ready = false;
    {
        std::unique_lock<std::mutex> lock(state_ptr->event_mutex);
        ready =
            state_ptr->event_cv.wait_for(lock, std::chrono::milliseconds(timeout_ms), [state_ptr] {
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
                *failure_detail = "login not complete (md_logged_in=" +
                                  std::string(md_logged_in ? "true" : "false") +
                                  ", td_logged_in=" + std::string(td_logged_in ? "true" : "false") +
                                  ")";
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
    ConnectionStateCallback callback;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (!connected_) {
            return;
        }
        healthy_ = false;
        callback = connection_state_callback_;
    }
    if (callback) {
        callback(false);
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
            reconnect_cv_.wait(lock, [this]() { return reconnect_stop_ || reconnect_requested_; });
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
            CtpReconnectCounter()->Increment();
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
    ConnectionStateCallback callback;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (!real_api_) {
            return;
        }
        std::lock_guard<std::mutex> event_lock(real_api_->event_mutex);
        healthy =
            real_api_->md_logged_in && real_api_->td_logged_in && real_api_->last_error.empty();
        if (healthy) {
            healthy_ = true;
            connected_ = true;
            callback = connection_state_callback_;
        }
    }
    if (healthy && callback) {
        callback(true);
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

    ConnectionStateCallback callback;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        connected_ = false;
        healthy_ = false;
        subscriptions_.clear();
        client_order_meta_.clear();
        order_ref_to_client_id_.clear();
        user_session_ = {};
        trading_account_snapshot_ = {};
        investor_position_snapshots_.clear();
        instrument_meta_snapshots_.clear();
        broker_trading_params_snapshot_ = {};
        instrument_margin_rate_snapshots_.clear();
        instrument_commission_rate_snapshots_.clear();
        instrument_order_comm_rate_snapshots_.clear();
        reconnect_requested_ = false;
        reconnect_in_progress_ = false;
        callback = connection_state_callback_;
    }
    if (callback) {
        callback(false);
    }
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
        if (!ctp_ids.empty() &&
            md_api->SubscribeMarketData(ctp_ids.data(), static_cast<int>(ctp_ids.size())) != 0) {
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
        if (!ctp_ids.empty() &&
            md_api->UnSubscribeMarketData(ctp_ids.data(), static_cast<int>(ctp_ids.size())) != 0) {
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
            meta.strategy_id = intent.strategy_id;
            meta.instrument_id = intent.instrument_id;
            meta.side = intent.side;
            meta.offset = intent.offset;
            meta.front_id = front_id_;
            meta.session_id = session_id_;
            meta.total_volume = intent.volume;
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
        simulated_event.strategy_id = intent.strategy_id;
        simulated_event.client_order_id = intent.client_order_id;
        simulated_event.exchange_order_id = "ctp-sim-" + intent.client_order_id;
        simulated_event.instrument_id = intent.instrument_id;
        simulated_event.side = intent.side;
        simulated_event.offset = intent.offset;
        simulated_event.status = OrderStatus::kAccepted;
        simulated_event.order_ref = intent.client_order_id;
        simulated_event.event_source = "simulated_place_order";
        simulated_event.total_volume = intent.volume;
        simulated_event.ts_ns = NowEpochNanos();
        simulated_event.trace_id = intent.trace_id;

        OrderMeta meta;
        meta.order_ref = intent.client_order_id;
        meta.strategy_id = intent.strategy_id;
        meta.instrument_id = intent.instrument_id;
        meta.side = intent.side;
        meta.offset = intent.offset;
        meta.front_id = front_id_;
        meta.session_id = session_id_;
        meta.total_volume = intent.volume;
        client_order_meta_[intent.client_order_id] = meta;
        order_ref_to_client_id_[meta.order_ref] = intent.client_order_id;
        emit_simulated_event = true;
    }

    if (emit_simulated_event && callback) {
        StampOrderEventTimestamps(&simulated_event);
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
        simulated_event.order_ref = client_order_id;
        simulated_event.event_source = "simulated_cancel_order";
        simulated_event.ts_ns = NowEpochNanos();
        simulated_event.trace_id = trace_id;
        const auto it = client_order_meta_.find(client_order_id);
        if (it != client_order_meta_.end()) {
            simulated_event.strategy_id = it->second.strategy_id;
            simulated_event.instrument_id = it->second.instrument_id;
            simulated_event.side = it->second.side;
            simulated_event.offset = it->second.offset;
            simulated_event.order_ref = it->second.order_ref;
            order_ref_to_client_id_.erase(it->second.order_ref);
            client_order_meta_.erase(it);
        }
        emit_simulated_event = true;
    }

    if (emit_simulated_event && callback) {
        StampOrderEventTimestamps(&simulated_event);
        callback(simulated_event);
    }

    return true;
}

void CtpGatewayAdapter::RegisterOrderEventCallback(OrderEventCallback callback) {
    std::lock_guard<std::mutex> lock(mutex_);
    order_event_callback_ = std::move(callback);
}

void CtpGatewayAdapter::RegisterConnectionStateCallback(ConnectionStateCallback callback) {
    std::lock_guard<std::mutex> lock(mutex_);
    connection_state_callback_ = std::move(callback);
}

void CtpGatewayAdapter::RegisterLoginResponseCallback(LoginResponseCallback callback) {
    std::lock_guard<std::mutex> lock(mutex_);
    login_response_callback_ = std::move(callback);
}

void CtpGatewayAdapter::RegisterQueryCompleteCallback(QueryCompleteCallback callback) {
    std::lock_guard<std::mutex> lock(mutex_);
    query_complete_callback_ = std::move(callback);
}

void CtpGatewayAdapter::RegisterSettlementConfirmCallback(SettlementConfirmCallback callback) {
    std::lock_guard<std::mutex> lock(mutex_);
    settlement_confirm_callback_ = std::move(callback);
}

bool CtpGatewayAdapter::RequestUserLogin(int request_id, const std::string& broker_id,
                                         const std::string& user_id, const std::string& password) {
    (void)broker_id;
    (void)password;
    LoginResponseCallback callback;
    CtpRuntimeConfig runtime;
    bool use_real = false;
#if QUANT_HFT_HAS_REAL_CTP
    CThostFtdcTraderApi* td_api = nullptr;
#endif
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (!connected_) {
            return false;
        }
        runtime = runtime_config_;
        callback = login_response_callback_;
        use_real = runtime_config_.enable_real_api;
#if QUANT_HFT_HAS_REAL_CTP
        if (use_real && real_api_) {
            td_api = real_api_->td_api;
        }
#endif
    }

    if (!use_real) {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            user_session_.investor_id = runtime.investor_id.empty() ? user_id : runtime.investor_id;
            user_session_.login_time = "09:00:00";
            user_session_.last_login_time = runtime.last_login_time;
            user_session_.reserve_info = runtime.reserve_info;
        }
        if (callback) {
            callback(request_id, 0, "");
        }
        return true;
    }

#if QUANT_HFT_HAS_REAL_CTP
    if (td_api == nullptr) {
        return false;
    }
    CThostFtdcReqUserLoginField login_req{};
    CopyCtpField(login_req.BrokerID, broker_id.empty() ? runtime.broker_id : broker_id);
    CopyCtpField(login_req.UserID, user_id.empty() ? runtime.user_id : user_id);
    CopyCtpField(login_req.Password, password.empty() ? runtime.password : password);
    return td_api->ReqUserLogin(&login_req, request_id) == 0;
#else
    return false;
#endif
}

bool CtpGatewayAdapter::RequestSettlementInfoConfirm(int request_id) {
    SettlementConfirmCallback callback;
    CtpRuntimeConfig runtime;
    bool use_real = false;
#if QUANT_HFT_HAS_REAL_CTP
    CThostFtdcTraderApi* td_api = nullptr;
#endif
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (!connected_) {
            return false;
        }
        runtime = runtime_config_;
        callback = settlement_confirm_callback_;
        use_real = runtime_config_.enable_real_api;
#if QUANT_HFT_HAS_REAL_CTP
        if (use_real && real_api_) {
            td_api = real_api_->td_api;
        }
#endif
    }

    if (!use_real) {
        if (callback) {
            callback(request_id, 0, "");
        }
        return true;
    }

#if QUANT_HFT_HAS_REAL_CTP
    if (td_api == nullptr) {
        return false;
    }
    CThostFtdcSettlementInfoConfirmField req{};
    CopyCtpField(req.BrokerID, runtime.broker_id);
    CopyCtpField(req.InvestorID, runtime.investor_id);
    return td_api->ReqSettlementInfoConfirm(&req, request_id) == 0;
#else
    return false;
#endif
}

bool CtpGatewayAdapter::EnqueueUserSessionQuery(int request_id) {
    (void)request_id;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (!connected_) {
            return false;
        }
        user_session_.investor_id = runtime_config_.investor_id;
        if (!runtime_config_.enable_real_api) {
            user_session_.login_time = "09:00:00";
        }
        user_session_.last_login_time = runtime_config_.last_login_time;
        user_session_.reserve_info = runtime_config_.reserve_info;
    }
    return true;
}

bool CtpGatewayAdapter::EnqueueTradingAccountQuery(int request_id) {
    bool query_ok = true;
    TradingAccountSnapshotCallback callback;
    TradingAccountSnapshot snapshot;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (!connected_) {
            return false;
        }
    }

    QueryScheduler::QueryTask task;
    task.request_id = request_id;
    task.priority = QueryScheduler::Priority::kHigh;
    task.execute = [this, request_id, &query_ok, &callback, &snapshot]() {
        CtpRuntimeConfig runtime;
#if QUANT_HFT_HAS_REAL_CTP
        CThostFtdcTraderApi* td_api = nullptr;
#endif
        {
            std::lock_guard<std::mutex> lock(mutex_);
            runtime = runtime_config_;
            if (runtime_config_.enable_real_api) {
#if QUANT_HFT_HAS_REAL_CTP
                if (!healthy_ || !real_api_ || !real_api_->td_api) {
                    query_ok = false;
                    return;
                }
                td_api = real_api_->td_api;
#else
                query_ok = false;
                return;
#endif
            }
            if (!runtime_config_.enable_real_api) {
                trading_account_snapshot_.account_id = runtime_config_.user_id;
                trading_account_snapshot_.investor_id = runtime_config_.investor_id;
                trading_account_snapshot_.trading_day = "19700101";
                trading_account_snapshot_.ts_ns = NowEpochNanos();
                trading_account_snapshot_.source = "simulated";
                snapshot = trading_account_snapshot_;
                callback = trading_account_snapshot_callback_;
                return;
            }
        }
#if QUANT_HFT_HAS_REAL_CTP
        CThostFtdcQryTradingAccountField req{};
        CopyCtpField(req.BrokerID, runtime.broker_id);
        CopyCtpField(req.InvestorID, runtime.investor_id);
        query_ok = ExecuteTdQueryWithRetry(
            [&]() { return td_api->ReqQryTradingAccount(&req, request_id); });
#endif
    };

    if (!query_scheduler_.TrySchedule(std::move(task))) {
        return false;
    }
    if (!FinishQuerySchedule(query_scheduler_.DrainOnce(), query_ok)) {
        return false;
    }
    if (callback) {
        callback(snapshot);
    }
    return true;
}

bool CtpGatewayAdapter::EnqueueInvestorPositionQuery(int request_id) {
    bool query_ok = true;
    InvestorPositionSnapshotCallback callback;
    std::vector<InvestorPositionSnapshot> snapshots;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (!connected_) {
            return false;
        }
    }

    QueryScheduler::QueryTask task;
    task.request_id = request_id;
    task.priority = QueryScheduler::Priority::kHigh;
    task.execute = [this, request_id, &query_ok, &callback, &snapshots]() {
        CtpRuntimeConfig runtime;
#if QUANT_HFT_HAS_REAL_CTP
        CThostFtdcTraderApi* td_api = nullptr;
#endif
        {
            std::lock_guard<std::mutex> lock(mutex_);
            runtime = runtime_config_;
            investor_position_snapshots_.clear();
            if (runtime_config_.enable_real_api) {
#if QUANT_HFT_HAS_REAL_CTP
                if (!healthy_ || !real_api_ || !real_api_->td_api) {
                    query_ok = false;
                    return;
                }
                td_api = real_api_->td_api;
#else
                query_ok = false;
                return;
#endif
            }
            if (!runtime_config_.enable_real_api) {
                snapshots = investor_position_snapshots_;
                callback = investor_position_snapshot_callback_;
                return;
            }
        }
#if QUANT_HFT_HAS_REAL_CTP
        CThostFtdcQryInvestorPositionField req{};
        CopyCtpField(req.BrokerID, runtime.broker_id);
        CopyCtpField(req.InvestorID, runtime.investor_id);
        query_ok = ExecuteTdQueryWithRetry(
            [&]() { return td_api->ReqQryInvestorPosition(&req, request_id); });
#endif
    };

    if (!query_scheduler_.TrySchedule(std::move(task))) {
        return false;
    }
    if (!FinishQuerySchedule(query_scheduler_.DrainOnce(), query_ok)) {
        return false;
    }
    if (callback) {
        callback(snapshots);
    }
    return true;
}

bool CtpGatewayAdapter::EnqueueInstrumentQuery(int request_id) {
    return EnqueueInstrumentQuery(request_id, "");
}

bool CtpGatewayAdapter::EnqueueInstrumentQuery(int request_id, const std::string& instrument_id) {
    bool query_ok = true;
    InstrumentMetaSnapshotCallback callback;
    std::vector<InstrumentMetaSnapshot> snapshots;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (!connected_) {
            return false;
        }
    }

    QueryScheduler::QueryTask task;
    task.request_id = request_id;
    task.priority = QueryScheduler::Priority::kNormal;
    task.execute = [this, request_id, instrument_id, &query_ok, &callback, &snapshots]() {
        CtpRuntimeConfig runtime;
#if QUANT_HFT_HAS_REAL_CTP
        CThostFtdcTraderApi* td_api = nullptr;
#endif
        {
            std::lock_guard<std::mutex> lock(mutex_);
            runtime = runtime_config_;
            if (instrument_id.empty()) {
                instrument_meta_snapshots_.clear();
            }
            if (runtime_config_.enable_real_api) {
#if QUANT_HFT_HAS_REAL_CTP
                if (!healthy_ || !real_api_ || !real_api_->td_api) {
                    query_ok = false;
                    return;
                }
                td_api = real_api_->td_api;
#else
                query_ok = false;
                return;
#endif
            }
            if (!runtime_config_.enable_real_api) {
                std::vector<std::string> requested_instruments;
                if (!instrument_id.empty()) {
                    requested_instruments.push_back(instrument_id);
                } else {
                    requested_instruments.assign(subscriptions_.begin(), subscriptions_.end());
                }
                instrument_meta_snapshots_.reserve(instrument_meta_snapshots_.size() +
                                                   requested_instruments.size());
                for (const auto& requested_instrument_id : requested_instruments) {
                    InstrumentMetaSnapshot meta;
                    meta.instrument_id = requested_instrument_id;
                    meta.exchange_id = InferExchangeIdFromInstrument(requested_instrument_id);
                    meta.volume_multiple = 1;
                    meta.source = "simulated";
                    meta.ts_ns = NowEpochNanos();
                    instrument_meta_snapshots_.erase(
                        std::remove_if(instrument_meta_snapshots_.begin(),
                                       instrument_meta_snapshots_.end(),
                                       [&](const auto& row) {
                                           return row.instrument_id == meta.instrument_id;
                                       }),
                        instrument_meta_snapshots_.end());
                    instrument_meta_snapshots_.push_back(std::move(meta));
                }
                snapshots = instrument_meta_snapshots_;
                callback = instrument_meta_snapshot_callback_;
                return;
            }
        }
#if QUANT_HFT_HAS_REAL_CTP
        CThostFtdcQryInstrumentField req{};
        if (!instrument_id.empty()) {
            CopyCtpField(req.InstrumentID, instrument_id);
        }
        query_ok =
            ExecuteTdQueryWithRetry([&]() { return td_api->ReqQryInstrument(&req, request_id); });
#endif
    };

    if (!query_scheduler_.TrySchedule(std::move(task))) {
        return false;
    }
    if (!FinishQuerySchedule(query_scheduler_.DrainOnce(), query_ok)) {
        return false;
    }
    if (callback) {
        callback(snapshots);
    }
    return true;
}

bool CtpGatewayAdapter::EnqueueInstrumentMarginRateQuery(int request_id,
                                                         const std::string& instrument_id) {
    if (instrument_id.empty()) {
        return false;
    }

    bool query_ok = true;
    InstrumentMarginRateSnapshotCallback callback;
    std::vector<InstrumentMarginRateSnapshot> snapshots;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (!connected_) {
            return false;
        }
    }

    QueryScheduler::QueryTask task;
    task.request_id = request_id;
    task.priority = QueryScheduler::Priority::kLow;
    task.execute = [this, request_id, instrument_id, &query_ok, &callback, &snapshots]() {
        CtpRuntimeConfig runtime;
#if QUANT_HFT_HAS_REAL_CTP
        CThostFtdcTraderApi* td_api = nullptr;
#endif
        {
            std::lock_guard<std::mutex> lock(mutex_);
            runtime = runtime_config_;
            if (!runtime_config_.enable_real_api) {
                InstrumentMarginRateSnapshot snapshot;
                snapshot.account_id = runtime_config_.user_id;
                snapshot.investor_id = runtime_config_.investor_id;
                snapshot.instrument_id = instrument_id;
                snapshot.exchange_id = InferExchangeIdFromInstrument(instrument_id);
                snapshot.hedge_flag = "1";
                snapshot.long_margin_ratio_by_money = 0.1;
                snapshot.short_margin_ratio_by_money = 0.1;
                snapshot.ts_ns = NowEpochNanos();
                snapshot.source = "simulated";
                instrument_margin_rate_snapshots_.erase(
                    std::remove_if(instrument_margin_rate_snapshots_.begin(),
                                   instrument_margin_rate_snapshots_.end(),
                                   [&](const auto& row) {
                                       return row.account_id == snapshot.account_id &&
                                              row.instrument_id == snapshot.instrument_id &&
                                              row.exchange_id == snapshot.exchange_id &&
                                              row.hedge_flag == snapshot.hedge_flag;
                                   }),
                    instrument_margin_rate_snapshots_.end());
                instrument_margin_rate_snapshots_.push_back(snapshot);
                snapshots = instrument_margin_rate_snapshots_;
                callback = instrument_margin_rate_snapshot_callback_;
                return;
            }
#if QUANT_HFT_HAS_REAL_CTP
            if (!healthy_ || !real_api_ || !real_api_->td_api) {
                query_ok = false;
                return;
            }
            td_api = real_api_->td_api;
#else
            query_ok = false;
            return;
#endif
        }
#if QUANT_HFT_HAS_REAL_CTP
        CThostFtdcQryInstrumentMarginRateField req{};
        CopyCtpField(req.BrokerID, runtime.broker_id);
        CopyCtpField(req.InvestorID, runtime.investor_id);
        CopyCtpField(req.InstrumentID, instrument_id);
        req.HedgeFlag = THOST_FTDC_HF_Speculation;
        query_ok = ExecuteTdQueryWithRetry(
            [&]() { return td_api->ReqQryInstrumentMarginRate(&req, request_id); });
#endif
    };

    if (!query_scheduler_.TrySchedule(std::move(task))) {
        return false;
    }
    if (!FinishQuerySchedule(query_scheduler_.DrainOnce(), query_ok)) {
        return false;
    }
    if (callback) {
        callback(snapshots);
    }
    return true;
}

bool CtpGatewayAdapter::EnqueueInstrumentCommissionRateQuery(int request_id,
                                                             const std::string& instrument_id) {
    if (instrument_id.empty()) {
        return false;
    }

    bool query_ok = true;
    InstrumentCommissionRateSnapshotCallback callback;
    std::vector<InstrumentCommissionRateSnapshot> snapshots;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (!connected_) {
            return false;
        }
    }

    QueryScheduler::QueryTask task;
    task.request_id = request_id;
    task.priority = QueryScheduler::Priority::kLow;
    task.execute = [this, request_id, instrument_id, &query_ok, &callback, &snapshots]() {
        CtpRuntimeConfig runtime;
#if QUANT_HFT_HAS_REAL_CTP
        CThostFtdcTraderApi* td_api = nullptr;
#endif
        {
            std::lock_guard<std::mutex> lock(mutex_);
            runtime = runtime_config_;
            if (!runtime_config_.enable_real_api) {
                InstrumentCommissionRateSnapshot snapshot;
                snapshot.account_id = runtime_config_.user_id;
                snapshot.investor_id = runtime_config_.investor_id;
                snapshot.instrument_id = instrument_id;
                snapshot.exchange_id = InferExchangeIdFromInstrument(instrument_id);
                snapshot.open_ratio_by_money = 0.0001;
                snapshot.close_ratio_by_money = 0.0001;
                snapshot.close_today_ratio_by_money = 0.0001;
                snapshot.ts_ns = NowEpochNanos();
                snapshot.source = "simulated";
                instrument_commission_rate_snapshots_.erase(
                    std::remove_if(instrument_commission_rate_snapshots_.begin(),
                                   instrument_commission_rate_snapshots_.end(),
                                   [&](const auto& row) {
                                       return row.account_id == snapshot.account_id &&
                                              row.instrument_id == snapshot.instrument_id &&
                                              row.exchange_id == snapshot.exchange_id;
                                   }),
                    instrument_commission_rate_snapshots_.end());
                instrument_commission_rate_snapshots_.push_back(snapshot);
                snapshots = instrument_commission_rate_snapshots_;
                callback = instrument_commission_rate_snapshot_callback_;
                return;
            }
#if QUANT_HFT_HAS_REAL_CTP
            if (!healthy_ || !real_api_ || !real_api_->td_api) {
                query_ok = false;
                return;
            }
            td_api = real_api_->td_api;
#else
            query_ok = false;
            return;
#endif
        }
#if QUANT_HFT_HAS_REAL_CTP
        CThostFtdcQryInstrumentCommissionRateField req{};
        CopyCtpField(req.BrokerID, runtime.broker_id);
        CopyCtpField(req.InvestorID, runtime.investor_id);
        CopyCtpField(req.InstrumentID, instrument_id);
        query_ok = ExecuteTdQueryWithRetry(
            [&]() { return td_api->ReqQryInstrumentCommissionRate(&req, request_id); });
#endif
    };

    if (!query_scheduler_.TrySchedule(std::move(task))) {
        return false;
    }
    if (!FinishQuerySchedule(query_scheduler_.DrainOnce(), query_ok)) {
        return false;
    }
    if (callback) {
        callback(snapshots);
    }
    return true;
}

bool CtpGatewayAdapter::EnqueueInstrumentOrderCommRateQuery(int request_id,
                                                            const std::string& instrument_id) {
    if (instrument_id.empty()) {
        return false;
    }

    bool query_ok = true;
    InstrumentOrderCommRateSnapshotCallback callback;
    std::vector<InstrumentOrderCommRateSnapshot> snapshots;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (!connected_) {
            return false;
        }
    }

    QueryScheduler::QueryTask task;
    task.request_id = request_id;
    task.priority = QueryScheduler::Priority::kLow;
    task.execute = [this, request_id, instrument_id, &query_ok, &callback, &snapshots]() {
        CtpRuntimeConfig runtime;
#if QUANT_HFT_HAS_REAL_CTP
        CThostFtdcTraderApi* td_api = nullptr;
#endif
        {
            std::lock_guard<std::mutex> lock(mutex_);
            runtime = runtime_config_;
            if (!runtime_config_.enable_real_api) {
                InstrumentOrderCommRateSnapshot snapshot;
                snapshot.account_id = runtime_config_.user_id;
                snapshot.investor_id = runtime_config_.investor_id;
                snapshot.instrument_id = instrument_id;
                snapshot.exchange_id = InferExchangeIdFromInstrument(instrument_id);
                snapshot.hedge_flag = "1";
                snapshot.ts_ns = NowEpochNanos();
                snapshot.source = "simulated";
                instrument_order_comm_rate_snapshots_.erase(
                    std::remove_if(instrument_order_comm_rate_snapshots_.begin(),
                                   instrument_order_comm_rate_snapshots_.end(),
                                   [&](const auto& row) {
                                       return row.account_id == snapshot.account_id &&
                                              row.instrument_id == snapshot.instrument_id &&
                                              row.exchange_id == snapshot.exchange_id &&
                                              row.hedge_flag == snapshot.hedge_flag;
                                   }),
                    instrument_order_comm_rate_snapshots_.end());
                instrument_order_comm_rate_snapshots_.push_back(snapshot);
                snapshots = instrument_order_comm_rate_snapshots_;
                callback = instrument_order_comm_rate_snapshot_callback_;
                return;
            }
#if QUANT_HFT_HAS_REAL_CTP
            if (!healthy_ || !real_api_ || !real_api_->td_api) {
                query_ok = false;
                return;
            }
            td_api = real_api_->td_api;
#else
            query_ok = false;
            return;
#endif
        }
#if QUANT_HFT_HAS_REAL_CTP
        CThostFtdcQryInstrumentOrderCommRateField req{};
        CopyCtpField(req.BrokerID, runtime.broker_id);
        CopyCtpField(req.InvestorID, runtime.investor_id);
        CopyCtpField(req.InstrumentID, instrument_id);
        query_ok = ExecuteTdQueryWithRetry(
            [&]() { return td_api->ReqQryInstrumentOrderCommRate(&req, request_id); });
#endif
    };

    if (!query_scheduler_.TrySchedule(std::move(task))) {
        return false;
    }
    if (!FinishQuerySchedule(query_scheduler_.DrainOnce(), query_ok)) {
        return false;
    }
    if (callback) {
        callback(snapshots);
    }
    return true;
}

bool CtpGatewayAdapter::EnqueueBrokerTradingParamsQuery(int request_id) {
    bool query_ok = true;
    BrokerTradingParamsSnapshotCallback callback;
    BrokerTradingParamsSnapshot snapshot;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (!connected_) {
            return false;
        }
    }

    QueryScheduler::QueryTask task;
    task.request_id = request_id;
    task.priority = QueryScheduler::Priority::kHigh;
    task.execute = [this, request_id, &query_ok, &callback, &snapshot]() {
        CtpRuntimeConfig runtime;
#if QUANT_HFT_HAS_REAL_CTP
        CThostFtdcTraderApi* td_api = nullptr;
#endif
        {
            std::lock_guard<std::mutex> lock(mutex_);
            runtime = runtime_config_;
            if (runtime_config_.enable_real_api) {
#if QUANT_HFT_HAS_REAL_CTP
                if (!healthy_ || !real_api_ || !real_api_->td_api) {
                    query_ok = false;
                    return;
                }
                td_api = real_api_->td_api;
#else
                query_ok = false;
                return;
#endif
            }
            if (!runtime_config_.enable_real_api) {
                broker_trading_params_snapshot_.account_id = runtime_config_.user_id;
                broker_trading_params_snapshot_.investor_id = runtime_config_.investor_id;
                broker_trading_params_snapshot_.margin_price_type = "1";
                broker_trading_params_snapshot_.algorithm = "pre_settlement";
                broker_trading_params_snapshot_.ts_ns = NowEpochNanos();
                broker_trading_params_snapshot_.source = "simulated";
                snapshot = broker_trading_params_snapshot_;
                callback = broker_trading_params_snapshot_callback_;
                return;
            }
        }
#if QUANT_HFT_HAS_REAL_CTP
        CThostFtdcQryBrokerTradingParamsField req{};
        CopyCtpField(req.BrokerID, runtime.broker_id);
        CopyCtpField(req.InvestorID, runtime.investor_id);
        query_ok = ExecuteTdQueryWithRetry(
            [&]() { return td_api->ReqQryBrokerTradingParams(&req, request_id); });
#endif
    };

    if (!query_scheduler_.TrySchedule(std::move(task))) {
        return false;
    }
    if (!FinishQuerySchedule(query_scheduler_.DrainOnce(), query_ok)) {
        return false;
    }
    if (callback) {
        callback(snapshot);
    }
    return true;
}

bool CtpGatewayAdapter::EnqueueOrderQuery(int request_id) {
    bool query_ok = true;
    QueryCompleteCallback completion_callback;
    bool completion_notified = false;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (!connected_) {
            return false;
        }
    }

    QueryScheduler::QueryTask task;
    task.request_id = request_id;
    task.priority = QueryScheduler::Priority::kHigh;
    task.execute = [this, request_id, &query_ok, &completion_callback, &completion_notified]() {
        CtpRuntimeConfig runtime;
#if QUANT_HFT_HAS_REAL_CTP
        CThostFtdcTraderApi* td_api = nullptr;
#endif
        {
            std::lock_guard<std::mutex> lock(mutex_);
            runtime = runtime_config_;
            if (!runtime_config_.enable_real_api) {
                completion_callback = query_complete_callback_;
                completion_notified = true;
                return;
            }
#if QUANT_HFT_HAS_REAL_CTP
            if (!healthy_ || !real_api_ || !real_api_->td_api) {
                query_ok = false;
                return;
            }
            td_api = real_api_->td_api;
#else
            query_ok = false;
            return;
#endif
        }
#if QUANT_HFT_HAS_REAL_CTP
        CThostFtdcQryOrderField req{};
        CopyCtpField(req.BrokerID, runtime.broker_id);
        CopyCtpField(req.InvestorID, runtime.investor_id);
        query_ok = ExecuteTdQueryWithRetry([&]() { return td_api->ReqQryOrder(&req, request_id); });
#endif
    };

    if (!query_scheduler_.TrySchedule(std::move(task))) {
        return false;
    }
    const bool ok = FinishQuerySchedule(query_scheduler_.DrainOnce(), query_ok);
    if (completion_notified && completion_callback) {
        completion_callback(request_id, "order", ok);
    }
    return ok;
}

bool CtpGatewayAdapter::EnqueueTradeQuery(int request_id) {
    bool query_ok = true;
    QueryCompleteCallback completion_callback;
    bool completion_notified = false;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (!connected_) {
            return false;
        }
    }

    QueryScheduler::QueryTask task;
    task.request_id = request_id;
    task.priority = QueryScheduler::Priority::kHigh;
    task.execute = [this, request_id, &query_ok, &completion_callback, &completion_notified]() {
        CtpRuntimeConfig runtime;
#if QUANT_HFT_HAS_REAL_CTP
        CThostFtdcTraderApi* td_api = nullptr;
#endif
        {
            std::lock_guard<std::mutex> lock(mutex_);
            runtime = runtime_config_;
            if (!runtime_config_.enable_real_api) {
                completion_callback = query_complete_callback_;
                completion_notified = true;
                return;
            }
#if QUANT_HFT_HAS_REAL_CTP
            if (!healthy_ || !real_api_ || !real_api_->td_api) {
                query_ok = false;
                return;
            }
            td_api = real_api_->td_api;
#else
            query_ok = false;
            return;
#endif
        }
#if QUANT_HFT_HAS_REAL_CTP
        CThostFtdcQryTradeField req{};
        CopyCtpField(req.BrokerID, runtime.broker_id);
        CopyCtpField(req.InvestorID, runtime.investor_id);
        query_ok = ExecuteTdQueryWithRetry([&]() { return td_api->ReqQryTrade(&req, request_id); });
#endif
    };

    if (!query_scheduler_.TrySchedule(std::move(task))) {
        return false;
    }
    const bool ok = FinishQuerySchedule(query_scheduler_.DrainOnce(), query_ok);
    if (completion_notified && completion_callback) {
        completion_callback(request_id, "trade", ok);
    }
    return ok;
}

void CtpGatewayAdapter::RegisterTradingAccountSnapshotCallback(
    TradingAccountSnapshotCallback callback) {
    std::lock_guard<std::mutex> lock(mutex_);
    trading_account_snapshot_callback_ = std::move(callback);
}

void CtpGatewayAdapter::RegisterInvestorPositionSnapshotCallback(
    InvestorPositionSnapshotCallback callback) {
    std::lock_guard<std::mutex> lock(mutex_);
    investor_position_snapshot_callback_ = std::move(callback);
}

void CtpGatewayAdapter::RegisterInstrumentMetaSnapshotCallback(
    InstrumentMetaSnapshotCallback callback) {
    std::lock_guard<std::mutex> lock(mutex_);
    instrument_meta_snapshot_callback_ = std::move(callback);
}

void CtpGatewayAdapter::RegisterBrokerTradingParamsSnapshotCallback(
    BrokerTradingParamsSnapshotCallback callback) {
    std::lock_guard<std::mutex> lock(mutex_);
    broker_trading_params_snapshot_callback_ = std::move(callback);
}

void CtpGatewayAdapter::RegisterInstrumentMarginRateSnapshotCallback(
    InstrumentMarginRateSnapshotCallback callback) {
    std::lock_guard<std::mutex> lock(mutex_);
    instrument_margin_rate_snapshot_callback_ = std::move(callback);
}

void CtpGatewayAdapter::RegisterInstrumentCommissionRateSnapshotCallback(
    InstrumentCommissionRateSnapshotCallback callback) {
    std::lock_guard<std::mutex> lock(mutex_);
    instrument_commission_rate_snapshot_callback_ = std::move(callback);
}

void CtpGatewayAdapter::RegisterInstrumentOrderCommRateSnapshotCallback(
    InstrumentOrderCommRateSnapshotCallback callback) {
    std::lock_guard<std::mutex> lock(mutex_);
    instrument_order_comm_rate_snapshot_callback_ = std::move(callback);
}

CtpUserSessionInfo CtpGatewayAdapter::GetLastUserSession() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return user_session_;
}

TradingAccountSnapshot CtpGatewayAdapter::GetLastTradingAccountSnapshot() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return trading_account_snapshot_;
}

std::vector<InvestorPositionSnapshot> CtpGatewayAdapter::GetLastInvestorPositionSnapshots() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return investor_position_snapshots_;
}

std::vector<InstrumentMetaSnapshot> CtpGatewayAdapter::GetLastInstrumentMetaSnapshots() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return instrument_meta_snapshots_;
}

BrokerTradingParamsSnapshot CtpGatewayAdapter::GetLastBrokerTradingParamsSnapshot() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return broker_trading_params_snapshot_;
}

std::vector<InstrumentMarginRateSnapshot> CtpGatewayAdapter::GetLastInstrumentMarginRateSnapshots()
    const {
    std::lock_guard<std::mutex> lock(mutex_);
    return instrument_margin_rate_snapshots_;
}

std::vector<InstrumentCommissionRateSnapshot>
CtpGatewayAdapter::GetLastInstrumentCommissionRateSnapshots() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return instrument_commission_rate_snapshots_;
}

std::vector<InstrumentOrderCommRateSnapshot>
CtpGatewayAdapter::GetLastInstrumentOrderCommRateSnapshots() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return instrument_order_comm_rate_snapshots_;
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

bool CtpGatewayAdapter::ExecuteTdQueryWithRetry(const std::function<int()>& request_fn) const {
    if (!request_fn) {
        return false;
    }

    int backoff_ms = 0;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        backoff_ms = std::max(0, runtime_config_.query_retry_backoff_ms);
    }

    constexpr int kMaxAttempts = 5;
    for (int attempt = 1; attempt <= kMaxAttempts; ++attempt) {
        const int rc = request_fn();
        if (rc == 0) {
            return true;
        }
        if (rc != -2 && rc != -3) {
            return false;
        }
        if (attempt < kMaxAttempts && backoff_ms > 0) {
            std::this_thread::sleep_for(std::chrono::milliseconds(backoff_ms));
        }
    }
    return false;
}

void CtpGatewayAdapter::CompleteScheduledQuery() {
    query_scheduler_.MarkComplete();
    (void)query_scheduler_.DrainOnce();
}

bool CtpGatewayAdapter::FinishQuerySchedule(std::size_t drained, bool query_ok) {
    bool complete_immediately = false;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        complete_immediately = !runtime_config_.enable_real_api;
    }
    if (drained > 0U && (complete_immediately || !query_ok)) {
        query_scheduler_.MarkComplete();
    }
    return complete_immediately ? (drained > 0U && query_ok) : query_ok;
}

int CtpGatewayAdapter::NextRequestIdLocked() { return ++request_id_seq_; }

std::string CtpGatewayAdapter::NextOrderRefLocked() { return std::to_string(++order_ref_seq_); }

}  // namespace quant_hft
