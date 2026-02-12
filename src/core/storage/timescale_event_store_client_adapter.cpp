#include "quant_hft/core/timescale_event_store_client_adapter.h"

#include <algorithm>
#include <chrono>
#include <thread>

namespace quant_hft {

namespace {

constexpr const char* kTableMarketSnapshots = "market_snapshots";
constexpr const char* kTableOrderEvents = "order_events";
constexpr const char* kTableRiskDecisions = "risk_decisions";
constexpr const char* kTableTradingAccounts = "ctp_trading_accounts";
constexpr const char* kTableInvestorPositions = "ctp_investor_positions";
constexpr const char* kTableBrokerTradingParams = "ctp_broker_trading_params";
constexpr const char* kTableInstrumentMeta = "ctp_instrument_meta";

}  // namespace

TimescaleEventStoreClientAdapter::TimescaleEventStoreClientAdapter(
    std::shared_ptr<ITimescaleSqlClient> client,
    StorageRetryPolicy retry_policy)
    : client_(std::move(client)), retry_policy_(retry_policy) {}

void TimescaleEventStoreClientAdapter::AppendMarketSnapshot(
    const MarketSnapshot& snapshot) {
    if (snapshot.instrument_id.empty()) {
        return;
    }

    std::unordered_map<std::string, std::string> row{
        {"instrument_id", snapshot.instrument_id},
        {"exchange_id", snapshot.exchange_id},
        {"trading_day", snapshot.trading_day},
        {"action_day", snapshot.action_day},
        {"update_time", snapshot.update_time},
        {"update_millisec", ToString(snapshot.update_millisec)},
        {"last_price", ToString(snapshot.last_price)},
        {"bid_price_1", ToString(snapshot.bid_price_1)},
        {"ask_price_1", ToString(snapshot.ask_price_1)},
        {"bid_volume_1", ToString(snapshot.bid_volume_1)},
        {"ask_volume_1", ToString(snapshot.ask_volume_1)},
        {"volume", ToString(snapshot.volume)},
        {"settlement_price", ToString(snapshot.settlement_price)},
        {"average_price_raw", ToString(snapshot.average_price_raw)},
        {"average_price_norm", ToString(snapshot.average_price_norm)},
        {"is_valid_settlement", snapshot.is_valid_settlement ? "1" : "0"},
        {"exchange_ts_ns", ToString(snapshot.exchange_ts_ns)},
        {"recv_ts_ns", ToString(snapshot.recv_ts_ns)},
    };
    (void)InsertWithRetry(kTableMarketSnapshots, row);
}

void TimescaleEventStoreClientAdapter::AppendOrderEvent(const OrderEvent& event) {
    if (event.client_order_id.empty()) {
        return;
    }

    std::unordered_map<std::string, std::string> row{
        {"account_id", event.account_id},
        {"client_order_id", event.client_order_id},
        {"exchange_order_id", event.exchange_order_id},
        {"instrument_id", event.instrument_id},
        {"exchange_id", event.exchange_id},
        {"status", OrderStatusToString(event.status)},
        {"total_volume", ToString(event.total_volume)},
        {"filled_volume", ToString(event.filled_volume)},
        {"avg_fill_price", ToString(event.avg_fill_price)},
        {"reason", event.reason},
        {"status_msg", event.status_msg},
        {"order_submit_status", event.order_submit_status},
        {"order_ref", event.order_ref},
        {"front_id", ToString(event.front_id)},
        {"session_id", ToString(event.session_id)},
        {"trade_id", event.trade_id},
        {"event_source", event.event_source},
        {"ts_ns", ToString(event.ts_ns)},
        {"trace_id", event.trace_id},
        {"execution_algo_id", event.execution_algo_id},
        {"slice_index", ToString(event.slice_index)},
        {"slice_total", ToString(event.slice_total)},
        {"throttle_applied", event.throttle_applied ? "1" : "0"},
        {"venue", event.venue},
        {"route_id", event.route_id},
        {"slippage_bps", ToString(event.slippage_bps)},
        {"impact_cost", ToString(event.impact_cost)},
    };
    (void)InsertWithRetry(kTableOrderEvents, row);
}

void TimescaleEventStoreClientAdapter::AppendRiskDecision(
    const OrderIntent& intent,
    const RiskDecision& decision) {
    const auto decision_ts_ns = decision.decision_ts_ns > 0 ? decision.decision_ts_ns
                                                             : NowEpochNanos();
    std::unordered_map<std::string, std::string> row{
        {"account_id", intent.account_id},
        {"client_order_id", intent.client_order_id},
        {"instrument_id", intent.instrument_id},
        {"side", SideToString(intent.side)},
        {"offset_flag", OffsetToString(intent.offset)},
        {"volume", ToString(intent.volume)},
        {"price", ToString(intent.price)},
        {"intent_ts_ns", ToString(intent.ts_ns)},
        {"trace_id", intent.trace_id},
        {"risk_action", RiskActionToString(decision.action)},
        {"rule_id", decision.rule_id},
        {"rule_group", decision.rule_group},
        {"rule_version", decision.rule_version},
        {"policy_id", decision.policy_id},
        {"policy_scope", decision.policy_scope},
        {"observed_value", ToString(decision.observed_value)},
        {"threshold_value", ToString(decision.threshold_value)},
        {"decision_tags", decision.decision_tags},
        {"reason", decision.reason},
        {"decision_ts_ns", ToString(decision_ts_ns)},
    };
    (void)InsertWithRetry(kTableRiskDecisions, row);
}

void TimescaleEventStoreClientAdapter::AppendTradingAccountSnapshot(
    const TradingAccountSnapshot& snapshot) {
    if (snapshot.account_id.empty()) {
        return;
    }

    std::unordered_map<std::string, std::string> row{
        {"account_id", snapshot.account_id},
        {"investor_id", snapshot.investor_id},
        {"balance", ToString(snapshot.balance)},
        {"available", ToString(snapshot.available)},
        {"curr_margin", ToString(snapshot.curr_margin)},
        {"frozen_margin", ToString(snapshot.frozen_margin)},
        {"frozen_cash", ToString(snapshot.frozen_cash)},
        {"frozen_commission", ToString(snapshot.frozen_commission)},
        {"commission", ToString(snapshot.commission)},
        {"close_profit", ToString(snapshot.close_profit)},
        {"position_profit", ToString(snapshot.position_profit)},
        {"trading_day", snapshot.trading_day},
        {"ts_ns", ToString(snapshot.ts_ns)},
        {"source", snapshot.source},
    };
    (void)InsertWithRetry(kTableTradingAccounts, row);
}

void TimescaleEventStoreClientAdapter::AppendInvestorPositionSnapshot(
    const InvestorPositionSnapshot& snapshot) {
    if (snapshot.account_id.empty() || snapshot.instrument_id.empty()) {
        return;
    }

    std::unordered_map<std::string, std::string> row{
        {"account_id", snapshot.account_id},
        {"investor_id", snapshot.investor_id},
        {"instrument_id", snapshot.instrument_id},
        {"exchange_id", snapshot.exchange_id},
        {"posi_direction", snapshot.posi_direction},
        {"hedge_flag", snapshot.hedge_flag},
        {"position_date", snapshot.position_date},
        {"position", ToString(snapshot.position)},
        {"today_position", ToString(snapshot.today_position)},
        {"yd_position", ToString(snapshot.yd_position)},
        {"long_frozen", ToString(snapshot.long_frozen)},
        {"short_frozen", ToString(snapshot.short_frozen)},
        {"open_volume", ToString(snapshot.open_volume)},
        {"close_volume", ToString(snapshot.close_volume)},
        {"position_cost", ToString(snapshot.position_cost)},
        {"open_cost", ToString(snapshot.open_cost)},
        {"position_profit", ToString(snapshot.position_profit)},
        {"close_profit", ToString(snapshot.close_profit)},
        {"margin_rate_by_money", ToString(snapshot.margin_rate_by_money)},
        {"margin_rate_by_volume", ToString(snapshot.margin_rate_by_volume)},
        {"use_margin", ToString(snapshot.use_margin)},
        {"ts_ns", ToString(snapshot.ts_ns)},
        {"source", snapshot.source},
    };
    (void)InsertWithRetry(kTableInvestorPositions, row);
}

void TimescaleEventStoreClientAdapter::AppendBrokerTradingParamsSnapshot(
    const BrokerTradingParamsSnapshot& snapshot) {
    if (snapshot.account_id.empty()) {
        return;
    }

    std::unordered_map<std::string, std::string> row{
        {"account_id", snapshot.account_id},
        {"investor_id", snapshot.investor_id},
        {"margin_price_type", snapshot.margin_price_type},
        {"algorithm", snapshot.algorithm},
        {"ts_ns", ToString(snapshot.ts_ns)},
        {"source", snapshot.source},
    };
    (void)InsertWithRetry(kTableBrokerTradingParams, row);
}

void TimescaleEventStoreClientAdapter::AppendInstrumentMetaSnapshot(
    const InstrumentMetaSnapshot& snapshot) {
    if (snapshot.instrument_id.empty()) {
        return;
    }

    std::unordered_map<std::string, std::string> row{
        {"instrument_id", snapshot.instrument_id},
        {"exchange_id", snapshot.exchange_id},
        {"product_id", snapshot.product_id},
        {"volume_multiple", ToString(snapshot.volume_multiple)},
        {"price_tick", ToString(snapshot.price_tick)},
        {"max_margin_side_algorithm", snapshot.max_margin_side_algorithm ? "1" : "0"},
        {"ts_ns", ToString(snapshot.ts_ns)},
        {"source", snapshot.source},
    };
    (void)InsertWithRetry(kTableInstrumentMeta, row);
}

std::vector<MarketSnapshot> TimescaleEventStoreClientAdapter::GetMarketSnapshots(
    const std::string& instrument_id) const {
    if (client_ == nullptr || instrument_id.empty()) {
        return {};
    }

    std::string error;
    const auto rows =
        client_->QueryRows(kTableMarketSnapshots, "instrument_id", instrument_id, &error);
    std::vector<MarketSnapshot> out;
    out.reserve(rows.size());
    for (const auto& row : rows) {
        MarketSnapshot snapshot;
        snapshot.instrument_id = GetOrEmpty(row, "instrument_id");
        snapshot.exchange_id = GetOrEmpty(row, "exchange_id");
        snapshot.trading_day = GetOrEmpty(row, "trading_day");
        snapshot.action_day = GetOrEmpty(row, "action_day");
        snapshot.update_time = GetOrEmpty(row, "update_time");
        (void)ParseInt32(row, "update_millisec", &snapshot.update_millisec);
        if (!ParseDouble(row, "last_price", &snapshot.last_price)) {
            continue;
        }
        (void)ParseDouble(row, "bid_price_1", &snapshot.bid_price_1);
        (void)ParseDouble(row, "ask_price_1", &snapshot.ask_price_1);
        (void)ParseInt64(row, "bid_volume_1", &snapshot.bid_volume_1);
        (void)ParseInt64(row, "ask_volume_1", &snapshot.ask_volume_1);
        (void)ParseInt64(row, "volume", &snapshot.volume);
        (void)ParseDouble(row, "settlement_price", &snapshot.settlement_price);
        (void)ParseDouble(row, "average_price_raw", &snapshot.average_price_raw);
        (void)ParseDouble(row, "average_price_norm", &snapshot.average_price_norm);
        std::int32_t valid_settlement = 0;
        if (ParseInt32(row, "is_valid_settlement", &valid_settlement)) {
            snapshot.is_valid_settlement = valid_settlement > 0;
        }
        (void)ParseInt64(row, "exchange_ts_ns", &snapshot.exchange_ts_ns);
        (void)ParseInt64(row, "recv_ts_ns", &snapshot.recv_ts_ns);
        out.push_back(snapshot);
    }
    return out;
}

std::vector<OrderEvent> TimescaleEventStoreClientAdapter::GetOrderEvents(
    const std::string& client_order_id) const {
    if (client_ == nullptr || client_order_id.empty()) {
        return {};
    }

    std::string error;
    const auto rows =
        client_->QueryRows(kTableOrderEvents, "client_order_id", client_order_id, &error);
    std::vector<OrderEvent> out;
    out.reserve(rows.size());
    for (const auto& row : rows) {
        OrderEvent event;
        event.account_id = GetOrEmpty(row, "account_id");
        event.client_order_id = GetOrEmpty(row, "client_order_id");
        event.exchange_order_id = GetOrEmpty(row, "exchange_order_id");
        event.instrument_id = GetOrEmpty(row, "instrument_id");
        event.exchange_id = GetOrEmpty(row, "exchange_id");
        if (!ParseOrderStatus(GetOrEmpty(row, "status"), &event.status)) {
            continue;
        }
        if (!ParseInt32(row, "total_volume", &event.total_volume)) {
            continue;
        }
        if (!ParseInt32(row, "filled_volume", &event.filled_volume)) {
            continue;
        }
        if (!ParseDouble(row, "avg_fill_price", &event.avg_fill_price)) {
            continue;
        }
        event.reason = GetOrEmpty(row, "reason");
        event.status_msg = GetOrEmpty(row, "status_msg");
        event.order_submit_status = GetOrEmpty(row, "order_submit_status");
        event.order_ref = GetOrEmpty(row, "order_ref");
        (void)ParseInt32(row, "front_id", &event.front_id);
        (void)ParseInt32(row, "session_id", &event.session_id);
        event.trade_id = GetOrEmpty(row, "trade_id");
        event.event_source = GetOrEmpty(row, "event_source");
        if (!ParseInt64(row, "ts_ns", &event.ts_ns)) {
            continue;
        }
        event.trace_id = GetOrEmpty(row, "trace_id");
        event.execution_algo_id = GetOrEmpty(row, "execution_algo_id");
        if (!ParseInt32(row, "slice_index", &event.slice_index)) {
            event.slice_index = 0;
        }
        if (!ParseInt32(row, "slice_total", &event.slice_total)) {
            event.slice_total = 0;
        }
        std::int32_t throttle_applied = 0;
        if (ParseInt32(row, "throttle_applied", &throttle_applied)) {
            event.throttle_applied = throttle_applied > 0;
        } else {
            const auto raw = GetOrEmpty(row, "throttle_applied");
            if (raw == "true" || raw == "TRUE" || raw == "yes" || raw == "YES") {
                event.throttle_applied = true;
            }
        }
        event.venue = GetOrEmpty(row, "venue");
        event.route_id = GetOrEmpty(row, "route_id");
        if (!ParseDouble(row, "slippage_bps", &event.slippage_bps)) {
            event.slippage_bps = 0.0;
        }
        if (!ParseDouble(row, "impact_cost", &event.impact_cost)) {
            event.impact_cost = 0.0;
        }
        out.push_back(event);
    }
    return out;
}

std::vector<RiskDecisionRow> TimescaleEventStoreClientAdapter::GetRiskDecisionRows() const {
    if (client_ == nullptr) {
        return {};
    }

    std::string error;
    const auto rows = client_->QueryAllRows(kTableRiskDecisions, &error);
    std::vector<RiskDecisionRow> out;
    out.reserve(rows.size());
    for (const auto& row : rows) {
        RiskDecisionRow item;
        item.intent.account_id = GetOrEmpty(row, "account_id");
        item.intent.client_order_id = GetOrEmpty(row, "client_order_id");
        item.intent.instrument_id = GetOrEmpty(row, "instrument_id");
        if (!ParseSide(GetOrEmpty(row, "side"), &item.intent.side)) {
            continue;
        }
        std::string offset_text = GetOrEmpty(row, "offset_flag");
        if (offset_text.empty()) {
            offset_text = GetOrEmpty(row, "offset");
        }
        if (!ParseOffset(offset_text, &item.intent.offset)) {
            continue;
        }
        if (!ParseInt32(row, "volume", &item.intent.volume)) {
            continue;
        }
        if (!ParseDouble(row, "price", &item.intent.price)) {
            continue;
        }
        if (!ParseInt64(row, "intent_ts_ns", &item.intent.ts_ns)) {
            continue;
        }
        item.intent.trace_id = GetOrEmpty(row, "trace_id");
        if (!ParseRiskAction(GetOrEmpty(row, "risk_action"), &item.decision.action)) {
            continue;
        }
        item.decision.rule_id = GetOrEmpty(row, "rule_id");
        item.decision.rule_group = GetOrEmpty(row, "rule_group");
        if (item.decision.rule_group.empty()) {
            item.decision.rule_group = "default";
        }
        item.decision.rule_version = GetOrEmpty(row, "rule_version");
        if (item.decision.rule_version.empty()) {
            item.decision.rule_version = "v1";
        }
        item.decision.policy_id = GetOrEmpty(row, "policy_id");
        item.decision.policy_scope = GetOrEmpty(row, "policy_scope");
        (void)ParseDouble(row, "observed_value", &item.decision.observed_value);
        (void)ParseDouble(row, "threshold_value", &item.decision.threshold_value);
        item.decision.decision_tags = GetOrEmpty(row, "decision_tags");
        item.decision.reason = GetOrEmpty(row, "reason");
        if (!ParseInt64(row, "decision_ts_ns", &item.decision.decision_ts_ns)) {
            item.decision.decision_ts_ns = item.intent.ts_ns;
        }
        item.ts_ns = item.decision.decision_ts_ns;
        out.push_back(item);
    }
    return out;
}

std::vector<TradingAccountSnapshot> TimescaleEventStoreClientAdapter::GetTradingAccountSnapshots(
    const std::string& account_id) const {
    if (client_ == nullptr || account_id.empty()) {
        return {};
    }

    std::string error;
    const auto rows =
        client_->QueryRows(kTableTradingAccounts, "account_id", account_id, &error);
    std::vector<TradingAccountSnapshot> out;
    out.reserve(rows.size());
    for (const auto& row : rows) {
        TradingAccountSnapshot snapshot;
        snapshot.account_id = GetOrEmpty(row, "account_id");
        snapshot.investor_id = GetOrEmpty(row, "investor_id");
        if (!ParseDouble(row, "balance", &snapshot.balance)) {
            continue;
        }
        (void)ParseDouble(row, "available", &snapshot.available);
        (void)ParseDouble(row, "curr_margin", &snapshot.curr_margin);
        (void)ParseDouble(row, "frozen_margin", &snapshot.frozen_margin);
        (void)ParseDouble(row, "frozen_cash", &snapshot.frozen_cash);
        (void)ParseDouble(row, "frozen_commission", &snapshot.frozen_commission);
        (void)ParseDouble(row, "commission", &snapshot.commission);
        (void)ParseDouble(row, "close_profit", &snapshot.close_profit);
        (void)ParseDouble(row, "position_profit", &snapshot.position_profit);
        snapshot.trading_day = GetOrEmpty(row, "trading_day");
        (void)ParseInt64(row, "ts_ns", &snapshot.ts_ns);
        snapshot.source = GetOrEmpty(row, "source");
        out.push_back(snapshot);
    }
    return out;
}

std::vector<InvestorPositionSnapshot>
TimescaleEventStoreClientAdapter::GetInvestorPositionSnapshots(
    const std::string& account_id,
    const std::string& instrument_id) const {
    if (client_ == nullptr || account_id.empty()) {
        return {};
    }

    std::string error;
    const auto rows =
        client_->QueryRows(kTableInvestorPositions, "account_id", account_id, &error);
    std::vector<InvestorPositionSnapshot> out;
    out.reserve(rows.size());
    for (const auto& row : rows) {
        InvestorPositionSnapshot snapshot;
        snapshot.account_id = GetOrEmpty(row, "account_id");
        snapshot.investor_id = GetOrEmpty(row, "investor_id");
        snapshot.instrument_id = GetOrEmpty(row, "instrument_id");
        if (!instrument_id.empty() && snapshot.instrument_id != instrument_id) {
            continue;
        }
        snapshot.exchange_id = GetOrEmpty(row, "exchange_id");
        snapshot.posi_direction = GetOrEmpty(row, "posi_direction");
        snapshot.hedge_flag = GetOrEmpty(row, "hedge_flag");
        snapshot.position_date = GetOrEmpty(row, "position_date");
        if (!ParseInt32(row, "position", &snapshot.position)) {
            continue;
        }
        (void)ParseInt32(row, "today_position", &snapshot.today_position);
        (void)ParseInt32(row, "yd_position", &snapshot.yd_position);
        (void)ParseInt32(row, "long_frozen", &snapshot.long_frozen);
        (void)ParseInt32(row, "short_frozen", &snapshot.short_frozen);
        (void)ParseInt32(row, "open_volume", &snapshot.open_volume);
        (void)ParseInt32(row, "close_volume", &snapshot.close_volume);
        (void)ParseDouble(row, "position_cost", &snapshot.position_cost);
        (void)ParseDouble(row, "open_cost", &snapshot.open_cost);
        (void)ParseDouble(row, "position_profit", &snapshot.position_profit);
        (void)ParseDouble(row, "close_profit", &snapshot.close_profit);
        (void)ParseDouble(row, "margin_rate_by_money", &snapshot.margin_rate_by_money);
        (void)ParseDouble(row, "margin_rate_by_volume", &snapshot.margin_rate_by_volume);
        (void)ParseDouble(row, "use_margin", &snapshot.use_margin);
        (void)ParseInt64(row, "ts_ns", &snapshot.ts_ns);
        snapshot.source = GetOrEmpty(row, "source");
        out.push_back(snapshot);
    }
    return out;
}

std::vector<BrokerTradingParamsSnapshot>
TimescaleEventStoreClientAdapter::GetBrokerTradingParamsSnapshots(
    const std::string& account_id) const {
    if (client_ == nullptr || account_id.empty()) {
        return {};
    }

    std::string error;
    const auto rows =
        client_->QueryRows(kTableBrokerTradingParams, "account_id", account_id, &error);
    std::vector<BrokerTradingParamsSnapshot> out;
    out.reserve(rows.size());
    for (const auto& row : rows) {
        BrokerTradingParamsSnapshot snapshot;
        snapshot.account_id = GetOrEmpty(row, "account_id");
        snapshot.investor_id = GetOrEmpty(row, "investor_id");
        snapshot.margin_price_type = GetOrEmpty(row, "margin_price_type");
        snapshot.algorithm = GetOrEmpty(row, "algorithm");
        (void)ParseInt64(row, "ts_ns", &snapshot.ts_ns);
        snapshot.source = GetOrEmpty(row, "source");
        out.push_back(snapshot);
    }
    return out;
}

std::vector<InstrumentMetaSnapshot> TimescaleEventStoreClientAdapter::GetInstrumentMetaSnapshots(
    const std::string& instrument_id) const {
    if (client_ == nullptr || instrument_id.empty()) {
        return {};
    }

    std::string error;
    const auto rows =
        client_->QueryRows(kTableInstrumentMeta, "instrument_id", instrument_id, &error);
    std::vector<InstrumentMetaSnapshot> out;
    out.reserve(rows.size());
    for (const auto& row : rows) {
        InstrumentMetaSnapshot snapshot;
        snapshot.instrument_id = GetOrEmpty(row, "instrument_id");
        snapshot.exchange_id = GetOrEmpty(row, "exchange_id");
        snapshot.product_id = GetOrEmpty(row, "product_id");
        if (!ParseInt32(row, "volume_multiple", &snapshot.volume_multiple)) {
            continue;
        }
        (void)ParseDouble(row, "price_tick", &snapshot.price_tick);
        std::int32_t flag = 0;
        if (ParseInt32(row, "max_margin_side_algorithm", &flag)) {
            snapshot.max_margin_side_algorithm = flag != 0;
        }
        (void)ParseInt64(row, "ts_ns", &snapshot.ts_ns);
        snapshot.source = GetOrEmpty(row, "source");
        out.push_back(snapshot);
    }
    return out;
}

bool TimescaleEventStoreClientAdapter::InsertWithRetry(
    const std::string& table,
    const std::unordered_map<std::string, std::string>& row) const {
    if (client_ == nullptr || table.empty()) {
        return false;
    }

    int attempts = std::max(1, retry_policy_.max_attempts);
    int backoff_ms = std::max(0, retry_policy_.initial_backoff_ms);
    const int max_backoff_ms = std::max(backoff_ms, retry_policy_.max_backoff_ms);

    for (int attempt = 1; attempt <= attempts; ++attempt) {
        std::string error;
        if (client_->InsertRow(table, row, &error)) {
            return true;
        }
        if (attempt < attempts && backoff_ms > 0) {
            std::this_thread::sleep_for(std::chrono::milliseconds(backoff_ms));
            backoff_ms = std::min(max_backoff_ms, backoff_ms * 2);
        }
    }
    return false;
}

std::string TimescaleEventStoreClientAdapter::ToString(std::int32_t value) {
    return std::to_string(value);
}

std::string TimescaleEventStoreClientAdapter::ToString(std::int64_t value) {
    return std::to_string(value);
}

std::string TimescaleEventStoreClientAdapter::ToString(double value) {
    return std::to_string(value);
}

std::string TimescaleEventStoreClientAdapter::SideToString(Side side) {
    return side == Side::kSell ? "SELL" : "BUY";
}

bool TimescaleEventStoreClientAdapter::ParseSide(const std::string& text, Side* out) {
    if (out == nullptr) {
        return false;
    }
    if (text == "BUY") {
        *out = Side::kBuy;
        return true;
    }
    if (text == "SELL") {
        *out = Side::kSell;
        return true;
    }
    return false;
}

std::string TimescaleEventStoreClientAdapter::OffsetToString(OffsetFlag offset) {
    switch (offset) {
        case OffsetFlag::kOpen:
            return "OPEN";
        case OffsetFlag::kClose:
            return "CLOSE";
        case OffsetFlag::kCloseToday:
            return "CLOSE_TODAY";
        case OffsetFlag::kCloseYesterday:
            return "CLOSE_YESTERDAY";
    }
    return "OPEN";
}

bool TimescaleEventStoreClientAdapter::ParseOffset(const std::string& text,
                                                   OffsetFlag* out) {
    if (out == nullptr) {
        return false;
    }
    if (text == "OPEN") {
        *out = OffsetFlag::kOpen;
        return true;
    }
    if (text == "CLOSE") {
        *out = OffsetFlag::kClose;
        return true;
    }
    if (text == "CLOSE_TODAY") {
        *out = OffsetFlag::kCloseToday;
        return true;
    }
    if (text == "CLOSE_YESTERDAY") {
        *out = OffsetFlag::kCloseYesterday;
        return true;
    }
    return false;
}

std::string TimescaleEventStoreClientAdapter::RiskActionToString(RiskAction action) {
    switch (action) {
        case RiskAction::kAllow:
            return "ALLOW";
        case RiskAction::kReject:
            return "REJECT";
        case RiskAction::kReview:
            return "REVIEW";
    }
    return "REVIEW";
}

bool TimescaleEventStoreClientAdapter::ParseRiskAction(const std::string& text,
                                                       RiskAction* out) {
    if (out == nullptr) {
        return false;
    }
    if (text == "ALLOW") {
        *out = RiskAction::kAllow;
        return true;
    }
    if (text == "REJECT") {
        *out = RiskAction::kReject;
        return true;
    }
    if (text == "REVIEW") {
        *out = RiskAction::kReview;
        return true;
    }
    return false;
}

std::string TimescaleEventStoreClientAdapter::OrderStatusToString(OrderStatus status) {
    switch (status) {
        case OrderStatus::kNew:
            return "NEW";
        case OrderStatus::kAccepted:
            return "ACCEPTED";
        case OrderStatus::kPartiallyFilled:
            return "PARTIALLY_FILLED";
        case OrderStatus::kFilled:
            return "FILLED";
        case OrderStatus::kCanceled:
            return "CANCELED";
        case OrderStatus::kRejected:
            return "REJECTED";
    }
    return "REJECTED";
}

bool TimescaleEventStoreClientAdapter::ParseOrderStatus(const std::string& text,
                                                        OrderStatus* out) {
    if (out == nullptr) {
        return false;
    }
    if (text == "NEW") {
        *out = OrderStatus::kNew;
        return true;
    }
    if (text == "ACCEPTED") {
        *out = OrderStatus::kAccepted;
        return true;
    }
    if (text == "PARTIALLY_FILLED") {
        *out = OrderStatus::kPartiallyFilled;
        return true;
    }
    if (text == "FILLED") {
        *out = OrderStatus::kFilled;
        return true;
    }
    if (text == "CANCELED") {
        *out = OrderStatus::kCanceled;
        return true;
    }
    if (text == "REJECTED") {
        *out = OrderStatus::kRejected;
        return true;
    }
    return false;
}

std::string TimescaleEventStoreClientAdapter::GetOrEmpty(
    const std::unordered_map<std::string, std::string>& row,
    const std::string& key) {
    const auto it = row.find(key);
    if (it == row.end()) {
        return "";
    }
    return it->second;
}

bool TimescaleEventStoreClientAdapter::ParseInt32(
    const std::unordered_map<std::string, std::string>& row,
    const std::string& key,
    std::int32_t* out) {
    if (out == nullptr) {
        return false;
    }
    const auto it = row.find(key);
    if (it == row.end()) {
        return false;
    }
    try {
        *out = static_cast<std::int32_t>(std::stoi(it->second));
        return true;
    } catch (...) {
        return false;
    }
}

bool TimescaleEventStoreClientAdapter::ParseInt64(
    const std::unordered_map<std::string, std::string>& row,
    const std::string& key,
    std::int64_t* out) {
    if (out == nullptr) {
        return false;
    }
    const auto it = row.find(key);
    if (it == row.end()) {
        return false;
    }
    try {
        *out = static_cast<std::int64_t>(std::stoll(it->second));
        return true;
    } catch (...) {
        return false;
    }
}

bool TimescaleEventStoreClientAdapter::ParseDouble(
    const std::unordered_map<std::string, std::string>& row,
    const std::string& key,
    double* out) {
    if (out == nullptr) {
        return false;
    }
    const auto it = row.find(key);
    if (it == row.end()) {
        return false;
    }
    try {
        *out = std::stod(it->second);
        return true;
    } catch (...) {
        return false;
    }
}

}  // namespace quant_hft
