#include "quant_hft/core/timescale_event_store_client_adapter.h"

#include <algorithm>
#include <chrono>
#include <thread>

namespace quant_hft {

namespace {

constexpr const char* kTableMarketSnapshots = "market_snapshots";
constexpr const char* kTableOrderEvents = "order_events";
constexpr const char* kTableRiskDecisions = "risk_decisions";

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
        {"last_price", ToString(snapshot.last_price)},
        {"bid_price_1", ToString(snapshot.bid_price_1)},
        {"ask_price_1", ToString(snapshot.ask_price_1)},
        {"bid_volume_1", ToString(snapshot.bid_volume_1)},
        {"ask_volume_1", ToString(snapshot.ask_volume_1)},
        {"volume", ToString(snapshot.volume)},
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
        {"status", OrderStatusToString(event.status)},
        {"total_volume", ToString(event.total_volume)},
        {"filled_volume", ToString(event.filled_volume)},
        {"avg_fill_price", ToString(event.avg_fill_price)},
        {"reason", event.reason},
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
        {"offset", OffsetToString(intent.offset)},
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
        if (!ParseDouble(row, "last_price", &snapshot.last_price)) {
            continue;
        }
        (void)ParseDouble(row, "bid_price_1", &snapshot.bid_price_1);
        (void)ParseDouble(row, "ask_price_1", &snapshot.ask_price_1);
        (void)ParseInt64(row, "bid_volume_1", &snapshot.bid_volume_1);
        (void)ParseInt64(row, "ask_volume_1", &snapshot.ask_volume_1);
        (void)ParseInt64(row, "volume", &snapshot.volume);
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
        if (!ParseOffset(GetOrEmpty(row, "offset"), &item.intent.offset)) {
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
