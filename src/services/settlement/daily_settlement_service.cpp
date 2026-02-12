#include "quant_hft/services/daily_settlement_service.h"

#include <algorithm>
#include <unordered_map>

namespace quant_hft {
namespace {

bool IsRunStale(const SettlementRunRecord& run, int stale_timeout_ms, EpochNanos now_ts_ns) {
    if (run.heartbeat_ts_ns <= 0) {
        return true;
    }
    const auto timeout_ms = std::max(1, stale_timeout_ms);
    const auto timeout_ns = static_cast<EpochNanos>(timeout_ms) * 1'000'000LL;
    return now_ts_ns - run.heartbeat_ts_ns >= timeout_ns;
}

}  // namespace

DailySettlementService::DailySettlementService(std::shared_ptr<ISettlementStore> store,
                                               std::shared_ptr<SettlementQueryClient> query_client,
                                               std::shared_ptr<ITradingDomainStore> domain_store)
    : store_(std::move(store)),
      query_client_(std::move(query_client)),
      domain_store_(std::move(domain_store)) {}

bool DailySettlementService::Run(const DailySettlementConfig& config,
                                 DailySettlementResult* result,
                                 std::string* error) {
    if (result == nullptr) {
        if (error != nullptr) {
            *error = "daily settlement result pointer is null";
        }
        return false;
    }
    *result = DailySettlementResult{};

    if (store_ == nullptr || query_client_ == nullptr) {
        if (error != nullptr) {
            *error = "daily settlement dependencies are null";
        }
        return false;
    }
    if (config.trading_day.empty()) {
        if (error != nullptr) {
            *error = "trading_day is required";
        }
        return false;
    }
    if (config.account_id.empty()) {
        if (error != nullptr) {
            *error = "account_id is required";
        }
        return false;
    }

    SettlementRunRecord existing;
    std::string run_error;
    if (!store_->GetRun(config.trading_day, &existing, &run_error)) {
        if (error != nullptr) {
            *error = "load settlement run failed: " + run_error;
        }
        return false;
    }
    if (existing.status == "COMPLETED" && !config.force_run) {
        result->success = true;
        result->noop = true;
        result->blocked = false;
        result->status = "COMPLETED";
        result->message = "settlement already completed for trading_day";
        return true;
    }
    const EpochNanos settlement_start_ts_ns = NowEpochNanos();
    if (existing.status == "RUNNING" && !config.force_run &&
        !IsRunStale(existing, config.running_stale_timeout_ms, settlement_start_ts_ns)) {
        result->success = true;
        result->noop = true;
        result->blocked = false;
        result->status = "RUNNING";
        result->message = "settlement run is already in progress and not stale";
        return true;
    }

    if (!WriteRunStatus(config, "RECONCILING", settlement_start_ts_ns, "", "", error)) {
        return false;
    }

    std::string query_error;
    if (!query_client_->QueryTradingAccountWithRetry(1, &query_error)) {
        if (!WriteBlockedRun(config, settlement_start_ts_ns, query_error, error)) {
            return false;
        }
        result->success = false;
        result->blocked = true;
        result->status = "BLOCKED";
        result->message = query_error;
        return true;
    }
    if (!query_client_->QueryInvestorPositionWithRetry(10, &query_error)) {
        if (!WriteBlockedRun(config, settlement_start_ts_ns, query_error, error)) {
            return false;
        }
        result->success = false;
        result->blocked = true;
        result->status = "BLOCKED";
        result->message = query_error;
        return true;
    }
    if (!query_client_->QueryInstrumentWithRetry(20, &query_error)) {
        if (!WriteBlockedRun(config, settlement_start_ts_ns, query_error, error)) {
            return false;
        }
        result->success = false;
        result->blocked = true;
        result->status = "BLOCKED";
        result->message = query_error;
        return true;
    }

    std::vector<OrderEvent> backfill_events;
    std::string backfill_error;
    if (!query_client_->QueryOrderTradeBackfill(&backfill_events, &backfill_error) &&
        config.strict_order_trade_backfill) {
        if (!WriteBlockedRun(config, settlement_start_ts_ns, backfill_error, error)) {
            return false;
        }
        result->success = false;
        result->blocked = true;
        result->status = "BLOCKED";
        result->message = backfill_error;
        return true;
    }
    if (!backfill_events.empty() && domain_store_ != nullptr) {
        std::string persist_error;
        if (!PersistBackfillEvents(
                config.account_id, backfill_events, settlement_start_ts_ns, &persist_error)) {
            if (!WriteBlockedRun(config, settlement_start_ts_ns, persist_error, error)) {
                return false;
            }
            result->success = false;
            result->blocked = true;
            result->status = "BLOCKED";
            result->message = persist_error;
            return true;
        }
    }

    SettlementSummaryRecord summary;
    summary.trading_day = config.trading_day;
    summary.account_id = config.account_id;
    summary.created_ts_ns = NowEpochNanos();
    std::string summary_error;
    if (!store_->AppendSummary(summary, &summary_error)) {
        if (error != nullptr) {
            *error = "append settlement summary failed: " + summary_error;
        }
        return false;
    }
    if (!WriteRunStatus(config, "CALCULATED", settlement_start_ts_ns, "", "", error)) {
        return false;
    }

    if (!WriteCompletedRun(config, settlement_start_ts_ns, error)) {
        return false;
    }
    result->success = true;
    result->noop = false;
    result->blocked = false;
    result->status = "COMPLETED";
    result->message = config.settlement_shadow_enabled ? "settlement completed (shadow mode)"
                                                       : "settlement completed";
    return true;
}

bool DailySettlementService::WriteRunStatus(const DailySettlementConfig& config,
                                            const std::string& status,
                                            EpochNanos started_ts_ns,
                                            const std::string& error_code,
                                            const std::string& error_msg,
                                            std::string* error) const {
    SettlementRunRecord run;
    run.trading_day = config.trading_day;
    run.status = status;
    run.force_run = config.force_run;
    run.heartbeat_ts_ns = NowEpochNanos();
    run.started_ts_ns = started_ts_ns > 0 ? started_ts_ns : run.heartbeat_ts_ns;
    run.completed_ts_ns = run.heartbeat_ts_ns;
    run.error_code = error_code;
    run.error_msg = error_msg;
    run.evidence_path = config.evidence_path;
    std::string run_error;
    if (!store_->UpsertRun(run, &run_error)) {
        if (error != nullptr) {
            *error = "upsert settlement run failed: " + run_error;
        }
        return false;
    }
    return true;
}

bool DailySettlementService::WriteBlockedRun(const DailySettlementConfig& config,
                                             EpochNanos started_ts_ns,
                                             const std::string& reason,
                                             std::string* error) const {
    if (!WriteRunStatus(
            config, "BLOCKED", started_ts_ns, "SETTLEMENT_BLOCKED", reason, error)) {
        return false;
    }

    SettlementReconcileDiffRecord diff;
    diff.trading_day = config.trading_day;
    diff.account_id = config.account_id;
    diff.diff_type = "QUERY_ERROR";
    diff.key_ref = "ctp_query";
    diff.local_value = 0.0;
    diff.ctp_value = 0.0;
    diff.delta_value = 0.0;
    diff.diagnose_hint = reason;
    diff.raw_payload = "{}";
    diff.created_ts_ns = NowEpochNanos();
    std::string diff_error;
    if (!store_->AppendReconcileDiff(diff, &diff_error) && error != nullptr) {
        *error = "append reconcile diff failed: " + diff_error;
        return false;
    }
    return true;
}

bool DailySettlementService::WriteCompletedRun(const DailySettlementConfig& config,
                                               EpochNanos started_ts_ns,
                                               std::string* error) const {
    return WriteRunStatus(config, "COMPLETED", started_ts_ns, "", "", error);
}

bool DailySettlementService::PersistBackfillEvents(const std::string& account_id,
                                                   const std::vector<OrderEvent>& events,
                                                   EpochNanos settlement_start_ts_ns,
                                                   std::string* error) const {
    if (domain_store_ == nullptr) {
        return true;
    }

    std::unordered_map<std::string, OrderEvent> latest_order_events;
    std::unordered_map<std::string, OrderEvent> unique_trade_events;
    for (const auto& event : events) {
        if (event.event_source == "OnRspQryOrder") {
            const std::string order_key =
                !event.order_ref.empty() ? event.order_ref : event.client_order_id;
            if (order_key.empty()) {
                continue;
            }
            auto it = latest_order_events.find(order_key);
            if (it == latest_order_events.end() || event.ts_ns >= it->second.ts_ns) {
                latest_order_events[order_key] = event;
            }
            continue;
        }
        if (event.event_source == "OnRspQryTrade") {
            std::string trade_key = event.trade_id;
            if (trade_key.empty()) {
                trade_key = event.order_ref + "|" + std::to_string(event.filled_volume) + "|" +
                            std::to_string(event.ts_ns);
            }
            unique_trade_events.emplace(std::move(trade_key), event);
        }
    }

    constexpr const char* kBackfillStrategyId = "settlement_backfill";
    for (const auto& [order_key, event] : latest_order_events) {
        (void)order_key;
        Order order;
        order.order_id = !event.order_ref.empty() ? event.order_ref : event.client_order_id;
        order.account_id = event.account_id.empty() ? account_id : event.account_id;
        order.strategy_id = kBackfillStrategyId;
        order.symbol = event.instrument_id;
        order.exchange = event.exchange_id;
        order.side = event.side;
        order.offset = event.offset;
        order.order_type = OrderType::kLimit;
        order.price = event.avg_fill_price;
        order.quantity = std::max(event.total_volume, event.filled_volume);
        order.filled_quantity = std::max(0, event.filled_volume);
        order.avg_fill_price = event.avg_fill_price;
        order.status = event.status;
        const EpochNanos event_ts_ns = event.exchange_ts_ns > 0
                                           ? event.exchange_ts_ns
                                           : (event.ts_ns > 0 ? event.ts_ns : NowEpochNanos());
        order.created_at_ns = event_ts_ns;
        order.updated_at_ns = event_ts_ns;
        order.message = event.status_msg;
        if (event_ts_ns > settlement_start_ts_ns) {
            if (!order.message.empty()) {
                order.message += " | ";
            }
            order.message += "post_settlement_backfill";
        }
        std::string upsert_error;
        if (!domain_store_->UpsertOrder(order, &upsert_error)) {
            if (error != nullptr) {
                *error = "persist backfill order failed: " + upsert_error;
            }
            return false;
        }
    }

    for (const auto& [trade_key, event] : unique_trade_events) {
        (void)trade_key;
        Trade trade;
        trade.trade_id = !event.trade_id.empty()
                             ? event.trade_id
                             : (!event.order_ref.empty()
                                    ? event.order_ref + "_" + std::to_string(event.ts_ns)
                                    : "settlement_backfill_" + std::to_string(event.ts_ns));
        trade.order_id = !event.order_ref.empty() ? event.order_ref : event.client_order_id;
        trade.account_id = event.account_id.empty() ? account_id : event.account_id;
        trade.strategy_id = kBackfillStrategyId;
        trade.symbol = event.instrument_id;
        trade.exchange = event.exchange_id;
        trade.side = event.side;
        trade.offset = event.offset;
        trade.price = event.avg_fill_price;
        trade.quantity = std::max(1, event.filled_volume);
        const EpochNanos event_ts_ns = event.exchange_ts_ns > 0
                                           ? event.exchange_ts_ns
                                           : (event.ts_ns > 0 ? event.ts_ns : NowEpochNanos());
        trade.trade_ts_ns = event_ts_ns;
        std::string append_error;
        if (!domain_store_->AppendTrade(trade, &append_error)) {
            if (error != nullptr) {
                *error = "persist backfill trade failed: " + append_error;
            }
            return false;
        }
    }
    return true;
}

}  // namespace quant_hft
