#include "quant_hft/services/daily_settlement_service.h"

#include <algorithm>
#include <chrono>
#include <cmath>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <sstream>
#include <stdexcept>
#include <unordered_map>
#include <unordered_set>

namespace quant_hft {
namespace {

struct PositionAgg {
    std::int32_t long_position{0};
    std::int32_t short_position{0};
    std::int32_t long_today{0};
    std::int32_t short_today{0};
    std::int32_t long_yd{0};
    std::int32_t short_yd{0};
};

bool IsRunStale(const SettlementRunRecord& run, int stale_timeout_ms, EpochNanos now_ts_ns) {
    if (run.heartbeat_ts_ns <= 0) {
        return true;
    }
    const auto timeout_ms = std::max(1, stale_timeout_ms);
    const auto timeout_ns = static_cast<EpochNanos>(timeout_ms) * 1'000'000LL;
    return now_ts_ns - run.heartbeat_ts_ns >= timeout_ns;
}

bool ParseDate(const std::string& text, std::tm* out) {
    if (out == nullptr || text.size() != 10) {
        return false;
    }
    std::tm value{};
    std::istringstream stream(text);
    stream >> std::get_time(&value, "%Y-%m-%d");
    if (stream.fail()) {
        return false;
    }
    *out = value;
    return true;
}

std::int64_t TimegmPortable(std::tm* utc_tm) {
#if defined(_WIN32)
    return static_cast<std::int64_t>(_mkgmtime(utc_tm));
#else
    return static_cast<std::int64_t>(timegm(utc_tm));
#endif
}

std::string JsonNumber(double value) {
    std::ostringstream stream;
    stream << std::fixed << std::setprecision(8) << value;
    return stream.str();
}

}  // namespace

DailySettlementService::DailySettlementService(std::shared_ptr<SettlementPriceProvider> price_provider,
                                               std::shared_ptr<ISettlementStore> store,
                                               std::shared_ptr<SettlementQueryClient> query_client,
                                               std::shared_ptr<ITradingDomainStore> domain_store)
    : price_provider_(std::move(price_provider)),
      store_(std::move(store)),
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
    result->diff_report_path = config.diff_report_path;

    if (store_ == nullptr || query_client_ == nullptr || price_provider_ == nullptr) {
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
    existing.status = NormalizeRunStatus(existing.status);

    if (existing.status == "COMPLETED" && !config.force_run) {
        result->success = true;
        result->noop = true;
        result->blocked = false;
        result->status = "COMPLETED";
        result->message = "settlement already completed for trading_day";
        return true;
    }

    const EpochNanos settlement_start_ts_ns = NowEpochNanos();
    if (!existing.status.empty() && !IsRunTerminalStatus(existing.status) && !config.force_run &&
        !IsRunStale(existing, config.running_stale_timeout_ms, settlement_start_ts_ns)) {
        result->success = true;
        result->noop = true;
        result->blocked = existing.status == "BLOCKED";
        result->status = existing.status;
        result->message = "settlement run is already in progress and not stale";
        return true;
    }

    if (!WriteRunStatus(config, "RECONCILING", settlement_start_ts_ns, "", "", error)) {
        return false;
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

    std::vector<SettlementOpenPositionRecord> positions;
    std::string load_error;
    if (!store_->LoadOpenPositions(config.account_id, &positions, &load_error)) {
        if (!WriteRunStatus(config,
                            "FAILED",
                            settlement_start_ts_ns,
                            "LOAD_OPEN_POSITIONS_FAILED",
                            load_error,
                            error)) {
            return false;
        }
        if (error != nullptr) {
            *error = load_error;
        }
        return false;
    }

    std::unordered_map<std::string, double> final_prices;
    std::unordered_map<std::string, SettlementInstrumentRecord> instruments;
    std::string price_error;
    if (!LoadSettlementPrices(config, positions, &final_prices, &instruments, &price_error)) {
        if (!WriteRunStatus(config,
                            "PENDING_PRICE",
                            settlement_start_ts_ns,
                            "MISSING_SETTLEMENT_PRICE",
                            price_error,
                            error)) {
            return false;
        }
        result->success = false;
        result->blocked = true;
        result->status = "PENDING_PRICE";
        result->message = price_error;
        return true;
    }

    std::int64_t total_position_profit_cents = 0;
    std::string settlement_error;
    if (!RunSettlementLoop(config,
                           &positions,
                           final_prices,
                           instruments,
                           &total_position_profit_cents,
                           &settlement_error)) {
        if (!WriteRunStatus(config,
                            "FAILED",
                            settlement_start_ts_ns,
                            "SETTLEMENT_LOOP_FAILED",
                            settlement_error,
                            error)) {
            return false;
        }
        if (error != nullptr) {
            *error = settlement_error;
        }
        return false;
    }

    std::string rollover_error;
    if (!RolloverPositions(config, &rollover_error)) {
        if (!WriteRunStatus(config,
                            "FAILED",
                            settlement_start_ts_ns,
                            "ROLLOVER_FAILED",
                            rollover_error,
                            error)) {
            return false;
        }
        if (error != nullptr) {
            *error = rollover_error;
        }
        return false;
    }

    SettlementAccountFundsRecord funds;
    SettlementSummaryRecord summary;
    std::string funds_error;
    if (!RebuildAccountFunds(config,
                             positions,
                             final_prices,
                             instruments,
                             total_position_profit_cents,
                             &funds,
                             &summary,
                             &funds_error)) {
        if (!WriteRunStatus(config,
                            "FAILED",
                            settlement_start_ts_ns,
                            "FUNDS_REBUILD_FAILED",
                            funds_error,
                            error)) {
            return false;
        }
        if (error != nullptr) {
            *error = funds_error;
        }
        return false;
    }

    std::string write_error;
    if (!store_->UpsertAccountFunds(funds, &write_error)) {
        if (!WriteRunStatus(config,
                            "FAILED",
                            settlement_start_ts_ns,
                            "UPSERT_ACCOUNT_FUNDS_FAILED",
                            write_error,
                            error)) {
            return false;
        }
        if (error != nullptr) {
            *error = write_error;
        }
        return false;
    }

    if (!store_->AppendSummary(summary, &write_error)) {
        if (!WriteRunStatus(config,
                            "FAILED",
                            settlement_start_ts_ns,
                            "APPEND_SUMMARY_FAILED",
                            write_error,
                            error)) {
            return false;
        }
        if (error != nullptr) {
            *error = write_error;
        }
        return false;
    }

    if (!WriteRunStatus(config, "CALCULATED", settlement_start_ts_ns, "", "", error)) {
        return false;
    }

    ReconcileResult reconcile;
    std::string reconcile_error;
    if (!VerifyAgainstCTP(config, funds, &reconcile, &reconcile_error)) {
        if (!WriteRunStatus(config,
                            "FAILED",
                            settlement_start_ts_ns,
                            "RECONCILE_FAILED",
                            reconcile_error,
                            error)) {
            return false;
        }
        if (error != nullptr) {
            *error = reconcile_error;
        }
        return false;
    }

    if (reconcile.blocked) {
        for (const auto& diff : reconcile.diffs) {
            std::string diff_error;
            if (!store_->AppendReconcileDiff(diff, &diff_error)) {
                if (error != nullptr) {
                    *error = "append reconcile diff failed: " + diff_error;
                }
                return false;
            }
        }
        if (!GenerateDiffReport(config, reconcile.diffs, error)) {
            return false;
        }
        if (!WriteRunStatus(config,
                            "BLOCKED",
                            settlement_start_ts_ns,
                            "RECONCILE_MISMATCH",
                            "local state mismatch with CTP snapshot",
                            error)) {
            return false;
        }
        std::string config_error;
        (void)store_->UpsertSystemConfig("trading_mode", "BLOCKED", &config_error);
        result->success = false;
        result->blocked = true;
        result->status = "BLOCKED";
        result->message = "reconcile mismatch";
        return true;
    }

    if (!WriteCompletedRun(config, settlement_start_ts_ns, error)) {
        return false;
    }
    std::string config_error;
    (void)store_->UpsertSystemConfig("trading_mode", "TRADING", &config_error);

    result->success = true;
    result->noop = false;
    result->blocked = false;
    result->status = "COMPLETED";
    result->message = config.settlement_shadow_enabled ? "settlement completed (shadow mode)"
                                                       : "settlement completed";
    return true;
}

bool DailySettlementService::LoadSettlementPrices(
    const DailySettlementConfig& config,
    const std::vector<SettlementOpenPositionRecord>& positions,
    std::unordered_map<std::string, double>* final_prices,
    std::unordered_map<std::string, SettlementInstrumentRecord>* instruments,
    std::string* error) {
    if (final_prices == nullptr || instruments == nullptr) {
        if (error != nullptr) {
            *error = "settlement price outputs are null";
        }
        return false;
    }
    final_prices->clear();
    instruments->clear();

    std::unordered_set<std::string> instrument_set;
    for (const auto& position : positions) {
        if (!position.instrument_id.empty()) {
            instrument_set.insert(position.instrument_id);
        }
    }

    std::vector<std::string> instrument_ids(instrument_set.begin(), instrument_set.end());
    if (!store_->LoadInstruments(instrument_ids, instruments, error)) {
        return false;
    }

    std::vector<std::string> missing;
    const EpochNanos now_ts = NowEpochNanos();
    for (const auto& instrument_id : instrument_ids) {
        auto price = price_provider_->GetSettlementPrice(instrument_id, config.trading_day);
        if (!price.has_value()) {
            SettlementPriceRecord missing_record;
            missing_record.trading_day = config.trading_day;
            missing_record.instrument_id = instrument_id;
            if (const auto inst_it = instruments->find(instrument_id); inst_it != instruments->end()) {
                (void)inst_it;
            }
            missing_record.exchange_id = "";
            missing_record.source = "MISSING";
            missing_record.has_settlement_price = false;
            missing_record.is_final = false;
            missing_record.created_ts_ns = now_ts;
            std::string persist_error;
            (void)store_->AppendPrice(missing_record, &persist_error);
            missing.push_back(instrument_id);
            continue;
        }

        (*final_prices)[instrument_id] = price->first;

        SettlementPriceRecord price_record;
        price_record.trading_day = config.trading_day;
        price_record.instrument_id = instrument_id;
        price_record.exchange_id = "";
        price_record.source = [&]() {
            switch (price->second.type) {
                case SettlementPriceSource::SourceType::kApi:
                    return std::string("API");
                case SettlementPriceSource::SourceType::kExchangeFile:
                    return std::string("EXCHANGE_FILE");
                case SettlementPriceSource::SourceType::kManual:
                    return std::string("MANUAL");
                case SettlementPriceSource::SourceType::kCache:
                    return std::string("CACHE");
            }
            return std::string("UNKNOWN");
        }();
        price_record.has_settlement_price = true;
        price_record.settlement_price = price->first;
        price_record.is_final = true;
        price_record.created_ts_ns = now_ts;

        std::string persist_error;
        (void)store_->AppendPrice(price_record, &persist_error);
    }

    if (!missing.empty()) {
        if (error != nullptr) {
            std::ostringstream stream;
            stream << "missing settlement prices: ";
            for (std::size_t i = 0; i < missing.size(); ++i) {
                if (i > 0) {
                    stream << ",";
                }
                stream << missing[i];
            }
            *error = stream.str();
        }
        return false;
    }
    return true;
}

bool DailySettlementService::RunSettlementLoop(
    const DailySettlementConfig& config,
    std::vector<SettlementOpenPositionRecord>* positions,
    const std::unordered_map<std::string, double>& final_prices,
    const std::unordered_map<std::string, SettlementInstrumentRecord>& instruments,
    std::int64_t* total_position_profit_cents,
    std::string* error) {
    if (positions == nullptr || total_position_profit_cents == nullptr) {
        if (error != nullptr) {
            *error = "settlement loop outputs are null";
        }
        return false;
    }
    *total_position_profit_cents = 0;

    std::string tx_error;
    if (!store_->BeginTransaction(&tx_error)) {
        if (error != nullptr) {
            *error = "begin transaction failed: " + tx_error;
        }
        return false;
    }

    bool committed = false;
    const EpochNanos now_ts = NowEpochNanos();
    for (auto& position : *positions) {
        const auto price_it = final_prices.find(position.instrument_id);
        if (price_it == final_prices.end()) {
            std::string rollback_error;
            (void)store_->RollbackTransaction(&rollback_error);
            if (error != nullptr) {
                *error = "missing settlement price for " + position.instrument_id;
            }
            return false;
        }
        const auto instrument_it = instruments.find(position.instrument_id);
        if (instrument_it == instruments.end()) {
            std::string rollback_error;
            (void)store_->RollbackTransaction(&rollback_error);
            if (error != nullptr) {
                *error = "missing instrument meta for " + position.instrument_id;
            }
            return false;
        }

        const long double settlement_price = static_cast<long double>(price_it->second);
        const long double open_price = static_cast<long double>(position.open_price);
        const long double multiplier = static_cast<long double>(instrument_it->second.contract_multiplier);
        const long double volume = static_cast<long double>(position.volume);
        const long double raw_profit = (settlement_price - open_price) * multiplier * volume;

        const std::int64_t profit_cents = ToCents(static_cast<double>(raw_profit), FixedRoundingMode::kHalfUp);
        position.last_settlement_profit = CentsToDouble(profit_cents);
        position.accumulated_mtm = CentsToDouble(
            ToCents(position.accumulated_mtm, FixedRoundingMode::kHalfUp) + profit_cents);
        position.last_settlement_date = config.trading_day;
        position.last_settlement_price = price_it->second;
        position.open_price = price_it->second;
        position.update_ts_ns = now_ts;
        *total_position_profit_cents += profit_cents;

        std::string update_error;
        if (!store_->UpdatePositionAfterSettlement(position, &update_error)) {
            std::string rollback_error;
            (void)store_->RollbackTransaction(&rollback_error);
            if (error != nullptr) {
                *error = "update position failed: " + update_error;
            }
            return false;
        }

        SettlementDetailRecord detail;
        detail.trading_day = config.trading_day;
        detail.settlement_id = 0;
        detail.position_id = position.position_id;
        detail.instrument_id = position.instrument_id;
        detail.volume = position.volume;
        detail.settlement_price = price_it->second;
        detail.profit = position.last_settlement_profit;
        detail.created_ts_ns = now_ts;

        std::string detail_error;
        if (!store_->AppendDetail(detail, &detail_error)) {
            std::string rollback_error;
            (void)store_->RollbackTransaction(&rollback_error);
            if (error != nullptr) {
                *error = "append settlement detail failed: " + detail_error;
            }
            return false;
        }
    }

    if (!store_->CommitTransaction(&tx_error)) {
        std::string rollback_error;
        (void)store_->RollbackTransaction(&rollback_error);
        if (error != nullptr) {
            *error = "commit transaction failed: " + tx_error;
        }
        return false;
    }
    committed = true;

    if (!committed) {
        std::string rollback_error;
        (void)store_->RollbackTransaction(&rollback_error);
    }
    return true;
}

bool DailySettlementService::RolloverPositions(const DailySettlementConfig& config,
                                               std::string* error) {
    std::string detail_error;
    if (!store_->RolloverPositionDetail(config.account_id, &detail_error)) {
        if (error != nullptr) {
            *error = "rollover position_detail failed: " + detail_error;
        }
        return false;
    }

    std::string summary_error;
    if (!store_->RolloverPositionSummary(config.account_id, &summary_error)) {
        if (error != nullptr) {
            *error = "rollover position_summary failed: " + summary_error;
        }
        return false;
    }
    return true;
}

bool DailySettlementService::RebuildAccountFunds(
    const DailySettlementConfig& config,
    const std::vector<SettlementOpenPositionRecord>& positions,
    const std::unordered_map<std::string, double>& final_prices,
    const std::unordered_map<std::string, SettlementInstrumentRecord>& instruments,
    std::int64_t total_position_profit_cents,
    SettlementAccountFundsRecord* funds_out,
    SettlementSummaryRecord* summary_out,
    std::string* error) {
    if (funds_out == nullptr || summary_out == nullptr) {
        if (error != nullptr) {
            *error = "fund outputs are null";
        }
        return false;
    }

    const std::string previous_day = PreviousTradingDay(config.trading_day);
    SettlementAccountFundsRecord previous;
    std::string load_error;
    if (!store_->LoadAccountFunds(config.account_id, previous_day, &previous, &load_error)) {
        if (error != nullptr) {
            *error = "load previous funds failed: " + load_error;
        }
        return false;
    }

    double deposit = 0.0;
    double withdraw = 0.0;
    double commission = 0.0;
    double close_profit = 0.0;
    if (!store_->SumDeposit(config.account_id, config.trading_day, &deposit, &load_error) ||
        !store_->SumWithdraw(config.account_id, config.trading_day, &withdraw, &load_error) ||
        !store_->SumCommission(config.account_id, config.trading_day, &commission, &load_error) ||
        !store_->SumCloseProfit(config.account_id, config.trading_day, &close_profit, &load_error)) {
        if (error != nullptr) {
            *error = "sum account deltas failed: " + load_error;
        }
        return false;
    }

    const std::int64_t pre_balance_cents = ToCents(previous.balance, FixedRoundingMode::kHalfUp);
    const std::int64_t deposit_cents = ToCents(deposit, FixedRoundingMode::kHalfUp);
    const std::int64_t withdraw_cents = ToCents(withdraw, FixedRoundingMode::kHalfUp);
    const std::int64_t commission_cents = ToCents(commission, FixedRoundingMode::kHalfUp);
    const std::int64_t close_profit_cents = ToCents(close_profit, FixedRoundingMode::kHalfUp);

    const std::int64_t balance_cents = pre_balance_cents + deposit_cents - withdraw_cents +
                                       close_profit_cents + total_position_profit_cents -
                                       commission_cents;

    std::int64_t margin_cents = 0;
    for (const auto& position : positions) {
        const auto price_it = final_prices.find(position.instrument_id);
        if (price_it == final_prices.end()) {
            continue;
        }
        const auto instrument_it = instruments.find(position.instrument_id);
        if (instrument_it == instruments.end()) {
            continue;
        }
        const auto& instrument = instrument_it->second;
        long double margin_rate = static_cast<long double>(instrument.long_margin_rate);
        if (position.volume < 0) {
            margin_rate = static_cast<long double>(instrument.short_margin_rate);
        }
        if (margin_rate < 0.0L) {
            margin_rate = 0.0L;
        }
        const long double raw_margin = std::fabs(static_cast<long double>(position.volume)) *
                                       static_cast<long double>(instrument.contract_multiplier) *
                                       static_cast<long double>(price_it->second) * margin_rate;
        margin_cents += RoundScaled6ToCents(raw_margin, FixedRoundingMode::kUp);
    }

    const std::int64_t available_cents = balance_cents - margin_cents;
    double risk_degree = 0.0;
    if (margin_cents > 0) {
        const long double raw_risk = static_cast<long double>(balance_cents) /
                                     static_cast<long double>(margin_cents);
        const auto scaled = FixedDecimal::ToScaled(raw_risk, 4, FixedRoundingMode::kHalfUp);
        risk_degree = static_cast<double>(FixedDecimal::ToLongDouble(scaled, 4));
    }

    SettlementAccountFundsRecord funds;
    funds.exists = true;
    funds.account_id = config.account_id;
    funds.trading_day = config.trading_day;
    funds.pre_balance = CentsToDouble(pre_balance_cents);
    funds.deposit = CentsToDouble(deposit_cents);
    funds.withdraw = CentsToDouble(withdraw_cents);
    funds.frozen_commission = 0.0;
    funds.frozen_margin = 0.0;
    funds.available = CentsToDouble(available_cents);
    funds.curr_margin = CentsToDouble(margin_cents);
    funds.commission = CentsToDouble(commission_cents);
    funds.close_profit = CentsToDouble(close_profit_cents);
    funds.position_profit = CentsToDouble(total_position_profit_cents);
    funds.balance = CentsToDouble(balance_cents);
    funds.risk_degree = risk_degree;
    funds.pre_settlement_balance = CentsToDouble(pre_balance_cents);
    funds.floating_profit = 0.0;
    funds.update_ts_ns = NowEpochNanos();

    SettlementSummaryRecord summary;
    summary.trading_day = config.trading_day;
    summary.account_id = config.account_id;
    summary.pre_balance = funds.pre_balance;
    summary.deposit = funds.deposit;
    summary.withdraw = funds.withdraw;
    summary.commission = funds.commission;
    summary.close_profit = funds.close_profit;
    summary.position_profit = funds.position_profit;
    summary.balance = funds.balance;
    summary.curr_margin = funds.curr_margin;
    summary.available = funds.available;
    summary.risk_degree = funds.risk_degree;
    summary.created_ts_ns = funds.update_ts_ns;

    *funds_out = funds;
    *summary_out = summary;
    return true;
}

bool DailySettlementService::VerifyAgainstCTP(const DailySettlementConfig& config,
                                              const SettlementAccountFundsRecord& local_funds,
                                              ReconcileResult* reconcile_result,
                                              std::string* error) {
    if (reconcile_result == nullptr) {
        if (error != nullptr) {
            *error = "reconcile result pointer is null";
        }
        return false;
    }
    *reconcile_result = ReconcileResult{};

    TradingAccountSnapshot ctp_account;
    std::string query_error;
    if (!query_client_->GetLastTradingAccountSnapshot(&ctp_account, &query_error)) {
        SettlementReconcileDiffRecord diff;
        diff.trading_day = config.trading_day;
        diff.account_id = config.account_id;
        diff.diff_type = "QUERY_ERROR";
        diff.key_ref = "ctp_trading_account";
        diff.local_value = 0.0;
        diff.ctp_value = 0.0;
        diff.delta_value = 0.0;
        diff.diagnose_hint = query_error;
        diff.raw_payload = "{}";
        diff.created_ts_ns = NowEpochNanos();
        reconcile_result->diffs.push_back(std::move(diff));
        reconcile_result->blocked = true;
        return true;
    }

    std::vector<InvestorPositionSnapshot> ctp_positions;
    if (!query_client_->GetLastInvestorPositionSnapshots(&ctp_positions, &query_error)) {
        SettlementReconcileDiffRecord diff;
        diff.trading_day = config.trading_day;
        diff.account_id = config.account_id;
        diff.diff_type = "QUERY_ERROR";
        diff.key_ref = "ctp_investor_position";
        diff.local_value = 0.0;
        diff.ctp_value = 0.0;
        diff.delta_value = 0.0;
        diff.diagnose_hint = query_error;
        diff.raw_payload = "{}";
        diff.created_ts_ns = NowEpochNanos();
        reconcile_result->diffs.push_back(std::move(diff));
        reconcile_result->blocked = true;
        return true;
    }

    auto append_funds_diff = [&](const std::string& key, double local_value, double ctp_value) {
        const auto local_cents = ToCents(local_value, FixedRoundingMode::kHalfUp);
        const auto ctp_cents = ToCents(ctp_value, FixedRoundingMode::kHalfUp);
        const auto delta_cents = local_cents - ctp_cents;
        if (delta_cents == 0) {
            return;
        }
        SettlementReconcileDiffRecord diff;
        diff.trading_day = config.trading_day;
        diff.account_id = config.account_id;
        diff.diff_type = "FUNDS";
        diff.key_ref = key;
        diff.local_value = CentsToDouble(local_cents);
        diff.ctp_value = CentsToDouble(ctp_cents);
        diff.delta_value = CentsToDouble(delta_cents);
        diff.diagnose_hint = "check settlement and late trade backfill";
        diff.raw_payload = "{}";
        diff.created_ts_ns = NowEpochNanos();
        reconcile_result->diffs.push_back(std::move(diff));
    };

    append_funds_diff("balance", local_funds.balance, ctp_account.balance);
    append_funds_diff("available", local_funds.available, ctp_account.available);
    append_funds_diff("curr_margin", local_funds.curr_margin, ctp_account.curr_margin);

    std::vector<SettlementPositionSummaryRecord> local_summary;
    std::string load_error;
    if (!store_->LoadPositionSummary(config.account_id, &local_summary, &load_error)) {
        if (error != nullptr) {
            *error = "load local position summary failed: " + load_error;
        }
        return false;
    }

    std::unordered_map<std::string, PositionAgg> local_agg;
    for (const auto& item : local_summary) {
        auto& agg = local_agg[item.instrument_id];
        agg.long_position += item.long_volume;
        agg.short_position += item.short_volume;
        agg.long_today += item.long_today_volume;
        agg.short_today += item.short_today_volume;
        agg.long_yd += item.long_yd_volume;
        agg.short_yd += item.short_yd_volume;
    }

    std::unordered_map<std::string, PositionAgg> ctp_agg;
    for (const auto& item : ctp_positions) {
        auto& agg = ctp_agg[item.instrument_id];
        const bool is_long = item.posi_direction == "2" || item.posi_direction == "L" ||
                             item.posi_direction == "l";
        if (is_long) {
            agg.long_position += item.position;
            agg.long_today += item.today_position;
            agg.long_yd += item.yd_position;
        } else {
            agg.short_position += item.position;
            agg.short_today += item.today_position;
            agg.short_yd += item.yd_position;
        }
    }

    std::unordered_set<std::string> instruments;
    for (const auto& [instrument_id, _] : local_agg) {
        (void)_;
        instruments.insert(instrument_id);
    }
    for (const auto& [instrument_id, _] : ctp_agg) {
        (void)_;
        instruments.insert(instrument_id);
    }

    auto append_position_diff = [&](const std::string& instrument_id,
                                    const std::string& field,
                                    int local_value,
                                    int ctp_value) {
        if (local_value == ctp_value) {
            return;
        }
        SettlementReconcileDiffRecord diff;
        diff.trading_day = config.trading_day;
        diff.account_id = config.account_id;
        diff.diff_type = "POSITION";
        diff.key_ref = instrument_id + ":" + field;
        diff.local_value = static_cast<double>(local_value);
        diff.ctp_value = static_cast<double>(ctp_value);
        diff.delta_value = static_cast<double>(local_value - ctp_value);
        diff.diagnose_hint = "check order/trade replay and offset mapping";
        diff.raw_payload = "{}";
        diff.created_ts_ns = NowEpochNanos();
        reconcile_result->diffs.push_back(std::move(diff));
    };

    for (const auto& instrument_id : instruments) {
        const auto local_it = local_agg.find(instrument_id);
        const auto ctp_it = ctp_agg.find(instrument_id);
        const PositionAgg local = local_it == local_agg.end() ? PositionAgg{} : local_it->second;
        const PositionAgg ctp = ctp_it == ctp_agg.end() ? PositionAgg{} : ctp_it->second;

        append_position_diff(instrument_id, "long_position", local.long_position, ctp.long_position);
        append_position_diff(instrument_id, "short_position", local.short_position, ctp.short_position);
        append_position_diff(instrument_id, "long_today", local.long_today, ctp.long_today);
        append_position_diff(instrument_id, "short_today", local.short_today, ctp.short_today);
        append_position_diff(instrument_id, "long_yd", local.long_yd, ctp.long_yd);
        append_position_diff(instrument_id, "short_yd", local.short_yd, ctp.short_yd);
    }

    reconcile_result->blocked = !reconcile_result->diffs.empty();
    reconcile_result->passed = !reconcile_result->blocked;
    return true;
}

bool DailySettlementService::GenerateDiffReport(
    const DailySettlementConfig& config,
    const std::vector<SettlementReconcileDiffRecord>& diffs,
    std::string* error) const {
    std::string path = config.diff_report_path;
    if (path.empty()) {
        path = "docs/results/settlement_diff_" + config.trading_day + ".json";
    }

    std::filesystem::path output(path);
    std::error_code ec;
    std::filesystem::create_directories(output.parent_path(), ec);

    std::ofstream out(output);
    if (!out.is_open()) {
        if (error != nullptr) {
            *error = "unable to write diff report: " + output.string();
        }
        return false;
    }

    out << "{\n";
    out << "  \"trading_day\": \"" << EscapeJson(config.trading_day) << "\",\n";
    out << "  \"account_id\": \"" << EscapeJson(config.account_id) << "\",\n";
    out << "  \"generated_at_ns\": " << NowEpochNanos() << ",\n";
    out << "  \"diff_count\": " << diffs.size() << ",\n";
    out << "  \"diffs\": [\n";
    for (std::size_t i = 0; i < diffs.size(); ++i) {
        const auto& diff = diffs[i];
        out << "    {\n";
        out << "      \"diff_type\": \"" << EscapeJson(diff.diff_type) << "\",\n";
        out << "      \"key_ref\": \"" << EscapeJson(diff.key_ref) << "\",\n";
        out << "      \"local_value\": " << JsonNumber(diff.local_value) << ",\n";
        out << "      \"ctp_value\": " << JsonNumber(diff.ctp_value) << ",\n";
        out << "      \"delta_value\": " << JsonNumber(diff.delta_value) << ",\n";
        out << "      \"diagnose_hint\": \"" << EscapeJson(diff.diagnose_hint) << "\"\n";
        out << "    }" << (i + 1 == diffs.size() ? "\n" : ",\n");
    }
    out << "  ]\n";
    out << "}\n";
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
    run.status = NormalizeRunStatus(status);
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

std::string DailySettlementService::NormalizeRunStatus(const std::string& status) {
    if (status == "RUNNING") {
        return "RECONCILING";
    }
    return status;
}

bool DailySettlementService::IsRunTerminalStatus(const std::string& status) {
    return status == "COMPLETED" || status == "BLOCKED" || status == "FAILED";
}

std::string DailySettlementService::PreviousTradingDay(const std::string& trading_day) {
    std::tm tm_utc{};
    if (!ParseDate(trading_day, &tm_utc)) {
        return trading_day;
    }
    const auto day_seconds = TimegmPortable(&tm_utc) - 24 * 3600;
    std::time_t ts = static_cast<std::time_t>(day_seconds);
    std::tm prev{};
#if defined(_WIN32)
    gmtime_s(&prev, &ts);
#else
    gmtime_r(&ts, &prev);
#endif
    char buffer[11] = {0};
    if (std::strftime(buffer, sizeof(buffer), "%Y-%m-%d", &prev) == 0) {
        return trading_day;
    }
    return std::string(buffer);
}

std::string DailySettlementService::DateFromEpochNanos(EpochNanos ts_ns) {
    const std::time_t seconds = static_cast<std::time_t>(ts_ns / 1'000'000'000LL);
    std::tm utc_tm{};
#if defined(_WIN32)
    gmtime_s(&utc_tm, &seconds);
#else
    gmtime_r(&seconds, &utc_tm);
#endif
    char buffer[11] = {0};
    if (std::strftime(buffer, sizeof(buffer), "%Y-%m-%d", &utc_tm) == 0) {
        return "1970-01-01";
    }
    return std::string(buffer);
}

std::string DailySettlementService::EscapeJson(const std::string& raw) {
    std::string out;
    out.reserve(raw.size() + 8);
    for (char ch : raw) {
        switch (ch) {
            case '\\':
                out += "\\\\";
                break;
            case '"':
                out += "\\\"";
                break;
            case '\n':
                out += "\\n";
                break;
            case '\r':
                out += "\\r";
                break;
            case '\t':
                out += "\\t";
                break;
            default:
                out.push_back(ch);
                break;
        }
    }
    return out;
}

double DailySettlementService::CentsToDouble(std::int64_t cents) {
    return static_cast<double>(FixedDecimal::ToLongDouble(cents, 2));
}

std::int64_t DailySettlementService::ToCents(double value, FixedRoundingMode mode) {
    return FixedDecimal::ToScaled(static_cast<long double>(value), 2, mode);
}

std::int64_t DailySettlementService::RoundScaled6ToCents(long double value,
                                                         FixedRoundingMode mode) {
    const auto scaled_6 = FixedDecimal::ToScaled(value, 6, FixedRoundingMode::kHalfUp);
    return FixedDecimal::Rescale(scaled_6, 6, 2, mode);
}

}  // namespace quant_hft
