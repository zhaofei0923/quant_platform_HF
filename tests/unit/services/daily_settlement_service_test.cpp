#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <gtest/gtest.h>

#include "quant_hft/core/ctp_trader_adapter.h"
#include "quant_hft/core/flow_controller.h"
#include "quant_hft/interfaces/settlement_store.h"
#include "quant_hft/interfaces/trading_domain_store.h"
#include "quant_hft/services/daily_settlement_service.h"
#include "quant_hft/services/settlement_price_provider.h"
#include "quant_hft/services/settlement_query_client.h"

namespace quant_hft {
namespace {

class FakeSettlementStore : public ISettlementStore {
public:
    SettlementRunRecord existing_run;
    std::vector<SettlementRunRecord> upserted_runs;
    std::vector<SettlementSummaryRecord> summaries;
    std::vector<SettlementDetailRecord> details;
    std::vector<SettlementPriceRecord> prices;
    std::vector<SettlementReconcileDiffRecord> diffs;
    std::vector<SettlementOpenPositionRecord> open_positions;
    std::unordered_map<std::string, SettlementInstrumentRecord> instruments;
    std::unordered_map<std::string, SettlementAccountFundsRecord> funds_by_day;
    std::vector<SettlementAccountFundsRecord> upserted_funds;
    std::vector<SettlementPositionSummaryRecord> position_summary;
    std::vector<SettlementOrderKey> order_keys;
    std::vector<std::string> trade_ids;
    std::unordered_map<std::string, std::string> system_config;
    std::unordered_map<std::string, double> deposit_sum;
    std::unordered_map<std::string, double> withdraw_sum;
    std::unordered_map<std::string, double> commission_sum;
    std::unordered_map<std::string, double> close_profit_sum;

    bool BeginTransaction(std::string* error) override {
        (void)error;
        in_transaction = true;
        return true;
    }

    bool CommitTransaction(std::string* error) override {
        (void)error;
        in_transaction = false;
        return true;
    }

    bool RollbackTransaction(std::string* error) override {
        (void)error;
        in_transaction = false;
        return true;
    }

    bool GetRun(const std::string& trading_day,
                SettlementRunRecord* out,
                std::string* error) const override {
        (void)error;
        if (out == nullptr) {
            return false;
        }
        if (!existing_run.trading_day.empty() && existing_run.trading_day == trading_day) {
            *out = existing_run;
        } else {
            *out = SettlementRunRecord{};
        }
        return true;
    }

    bool UpsertRun(const SettlementRunRecord& run, std::string* error) override {
        (void)error;
        upserted_runs.push_back(run);
        existing_run = run;
        return true;
    }

    bool AppendSummary(const SettlementSummaryRecord& summary, std::string* error) override {
        (void)error;
        summaries.push_back(summary);
        return true;
    }

    bool AppendDetail(const SettlementDetailRecord& detail, std::string* error) override {
        (void)error;
        details.push_back(detail);
        return true;
    }

    bool AppendPrice(const SettlementPriceRecord& price, std::string* error) override {
        (void)error;
        prices.push_back(price);
        return true;
    }

    bool AppendReconcileDiff(const SettlementReconcileDiffRecord& diff,
                             std::string* error) override {
        (void)error;
        diffs.push_back(diff);
        return true;
    }

    bool LoadOpenPositions(const std::string& account_id,
                           std::vector<SettlementOpenPositionRecord>* out,
                           std::string* error) const override {
        (void)error;
        if (out == nullptr) {
            return false;
        }
        out->clear();
        for (const auto& position : open_positions) {
            if (position.account_id == account_id && position.position_status == 1) {
                out->push_back(position);
            }
        }
        return true;
    }

    bool LoadInstruments(const std::vector<std::string>& instrument_ids,
                         std::unordered_map<std::string, SettlementInstrumentRecord>* out,
                         std::string* error) const override {
        (void)error;
        if (out == nullptr) {
            return false;
        }
        out->clear();
        for (const auto& instrument_id : instrument_ids) {
            const auto it = instruments.find(instrument_id);
            if (it != instruments.end()) {
                (*out)[instrument_id] = it->second;
            }
        }
        return true;
    }

    bool UpdatePositionAfterSettlement(const SettlementOpenPositionRecord& position,
                                       std::string* error) override {
        (void)error;
        for (auto& item : open_positions) {
            if (item.position_id == position.position_id) {
                item = position;
                return true;
            }
        }
        open_positions.push_back(position);
        return true;
    }

    bool RolloverPositionDetail(const std::string& account_id, std::string* error) override {
        (void)error;
        for (auto& position : open_positions) {
            if (position.account_id == account_id && position.position_status == 1) {
                position.is_today = false;
            }
        }
        return true;
    }

    bool RolloverPositionSummary(const std::string& account_id, std::string* error) override {
        (void)error;
        for (auto& summary : position_summary) {
            if (summary.account_id != account_id) {
                continue;
            }
            summary.long_yd_volume += summary.long_today_volume;
            summary.short_yd_volume += summary.short_today_volume;
            summary.long_today_volume = 0;
            summary.short_today_volume = 0;
        }
        return true;
    }

    bool LoadAccountFunds(const std::string& account_id,
                          const std::string& trading_day,
                          SettlementAccountFundsRecord* out,
                          std::string* error) const override {
        (void)error;
        if (out == nullptr) {
            return false;
        }
        *out = SettlementAccountFundsRecord{};
        out->account_id = account_id;
        out->trading_day = trading_day;
        if (const auto it = funds_by_day.find(trading_day); it != funds_by_day.end()) {
            *out = it->second;
            out->exists = true;
        }
        return true;
    }

    bool SumDeposit(const std::string& account_id,
                    const std::string& trading_day,
                    double* out,
                    std::string* error) const override {
        (void)account_id;
        (void)error;
        if (out == nullptr) {
            return false;
        }
        *out = 0.0;
        if (const auto it = deposit_sum.find(trading_day); it != deposit_sum.end()) {
            *out = it->second;
        }
        return true;
    }

    bool SumWithdraw(const std::string& account_id,
                     const std::string& trading_day,
                     double* out,
                     std::string* error) const override {
        (void)account_id;
        (void)error;
        if (out == nullptr) {
            return false;
        }
        *out = 0.0;
        if (const auto it = withdraw_sum.find(trading_day); it != withdraw_sum.end()) {
            *out = it->second;
        }
        return true;
    }

    bool SumCommission(const std::string& account_id,
                       const std::string& trading_day,
                       double* out,
                       std::string* error) const override {
        (void)account_id;
        (void)error;
        if (out == nullptr) {
            return false;
        }
        *out = 0.0;
        if (const auto it = commission_sum.find(trading_day); it != commission_sum.end()) {
            *out = it->second;
        }
        return true;
    }

    bool SumCloseProfit(const std::string& account_id,
                        const std::string& trading_day,
                        double* out,
                        std::string* error) const override {
        (void)account_id;
        (void)error;
        if (out == nullptr) {
            return false;
        }
        *out = 0.0;
        if (const auto it = close_profit_sum.find(trading_day); it != close_profit_sum.end()) {
            *out = it->second;
        }
        return true;
    }

    bool UpsertAccountFunds(const SettlementAccountFundsRecord& funds, std::string* error) override {
        (void)error;
        funds_by_day[funds.trading_day] = funds;
        upserted_funds.push_back(funds);
        return true;
    }

    bool LoadPositionSummary(const std::string& account_id,
                             std::vector<SettlementPositionSummaryRecord>* out,
                             std::string* error) const override {
        (void)error;
        if (out == nullptr) {
            return false;
        }
        out->clear();
        for (const auto& row : position_summary) {
            if (row.account_id == account_id) {
                out->push_back(row);
            }
        }
        return true;
    }

    bool LoadOrderKeysByDay(const std::string& account_id,
                            const std::string& trading_day,
                            std::vector<SettlementOrderKey>* out,
                            std::string* error) const override {
        (void)account_id;
        (void)trading_day;
        (void)error;
        if (out == nullptr) {
            return false;
        }
        *out = order_keys;
        return true;
    }

    bool LoadTradeIdsByDay(const std::string& account_id,
                           const std::string& trading_day,
                           std::vector<std::string>* out,
                           std::string* error) const override {
        (void)account_id;
        (void)trading_day;
        (void)error;
        if (out == nullptr) {
            return false;
        }
        *out = trade_ids;
        return true;
    }

    bool UpsertSystemConfig(const std::string& key,
                            const std::string& value,
                            std::string* error) override {
        (void)error;
        system_config[key] = value;
        return true;
    }

    bool in_transaction{false};
};

class FakePriceProvider : public SettlementPriceProvider {
public:
    std::optional<std::pair<double, SettlementPriceSource>> GetSettlementPrice(
        const std::string& instrument_id,
        const std::string& trading_day) override {
        const auto manual_it = manual.find(trading_day + "|" + instrument_id);
        if (manual_it != manual.end()) {
            return std::make_pair(manual_it->second,
                                  SettlementPriceSource{SettlementPriceSource::SourceType::kManual,
                                                        "manual"});
        }
        const auto it = prices.find(trading_day + "|" + instrument_id);
        if (it == prices.end()) {
            return std::nullopt;
        }
        return std::make_pair(it->second,
                              SettlementPriceSource{SettlementPriceSource::SourceType::kApi,
                                                    "api"});
    }

    std::unordered_map<std::string, std::pair<double, SettlementPriceSource>> BatchGetSettlementPrices(
        const std::vector<std::string>& instrument_ids,
        const std::string& trading_day) override {
        std::unordered_map<std::string, std::pair<double, SettlementPriceSource>> result;
        for (const auto& instrument_id : instrument_ids) {
            auto price = GetSettlementPrice(instrument_id, trading_day);
            if (price.has_value()) {
                result[instrument_id] = *price;
            }
        }
        return result;
    }

    void SetManualOverride(const std::string& instrument_id,
                           const std::string& trading_day,
                           double price,
                           const std::string& operator_id) override {
        (void)operator_id;
        manual[trading_day + "|" + instrument_id] = price;
    }

    std::unordered_map<std::string, double> prices;
    std::unordered_map<std::string, double> manual;
};

class FakeTradingDomainStore : public ITradingDomainStore {
public:
    bool UpsertOrder(const Order& order, std::string* error) override {
        (void)error;
        orders.push_back(order);
        return true;
    }

    bool AppendTrade(const Trade& trade, std::string* error) override {
        (void)error;
        trades.push_back(trade);
        return true;
    }

    bool UpsertPosition(const Position& position, std::string* error) override {
        (void)position;
        (void)error;
        return true;
    }

    bool UpsertAccount(const Account& account, std::string* error) override {
        (void)account;
        (void)error;
        return true;
    }

    bool AppendRiskEvent(const RiskEventRecord& risk_event, std::string* error) override {
        (void)risk_event;
        (void)error;
        return true;
    }

    bool MarkProcessedOrderEvent(const ProcessedOrderEventRecord& event,
                                 std::string* error) override {
        (void)event;
        (void)error;
        return true;
    }

    bool ExistsProcessedOrderEvent(const std::string& event_key,
                                   bool* exists,
                                   std::string* error) const override {
        (void)event_key;
        (void)error;
        if (exists != nullptr) {
            *exists = false;
        }
        return true;
    }

    bool InsertPositionDetailFromTrade(const Trade& trade, std::string* error) override {
        (void)trade;
        (void)error;
        return true;
    }

    bool ClosePositionDetailFifo(const Trade& trade, std::string* error) override {
        (void)trade;
        (void)error;
        return true;
    }

    bool LoadPositionSummary(const std::string& account_id,
                             const std::string& strategy_id,
                             std::vector<Position>* out,
                             std::string* error) const override {
        (void)account_id;
        (void)strategy_id;
        (void)error;
        if (out != nullptr) {
            out->clear();
        }
        return true;
    }

    bool UpdateOrderCancelRetry(const std::string& client_order_id,
                                std::int32_t cancel_retry_count,
                                EpochNanos last_cancel_ts_ns,
                                std::string* error) override {
        (void)client_order_id;
        (void)cancel_retry_count;
        (void)last_cancel_ts_ns;
        (void)error;
        return true;
    }

    std::vector<Order> orders;
    std::vector<Trade> trades;
};

MarketDataConnectConfig BuildSimConfig() {
    MarketDataConnectConfig cfg;
    cfg.market_front_address = "tcp://sim-md";
    cfg.trader_front_address = "tcp://sim-td";
    cfg.broker_id = "9999";
    cfg.user_id = "191202";
    cfg.investor_id = "191202";
    cfg.password = "pwd";
    cfg.is_production_mode = false;
    return cfg;
}

struct QueryClientBundle {
    std::shared_ptr<CTPTraderAdapter> trader;
    std::shared_ptr<SettlementQueryClient> query_client;
};

QueryClientBundle BuildConnectedQueryClient() {
    QueryClientBundle bundle;
    bundle.trader = std::make_shared<CTPTraderAdapter>(10, 1);
    EXPECT_TRUE(bundle.trader->Connect(BuildSimConfig()));
    EXPECT_TRUE(bundle.trader->ConfirmSettlement());

    auto flow = std::make_shared<FlowController>();
    FlowRule query_rule;
    query_rule.account_id = "191202";
    query_rule.type = OperationType::kSettlementQuery;
    query_rule.rate_per_second = 10.0;
    query_rule.capacity = 5;
    flow->AddRule(query_rule);

    SettlementQueryClientConfig query_cfg;
    query_cfg.account_id = "191202";
    query_cfg.retry_max = 1;
    query_cfg.backoff_initial_ms = 1;
    query_cfg.backoff_max_ms = 1;
    query_cfg.acquire_timeout_ms = 10;
    bundle.query_client = std::make_shared<SettlementQueryClient>(bundle.trader, flow, query_cfg);
    return bundle;
}

DailySettlementConfig BaseConfig(const std::string& account_id = "acc1",
                                 const std::string& trading_day = "2026-02-12") {
    DailySettlementConfig cfg;
    cfg.account_id = account_id;
    cfg.trading_day = trading_day;
    cfg.running_stale_timeout_ms = 10'000;
    cfg.diff_report_path = "runtime/settlement_diff_test.json";
    return cfg;
}

std::shared_ptr<SettlementQueryClient> BuildFailingQueryClient() {
    SettlementQueryClientConfig cfg;
    cfg.account_id = "acc1";
    return std::make_shared<SettlementQueryClient>(nullptr, nullptr, cfg);
}

}  // namespace

TEST(DailySettlementServiceTest, SettlementPriceMissingLeadsToPendingPrice) {
    auto store = std::make_shared<FakeSettlementStore>();
    auto price = std::make_shared<FakePriceProvider>();
    auto query_client = BuildFailingQueryClient();

    SettlementOpenPositionRecord position;
    position.position_id = 1;
    position.account_id = "acc1";
    position.instrument_id = "rb2405";
    position.strategy_id = "s1";
    position.exchange_id = "SHFE";
    position.open_date = "2026-02-12";
    position.position_date = "2026-02-12";
    position.volume = 1;
    position.open_price = 3800.0;
    position.position_status = 1;
    store->open_positions.push_back(position);

    SettlementInstrumentRecord instrument;
    instrument.instrument_id = "rb2405";
    instrument.contract_multiplier = 10;
    store->instruments["rb2405"] = instrument;

    DailySettlementService service(price, store, query_client);

    DailySettlementResult result;
    std::string error;
    ASSERT_TRUE(service.Run(BaseConfig(), &result, &error)) << error;
    EXPECT_FALSE(result.success);
    EXPECT_TRUE(result.blocked);
    EXPECT_EQ(result.status, "PENDING_PRICE");
    ASSERT_FALSE(store->prices.empty());
    EXPECT_EQ(store->prices.back().source, "MISSING");
    EXPECT_FALSE(store->prices.back().has_settlement_price);
}

TEST(DailySettlementServiceTest, SettlementLoopUpdatesPositionsAndProfit) {
    auto store = std::make_shared<FakeSettlementStore>();
    auto price = std::make_shared<FakePriceProvider>();
    auto query_client = BuildFailingQueryClient();

    SettlementOpenPositionRecord position;
    position.position_id = 11;
    position.account_id = "acc1";
    position.strategy_id = "s1";
    position.instrument_id = "rb2405";
    position.exchange_id = "SHFE";
    position.open_date = "2026-02-12";
    position.position_date = "2026-02-12";
    position.volume = 2;
    position.open_price = 100.0;
    position.position_status = 1;
    store->open_positions.push_back(position);

    SettlementInstrumentRecord instrument;
    instrument.instrument_id = "rb2405";
    instrument.contract_multiplier = 10;
    instrument.long_margin_rate = 0.0;
    instrument.short_margin_rate = 0.0;
    store->instruments["rb2405"] = instrument;
    price->prices["2026-02-12|rb2405"] = 102.0;

    SettlementAccountFundsRecord prev;
    prev.exists = true;
    prev.account_id = "acc1";
    prev.trading_day = "2026-02-11";
    prev.balance = -40.0;
    store->funds_by_day["2026-02-11"] = prev;

    DailySettlementService service(price, store, query_client);

    DailySettlementResult result;
    std::string error;
    ASSERT_TRUE(service.Run(BaseConfig(), &result, &error)) << error;
    ASSERT_EQ(store->open_positions.size(), 1U);
    EXPECT_DOUBLE_EQ(store->open_positions[0].open_price, 102.0);
    EXPECT_DOUBLE_EQ(store->open_positions[0].last_settlement_profit, 40.0);
    EXPECT_DOUBLE_EQ(store->open_positions[0].accumulated_mtm, 40.0);
    EXPECT_EQ(store->open_positions[0].last_settlement_date, "2026-02-12");
    ASSERT_EQ(store->details.size(), 1U);
    ASSERT_FALSE(store->upserted_funds.empty());
    EXPECT_DOUBLE_EQ(store->upserted_funds.back().position_profit, 40.0);
}

TEST(DailySettlementServiceTest, RolloverUpdatesPositionSummary) {
    auto store = std::make_shared<FakeSettlementStore>();
    auto price = std::make_shared<FakePriceProvider>();
    auto query_client = BuildFailingQueryClient();

    SettlementPositionSummaryRecord summary;
    summary.account_id = "acc1";
    summary.strategy_id = "s1";
    summary.instrument_id = "rb2405";
    summary.long_volume = 5;
    summary.short_volume = 0;
    summary.long_today_volume = 2;
    summary.short_today_volume = 0;
    summary.long_yd_volume = 3;
    summary.short_yd_volume = 0;
    store->position_summary.push_back(summary);

    SettlementAccountFundsRecord prev;
    prev.exists = true;
    prev.account_id = "acc1";
    prev.trading_day = "2026-02-11";
    prev.balance = 0.0;
    store->funds_by_day["2026-02-11"] = prev;

    DailySettlementService service(price, store, query_client);

    DailySettlementResult result;
    std::string error;
    ASSERT_TRUE(service.Run(BaseConfig(), &result, &error)) << error;
    ASSERT_EQ(store->position_summary.size(), 1U);
    EXPECT_EQ(store->position_summary[0].long_today_volume, 0);
    EXPECT_EQ(store->position_summary[0].long_yd_volume, 5);
}

TEST(DailySettlementServiceTest, FundsInsertedCorrectlyAfterSettlement) {
    auto store = std::make_shared<FakeSettlementStore>();
    auto price = std::make_shared<FakePriceProvider>();
    auto query_client = BuildFailingQueryClient();

    SettlementOpenPositionRecord position;
    position.position_id = 21;
    position.account_id = "acc1";
    position.strategy_id = "s1";
    position.instrument_id = "rb2405";
    position.exchange_id = "SHFE";
    position.open_date = "2026-02-12";
    position.position_date = "2026-02-12";
    position.volume = 1;
    position.open_price = 100.0;
    position.position_status = 1;
    store->open_positions.push_back(position);

    SettlementInstrumentRecord instrument;
    instrument.instrument_id = "rb2405";
    instrument.contract_multiplier = 10;
    instrument.long_margin_rate = 0.0;
    instrument.short_margin_rate = 0.0;
    store->instruments["rb2405"] = instrument;
    price->prices["2026-02-12|rb2405"] = 102.0;

    SettlementAccountFundsRecord prev;
    prev.exists = true;
    prev.account_id = "acc1";
    prev.trading_day = "2026-02-11";
    prev.balance = 100.0;
    store->funds_by_day["2026-02-11"] = prev;

    store->deposit_sum["2026-02-12"] = 10.0;
    store->withdraw_sum["2026-02-12"] = 5.0;
    store->commission_sum["2026-02-12"] = 2.0;
    store->close_profit_sum["2026-02-12"] = 7.0;

    DailySettlementService service(price, store, query_client);

    DailySettlementResult result;
    std::string error;
    ASSERT_TRUE(service.Run(BaseConfig(), &result, &error)) << error;
    ASSERT_FALSE(store->upserted_funds.empty());
    const auto& funds = store->upserted_funds.back();
    EXPECT_DOUBLE_EQ(funds.balance, 130.0);
    EXPECT_DOUBLE_EQ(funds.available, 130.0);
    EXPECT_DOUBLE_EQ(funds.curr_margin, 0.0);
    EXPECT_DOUBLE_EQ(funds.position_profit, 20.0);
}

TEST(DailySettlementServiceTest, ReconcileDetectsMismatchAndBlocks) {
    auto store = std::make_shared<FakeSettlementStore>();
    auto price = std::make_shared<FakePriceProvider>();
    auto bundle = BuildConnectedQueryClient();

    SettlementAccountFundsRecord prev;
    prev.exists = true;
    prev.account_id = "191202";
    prev.trading_day = "2026-02-11";
    prev.balance = 100.0;
    store->funds_by_day["2026-02-11"] = prev;

    DailySettlementService service(price, store, bundle.query_client);

    DailySettlementResult result;
    std::string error;
    ASSERT_TRUE(service.Run(BaseConfig("191202"), &result, &error)) << error;
    EXPECT_FALSE(result.success);
    EXPECT_TRUE(result.blocked);
    EXPECT_EQ(result.status, "BLOCKED");
    EXPECT_FALSE(store->diffs.empty());

    bundle.trader->Disconnect();
}

TEST(DailySettlementServiceTest, ReconcilePassesAndCompletes) {
    auto store = std::make_shared<FakeSettlementStore>();
    auto price = std::make_shared<FakePriceProvider>();
    auto bundle = BuildConnectedQueryClient();

    SettlementAccountFundsRecord prev;
    prev.exists = true;
    prev.account_id = "191202";
    prev.trading_day = "2026-02-11";
    prev.balance = 0.0;
    store->funds_by_day["2026-02-11"] = prev;

    DailySettlementService service(price, store, bundle.query_client);

    DailySettlementResult result;
    std::string error;
    ASSERT_TRUE(service.Run(BaseConfig("191202"), &result, &error)) << error;
    EXPECT_TRUE(result.success);
    EXPECT_FALSE(result.blocked);
    EXPECT_EQ(result.status, "COMPLETED");
    EXPECT_EQ(store->system_config["trading_mode"], "TRADING");

    bundle.trader->Disconnect();
}

TEST(DailySettlementServiceTest, PostSettlementTradeBackfillIncludedInSettlement) {
    auto store = std::make_shared<FakeSettlementStore>();
    auto price = std::make_shared<FakePriceProvider>();
    auto domain_store = std::make_shared<FakeTradingDomainStore>();
    auto bundle = BuildConnectedQueryClient();

    SettlementAccountFundsRecord prev;
    prev.exists = true;
    prev.account_id = "191202";
    prev.trading_day = "2026-02-11";
    prev.balance = 0.0;
    store->funds_by_day["2026-02-11"] = prev;

    OrderIntent order;
    order.account_id = "191202";
    order.strategy_id = "s1";
    order.instrument_id = "rb2405";
    order.volume = 1;
    order.price = 3600.0;
    order.type = OrderType::kLimit;
    order.side = Side::kBuy;
    order.offset = OffsetFlag::kOpen;
    ASSERT_FALSE(bundle.trader->PlaceOrderWithRef(order).empty());

    DailySettlementService service(price, store, bundle.query_client, domain_store);

    DailySettlementResult result;
    std::string error;
    ASSERT_TRUE(service.Run(BaseConfig("191202"), &result, &error)) << error;
    EXPECT_TRUE(result.success || result.blocked);
    EXPECT_GE(domain_store->orders.size(), 0U);

    bundle.trader->Disconnect();
}

TEST(DailySettlementServiceTest, CompletedRunWithoutForceNoOp) {
    auto store = std::make_shared<FakeSettlementStore>();
    auto price = std::make_shared<FakePriceProvider>();
    auto query_client = BuildFailingQueryClient();

    store->existing_run.trading_day = "2026-02-12";
    store->existing_run.status = "COMPLETED";

    DailySettlementService service(price, store, query_client);

    DailySettlementResult result;
    std::string error;
    ASSERT_TRUE(service.Run(BaseConfig(), &result, &error)) << error;
    EXPECT_TRUE(result.success);
    EXPECT_TRUE(result.noop);
    EXPECT_EQ(result.status, "COMPLETED");
    EXPECT_TRUE(store->upserted_runs.empty());
}

}  // namespace quant_hft
