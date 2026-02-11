#include <iostream>
#include <memory>
#include <string>
#include <utility>

#include "quant_hft/contracts/types.h"
#include "quant_hft/core/ctp_config_loader.h"
#include "quant_hft/core/ctp_gateway_adapter.h"
#include "quant_hft/core/local_wal_regulatory_sink.h"
#include "quant_hft/core/query_scheduler.h"
#include "quant_hft/core/redis_realtime_store_client_adapter.h"
#include "quant_hft/core/storage_client_factory.h"
#include "quant_hft/core/storage_client_pool.h"
#include "quant_hft/core/storage_connection_config.h"
#include "quant_hft/core/storage_retry_policy.h"
#include "quant_hft/core/timescale_buffered_event_store.h"
#include "quant_hft/core/timescale_event_store_client_adapter.h"
#include "quant_hft/core/wal_replay_loader.h"
#include "quant_hft/services/basic_risk_engine.h"
#include "quant_hft/services/in_memory_portfolio_ledger.h"
#include "quant_hft/services/order_state_machine.h"
#include "quant_hft/services/rule_market_state_engine.h"

int main(int argc, char** argv) {
    using namespace quant_hft;

    const std::string config_path = argc > 1 ? argv[1] : "configs/sim/ctp.yaml";
    CtpFileConfig file_config;
    std::string error;
    if (!CtpConfigLoader::LoadFromYaml(config_path, &file_config, &error)) {
        std::cerr << "failed to load CTP config: " << error << '\n';
        return 1;
    }
    const auto& config = file_config.runtime;

    QueryScheduler scheduler(file_config.query_rate_limit_qps);
    BasicRiskEngine risk(BasicRiskLimits{});
    CtpGatewayAdapter ctp_gateway(10);
    InMemoryPortfolioLedger ledger;
    OrderStateMachine order_state_machine;
    WalReplayLoader replay_loader;
    StorageRetryPolicy storage_retry_policy;
    storage_retry_policy.max_attempts = 3;
    storage_retry_policy.initial_backoff_ms = 1;
    storage_retry_policy.max_backoff_ms = 5;
    const auto storage_config = StorageConnectionConfig::FromEnvironment();
    auto redis_client = StorageClientFactory::CreateRedisClient(storage_config, &error);
    if (redis_client == nullptr) {
        std::cerr << "failed to create redis client: " << error << '\n';
        return 5;
    }
    std::string redis_ping_error;
    if (!redis_client->Ping(&redis_ping_error)) {
        std::cerr << "redis client unhealthy: " << redis_ping_error << '\n';
        return 5;
    }
    auto pooled_redis = std::make_shared<PooledRedisHashClient>(
        std::vector<std::shared_ptr<IRedisHashClient>>{redis_client});
    RedisRealtimeStoreClientAdapter realtime_cache(pooled_redis, storage_retry_policy);
    auto timescale_client =
        StorageClientFactory::CreateTimescaleClient(storage_config, &error);
    if (timescale_client == nullptr) {
        std::cerr << "failed to create timescale client: " << error << '\n';
        return 6;
    }
    std::string timescale_ping_error;
    if (!timescale_client->Ping(&timescale_ping_error)) {
        std::cerr << "timescale client unhealthy: " << timescale_ping_error << '\n';
        return 6;
    }
    auto pooled_timescale = std::make_shared<PooledTimescaleSqlClient>(
        std::vector<std::shared_ptr<ITimescaleSqlClient>>{timescale_client});
    TimescaleBufferedStoreOptions buffered_opts;
    buffered_opts.batch_size = 16;
    buffered_opts.flush_interval_ms = 10;
    TimescaleBufferedEventStore timeseries_store(pooled_timescale,
                                                 storage_retry_policy,
                                                 buffered_opts);
    RuleMarketStateEngine market_state(32);
    LocalWalRegulatorySink wal_sink("runtime_events.wal");

    const auto replay_stats =
        replay_loader.Replay("runtime_events.wal", &order_state_machine, &ledger);
    if (replay_stats.lines_total > 0 || replay_stats.parse_errors > 0) {
        std::cout << "WAL replay lines=" << replay_stats.lines_total
                  << " events=" << replay_stats.events_loaded
                  << " parse_errors=" << replay_stats.parse_errors
                  << " state_rejected=" << replay_stats.state_rejected
                  << " ledger_applied=" << replay_stats.ledger_applied << '\n';
    }

    MarketDataConnectConfig connect_cfg;
    connect_cfg.market_front_address = config.md_front;
    connect_cfg.trader_front_address = config.td_front;
    connect_cfg.flow_path = config.flow_path;
    connect_cfg.broker_id = config.broker_id;
    connect_cfg.user_id = config.user_id;
    connect_cfg.investor_id = config.investor_id;
    connect_cfg.password = config.password;
    connect_cfg.app_id = config.app_id;
    connect_cfg.auth_code = config.auth_code;
    connect_cfg.is_production_mode = config.is_production_mode;
    connect_cfg.enable_real_api = config.enable_real_api;
    connect_cfg.enable_terminal_auth = config.enable_terminal_auth;
    connect_cfg.connect_timeout_ms = config.connect_timeout_ms;
    connect_cfg.reconnect_max_attempts = config.reconnect_max_attempts;
    connect_cfg.reconnect_initial_backoff_ms = config.reconnect_initial_backoff_ms;
    connect_cfg.reconnect_max_backoff_ms = config.reconnect_max_backoff_ms;

    if (!ctp_gateway.Connect(connect_cfg)) {
        std::cerr << "CTP gateway connect failed" << '\n';
        const auto diagnostic = ctp_gateway.GetLastConnectDiagnostic();
        if (!diagnostic.empty()) {
            std::cerr << "CTP connect diagnostic: " << diagnostic << '\n';
        }
        return 2;
    }
    ctp_gateway.Subscribe({"SHFE.ag2406"});

    ctp_gateway.RegisterOrderEventCallback(
        [&](const OrderEvent& event) {
            if (!order_state_machine.OnOrderEvent(event)) {
                std::cerr << "Order state transition rejected for "
                          << event.client_order_id << '\n';
                return;
            }
            ledger.OnOrderEvent(event);
            wal_sink.AppendOrderEvent(event);
            realtime_cache.UpsertOrderEvent(event);
            timeseries_store.AppendOrderEvent(event);
            auto direction = PositionDirection::kLong;
            if (event.reason == "short") {
                direction = PositionDirection::kShort;
            }
            realtime_cache.UpsertPositionSnapshot(
                ledger.GetPositionSnapshot(event.account_id, event.instrument_id, direction));
            std::cout << "Order event status=" << static_cast<int>(event.status)
                      << " order=" << event.client_order_id << '\n';
        });

    market_state.RegisterStateCallback(
        [](const StateSnapshot7D& state) {
            std::cout << "state update instrument=" << state.instrument_id
                      << " trend=" << state.trend.score << '\n';
        });

    MarketSnapshot snapshot;
    snapshot.instrument_id = "SHFE.ag2406";
    snapshot.last_price = 4500.0;
    snapshot.bid_price_1 = 4499.0;
    snapshot.ask_price_1 = 4501.0;
    snapshot.bid_volume_1 = 20;
    snapshot.ask_volume_1 = 15;
    snapshot.volume = 150;
    snapshot.recv_ts_ns = NowEpochNanos();
    realtime_cache.UpsertMarketSnapshot(snapshot);
    timeseries_store.AppendMarketSnapshot(snapshot);
    market_state.OnMarketSnapshot(snapshot);

    OrderIntent intent;
    intent.account_id = "sim-account";
    intent.client_order_id = "ord-0001";
    intent.instrument_id = "SHFE.ag2406";
    intent.side = Side::kBuy;
    intent.volume = 2;
    intent.price = 4500.0;
    intent.trace_id = "trace-0001";
    intent.ts_ns = NowEpochNanos();

    const auto decision = risk.PreCheck(intent);
    timeseries_store.AppendRiskDecision(intent, decision);
    if (decision.action != RiskAction::kAllow) {
        std::cerr << "Risk rejected: " << decision.reason << '\n';
        return 3;
    }
    if (!order_state_machine.OnOrderIntent(intent)) {
        std::cerr << "Order state machine rejected order intent: "
                  << intent.client_order_id << '\n';
        return 4;
    }

    QueryScheduler::QueryTask query_task;
    query_task.request_id = 1;
    query_task.priority = QueryScheduler::Priority::kHigh;
    query_task.execute = [] {
        // Placeholder for other query pipelines.
    };
    scheduler.TrySchedule(std::move(query_task));
    scheduler.DrainOnce();

    ctp_gateway.EnqueueUserSessionQuery(2);
    ctp_gateway.PlaceOrder(intent);

    timeseries_store.Flush();
    wal_sink.Flush();
    std::cout << "timeseries rows market="
              << timeseries_store.GetMarketSnapshots("SHFE.ag2406").size()
              << " risk=" << timeseries_store.GetRiskDecisionRows().size()
              << '\n';
    std::cout << "core_engine bootstrap run completed" << '\n';
    return 0;
}
