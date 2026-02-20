#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <csignal>
#include <functional>
#include <iostream>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "quant_hft/contracts/types.h"
#include "quant_hft/core/circuit_breaker.h"
#include "quant_hft/core/ctp_config_loader.h"
#include "quant_hft/core/ctp_md_adapter.h"
#include "quant_hft/core/ctp_trader_adapter.h"
#include "quant_hft/core/flow_controller.h"
#include "quant_hft/core/local_wal_regulatory_sink.h"
#include "quant_hft/core/market_bus_producer.h"
#include "quant_hft/core/redis_realtime_store_client_adapter.h"
#include "quant_hft/core/storage_client_factory.h"
#include "quant_hft/core/storage_client_pool.h"
#include "quant_hft/core/storage_connection_config.h"
#include "quant_hft/core/storage_retry_policy.h"
#include "quant_hft/core/structured_log.h"
#include "quant_hft/core/timescale_buffered_event_store.h"
#include "quant_hft/core/trading_domain_store_client_adapter.h"
#include "quant_hft/core/trading_ledger_store_client_adapter.h"
#include "quant_hft/core/wal_replay_loader.h"
#include "quant_hft/monitoring/exporter.h"
#include "quant_hft/monitoring/metric_registry.h"
#include "quant_hft/risk/risk_manager.h"
#include "quant_hft/services/bar_aggregator.h"
#include "quant_hft/services/basic_risk_engine.h"
#include "quant_hft/services/ctp_account_ledger.h"
#include "quant_hft/services/ctp_position_ledger.h"
#include "quant_hft/services/execution_engine.h"
#include "quant_hft/services/execution_planner.h"
#include "quant_hft/services/execution_router.h"
#include "quant_hft/services/in_memory_portfolio_ledger.h"
#include "quant_hft/services/order_manager.h"
#include "quant_hft/services/order_state_machine.h"
#include "quant_hft/services/position_manager.h"
#include "quant_hft/services/rule_market_state_engine.h"
#include "quant_hft/strategy/composite_strategy.h"
#include "quant_hft/strategy/demo_live_strategy.h"
#include "quant_hft/strategy/strategy_engine.h"
#include "quant_hft/strategy/state_persistence.h"

namespace {

std::atomic<bool> g_stop_requested{false};

void OnSignal(int /*signal*/) { g_stop_requested.store(true); }

bool ParseIntArg(const std::string& raw, int* out) {
    if (out == nullptr) {
        return false;
    }
    try {
        *out = std::stoi(raw);
        return true;
    } catch (...) {
        return false;
    }
}

bool ParseArgs(int argc, char** argv, std::string* config_path, int* run_seconds,
               std::string* error) {
    if (config_path == nullptr || run_seconds == nullptr) {
        if (error != nullptr) {
            *error = "output argument pointer is null";
        }
        return false;
    }

    const auto quant_root = quant_hft::GetEnvOrDefault("QUANT_ROOT", "");
    const auto default_config =
        quant_root.empty() ? "configs/sim/ctp.yaml" : (quant_root + "/configs/sim/ctp.yaml");
    *config_path = quant_hft::GetEnvOrDefault("CTP_CONFIG_PATH", default_config);
    *run_seconds = 0;

    for (int i = 1; i < argc; ++i) {
        const std::string arg = argv[i];
        if (arg == "--run-seconds") {
            if (i + 1 >= argc) {
                if (error != nullptr) {
                    *error = "--run-seconds requires a value";
                }
                return false;
            }
            int parsed = 0;
            if (!ParseIntArg(argv[++i], &parsed) || parsed <= 0) {
                if (error != nullptr) {
                    *error = "--run-seconds must be a positive integer";
                }
                return false;
            }
            *run_seconds = parsed;
            continue;
        }
        if (arg.rfind("--run-seconds=", 0) == 0) {
            const auto value = arg.substr(std::string("--run-seconds=").size());
            int parsed = 0;
            if (!ParseIntArg(value, &parsed) || parsed <= 0) {
                if (error != nullptr) {
                    *error = "--run-seconds must be a positive integer";
                }
                return false;
            }
            *run_seconds = parsed;
            continue;
        }
        if (!arg.empty() && arg[0] == '-') {
            if (error != nullptr) {
                *error = "unknown option: " + arg;
            }
            return false;
        }
        *config_path = arg;
    }

    return true;
}

std::string InferExchangeId(const std::string& instrument_id) {
    const auto dot_pos = instrument_id.find('.');
    if (dot_pos == std::string::npos || dot_pos == 0) {
        return "";
    }
    return instrument_id.substr(0, dot_pos);
}

std::vector<std::string> ResolveInstruments(const quant_hft::CtpFileConfig& config) {
    if (!config.instruments.empty()) {
        return config.instruments;
    }
    return {"SHFE.ag2406"};
}

std::vector<std::string> ResolveStrategyIds(const quant_hft::CtpFileConfig& config) {
    if (!config.strategy_ids.empty()) {
        return config.strategy_ids;
    }
    return {"demo"};
}

struct ExecutionMetadata {
    std::string strategy_id;
    std::string execution_algo_id;
    std::int32_t slice_index{0};
    std::int32_t slice_total{0};
    bool throttle_applied{false};
    std::string venue;
    std::string route_id;
    double slippage_bps{0.0};
    double impact_cost{0.0};
};

quant_hft::OrderEvent BuildRejectedEvent(const quant_hft::OrderIntent& intent,
                                         const std::string& reason,
                                         const ExecutionMetadata& metadata) {
    quant_hft::OrderEvent event;
    event.account_id = intent.account_id;
    event.strategy_id = intent.strategy_id;
    event.client_order_id = intent.client_order_id;
    event.exchange_order_id = "internal-reject";
    event.instrument_id = intent.instrument_id;
    event.status = quant_hft::OrderStatus::kRejected;
    event.total_volume = intent.volume;
    event.filled_volume = 0;
    event.avg_fill_price = 0.0;
    event.reason = reason;
    event.recv_ts_ns = quant_hft::NowEpochNanos();
    event.exchange_ts_ns = event.recv_ts_ns;
    event.ts_ns = event.recv_ts_ns;
    event.trace_id = intent.trace_id;
    event.execution_algo_id = metadata.execution_algo_id;
    event.slice_index = metadata.slice_index;
    event.slice_total = metadata.slice_total;
    event.throttle_applied = metadata.throttle_applied;
    event.venue = metadata.venue;
    event.route_id = metadata.route_id;
    event.slippage_bps = metadata.slippage_bps;
    event.impact_cost = metadata.impact_cost;
    return event;
}

quant_hft::PositionDirection ResolveLedgerDirection(const quant_hft::OrderIntent& intent) {
    const bool is_close = intent.offset == quant_hft::OffsetFlag::kClose ||
                          intent.offset == quant_hft::OffsetFlag::kCloseToday ||
                          intent.offset == quant_hft::OffsetFlag::kCloseYesterday;
    if (is_close) {
        return intent.side == quant_hft::Side::kBuy ? quant_hft::PositionDirection::kShort
                                                    : quant_hft::PositionDirection::kLong;
    }
    return intent.side == quant_hft::Side::kBuy ? quant_hft::PositionDirection::kLong
                                                : quant_hft::PositionDirection::kShort;
}

quant_hft::CtpOrderIntentForLedger BuildCtpLedgerIntent(const quant_hft::OrderIntent& intent) {
    quant_hft::CtpOrderIntentForLedger ledger_intent;
    ledger_intent.client_order_id = intent.client_order_id;
    ledger_intent.account_id = intent.account_id;
    ledger_intent.instrument_id = intent.instrument_id;
    ledger_intent.direction = ResolveLedgerDirection(intent);
    ledger_intent.offset = intent.offset;
    ledger_intent.requested_volume = intent.volume;
    return ledger_intent;
}

bool IsTerminalStatus(quant_hft::OrderStatus status) {
    return status == quant_hft::OrderStatus::kFilled ||
           status == quant_hft::OrderStatus::kCanceled ||
           status == quant_hft::OrderStatus::kRejected;
}

}  // namespace

int main(int argc, char** argv) {
    using namespace quant_hft;

    CtpRuntimeConfig bootstrap_runtime;

    std::string config_path;
    int run_seconds = 0;
    std::string parse_error;
    if (!ParseArgs(argc, argv, &config_path, &run_seconds, &parse_error)) {
        EmitStructuredLog(&bootstrap_runtime, "core_engine", "error", "invalid_arguments",
                          {{"error", parse_error}});
        return 1;
    }

    CtpFileConfig file_config;
    std::string error;
    if (!CtpConfigLoader::LoadFromYaml(config_path, &file_config, &error)) {
        EmitStructuredLog(&bootstrap_runtime, "core_engine", "error", "config_load_failed",
                          {{"config_path", config_path}, {"error", error}});
        return 1;
    }
    const auto& config = file_config.runtime;
    const auto instruments = ResolveInstruments(file_config);
    const auto strategy_ids = ResolveStrategyIds(file_config);
    const std::string strategy_factory =
        file_config.strategy_factory.empty() ? std::string("demo") : file_config.strategy_factory;
    const std::string run_type =
        file_config.run_type.empty() ? std::string("live") : file_config.run_type;
    if (run_type == "backtest") {
        EmitStructuredLog(
            &config, "core_engine", "error", "invalid_run_type",
            {{"run_type", run_type}, {"error", "core_engine does not support run_type=backtest"}});
        return 1;
    }
    const std::size_t strategy_queue_capacity =
        static_cast<std::size_t>(std::max(1, file_config.strategy_queue_capacity));
    const std::string account_id =
        file_config.account_id.empty() ? config.user_id : file_config.account_id;
    const ExecutionConfig execution_config = file_config.execution;
    MetricsExporter metrics_exporter;
    if (config.metrics_enabled) {
        std::string metrics_error;
        if (!metrics_exporter.Start(config.metrics_port, &metrics_error)) {
            std::cerr << "warning: metrics exporter start failed: " << metrics_error << '\n';
        }
    }
    ExecutionPlanner execution_planner;
    ExecutionRouter execution_router;
    auto ctp_trader = std::make_shared<CTPTraderAdapter>(
        static_cast<std::size_t>(std::max(1, config.query_rate_per_sec)), 2);
    auto ctp_md = std::make_shared<CTPMdAdapter>(
        static_cast<std::size_t>(std::max(1, config.query_rate_per_sec)), 2);
    auto flow_controller = std::make_shared<FlowController>();
    auto breaker_manager = std::make_shared<CircuitBreakerManager>();
    {
        FlowRule rule;
        rule.account_id = account_id;
        rule.type = OperationType::kOrderInsert;
        rule.rate_per_second = static_cast<double>(config.order_insert_rate_per_sec);
        rule.capacity = config.order_bucket_capacity;
        flow_controller->AddRule(rule);
    }
    {
        FlowRule rule;
        rule.account_id = account_id;
        rule.type = OperationType::kOrderCancel;
        rule.rate_per_second = static_cast<double>(config.order_cancel_rate_per_sec);
        rule.capacity = config.cancel_bucket_capacity;
        flow_controller->AddRule(rule);
    }
    {
        FlowRule rule;
        rule.account_id = account_id;
        rule.type = OperationType::kQuery;
        rule.rate_per_second = static_cast<double>(config.query_rate_per_sec);
        rule.capacity = config.query_bucket_capacity;
        flow_controller->AddRule(rule);
    }
    CircuitBreakerConfig breaker_cfg;
    breaker_cfg.failure_threshold = config.breaker_failure_threshold;
    breaker_cfg.timeout_ms = config.breaker_timeout_ms;
    breaker_cfg.half_open_timeout_ms = config.breaker_half_open_timeout_ms;
    breaker_manager->Configure(BreakerScope::kStrategy, breaker_cfg,
                               config.breaker_strategy_enabled);
    breaker_manager->Configure(BreakerScope::kAccount, breaker_cfg, config.breaker_account_enabled);
    breaker_manager->Configure(BreakerScope::kSystem, breaker_cfg, config.breaker_system_enabled);
    InMemoryPortfolioLedger ledger;
    CtpPositionLedger ctp_position_ledger;
    CtpAccountLedger ctp_account_ledger;
    OrderStateMachine order_state_machine;
    BarAggregator bar_aggregator;
    (void)bar_aggregator.Flush();
    std::mutex ctp_ledger_mutex;
    std::mutex planner_mutex;
    std::mutex execution_metadata_mutex;
    std::mutex market_history_mutex;
    std::unordered_map<std::string, ExecutionMetadata> execution_metadata_by_order;
    std::unordered_map<std::string, std::vector<MarketSnapshot>> recent_market_history;
    std::mutex cancel_pending_mutex;
    std::unordered_set<std::string> cancel_pending_orders;
    std::function<void(const SignalIntent&)> process_signal_intent;
    std::unique_ptr<StrategyEngine> strategy_engine;
    WalReplayLoader replay_loader;
    StorageRetryPolicy storage_retry_policy;
    storage_retry_policy.max_attempts = 3;
    storage_retry_policy.initial_backoff_ms = 1;
    storage_retry_policy.max_backoff_ms = 5;

    const auto storage_config = StorageConnectionConfig::FromEnvironment();
    auto redis_client = StorageClientFactory::CreateRedisClient(storage_config, &error);
    if (redis_client == nullptr) {
        EmitStructuredLog(&config, "core_engine", "error", "redis_client_create_failed",
                          {{"error", error}});
        return 5;
    }
    std::string redis_ping_error;
    if (!redis_client->Ping(&redis_ping_error)) {
        EmitStructuredLog(&config, "core_engine", "error", "redis_client_unhealthy",
                          {{"error", redis_ping_error}});
        return 5;
    }
    auto pooled_redis = std::make_shared<PooledRedisHashClient>(
        std::vector<std::shared_ptr<IRedisHashClient>>{redis_client});
    RedisRealtimeStoreClientAdapter realtime_cache(pooled_redis, storage_retry_policy);

    std::shared_ptr<IStrategyStatePersistence> strategy_state_persistence;
    if (file_config.strategy_state_persist_enabled) {
        strategy_state_persistence = std::make_shared<RedisStrategyStatePersistence>(
            pooled_redis, file_config.strategy_state_key_prefix, file_config.strategy_state_ttl_seconds);
    }
    StrategyEngineConfig strategy_engine_config;
    strategy_engine_config.queue_capacity = strategy_queue_capacity;
    strategy_engine_config.state_persistence = strategy_state_persistence;
    strategy_engine_config.load_state_on_start = file_config.strategy_state_persist_enabled;
    strategy_engine_config.state_snapshot_interval_ns =
        static_cast<EpochNanos>(file_config.strategy_state_snapshot_interval_ms) * 1'000'000;
    strategy_engine_config.metrics_collect_interval_ns =
        static_cast<EpochNanos>(file_config.strategy_metrics_emit_interval_ms) * 1'000'000;
    strategy_engine = std::make_unique<StrategyEngine>(strategy_engine_config,
                                                       [&](const SignalIntent& signal) {
                                                           if (process_signal_intent) {
                                                               process_signal_intent(signal);
                                                           }
                                                       });

    auto timescale_client = StorageClientFactory::CreateTimescaleClient(storage_config, &error);
    if (timescale_client == nullptr) {
        EmitStructuredLog(&config, "core_engine", "error", "timescale_client_create_failed",
                          {{"error", error}});
        return 6;
    }
    std::string timescale_ping_error;
    if (!timescale_client->Ping(&timescale_ping_error)) {
        EmitStructuredLog(&config, "core_engine", "error", "timescale_client_unhealthy",
                          {{"error", timescale_ping_error}});
        return 6;
    }
    auto pooled_timescale = std::make_shared<PooledTimescaleSqlClient>(
        std::vector<std::shared_ptr<ITimescaleSqlClient>>{timescale_client});
    TimescaleBufferedStoreOptions buffered_opts;
    buffered_opts.batch_size = 16;
    buffered_opts.flush_interval_ms = 10;
    buffered_opts.schema = storage_config.timescale.analytics_schema;
    TimescaleBufferedEventStore timeseries_store(pooled_timescale, storage_retry_policy,
                                                 buffered_opts);
    TimescaleEventStoreClientAdapter ctp_query_snapshot_store(
        pooled_timescale, storage_retry_policy, storage_config.timescale.analytics_schema);
    TradingLedgerStoreClientAdapter trading_ledger_store(pooled_timescale, storage_retry_policy,
                                                         storage_config.timescale.trading_schema);
    auto trading_domain_store = std::make_shared<TradingDomainStoreClientAdapter>(
        pooled_timescale, storage_retry_policy, storage_config.timescale.trading_schema);
    auto order_manager = std::make_shared<OrderManager>(trading_domain_store);
    auto position_manager = std::make_shared<PositionManager>(trading_domain_store, pooled_redis);
    ExecutionEngine execution_engine(
        ctp_trader, flow_controller, breaker_manager, order_manager, position_manager,
        trading_domain_store, 1000, config.cancel_retry_max, config.cancel_retry_base_ms,
        config.cancel_retry_max_delay_ms, config.cancel_wait_ack_timeout_ms);
    ctp_trader->SetCircuitBreaker([breaker_manager, &config](bool opened) {
        if (!opened) {
            return;
        }
        breaker_manager->RecordFailure(BreakerScope::kSystem, "__system__");
        EmitStructuredLog(&config, "core_engine", "warn",
                          "callback_dispatcher_breaker_failure_recorded", {{"scope", "system"}});
    });
    auto risk_manager = CreateRiskManager(order_manager, trading_domain_store);
    RiskManagerConfig risk_manager_config;
    risk_manager_config.default_max_order_volume = file_config.risk.default_max_order_volume;
    risk_manager_config.default_max_order_rate = config.order_insert_rate_per_sec;
    risk_manager_config.default_max_cancel_rate = config.order_cancel_rate_per_sec;
    risk_manager_config.rule_file_path =
        quant_hft::GetEnvOrDefault("RISK_RULE_FILE_PATH", "configs/risk_rules.yaml");
    risk_manager_config.enable_dynamic_reload = true;
    (void)risk_manager->Initialize(risk_manager_config);
    execution_engine.SetRiskManager(risk_manager);
    RuleMarketStateEngine market_state(32, file_config.market_state_detector);
    MarketBusProducer market_bus_producer(config.kafka_bootstrap_servers, config.kafka_topic_ticks);
    LocalWalRegulatorySink wal_sink("runtime_events.wal");
    std::atomic<std::uint64_t> wal_write_failures{0};
    std::atomic<std::uint64_t> trading_write_failures{0};

    const auto replay_stats =
        replay_loader.Replay("runtime_events.wal", &order_state_machine, &ledger);
    if (replay_stats.lines_total > 0 || replay_stats.parse_errors > 0) {
        std::cout << "WAL replay lines=" << replay_stats.lines_total
                  << " events=" << replay_stats.events_loaded
                  << " parse_errors=" << replay_stats.parse_errors
                  << " state_rejected=" << replay_stats.state_rejected
                  << " ledger_applied=" << replay_stats.ledger_applied << '\n';
    }

    auto process_order_event = [&](const OrderEvent& raw_event) {
        OrderEvent event = raw_event;
        if (event.recv_ts_ns <= 0) {
            event.recv_ts_ns = event.ts_ns > 0 ? event.ts_ns : NowEpochNanos();
        }
        if (event.exchange_ts_ns <= 0) {
            event.exchange_ts_ns = event.recv_ts_ns;
        }
        if (event.ts_ns <= 0) {
            event.ts_ns = event.recv_ts_ns;
        }
        {
            std::lock_guard<std::mutex> lock(execution_metadata_mutex);
            const auto it = execution_metadata_by_order.find(event.client_order_id);
            if (it != execution_metadata_by_order.end()) {
                if (event.strategy_id.empty()) {
                    event.strategy_id = it->second.strategy_id;
                }
                event.execution_algo_id = it->second.execution_algo_id;
                event.slice_index = it->second.slice_index;
                event.slice_total = it->second.slice_total;
                event.throttle_applied = event.throttle_applied || it->second.throttle_applied;
                if (event.venue.empty()) {
                    event.venue = it->second.venue;
                }
                if (event.route_id.empty()) {
                    event.route_id = it->second.route_id;
                }
                if (std::fabs(event.slippage_bps) < 1e-9) {
                    event.slippage_bps = it->second.slippage_bps;
                }
                if (std::fabs(event.impact_cost) < 1e-9) {
                    event.impact_cost = it->second.impact_cost;
                }
            }
        }

        bool state_applied = order_state_machine.OnOrderEvent(event);
        if (!state_applied) {
            state_applied = order_state_machine.RecoverFromOrderEvent(event);
        }
        if (!state_applied) {
            EmitStructuredLog(&config, "core_engine", "warn", "legacy_state_machine_rejected",
                              {{"client_order_id", event.client_order_id}});
        }
        execution_engine.HandleOrderEvent(event);
        if (event.strategy_id.empty()) {
            const auto tracked_order = order_manager->GetOrder(event.client_order_id);
            if (tracked_order.has_value()) {
                event.strategy_id = tracked_order->strategy_id;
            }
        }
        if (IsTerminalStatus(event.status)) {
            std::lock_guard<std::mutex> lock(cancel_pending_mutex);
            cancel_pending_orders.erase(event.client_order_id);
            std::lock_guard<std::mutex> metadata_lock(execution_metadata_mutex);
            execution_metadata_by_order.erase(event.client_order_id);
        }
        {
            std::lock_guard<std::mutex> lock(planner_mutex);
            execution_planner.RecordOrderResult(event.status == OrderStatus::kRejected);
        }
        ledger.OnOrderEvent(event);
        {
            std::string ctp_ledger_error;
            std::lock_guard<std::mutex> lock(ctp_ledger_mutex);
            if (!ctp_position_ledger.ApplyOrderEvent(event, &ctp_ledger_error) &&
                ctp_ledger_error != "order intent not registered") {
                EmitStructuredLog(
                    &config, "core_engine", "warn", "ctp_position_ledger_apply_failed",
                    {{"client_order_id", event.client_order_id}, {"error", ctp_ledger_error}});
            }
        }
        if (!wal_sink.AppendOrderEvent(event)) {
            const auto failure_count = wal_write_failures.fetch_add(1) + 1;
            EmitStructuredLog(&config, "core_engine", "error", "wal_append_order_event_failed",
                              {{"client_order_id", event.client_order_id},
                               {"failure_count", std::to_string(failure_count)}});
        }

        std::string trading_error;
        if (!trading_ledger_store.AppendOrderEvent(event, &trading_error)) {
            const auto failure_count = trading_write_failures.fetch_add(1) + 1;
            EmitStructuredLog(&config, "core_engine", "error", "trading_append_order_event_failed",
                              {{"client_order_id", event.client_order_id},
                               {"error", trading_error},
                               {"failure_count", std::to_string(failure_count)}});
        }
        if ((event.status == OrderStatus::kPartiallyFilled ||
             event.status == OrderStatus::kFilled) &&
            event.filled_volume > 0) {
            if (!trading_ledger_store.AppendTradeEvent(event, &trading_error)) {
                const auto failure_count = trading_write_failures.fetch_add(1) + 1;
                EmitStructuredLog(&config, "core_engine", "error",
                                  "trading_append_trade_event_failed",
                                  {{"client_order_id", event.client_order_id},
                                   {"trade_id", event.trade_id},
                                   {"error", trading_error},
                                   {"failure_count", std::to_string(failure_count)}});
            }
        }

        realtime_cache.UpsertOrderEvent(event);

        realtime_cache.UpsertPositionSnapshot(ledger.GetPositionSnapshot(
            event.account_id, event.instrument_id, PositionDirection::kLong));
        realtime_cache.UpsertPositionSnapshot(ledger.GetPositionSnapshot(
            event.account_id, event.instrument_id, PositionDirection::kShort));

        timeseries_store.AppendOrderEvent(event);
        if (strategy_engine != nullptr) {
            strategy_engine->EnqueueOrderEvent(event);
        }
    };

    process_signal_intent = [&](const SignalIntent& signal) {
        std::vector<MarketSnapshot> recent_market;
        {
            std::lock_guard<std::mutex> lock(market_history_mutex);
            const auto it = recent_market_history.find(signal.instrument_id);
            if (it != recent_market_history.end()) {
                recent_market = it->second;
            }
        }
        const auto plans =
            execution_planner.BuildPlan(signal, account_id, execution_config, recent_market);
        for (const auto& planned : plans) {
            const auto& intent = planned.intent;
            ExecutionMetadata metadata;
            metadata.strategy_id = intent.strategy_id;
            metadata.execution_algo_id = planned.execution_algo_id;
            metadata.slice_index = planned.slice_index;
            metadata.slice_total = planned.slice_total;
            const std::int64_t observed_market_volume =
                recent_market.empty() ? 0 : recent_market.back().volume;
            const auto route =
                execution_router.Route(planned, execution_config, observed_market_volume);
            metadata.venue = route.venue;
            metadata.route_id = route.route_id;
            metadata.slippage_bps = route.slippage_bps;
            metadata.impact_cost = route.impact_cost;
            {
                std::lock_guard<std::mutex> lock(execution_metadata_mutex);
                execution_metadata_by_order[intent.client_order_id] = metadata;
            }

            bool throttle_applied = false;
            double throttle_ratio = 0.0;
            if (execution_config.throttle_reject_ratio > 0.0) {
                std::lock_guard<std::mutex> lock(planner_mutex);
                throttle_applied =
                    execution_planner.ShouldThrottle(execution_config.throttle_reject_ratio);
                throttle_ratio = execution_planner.CurrentRejectRatio();
            }
            if (throttle_applied) {
                RiskDecision throttle_decision;
                throttle_decision.action = RiskAction::kReject;
                throttle_decision.rule_id = "policy.execution.throttle.reject_ratio";
                throttle_decision.rule_group = "execution";
                throttle_decision.rule_version = "v1";
                throttle_decision.policy_id = "policy.execution.throttle";
                throttle_decision.policy_scope = "execution";
                throttle_decision.observed_value = throttle_ratio;
                throttle_decision.threshold_value = execution_config.throttle_reject_ratio;
                throttle_decision.decision_tags = "execution,throttle";
                throttle_decision.reason = "reject ratio exceeds threshold";
                throttle_decision.decision_ts_ns = NowEpochNanos();
                timeseries_store.AppendRiskDecision(intent, throttle_decision);

                metadata.throttle_applied = true;
                process_order_event(
                    BuildRejectedEvent(intent, "throttled:reject_ratio_exceeded", metadata));
                continue;
            }

            if (!order_state_machine.OnOrderIntent(intent)) {
                process_order_event(BuildRejectedEvent(
                    intent, "order_state_reject:duplicate_or_invalid", metadata));
            } else {
                std::string ctp_ledger_error;
                {
                    const auto ledger_intent = BuildCtpLedgerIntent(intent);
                    std::lock_guard<std::mutex> lock(ctp_ledger_mutex);
                    if (!ctp_position_ledger.RegisterOrderIntent(ledger_intent,
                                                                 &ctp_ledger_error)) {
                        process_order_event(BuildRejectedEvent(
                            intent, "position_ledger_reject:" + ctp_ledger_error, metadata));
                        continue;
                    }
                }
                if (!execution_engine.PlaceOrderAsync(intent).get().success) {
                    process_order_event(
                        BuildRejectedEvent(intent, "gateway_reject:place_order_failed", metadata));
                    continue;
                }
                std::lock_guard<std::mutex> lock(planner_mutex);
                execution_planner.RecordOrderResult(false);
            }

            const bool is_last_slice = planned.slice_index == planned.slice_total;
            const bool interval_enabled = execution_config.algo != ExecutionAlgo::kDirect &&
                                          execution_config.slice_interval_ms > 0;
            if (!is_last_slice && interval_enabled) {
                std::this_thread::sleep_for(
                    std::chrono::milliseconds(execution_config.slice_interval_ms));
            }
        }
    };

    auto process_market_snapshot = [&](const MarketSnapshot& raw_snapshot) {
        MarketSnapshot snapshot = raw_snapshot;
        if (!bar_aggregator.ShouldProcessSnapshot(snapshot)) {
            return;
        }
        (void)bar_aggregator.OnMarketSnapshot(snapshot);
        {
            std::lock_guard<std::mutex> lock(market_history_mutex);
            auto& history = recent_market_history[snapshot.instrument_id];
            history.push_back(snapshot);
            if (history.size() > 64) {
                history.erase(history.begin());
            }
        }
        realtime_cache.UpsertMarketSnapshot(snapshot);
        timeseries_store.AppendMarketSnapshot(snapshot);
        market_state.OnMarketSnapshot(snapshot);
        const auto publish_result = market_bus_producer.PublishTick(snapshot);
        if (!publish_result.ok) {
            EmitStructuredLog(
                &config, "core_engine", "error", "market_bus_publish_failed",
                {{"topic", config.kafka_topic_ticks}, {"reason", publish_result.reason}});
        }
    };

    ctp_trader->RegisterOrderEventCallback(
        [&](const OrderEvent& event) { process_order_event(event); });
    ctp_md->RegisterTickCallback(
        [&](const MarketSnapshot& snapshot) { process_market_snapshot(snapshot); });
    ctp_trader->RegisterTradingAccountSnapshotCallback([&](const TradingAccountSnapshot& snapshot) {
        {
            std::lock_guard<std::mutex> lock(ctp_ledger_mutex);
            ctp_account_ledger.ApplyTradingAccountSnapshot(snapshot);
            if (!snapshot.trading_day.empty()) {
                ctp_account_ledger.RollTradingDay(snapshot.trading_day);
            }
        }
        std::string trading_error;
        if (!trading_ledger_store.AppendAccountSnapshot(snapshot, &trading_error)) {
            const auto failure_count = trading_write_failures.fetch_add(1) + 1;
            EmitStructuredLog(&config, "core_engine", "error",
                              "trading_append_account_snapshot_failed",
                              {{"account_id", snapshot.account_id},
                               {"error", trading_error},
                               {"failure_count", std::to_string(failure_count)}});
        }
        ctp_query_snapshot_store.AppendTradingAccountSnapshot(snapshot);
        if (strategy_engine != nullptr) {
            strategy_engine->EnqueueAccountSnapshot(snapshot);
        }
    });
    ctp_trader->RegisterInvestorPositionSnapshotCallback(
        [&](const std::vector<InvestorPositionSnapshot>& snapshots) {
            std::string ctp_ledger_error;
            for (const auto& snapshot : snapshots) {
                {
                    std::lock_guard<std::mutex> lock(ctp_ledger_mutex);
                    if (!ctp_position_ledger.ApplyInvestorPositionSnapshot(snapshot,
                                                                           &ctp_ledger_error)) {
                        EmitStructuredLog(&config, "core_engine", "warn",
                                          "ctp_position_snapshot_apply_failed",
                                          {{"instrument_id", snapshot.instrument_id},
                                           {"error", ctp_ledger_error}});
                    }
                }
                std::string trading_error;
                if (!trading_ledger_store.AppendPositionSnapshot(snapshot, &trading_error)) {
                    const auto failure_count = trading_write_failures.fetch_add(1) + 1;
                    EmitStructuredLog(&config, "core_engine", "error",
                                      "trading_append_position_snapshot_failed",
                                      {{"account_id", snapshot.account_id},
                                       {"instrument_id", snapshot.instrument_id},
                                       {"error", trading_error},
                                       {"failure_count", std::to_string(failure_count)}});
                }
                ctp_query_snapshot_store.AppendInvestorPositionSnapshot(snapshot);
            }
        });
    ctp_trader->RegisterInstrumentMetaSnapshotCallback(
        [&](const std::vector<InstrumentMetaSnapshot>& snapshots) {
            for (const auto& snapshot : snapshots) {
                ctp_query_snapshot_store.AppendInstrumentMetaSnapshot(snapshot);
            }
        });
    ctp_trader->RegisterBrokerTradingParamsSnapshotCallback(
        [&](const BrokerTradingParamsSnapshot& snapshot) {
            if (!snapshot.margin_price_type.empty()) {
                std::lock_guard<std::mutex> lock(ctp_ledger_mutex);
                ctp_account_ledger.SetMarginPriceType(snapshot.margin_price_type.front());
            }
            ctp_query_snapshot_store.AppendBrokerTradingParamsSnapshot(snapshot);
        });
    market_state.RegisterStateCallback([&](const StateSnapshot7D& state) {
        realtime_cache.UpsertStateSnapshot7D(state);
        if (strategy_engine != nullptr) {
            strategy_engine->EnqueueState(state);
        }
    });

    std::string strategy_register_error;
    if (!RegisterDemoLiveStrategy(&strategy_register_error)) {
        EmitStructuredLog(&config, "core_engine", "error", "strategy_factory_register_failed",
                          {{"strategy_factory", "demo"}, {"error", strategy_register_error}});
        return 7;
    }
    if (!RegisterCompositeStrategy(&strategy_register_error)) {
        EmitStructuredLog(&config, "core_engine", "error", "strategy_factory_register_failed",
                          {{"strategy_factory", "composite"}, {"error", strategy_register_error}});
        return 7;
    }
    StrategyContext strategy_context;
    strategy_context.account_id = account_id;
    strategy_context.metadata["run_type"] = run_type;
    strategy_context.metadata["strategy_factory"] = strategy_factory;
    if (strategy_factory == "composite") {
        strategy_context.metadata["composite_config_path"] = file_config.strategy_composite_config;
    }
    if (strategy_engine == nullptr ||
        !strategy_engine->Start(strategy_ids, strategy_factory, strategy_context,
                                &strategy_register_error)) {
        EmitStructuredLog(
            &config, "core_engine", "error", "strategy_engine_start_failed",
            {{"strategy_factory", strategy_factory}, {"error", strategy_register_error}});
        return 7;
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
    connect_cfg.query_retry_backoff_ms = config.query_retry_backoff_ms;
    connect_cfg.recovery_quiet_period_ms = config.recovery_quiet_period_ms;
    connect_cfg.settlement_confirm_required = config.settlement_confirm_required;

    if (!ctp_trader->Connect(connect_cfg)) {
        EmitStructuredLog(&config, "core_engine", "error", "ctp_trader_connect_failed");
        const auto diagnostic = ctp_trader->GetLastConnectDiagnostic();
        if (!diagnostic.empty()) {
            EmitStructuredLog(&config, "core_engine", "error", "ctp_connect_diagnostic",
                              {{"detail", diagnostic}});
        }
        return 2;
    }
    if (!ctp_md->Connect(connect_cfg)) {
        EmitStructuredLog(&config, "core_engine", "error", "ctp_md_connect_failed");
        const auto diagnostic = ctp_md->GetLastConnectDiagnostic();
        if (!diagnostic.empty()) {
            EmitStructuredLog(&config, "core_engine", "error", "ctp_connect_diagnostic",
                              {{"detail", diagnostic}});
        }
        return 2;
    }
    if (!ctp_trader->ConfirmSettlement()) {
        EmitStructuredLog(&config, "core_engine", "error", "ctp_settlement_confirm_failed");
        return 2;
    }
    if (!ctp_md->Subscribe(instruments)) {
        EmitStructuredLog(&config, "core_engine", "error", "ctp_subscribe_failed",
                          {{"instrument_count", std::to_string(instruments.size())}});
        return 2;
    }
    for (const auto& instrument_id : instruments) {
        bar_aggregator.ResetInstrument(instrument_id);
    }

    std::atomic<int> query_request_id{1};
    auto next_query_request_id = [&query_request_id]() { return query_request_id.fetch_add(1); };
    if (!ctp_trader->EnqueueUserSessionQuery(next_query_request_id())) {
        EmitStructuredLog(&config, "core_engine", "warn", "initial_user_session_query_failed");
    }
    try {
        (void)execution_engine.QueryTradingAccountAsync().get();
    } catch (...) {
        EmitStructuredLog(&config, "core_engine", "warn", "initial_trading_account_query_failed");
    }
    try {
        (void)execution_engine.QueryInvestorPositionAsync().get();
    } catch (...) {
        EmitStructuredLog(&config, "core_engine", "warn", "initial_investor_position_query_failed");
    }
    if (!ctp_trader->EnqueueInstrumentQuery(next_query_request_id())) {
        EmitStructuredLog(&config, "core_engine", "warn", "initial_instrument_query_failed");
    }
    if (!ctp_trader->EnqueueBrokerTradingParamsQuery(next_query_request_id())) {
        EmitStructuredLog(&config, "core_engine", "warn",
                          "initial_broker_trading_params_query_failed");
    }
    for (const auto& instrument_id : instruments) {
        if (!ctp_trader->EnqueueInstrumentMarginRateQuery(next_query_request_id(), instrument_id)) {
            EmitStructuredLog(&config, "core_engine", "warn", "initial_margin_rate_query_failed",
                              {{"instrument_id", instrument_id}});
        }
        if (!ctp_trader->EnqueueInstrumentCommissionRateQuery(next_query_request_id(),
                                                              instrument_id)) {
            EmitStructuredLog(&config, "core_engine", "warn",
                              "initial_commission_rate_query_failed",
                              {{"instrument_id", instrument_id}});
        }
    }

    std::signal(SIGINT, OnSignal);
    std::signal(SIGTERM, OnSignal);
    g_stop_requested.store(false);

    std::atomic<bool> query_loop_stop{false};
    std::atomic<bool> execution_loop_stop{false};
    std::thread query_poll_thread([&]() {
        auto next_account_query = std::chrono::steady_clock::now();
        auto next_position_query = std::chrono::steady_clock::now();
        auto next_instrument_query = std::chrono::steady_clock::now();
        while (!query_loop_stop.load()) {
            const auto now = std::chrono::steady_clock::now();
            if (now >= next_account_query) {
                try {
                    (void)execution_engine.QueryTradingAccountAsync().get();
                } catch (...) {
                }
                next_account_query = now + std::chrono::milliseconds(
                                               std::max(1, file_config.account_query_interval_ms));
            }
            if (now >= next_position_query) {
                try {
                    (void)execution_engine.QueryInvestorPositionAsync().get();
                } catch (...) {
                }
                next_position_query = now + std::chrono::milliseconds(std::max(
                                                1, file_config.position_query_interval_ms));
            }
            if (now >= next_instrument_query) {
                (void)ctp_trader->EnqueueInstrumentQuery(next_query_request_id());
                (void)ctp_trader->EnqueueBrokerTradingParamsQuery(next_query_request_id());
                for (const auto& instrument_id : instruments) {
                    (void)ctp_trader->EnqueueInstrumentMarginRateQuery(next_query_request_id(),
                                                                       instrument_id);
                    (void)ctp_trader->EnqueueInstrumentCommissionRateQuery(next_query_request_id(),
                                                                           instrument_id);
                }
                next_instrument_query = now + std::chrono::milliseconds(std::max(
                                                  1, file_config.instrument_query_interval_ms));
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
        }
    });

    std::thread execution_maintenance_thread([&]() {
        auto next_cancel_scan = std::chrono::steady_clock::now();
        while (!execution_loop_stop.load()) {
            if (execution_config.cancel_after_ms > 0) {
                const auto now = std::chrono::steady_clock::now();
                if (now >= next_cancel_scan) {
                    const auto now_ns = NowEpochNanos();
                    const auto cancel_after_ns =
                        static_cast<EpochNanos>(execution_config.cancel_after_ms) * 1'000'000;
                    const auto cutoff_ns = now_ns - cancel_after_ns;
                    for (const auto& order : execution_engine.GetActiveOrders()) {
                        if (order.updated_at_ns == 0 || order.updated_at_ns > cutoff_ns) {
                            continue;
                        }

                        bool first_request = false;
                        {
                            std::lock_guard<std::mutex> lock(cancel_pending_mutex);
                            first_request = cancel_pending_orders.insert(order.order_id).second;
                        }
                        if (!first_request) {
                            continue;
                        }

                        if (!execution_engine.CancelOrderAsync(order.order_id).get()) {
                            std::lock_guard<std::mutex> lock(cancel_pending_mutex);
                            cancel_pending_orders.erase(order.order_id);
                        }
                    }

                    next_cancel_scan = now + std::chrono::milliseconds(std::max(
                                                 1, execution_config.cancel_check_interval_ms));
                }
            }

            std::this_thread::sleep_for(std::chrono::milliseconds(50));
        }
    });

    const auto start = std::chrono::steady_clock::now();
    auto next_strategy_metrics_emit = std::chrono::steady_clock::now();
    std::size_t synthetic_tick = 0;
    while (!g_stop_requested.load()) {
        if (run_seconds > 0) {
            const auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(
                std::chrono::steady_clock::now() - start);
            if (elapsed.count() >= run_seconds) {
                break;
            }
        }

        if (file_config.strategy_metrics_emit_interval_ms > 0 &&
            std::chrono::steady_clock::now() >= next_strategy_metrics_emit &&
            strategy_engine != nullptr) {
            const std::vector<StrategyMetric> metrics = strategy_engine->CollectAllMetrics();
            for (const auto& metric : metrics) {
                std::string strategy_id = "";
                if (const auto it = metric.labels.find("strategy_id"); it != metric.labels.end()) {
                    strategy_id = it->second;
                }
                EmitStructuredLog(&config, "core_engine", "info", "strategy_metric",
                                  {{"name", metric.name},
                                   {"value", std::to_string(metric.value)},
                                   {"strategy_id", strategy_id}});
                if (config.metrics_enabled) {
                    MetricLabels gauge_labels;
                    for (const auto& [label_key, label_value] : metric.labels) {
                        gauge_labels[label_key] = label_value;
                    }
                    auto gauge =
                        MetricRegistry::Instance().BuildGauge(metric.name, "strategy metric",
                                                              gauge_labels);
                    gauge->Set(metric.value);
                }
            }
            next_strategy_metrics_emit =
                std::chrono::steady_clock::now() +
                std::chrono::milliseconds(file_config.strategy_metrics_emit_interval_ms);
        }

        if (!config.enable_real_api) {
            for (const auto& instrument_id : instruments) {
                MarketSnapshot snapshot;
                snapshot.instrument_id = instrument_id;
                snapshot.last_price = 4500.0 + static_cast<double>(synthetic_tick % 20) * 0.5;
                snapshot.bid_price_1 = snapshot.last_price - 0.5;
                snapshot.ask_price_1 = snapshot.last_price + 0.5;
                snapshot.bid_volume_1 = 20 + static_cast<std::int64_t>(synthetic_tick % 5);
                snapshot.ask_volume_1 = 15 + static_cast<std::int64_t>(synthetic_tick % 4);
                snapshot.volume = 100 + static_cast<std::int64_t>(synthetic_tick);
                snapshot.exchange_id = InferExchangeId(snapshot.instrument_id);
                snapshot.trading_day = "19700101";
                snapshot.action_day = "19700101";
                snapshot.update_time = "09:30:00";
                snapshot.update_millisec = static_cast<std::int32_t>(synthetic_tick % 1000);
                snapshot.exchange_ts_ns = NowEpochNanos();
                snapshot.recv_ts_ns = snapshot.exchange_ts_ns;
                process_market_snapshot(snapshot);
            }
            ++synthetic_tick;
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(500));
    }

    execution_loop_stop.store(true);
    if (execution_maintenance_thread.joinable()) {
        execution_maintenance_thread.join();
    }
    query_loop_stop.store(true);
    if (query_poll_thread.joinable()) {
        query_poll_thread.join();
    }

    if (strategy_engine != nullptr) {
        strategy_engine->Stop();
    }
    ctp_md->Disconnect();
    ctp_trader->Disconnect();
    metrics_exporter.Stop();
    (void)bar_aggregator.Flush();
    timeseries_store.Flush();
    wal_sink.Flush();

    std::cout << "core_engine stopped cleanly" << '\n';
    return 0;
}
