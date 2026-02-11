#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <csignal>
#include <iostream>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "quant_hft/contracts/types.h"
#include "quant_hft/core/ctp_config_loader.h"
#include "quant_hft/core/ctp_gateway_adapter.h"
#include "quant_hft/core/local_wal_regulatory_sink.h"
#include "quant_hft/core/redis_realtime_store_client_adapter.h"
#include "quant_hft/core/storage_client_factory.h"
#include "quant_hft/core/storage_client_pool.h"
#include "quant_hft/core/storage_connection_config.h"
#include "quant_hft/core/storage_retry_policy.h"
#include "quant_hft/core/strategy_intent_inbox.h"
#include "quant_hft/core/timescale_buffered_event_store.h"
#include "quant_hft/core/wal_replay_loader.h"
#include "quant_hft/services/basic_risk_engine.h"
#include "quant_hft/services/execution_planner.h"
#include "quant_hft/services/in_memory_portfolio_ledger.h"
#include "quant_hft/services/order_state_machine.h"
#include "quant_hft/services/risk_policy_engine.h"
#include "quant_hft/services/rule_market_state_engine.h"

namespace {

std::atomic<bool> g_stop_requested{false};

void OnSignal(int /*signal*/) {
    g_stop_requested.store(true);
}

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

bool ParseArgs(int argc,
               char** argv,
               std::string* config_path,
               int* run_seconds,
               std::string* error) {
    if (config_path == nullptr || run_seconds == nullptr) {
        if (error != nullptr) {
            *error = "output argument pointer is null";
        }
        return false;
    }

    *config_path = "configs/sim/ctp.yaml";
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

std::vector<quant_hft::RiskPolicyRule> BuildRiskPolicyRules(
    const quant_hft::RiskConfig& risk_config) {
    std::vector<quant_hft::RiskPolicyRule> rules;
    rules.reserve(risk_config.rules.size());
    for (const auto& rule_cfg : risk_config.rules) {
        quant_hft::RiskPolicyRule rule;
        rule.policy_id = rule_cfg.policy_id.empty()
                             ? (rule_cfg.rule_id.empty() ? "policy." + rule_cfg.rule_group
                                                         : rule_cfg.rule_id)
                             : rule_cfg.policy_id;
        rule.policy_scope = rule_cfg.policy_scope.empty() ? "global" : rule_cfg.policy_scope;
        rule.decision_tags = rule_cfg.decision_tags;
        rule.rule_group = rule_cfg.rule_group.empty() ? "default" : rule_cfg.rule_group;
        rule.rule_version = rule_cfg.rule_version.empty() ? "v1" : rule_cfg.rule_version;
        rule.account_id = rule_cfg.account_id;
        rule.instrument_id = rule_cfg.instrument_id;
        rule.window_start_hhmm = rule_cfg.window_start_hhmm;
        rule.window_end_hhmm = rule_cfg.window_end_hhmm;
        rule.max_order_volume = rule_cfg.max_order_volume;
        rule.max_order_notional = rule_cfg.max_order_notional;
        rule.max_active_orders = rule_cfg.max_active_orders;
        rule.max_position_notional = rule_cfg.max_position_notional;
        rules.push_back(std::move(rule));
    }
    return rules;
}

struct ExecutionMetadata {
    std::string execution_algo_id;
    std::int32_t slice_index{0};
    std::int32_t slice_total{0};
    bool throttle_applied{false};
};

double EstimatePositionNotional(const quant_hft::InMemoryPortfolioLedger& ledger,
                                const quant_hft::OrderIntent& intent) {
    const auto long_pos =
        ledger.GetPositionSnapshot(intent.account_id,
                                   intent.instrument_id,
                                   quant_hft::PositionDirection::kLong);
    const auto short_pos =
        ledger.GetPositionSnapshot(intent.account_id,
                                   intent.instrument_id,
                                   quant_hft::PositionDirection::kShort);
    return std::fabs(static_cast<double>(long_pos.volume) * long_pos.avg_price) +
           std::fabs(static_cast<double>(short_pos.volume) * short_pos.avg_price);
}

quant_hft::RiskContext BuildRiskContext(
    const quant_hft::OrderIntent& intent,
    const quant_hft::InMemoryPortfolioLedger& ledger,
    const quant_hft::OrderStateMachine& order_state_machine) {
    quant_hft::RiskContext context;
    context.account_id = intent.account_id;
    context.instrument_id = intent.instrument_id;
    context.active_order_count = static_cast<std::int32_t>(order_state_machine.ActiveOrderCount());
    context.account_position_notional = EstimatePositionNotional(ledger, intent);
    context.session_hhmm = -1;
    return context;
}

quant_hft::OrderEvent BuildRejectedEvent(const quant_hft::OrderIntent& intent,
                                         const std::string& reason,
                                         const ExecutionMetadata& metadata) {
    quant_hft::OrderEvent event;
    event.account_id = intent.account_id;
    event.client_order_id = intent.client_order_id;
    event.exchange_order_id = "internal-reject";
    event.instrument_id = intent.instrument_id;
    event.status = quant_hft::OrderStatus::kRejected;
    event.total_volume = intent.volume;
    event.filled_volume = 0;
    event.avg_fill_price = 0.0;
    event.reason = reason;
    event.ts_ns = quant_hft::NowEpochNanos();
    event.trace_id = intent.trace_id;
    event.execution_algo_id = metadata.execution_algo_id;
    event.slice_index = metadata.slice_index;
    event.slice_total = metadata.slice_total;
    event.throttle_applied = metadata.throttle_applied;
    return event;
}

bool IsTerminalStatus(quant_hft::OrderStatus status) {
    return status == quant_hft::OrderStatus::kFilled ||
           status == quant_hft::OrderStatus::kCanceled ||
           status == quant_hft::OrderStatus::kRejected;
}

}  // namespace

int main(int argc, char** argv) {
    using namespace quant_hft;

    std::string config_path;
    int run_seconds = 0;
    std::string parse_error;
    if (!ParseArgs(argc, argv, &config_path, &run_seconds, &parse_error)) {
        std::cerr << "invalid arguments: " << parse_error << '\n';
        return 1;
    }

    CtpFileConfig file_config;
    std::string error;
    if (!CtpConfigLoader::LoadFromYaml(config_path, &file_config, &error)) {
        std::cerr << "failed to load CTP config: " << error << '\n';
        return 1;
    }
    const auto& config = file_config.runtime;
    const auto instruments = ResolveInstruments(file_config);
    const auto strategy_ids = ResolveStrategyIds(file_config);
    const int poll_interval_ms = std::max(1, file_config.strategy_poll_interval_ms);
    const std::string account_id = file_config.account_id.empty() ? config.user_id
                                                                   : file_config.account_id;
    const ExecutionConfig execution_config = file_config.execution;
    RiskPolicyDefaults risk_defaults;
    risk_defaults.max_order_volume = file_config.risk.default_max_order_volume;
    risk_defaults.max_order_notional = file_config.risk.default_max_order_notional;
    risk_defaults.max_active_orders = file_config.risk.default_max_active_orders;
    risk_defaults.max_position_notional = file_config.risk.default_max_position_notional;
    risk_defaults.policy_id = file_config.risk.default_policy_id;
    risk_defaults.policy_scope = file_config.risk.default_policy_scope;
    risk_defaults.decision_tags = file_config.risk.default_decision_tags;
    risk_defaults.rule_group = file_config.risk.default_rule_group;
    risk_defaults.rule_version = file_config.risk.default_rule_version;
    RiskPolicyEngine risk(risk_defaults, BuildRiskPolicyRules(file_config.risk));
    ExecutionPlanner execution_planner;
    CtpGatewayAdapter ctp_gateway(10);
    InMemoryPortfolioLedger ledger;
    OrderStateMachine order_state_machine;
    std::mutex planner_mutex;
    std::mutex execution_metadata_mutex;
    std::mutex market_history_mutex;
    std::unordered_map<std::string, ExecutionMetadata> execution_metadata_by_order;
    std::unordered_map<std::string, std::vector<MarketSnapshot>> recent_market_history;
    std::mutex cancel_pending_mutex;
    std::unordered_set<std::string> cancel_pending_orders;
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
    StrategyIntentInbox strategy_inbox(pooled_redis);

    auto timescale_client = StorageClientFactory::CreateTimescaleClient(storage_config, &error);
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

    auto process_order_event = [&](const OrderEvent& raw_event) {
        OrderEvent event = raw_event;
        {
            std::lock_guard<std::mutex> lock(execution_metadata_mutex);
            const auto it = execution_metadata_by_order.find(event.client_order_id);
            if (it != execution_metadata_by_order.end()) {
                event.execution_algo_id = it->second.execution_algo_id;
                event.slice_index = it->second.slice_index;
                event.slice_total = it->second.slice_total;
                event.throttle_applied = event.throttle_applied || it->second.throttle_applied;
            }
        }

        bool state_applied = order_state_machine.OnOrderEvent(event);
        if (!state_applied) {
            state_applied = order_state_machine.RecoverFromOrderEvent(event);
        }
        if (!state_applied) {
            std::cerr << "order event dropped for " << event.client_order_id << '\n';
            return;
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
        wal_sink.AppendOrderEvent(event);
        realtime_cache.UpsertOrderEvent(event);
        timeseries_store.AppendOrderEvent(event);

        realtime_cache.UpsertPositionSnapshot(
            ledger.GetPositionSnapshot(event.account_id, event.instrument_id, PositionDirection::kLong));
        realtime_cache.UpsertPositionSnapshot(
            ledger.GetPositionSnapshot(event.account_id, event.instrument_id, PositionDirection::kShort));
    };

    auto process_market_snapshot = [&](const MarketSnapshot& snapshot) {
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
    };

    ctp_gateway.RegisterOrderEventCallback(
        [&](const OrderEvent& event) { process_order_event(event); });
    ctp_gateway.RegisterMarketDataCallback(
        [&](const MarketSnapshot& snapshot) { process_market_snapshot(snapshot); });
    market_state.RegisterStateCallback(
        [&](const StateSnapshot7D& state) { realtime_cache.UpsertStateSnapshot7D(state); });

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
    if (!ctp_gateway.Subscribe(instruments)) {
        std::cerr << "CTP subscribe failed" << '\n';
        return 2;
    }

    std::signal(SIGINT, OnSignal);
    std::signal(SIGTERM, OnSignal);
    g_stop_requested.store(false);

    std::atomic<bool> strategy_loop_stop{false};
    std::thread strategy_poll_thread([&]() {
        auto next_cancel_scan = std::chrono::steady_clock::now();
        while (!strategy_loop_stop.load()) {
            for (const auto& strategy_id : strategy_ids) {
                StrategyIntentBatch batch;
                std::string read_error;
                if (!strategy_inbox.ReadLatest(strategy_id, &batch, &read_error)) {
                    if (read_error.find("missing") == std::string::npos) {
                        std::cerr << "strategy inbox read failed strategy=" << strategy_id
                                  << " error=" << read_error << '\n';
                    }
                    continue;
                }

                for (const auto& signal : batch.intents) {
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
                        metadata.execution_algo_id = planned.execution_algo_id;
                        metadata.slice_index = planned.slice_index;
                        metadata.slice_total = planned.slice_total;
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
                            process_order_event(BuildRejectedEvent(
                                intent,
                                "throttled:reject_ratio_exceeded",
                                metadata));
                            continue;
                        }

                        const auto context = BuildRiskContext(intent, ledger, order_state_machine);
                        const auto decision = risk.PreCheck(intent, context);
                        timeseries_store.AppendRiskDecision(intent, decision);

                        if (decision.action != RiskAction::kAllow) {
                            process_order_event(BuildRejectedEvent(
                                intent, "risk_reject:" + decision.reason, metadata));
                        } else if (!order_state_machine.OnOrderIntent(intent)) {
                            process_order_event(BuildRejectedEvent(
                                intent, "order_state_reject:duplicate_or_invalid", metadata));
                        } else if (!ctp_gateway.PlaceOrder(intent)) {
                            process_order_event(BuildRejectedEvent(
                                intent, "gateway_reject:place_order_failed", metadata));
                        } else {
                            std::lock_guard<std::mutex> lock(planner_mutex);
                            execution_planner.RecordOrderResult(false);
                        }

                        const bool is_last_slice = planned.slice_index == planned.slice_total;
                        const bool interval_enabled =
                            execution_config.algo != ExecutionAlgo::kDirect &&
                            execution_config.slice_interval_ms > 0;
                        if (!is_last_slice && interval_enabled) {
                            std::this_thread::sleep_for(
                                std::chrono::milliseconds(execution_config.slice_interval_ms));
                        }
                    }
                }
            }

            if (execution_config.cancel_after_ms > 0) {
                const auto now = std::chrono::steady_clock::now();
                if (now >= next_cancel_scan) {
                    const auto now_ns = NowEpochNanos();
                    const auto cancel_after_ns =
                        static_cast<EpochNanos>(execution_config.cancel_after_ms) * 1'000'000;
                    const auto cutoff_ns = now_ns - cancel_after_ns;
                    for (const auto& order : order_state_machine.GetActiveOrders()) {
                        if (order.last_update_ts_ns == 0 ||
                            order.last_update_ts_ns > cutoff_ns) {
                            continue;
                        }

                        bool first_request = false;
                        {
                            std::lock_guard<std::mutex> lock(cancel_pending_mutex);
                            first_request = cancel_pending_orders
                                                .insert(order.client_order_id)
                                                .second;
                        }
                        if (!first_request) {
                            continue;
                        }

                        if (!ctp_gateway.CancelOrder(order.client_order_id,
                                                     order.client_order_id)) {
                            std::lock_guard<std::mutex> lock(cancel_pending_mutex);
                            cancel_pending_orders.erase(order.client_order_id);
                        }
                    }

                    next_cancel_scan =
                        now + std::chrono::milliseconds(std::max(
                                  1, execution_config.cancel_check_interval_ms));
                }
            }

            std::this_thread::sleep_for(std::chrono::milliseconds(poll_interval_ms));
        }
    });

    const auto start = std::chrono::steady_clock::now();
    std::size_t synthetic_tick = 0;
    while (!g_stop_requested.load()) {
        if (run_seconds > 0) {
            const auto elapsed = std::chrono::duration_cast<std::chrono::seconds>(
                std::chrono::steady_clock::now() - start);
            if (elapsed.count() >= run_seconds) {
                break;
            }
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
                snapshot.exchange_ts_ns = NowEpochNanos();
                snapshot.recv_ts_ns = snapshot.exchange_ts_ns;
                process_market_snapshot(snapshot);
            }
            ++synthetic_tick;
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(500));
    }

    strategy_loop_stop.store(true);
    if (strategy_poll_thread.joinable()) {
        strategy_poll_thread.join();
    }

    ctp_gateway.Disconnect();
    timeseries_store.Flush();
    wal_sink.Flush();

    std::cout << "core_engine stopped cleanly" << '\n';
    return 0;
}
