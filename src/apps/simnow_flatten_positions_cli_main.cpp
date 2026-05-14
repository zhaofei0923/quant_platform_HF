#include <algorithm>
#include <chrono>
#include <condition_variable>
#include <cctype>
#include <cstdlib>
#include <iostream>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "quant_hft/core/ctp_config_loader.h"
#include "quant_hft/core/ctp_trader_adapter.h"
#include "quant_hft/core/structured_log.h"

namespace {

struct CliOptions {
    std::string config_path;
    std::string flow_path;
    bool execute{false};
    bool skip_settlement_confirm{false};
    int query_timeout_seconds{30};
    int order_wait_seconds{45};
};

struct ClosePlan {
    quant_hft::InvestorPositionSnapshot snapshot;
    quant_hft::Side side{quant_hft::Side::kSell};
    quant_hft::OffsetFlag offset{quant_hft::OffsetFlag::kClose};
    std::int32_t closable{0};
};

struct OrderState {
    std::string client_order_id;
    std::string instrument_id;
    quant_hft::OrderStatus status{quant_hft::OrderStatus::kNew};
    std::int32_t total_volume{0};
    std::int32_t filled_volume{0};
    std::string reason;
};

std::string ToLowerAscii(std::string value) {
    std::transform(value.begin(), value.end(), value.begin(), [](unsigned char ch) {
        return static_cast<char>(std::tolower(ch));
    });
    return value;
}

std::string ToUpperAscii(std::string value) {
    std::transform(value.begin(), value.end(), value.begin(), [](unsigned char ch) {
        return static_cast<char>(std::toupper(ch));
    });
    return value;
}

std::string SideText(quant_hft::Side side) {
    return side == quant_hft::Side::kBuy ? "buy" : "sell";
}

std::string OffsetText(quant_hft::OffsetFlag offset) {
    switch (offset) {
        case quant_hft::OffsetFlag::kCloseToday:
            return "close_today";
        case quant_hft::OffsetFlag::kCloseYesterday:
            return "close_yesterday";
        case quant_hft::OffsetFlag::kClose:
            return "close";
        case quant_hft::OffsetFlag::kOpen:
        default:
            return "open";
    }
}

std::string DirectionText(const quant_hft::InvestorPositionSnapshot& snapshot) {
    const auto direction = ToLowerAscii(snapshot.posi_direction);
    if (direction == "2" || direction == "long" || direction == "l") {
        return "long";
    }
    return "short";
}

bool IsLongPosition(const quant_hft::InvestorPositionSnapshot& snapshot) {
    return DirectionText(snapshot) == "long";
}

bool IsTodayPosition(const quant_hft::InvestorPositionSnapshot& snapshot) {
    const auto position_date = ToLowerAscii(snapshot.position_date);
    if (position_date == "1" || position_date == "today" || position_date == "td") {
        return true;
    }
    if (position_date == "2" || position_date == "history" || position_date == "yesterday" ||
        position_date == "yd") {
        return false;
    }
    return snapshot.today_position > 0 && snapshot.yd_position == 0;
}

quant_hft::OffsetFlag ResolveCloseOffset(const quant_hft::InvestorPositionSnapshot& snapshot) {
    const auto exchange_id = ToUpperAscii(snapshot.exchange_id);
    if (exchange_id == "SHFE" || exchange_id == "INE") {
        return IsTodayPosition(snapshot) ? quant_hft::OffsetFlag::kCloseToday
                                         : quant_hft::OffsetFlag::kCloseYesterday;
    }
    return quant_hft::OffsetFlag::kClose;
}

std::int32_t ClosableVolume(const quant_hft::InvestorPositionSnapshot& snapshot) {
    const auto position = std::max(0, snapshot.position);
    const auto preferred_frozen =
        IsLongPosition(snapshot) ? snapshot.long_frozen : snapshot.short_frozen;
    const auto fallback_frozen = std::max(snapshot.long_frozen, snapshot.short_frozen);
    const auto frozen = std::max(0, preferred_frozen > 0 ? preferred_frozen : fallback_frozen);
    return std::max(0, position - std::min(position, frozen));
}

bool IsTerminal(quant_hft::OrderStatus status) {
    return status == quant_hft::OrderStatus::kFilled ||
           status == quant_hft::OrderStatus::kCanceled ||
           status == quant_hft::OrderStatus::kRejected;
}

bool ParsePositiveInt(const std::string& value, int* out) {
    if (out == nullptr) {
        return false;
    }
    try {
        const int parsed = std::stoi(value);
        if (parsed <= 0) {
            return false;
        }
        *out = parsed;
        return true;
    } catch (...) {
        return false;
    }
}

void PrintUsage(const char* argv0) {
    std::cerr << "Usage: " << argv0
              << " [--config <path>] [--flow-path <path>] [--execute] [--skip-settlement-confirm] "
                 "[--query-timeout-seconds <int>] [--order-wait-seconds <int>]\n";
}

bool ParseArgs(int argc, char** argv, CliOptions* options, std::string* error) {
    if (options == nullptr) {
        if (error != nullptr) {
            *error = "options pointer is null";
        }
        return false;
    }
    const auto quant_root = quant_hft::GetEnvOrDefault("QUANT_ROOT", "");
    const auto default_config = quant_root.empty() ? "configs/sim/ctp.yaml"
                                                   : (quant_root + "/configs/sim/ctp.yaml");
    options->config_path = quant_hft::GetEnvOrDefault("CTP_CONFIG_PATH", default_config);

    for (int index = 1; index < argc; ++index) {
        const std::string arg = argv[index];
        if (arg == "--config") {
            if (index + 1 >= argc) {
                if (error != nullptr) {
                    *error = "--config requires a value";
                }
                return false;
            }
            options->config_path = argv[++index];
            continue;
        }
        if (arg == "--execute") {
            options->execute = true;
            continue;
        }
        if (arg == "--flow-path") {
            if (index + 1 >= argc) {
                if (error != nullptr) {
                    *error = "--flow-path requires a value";
                }
                return false;
            }
            options->flow_path = argv[++index];
            continue;
        }
        if (arg == "--skip-settlement-confirm") {
            options->skip_settlement_confirm = true;
            continue;
        }
        if (arg == "--query-timeout-seconds") {
            if (index + 1 >= argc ||
                !ParsePositiveInt(argv[++index], &options->query_timeout_seconds)) {
                if (error != nullptr) {
                    *error = "--query-timeout-seconds requires a positive integer";
                }
                return false;
            }
            continue;
        }
        if (arg == "--order-wait-seconds") {
            if (index + 1 >= argc || !ParsePositiveInt(argv[++index], &options->order_wait_seconds)) {
                if (error != nullptr) {
                    *error = "--order-wait-seconds requires a positive integer";
                }
                return false;
            }
            continue;
        }
        if (arg == "-h" || arg == "--help") {
            PrintUsage(argv[0]);
            std::exit(0);
        }
        if (error != nullptr) {
            *error = "unknown option: " + arg;
        }
        return false;
    }
    return true;
}

quant_hft::MarketDataConnectConfig BuildConnectConfig(const quant_hft::CtpRuntimeConfig& runtime,
                                                       bool skip_settlement_confirm) {
    quant_hft::MarketDataConnectConfig connect_cfg;
    connect_cfg.market_front_address = runtime.md_front;
    connect_cfg.trader_front_address = runtime.td_front;
    connect_cfg.flow_path = runtime.flow_path;
    connect_cfg.broker_id = runtime.broker_id;
    connect_cfg.user_id = runtime.user_id;
    connect_cfg.investor_id = runtime.investor_id;
    connect_cfg.password = runtime.password;
    connect_cfg.app_id = runtime.app_id;
    connect_cfg.auth_code = runtime.auth_code;
    connect_cfg.is_production_mode = runtime.is_production_mode;
    connect_cfg.enable_real_api = runtime.enable_real_api;
    connect_cfg.enable_terminal_auth = runtime.enable_terminal_auth;
    connect_cfg.connect_timeout_ms = runtime.connect_timeout_ms;
    connect_cfg.reconnect_max_attempts = runtime.reconnect_max_attempts;
    connect_cfg.reconnect_initial_backoff_ms = runtime.reconnect_initial_backoff_ms;
    connect_cfg.reconnect_max_backoff_ms = runtime.reconnect_max_backoff_ms;
    connect_cfg.query_retry_backoff_ms = runtime.query_retry_backoff_ms;
    connect_cfg.recovery_quiet_period_ms = runtime.recovery_quiet_period_ms;
    connect_cfg.settlement_confirm_required = runtime.settlement_confirm_required &&
                                               !skip_settlement_confirm;
    return connect_cfg;
}

std::vector<ClosePlan> BuildClosePlans(
    const std::vector<quant_hft::InvestorPositionSnapshot>& snapshots) {
    std::vector<ClosePlan> plans;
    for (const auto& snapshot : snapshots) {
        if (snapshot.instrument_id.empty() || snapshot.position <= 0) {
            continue;
        }
        ClosePlan plan;
        plan.snapshot = snapshot;
        plan.closable = ClosableVolume(snapshot);
        if (plan.closable <= 0) {
            continue;
        }
        plan.side = IsLongPosition(snapshot) ? quant_hft::Side::kSell : quant_hft::Side::kBuy;
        plan.offset = ResolveCloseOffset(snapshot);
        plans.push_back(std::move(plan));
    }
    return plans;
}

bool QueryPositions(quant_hft::CTPTraderAdapter* trader,
                    int timeout_seconds,
                    std::vector<quant_hft::InvestorPositionSnapshot>* out) {
    if (trader == nullptr || out == nullptr) {
        return false;
    }
    std::mutex mutex;
    std::condition_variable cv;
    bool ready = false;
    std::vector<quant_hft::InvestorPositionSnapshot> snapshots;
    trader->RegisterInvestorPositionSnapshotCallback(
        [&](const std::vector<quant_hft::InvestorPositionSnapshot>& received) {
            {
                std::lock_guard<std::mutex> lock(mutex);
                snapshots = received;
                ready = true;
            }
            cv.notify_all();
        });

    const int request_id = trader->EnqueueInvestorPositionQuery();
    if (request_id < 0) {
        return false;
    }
    std::unique_lock<std::mutex> lock(mutex);
    if (!cv.wait_for(lock, std::chrono::seconds(timeout_seconds), [&]() { return ready; })) {
        return false;
    }
    *out = std::move(snapshots);
    return true;
}

int SubmitCloseOrders(quant_hft::CTPTraderAdapter* trader,
                      const quant_hft::CtpFileConfig& file_config,
                      const std::vector<ClosePlan>& plans,
                      int order_wait_seconds) {
    if (trader == nullptr) {
        return 2;
    }
    std::mutex mutex;
    std::condition_variable cv;
    std::unordered_map<std::string, OrderState> orders;

    trader->RegisterOrderEventCallback([&](const quant_hft::OrderEvent& event) {
        if (event.client_order_id.empty()) {
            return;
        }
        {
            std::lock_guard<std::mutex> lock(mutex);
            auto& state = orders[event.client_order_id];
            state.client_order_id = event.client_order_id;
            state.instrument_id = event.instrument_id;
            state.status = event.status;
            state.total_volume = event.total_volume;
            state.filled_volume = event.filled_volume;
            state.reason = event.reason.empty() ? event.status_msg : event.reason;
        }
        quant_hft::EmitStructuredLog(nullptr,
                                     "simnow_flatten_positions",
                                     "info",
                                     "order_event",
                                     {{"instrument_id", event.instrument_id},
                                      {"client_order_id", event.client_order_id},
                                      {"filled_volume", std::to_string(event.filled_volume)},
                                      {"total_volume", std::to_string(event.total_volume)},
                                      {"status", std::to_string(static_cast<int>(event.status))},
                                      {"reason", event.reason.empty() ? event.status_msg
                                                                       : event.reason}});
        cv.notify_all();
    });

    const std::string account_id =
        file_config.account_id.empty() ? file_config.runtime.investor_id : file_config.account_id;
    int submit_failures = 0;
    for (std::size_t index = 0; index < plans.size(); ++index) {
        const auto& plan = plans[index];
        quant_hft::OrderIntent intent;
        intent.account_id = account_id;
        intent.strategy_id = "manual_flatten";
        intent.instrument_id = plan.snapshot.instrument_id;
        intent.side = plan.side;
        intent.offset = plan.offset;
        intent.type = quant_hft::OrderType::kMarket;
        intent.volume = plan.closable;
        intent.price = 0.0;
        intent.trace_id = "manual-flatten-" + std::to_string(index + 1);

        const auto client_order_id = trader->PlaceOrderWithRef(intent);
        if (client_order_id.empty()) {
            ++submit_failures;
            quant_hft::EmitStructuredLog(nullptr,
                                         "simnow_flatten_positions",
                                         "error",
                                         "close_order_submit_failed",
                                         {{"instrument_id", plan.snapshot.instrument_id},
                                          {"side", SideText(plan.side)},
                                          {"offset", OffsetText(plan.offset)},
                                          {"volume", std::to_string(plan.closable)}});
            continue;
        }
        {
            std::lock_guard<std::mutex> lock(mutex);
            orders[client_order_id] = OrderState{client_order_id,
                                                 plan.snapshot.instrument_id,
                                                 quant_hft::OrderStatus::kNew,
                                                 plan.closable,
                                                 0,
                                                 "submitted"};
        }
        quant_hft::EmitStructuredLog(nullptr,
                                     "simnow_flatten_positions",
                                     "info",
                                     "close_order_submitted",
                                     {{"instrument_id", plan.snapshot.instrument_id},
                                      {"client_order_id", client_order_id},
                                      {"side", SideText(plan.side)},
                                      {"offset", OffsetText(plan.offset)},
                                      {"volume", std::to_string(plan.closable)}});
    }

    std::unique_lock<std::mutex> lock(mutex);
    cv.wait_for(lock, std::chrono::seconds(order_wait_seconds), [&]() {
        if (orders.empty()) {
            return true;
        }
        return std::all_of(orders.begin(), orders.end(), [](const auto& pair) {
            return IsTerminal(pair.second.status);
        });
    });

    int incomplete = submit_failures;
    for (const auto& [client_order_id, state] : orders) {
        if (!IsTerminal(state.status) || state.status == quant_hft::OrderStatus::kRejected ||
            state.status == quant_hft::OrderStatus::kCanceled) {
            ++incomplete;
            quant_hft::EmitStructuredLog(nullptr,
                                         "simnow_flatten_positions",
                                         "warn",
                                         "close_order_not_filled",
                                         {{"instrument_id", state.instrument_id},
                                          {"client_order_id", client_order_id},
                                          {"filled_volume", std::to_string(state.filled_volume)},
                                          {"total_volume", std::to_string(state.total_volume)},
                                          {"status", std::to_string(static_cast<int>(state.status))},
                                          {"reason", state.reason}});
        }
    }
    return incomplete == 0 ? 0 : 3;
}

}  // namespace

int main(int argc, char** argv) {
    using namespace quant_hft;

    CtpRuntimeConfig bootstrap_runtime;
    CliOptions options;
    std::string error;
    if (!ParseArgs(argc, argv, &options, &error)) {
        PrintUsage(argv[0]);
        EmitStructuredLog(&bootstrap_runtime,
                          "simnow_flatten_positions",
                          "error",
                          "invalid_arguments",
                          {{"error", error}});
        return 1;
    }

    CtpFileConfig file_config;
    if (!CtpConfigLoader::LoadFromYaml(options.config_path, &file_config, &error)) {
        EmitStructuredLog(&bootstrap_runtime,
                          "simnow_flatten_positions",
                          "error",
                          "config_load_failed",
                          {{"config_path", options.config_path}, {"error", error}});
        return 1;
    }

    auto runtime = file_config.runtime;
    if (!options.flow_path.empty()) {
        runtime.flow_path = options.flow_path;
    }
    runtime.enable_real_api = true;
    EmitStructuredLog(&runtime,
                      "simnow_flatten_positions",
                      "info",
                      "run_started",
                      {{"config_path", options.config_path},
                       {"execute", options.execute ? "true" : "false"},
                       {"skip_settlement_confirm",
                        options.skip_settlement_confirm ? "true" : "false"}});

    CTPTraderAdapter trader(static_cast<std::size_t>(std::max(1, runtime.query_rate_per_sec)), 1);
    const auto connect_cfg = BuildConnectConfig(runtime, options.skip_settlement_confirm);
    if (!trader.Connect(connect_cfg)) {
        EmitStructuredLog(&runtime,
                          "simnow_flatten_positions",
                          "error",
                          "trader_connect_failed",
                          {{"diagnostic", trader.GetLastConnectDiagnostic()}});
        return 2;
    }
    if (runtime.settlement_confirm_required && !options.skip_settlement_confirm &&
        !trader.ConfirmSettlement()) {
        EmitStructuredLog(&runtime,
                          "simnow_flatten_positions",
                          "error",
                          "settlement_confirm_failed");
        trader.Disconnect();
        return 2;
    }
    if (options.skip_settlement_confirm) {
        EmitStructuredLog(&runtime,
                          "simnow_flatten_positions",
                          "warn",
                          "settlement_confirm_skipped");
    }

    std::vector<InvestorPositionSnapshot> snapshots;
    if (!QueryPositions(&trader, options.query_timeout_seconds, &snapshots)) {
        EmitStructuredLog(&runtime,
                          "simnow_flatten_positions",
                          "error",
                          "position_query_failed");
        trader.Disconnect();
        return 2;
    }

    const auto plans = BuildClosePlans(snapshots);
    for (const auto& plan : plans) {
        EmitStructuredLog(&runtime,
                          "simnow_flatten_positions",
                          "info",
                          "open_position",
                          {{"instrument_id", plan.snapshot.instrument_id},
                           {"exchange_id", plan.snapshot.exchange_id},
                           {"direction", DirectionText(plan.snapshot)},
                           {"position_date", plan.snapshot.position_date},
                           {"position", std::to_string(plan.snapshot.position)},
                           {"today_position", std::to_string(plan.snapshot.today_position)},
                           {"yd_position", std::to_string(plan.snapshot.yd_position)},
                           {"closable", std::to_string(plan.closable)},
                           {"close_side", SideText(plan.side)},
                           {"close_offset", OffsetText(plan.offset)}});
    }
    EmitStructuredLog(&runtime,
                      "simnow_flatten_positions",
                      "info",
                      "position_summary",
                      {{"open_position_rows", std::to_string(plans.size())}});

    if (plans.empty()) {
        trader.Disconnect();
        EmitStructuredLog(&runtime, "simnow_flatten_positions", "info", "nothing_to_flatten");
        return 0;
    }
    if (!options.execute) {
        trader.Disconnect();
        EmitStructuredLog(&runtime,
                          "simnow_flatten_positions",
                          "info",
                          "dry_run_completed",
                          {{"hint", "rerun with --execute to submit close orders"}});
        return 0;
    }

    const int close_result = SubmitCloseOrders(&trader, file_config, plans, options.order_wait_seconds);

    std::vector<InvestorPositionSnapshot> after_snapshots;
    if (QueryPositions(&trader, options.query_timeout_seconds, &after_snapshots)) {
        const auto remaining = BuildClosePlans(after_snapshots);
        EmitStructuredLog(&runtime,
                          "simnow_flatten_positions",
                          remaining.empty() ? "info" : "warn",
                          "remaining_position_summary",
                          {{"open_position_rows", std::to_string(remaining.size())}});
    } else {
        EmitStructuredLog(&runtime,
                          "simnow_flatten_positions",
                          "warn",
                          "remaining_position_query_failed");
    }

    trader.Disconnect();
    return close_result;
}