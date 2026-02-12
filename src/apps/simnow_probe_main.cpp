#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <iostream>
#include <string>
#include <thread>

#include "quant_hft/core/ctp_config_loader.h"
#include "quant_hft/core/ctp_md_adapter.h"
#include "quant_hft/core/structured_log.h"
#include "quant_hft/core/ctp_trader_adapter.h"

int main(int argc, char** argv) {
    using namespace quant_hft;
    CtpRuntimeConfig bootstrap_runtime;

#if !(defined(QUANT_HFT_ENABLE_CTP_REAL_API) && QUANT_HFT_ENABLE_CTP_REAL_API)
    EmitStructuredLog(&bootstrap_runtime,
                      "simnow_probe",
                      "error",
                      "ctp_real_api_disabled",
                      {{"hint", "rebuild with -DQUANT_HFT_ENABLE_CTP_REAL_API=ON"}});
    return 2;
#endif

    const auto quant_root = GetEnvOrDefault("QUANT_ROOT", "");
    const auto default_config =
        quant_root.empty() ? "configs/sim/ctp.yaml" : (quant_root + "/configs/sim/ctp.yaml");
    std::string config_path = GetEnvOrDefault("CTP_CONFIG_PATH", default_config);
    int monitor_seconds = 300;
    int health_interval_ms = 1000;
    for (int i = 1; i < argc; ++i) {
        const std::string arg = argv[i];
        if (arg == "--monitor-seconds" && i + 1 < argc) {
            monitor_seconds = std::stoi(argv[++i]);
            continue;
        }
        if (arg == "--health-interval-ms" && i + 1 < argc) {
            health_interval_ms = std::stoi(argv[++i]);
            continue;
        }
        if (!arg.empty() && arg.rfind("--", 0) == 0) {
            EmitStructuredLog(&bootstrap_runtime,
                              "simnow_probe",
                              "error",
                              "invalid_argument",
                              {{"arg", arg}});
            return 2;
        }
        config_path = arg;
    }
    CtpFileConfig file_config;
    std::string error;
    if (!CtpConfigLoader::LoadFromYaml(config_path, &file_config, &error)) {
        EmitStructuredLog(&bootstrap_runtime,
                          "simnow_probe",
                          "error",
                          "config_load_failed",
                          {{"config_path", config_path}, {"error", error}});
        return 3;
    }
    auto runtime = file_config.runtime;
    runtime.enable_real_api = true;

    MarketDataConnectConfig cfg;
    cfg.market_front_address = runtime.md_front;
    cfg.trader_front_address = runtime.td_front;
    cfg.flow_path = runtime.flow_path;
    cfg.broker_id = runtime.broker_id;
    cfg.user_id = runtime.user_id;
    cfg.investor_id = runtime.investor_id;
    cfg.app_id = runtime.app_id;
    cfg.auth_code = runtime.auth_code;
    cfg.is_production_mode = runtime.is_production_mode;
    cfg.enable_real_api = runtime.enable_real_api;
    cfg.enable_terminal_auth = runtime.enable_terminal_auth;
    cfg.connect_timeout_ms = runtime.connect_timeout_ms;
    cfg.reconnect_max_attempts = runtime.reconnect_max_attempts;
    cfg.reconnect_initial_backoff_ms = runtime.reconnect_initial_backoff_ms;
    cfg.reconnect_max_backoff_ms = runtime.reconnect_max_backoff_ms;
    cfg.password = runtime.password;
    cfg.recovery_quiet_period_ms = runtime.recovery_quiet_period_ms;
    cfg.settlement_confirm_required = runtime.settlement_confirm_required;

    CTPTraderAdapter trader(10, 1);
    CTPMdAdapter md(10, 1);

    EmitStructuredLog(&runtime,
                      "simnow_probe",
                      "info",
                      "probe_started",
                      {{"config_path", config_path}});

    md.RegisterTickCallback([](const MarketSnapshot& snapshot) {
        EmitStructuredLog(nullptr,
                          "simnow_probe",
                          "info",
                          "md_tick",
                          {{"instrument_id", snapshot.instrument_id},
                           {"last_price", std::to_string(snapshot.last_price)},
                           {"bid1", std::to_string(snapshot.bid_price_1)},
                           {"ask1", std::to_string(snapshot.ask_price_1)}});
    });

    trader.RegisterOrderEventCallback([](const OrderEvent& event) {
        EmitStructuredLog(nullptr,
                          "simnow_probe",
                          "info",
                          "order_event",
                          {{"client_order_id", event.client_order_id},
                           {"status", std::to_string(static_cast<int>(event.status))},
                           {"filled_volume", std::to_string(event.filled_volume)}});
    });

    if (!trader.Connect(cfg)) {
        EmitStructuredLog(&runtime,
                          "simnow_probe",
                          "error",
                          "trader_connect_failed",
                          {{"md_front", cfg.market_front_address},
                           {"td_front", cfg.trader_front_address}});
        const auto diagnostic = trader.GetLastConnectDiagnostic();
        if (!diagnostic.empty()) {
            EmitStructuredLog(
                &runtime, "simnow_probe", "error", "connect_diagnostic", {{"detail", diagnostic}});
        }
        return 4;
    }
    if (!md.Connect(cfg)) {
        EmitStructuredLog(&runtime,
                          "simnow_probe",
                          "error",
                          "md_connect_failed",
                          {{"md_front", cfg.market_front_address},
                           {"td_front", cfg.trader_front_address}});
        const auto diagnostic = md.GetLastConnectDiagnostic();
        if (!diagnostic.empty()) {
            EmitStructuredLog(
                &runtime, "simnow_probe", "error", "connect_diagnostic", {{"detail", diagnostic}});
        }
        return 4;
    }
    if (!trader.ConfirmSettlement()) {
        EmitStructuredLog(&runtime, "simnow_probe", "error", "settlement_confirm_failed");
        return 4;
    }

    const std::string instrument = std::getenv("CTP_SIM_INSTRUMENT") != nullptr
                                       ? std::string(std::getenv("CTP_SIM_INSTRUMENT"))
                                       : "SHFE.ag2406";
    if (!md.Subscribe({instrument})) {
        EmitStructuredLog(
            &runtime, "simnow_probe", "error", "subscribe_failed", {{"instrument_id", instrument}});
        return 5;
    }

    trader.EnqueueUserSessionQuery(1);
    const auto session = trader.GetLastUserSession();
    EmitStructuredLog(&runtime,
                      "simnow_probe",
                      "info",
                      "session_snapshot",
                      {{"investor_id", session.investor_id},
                       {"login_time", session.login_time},
                       {"last_login_time", session.last_login_time}});

    const auto started_at = std::chrono::steady_clock::now();
    while (monitor_seconds < 0 ||
           std::chrono::duration_cast<std::chrono::seconds>(std::chrono::steady_clock::now() -
                                                            started_at)
                   .count() < monitor_seconds) {
        const bool healthy = trader.IsReady() && md.IsReady();
        EmitStructuredLog(&runtime,
                          "simnow_probe",
                          healthy ? "info" : "warn",
                          "health_status",
                          {{"state", healthy ? "healthy" : "unhealthy"}});
        std::this_thread::sleep_for(
            std::chrono::milliseconds(std::max(100, health_interval_ms)));
    }
    md.Disconnect();
    trader.Disconnect();
    EmitStructuredLog(&runtime, "simnow_probe", "info", "probe_completed");
    return 0;
}
