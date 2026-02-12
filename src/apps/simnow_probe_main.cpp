#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <iostream>
#include <string>
#include <thread>

#include "quant_hft/core/ctp_config_loader.h"
#include "quant_hft/core/ctp_md_adapter.h"
#include "quant_hft/core/ctp_trader_adapter.h"

int main(int argc, char** argv) {
    using namespace quant_hft;

#if !(defined(QUANT_HFT_ENABLE_CTP_REAL_API) && QUANT_HFT_ENABLE_CTP_REAL_API)
    std::cerr << "Real CTP API is disabled. Rebuild with -DQUANT_HFT_ENABLE_CTP_REAL_API=ON\n";
    return 2;
#endif

    std::string config_path = "configs/sim/ctp.yaml";
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
            std::cerr << "Unknown option: " << arg << '\n';
            return 2;
        }
        config_path = arg;
    }
    CtpFileConfig file_config;
    std::string error;
    if (!CtpConfigLoader::LoadFromYaml(config_path, &file_config, &error)) {
        std::cerr << "Failed to load CTP config from " << config_path << ": " << error << '\n';
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

    md.RegisterTickCallback([](const MarketSnapshot& snapshot) {
        std::cout << "[md] " << snapshot.instrument_id << " last=" << snapshot.last_price
                  << " bid1=" << snapshot.bid_price_1 << " ask1=" << snapshot.ask_price_1
                  << std::endl;
    });

    trader.RegisterOrderEventCallback([](const OrderEvent& event) {
        std::cout << "[order] " << event.client_order_id << " status=" << static_cast<int>(event.status)
                  << " filled=" << event.filled_volume << std::endl;
    });

    if (!trader.Connect(cfg)) {
        std::cerr << "Connect failed. fronts: md=" << cfg.market_front_address
                  << " td=" << cfg.trader_front_address << '\n';
        const auto diagnostic = trader.GetLastConnectDiagnostic();
        if (!diagnostic.empty()) {
            std::cerr << "Connect diagnostic: " << diagnostic << '\n';
        }
        return 4;
    }
    if (!md.Connect(cfg)) {
        std::cerr << "Connect failed. fronts: md=" << cfg.market_front_address
                  << " td=" << cfg.trader_front_address << '\n';
        const auto diagnostic = md.GetLastConnectDiagnostic();
        if (!diagnostic.empty()) {
            std::cerr << "Connect diagnostic: " << diagnostic << '\n';
        }
        return 4;
    }
    if (!trader.ConfirmSettlement()) {
        std::cerr << "Settlement confirm failed\n";
        return 4;
    }

    const std::string instrument = std::getenv("CTP_SIM_INSTRUMENT") != nullptr
                                       ? std::string(std::getenv("CTP_SIM_INSTRUMENT"))
                                       : "SHFE.ag2406";
    if (!md.Subscribe({instrument})) {
        std::cerr << "Subscribe failed for " << instrument << '\n';
        return 5;
    }

    trader.EnqueueUserSessionQuery(1);
    const auto session = trader.GetLastUserSession();
    std::cout << "[session] investor=" << session.investor_id
              << " login=" << session.login_time
              << " last_login=" << session.last_login_time << std::endl;

    const auto started_at = std::chrono::steady_clock::now();
    while (monitor_seconds < 0 ||
           std::chrono::duration_cast<std::chrono::seconds>(std::chrono::steady_clock::now() -
                                                            started_at)
                   .count() < monitor_seconds) {
        const bool healthy = trader.IsReady() && md.IsReady();
        std::cout << "[health] ts_ns=" << NowEpochNanos()
                  << " state=" << (healthy ? "healthy" : "unhealthy") << std::endl;
        std::this_thread::sleep_for(
            std::chrono::milliseconds(std::max(100, health_interval_ms)));
    }
    md.Disconnect();
    trader.Disconnect();
    std::cout << "Probe completed" << std::endl;
    return 0;
}
