#include <algorithm>
#include <cctype>
#include <chrono>
#include <condition_variable>
#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "quant_hft/core/ctp_config_loader.h"
#include "quant_hft/core/ctp_md_adapter.h"
#include "quant_hft/core/ctp_trader_adapter.h"
#include "quant_hft/core/instrument_meta_cache.h"
#include "quant_hft/core/structured_log.h"
#include "quant_hft/services/dominant_contract_coordinator.h"
#include "quant_hft/services/trading_session_calendar.h"

namespace {

std::string ToLowerAscii(std::string value) {
    std::transform(value.begin(), value.end(), value.begin(),
                   [](unsigned char ch) { return static_cast<char>(std::tolower(ch)); });
    return value;
}

std::string ExtractProductId(const std::string& instrument) {
    std::string symbol = instrument;
    const auto dot = symbol.find('.');
    if (dot != std::string::npos) {
        symbol = symbol.substr(dot + 1);
    }
    std::string product_id;
    for (unsigned char ch : symbol) {
        if (std::isalpha(ch) == 0) {
            break;
        }
        product_id.push_back(static_cast<char>(std::tolower(ch)));
    }
    return product_id;
}

std::vector<std::string> CollectConfiguredProductIds(const std::vector<std::string>& instruments) {
    std::unordered_set<std::string> seen;
    std::vector<std::string> product_ids;
    for (const auto& instrument : instruments) {
        const std::string product_id = ExtractProductId(instrument);
        if (!product_id.empty() && seen.insert(product_id).second) {
            product_ids.push_back(product_id);
        }
    }
    return product_ids;
}

std::vector<std::string> ResolveConfiguredProductIds(const quant_hft::CtpFileConfig& file_config,
                                                     const std::string& fallback_instrument) {
    if (!file_config.product_ids.empty()) {
        std::vector<std::string> normalized = file_config.product_ids;
        for (auto& product_id : normalized) {
            product_id = ToLowerAscii(product_id);
        }
        return normalized;
    }

    std::vector<std::string> configured_instruments = file_config.instruments;
    if (configured_instruments.empty() && !fallback_instrument.empty()) {
        configured_instruments.push_back(fallback_instrument);
    }
    return CollectConfiguredProductIds(configured_instruments);
}

std::filesystem::path InstrumentUniverseManifestPath() {
    const char* configured_root = std::getenv("SIMNOW_INSTRUMENT_CACHE_ROOT");
    const std::filesystem::path root = configured_root == nullptr || configured_root[0] == '\0'
                                           ? std::filesystem::path("runtime/ctp_instruments")
                                           : std::filesystem::path(configured_root);
    return root / "contract_universe_manifest.json";
}

}  // namespace

int main(int argc, char** argv) {
    using namespace quant_hft;
    CtpRuntimeConfig bootstrap_runtime;

#if !(defined(QUANT_HFT_ENABLE_CTP_REAL_API) && QUANT_HFT_ENABLE_CTP_REAL_API)
    EmitStructuredLog(&bootstrap_runtime, "simnow_probe", "error", "ctp_real_api_disabled",
                      {{"hint", "rebuild with -DQUANT_HFT_ENABLE_CTP_REAL_API=ON"}});
    return 2;
#endif

    const auto quant_root = GetEnvOrDefault("QUANT_ROOT", "");
    const auto default_config =
        quant_root.empty() ? "configs/sim/ctp.yaml" : (quant_root + "/configs/sim/ctp.yaml");
    std::string config_path = GetEnvOrDefault("CTP_CONFIG_PATH", default_config);
    int monitor_seconds = 300;
    int health_interval_ms = 1000;
    int instrument_timeout_seconds = 15;
    bool force_instrument_refresh = false;
    bool preopen_connectivity_only = false;
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
        if (arg == "--instrument-timeout-seconds" && i + 1 < argc) {
            instrument_timeout_seconds = std::stoi(argv[++i]);
            continue;
        }
        if (arg == "--force-instrument-refresh") {
            force_instrument_refresh = true;
            continue;
        }
        if (arg == "--preopen-connectivity-only") {
            preopen_connectivity_only = true;
            continue;
        }
        if (!arg.empty() && arg.rfind("--", 0) == 0) {
            EmitStructuredLog(&bootstrap_runtime, "simnow_probe", "error", "invalid_argument",
                              {{"arg", arg}});
            return 2;
        }
        config_path = arg;
    }
    CtpFileConfig file_config;
    std::string error;
    if (!CtpConfigLoader::LoadFromYaml(config_path, &file_config, &error)) {
        EmitStructuredLog(&bootstrap_runtime, "simnow_probe", "error", "config_load_failed",
                          {{"config_path", config_path}, {"error", error}});
        return 3;
    }
    auto runtime = file_config.runtime;
    runtime.enable_real_api = true;
    const bool dominant_contract_mode =
        file_config.active_contract_mode == "dominant_open_interest";

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

    auto gateway = std::make_shared<CtpGatewayAdapter>(10);
    CTPTraderAdapter trader(gateway, 1);
    CTPMdAdapter md(gateway, 1);
    std::mutex instrument_meta_mutex;
    std::condition_variable instrument_meta_cv;
    bool instrument_meta_ready = false;
    std::vector<InstrumentMetaSnapshot> instrument_meta_snapshots;
    std::mutex depth_market_mutex;
    std::condition_variable depth_market_cv;
    bool depth_market_ready = false;
    std::vector<MarketSnapshot> depth_market_snapshots;
    std::mutex market_snapshot_mutex;
    std::condition_variable market_snapshot_cv;
    std::unordered_map<std::string, MarketSnapshot> dominant_candidate_snapshots;
    std::unordered_set<std::string> dominant_candidate_ids;

    EmitStructuredLog(&runtime, "simnow_probe", "info", "probe_started",
                      {{"config_path", config_path}});

    md.RegisterTickCallback([&](const MarketSnapshot& snapshot) {
        if (dominant_contract_mode) {
            bool tracked = false;
            {
                std::lock_guard<std::mutex> lock(market_snapshot_mutex);
                if (dominant_candidate_ids.find(snapshot.instrument_id) !=
                    dominant_candidate_ids.end()) {
                    auto& latest = dominant_candidate_snapshots[snapshot.instrument_id];
                    const EpochNanos incoming_event_ts =
                        snapshot.exchange_ts_ns > 0 ? snapshot.exchange_ts_ns : snapshot.recv_ts_ns;
                    const EpochNanos current_event_ts =
                        latest.exchange_ts_ns > 0 ? latest.exchange_ts_ns : latest.recv_ts_ns;
                    if (latest.instrument_id.empty() || incoming_event_ts > current_event_ts ||
                        (incoming_event_ts == current_event_ts &&
                         snapshot.recv_ts_ns >= latest.recv_ts_ns)) {
                        latest = snapshot;
                    }
                    tracked = true;
                }
            }
            if (tracked) {
                market_snapshot_cv.notify_all();
            }
            return;
        }

        EmitStructuredLog(nullptr, "simnow_probe", "info", "md_tick",
                          {{"instrument_id", snapshot.instrument_id},
                           {"last_price", std::to_string(snapshot.last_price)},
                           {"bid1", std::to_string(snapshot.bid_price_1)},
                           {"ask1", std::to_string(snapshot.ask_price_1)}});
    });

    trader.RegisterOrderEventCallback([](const OrderEvent& event) {
        EmitStructuredLog(nullptr, "simnow_probe", "info", "order_event",
                          {{"client_order_id", event.client_order_id},
                           {"status", std::to_string(static_cast<int>(event.status))},
                           {"filled_volume", std::to_string(event.filled_volume)}});
    });

    trader.RegisterInstrumentMetaSnapshotCallback(
        [&](const std::vector<InstrumentMetaSnapshot>& snapshots) {
            {
                std::lock_guard<std::mutex> lock(instrument_meta_mutex);
                instrument_meta_snapshots = snapshots;
                instrument_meta_ready = true;
            }
            instrument_meta_cv.notify_all();
        });
    trader.RegisterDepthMarketSnapshotCallback([&](const std::vector<MarketSnapshot>& snapshots) {
        {
            std::lock_guard<std::mutex> lock(depth_market_mutex);
            depth_market_snapshots = snapshots;
            depth_market_ready = true;
        }
        depth_market_cv.notify_all();
    });

    if (!trader.Connect(cfg)) {
        EmitStructuredLog(
            &runtime, "simnow_probe", "error", "trader_connect_failed",
            {{"md_front", cfg.market_front_address}, {"td_front", cfg.trader_front_address}});
        const auto diagnostic = trader.GetLastConnectDiagnostic();
        if (!diagnostic.empty()) {
            EmitStructuredLog(&runtime, "simnow_probe", "error", "connect_diagnostic",
                              {{"detail", diagnostic}});
        }
        return 4;
    }
    if (!md.Connect(cfg)) {
        EmitStructuredLog(
            &runtime, "simnow_probe", "error", "md_connect_failed",
            {{"md_front", cfg.market_front_address}, {"td_front", cfg.trader_front_address}});
        const auto diagnostic = md.GetLastConnectDiagnostic();
        if (!diagnostic.empty()) {
            EmitStructuredLog(&runtime, "simnow_probe", "error", "connect_diagnostic",
                              {{"detail", diagnostic}});
        }
        return 4;
    }
    if (!trader.ConfirmSettlement()) {
        EmitStructuredLog(&runtime, "simnow_probe", "error", "settlement_confirm_failed");
        return 4;
    }
    EmitStructuredLog(
        &runtime, "simnow_probe", "info", "settlement_confirmed",
        {{"settlement_confirm_required", runtime.settlement_confirm_required ? "true" : "false"}});

    std::string instrument =
        std::getenv("CTP_SIM_INSTRUMENT") != nullptr
            ? std::string(std::getenv("CTP_SIM_INSTRUMENT"))
            : (!file_config.instruments.empty() ? file_config.instruments.front() : "SHFE.ag2406");
    if (!dominant_contract_mode && !md.Subscribe({instrument})) {
        EmitStructuredLog(&runtime, "simnow_probe", "error", "subscribe_failed",
                          {{"instrument_id", instrument}});
        return 5;
    }

    trader.EnqueueUserSessionQuery(1);
    const auto session = trader.GetLastUserSession();
    EmitStructuredLog(&runtime, "simnow_probe", "info", "session_snapshot",
                      {{"investor_id", session.investor_id},
                       {"login_time", session.login_time},
                       {"last_login_time", session.last_login_time}});

    const int trading_account_request_id = trader.EnqueueTradingAccountQuery();
    if (trading_account_request_id < 0) {
        EmitStructuredLog(&runtime, "simnow_probe", "error", "trading_account_query_submit_failed",
                          {{"reason", "enqueue_failed"}});
        return 6;
    }

    TradingAccountSnapshot trading_account;
    bool has_trading_account_snapshot = false;
    const int trading_account_wait_attempts = std::max(1, instrument_timeout_seconds * 10);
    for (int attempt = 0; attempt < trading_account_wait_attempts; ++attempt) {
        trading_account = trader.GetLastTradingAccountSnapshot();
        if (trading_account.ts_ns > 0) {
            has_trading_account_snapshot = true;
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    if (!has_trading_account_snapshot) {
        EmitStructuredLog(&runtime, "simnow_probe", "error", "trading_account_query_timeout",
                          {{"request_id", std::to_string(trading_account_request_id)}});
        return 6;
    }

    EmitStructuredLog(&runtime, "simnow_probe", "info", "trading_account_snapshot",
                      {{"account_id", trading_account.account_id},
                       {"investor_id", trading_account.investor_id},
                       {"balance", std::to_string(trading_account.balance)},
                       {"available", std::to_string(trading_account.available)},
                       {"curr_margin", std::to_string(trading_account.curr_margin)},
                       {"frozen_margin", std::to_string(trading_account.frozen_margin)},
                       {"close_profit", std::to_string(trading_account.close_profit)},
                       {"position_profit", std::to_string(trading_account.position_profit)},
                       {"trading_day", trading_account.trading_day},
                       {"source", trading_account.source}});

    const auto product_ids = ResolveConfiguredProductIds(file_config, instrument);
    if (product_ids.empty()) {
        EmitStructuredLog(&runtime, "simnow_probe", "error", "instrument_product_filter_missing",
                          {{"instrument_id", instrument}});
        return 7;
    }

    auto query_single_instrument_meta = [&](const std::string& target_instrument_id,
                                            const std::string& event_prefix) {
        {
            std::lock_guard<std::mutex> lock(instrument_meta_mutex);
            instrument_meta_snapshots.clear();
            instrument_meta_ready = false;
        }
        const int request_id = trader.EnqueueInstrumentQuery(target_instrument_id);
        if (request_id < 0) {
            EmitStructuredLog(
                &runtime, "simnow_probe", "error", event_prefix + "_submit_failed",
                {{"instrument_id", target_instrument_id}, {"reason", "enqueue_failed"}});
            return false;
        }
        std::unique_lock<std::mutex> lock(instrument_meta_mutex);
        if (!instrument_meta_cv.wait_for(
                lock, std::chrono::seconds(instrument_timeout_seconds), [&]() {
                    return instrument_meta_ready &&
                           std::any_of(instrument_meta_snapshots.begin(),
                                       instrument_meta_snapshots.end(), [&](const auto& snapshot) {
                                           return snapshot.instrument_id == target_instrument_id;
                                       });
                })) {
            EmitStructuredLog(&runtime, "simnow_probe", "error", event_prefix + "_timeout",
                              {{"instrument_id", target_instrument_id},
                               {"request_id", std::to_string(request_id)},
                               {"timeout_seconds", std::to_string(instrument_timeout_seconds)}});
            return false;
        }
        return true;
    };

    std::vector<InstrumentMetaSnapshot> received_instrument_meta;
    const std::string broker_trading_day = !trading_account.trading_day.empty()
                                               ? trading_account.trading_day
                                               : trader.GetLastUserSession().trading_day;
    if (dominant_contract_mode) {
        if (broker_trading_day.size() != 8U) {
            EmitStructuredLog(&runtime, "simnow_probe", "error", "instrument_trading_day_missing");
            return 7;
        }
        InstrumentUniverseManifest manifest;
        std::string manifest_error;
        const bool manifest_loaded = LoadInstrumentUniverseManifest(
            InstrumentUniverseManifestPath().string(), &manifest, &manifest_error);
        const bool manifest_current =
            manifest_loaded && IsInstrumentUniverseManifestCurrent(
                                   manifest, broker_trading_day, product_ids, NowEpochNanos(),
                                   file_config.dominant_contract_cache_max_age_ms, &manifest_error);
        EmitStructuredLog(&runtime, "simnow_probe", manifest_current ? "info" : "error",
                          manifest_current ? "instrument_universe_cache_current"
                                           : "instrument_universe_cache_invalid",
                          {{"trading_day", broker_trading_day},
                           {"force_refresh", force_instrument_refresh ? "true" : "false"},
                           {"reason", manifest_current ? "" : manifest_error}});
        if (!manifest_current) {
            return 7;
        }

        for (const auto& product_id : product_ids) {
            InstrumentMetaCacheDocument cached;
            std::string cache_error;
            std::string cache_reason;
            const auto cache_path = std::filesystem::path(manifest.cache_dir) /
                                    (ToLowerAscii(product_id) + "_contracts.json");
            const bool loaded =
                LoadInstrumentMetaCacheDocument(cache_path.string(), &cached, &cache_error);
            const bool current = loaded && cached.generated_ts_ns == manifest.generated_ts_ns &&
                                 IsInstrumentMetaCacheCurrent(
                                     cached, broker_trading_day, NowEpochNanos(),
                                     file_config.dominant_contract_cache_max_age_ms, &cache_reason);
            EmitStructuredLog(
                &runtime, "simnow_probe", current ? "info" : "error",
                current ? "instrument_meta_cache_current" : "instrument_meta_cache_invalid",
                {{"product_id", product_id},
                 {"path", cache_path.string()},
                 {"reason", loaded ? cache_reason : cache_error}});
            if (!current) {
                return 7;
            }
            const auto eligible =
                FilterEligibleFuturesContracts(cached.instruments, product_id, broker_trading_day);
            if (eligible.empty()) {
                EmitStructuredLog(
                    &runtime, "simnow_probe", "error", "instrument_meta_filter_empty",
                    {{"product_id", product_id}, {"trading_day", broker_trading_day}});
                return 7;
            }
            md.UpdateInstrumentMetadata(cached.instruments);
            received_instrument_meta.insert(received_instrument_meta.end(), eligible.begin(),
                                            eligible.end());
            EmitStructuredLog(&runtime, "simnow_probe", "info", "instrument_meta_cache_loaded",
                              {{"product_id", product_id},
                               {"generation", std::to_string(manifest.generation)},
                               {"contract_count", std::to_string(cached.instruments.size())},
                               {"eligible_count", std::to_string(eligible.size())}});
        }
    } else if (!query_single_instrument_meta(instrument, "instrument_meta_query")) {
        return 7;
    }
    if (dominant_contract_mode) {
        DominantContractCoordinatorConfig coordinator_config;
        coordinator_config.min_lead_ratio = file_config.dominant_contract_min_lead_ratio;
        coordinator_config.min_lead_windows = file_config.dominant_contract_min_lead_windows;
        coordinator_config.min_hold_ms = file_config.dominant_contract_min_hold_ms;
        coordinator_config.max_tick_age_ms = file_config.dominant_contract_max_tick_age_ms;
        coordinator_config.min_warmup_bars = file_config.dominant_contract_warmup_bars;
        coordinator_config.require_complete_baseline =
            file_config.dominant_contract_require_complete_baseline;
        DominantContractCoordinator coordinator(coordinator_config);
        TradingSessionCalendar session_calendar;
        std::size_t selected_product_count = 0;
        std::size_t deferred_product_count = 0;

        std::unordered_map<std::string, std::vector<std::string>> candidate_ids_by_product;
        std::vector<std::string> all_candidate_ids;
        for (const auto& product_id : product_ids) {
            const auto eligible = FilterEligibleFuturesContracts(received_instrument_meta,
                                                                 product_id, broker_trading_day);
            std::vector<std::string> candidate_ids;
            for (const auto& snapshot : eligible) {
                candidate_ids.push_back(snapshot.instrument_id);
            }
            if (candidate_ids.empty()) {
                EmitStructuredLog(&runtime, "simnow_probe", "error",
                                  "dominant_contract_candidate_empty",
                                  {{"product_id", product_id}});
                return 8;
            }
            std::string register_error;
            if (!coordinator.RegisterProduct(product_id, broker_trading_day, "", candidate_ids,
                                             NowEpochNanos(), &register_error)) {
                EmitStructuredLog(&runtime, "simnow_probe", "error",
                                  "dominant_contract_register_failed",
                                  {{"product_id", product_id}, {"error", register_error}});
                return 8;
            }
            DominantContractBrokerState broker;
            broker.truth_complete = true;
            coordinator.UpdateBrokerState(product_id, broker);
            all_candidate_ids.insert(all_candidate_ids.end(), candidate_ids.begin(),
                                     candidate_ids.end());
            candidate_ids_by_product.emplace(product_id, std::move(candidate_ids));
        }
        std::sort(all_candidate_ids.begin(), all_candidate_ids.end());
        all_candidate_ids.erase(std::unique(all_candidate_ids.begin(), all_candidate_ids.end()),
                                all_candidate_ids.end());

        for (const auto& candidate_id : all_candidate_ids) {
            {
                std::lock_guard<std::mutex> lock(depth_market_mutex);
                depth_market_ready = false;
                depth_market_snapshots.clear();
            }
            const int depth_request_id = trader.EnqueueDepthMarketDataQuery(candidate_id);
            if (depth_request_id < 0) {
                EmitStructuredLog(&runtime, "simnow_probe", "error",
                                  "dominant_contract_depth_query_submit_failed",
                                  {{"instrument_id", candidate_id}});
                return 8;
            }
            std::unique_lock<std::mutex> lock(depth_market_mutex);
            if (!depth_market_cv.wait_for(lock, std::chrono::seconds(instrument_timeout_seconds),
                                          [&]() { return depth_market_ready; })) {
                EmitStructuredLog(&runtime, "simnow_probe", "error",
                                  "dominant_contract_depth_query_timeout",
                                  {{"instrument_id", candidate_id},
                                   {"request_id", std::to_string(depth_request_id)}});
                return 8;
            }
            const auto snapshot_it =
                std::find_if(depth_market_snapshots.begin(), depth_market_snapshots.end(),
                             [&](const auto& row) { return row.instrument_id == candidate_id; });
            if (snapshot_it == depth_market_snapshots.end()) {
                EmitStructuredLog(&runtime, "simnow_probe", "error",
                                  "dominant_contract_depth_query_empty",
                                  {{"instrument_id", candidate_id}});
                return 8;
            }
            const auto product = coordinator.ProductForInstrument(candidate_id);
            if (product.has_value()) {
                coordinator.UpdateBaselineSnapshot(*product, *snapshot_it);
            }
        }

        {
            std::lock_guard<std::mutex> lock(market_snapshot_mutex);
            dominant_candidate_ids.clear();
            dominant_candidate_ids.insert(all_candidate_ids.begin(), all_candidate_ids.end());
            dominant_candidate_snapshots.clear();
        }
        if (!md.Subscribe(all_candidate_ids)) {
            EmitStructuredLog(&runtime, "simnow_probe", "error",
                              "dominant_contract_candidate_subscribe_failed",
                              {{"product_count", std::to_string(product_ids.size())},
                               {"candidate_count", std::to_string(all_candidate_ids.size())}});
            return 8;
        }
        const auto sample_deadline =
            std::chrono::steady_clock::now() +
            std::chrono::milliseconds(file_config.dominant_contract_wait_ms);
        {
            std::unique_lock<std::mutex> lock(market_snapshot_mutex);
            while (dominant_candidate_snapshots.size() < all_candidate_ids.size()) {
                if (market_snapshot_cv.wait_until(lock, sample_deadline) ==
                    std::cv_status::timeout) {
                    break;
                }
            }
            for (const auto& [instrument_id, snapshot] : dominant_candidate_snapshots) {
                (void)instrument_id;
                coordinator.UpdateLiveSnapshot(snapshot);
            }
        }

        for (const auto& product_id : product_ids) {
            const auto& candidates = candidate_ids_by_product.at(product_id);
            MarketSnapshot session_tick;
            bool have_session_tick = false;
            {
                std::lock_guard<std::mutex> lock(market_snapshot_mutex);
                for (const auto& candidate : candidates) {
                    const auto it = dominant_candidate_snapshots.find(candidate);
                    if (it == dominant_candidate_snapshots.end()) {
                        continue;
                    }
                    if (!have_session_tick || it->second.recv_ts_ns > session_tick.recv_ts_ns) {
                        session_tick = it->second;
                        have_session_tick = true;
                    }
                }
            }
            const EpochNanos now_ns = NowEpochNanos();
            const bool fresh =
                have_session_tick && session_tick.recv_ts_ns > 0 &&
                now_ns >= session_tick.recv_ts_ns &&
                now_ns - session_tick.recv_ts_ns <=
                    static_cast<EpochNanos>(file_config.dominant_contract_max_tick_age_ms) *
                        1'000'000;
            const auto session_decision = session_calendar.EvaluateOrderTime(
                session_tick.exchange_id, session_tick.instrument_id, broker_trading_day, now_ns, 0,
                fresh);
            if (preopen_connectivity_only && !session_decision.in_session &&
                session_decision.reason == "outside_session") {
                const auto status = coordinator.GetStatus(product_id);
                EmitStructuredLog(
                    &runtime, "simnow_probe", "info", "dominant_contract_selection_deferred",
                    {{"product_id", product_id},
                     {"reason", session_decision.reason},
                     {"candidate_count", std::to_string(status.eligible_count)},
                     {"baseline_count", std::to_string(status.baseline_count)},
                     {"live_count", std::to_string(dominant_candidate_snapshots.size())}});
                ++deferred_product_count;
                continue;
            }
            const auto decision =
                coordinator.Evaluate(product_id, now_ns, session_decision.in_session && fresh);
            if (decision.action != DominantContractAction::kSelectInitial) {
                const auto status = coordinator.GetStatus(product_id);
                EmitStructuredLog(
                    &runtime, "simnow_probe", "error", "dominant_contract_select_failed",
                    {{"product_id", product_id},
                     {"reason", decision.reason},
                     {"candidate_count", std::to_string(status.eligible_count)},
                     {"baseline_count", std::to_string(status.baseline_count)},
                     {"live_count", std::to_string(dominant_candidate_snapshots.size())}});
                return 8;
            }
            std::string commit_error;
            if (!coordinator.CommitInitialSelection(product_id, decision.candidate_instrument_id,
                                                    now_ns, &commit_error)) {
                EmitStructuredLog(&runtime, "simnow_probe", "error",
                                  "dominant_contract_commit_failed",
                                  {{"product_id", product_id}, {"error", commit_error}});
                return 8;
            }
            const std::filesystem::path status_path =
                std::filesystem::path("runtime/ctp_instruments") /
                (product_id + "_dominant_contract.json");
            std::string persist_error;
            if (!coordinator.PersistStatusAtomically(product_id, status_path.string(),
                                                     &persist_error)) {
                EmitStructuredLog(&runtime, "simnow_probe", "error",
                                  "dominant_contract_save_failed",
                                  {{"product_id", product_id},
                                   {"path", status_path.string()},
                                   {"error", persist_error}});
                return 8;
            }
            const auto status = coordinator.GetStatus(product_id);
            EmitStructuredLog(&runtime, "simnow_probe", "info", "dominant_contract_selected",
                              {{"product_id", product_id},
                               {"instrument_id", status.current_instrument_id},
                               {"selection_metric", status.selection_metric},
                               {"candidate_count", std::to_string(status.eligible_count)},
                               {"baseline_count", std::to_string(status.baseline_count)},
                               {"phase", DominantContractPhaseName(status.phase)},
                               {"generation", std::to_string(status.generation)},
                               {"path", status_path.string()}});
            ++selected_product_count;
        }
        EmitStructuredLog(
            &runtime, "simnow_probe", "info", "dominant_contract_probe_summary",
            {{"mode", preopen_connectivity_only ? "preopen_connectivity_only" : "strict"},
             {"selected_count", std::to_string(selected_product_count)},
             {"deferred_count", std::to_string(deferred_product_count)},
             {"product_count", std::to_string(product_ids.size())}});
    }

    const auto started_at = std::chrono::steady_clock::now();
    while (monitor_seconds < 0 || std::chrono::duration_cast<std::chrono::seconds>(
                                      std::chrono::steady_clock::now() - started_at)
                                          .count() < monitor_seconds) {
        const bool healthy = trader.IsReady() && md.IsReady();
        EmitStructuredLog(&runtime, "simnow_probe", healthy ? "info" : "warn", "health_status",
                          {{"state", healthy ? "healthy" : "unhealthy"}});
        std::this_thread::sleep_for(std::chrono::milliseconds(std::max(100, health_interval_ms)));
    }
    md.Disconnect();
    trader.Disconnect();
    EmitStructuredLog(&runtime, "simnow_probe", "info", "probe_completed");
    return 0;
}
