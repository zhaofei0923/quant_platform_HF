#include <algorithm>
#include <cctype>
#include <chrono>
#include <condition_variable>
#include <cstdlib>
#include <filesystem>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "quant_hft/apps/cli_support.h"
#include "quant_hft/core/ctp_config_loader.h"
#include "quant_hft/core/ctp_gateway_adapter.h"
#include "quant_hft/core/ctp_trader_adapter.h"
#include "quant_hft/core/instrument_meta_cache.h"
#include "quant_hft/core/structured_log.h"

namespace {

using quant_hft::EpochNanos;
using quant_hft::InstrumentMetaCacheDocument;
using quant_hft::InstrumentMetaSnapshot;
using quant_hft::InstrumentQueryFilter;
using quant_hft::InstrumentUniverseManifest;

std::string ToLowerAscii(std::string value) {
    std::transform(value.begin(), value.end(), value.begin(),
                   [](unsigned char ch) { return static_cast<char>(std::tolower(ch)); });
    return value;
}

std::string ExpectedExchangeId(const std::string& product_id) {
    const std::string normalized = ToLowerAscii(product_id);
    if (normalized == "c") {
        return "DCE";
    }
    if (normalized == "hc") {
        return "SHFE";
    }
    return {};
}

std::filesystem::path ManifestPath(const std::filesystem::path& cache_root) {
    return cache_root / "contract_universe_manifest.json";
}

bool LoadCurrentUniverse(const std::filesystem::path& cache_root, const std::string& trading_day,
                         const std::vector<std::string>& product_ids, EpochNanos now_ns,
                         std::int64_t max_age_ms, std::size_t* instrument_count,
                         std::string* error) {
    InstrumentUniverseManifest manifest;
    if (!quant_hft::LoadInstrumentUniverseManifest(ManifestPath(cache_root).string(), &manifest,
                                                   error) ||
        !quant_hft::IsInstrumentUniverseManifestCurrent(manifest, trading_day, product_ids, now_ns,
                                                        max_age_ms, error)) {
        return false;
    }
    std::size_t total = 0;
    for (const auto& product_id : product_ids) {
        InstrumentMetaCacheDocument document;
        const auto cache_path = std::filesystem::path(manifest.cache_dir) /
                                (ToLowerAscii(product_id) + "_contracts.json");
        std::string cache_error;
        if (!quant_hft::LoadInstrumentMetaCacheDocument(cache_path.string(), &document,
                                                        &cache_error) ||
            document.generated_ts_ns != manifest.generated_ts_ns ||
            !quant_hft::IsInstrumentMetaCacheCurrent(document, trading_day, now_ns, max_age_ms,
                                                     &cache_error)) {
            if (error != nullptr) {
                *error = "product_cache_invalid:" + ToLowerAscii(product_id) + ":" + cache_error;
            }
            return false;
        }
        total += document.instruments.size();
    }
    if (instrument_count != nullptr) {
        *instrument_count = total;
    }
    return true;
}

}  // namespace

int main(int argc, char** argv) {
    using namespace quant_hft;
    CtpRuntimeConfig bootstrap_runtime;

#if !(defined(QUANT_HFT_ENABLE_CTP_REAL_API) && QUANT_HFT_ENABLE_CTP_REAL_API)
    EmitStructuredLog(&bootstrap_runtime, "simnow_contract_universe_refresh", "error",
                      "ctp_real_api_disabled");
    return 2;
#endif

    const std::string quant_root = GetEnvOrDefault("QUANT_ROOT", "");
    std::string config_path = quant_root.empty()
                                  ? "configs/sim/ctp_sim_trade_candidates.yaml"
                                  : quant_root + "/configs/sim/ctp_sim_trade_candidates.yaml";
    const std::filesystem::path default_cache_root =
        quant_root.empty() ? std::filesystem::path("runtime/ctp_instruments")
                           : std::filesystem::path(quant_root) / "runtime/ctp_instruments";
    std::filesystem::path cache_root =
        GetEnvOrDefault("SIMNOW_INSTRUMENT_CACHE_ROOT", default_cache_root.string());
    int timeout_seconds = 60;
    bool force = false;
    for (int index = 1; index < argc; ++index) {
        const std::string arg = argv[index];
        if (arg == "--config" && index + 1 < argc) {
            config_path = argv[++index];
        } else if (arg == "--cache-root" && index + 1 < argc) {
            cache_root = argv[++index];
        } else if (arg == "--timeout-seconds" && index + 1 < argc) {
            timeout_seconds = std::stoi(argv[++index]);
        } else if (arg == "--force") {
            force = true;
        } else {
            EmitStructuredLog(&bootstrap_runtime, "simnow_contract_universe_refresh", "error",
                              "invalid_argument", {{"arg", arg}});
            return 2;
        }
    }
    if (timeout_seconds <= 0) {
        EmitStructuredLog(&bootstrap_runtime, "simnow_contract_universe_refresh", "error",
                          "invalid_timeout");
        return 2;
    }

    CtpFileConfig file_config;
    std::string error;
    if (!CtpConfigLoader::LoadFromYaml(config_path, &file_config, &error)) {
        EmitStructuredLog(&bootstrap_runtime, "simnow_contract_universe_refresh", "error",
                          "config_load_failed", {{"error", error}});
        return 3;
    }
    std::vector<std::string> product_ids = file_config.product_ids;
    for (auto& product_id : product_ids) {
        product_id = ToLowerAscii(product_id);
    }
    std::sort(product_ids.begin(), product_ids.end());
    product_ids.erase(std::unique(product_ids.begin(), product_ids.end()), product_ids.end());
    if (product_ids.empty()) {
        EmitStructuredLog(&file_config.runtime, "simnow_contract_universe_refresh", "error",
                          "product_ids_missing");
        return 3;
    }
    for (const auto& product_id : product_ids) {
        if (ExpectedExchangeId(product_id).empty()) {
            EmitStructuredLog(&file_config.runtime, "simnow_contract_universe_refresh", "error",
                              "unsupported_product", {{"product_id", product_id}});
            return 3;
        }
    }

    auto runtime = file_config.runtime;
    runtime.enable_real_api = true;
    MarketDataConnectConfig connect;
    connect.market_front_address = runtime.md_front;
    connect.trader_front_address = runtime.td_front;
    connect.flow_path = runtime.flow_path;
    connect.broker_id = runtime.broker_id;
    connect.user_id = runtime.user_id;
    connect.investor_id = runtime.investor_id;
    connect.app_id = runtime.app_id;
    connect.auth_code = runtime.auth_code;
    connect.is_production_mode = runtime.is_production_mode;
    connect.enable_real_api = true;
    connect.enable_terminal_auth = runtime.enable_terminal_auth;
    connect.connect_timeout_ms = runtime.connect_timeout_ms;
    connect.reconnect_max_attempts = runtime.reconnect_max_attempts;
    connect.reconnect_initial_backoff_ms = runtime.reconnect_initial_backoff_ms;
    connect.reconnect_max_backoff_ms = runtime.reconnect_max_backoff_ms;
    connect.password = runtime.password;
    connect.recovery_quiet_period_ms = runtime.recovery_quiet_period_ms;
    connect.settlement_confirm_required = runtime.settlement_confirm_required;

    auto gateway = std::make_shared<CtpGatewayAdapter>(10);
    CTPTraderAdapter trader(gateway, 1);
    std::mutex meta_mutex;
    std::condition_variable meta_cv;
    bool meta_ready = false;
    std::vector<InstrumentMetaSnapshot> meta_snapshots;
    trader.RegisterInstrumentMetaSnapshotCallback(
        [&](const std::vector<InstrumentMetaSnapshot>& snapshots) {
            {
                std::lock_guard<std::mutex> lock(meta_mutex);
                meta_snapshots = snapshots;
                meta_ready = true;
            }
            meta_cv.notify_all();
        });

    EmitStructuredLog(&runtime, "simnow_contract_universe_refresh", "info", "refresh_started",
                      {{"product_count", std::to_string(product_ids.size())}});
    if (!trader.Connect(connect)) {
        EmitStructuredLog(&runtime, "simnow_contract_universe_refresh", "error",
                          "trader_connect_failed");
        return 4;
    }
    if (!trader.ConfirmSettlement()) {
        EmitStructuredLog(&runtime, "simnow_contract_universe_refresh", "error",
                          "settlement_confirm_failed");
        trader.Disconnect();
        return 4;
    }

    const int account_request_id = trader.EnqueueTradingAccountQuery();
    TradingAccountSnapshot account;
    const auto account_deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(timeout_seconds);
    while (std::chrono::steady_clock::now() < account_deadline) {
        account = trader.GetLastTradingAccountSnapshot();
        if (account.ts_ns > 0 && account.trading_day.size() == 8U) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    if (account_request_id < 0 || account.trading_day.size() != 8U) {
        EmitStructuredLog(&runtime, "simnow_contract_universe_refresh", "error",
                          "broker_trading_day_unavailable");
        trader.Disconnect();
        return 5;
    }
    const std::string trading_day = account.trading_day;
    const EpochNanos now_ns = NowEpochNanos();
    if (!force) {
        std::size_t cached_count = 0;
        std::string cache_error;
        if (LoadCurrentUniverse(cache_root, trading_day, product_ids, now_ns,
                                file_config.dominant_contract_cache_max_age_ms, &cached_count,
                                &cache_error)) {
            EmitStructuredLog(
                &runtime, "simnow_contract_universe_refresh", "info",
                "instrument_universe_cache_current",
                {{"trading_day", trading_day}, {"instrument_count", std::to_string(cached_count)}});
            trader.Disconnect();
            return 0;
        }
        EmitStructuredLog(&runtime, "simnow_contract_universe_refresh", "info",
                          "instrument_universe_refresh_required", {{"reason", cache_error}});
    }

    std::unordered_map<std::string, std::vector<InstrumentMetaSnapshot>> metadata_by_product;
    for (const auto& product_id : product_ids) {
        {
            std::lock_guard<std::mutex> lock(meta_mutex);
            meta_ready = false;
            meta_snapshots.clear();
        }
        InstrumentQueryFilter filter;
        filter.exchange_id = ExpectedExchangeId(product_id);
        filter.product_id = product_id;
        const int request_id = trader.EnqueueInstrumentQuery(filter);
        if (request_id < 0) {
            EmitStructuredLog(&runtime, "simnow_contract_universe_refresh", "error",
                              "product_query_submit_failed", {{"product_id", product_id}});
            trader.Disconnect();
            return 6;
        }
        std::vector<InstrumentMetaSnapshot> received;
        {
            std::unique_lock<std::mutex> lock(meta_mutex);
            if (!meta_cv.wait_for(lock, std::chrono::seconds(timeout_seconds),
                                  [&]() { return meta_ready; })) {
                EmitStructuredLog(
                    &runtime, "simnow_contract_universe_refresh", "error", "product_query_timeout",
                    {{"product_id", product_id}, {"request_id", std::to_string(request_id)}});
                trader.Disconnect();
                return 6;
            }
            received = meta_snapshots;
        }
        auto eligible = FilterEligibleFuturesContracts(received, product_id, trading_day);
        if (eligible.empty()) {
            EmitStructuredLog(&runtime, "simnow_contract_universe_refresh", "error",
                              "product_query_empty", {{"product_id", product_id}});
            trader.Disconnect();
            return 6;
        }
        if (std::any_of(eligible.begin(), eligible.end(),
                        [&](const auto& row) { return row.exchange_id != filter.exchange_id; })) {
            EmitStructuredLog(&runtime, "simnow_contract_universe_refresh", "error",
                              "product_query_exchange_mismatch", {{"product_id", product_id}});
            trader.Disconnect();
            return 6;
        }
        metadata_by_product.emplace(product_id, std::move(eligible));
        EmitStructuredLog(
            &runtime, "simnow_contract_universe_refresh", "info", "product_query_completed",
            {{"product_id", product_id},
             {"contract_count", std::to_string(metadata_by_product.at(product_id).size())}});
    }

    const EpochNanos generated_ts_ns = NowEpochNanos();
    const std::uint64_t generation = static_cast<std::uint64_t>(generated_ts_ns);
    const std::filesystem::path generation_dir =
        cache_root / "universe" / (trading_day + "." + std::to_string(generation));
    for (const auto& product_id : product_ids) {
        const auto cache_path = generation_dir / (product_id + "_contracts.json");
        if (!WriteInstrumentMetaCacheV2Atomically(cache_path.string(), product_id, trading_day,
                                                  metadata_by_product.at(product_id),
                                                  generated_ts_ns, &error)) {
            EmitStructuredLog(&runtime, "simnow_contract_universe_refresh", "error",
                              "product_cache_write_failed",
                              {{"product_id", product_id}, {"error", error}});
            trader.Disconnect();
            return 7;
        }
    }
    InstrumentUniverseManifest manifest;
    manifest.schema_version = 1;
    manifest.broker_trading_day = trading_day;
    manifest.generated_ts_ns = generated_ts_ns;
    manifest.generation = generation;
    manifest.complete = true;
    manifest.cache_dir = std::filesystem::absolute(generation_dir).lexically_normal().string();
    manifest.product_ids = product_ids;
    if (!WriteInstrumentUniverseManifestAtomically(ManifestPath(cache_root).string(), manifest,
                                                   &error)) {
        EmitStructuredLog(&runtime, "simnow_contract_universe_refresh", "error",
                          "manifest_write_failed", {{"error", error}});
        trader.Disconnect();
        return 7;
    }

    std::size_t total = 0;
    for (const auto& product_id : product_ids) {
        total += metadata_by_product.at(product_id).size();
        std::string compatibility_error;
        const auto compatibility_path = cache_root / (product_id + "_contracts.json");
        if (!WriteInstrumentMetaCacheV2Atomically(compatibility_path.string(), product_id,
                                                  trading_day, metadata_by_product.at(product_id),
                                                  generated_ts_ns, &compatibility_error)) {
            EmitStructuredLog(&runtime, "simnow_contract_universe_refresh", "warn",
                              "compatibility_cache_write_failed",
                              {{"product_id", product_id}, {"error", compatibility_error}});
        }
    }
    EmitStructuredLog(&runtime, "simnow_contract_universe_refresh", "info",
                      "instrument_universe_refresh_completed",
                      {{"trading_day", trading_day},
                       {"generation", std::to_string(generation)},
                       {"instrument_count", std::to_string(total)},
                       {"manifest", ManifestPath(cache_root).string()}});
    trader.Disconnect();
    return 0;
}
