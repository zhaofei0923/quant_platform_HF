#include <algorithm>
#include <cctype>
#include <chrono>
#include <condition_variable>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "quant_hft/core/ctp_config_loader.h"
#include "quant_hft/core/ctp_md_adapter.h"
#include "quant_hft/core/ctp_trader_adapter.h"
#include "quant_hft/core/structured_log.h"

namespace {

std::string ToLowerAscii(std::string value) {
    std::transform(value.begin(), value.end(), value.begin(),
                   [](unsigned char ch) { return static_cast<char>(std::tolower(ch)); });
    return value;
}

std::string JsonEscape(const std::string& value) {
    std::string escaped;
    escaped.reserve(value.size());
    for (char ch : value) {
        switch (ch) {
            case '\\':
                escaped += "\\\\";
                break;
            case '"':
                escaped += "\\\"";
                break;
            case '\n':
                escaped += "\\n";
                break;
            case '\r':
                escaped += "\\r";
                break;
            case '\t':
                escaped += "\\t";
                break;
            default:
                escaped.push_back(ch);
                break;
        }
    }
    return escaped;
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

std::string StripExchangePrefix(const std::string& instrument) {
    const auto dot = instrument.find('.');
    if (dot == std::string::npos) {
        return instrument;
    }
    return instrument.substr(dot + 1);
}

bool IsPlainFuturesContractId(const std::string& instrument_id, const std::string& product_id) {
    const std::string symbol = ToLowerAscii(StripExchangePrefix(instrument_id));
    if (symbol.size() <= product_id.size() || symbol.rfind(product_id, 0) != 0) {
        return false;
    }
    for (std::size_t index = product_id.size(); index < symbol.size(); ++index) {
        if (std::isdigit(static_cast<unsigned char>(symbol[index])) == 0) {
            return false;
        }
    }
    return true;
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

bool MatchesProductId(const quant_hft::InstrumentMetaSnapshot& snapshot,
                      const std::string& product_id) {
    return ToLowerAscii(snapshot.product_id) == product_id ||
           ExtractProductId(snapshot.instrument_id) == product_id;
}

bool WriteInstrumentMetaJson(const std::filesystem::path& output_path,
                             const std::vector<quant_hft::InstrumentMetaSnapshot>& snapshots,
                             std::string* error) {
    std::error_code ec;
    std::filesystem::create_directories(output_path.parent_path(), ec);
    if (ec) {
        if (error != nullptr) {
            *error = "failed to create directory: " + ec.message();
        }
        return false;
    }

    std::ofstream out(output_path);
    if (!out.is_open()) {
        if (error != nullptr) {
            *error = "failed to open output file";
        }
        return false;
    }

    out << "[\n";
    for (std::size_t index = 0; index < snapshots.size(); ++index) {
        const auto& snapshot = snapshots[index];
        out << "  {\n";
        out << "    \"instrument_id\": \"" << JsonEscape(snapshot.instrument_id) << "\",\n";
        out << "    \"exchange_id\": \"" << JsonEscape(snapshot.exchange_id) << "\",\n";
        out << "    \"product_id\": \"" << JsonEscape(snapshot.product_id) << "\",\n";
        out << "    \"volume_multiple\": " << snapshot.volume_multiple << ",\n";
        out << "    \"price_tick\": " << snapshot.price_tick << ",\n";
        out << "    \"max_margin_side_algorithm\": "
            << (snapshot.max_margin_side_algorithm ? "true" : "false") << ",\n";
        out << "    \"ts_ns\": " << snapshot.ts_ns << ",\n";
        out << "    \"source\": \"" << JsonEscape(snapshot.source) << "\"\n";
        out << "  }" << (index + 1 == snapshots.size() ? "\n" : ",\n");
    }
    out << "]\n";
    return true;
}

bool WriteDominantContractJson(const std::filesystem::path& output_path,
                               const std::string& product_id,
                               const quant_hft::MarketSnapshot& snapshot,
                               const std::string& selection_metric, std::string* error) {
    std::error_code ec;
    std::filesystem::create_directories(output_path.parent_path(), ec);
    if (ec) {
        if (error != nullptr) {
            *error = "failed to create directory: " + ec.message();
        }
        return false;
    }

    std::ofstream out(output_path);
    if (!out.is_open()) {
        if (error != nullptr) {
            *error = "failed to open output file";
        }
        return false;
    }

    out << "{\n";
    out << "  \"product_id\": \"" << JsonEscape(product_id) << "\",\n";
    out << "  \"instrument_id\": \"" << JsonEscape(snapshot.instrument_id) << "\",\n";
    out << "  \"exchange_id\": \"" << JsonEscape(snapshot.exchange_id) << "\",\n";
    out << "  \"selection_metric\": \"" << JsonEscape(selection_metric) << "\",\n";
    out << "  \"open_interest\": " << snapshot.open_interest << ",\n";
    out << "  \"volume\": " << snapshot.volume << ",\n";
    out << "  \"recv_ts_ns\": " << snapshot.recv_ts_ns << "\n";
    out << "}\n";
    return true;
}

std::filesystem::path InstrumentMetaCachePath(const std::string& product_id) {
    return std::filesystem::path("runtime/ctp_instruments") /
           (ToLowerAscii(product_id) + "_contracts.json");
}

std::string ExtractJsonStringValue(const std::string& line) {
    const auto colon = line.find(':');
    if (colon == std::string::npos) {
        return "";
    }
    const auto open = line.find('"', colon + 1);
    if (open == std::string::npos) {
        return "";
    }
    std::string value;
    bool escaped = false;
    for (std::size_t index = open + 1; index < line.size(); ++index) {
        const char ch = line[index];
        if (escaped) {
            switch (ch) {
                case 'n':
                    value.push_back('\n');
                    break;
                case 'r':
                    value.push_back('\r');
                    break;
                case 't':
                    value.push_back('\t');
                    break;
                default:
                    value.push_back(ch);
                    break;
            }
            escaped = false;
            continue;
        }
        if (ch == '\\') {
            escaped = true;
            continue;
        }
        if (ch == '"') {
            break;
        }
        value.push_back(ch);
    }
    return value;
}

std::string ExtractJsonScalarValue(const std::string& line) {
    const auto colon = line.find(':');
    if (colon == std::string::npos) {
        return "";
    }
    std::string value = line.substr(colon + 1);
    while (!value.empty() && std::isspace(static_cast<unsigned char>(value.front())) != 0) {
        value.erase(value.begin());
    }
    while (!value.empty() &&
           (std::isspace(static_cast<unsigned char>(value.back())) != 0 || value.back() == ',')) {
        value.pop_back();
    }
    return value;
}

bool LoadInstrumentMetaCache(const std::string& product_id,
                             std::vector<quant_hft::InstrumentMetaSnapshot>* snapshots,
                             std::string* error) {
    if (snapshots == nullptr) {
        if (error != nullptr) {
            *error = "output snapshot pointer is null";
        }
        return false;
    }
    snapshots->clear();

    const std::filesystem::path path = InstrumentMetaCachePath(product_id);
    std::ifstream in(path);
    if (!in.is_open()) {
        if (error != nullptr) {
            *error = "cache file not found: " + path.string();
        }
        return false;
    }

    quant_hft::InstrumentMetaSnapshot current;
    std::string line;
    while (std::getline(in, line)) {
        if (line.find("\"instrument_id\"") != std::string::npos) {
            current.instrument_id = ExtractJsonStringValue(line);
        } else if (line.find("\"exchange_id\"") != std::string::npos) {
            current.exchange_id = ExtractJsonStringValue(line);
        } else if (line.find("\"product_id\"") != std::string::npos) {
            current.product_id = ToLowerAscii(ExtractJsonStringValue(line));
        } else if (line.find("\"volume_multiple\"") != std::string::npos) {
            try {
                current.volume_multiple = std::stoi(ExtractJsonScalarValue(line));
            } catch (...) {
                current.volume_multiple = 0;
            }
        } else if (line.find("\"price_tick\"") != std::string::npos) {
            try {
                current.price_tick = std::stod(ExtractJsonScalarValue(line));
            } catch (...) {
                current.price_tick = 0.0;
            }
        } else if (line.find("\"max_margin_side_algorithm\"") != std::string::npos) {
            current.max_margin_side_algorithm = ExtractJsonScalarValue(line) == "true";
        } else if (line.find("\"source\"") != std::string::npos) {
            current.source = ExtractJsonStringValue(line);
        }

        if (line.find('}') == std::string::npos || current.instrument_id.empty()) {
            continue;
        }
        if (current.product_id.empty()) {
            current.product_id = ToLowerAscii(product_id);
        }
        if (MatchesProductId(current, ToLowerAscii(product_id)) &&
            IsPlainFuturesContractId(current.instrument_id, ToLowerAscii(product_id))) {
            snapshots->push_back(current);
        }
        current = quant_hft::InstrumentMetaSnapshot{};
    }

    std::sort(snapshots->begin(), snapshots->end(), [](const auto& lhs, const auto& rhs) {
        return lhs.instrument_id < rhs.instrument_id;
    });
    snapshots->erase(std::unique(snapshots->begin(), snapshots->end(),
                                 [](const auto& lhs, const auto& rhs) {
                                     return lhs.instrument_id == rhs.instrument_id;
                                 }),
                     snapshots->end());
    if (snapshots->empty()) {
        if (error != nullptr) {
            *error = "cache has no plain futures contracts for product: " + product_id;
        }
        return false;
    }
    return true;
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

    CTPTraderAdapter trader(10, 1);
    CTPMdAdapter md(10, 1);
    std::mutex instrument_meta_mutex;
    std::condition_variable instrument_meta_cv;
    bool instrument_meta_ready = false;
    std::vector<InstrumentMetaSnapshot> instrument_meta_snapshots;
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
                    dominant_candidate_snapshots[snapshot.instrument_id] = snapshot;
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
    for (int attempt = 0; attempt < 30; ++attempt) {
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
    bool full_instrument_query_used = false;
    if (dominant_contract_mode) {
        bool loaded_all_candidates_from_cache = true;
        for (const auto& product_id : product_ids) {
            std::vector<InstrumentMetaSnapshot> cached_meta;
            std::string cache_error;
            if (!LoadInstrumentMetaCache(product_id, &cached_meta, &cache_error)) {
                loaded_all_candidates_from_cache = false;
                EmitStructuredLog(&runtime, "simnow_probe", "warn",
                                  "instrument_meta_cache_unavailable",
                                  {{"product_id", product_id}, {"error", cache_error}});
                break;
            }
            EmitStructuredLog(&runtime, "simnow_probe", "info", "instrument_meta_cache_loaded",
                              {{"product_id", product_id},
                               {"path", InstrumentMetaCachePath(product_id).string()},
                               {"contract_count", std::to_string(cached_meta.size())}});
            received_instrument_meta.insert(received_instrument_meta.end(), cached_meta.begin(),
                                            cached_meta.end());
        }

        if (!loaded_all_candidates_from_cache) {
            const int instrument_query_request_id = trader.EnqueueInstrumentQuery();
            if (instrument_query_request_id < 0) {
                EmitStructuredLog(&runtime, "simnow_probe", "error",
                                  "instrument_query_submit_failed", {{"reason", "enqueue_failed"}});
                return 7;
            }

            {
                std::unique_lock<std::mutex> lock(instrument_meta_mutex);
                if (!instrument_meta_cv.wait_for(lock,
                                                 std::chrono::seconds(instrument_timeout_seconds),
                                                 [&]() { return instrument_meta_ready; })) {
                    EmitStructuredLog(
                        &runtime, "simnow_probe", "error", "instrument_query_timeout",
                        {{"request_id", std::to_string(instrument_query_request_id)},
                         {"timeout_seconds", std::to_string(instrument_timeout_seconds)}});
                    return 7;
                }
                received_instrument_meta = instrument_meta_snapshots;
            }
            full_instrument_query_used = true;
        }
    } else if (!query_single_instrument_meta(instrument, "instrument_meta_query")) {
        return 7;
    }

    if (full_instrument_query_used) {
        for (const auto& product_id : product_ids) {
            std::vector<InstrumentMetaSnapshot> filtered;
            filtered.reserve(received_instrument_meta.size());
            for (const auto& snapshot : received_instrument_meta) {
                if (MatchesProductId(snapshot, product_id)) {
                    filtered.push_back(snapshot);
                }
            }
            if (filtered.empty()) {
                EmitStructuredLog(&runtime, "simnow_probe", "error", "instrument_meta_filter_empty",
                                  {{"product_id", product_id}});
                return 7;
            }

            const std::filesystem::path output_path = InstrumentMetaCachePath(product_id);
            std::string write_error;
            if (!WriteInstrumentMetaJson(output_path, filtered, &write_error)) {
                EmitStructuredLog(&runtime, "simnow_probe", "error", "instrument_meta_save_failed",
                                  {{"product_id", product_id},
                                   {"path", output_path.string()},
                                   {"error", write_error}});
                return 7;
            }

            EmitStructuredLog(&runtime, "simnow_probe", "info", "instrument_meta_saved",
                              {{"product_id", product_id},
                               {"path", output_path.string()},
                               {"contract_count", std::to_string(filtered.size())}});
        }
    }

    if (dominant_contract_mode) {
        std::unordered_map<std::string, std::vector<std::string>> candidate_ids_by_product;
        std::vector<std::string> all_candidate_ids;
        for (const auto& product_id : product_ids) {
            std::vector<std::string> candidate_ids;
            candidate_ids.reserve(received_instrument_meta.size());
            for (const auto& snapshot : received_instrument_meta) {
                if (!MatchesProductId(snapshot, product_id) ||
                    !IsPlainFuturesContractId(snapshot.instrument_id, product_id)) {
                    continue;
                }
                candidate_ids.push_back(snapshot.instrument_id);
            }
            std::sort(candidate_ids.begin(), candidate_ids.end());
            candidate_ids.erase(std::unique(candidate_ids.begin(), candidate_ids.end()),
                                candidate_ids.end());

            if (candidate_ids.empty()) {
                EmitStructuredLog(&runtime, "simnow_probe", "error",
                                  "dominant_contract_candidate_empty",
                                  {{"product_id", product_id}});
                return 8;
            }
            all_candidate_ids.insert(all_candidate_ids.end(), candidate_ids.begin(),
                                     candidate_ids.end());
            candidate_ids_by_product.emplace(product_id, std::move(candidate_ids));
        }
        std::sort(all_candidate_ids.begin(), all_candidate_ids.end());
        all_candidate_ids.erase(std::unique(all_candidate_ids.begin(), all_candidate_ids.end()),
                                all_candidate_ids.end());

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
        }

        auto better_by_open_interest = [](const MarketSnapshot& lhs, const MarketSnapshot& rhs) {
            if (lhs.open_interest != rhs.open_interest) {
                return lhs.open_interest < rhs.open_interest;
            }
            if (lhs.volume != rhs.volume) {
                return lhs.volume < rhs.volume;
            }
            return lhs.instrument_id > rhs.instrument_id;
        };
        auto better_by_volume = [](const MarketSnapshot& lhs, const MarketSnapshot& rhs) {
            if (lhs.volume != rhs.volume) {
                return lhs.volume < rhs.volume;
            }
            if (lhs.open_interest != rhs.open_interest) {
                return lhs.open_interest < rhs.open_interest;
            }
            return lhs.instrument_id > rhs.instrument_id;
        };

        std::vector<std::string> selected_instruments;
        selected_instruments.reserve(product_ids.size());
        std::vector<std::string> unsubscribe_ids;
        unsubscribe_ids.reserve(all_candidate_ids.size());
        for (const auto& product_id : product_ids) {
            const auto& candidate_ids = candidate_ids_by_product.at(product_id);
            std::vector<MarketSnapshot> observed_snapshots;
            observed_snapshots.reserve(candidate_ids.size());
            {
                std::lock_guard<std::mutex> lock(market_snapshot_mutex);
                for (const auto& candidate_id : candidate_ids) {
                    const auto snapshot_it = dominant_candidate_snapshots.find(candidate_id);
                    if (snapshot_it != dominant_candidate_snapshots.end()) {
                        observed_snapshots.push_back(snapshot_it->second);
                    }
                }
            }

            if (observed_snapshots.empty()) {
                EmitStructuredLog(
                    &runtime, "simnow_probe", "error", "dominant_contract_sample_timeout",
                    {{"product_id", product_id},
                     {"wait_ms", std::to_string(file_config.dominant_contract_wait_ms)}});
                return 8;
            }

            const bool has_positive_open_interest = std::any_of(
                observed_snapshots.begin(), observed_snapshots.end(),
                [](const MarketSnapshot& snapshot) { return snapshot.open_interest > 0; });
            const std::string selection_metric =
                has_positive_open_interest ? "open_interest" : "volume";
            const auto best_it = std::max_element(
                observed_snapshots.begin(), observed_snapshots.end(),
                has_positive_open_interest ? better_by_open_interest : better_by_volume);
            if (best_it == observed_snapshots.end()) {
                EmitStructuredLog(&runtime, "simnow_probe", "error",
                                  "dominant_contract_select_failed", {{"product_id", product_id}});
                return 8;
            }

            selected_instruments.push_back(best_it->instrument_id);
            for (const auto& candidate_id : candidate_ids) {
                if (candidate_id != best_it->instrument_id) {
                    unsubscribe_ids.push_back(candidate_id);
                }
            }

            const std::filesystem::path dominant_output_path =
                std::filesystem::path("runtime/ctp_instruments") /
                (product_id + "_dominant_contract.json");
            std::string write_error;
            if (!WriteDominantContractJson(dominant_output_path, product_id, *best_it,
                                           selection_metric, &write_error)) {
                EmitStructuredLog(&runtime, "simnow_probe", "error",
                                  "dominant_contract_save_failed",
                                  {{"product_id", product_id},
                                   {"path", dominant_output_path.string()},
                                   {"error", write_error}});
                return 8;
            }

            EmitStructuredLog(&runtime, "simnow_probe", "info", "dominant_contract_selected",
                              {{"product_id", product_id},
                               {"instrument_id", best_it->instrument_id},
                               {"exchange_id", best_it->exchange_id},
                               {"selection_metric", selection_metric},
                               {"open_interest", std::to_string(best_it->open_interest)},
                               {"volume", std::to_string(best_it->volume)},
                               {"candidate_count", std::to_string(candidate_ids.size())},
                               {"observed_count", std::to_string(observed_snapshots.size())},
                               {"path", dominant_output_path.string()}});
        }
        std::sort(unsubscribe_ids.begin(), unsubscribe_ids.end());
        unsubscribe_ids.erase(std::unique(unsubscribe_ids.begin(), unsubscribe_ids.end()),
                              unsubscribe_ids.end());
        if (!unsubscribe_ids.empty() && !md.Unsubscribe(unsubscribe_ids)) {
            EmitStructuredLog(&runtime, "simnow_probe", "warn",
                              "dominant_contract_unsubscribe_partial_failed",
                              {{"product_count", std::to_string(product_ids.size())},
                               {"unsubscribe_count", std::to_string(unsubscribe_ids.size())}});
        }

        for (const auto& selected_instrument : selected_instruments) {
            if (!query_single_instrument_meta(selected_instrument,
                                              "selected_instrument_meta_query")) {
                return 8;
            }
        }

        {
            std::lock_guard<std::mutex> lock(market_snapshot_mutex);
            dominant_candidate_ids.clear();
            dominant_candidate_ids.insert(selected_instruments.begin(), selected_instruments.end());
        }
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
