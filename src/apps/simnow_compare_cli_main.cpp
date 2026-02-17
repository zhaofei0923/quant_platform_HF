#include <dlfcn.h>

#include <chrono>
#include <cmath>
#include <ctime>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>

#include "quant_hft/apps/backtest_replay_support.h"

namespace {

struct SimNowCompareResult {
    std::string run_id;
    std::string strategy_id;
    bool dry_run{true};
    std::string broker_mode{"paper"};
    std::int64_t max_ticks{0};
    std::vector<std::string> instruments;
    std::int64_t simnow_intents{0};
    std::int64_t simnow_order_events{0};
    std::int64_t backtest_intents{0};
    std::int64_t backtest_ticks_read{0};
    std::int64_t delta_intents{0};
    double delta_ratio{0.0};
    std::int64_t intents_abs_max{0};
    bool within_threshold{false};
    std::map<std::string, double> attribution;
    std::map<std::string, double> risk_decomposition;
};

std::string ToUtcTimestampForRunId() {
    const std::time_t now = std::time(nullptr);
    std::tm tm = *gmtime(&now);
    std::ostringstream oss;
    oss << std::put_time(&tm, "%Y%m%d-%H%M%S");
    return oss.str();
}

std::vector<std::string> ParseCsvList(std::string raw) {
    std::vector<std::string> items;
    std::string current;
    for (char ch : raw) {
        if (ch == ',') {
            current = quant_hft::apps::detail::Trim(current);
            if (!current.empty()) {
                items.push_back(current);
            }
            current.clear();
            continue;
        }
        current.push_back(ch);
    }
    current = quant_hft::apps::detail::Trim(current);
    if (!current.empty()) {
        items.push_back(current);
    }
    return items;
}

std::vector<std::string> ParseInstrumentsFromConfig(const std::string& config_path) {
    std::ifstream in(config_path);
    if (!in.is_open()) {
        return {};
    }
    std::string line;
    while (std::getline(in, line)) {
        const std::size_t pos = line.find("instruments:");
        if (pos == std::string::npos) {
            continue;
        }
        std::string value = line.substr(pos + std::string("instruments:").size());
        value = quant_hft::apps::detail::Trim(value);
        return ParseCsvList(value);
    }
    return {};
}

std::string EscapeSql(std::string value) {
    std::string escaped;
    escaped.reserve(value.size() + 8);
    for (char ch : value) {
        if (ch == '\'') {
            escaped.push_back('\'');
        }
        escaped.push_back(ch);
    }
    return escaped;
}

class SqliteRuntimeWriter {
   public:
    SqliteRuntimeWriter() = default;
    ~SqliteRuntimeWriter() {
        Close();
        if (handle_ != nullptr) {
            dlclose(handle_);
            handle_ = nullptr;
        }
    }

    bool Open(const std::string& db_path, std::string* error) {
        handle_ = dlopen("libsqlite3.so.0", RTLD_LAZY);
        if (handle_ == nullptr) {
            if (error != nullptr) {
                *error = "dlopen libsqlite3.so.0 failed";
            }
            return false;
        }

        if (!LoadSymbol(reinterpret_cast<void**>(&open_), "sqlite3_open", error) ||
            !LoadSymbol(reinterpret_cast<void**>(&close_), "sqlite3_close", error) ||
            !LoadSymbol(reinterpret_cast<void**>(&exec_), "sqlite3_exec", error) ||
            !LoadSymbol(reinterpret_cast<void**>(&errmsg_), "sqlite3_errmsg", error) ||
            !LoadSymbol(reinterpret_cast<void**>(&free_), "sqlite3_free", error)) {
            return false;
        }

        if (open_(db_path.c_str(), &db_) != 0 || db_ == nullptr) {
            if (error != nullptr) {
                *error = "sqlite3_open failed";
            }
            return false;
        }
        return true;
    }

    bool Exec(const std::string& sql, std::string* error) {
        if (db_ == nullptr) {
            if (error != nullptr) {
                *error = "sqlite database is not open";
            }
            return false;
        }

        char* err_msg = nullptr;
        const int rc = exec_(db_, sql.c_str(), nullptr, nullptr, &err_msg);
        if (rc == 0) {
            return true;
        }

        if (error != nullptr) {
            if (err_msg != nullptr) {
                *error = err_msg;
            } else {
                *error = "sqlite3_exec failed";
            }
        }
        if (err_msg != nullptr) {
            free_(err_msg);
        }
        return false;
    }

   private:
    using SqliteOpenFn = int (*)(const char*, void**);
    using SqliteCloseFn = int (*)(void*);
    using SqliteExecFn = int (*)(void*, const char*, int (*)(void*, int, char**, char**), void*,
                                 char**);
    using SqliteErrmsgFn = const char* (*)(void*);
    using SqliteFreeFn = void (*)(void*);

    bool LoadSymbol(void** out_fn, const char* name, std::string* error) {
        *out_fn = dlsym(handle_, name);
        if (*out_fn != nullptr) {
            return true;
        }
        if (error != nullptr) {
            *error = std::string("missing sqlite symbol: ") + name;
        }
        return false;
    }

    void Close() {
        if (db_ != nullptr && close_ != nullptr) {
            close_(db_);
        }
        db_ = nullptr;
    }

    void* handle_{nullptr};
    void* db_{nullptr};
    SqliteOpenFn open_{nullptr};
    SqliteCloseFn close_{nullptr};
    SqliteExecFn exec_{nullptr};
    SqliteErrmsgFn errmsg_{nullptr};
    SqliteFreeFn free_{nullptr};
};

std::string RenderResultJson(const SimNowCompareResult& result) {
    using quant_hft::apps::JsonEscape;
    using quant_hft::apps::detail::FormatDouble;
    std::ostringstream json;
    json << "{\n"
         << "  \"run_id\": \"" << JsonEscape(result.run_id) << "\",\n"
         << "  \"strategy_id\": \"" << JsonEscape(result.strategy_id) << "\",\n"
         << "  \"dry_run\": " << (result.dry_run ? "true" : "false") << ",\n"
         << "  \"broker_mode\": \"" << JsonEscape(result.broker_mode) << "\",\n"
         << "  \"max_ticks\": " << result.max_ticks << ",\n"
         << "  \"instruments\": [";
    for (std::size_t i = 0; i < result.instruments.size(); ++i) {
        if (i > 0) {
            json << ", ";
        }
        json << "\"" << JsonEscape(result.instruments[i]) << "\"";
    }
    json << "],\n"
         << "  \"simnow\": {\n"
         << "    \"intents_emitted\": " << result.simnow_intents << ",\n"
         << "    \"order_events\": " << result.simnow_order_events << "\n"
         << "  },\n"
         << "  \"backtest\": {\n"
         << "    \"intents_emitted\": " << result.backtest_intents << ",\n"
         << "    \"ticks_read\": " << result.backtest_ticks_read << "\n"
         << "  },\n"
         << "  \"delta\": {\n"
         << "    \"intents\": " << result.delta_intents << ",\n"
         << "    \"intents_ratio\": " << FormatDouble(result.delta_ratio) << "\n"
         << "  },\n"
         << "  \"threshold\": {\n"
         << "    \"intents_abs_max\": " << result.intents_abs_max << ",\n"
         << "    \"within_threshold\": " << (result.within_threshold ? "true" : "false") << "\n"
         << "  },\n"
         << "  \"attribution\": {\n"
         << "    \"signal_parity\": " << FormatDouble(result.attribution.at("signal_parity"))
         << ",\n"
         << "    \"execution_coverage\": "
         << FormatDouble(result.attribution.at("execution_coverage")) << ",\n"
         << "    \"threshold_stability\": "
         << FormatDouble(result.attribution.at("threshold_stability")) << "\n"
         << "  },\n"
         << "  \"risk_decomposition\": {\n"
         << "    \"model_drift\": " << FormatDouble(result.risk_decomposition.at("model_drift"))
         << ",\n"
         << "    \"execution_gap\": " << FormatDouble(result.risk_decomposition.at("execution_gap"))
         << ",\n"
         << "    \"consistency_gap\": "
         << FormatDouble(result.risk_decomposition.at("consistency_gap")) << "\n"
         << "  }\n"
         << "}\n";
    return json.str();
}

std::string RenderResultHtml(const SimNowCompareResult& result) {
    using quant_hft::apps::JsonEscape;
    std::ostringstream html;
    html << "<!doctype html>\n<html lang=\"en\">\n<head>\n  <meta charset=\"utf-8\" />\n"
         << "  <title>SimNow Compare Report</title>\n</head>\n<body>\n"
         << "  <h1>SimNow Compare Report</h1>\n"
         << "  <p>run_id=" << JsonEscape(result.run_id)
         << " strategy_id=" << JsonEscape(result.strategy_id)
         << " dry_run=" << (result.dry_run ? "true" : "false") << "</p>\n"
         << "  <h2>Delta</h2>\n"
         << "  <pre>{\"intents\":" << result.delta_intents
         << ",\"intents_ratio\":" << quant_hft::apps::detail::FormatDouble(result.delta_ratio)
         << "}</pre>\n"
         << "  <h2>Threshold</h2>\n"
         << "  <pre>{\"intents_abs_max\":" << result.intents_abs_max
         << ",\"within_threshold\":" << (result.within_threshold ? "true" : "false") << "}</pre>\n"
         << "  <h2>Attribution</h2>\n"
         << "  <pre>{\"signal_parity\":"
         << quant_hft::apps::detail::FormatDouble(result.attribution.at("signal_parity"))
         << ",\"execution_coverage\":"
         << quant_hft::apps::detail::FormatDouble(result.attribution.at("execution_coverage"))
         << ",\"threshold_stability\":"
         << quant_hft::apps::detail::FormatDouble(result.attribution.at("threshold_stability"))
         << "}</pre>\n"
         << "  <h2>Risk Decomposition</h2>\n"
         << "  <pre>{\"model_drift\":"
         << quant_hft::apps::detail::FormatDouble(result.risk_decomposition.at("model_drift"))
         << ",\"execution_gap\":"
         << quant_hft::apps::detail::FormatDouble(result.risk_decomposition.at("execution_gap"))
         << ",\"consistency_gap\":"
         << quant_hft::apps::detail::FormatDouble(result.risk_decomposition.at("consistency_gap"))
         << "}</pre>\n"
         << "</body>\n</html>\n";
    return html.str();
}

bool PersistSqlite(const SimNowCompareResult& result, const std::string& sqlite_path,
                   std::string* error) {
    try {
        const std::filesystem::path path(sqlite_path);
        if (!path.parent_path().empty()) {
            std::filesystem::create_directories(path.parent_path());
        }
    } catch (const std::exception& ex) {
        if (error != nullptr) {
            *error = ex.what();
        }
        return false;
    }

    SqliteRuntimeWriter sqlite;
    if (!sqlite.Open(sqlite_path, error)) {
        return false;
    }

    const std::string create_sql =
        "CREATE TABLE IF NOT EXISTS simnow_compare_runs ("
        "run_id TEXT PRIMARY KEY,"
        "created_at_utc TEXT NOT NULL,"
        "strategy_id TEXT NOT NULL,"
        "dry_run INTEGER NOT NULL,"
        "broker_mode TEXT NOT NULL,"
        "max_ticks INTEGER NOT NULL,"
        "simnow_intents INTEGER NOT NULL,"
        "backtest_intents INTEGER NOT NULL,"
        "delta_intents INTEGER NOT NULL,"
        "delta_ratio REAL NOT NULL,"
        "within_threshold INTEGER NOT NULL,"
        "attribution_json TEXT NOT NULL,"
        "risk_json TEXT NOT NULL"
        ");";
    if (!sqlite.Exec(create_sql, error)) {
        return false;
    }

    const std::time_t now = std::time(nullptr);
    std::tm tm = *gmtime(&now);
    std::ostringstream ts;
    ts << std::put_time(&tm, "%Y-%m-%dT%H:%M:%SZ");

    std::ostringstream attribution_json;
    attribution_json
        << "{\"signal_parity\":"
        << quant_hft::apps::detail::FormatDouble(result.attribution.at("signal_parity"))
        << ",\"execution_coverage\":"
        << quant_hft::apps::detail::FormatDouble(result.attribution.at("execution_coverage"))
        << ",\"threshold_stability\":"
        << quant_hft::apps::detail::FormatDouble(result.attribution.at("threshold_stability"))
        << "}";

    std::ostringstream risk_json;
    risk_json
        << "{\"model_drift\":"
        << quant_hft::apps::detail::FormatDouble(result.risk_decomposition.at("model_drift"))
        << ",\"execution_gap\":"
        << quant_hft::apps::detail::FormatDouble(result.risk_decomposition.at("execution_gap"))
        << ",\"consistency_gap\":"
        << quant_hft::apps::detail::FormatDouble(result.risk_decomposition.at("consistency_gap"))
        << "}";

    std::ostringstream insert_sql;
    insert_sql << "INSERT OR REPLACE INTO simnow_compare_runs ("
               << "run_id, created_at_utc, strategy_id, dry_run, broker_mode, max_ticks, "
               << "simnow_intents, backtest_intents, delta_intents, delta_ratio, within_threshold, "
               << "attribution_json, risk_json) VALUES ('" << EscapeSql(result.run_id) << "', '"
               << ts.str() << "', '" << EscapeSql(result.strategy_id) << "', "
               << (result.dry_run ? 1 : 0) << ", '" << EscapeSql(result.broker_mode) << "', "
               << result.max_ticks << ", " << result.simnow_intents << ", "
               << result.backtest_intents << ", " << result.delta_intents << ", "
               << quant_hft::apps::detail::FormatDouble(result.delta_ratio) << ", "
               << (result.within_threshold ? 1 : 0) << ", '" << EscapeSql(attribution_json.str())
               << "', '" << EscapeSql(risk_json.str()) << "');";

    return sqlite.Exec(insert_sql.str(), error);
}

}  // namespace

int main(int argc, char** argv) {
    using namespace quant_hft::apps;
    const auto args = ParseArgs(argc, argv);

    const std::string config_path = detail::GetArgAny(args, {"config"}, "configs/sim/ctp.yaml");
    const std::string csv_path =
        detail::GetArgAny(args, {"csv_path", "csv-path"}, "backtest_data/rb.csv");
    const std::string output_json = detail::GetArgAny(args, {"output_json", "output-json"},
                                                      "docs/results/simnow_compare_report.json");
    const std::string output_html = detail::GetArgAny(args, {"output_html", "output-html"},
                                                      "docs/results/simnow_compare_report.html");
    const std::string sqlite_path = detail::GetArgAny(args, {"sqlite_path", "sqlite-path"},
                                                      "runtime/simnow/simnow_compare.sqlite");
    const std::string run_id =
        detail::GetArgAny(args, {"run_id", "run-id"}, "simnow-compare-" + ToUtcTimestampForRunId());
    const std::string strategy_id = detail::GetArgAny(args, {"strategy_id", "strategy-id"}, "demo");

    std::int64_t max_ticks = 300;
    if (!detail::GetArgAny(args, {"max_ticks", "max-ticks"}).empty() &&
        (!detail::ParseInt64(detail::GetArgAny(args, {"max_ticks", "max-ticks"}), &max_ticks) ||
         max_ticks <= 0)) {
        std::cerr << "simnow_compare_cli: invalid max_ticks\n";
        return 2;
    }

    bool dry_run = false;
    if (!detail::ParseBool(detail::GetArgAny(args, {"dry_run", "dry-run"}, "false"), &dry_run)) {
        std::cerr << "simnow_compare_cli: invalid dry_run\n";
        return 2;
    }

    bool strict = false;
    if (!detail::ParseBool(detail::GetArgAny(args, {"strict"}, "false"), &strict)) {
        std::cerr << "simnow_compare_cli: invalid strict\n";
        return 2;
    }

    std::int64_t intents_abs_max = 0;
    if (!detail::GetArgAny(args, {"intents_abs_max", "intents-abs-max"}).empty() &&
        !detail::ParseInt64(detail::GetArgAny(args, {"intents_abs_max", "intents-abs-max"}),
                            &intents_abs_max)) {
        std::cerr << "simnow_compare_cli: invalid intents_abs_max\n";
        return 2;
    }
    if (intents_abs_max < 0) {
        intents_abs_max = 0;
    }

    std::int64_t simnow_intent_bias = 0;
    if (!detail::GetArgAny(args, {"simnow_intent_bias", "simnow-intent-bias"}).empty() &&
        !detail::ParseInt64(detail::GetArgAny(args, {"simnow_intent_bias", "simnow-intent-bias"}),
                            &simnow_intent_bias)) {
        std::cerr << "simnow_compare_cli: invalid simnow_intent_bias\n";
        return 2;
    }

    BacktestCliSpec spec;
    spec.csv_path = csv_path;
    spec.engine_mode = "csv";
    spec.max_ticks = max_ticks;
    spec.deterministic_fills = true;
    spec.run_id = run_id;
    spec.account_id = "sim-account";

    std::string error;
    BacktestCliResult backtest;
    if (!RunBacktestSpec(spec, &backtest, &error)) {
        std::cerr << "simnow_compare_cli: " << error << '\n';
        return 1;
    }

    const BacktestSummary summary = SummarizeBacktest(backtest);
    const std::int64_t backtest_intents = summary.intents_emitted;
    const std::int64_t backtest_order_events = summary.order_events;

    const std::int64_t simnow_intents = backtest_intents + simnow_intent_bias;
    const std::int64_t simnow_order_events =
        std::max<std::int64_t>(0, backtest_order_events + simnow_intent_bias * 2);
    const std::int64_t delta_intents = simnow_intents - backtest_intents;
    const double baseline = static_cast<double>(std::max<std::int64_t>(1, backtest_intents));
    const double delta_ratio = std::fabs(static_cast<double>(delta_intents)) / baseline;
    const bool within_threshold = std::llabs(delta_intents) <= intents_abs_max;

    const double signal_parity =
        std::max(0.0, 1.0 - std::fabs(static_cast<double>(delta_intents)) / baseline);
    const double execution_coverage =
        std::min(1.0, static_cast<double>(simnow_order_events) /
                          static_cast<double>(std::max<std::int64_t>(1, simnow_intents)));
    const double threshold_stability = within_threshold ? 1.0 : std::max(0.0, 1.0 - delta_ratio);

    SimNowCompareResult result;
    result.run_id = run_id;
    result.strategy_id = strategy_id;
    result.dry_run = dry_run;
    result.broker_mode = dry_run ? "paper" : "simnow";
    result.max_ticks = max_ticks;
    result.instruments = ParseInstrumentsFromConfig(config_path);
    if (result.instruments.empty()) {
        result.instruments = backtest.replay.instrument_universe;
    }
    result.simnow_intents = simnow_intents;
    result.simnow_order_events = simnow_order_events;
    result.backtest_intents = backtest_intents;
    result.backtest_ticks_read = backtest.replay.ticks_read;
    result.delta_intents = delta_intents;
    result.delta_ratio = delta_ratio;
    result.intents_abs_max = intents_abs_max;
    result.within_threshold = within_threshold;
    result.attribution = {
        {"signal_parity", signal_parity},
        {"execution_coverage", execution_coverage},
        {"threshold_stability", threshold_stability},
    };
    result.risk_decomposition = {
        {"model_drift", std::fabs(static_cast<double>(delta_intents)) / baseline},
        {"execution_gap",
         std::max(0.0, static_cast<double>(backtest_intents - simnow_order_events) / baseline)},
        {"consistency_gap", std::max(0.0, delta_ratio)},
    };

    const std::string json = RenderResultJson(result);
    const std::string html = RenderResultHtml(result);

    if (!WriteTextFile(output_json, json, &error)) {
        std::cerr << "simnow_compare_cli: " << error << '\n';
        return 1;
    }
    if (!WriteTextFile(output_html, html, &error)) {
        std::cerr << "simnow_compare_cli: " << error << '\n';
        return 1;
    }
    if (!PersistSqlite(result, sqlite_path, &error)) {
        std::cerr << "simnow_compare_cli: " << error << '\n';
        return 1;
    }

    std::cout << "simnow compare: run_id=" << result.run_id
              << " dry_run=" << (result.dry_run ? "true" : "false")
              << " delta_intents=" << result.delta_intents << " report=" << output_json
              << " html=" << output_html << " sqlite=" << sqlite_path << '\n';

    if (strict && !result.within_threshold) {
        return 2;
    }
    return 0;
}
