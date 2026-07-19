#include <openssl/evp.h>

#include <algorithm>
#include <cctype>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <sstream>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "quant_hft/apps/cli_support.h"
#include "quant_hft/contracts/types.h"
#include "quant_hft/services/market_bar_pipeline.h"
#include "quant_hft/services/trading_session_calendar.h"
#include "quant_hft/strategy/composite_config_loader.h"
#include "quant_hft/strategy/composite_strategy.h"

namespace {

namespace fs = std::filesystem;

constexpr quant_hft::EpochNanos kTenMinutesNs = 600'000'000'000LL;

struct DayProductInfo {
    std::string trading_day;
    std::string product;
    std::string instrument_id;
    std::string exchange_id;
};

struct ValidationStats {
    std::int64_t trading_days{0};
    std::int64_t product_days{0};
    std::int64_t input_tick_rows{0};
    std::int64_t parse_errors{0};
    std::int64_t duplicate_ticks{0};
    std::int64_t late_ticks{0};
    std::int64_t endpoint_1m_bars{0};
    std::int64_t endpoint_5m_bars{0};
    std::int64_t incomplete_5m_bars{0};
    std::int64_t ineligible_integrity_violations{0};
    std::int64_t critical_conflicts{0};
    std::int64_t duplicate_1m_keys{0};
    std::int64_t duplicate_5m_keys{0};
    std::int64_t expected_1m_slots{0};
    std::int64_t canonical_1m_slots{0};
    std::int64_t no_tick_1m_gaps{0};
    std::int64_t expected_5m_keys{0};
    std::int64_t canonical_5m_keys{0};
    std::int64_t no_tick_5m_gaps{0};
    std::int64_t strategy_evaluations{0};
    std::int64_t signal_candidates{0};
    std::int64_t evidence_files{0};
    std::uintmax_t evidence_bytes{0};
    std::int64_t hc_1125_old_rows{0};
    std::int64_t hc_1125_old_sell_candidates{0};
    std::int64_t hc_1125_corrected_bars{0};
    bool hc_1125_complete{false};
    double hc_1125_close{0.0};
    std::string hc_1125_corrected_raw_signal;
};

struct ProductRuntime {
    explicit ProductRuntime(const std::string& product_id)
        : product(product_id), pipeline(Config()) {}

    static quant_hft::MarketBarPipelineConfig Config() {
        quant_hft::MarketBarPipelineConfig config;
        config.bar_aggregator.is_backtest_mode = true;
        config.bar_aggregator.allowed_lateness_ms = 3500;
        config.timeframes = {5};
        return config;
    }

    std::string product;
    quant_hft::MarketBarPipeline pipeline;
    std::unique_ptr<quant_hft::CompositeStrategy> strategy;
};

class OutputFiles {
   public:
    explicit OutputFiles(fs::path root) : root_(std::move(root)) {}

    std::ofstream* Open(const fs::path& relative, const std::string& header, std::string* error) {
        const std::string key = relative.generic_string();
        const auto existing = streams_.find(key);
        if (existing != streams_.end()) {
            return existing->second.get();
        }
        const fs::path path = root_ / relative;
        std::error_code ec;
        fs::create_directories(path.parent_path(), ec);
        if (ec) {
            SetError(error, "failed to create output directory: " + ec.message());
            return nullptr;
        }
        auto stream = std::make_unique<std::ofstream>(path, std::ios::out | std::ios::trunc);
        if (!stream->is_open()) {
            SetError(error, "failed to open output: " + path.string());
            return nullptr;
        }
        *stream << header << '\n';
        auto* raw = stream.get();
        streams_.emplace(key, std::move(stream));
        return raw;
    }

    bool Close(std::string* error) {
        for (auto& [path, stream] : streams_) {
            stream->flush();
            if (!stream->good()) {
                SetError(error, "failed to flush output: " + path);
                return false;
            }
            stream->close();
        }
        streams_.clear();
        return true;
    }

   private:
    static void SetError(std::string* error, const std::string& value) {
        if (error != nullptr) {
            *error = value;
        }
    }

    fs::path root_;
    std::map<std::string, std::unique_ptr<std::ofstream>> streams_;
};

void SetError(std::string* error, const std::string& value) {
    if (error != nullptr) {
        *error = value;
    }
}

std::string JsonEscape(const std::string& value) {
    std::string escaped;
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

std::vector<std::string> SplitCsv(const std::string& line) {
    std::vector<std::string> values;
    std::string value;
    std::istringstream input(line);
    while (std::getline(input, value, ',')) {
        values.push_back(value);
    }
    if (!line.empty() && line.back() == ',') {
        values.emplace_back();
    }
    return values;
}

std::unordered_map<std::string, std::size_t> HeaderMap(const std::string& header) {
    std::unordered_map<std::string, std::size_t> result;
    const auto columns = SplitCsv(header);
    for (std::size_t index = 0; index < columns.size(); ++index) {
        result[columns[index]] = index;
    }
    return result;
}

std::string Field(const std::vector<std::string>& row,
                  const std::unordered_map<std::string, std::size_t>& header,
                  const std::string& name) {
    const auto it = header.find(name);
    return it == header.end() || it->second >= row.size() ? "" : row[it->second];
}

bool ParseInt64(const std::string& text, std::int64_t* out) {
    if (out == nullptr || text.empty()) {
        return false;
    }
    try {
        std::size_t consumed = 0;
        *out = std::stoll(text, &consumed);
        return consumed == text.size();
    } catch (...) {
        return false;
    }
}

bool ParseInt32(const std::string& text, std::int32_t* out) {
    std::int64_t parsed = 0;
    if (!ParseInt64(text, &parsed) || out == nullptr) {
        return false;
    }
    *out = static_cast<std::int32_t>(parsed);
    return true;
}

bool ParseDouble(const std::string& text, double* out) {
    if (out == nullptr || text.empty()) {
        return false;
    }
    try {
        std::size_t consumed = 0;
        *out = std::stod(text, &consumed);
        return consumed == text.size();
    } catch (...) {
        return false;
    }
}

std::optional<quant_hft::MarketSnapshot> ParseTick(
    const std::vector<std::string>& row,
    const std::unordered_map<std::string, std::size_t>& header) {
    quant_hft::MarketSnapshot snapshot;
    snapshot.instrument_id = Field(row, header, "instrument_id");
    snapshot.exchange_id = Field(row, header, "exchange_id");
    snapshot.trading_day = Field(row, header, "trading_day");
    snapshot.action_day = Field(row, header, "action_day");
    snapshot.update_time = Field(row, header, "update_time");
    if (snapshot.instrument_id.empty() || snapshot.exchange_id.empty() ||
        snapshot.trading_day.empty() || snapshot.update_time.empty() ||
        !ParseInt32(Field(row, header, "update_millisec"), &snapshot.update_millisec) ||
        !ParseDouble(Field(row, header, "last_price"), &snapshot.last_price) ||
        !ParseDouble(Field(row, header, "bid_price_1"), &snapshot.bid_price_1) ||
        !ParseDouble(Field(row, header, "ask_price_1"), &snapshot.ask_price_1) ||
        !ParseInt64(Field(row, header, "bid_volume_1"), &snapshot.bid_volume_1) ||
        !ParseInt64(Field(row, header, "ask_volume_1"), &snapshot.ask_volume_1) ||
        !ParseInt64(Field(row, header, "volume"), &snapshot.volume) ||
        !ParseInt64(Field(row, header, "open_interest"), &snapshot.open_interest)) {
        return std::nullopt;
    }
    (void)ParseDouble(Field(row, header, "settlement_price"), &snapshot.settlement_price);
    (void)ParseDouble(Field(row, header, "average_price_raw"), &snapshot.average_price_raw);
    (void)ParseDouble(Field(row, header, "average_price_norm"), &snapshot.average_price_norm);
    std::int64_t bool_value = 0;
    if (ParseInt64(Field(row, header, "is_valid_settlement"), &bool_value)) {
        snapshot.is_valid_settlement = bool_value != 0;
    }
    (void)ParseInt64(Field(row, header, "exchange_ts_ns"), &snapshot.exchange_ts_ns);
    (void)ParseInt64(Field(row, header, "recv_ts_ns"), &snapshot.recv_ts_ns);
    if (ParseInt64(Field(row, header, "average_price_norm_valid"), &bool_value)) {
        snapshot.average_price_norm_valid = bool_value != 0;
    }
    return snapshot;
}

std::string ProductFromPath(const fs::path& path) {
    const auto parts = path.lexically_normal();
    for (auto it = parts.begin(); it != parts.end(); ++it) {
        if (*it == "varieties") {
            ++it;
            return it == parts.end() ? "" : it->string();
        }
    }
    return "";
}

std::string DayFromPath(const fs::path& path) {
    for (const auto& part : path.lexically_normal()) {
        const std::string value = part.string();
        if (value.rfind("trading_day=", 0) == 0) {
            return value.substr(12);
        }
    }
    return "";
}

std::string SlotKey(const std::string& product, const std::string& day, const std::string& minute) {
    return product + "|" + day + "|" + minute;
}

std::string FormatMinute(int minute_of_day) {
    std::ostringstream out;
    out << std::setfill('0') << std::setw(2) << minute_of_day / 60 << ':' << std::setw(2)
        << minute_of_day % 60;
    return out.str();
}

std::string Sha256File(const fs::path& path, std::string* error) {
    std::ifstream input(path, std::ios::in | std::ios::binary);
    if (!input.is_open()) {
        SetError(error, "failed to open evidence file: " + path.string());
        return "";
    }
    EVP_MD_CTX* context = EVP_MD_CTX_new();
    if (context == nullptr || EVP_DigestInit_ex(context, EVP_sha256(), nullptr) != 1) {
        if (context != nullptr) {
            EVP_MD_CTX_free(context);
        }
        SetError(error, "failed to initialize SHA-256");
        return "";
    }
    std::vector<char> buffer(1 << 20);
    while (input.good()) {
        input.read(buffer.data(), static_cast<std::streamsize>(buffer.size()));
        const auto count = input.gcount();
        if (count > 0 &&
            EVP_DigestUpdate(context, buffer.data(), static_cast<std::size_t>(count)) != 1) {
            EVP_MD_CTX_free(context);
            SetError(error, "failed to update SHA-256: " + path.string());
            return "";
        }
    }
    unsigned char digest[EVP_MAX_MD_SIZE];
    unsigned int digest_size = 0;
    if (EVP_DigestFinal_ex(context, digest, &digest_size) != 1) {
        EVP_MD_CTX_free(context);
        SetError(error, "failed to finalize SHA-256: " + path.string());
        return "";
    }
    EVP_MD_CTX_free(context);
    std::ostringstream out;
    out << std::hex << std::setfill('0');
    for (unsigned int index = 0; index < digest_size; ++index) {
        out << std::setw(2) << static_cast<int>(digest[index]);
    }
    return out.str();
}

bool ContainsRelevantDay(const std::string& value, const std::string& start_day,
                         const std::string& end_day) {
    for (std::size_t index = 0; index + 8 <= value.size(); ++index) {
        if (std::all_of(value.begin() + static_cast<std::ptrdiff_t>(index),
                        value.begin() + static_cast<std::ptrdiff_t>(index + 8),
                        [](unsigned char ch) { return std::isdigit(ch) != 0; })) {
            const std::string day = value.substr(index, 8);
            if (day >= start_day && day <= end_day) {
                return true;
            }
        }
    }
    return false;
}

std::vector<fs::path> CollectEvidenceFiles(const fs::path& input_root, const fs::path& trading_root,
                                           const fs::path& wal_file, const std::string& start_day,
                                           const std::string& end_day) {
    std::set<fs::path> files;
    std::error_code ec;
    if (fs::exists(input_root, ec)) {
        for (const auto& entry : fs::recursive_directory_iterator(
                 input_root, fs::directory_options::skip_permission_denied, ec)) {
            if (!ec && entry.is_regular_file(ec) &&
                ContainsRelevantDay(entry.path().generic_string(), start_day, end_day)) {
                files.insert(entry.path());
            }
        }
    }
    if (fs::exists(trading_root, ec)) {
        for (const auto& entry : fs::recursive_directory_iterator(
                 trading_root, fs::directory_options::skip_permission_denied, ec)) {
            if (ec) {
                break;
            }
            if (!entry.is_regular_file(ec)) {
                continue;
            }
            const std::string path = entry.path().generic_string();
            if (ContainsRelevantDay(path, start_day, end_day) ||
                path.find("signal_execution_watch.jsonl") != std::string::npos) {
                files.insert(entry.path());
            }
        }
    }
    if (fs::is_regular_file(wal_file, ec)) {
        files.insert(wal_file);
    }
    return {files.begin(), files.end()};
}

bool WriteAtomicText(const fs::path& path, const std::string& payload, std::string* error) {
    std::error_code ec;
    fs::create_directories(path.parent_path(), ec);
    if (ec) {
        SetError(error, "failed to create report directory: " + ec.message());
        return false;
    }
    const fs::path temporary = path.string() + ".tmp";
    {
        std::ofstream output(temporary, std::ios::out | std::ios::trunc);
        if (!output.is_open()) {
            SetError(error, "failed to open report temp file: " + temporary.string());
            return false;
        }
        output << payload;
        output.flush();
        if (!output.good()) {
            SetError(error, "failed to flush report temp file: " + temporary.string());
            return false;
        }
    }
    fs::rename(temporary, path, ec);
    if (ec) {
        SetError(error, "failed to publish report: " + ec.message());
        return false;
    }
    return true;
}

std::string BarHeader() {
    return "instrument_id,exchange_id,trading_day,action_day,minute,open,high,low,close,"
           "analysis_open,analysis_high,analysis_low,analysis_close,analysis_price_offset,volume,"
           "ts_ns,period_end_ts_ns,finalized_ts_ns,expected_source_bars,observed_source_bars,"
           "is_complete,is_session_endpoint,strategy_eligible,volume_complete,has_conflict,"
           "is_recovery_replay";
}

void WriteBar(std::ofstream* output, const quant_hft::BarSnapshot& bar) {
    *output << std::setprecision(17) << bar.instrument_id << ',' << bar.exchange_id << ','
            << bar.trading_day << ',' << bar.action_day << ',' << bar.minute << ',' << bar.open
            << ',' << bar.high << ',' << bar.low << ',' << bar.close << ',' << bar.analysis_open
            << ',' << bar.analysis_high << ',' << bar.analysis_low << ',' << bar.analysis_close
            << ',' << bar.analysis_price_offset << ',' << bar.volume << ',' << bar.ts_ns << ','
            << bar.period_end_ts_ns << ',' << bar.finalized_ts_ns << ',' << bar.expected_source_bars
            << ',' << bar.observed_source_bars << ',' << (bar.is_complete ? 1 : 0) << ','
            << (bar.is_session_endpoint ? 1 : 0) << ',' << (bar.strategy_eligible ? 1 : 0) << ','
            << (bar.volume_complete ? 1 : 0) << ',' << (bar.has_conflict ? 1 : 0) << ','
            << (bar.is_recovery_replay ? 1 : 0) << '\n';
}

bool InitializeStrategy(ProductRuntime* runtime, const fs::path& repository_root,
                        std::string* error) {
    const fs::path config = repository_root / "configs" / "strategies" /
                            ("main_sim_trade_candidate_" + runtime->product + ".yaml");
    if (!fs::is_regular_file(config)) {
        return true;
    }
    quant_hft::CompositeStrategyDefinition definition;
    if (!quant_hft::LoadCompositeStrategyDefinition(config.string(), &definition, error)) {
        return false;
    }
    definition.run_type = "backtest";
    runtime->strategy = std::make_unique<quant_hft::CompositeStrategy>(std::move(definition));
    quant_hft::StrategyContext context;
    context.strategy_id = "repair_validation_" + runtime->product;
    context.account_id = "repair_validation";
    context.metadata["run_type"] = "backtest";
    context.metadata["log_level"] = "error";
    context.metadata["log_sink"] = "stderr";
    try {
        runtime->strategy->Initialize(context);
        runtime->strategy->SetBacktestAccountSnapshot(200'000.0, 0.0);
    } catch (const std::exception& ex) {
        SetError(error, "failed to initialize repair strategy: " + std::string(ex.what()));
        return false;
    }
    return true;
}

bool ProcessResult(ProductRuntime* runtime, quant_hft::MarketBarPipelineResult result,
                   OutputFiles* outputs, ValidationStats* stats, std::set<std::string>* observed_1m,
                   std::set<std::string>* observed_5m, std::string* error) {
    if (result.duplicate_tick) {
        ++stats->duplicate_ticks;
    }
    if (result.late_tick) {
        ++stats->late_ticks;
    }
    stats->critical_conflicts += static_cast<std::int64_t>(result.critical_conflicts.size());

    for (const auto& bar : result.one_minute_bars) {
        auto* output = outputs->Open(fs::path("trading_day=" + bar.trading_day) / "varieties" /
                                         runtime->product / "market" / "bars_1m.csv",
                                     BarHeader(), error);
        if (output == nullptr) {
            return false;
        }
        WriteBar(output, bar);
        if (bar.is_session_endpoint) {
            ++stats->endpoint_1m_bars;
            continue;
        }
        if (!observed_1m->insert(SlotKey(runtime->product, bar.trading_day, bar.minute)).second) {
            ++stats->duplicate_1m_keys;
        }
    }

    for (auto& emission : result.timeframe_emissions) {
        if (emission.timeframe_minutes != 5) {
            continue;
        }
        auto* output = outputs->Open(fs::path("trading_day=" + emission.bar.trading_day) /
                                         "varieties" / runtime->product / "market" / "bars_5m.csv",
                                     BarHeader(), error);
        if (output == nullptr) {
            return false;
        }
        WriteBar(output, emission.bar);
        if (emission.bar.is_session_endpoint) {
            ++stats->endpoint_5m_bars;
            continue;
        }
        if (!observed_5m
                 ->insert(SlotKey(runtime->product, emission.bar.trading_day, emission.bar.minute))
                 .second) {
            ++stats->duplicate_5m_keys;
        }
        if (!emission.bar.is_complete) {
            ++stats->incomplete_5m_bars;
        }
        if (emission.strategy_eligible &&
            (!emission.bar.is_complete || emission.bar.has_conflict ||
             emission.bar.is_recovery_replay || emission.bar.is_session_endpoint)) {
            ++stats->ineligible_integrity_violations;
        }

        std::string raw_signal;
        std::int64_t decision_count = 0;
        if (emission.strategy_eligible && runtime->strategy != nullptr) {
            const auto decisions = runtime->strategy->OnState(emission.state);
            decision_count = static_cast<std::int64_t>(decisions.size());
            ++stats->strategy_evaluations;
            for (const auto& trace : runtime->strategy->CollectAtomicIndicatorTrace()) {
                if (!trace.raw_signal.empty()) {
                    raw_signal = trace.raw_signal;
                    ++stats->signal_candidates;
                }
            }
            auto* signal_output = outputs->Open(
                fs::path("trading_day=" + emission.bar.trading_day) / "varieties" /
                    runtime->product / "strategy" / "canonical_candidates_5m.csv",
                "minute,instrument_id,strategy_eligible,is_complete,raw_signal,decision_count,"
                "ts_ns",
                error);
            if (signal_output == nullptr) {
                return false;
            }
            *signal_output << emission.bar.minute << ',' << emission.bar.instrument_id << ",1,1,"
                           << raw_signal << ',' << decision_count << ',' << emission.bar.ts_ns
                           << '\n';
        }
        if (runtime->product == "hc" && emission.bar.trading_day == "20260713" &&
            emission.bar.minute == "20260713 11:25") {
            ++stats->hc_1125_corrected_bars;
            stats->hc_1125_complete = emission.bar.is_complete;
            stats->hc_1125_close = emission.bar.close;
            stats->hc_1125_corrected_raw_signal = raw_signal;
        }
    }
    return true;
}

void CountOldHcSignal(const fs::path& input_root, ValidationStats* stats) {
    const fs::path path =
        input_root / "trading_day=20260713" / "varieties" / "hc" / "strategy" / "kama_5m.csv";
    std::ifstream input(path);
    std::string line;
    if (!std::getline(input, line)) {
        return;
    }
    const auto header = HeaderMap(line);
    while (std::getline(input, line)) {
        const auto row = SplitCsv(line);
        if (Field(row, header, "minute") != "20260713 11:25") {
            continue;
        }
        ++stats->hc_1125_old_rows;
        if (Field(row, header, "raw_signal") == "sell") {
            ++stats->hc_1125_old_sell_candidates;
        }
    }
}

fs::path PortableReportPath(const fs::path& path, const fs::path& repository_root) {
    std::error_code ec;
    const fs::path absolute_path = fs::absolute(path, ec).lexically_normal();
    if (ec) {
        return path.lexically_normal();
    }
    const fs::path absolute_root = fs::absolute(repository_root, ec).lexically_normal();
    if (ec) {
        return path.lexically_normal();
    }
    const fs::path relative = absolute_path.lexically_relative(absolute_root);
    if (!relative.empty() && relative.native() != ".." && relative.native().rfind("../", 0) != 0) {
        return relative;
    }
    return absolute_path;
}

std::string RenderJson(const ValidationStats& stats, const std::string& status,
                       const fs::path& input_root, const fs::path& output_root,
                       const std::string& start_day, const std::string& end_day) {
    const bool false_sell_removed = stats.hc_1125_old_sell_candidates > 0 &&
                                    stats.hc_1125_corrected_bars == 1 && stats.hc_1125_complete &&
                                    stats.hc_1125_corrected_raw_signal.empty();
    std::ostringstream out;
    out << "{\n";
    out << "  \"schema_version\": 2,\n";
    out << "  \"status\": \"" << status << "\",\n";
    out << "  \"period\": {\"start\": \"" << start_day << "\", \"end\": \"" << end_day << "\"},\n";
    out << "  \"input_root\": \"" << JsonEscape(input_root.string()) << "\",\n";
    out << "  \"output_root\": \"" << JsonEscape(output_root.string()) << "\",\n";
    out << "  \"fixed_lateness_ms\": 3500,\n";
    out << "  \"trading_days\": " << stats.trading_days << ",\n";
    out << "  \"product_days\": " << stats.product_days << ",\n";
    out << "  \"input_tick_rows\": " << stats.input_tick_rows << ",\n";
    out << "  \"parse_errors\": " << stats.parse_errors << ",\n";
    out << "  \"duplicate_ticks\": " << stats.duplicate_ticks << ",\n";
    out << "  \"late_ticks\": " << stats.late_ticks << ",\n";
    out << "  \"one_minute\": {\"expected_slots\": " << stats.expected_1m_slots
        << ", \"canonical_bars\": " << stats.canonical_1m_slots
        << ", \"explicit_no_tick_gaps\": " << stats.no_tick_1m_gaps
        << ", \"endpoint_bars\": " << stats.endpoint_1m_bars
        << ", \"duplicate_keys\": " << stats.duplicate_1m_keys << "},\n";
    out << "  \"five_minute\": {\"expected_keys\": " << stats.expected_5m_keys
        << ", \"canonical_results\": " << stats.canonical_5m_keys
        << ", \"explicit_no_tick_gaps\": " << stats.no_tick_5m_gaps
        << ", \"incomplete_bars\": " << stats.incomplete_5m_bars
        << ", \"endpoint_bars\": " << stats.endpoint_5m_bars
        << ", \"duplicate_keys\": " << stats.duplicate_5m_keys << "},\n";
    out << "  \"strategy\": {\"evaluations\": " << stats.strategy_evaluations
        << ", \"candidates\": " << stats.signal_candidates
        << ", \"integrity_eligibility_violations\": " << stats.ineligible_integrity_violations
        << "},\n";
    out << "  \"hc_20260713_1125\": {\"old_rows\": " << stats.hc_1125_old_rows
        << ", \"old_sell_candidates\": " << stats.hc_1125_old_sell_candidates
        << ", \"corrected_bars\": " << stats.hc_1125_corrected_bars
        << ", \"complete\": " << (stats.hc_1125_complete ? "true" : "false")
        << ", \"close\": " << std::setprecision(17) << stats.hc_1125_close
        << ", \"corrected_raw_signal\": \"" << JsonEscape(stats.hc_1125_corrected_raw_signal)
        << "\", \"false_sell_removed\": " << (false_sell_removed ? "true" : "false") << "},\n";
    out << "  \"critical_conflicts\": " << stats.critical_conflicts << ",\n";
    out << "  \"evidence\": {\"files\": " << stats.evidence_files
        << ", \"bytes\": " << stats.evidence_bytes << ", \"manifest\": \"baseline_sha256.csv\"}\n";
    out << "}\n";
    return out.str();
}

std::string RenderMarkdown(const ValidationStats& stats, const std::string& status,
                           const fs::path& output_root) {
    const bool false_sell_removed = stats.hc_1125_old_sell_candidates > 0 &&
                                    stats.hc_1125_corrected_bars == 1 && stats.hc_1125_complete &&
                                    stats.hc_1125_corrected_raw_signal.empty();
    std::ostringstream out;
    out << "# 2026-07-01—2026-07-18 修复回放验收\n\n";
    out << "- 状态：`" << status << "`\n";
    out << "- 输出目录：`" << output_root.string() << "`\n";
    out << "- 交易日：" << stats.trading_days << "，品种交易日：" << stats.product_days
        << "，原始 Tick：" << stats.input_tick_rows << "\n";
    out << "- 1m：预期 " << stats.expected_1m_slots << "，canonical " << stats.canonical_1m_slots
        << "，显式 no-tick gap " << stats.no_tick_1m_gaps << "，重复键 " << stats.duplicate_1m_keys
        << "\n";
    out << "- 5m：预期 " << stats.expected_5m_keys << "，canonical/诊断 " << stats.canonical_5m_keys
        << "，显式 no-tick gap " << stats.no_tick_5m_gaps << "，不完整 " << stats.incomplete_5m_bars
        << "，重复键 " << stats.duplicate_5m_keys << "\n";
    out << "- 完整性违规后仍可交易：" << stats.ineligible_integrity_violations
        << "；critical conflict：" << stats.critical_conflicts << "\n";
    out << "- 7月13日 hc 11:25：旧 sell 候选 " << stats.hc_1125_old_sell_candidates
        << "，修复后唯一 Bar " << stats.hc_1125_corrected_bars << "，close=" << stats.hc_1125_close
        << "，修复后 raw signal=`" << stats.hc_1125_corrected_raw_signal
        << "`，伪信号移除=" << (false_sell_removed ? "是" : "否") << "\n";
    out << "- 原始证据 SHA-256：" << stats.evidence_files << " 个文件，" << stats.evidence_bytes
        << " bytes；清单见 `baseline_sha256.csv`。\n\n";
    out << "修复回放不覆盖原始目录；endpoint Bar 单独保存且不可交易，gap 仅作为诊断记录，"
           "不合成 OHLC。\n";
    return out.str();
}

}  // namespace

int main(int argc, char** argv) {
    const auto args = quant_hft::apps::ParseArgs(argc, argv);
    const fs::path repository_root =
        quant_hft::apps::GetArg(args, "repo-root", fs::current_path().string());
    const fs::path input_root = quant_hft::apps::GetArg(
        args, "input-root", (repository_root / "runtime/market_data/simnow").string());
    const fs::path output_root = quant_hft::apps::GetArg(
        args, "output-root",
        (repository_root / "runtime/market_data/simnow_corrected_v2").string());
    const fs::path trading_root = quant_hft::apps::GetArg(
        args, "trading-root", (repository_root / "runtime/trading").string());
    const fs::path wal_file = quant_hft::apps::GetArg(
        args, "wal-file", (repository_root / "runtime/trading/wal/simnow/events.wal").string());
    const fs::path report_json = quant_hft::apps::GetArg(
        args, "report-json",
        (repository_root / "docs/results/20260701_20260718_repair_validation.json").string());
    const fs::path report_md = quant_hft::apps::GetArg(
        args, "report-md",
        (repository_root / "docs/results/20260701_20260718_repair_validation.md").string());
    const std::string start_day = quant_hft::apps::GetArg(args, "start-day", "20260701");
    const std::string end_day = quant_hft::apps::GetArg(args, "end-day", "20260718");

    std::error_code ec;
    if (!fs::is_directory(input_root, ec)) {
        std::cerr << "market_data_repair_cli: input root is missing: " << input_root << '\n';
        return 2;
    }
    if (fs::exists(output_root, ec)) {
        std::cerr << "market_data_repair_cli: refusing to overwrite output root: " << output_root
                  << '\n';
        return 2;
    }
    const auto stamp = std::chrono::duration_cast<std::chrono::nanoseconds>(
                           std::chrono::system_clock::now().time_since_epoch())
                           .count();
    const fs::path temporary_root = output_root.string() + ".tmp." + std::to_string(stamp);
    fs::create_directories(temporary_root, ec);
    if (ec) {
        std::cerr << "market_data_repair_cli: failed to create temporary output: " << ec.message()
                  << '\n';
        return 2;
    }

    OutputFiles outputs(temporary_root);
    ValidationStats stats;
    std::string error;
    std::vector<fs::path> tick_files;
    std::set<std::string> discovered_days;
    for (const auto& entry : fs::recursive_directory_iterator(
             input_root, fs::directory_options::skip_permission_denied, ec)) {
        if (ec) {
            break;
        }
        if (!entry.is_regular_file(ec) || entry.path().filename() != "ticks.csv") {
            continue;
        }
        const std::string day = DayFromPath(entry.path());
        if (day >= start_day && day <= end_day) {
            tick_files.push_back(entry.path());
            discovered_days.insert(day);
        }
    }
    std::sort(tick_files.begin(), tick_files.end(), [](const fs::path& lhs, const fs::path& rhs) {
        const std::string lhs_product = ProductFromPath(lhs);
        const std::string rhs_product = ProductFromPath(rhs);
        if (lhs_product != rhs_product) {
            return lhs_product < rhs_product;
        }
        return DayFromPath(lhs) < DayFromPath(rhs);
    });
    stats.trading_days = static_cast<std::int64_t>(discovered_days.size());

    std::map<std::string, std::unique_ptr<ProductRuntime>> runtimes;
    std::map<std::string, DayProductInfo> day_product_info;
    std::set<std::string> observed_1m;
    std::set<std::string> observed_5m;

    for (const auto& tick_file : tick_files) {
        const std::string product = ProductFromPath(tick_file);
        const std::string path_day = DayFromPath(tick_file);
        auto& runtime = runtimes[product];
        if (runtime == nullptr) {
            runtime = std::make_unique<ProductRuntime>(product);
            if (!InitializeStrategy(runtime.get(), repository_root, &error)) {
                std::cerr << "market_data_repair_cli: " << error << '\n';
                fs::remove_all(temporary_root, ec);
                return 3;
            }
        }

        std::ifstream input(tick_file);
        std::string line;
        if (!std::getline(input, line)) {
            ++stats.parse_errors;
            continue;
        }
        const auto header = HeaderMap(line);
        quant_hft::EpochNanos max_recv_ts_ns = 0;
        while (std::getline(input, line)) {
            if (line.empty()) {
                continue;
            }
            ++stats.input_tick_rows;
            const auto parsed = ParseTick(SplitCsv(line), header);
            if (!parsed.has_value()) {
                ++stats.parse_errors;
                continue;
            }
            const auto& snapshot = *parsed;
            const std::string info_key = product + "|" + path_day;
            day_product_info.try_emplace(
                info_key,
                DayProductInfo{path_day, product, snapshot.instrument_id, snapshot.exchange_id});
            max_recv_ts_ns = std::max(max_recv_ts_ns, snapshot.recv_ts_ns);
            if (!ProcessResult(runtime.get(), runtime->pipeline.OnTick(snapshot), &outputs, &stats,
                               &observed_1m, &observed_5m, &error)) {
                std::cerr << "market_data_repair_cli: " << error << '\n';
                fs::remove_all(temporary_root, ec);
                return 3;
            }
        }
        if (max_recv_ts_ns > 0 &&
            !ProcessResult(runtime.get(),
                           runtime->pipeline.AdvanceWatermark(max_recv_ts_ns + kTenMinutesNs),
                           &outputs, &stats, &observed_1m, &observed_5m, &error)) {
            std::cerr << "market_data_repair_cli: " << error << '\n';
            fs::remove_all(temporary_root, ec);
            return 3;
        }
    }
    stats.product_days = static_cast<std::int64_t>(day_product_info.size());

    quant_hft::TradingSessionCalendar session_calendar;
    std::set<std::string> expected_1m;
    std::set<std::string> expected_5m;
    for (const auto& [key, info] : day_product_info) {
        (void)key;
        for (int minute = 0; minute < 24 * 60; ++minute) {
            const std::string time = FormatMinute(minute);
            if (!session_calendar.IsOrderTime(info.exchange_id, info.instrument_id, time + ":00")) {
                continue;
            }
            expected_1m.insert(
                SlotKey(info.product, info.trading_day, info.trading_day + " " + time));
            const std::string bucket_time = FormatMinute((minute / 5) * 5);
            expected_5m.insert(
                SlotKey(info.product, info.trading_day, info.trading_day + " " + bucket_time));
        }
    }
    stats.expected_1m_slots = static_cast<std::int64_t>(expected_1m.size());
    stats.expected_5m_keys = static_cast<std::int64_t>(expected_5m.size());
    for (const auto& key : expected_1m) {
        if (observed_1m.find(key) != observed_1m.end()) {
            ++stats.canonical_1m_slots;
            continue;
        }
        ++stats.no_tick_1m_gaps;
        auto* gaps =
            outputs.Open("validation/gaps_1m.csv", "product,trading_day,minute,reason", &error);
        if (gaps == nullptr) {
            std::cerr << "market_data_repair_cli: " << error << '\n';
            fs::remove_all(temporary_root, ec);
            return 3;
        }
        const auto first = key.find('|');
        const auto second = key.find('|', first + 1);
        *gaps << key.substr(0, first) << ',' << key.substr(first + 1, second - first - 1) << ','
              << key.substr(second + 1) << ",no_tick\n";
    }
    for (const auto& key : expected_5m) {
        if (observed_5m.find(key) != observed_5m.end()) {
            ++stats.canonical_5m_keys;
            continue;
        }
        ++stats.no_tick_5m_gaps;
        auto* gaps =
            outputs.Open("validation/gaps_5m.csv", "product,trading_day,minute,reason", &error);
        if (gaps == nullptr) {
            std::cerr << "market_data_repair_cli: " << error << '\n';
            fs::remove_all(temporary_root, ec);
            return 3;
        }
        const auto first = key.find('|');
        const auto second = key.find('|', first + 1);
        *gaps << key.substr(0, first) << ',' << key.substr(first + 1, second - first - 1) << ','
              << key.substr(second + 1) << ",no_tick\n";
    }

    CountOldHcSignal(input_root, &stats);
    const auto evidence_files =
        CollectEvidenceFiles(input_root, trading_root, wal_file, start_day, end_day);
    auto* manifest = outputs.Open("baseline_sha256.csv", "sha256,size_bytes,path", &error);
    if (manifest == nullptr) {
        std::cerr << "market_data_repair_cli: " << error << '\n';
        fs::remove_all(temporary_root, ec);
        return 3;
    }
    for (const auto& path : evidence_files) {
        const std::string digest = Sha256File(path, &error);
        if (digest.empty()) {
            std::cerr << "market_data_repair_cli: " << error << '\n';
            fs::remove_all(temporary_root, ec);
            return 3;
        }
        const auto size = fs::file_size(path, ec);
        if (ec) {
            std::cerr << "market_data_repair_cli: failed to stat evidence: " << path << '\n';
            fs::remove_all(temporary_root, ec);
            return 3;
        }
        *manifest << digest << ',' << size << ','
                  << PortableReportPath(path, repository_root).generic_string() << '\n';
        ++stats.evidence_files;
        stats.evidence_bytes += size;
    }

    if (!outputs.Close(&error)) {
        std::cerr << "market_data_repair_cli: " << error << '\n';
        fs::remove_all(temporary_root, ec);
        return 3;
    }
    fs::rename(temporary_root, output_root, ec);
    if (ec) {
        std::cerr << "market_data_repair_cli: failed to publish output directory: " << ec.message()
                  << '\n';
        return 3;
    }

    const bool false_sell_removed = stats.hc_1125_old_sell_candidates > 0 &&
                                    stats.hc_1125_corrected_bars == 1 && stats.hc_1125_complete &&
                                    stats.hc_1125_corrected_raw_signal.empty();
    const bool passed =
        stats.trading_days == 13 && stats.product_days == 26 && stats.expected_1m_slots == 8970 &&
        stats.canonical_1m_slots + stats.no_tick_1m_gaps == stats.expected_1m_slots &&
        stats.expected_5m_keys == 1794 &&
        stats.canonical_5m_keys + stats.no_tick_5m_gaps == stats.expected_5m_keys &&
        stats.parse_errors == 0 && stats.duplicate_1m_keys == 0 && stats.duplicate_5m_keys == 0 &&
        stats.critical_conflicts == 0 && stats.ineligible_integrity_violations == 0 &&
        false_sell_removed;
    const std::string status = passed ? "pass" : "failed";
    const fs::path report_input_root = PortableReportPath(input_root, repository_root);
    const fs::path report_output_root = PortableReportPath(output_root, repository_root);
    if (!WriteAtomicText(
            report_json,
            RenderJson(stats, status, report_input_root, report_output_root, start_day, end_day),
            &error) ||
        !WriteAtomicText(report_md, RenderMarkdown(stats, status, report_output_root), &error)) {
        std::cerr << "market_data_repair_cli: " << error << '\n';
        return 3;
    }

    std::cout << "market_data_repair_cli status=" << status << " ticks=" << stats.input_tick_rows
              << " expected_1m=" << stats.expected_1m_slots
              << " canonical_1m=" << stats.canonical_1m_slots
              << " gaps_1m=" << stats.no_tick_1m_gaps << " expected_5m=" << stats.expected_5m_keys
              << " canonical_5m=" << stats.canonical_5m_keys << " gaps_5m=" << stats.no_tick_5m_gaps
              << '\n';
    return passed ? 0 : 4;
}
