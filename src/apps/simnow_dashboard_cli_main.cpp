#include <unistd.h>

#include <algorithm>
#include <cctype>
#include <chrono>
#include <csignal>
#include <cstdint>
#include <cstdlib>
#include <ctime>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <map>
#include <optional>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include "quant_hft/apps/cli_support.h"

namespace {

namespace fs = std::filesystem;

constexpr std::int64_t kTickStaleSeconds = 180;
constexpr std::int64_t kBarStaleSeconds = 240;
constexpr std::size_t kRecentAlertLimit = 24;

struct DashboardOptions {
    std::string run_root;
    std::string market_data_dir;
    std::string wal_file;
    std::string report_root;
    std::string export_root;
    std::string output_dir;
    int watch_seconds{0};
    bool strict_exit{false};
};

struct ProcessStatus {
    std::string status{"missing_pid"};
    bool healthy{false};
    std::string pid;
    std::string run_dir;
    std::string core_log;
    std::vector<std::string> supervisor_pids;
};

struct ContractStatus {
    std::string product;
    std::string instrument_id;
    std::string exchange_id;
    std::string selection_metric;
    std::string path;
};

struct TickSnapshot {
    std::string instrument_id;
    std::string exchange_id;
    std::string trading_day;
    std::string action_day;
    std::string update_time;
    std::string last_price;
    std::string bid_price_1;
    std::string ask_price_1;
    std::string volume;
    std::string open_interest;
};

struct BarSnapshot {
    std::string instrument_id;
    std::string exchange_id;
    std::string trading_day;
    std::string action_day;
    std::string minute;
    std::string open;
    std::string high;
    std::string low;
    std::string close;
    std::string volume;
};

struct MarketFileStatus {
    std::string product;
    std::string trading_day;
    std::string tick_file;
    std::string bar_file;
    std::optional<std::int64_t> tick_age_seconds;
    std::optional<std::int64_t> bar_age_seconds;
    std::string tick_status{"missing"};
    std::string bar_status{"missing"};
    bool healthy{false};
    TickSnapshot tick;
    BarSnapshot bar;
};

struct WalStatus {
    std::string path;
    bool exists{false};
    std::int64_t lines_total{0};
    std::int64_t order_events{0};
    std::int64_t trade_or_fill_events{0};
};

struct DailyStatus {
    std::string trading_day;
    bool report_found{false};
    bool export_found{false};
    bool health_report_found{false};
    bool alert_report_found{false};
    std::int64_t tick_rows{0};
    std::int64_t bar_rows{0};
    std::int64_t wal_order_events{0};
    std::int64_t wal_trade_events{0};
    std::int64_t exported_order_events{0};
    std::int64_t exported_trade_fills{0};
    std::int64_t parse_errors{0};
    bool wal_missing_in_export{false};
    std::int64_t order_csv_rows{0};
    std::int64_t fill_csv_rows{0};
    std::optional<bool> ops_overall_healthy;
    std::int64_t critical_alerts{0};
    std::int64_t warn_alerts{0};
    std::int64_t info_alerts{0};
    std::string report_path;
    std::string export_summary_path;
    std::string health_report_path;
    std::string alert_report_path;
};

struct DashboardState {
    std::int64_t generated_ts_ns{0};
    std::string generated_at_local;
    bool overall_healthy{false};
    std::int64_t warning_count{0};
    ProcessStatus process;
    std::vector<ContractStatus> contracts;
    std::vector<MarketFileStatus> markets;
    WalStatus wal;
    DailyStatus daily;
    std::vector<std::string> recent_alerts;
    std::map<std::string, std::string> paths;
};

std::string GetEnvOrDefault(const char* key, const std::string& fallback) {
    const char* value = std::getenv(key);
    if (value == nullptr || std::string(value).empty()) {
        return fallback;
    }
    return std::string(value);
}

std::string DefaultPathFromRoot(const std::string& suffix) {
    const std::string root = GetEnvOrDefault("QUANT_ROOT", "");
    if (root.empty()) {
        return suffix;
    }
    return (fs::path(root) / suffix).string();
}

bool IsAllDigits(const std::string& text) {
    return !text.empty() && std::all_of(text.begin(), text.end(),
                                        [](unsigned char ch) { return std::isdigit(ch) != 0; });
}

bool ParseNonNegativeInt(const std::string& text, int* out) {
    if (out == nullptr || !IsAllDigits(text)) {
        return false;
    }
    try {
        *out = std::stoi(text);
        return *out >= 0;
    } catch (...) {
        return false;
    }
}

bool ParseBoolArg(const quant_hft::apps::ArgMap& args, const std::string& key, bool fallback) {
    if (!quant_hft::apps::HasArg(args, key)) {
        return fallback;
    }
    const std::string value = quant_hft::apps::GetArg(args, key);
    if (value.empty() || value == "true" || value == "1" || value == "yes" || value == "on") {
        return true;
    }
    if (value == "false" || value == "0" || value == "no" || value == "off") {
        return false;
    }
    return fallback;
}

std::int64_t UnixEpochNanosNow() {
    const auto now = std::chrono::system_clock::now();
    return std::chrono::duration_cast<std::chrono::nanoseconds>(now.time_since_epoch()).count();
}

std::string FormatLocalTime(std::int64_t ts_ns) {
    std::time_t seconds = static_cast<std::time_t>(ts_ns / 1'000'000'000LL);
    std::tm tm{};
#if defined(_WIN32)
    localtime_s(&tm, &seconds);
#else
    localtime_r(&seconds, &tm);
#endif
    char buffer[32] = {0};
    if (std::strftime(buffer, sizeof(buffer), "%Y-%m-%d %H:%M:%S %z", &tm) == 0) {
        return "";
    }
    return std::string(buffer);
}

std::optional<std::string> ReadTextFile(const fs::path& path) {
    std::ifstream input(path);
    if (!input.is_open()) {
        return std::nullopt;
    }
    std::ostringstream buffer;
    buffer << input.rdbuf();
    return buffer.str();
}

std::string ReadFirstLine(const fs::path& path) {
    std::ifstream input(path);
    std::string line;
    if (input.is_open() && std::getline(input, line)) {
        return line;
    }
    return "";
}

std::string ReadDigitsOnly(const fs::path& path) {
    const auto text = ReadTextFile(path);
    if (!text.has_value()) {
        return "";
    }
    std::string digits;
    for (const char ch : *text) {
        if (std::isdigit(static_cast<unsigned char>(ch)) != 0) {
            digits.push_back(ch);
        }
    }
    return digits;
}

std::optional<std::int64_t> FileAgeSeconds(const fs::path& path) {
    std::error_code ec;
    const auto modified = fs::last_write_time(path, ec);
    if (ec) {
        return std::nullopt;
    }
    const auto age = std::chrono::duration_cast<std::chrono::seconds>(
                         fs::file_time_type::clock::now() - modified)
                         .count();
    return std::max<std::int64_t>(0, age);
}

bool ProcessIsAlive(const std::string& pid_text) {
    if (!IsAllDigits(pid_text)) {
        return false;
    }
    try {
        const auto pid = static_cast<pid_t>(std::stol(pid_text));
        return pid > 0 && kill(pid, 0) == 0;
    } catch (...) {
        return false;
    }
}

std::vector<std::string> FindSupervisorPids() {
    std::vector<std::string> pids;
    std::error_code ec;
    if (!fs::exists("/proc", ec)) {
        return pids;
    }
    const std::string self_pid = std::to_string(static_cast<long long>(getpid()));
    for (const auto& entry :
         fs::directory_iterator("/proc", fs::directory_options::skip_permission_denied, ec)) {
        if (ec) {
            break;
        }
        if (!entry.is_directory(ec)) {
            continue;
        }
        const std::string pid = entry.path().filename().string();
        if (pid == self_pid || !IsAllDigits(pid)) {
            continue;
        }
        const auto cmdline = ReadTextFile(entry.path() / "cmdline");
        if (!cmdline.has_value()) {
            continue;
        }
        std::string text = *cmdline;
        std::replace(text.begin(), text.end(), '\0', ' ');
        if (text.find("supervise_simnow_trading.sh") != std::string::npos) {
            pids.push_back(pid);
        }
    }
    std::sort(pids.begin(), pids.end());
    return pids;
}

std::string ToLower(std::string text) {
    std::transform(text.begin(), text.end(), text.begin(),
                   [](unsigned char ch) { return static_cast<char>(std::tolower(ch)); });
    return text;
}

std::vector<std::string> SplitCsvLine(const std::string& line) {
    std::vector<std::string> fields;
    std::string field;
    std::istringstream input(line);
    while (std::getline(input, field, ',')) {
        fields.push_back(field);
    }
    if (!line.empty() && line.back() == ',') {
        fields.emplace_back();
    }
    return fields;
}

std::string LastDataLine(const fs::path& path) {
    std::ifstream input(path);
    if (!input.is_open()) {
        return "";
    }
    std::string line;
    std::string last;
    bool first = true;
    while (std::getline(input, line)) {
        if (first) {
            first = false;
            continue;
        }
        if (!line.empty()) {
            last = line;
        }
    }
    return last;
}

std::int64_t CountCsvDataRows(const fs::path& path) {
    std::ifstream input(path);
    if (!input.is_open()) {
        return 0;
    }
    std::string line;
    std::int64_t rows = 0;
    bool first = true;
    while (std::getline(input, line)) {
        if (first) {
            first = false;
            continue;
        }
        if (!line.empty()) {
            ++rows;
        }
    }
    return rows;
}

TickSnapshot ParseTick(const fs::path& path) {
    TickSnapshot tick;
    const auto fields = SplitCsvLine(LastDataLine(path));
    if (fields.size() > 0) {
        tick.instrument_id = fields[0];
    }
    if (fields.size() > 1) {
        tick.exchange_id = fields[1];
    }
    if (fields.size() > 2) {
        tick.trading_day = fields[2];
    }
    if (fields.size() > 3) {
        tick.action_day = fields[3];
    }
    if (fields.size() > 4) {
        tick.update_time = fields[4];
    }
    if (fields.size() > 6) {
        tick.last_price = fields[6];
    }
    if (fields.size() > 7) {
        tick.bid_price_1 = fields[7];
    }
    if (fields.size() > 8) {
        tick.ask_price_1 = fields[8];
    }
    if (fields.size() > 11) {
        tick.volume = fields[11];
    }
    if (fields.size() > 12) {
        tick.open_interest = fields[12];
    }
    return tick;
}

BarSnapshot ParseBar(const fs::path& path) {
    BarSnapshot bar;
    const auto fields = SplitCsvLine(LastDataLine(path));
    if (fields.size() > 0) {
        bar.instrument_id = fields[0];
    }
    if (fields.size() > 1) {
        bar.exchange_id = fields[1];
    }
    if (fields.size() > 2) {
        bar.trading_day = fields[2];
    }
    if (fields.size() > 3) {
        bar.action_day = fields[3];
    }
    if (fields.size() > 4) {
        bar.minute = fields[4];
    }
    if (fields.size() > 5) {
        bar.open = fields[5];
    }
    if (fields.size() > 6) {
        bar.high = fields[6];
    }
    if (fields.size() > 7) {
        bar.low = fields[7];
    }
    if (fields.size() > 8) {
        bar.close = fields[8];
    }
    if (fields.size() > 14) {
        bar.volume = fields[14];
    }
    return bar;
}

std::string ProductFromMarketPath(const fs::path& path) {
    std::vector<std::string> parts;
    for (const auto& part : path) {
        parts.push_back(part.string());
    }
    for (std::size_t i = 0; i + 2 < parts.size(); ++i) {
        if (parts[i] == "varieties" && parts[i + 2] == "market") {
            return parts[i + 1];
        }
    }
    return "";
}

std::string TradingDayFromPath(const fs::path& path) {
    constexpr const char* kPrefix = "trading_day=";
    for (const auto& part : path) {
        const std::string value = part.string();
        if (value.rfind(kPrefix, 0) == 0) {
            return value.substr(std::string(kPrefix).size());
        }
    }
    return "";
}

int TradingDayRank(const std::string& trading_day) {
    return trading_day.size() == 8 &&
                   std::all_of(
                       trading_day.begin(), trading_day.end(),
                       [](char ch) { return std::isdigit(static_cast<unsigned char>(ch)) != 0; })
               ? 1
               : 0;
}

fs::path LatestTradingDayDirectory(const fs::path& root) {
    std::error_code ec;
    if (!fs::exists(root, ec)) {
        return {};
    }
    fs::path latest;
    std::string latest_day;
    int latest_rank = -1;
    for (const auto& entry :
         fs::directory_iterator(root, fs::directory_options::skip_permission_denied, ec)) {
        if (ec) {
            break;
        }
        if (!entry.is_directory(ec)) {
            continue;
        }
        const std::string trading_day = TradingDayFromPath(entry.path());
        if (trading_day.empty()) {
            continue;
        }
        const int rank = TradingDayRank(trading_day);
        if (latest.empty() || rank > latest_rank ||
            (rank == latest_rank && trading_day > latest_day)) {
            latest = entry.path();
            latest_day = trading_day;
            latest_rank = rank;
        }
    }
    return latest;
}

void MaybeSetLatest(const fs::path& candidate, std::string* target_path) {
    if (target_path == nullptr) {
        return;
    }
    if (target_path->empty()) {
        *target_path = candidate.string();
        return;
    }
    std::error_code ec;
    const auto current_time = fs::last_write_time(*target_path, ec);
    if (ec) {
        *target_path = candidate.string();
        return;
    }
    const auto candidate_time = fs::last_write_time(candidate, ec);
    if (!ec && candidate_time > current_time) {
        *target_path = candidate.string();
    }
}

std::map<std::string, MarketFileStatus> DiscoverMarketFiles(const std::string& market_data_dir) {
    std::map<std::string, MarketFileStatus> markets;
    std::error_code ec;
    const fs::path root(market_data_dir);
    if (!fs::exists(root, ec)) {
        return markets;
    }
    const fs::path latest_day_dir = LatestTradingDayDirectory(root);
    if (latest_day_dir.empty()) {
        return markets;
    }
    const std::string trading_day = TradingDayFromPath(latest_day_dir);
    for (const auto& entry : fs::recursive_directory_iterator(
             latest_day_dir, fs::directory_options::skip_permission_denied, ec)) {
        if (ec) {
            break;
        }
        if (!entry.is_regular_file(ec)) {
            continue;
        }
        const std::string filename = entry.path().filename().string();
        if (filename != "ticks.csv" && filename != "bars_1m.csv") {
            continue;
        }
        std::string product = ProductFromMarketPath(entry.path());
        if (product.empty()) {
            product = "global";
        }
        MarketFileStatus& status = markets[product];
        status.product = product;
        status.trading_day = trading_day;
        if (filename == "ticks.csv") {
            MaybeSetLatest(entry.path(), &status.tick_file);
        } else {
            MaybeSetLatest(entry.path(), &status.bar_file);
        }
    }
    return markets;
}

std::string ExtractJsonString(const std::string& text, const std::string& key) {
    const std::string marker = "\"" + key + "\"";
    const auto key_pos = text.find(marker);
    if (key_pos == std::string::npos) {
        return "";
    }
    auto colon = text.find(':', key_pos + marker.size());
    if (colon == std::string::npos) {
        return "";
    }
    ++colon;
    while (colon < text.size() && std::isspace(static_cast<unsigned char>(text[colon])) != 0) {
        ++colon;
    }
    if (colon >= text.size() || text[colon] != '"') {
        return "";
    }
    ++colon;
    std::string value;
    bool escaped = false;
    for (std::size_t i = colon; i < text.size(); ++i) {
        const char ch = text[i];
        if (escaped) {
            value.push_back(ch);
            escaped = false;
            continue;
        }
        if (ch == '\\') {
            escaped = true;
            continue;
        }
        if (ch == '"') {
            return value;
        }
        value.push_back(ch);
    }
    return "";
}

std::optional<std::int64_t> ExtractJsonInt(const std::string& text, const std::string& key) {
    const std::string marker = "\"" + key + "\"";
    const auto key_pos = text.find(marker);
    if (key_pos == std::string::npos) {
        return std::nullopt;
    }
    auto colon = text.find(':', key_pos + marker.size());
    if (colon == std::string::npos) {
        return std::nullopt;
    }
    ++colon;
    while (colon < text.size() && std::isspace(static_cast<unsigned char>(text[colon])) != 0) {
        ++colon;
    }
    std::size_t end = colon;
    if (end < text.size() && text[end] == '-') {
        ++end;
    }
    while (end < text.size() && std::isdigit(static_cast<unsigned char>(text[end])) != 0) {
        ++end;
    }
    if (end <= colon) {
        return std::nullopt;
    }
    try {
        return std::stoll(text.substr(colon, end - colon));
    } catch (...) {
        return std::nullopt;
    }
}

std::optional<bool> ExtractJsonBool(const std::string& text, const std::string& key) {
    const std::string marker = "\"" + key + "\"";
    const auto key_pos = text.find(marker);
    if (key_pos == std::string::npos) {
        return std::nullopt;
    }
    auto colon = text.find(':', key_pos + marker.size());
    if (colon == std::string::npos) {
        return std::nullopt;
    }
    ++colon;
    while (colon < text.size() && std::isspace(static_cast<unsigned char>(text[colon])) != 0) {
        ++colon;
    }
    if (text.compare(colon, 4, "true") == 0) {
        return true;
    }
    if (text.compare(colon, 5, "false") == 0) {
        return false;
    }
    return std::nullopt;
}

std::int64_t CountOccurrences(const std::string& text, const std::string& needle) {
    if (needle.empty()) {
        return 0;
    }
    std::int64_t count = 0;
    std::size_t pos = 0;
    while ((pos = text.find(needle, pos)) != std::string::npos) {
        ++count;
        pos += needle.size();
    }
    return count;
}

std::vector<ContractStatus> DiscoverContracts() {
    std::vector<ContractStatus> contracts;
    const fs::path root = fs::path(DefaultPathFromRoot("runtime/ctp_instruments"));
    std::error_code ec;
    if (!fs::exists(root, ec)) {
        return contracts;
    }
    for (const auto& entry :
         fs::directory_iterator(root, fs::directory_options::skip_permission_denied, ec)) {
        if (ec) {
            break;
        }
        if (!entry.is_regular_file(ec)) {
            continue;
        }
        const std::string filename = entry.path().filename().string();
        const std::string suffix = "_dominant_contract.json";
        if (filename.size() <= suffix.size() ||
            filename.substr(filename.size() - suffix.size()) != suffix) {
            continue;
        }
        const auto payload = ReadTextFile(entry.path());
        if (!payload.has_value()) {
            continue;
        }
        ContractStatus contract;
        contract.product = filename.substr(0, filename.size() - suffix.size());
        contract.instrument_id = ExtractJsonString(*payload, "instrument_id");
        contract.exchange_id = ExtractJsonString(*payload, "exchange_id");
        contract.selection_metric = ExtractJsonString(*payload, "selection_metric");
        contract.path = entry.path().string();
        contracts.push_back(std::move(contract));
    }
    std::sort(contracts.begin(), contracts.end(),
              [](const auto& lhs, const auto& rhs) { return lhs.product < rhs.product; });
    return contracts;
}

std::optional<std::string> LatestDirectoryName(const fs::path& root) {
    std::error_code ec;
    if (!fs::exists(root, ec)) {
        return std::nullopt;
    }
    std::string latest;
    for (const auto& entry :
         fs::directory_iterator(root, fs::directory_options::skip_permission_denied, ec)) {
        if (ec) {
            break;
        }
        if (!entry.is_directory(ec)) {
            continue;
        }
        const std::string name = entry.path().filename().string();
        if (latest.empty() || name > latest) {
            latest = name;
        }
    }
    if (latest.empty()) {
        return std::nullopt;
    }
    return latest;
}

std::string StatusForAge(const std::optional<std::int64_t>& age, std::int64_t threshold) {
    if (!age.has_value()) {
        return "missing";
    }
    return *age <= threshold ? "fresh" : "stale";
}

ProcessStatus CollectProcessStatus(const DashboardOptions& options) {
    ProcessStatus status;
    const fs::path run_root(options.run_root);
    status.pid = ReadDigitsOnly(run_root / "current_core_engine.pid");
    status.run_dir = ReadFirstLine(run_root / "current_run_dir");
    status.core_log = ReadFirstLine(run_root / "current_core_engine_log");
    status.supervisor_pids = FindSupervisorPids();

    if (!status.pid.empty() && ProcessIsAlive(status.pid)) {
        status.status = "alive";
        status.healthy = true;
        return status;
    }
    if (!status.pid.empty()) {
        status.status = "dead";
        status.healthy = false;
        return status;
    }
    if (!status.supervisor_pids.empty() && status.run_dir.empty()) {
        status.status = "waiting_for_trading_window";
        status.healthy = true;
        return status;
    }
    status.status = "missing_pid";
    status.healthy = false;
    return status;
}

std::vector<MarketFileStatus> CollectMarketStatus(const DashboardOptions& options) {
    auto discovered = DiscoverMarketFiles(options.market_data_dir);
    for (auto& [product, status] : discovered) {
        (void)product;
        if (!status.tick_file.empty()) {
            status.tick_age_seconds = FileAgeSeconds(status.tick_file);
            status.tick_status = StatusForAge(status.tick_age_seconds, kTickStaleSeconds);
            status.tick = ParseTick(status.tick_file);
        }
        if (!status.bar_file.empty()) {
            status.bar_age_seconds = FileAgeSeconds(status.bar_file);
            status.bar_status = StatusForAge(status.bar_age_seconds, kBarStaleSeconds);
            status.bar = ParseBar(status.bar_file);
        }
        status.healthy = status.tick_status == "fresh" && status.bar_status == "fresh";
    }
    std::vector<MarketFileStatus> markets;
    for (auto& [_, status] : discovered) {
        markets.push_back(std::move(status));
    }
    return markets;
}

WalStatus CollectWalStatus(const DashboardOptions& options) {
    WalStatus status;
    status.path = options.wal_file;
    std::ifstream input(options.wal_file);
    if (!input.is_open()) {
        status.exists = false;
        return status;
    }
    status.exists = true;
    std::string line;
    while (std::getline(input, line)) {
        if (line.empty()) {
            continue;
        }
        ++status.lines_total;
        if (line.find("\"event_type\":\"order_update\"") != std::string::npos ||
            line.find("\"kind\":\"order\"") != std::string::npos) {
            ++status.order_events;
        }
        if (line.find("\"event_type\":\"trade_fill\"") != std::string::npos ||
            line.find("\"kind\":\"trade\"") != std::string::npos) {
            ++status.trade_or_fill_events;
        }
    }
    return status;
}

DailyStatus CollectDailyStatus(const DashboardOptions& options) {
    DailyStatus status;
    const auto latest_report_day = LatestDirectoryName(options.report_root);
    const auto latest_export_day = LatestDirectoryName(options.export_root);
    if (latest_report_day.has_value()) {
        status.trading_day = *latest_report_day;
    } else if (latest_export_day.has_value()) {
        status.trading_day = *latest_export_day;
    }
    if (status.trading_day.empty()) {
        return status;
    }

    const fs::path report_dir = fs::path(options.report_root) / status.trading_day;
    const fs::path export_dir = fs::path(options.export_root) / status.trading_day;
    const fs::path daily_report = report_dir / "simnow_daily_report.json";
    const fs::path export_summary = export_dir / "summary.json";
    const fs::path health_report = report_dir / "ops_health_report.json";
    const fs::path alert_report = report_dir / "ops_alert_report.json";

    status.report_path = daily_report.string();
    status.export_summary_path = export_summary.string();
    status.health_report_path = health_report.string();
    status.alert_report_path = alert_report.string();

    if (const auto payload = ReadTextFile(daily_report); payload.has_value()) {
        status.report_found = true;
        status.tick_rows = ExtractJsonInt(*payload, "tick_rows").value_or(0);
        status.bar_rows = ExtractJsonInt(*payload, "bar_rows").value_or(0);
        status.wal_order_events = ExtractJsonInt(*payload, "wal_order_events").value_or(0);
        status.wal_trade_events = ExtractJsonInt(*payload, "wal_trade_events").value_or(0);
    }
    if (const auto payload = ReadTextFile(export_summary); payload.has_value()) {
        status.export_found = true;
        status.exported_order_events = ExtractJsonInt(*payload, "order_events").value_or(0);
        status.exported_trade_fills = ExtractJsonInt(*payload, "trade_fills").value_or(0);
        status.parse_errors = ExtractJsonInt(*payload, "parse_errors").value_or(0);
        status.wal_missing_in_export = ExtractJsonBool(*payload, "wal_missing").value_or(false);
    }
    status.order_csv_rows = CountCsvDataRows(export_dir / "orders.csv");
    status.fill_csv_rows = CountCsvDataRows(export_dir / "trade_fills.csv");
    if (const auto payload = ReadTextFile(health_report); payload.has_value()) {
        status.health_report_found = true;
        status.ops_overall_healthy = ExtractJsonBool(*payload, "overall_healthy");
    }
    if (const auto payload = ReadTextFile(alert_report); payload.has_value()) {
        status.alert_report_found = true;
        status.critical_alerts = CountOccurrences(*payload, "\"severity\": \"critical\"") +
                                 CountOccurrences(*payload, "\"severity\":\"critical\"");
        status.warn_alerts = CountOccurrences(*payload, "\"severity\": \"warn\"") +
                             CountOccurrences(*payload, "\"severity\":\"warn\"");
        status.info_alerts = CountOccurrences(*payload, "\"severity\": \"info\"") +
                             CountOccurrences(*payload, "\"severity\":\"info\"");
    }
    return status;
}

std::string RedactAfterKey(std::string line, const std::string& key) {
    std::size_t pos = 0;
    while ((pos = line.find(key, pos)) != std::string::npos) {
        const std::size_t value_start = pos + key.size();
        const auto value_end = line.find('"', value_start);
        if (value_end != std::string::npos) {
            line.replace(value_start, value_end - value_start, "<redacted>");
            pos = value_start + 10;
            continue;
        }
        pos = value_start;
    }
    return line;
}

std::string SanitizeLogLine(std::string line) {
    const std::vector<std::string> quoted_keys = {
        "account_id=\"",    "investor_id=\"",  "user_id=\"",
        "balance=\"",       "available=\"",    "curr_margin=\"",
        "frozen_margin=\"", "close_profit=\"", "position_profit=\""};
    for (const auto& key : quoted_keys) {
        line = RedactAfterKey(std::move(line), key);
    }
    return line;
}

std::vector<std::string> CollectRecentAlerts(const std::string& core_log) {
    std::vector<std::string> alerts;
    if (core_log.empty()) {
        alerts.emplace_back("core log is unavailable");
        return alerts;
    }
    std::ifstream input(core_log);
    if (!input.is_open()) {
        alerts.emplace_back("core log is unavailable");
        return alerts;
    }
    const std::vector<std::string> needles = {
        "level=warn", "level=error",     "reject",       "timeout",          "disconnect",
        "critical",   "client_order_id", "order_insert", "order_event",      "order_intent",
        "onrtntrade", "filled_volume",   "trade_id",     "partially_filled", "filled"};
    std::string line;
    while (std::getline(input, line)) {
        const std::string lower = ToLower(line);
        const bool matched = std::any_of(needles.begin(), needles.end(), [&](const auto& needle) {
            return lower.find(needle) != std::string::npos;
        });
        if (!matched) {
            continue;
        }
        alerts.push_back(SanitizeLogLine(line));
        if (alerts.size() > kRecentAlertLimit) {
            alerts.erase(alerts.begin());
        }
    }
    if (alerts.empty()) {
        alerts.emplace_back("no recent alert lines matched");
    }
    return alerts;
}

std::string JsonString(const std::string& value) {
    return "\"" + quant_hft::apps::JsonEscape(value) + "\"";
}

std::string JsonOptionalInt(const std::optional<std::int64_t>& value) {
    return value.has_value() ? std::to_string(*value) : "null";
}

std::string JsonOptionalBool(const std::optional<bool>& value) {
    if (!value.has_value()) {
        return "null";
    }
    return *value ? "true" : "false";
}

std::string RenderStateJson(const DashboardState& state) {
    std::ostringstream out;
    out << "{\n";
    out << "  \"generated_ts_ns\": " << state.generated_ts_ns << ",\n";
    out << "  \"generated_at_local\": " << JsonString(state.generated_at_local) << ",\n";
    out << "  \"overall_healthy\": " << (state.overall_healthy ? "true" : "false") << ",\n";
    out << "  \"warning_count\": " << state.warning_count << ",\n";
    out << "  \"paths\": {\n";
    std::size_t path_index = 0;
    for (const auto& [key, value] : state.paths) {
        out << "    " << JsonString(key) << ": " << JsonString(value);
        if (++path_index < state.paths.size()) {
            out << ',';
        }
        out << "\n";
    }
    out << "  },\n";

    out << "  \"process\": {\n";
    out << "    \"status\": " << JsonString(state.process.status) << ",\n";
    out << "    \"healthy\": " << (state.process.healthy ? "true" : "false") << ",\n";
    out << "    \"pid\": " << JsonString(state.process.pid) << ",\n";
    out << "    \"run_dir\": " << JsonString(state.process.run_dir) << ",\n";
    out << "    \"core_log\": " << JsonString(state.process.core_log) << ",\n";
    out << "    \"supervisor_pids\": [";
    for (std::size_t i = 0; i < state.process.supervisor_pids.size(); ++i) {
        if (i > 0) {
            out << ", ";
        }
        out << JsonString(state.process.supervisor_pids[i]);
    }
    out << "]\n";
    out << "  },\n";

    out << "  \"contracts\": [\n";
    for (std::size_t i = 0; i < state.contracts.size(); ++i) {
        const auto& item = state.contracts[i];
        out << "    {\"product\": " << JsonString(item.product)
            << ", \"instrument_id\": " << JsonString(item.instrument_id)
            << ", \"exchange_id\": " << JsonString(item.exchange_id)
            << ", \"selection_metric\": " << JsonString(item.selection_metric)
            << ", \"path\": " << JsonString(item.path) << "}";
        if (i + 1 < state.contracts.size()) {
            out << ',';
        }
        out << "\n";
    }
    out << "  ],\n";

    out << "  \"market\": [\n";
    for (std::size_t i = 0; i < state.markets.size(); ++i) {
        const auto& item = state.markets[i];
        out << "    {\n";
        out << "      \"product\": " << JsonString(item.product) << ",\n";
        out << "      \"trading_day\": " << JsonString(item.trading_day) << ",\n";
        out << "      \"healthy\": " << (item.healthy ? "true" : "false") << ",\n";
        out << "      \"tick_status\": " << JsonString(item.tick_status) << ",\n";
        out << "      \"bar_status\": " << JsonString(item.bar_status) << ",\n";
        out << "      \"tick_age_seconds\": " << JsonOptionalInt(item.tick_age_seconds) << ",\n";
        out << "      \"bar_age_seconds\": " << JsonOptionalInt(item.bar_age_seconds) << ",\n";
        out << "      \"tick_file\": " << JsonString(item.tick_file) << ",\n";
        out << "      \"bar_file\": " << JsonString(item.bar_file) << ",\n";
        out << "      \"tick\": {\"instrument_id\": " << JsonString(item.tick.instrument_id)
            << ", \"exchange_id\": " << JsonString(item.tick.exchange_id)
            << ", \"trading_day\": " << JsonString(item.tick.trading_day)
            << ", \"action_day\": " << JsonString(item.tick.action_day)
            << ", \"update_time\": " << JsonString(item.tick.update_time)
            << ", \"last_price\": " << JsonString(item.tick.last_price)
            << ", \"bid_price_1\": " << JsonString(item.tick.bid_price_1)
            << ", \"ask_price_1\": " << JsonString(item.tick.ask_price_1)
            << ", \"volume\": " << JsonString(item.tick.volume)
            << ", \"open_interest\": " << JsonString(item.tick.open_interest) << "},\n";
        out << "      \"bar\": {\"instrument_id\": " << JsonString(item.bar.instrument_id)
            << ", \"exchange_id\": " << JsonString(item.bar.exchange_id)
            << ", \"trading_day\": " << JsonString(item.bar.trading_day)
            << ", \"action_day\": " << JsonString(item.bar.action_day)
            << ", \"minute\": " << JsonString(item.bar.minute)
            << ", \"open\": " << JsonString(item.bar.open)
            << ", \"high\": " << JsonString(item.bar.high)
            << ", \"low\": " << JsonString(item.bar.low)
            << ", \"close\": " << JsonString(item.bar.close)
            << ", \"volume\": " << JsonString(item.bar.volume) << "}\n";
        out << "    }";
        if (i + 1 < state.markets.size()) {
            out << ',';
        }
        out << "\n";
    }
    out << "  ],\n";

    out << "  \"wal\": {\n";
    out << "    \"path\": " << JsonString(state.wal.path) << ",\n";
    out << "    \"exists\": " << (state.wal.exists ? "true" : "false") << ",\n";
    out << "    \"lines_total\": " << state.wal.lines_total << ",\n";
    out << "    \"wal_order_events\": " << state.wal.order_events << ",\n";
    out << "    \"wal_trade_or_fill_events\": " << state.wal.trade_or_fill_events << "\n";
    out << "  },\n";

    out << "  \"daily_report\": {\n";
    out << "    \"trading_day\": " << JsonString(state.daily.trading_day) << ",\n";
    out << "    \"report_found\": " << (state.daily.report_found ? "true" : "false") << ",\n";
    out << "    \"export_found\": " << (state.daily.export_found ? "true" : "false") << ",\n";
    out << "    \"health_report_found\": " << (state.daily.health_report_found ? "true" : "false")
        << ",\n";
    out << "    \"alert_report_found\": " << (state.daily.alert_report_found ? "true" : "false")
        << ",\n";
    out << "    \"tick_rows\": " << state.daily.tick_rows << ",\n";
    out << "    \"bar_rows\": " << state.daily.bar_rows << ",\n";
    out << "    \"wal_order_events\": " << state.daily.wal_order_events << ",\n";
    out << "    \"wal_trade_events\": " << state.daily.wal_trade_events << ",\n";
    out << "    \"exported_order_events\": " << state.daily.exported_order_events << ",\n";
    out << "    \"exported_trade_fills\": " << state.daily.exported_trade_fills << ",\n";
    out << "    \"parse_errors\": " << state.daily.parse_errors << ",\n";
    out << "    \"wal_missing_in_export\": "
        << (state.daily.wal_missing_in_export ? "true" : "false") << ",\n";
    out << "    \"order_csv_rows\": " << state.daily.order_csv_rows << ",\n";
    out << "    \"fill_csv_rows\": " << state.daily.fill_csv_rows << ",\n";
    out << "    \"ops_overall_healthy\": " << JsonOptionalBool(state.daily.ops_overall_healthy)
        << ",\n";
    out << "    \"critical_alerts\": " << state.daily.critical_alerts << ",\n";
    out << "    \"warn_alerts\": " << state.daily.warn_alerts << ",\n";
    out << "    \"info_alerts\": " << state.daily.info_alerts << ",\n";
    out << "    \"report_path\": " << JsonString(state.daily.report_path) << ",\n";
    out << "    \"export_summary_path\": " << JsonString(state.daily.export_summary_path) << ",\n";
    out << "    \"health_report_path\": " << JsonString(state.daily.health_report_path) << ",\n";
    out << "    \"alert_report_path\": " << JsonString(state.daily.alert_report_path) << "\n";
    out << "  },\n";

    out << "  \"recent_alerts\": [\n";
    for (std::size_t i = 0; i < state.recent_alerts.size(); ++i) {
        out << "    " << JsonString(state.recent_alerts[i]);
        if (i + 1 < state.recent_alerts.size()) {
            out << ',';
        }
        out << "\n";
    }
    out << "  ]\n";
    out << "}\n";
    return out.str();
}

std::string HtmlEscape(const std::string& text) {
    std::ostringstream out;
    for (const char ch : text) {
        switch (ch) {
            case '&':
                out << "&amp;";
                break;
            case '<':
                out << "&lt;";
                break;
            case '>':
                out << "&gt;";
                break;
            case '"':
                out << "&quot;";
                break;
            default:
                out << ch;
                break;
        }
    }
    return out.str();
}

std::string EmptyAsDash(const std::string& value) { return value.empty() ? "-" : value; }

std::string BadgeClass(const std::string& status, bool healthy) {
    if (healthy || status == "fresh" || status == "alive") {
        return "ok";
    }
    if (status == "missing" || status == "missing_pid" || status == "dead" || status == "stale") {
        return "bad";
    }
    return "warn";
}

std::string RenderBadge(const std::string& status, bool healthy = false) {
    return "<span class=\"badge " + BadgeClass(status, healthy) + "\">" + HtmlEscape(status) +
           "</span>";
}

std::string RenderMetric(const std::string& label, const std::string& value) {
    return "<div class=\"metric\"><div class=\"metric-label\">" + HtmlEscape(label) +
           "</div><div class=\"metric-value\">" + HtmlEscape(EmptyAsDash(value)) + "</div></div>";
}

std::string RenderMetric(const std::string& label, std::int64_t value) {
    return RenderMetric(label, std::to_string(value));
}

std::string RenderPathTable(const std::map<std::string, std::string>& paths) {
    std::ostringstream out;
    out << "<table><thead><tr><th>Name</th><th>Path</th></tr></thead><tbody>";
    for (const auto& [key, value] : paths) {
        out << "<tr><td>" << HtmlEscape(key) << "</td><td><code>" << HtmlEscape(value)
            << "</code></td></tr>";
    }
    out << "</tbody></table>";
    return out.str();
}

std::string RenderHtml(const DashboardState& state) {
    std::ostringstream html;
    html << "<!doctype html>\n<html lang=\"en\">\n<head>\n";
    html << "<meta charset=\"utf-8\">\n";
    html << "<meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">\n";
    html << "<meta http-equiv=\"refresh\" content=\"30\">\n";
    html << "<title>SimNow Dashboard</title>\n";
    html << "<style>\n";
    html << ":root{color-scheme:light;--paper:#f4efe6;--surface:#fbf8f1;--surface-2:#efe7da;"
            "--ink:#201f1a;--muted:#675f55;--line:#d8cdbc;--line-strong:#292822;"
            "--ok:#28724f;--bad:#b24432;--warn:#8a5f16;--blue:#405f8d;--soft:#f7f1e8;}\n";
    html << "*{box-sizing:border-box}body{margin:0;background:var(--paper);color:var(--ink);"
            "font:13px/1.35 Inter,ui-sans-serif,system-ui,-apple-system,BlinkMacSystemFont,"
            "\"Segoe UI\",sans-serif;}\n";
    html << ".topbar{height:58px;padding:0 28px;border-bottom:1px solid var(--line-strong);"
            "background:var(--paper);display:flex;align-items:center;justify-content:space-between;"
            "gap:16px;}"
            ".brand{display:flex;align-items:center;gap:10px;font-weight:700;}"
            ".brand-mark{width:18px;height:18px;border:1px solid "
            "var(--line-strong);background:var(--ink);"
            "box-shadow:6px 6px 0 var(--surface-2);}"
            ".nav{display:flex;gap:18px;align-items:center;color:var(--muted);font-size:12px;}"
            ".nav span{white-space:nowrap;}\n";
    html << ".hero{border-bottom:1px solid var(--line-strong);padding:24px 0 "
            "18px;margin-bottom:14px;"
            "display:grid;grid-template-columns:minmax(0,1fr) auto;gap:24px;align-items:end;}"
            ".eyebrow{color:var(--muted);font:700 12px/1.2 "
            "ui-monospace,SFMono-Regular,Menlo,monospace;"
            "text-transform:uppercase;}"
            "h1{font-family:Georgia,\"Times New "
            "Roman\",serif;font-size:40px;line-height:1.04;margin:7px 0 0;"
            "font-weight:500;letter-spacing:0;max-width:920px;}"
            "h2{font-size:13px;margin:0 0 "
            "10px;font-weight:800;letter-spacing:0;text-transform:uppercase;"
            "font-family:ui-monospace,SFMono-Regular,Menlo,monospace;color:var(--muted);}\n";
    html << "main{max-width:1520px;margin:0 auto;padding:0 20px 30px;}"
            ".grid{display:grid;grid-template-columns:repeat(12,1fr);gap:10px;}"
            ".panel{background:var(--surface);border:1px solid var(--line-strong);border-radius:0;"
            "padding:12px;min-width:0;box-shadow:4px 4px 0 var(--surface-2);}"
            ".span-3{grid-column:span 3}.span-4{grid-column:span 4}.span-5{grid-column:span 5}"
            ".span-6{grid-column:span 6}.span-7{grid-column:span 7}.span-8{grid-column:span 8}"
            ".span-12{grid-column:span 12}\n";
    html
        << ".badge{display:inline-flex;align-items:center;min-height:20px;border-radius:0;"
           "padding:1px 7px;font-weight:800;font-size:10px;line-height:1.1;border:1px solid "
           "currentColor;"
           "background:transparent;font-family:ui-monospace,SFMono-Regular,Menlo,monospace;}"
           ".badge.ok{color:var(--ok)}.badge.bad{color:var(--bad)}.badge.warn{color:var(--warn)}\n";
    html << ".metrics{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:0;border-top:"
            "1px solid "
            "var(--line);border-left:1px solid var(--line);}"
            ".metric{background:transparent;border-right:1px solid var(--line);border-bottom:1px "
            "solid var(--line);"
            "padding:9px 10px;min-height:58px;min-width:0;}"
            ".metric-label{color:var(--muted);font:700 10px/1.15 "
            "ui-monospace,SFMono-Regular,Menlo,monospace;"
            "text-transform:uppercase;margin-bottom:6px;white-space:nowrap;overflow:hidden;"
            "text-overflow:ellipsis;}"
            ".metric-value{font-size:16px;line-height:1.18;font-weight:740;white-space:nowrap;"
            "overflow:hidden;text-overflow:ellipsis;}\n";
    html << "table{width:100%;border-collapse:collapse;table-layout:fixed;border-top:1px solid "
            "var(--line);"
            "border-left:1px solid var(--line);}th,td{padding:7px 8px;border-right:1px solid "
            "var(--line);"
            "border-bottom:1px solid "
            "var(--line);text-align:left;vertical-align:top;white-space:nowrap;overflow:hidden;"
            "text-overflow:ellipsis;}"
            "th{font:800 10px/1.15 "
            "ui-monospace,SFMono-Regular,Menlo,monospace;text-transform:uppercase;"
            "color:var(--muted);background:var(--soft);}"
            "td{background:#fffaf2;}"
            "code{font-family:ui-monospace,SFMono-Regular,Menlo,monospace;font-size:11px;}"
            ".logs{margin:0;padding:0;list-style:none;display:grid;gap:7px;}"
            ".logs li{background:#fffaf2;border:1px solid var(--line);border-radius:0;padding:9px "
            "10px;"
            "font-family:ui-monospace,SFMono-Regular,Menlo,monospace;font-size:12px;overflow-wrap:"
            "anywhere;}"
            ".subtle{color:var(--muted);white-space:nowrap}.hero-meta{display:grid;gap:8px;justify-"
            "items:end;}"
            ".hero-meta .subtle{text-align:right;max-width:320px;}.section-note{color:var(--muted);"
            "margin:-4px 0 10px;font-size:12px;}\n";
    html
        << "@media(max-width:980px){.span-3,.span-4,.span-5,.span-6,.span-7,.span-8{grid-column:"
           "span 12}"
           ".metrics{grid-template-columns:repeat(2,minmax(0,1fr));}.topbar,main{padding-left:14px;"
           "padding-right:14px;}.hero{grid-template-columns:1fr;}.hero-meta{justify-items:start;}"
           ".hero-meta .subtle{text-align:left;}h1{font-size:32px;}}"
           "@media(max-width:560px){.metrics{grid-template-columns:1fr;}.nav{display:none;}}\n";
    html << "</style>\n</head>\n<body>\n";

    html << "<header class=\"topbar\"><div class=\"brand\"><span class=\"brand-mark\"></span>"
            "<span>quant_hft</span></div><nav class=\"nav\"><span>Local HTML</span><span>No "
            "DB</span>"
            "<span>30s refresh</span></nav></header>\n";
    html << "<main><section class=\"hero\"><div><div class=\"eyebrow\">SimNow operations</div>"
            "<h1>Trading console for live status, market freshness, and audit evidence.</h1></div>"
            "<div class=\"hero-meta\">"
         << RenderBadge(state.overall_healthy ? "healthy" : "unhealthy", state.overall_healthy)
         << "<div class=\"subtle\">Generated " << HtmlEscape(state.generated_at_local)
         << "</div></div></section><div class=\"grid\">\n";

    html << "<section class=\"panel span-4\"><h2>Runtime</h2><div class=\"metrics\">";
    html << RenderMetric("core_engine", state.process.status);
    html << RenderMetric("pid", state.process.pid);
    html << RenderMetric("supervisor",
                         state.process.supervisor_pids.empty()
                             ? "not_running"
                             : std::to_string(state.process.supervisor_pids.size()) + " running");
    html << RenderMetric("warnings", state.warning_count);
    html << "</div></section>\n";

    html << "<section class=\"panel span-4\"><h2>WAL</h2><div class=\"metrics\">";
    html << RenderMetric("file", state.wal.exists ? "present" : "missing");
    html << RenderMetric("lines", state.wal.lines_total);
    html << RenderMetric("orders", state.wal.order_events);
    html << RenderMetric("fills", state.wal.trade_or_fill_events);
    html << "</div></section>\n";

    html << "<section class=\"panel span-4\"><h2>Latest Day</h2><div class=\"metrics\">";
    html << RenderMetric("trading day", state.daily.trading_day);
    html << RenderMetric("report", state.daily.report_found ? "present" : "missing");
    html << RenderMetric("export orders", state.daily.exported_order_events);
    html << RenderMetric("export fills", state.daily.exported_trade_fills);
    html << "</div></section>\n";

    html << "<section class=\"panel span-7\"><h2>Market Freshness</h2>"
            "<div class=\"section-note\">Latest trading_day partitioned CSV observations by "
            "product.</div>";
    html << "<table><thead><tr><th>Product</th><th>Instrument</th><th>Tick</th><th>Bar</th>"
            "<th>Last</th><th>Bid / Ask</th><th>Bar Close</th></tr></thead><tbody>";
    if (state.markets.empty()) {
        html << "<tr><td colspan=\"7\">No market CSV files found</td></tr>";
    }
    for (const auto& item : state.markets) {
        html << "<tr><td>" << HtmlEscape(item.product) << "</td><td>"
             << HtmlEscape(EmptyAsDash(item.tick.instrument_id.empty() ? item.bar.instrument_id
                                                                       : item.tick.instrument_id))
             << "</td><td>" << RenderBadge(item.tick_status, item.tick_status == "fresh")
             << " <span class=\"subtle\">"
             << (item.tick_age_seconds.has_value() ? std::to_string(*item.tick_age_seconds) + "s"
                                                   : "n/a")
             << "</span></td><td>" << RenderBadge(item.bar_status, item.bar_status == "fresh")
             << " <span class=\"subtle\">"
             << (item.bar_age_seconds.has_value() ? std::to_string(*item.bar_age_seconds) + "s"
                                                  : "n/a")
             << "</span></td><td>" << HtmlEscape(EmptyAsDash(item.tick.last_price)) << "</td><td>"
             << HtmlEscape(EmptyAsDash(item.tick.bid_price_1)) << " / "
             << HtmlEscape(EmptyAsDash(item.tick.ask_price_1)) << "</td><td>"
             << HtmlEscape(EmptyAsDash(item.bar.close)) << " <span class=\"subtle\">"
             << HtmlEscape(EmptyAsDash(item.bar.minute)) << "</span></td></tr>";
    }
    html << "</tbody></table></section>\n";

    html << "<section class=\"panel span-5\"><h2>Dominant Contracts</h2>";
    html << "<table><thead><tr><th>Product</th><th>Instrument</th><th>Exchange</th><th>Metric</"
            "th></tr></thead><tbody>";
    if (state.contracts.empty()) {
        html << "<tr><td colspan=\"4\">No dominant contract files found</td></tr>";
    }
    for (const auto& item : state.contracts) {
        html << "<tr><td>" << HtmlEscape(item.product) << "</td><td>"
             << HtmlEscape(EmptyAsDash(item.instrument_id)) << "</td><td>"
             << HtmlEscape(EmptyAsDash(item.exchange_id)) << "</td><td>"
             << HtmlEscape(EmptyAsDash(item.selection_metric)) << "</td></tr>";
    }
    html << "</tbody></table></section>\n";

    html << "<section class=\"panel span-6\"><h2>Orders And Fills</h2><div class=\"metrics\">";
    html << RenderMetric("WAL orders", state.wal.order_events);
    html << RenderMetric("WAL fills", state.wal.trade_or_fill_events);
    html << RenderMetric("CSV orders", state.daily.order_csv_rows);
    html << RenderMetric("CSV fills", state.daily.fill_csv_rows);
    html << "</div></section>\n";

    html << "<section class=\"panel span-6\"><h2>Ops Reports</h2><div class=\"metrics\">";
    html << RenderMetric("ops healthy", state.daily.ops_overall_healthy.has_value()
                                            ? (*state.daily.ops_overall_healthy ? "true" : "false")
                                            : "unknown");
    html << RenderMetric("critical alerts", state.daily.critical_alerts);
    html << RenderMetric("warn alerts", state.daily.warn_alerts);
    html << RenderMetric("parse errors", state.daily.parse_errors);
    html << "</div></section>\n";

    html << "<section class=\"panel span-12\"><h2>Recent Alerts</h2><ul class=\"logs\">";
    for (const auto& line : state.recent_alerts) {
        html << "<li>" << HtmlEscape(line) << "</li>";
    }
    html << "</ul></section>\n";

    html << "<section class=\"panel span-12\"><h2>Paths</h2>" << RenderPathTable(state.paths)
         << "</section>\n";

    html << "</div></main>\n</body>\n</html>\n";
    return html.str();
}

DashboardOptions ParseOptions(int argc, char** argv, std::string* error) {
    const auto args = quant_hft::apps::ParseArgs(argc, argv);
    DashboardOptions options;
    options.run_root = quant_hft::apps::GetArg(args, "run-root",
                                               DefaultPathFromRoot("runtime/trading/runs/simnow"));
    options.market_data_dir = quant_hft::apps::GetArg(
        args, "market-data-dir", DefaultPathFromRoot("runtime/market_data/simnow"));
    options.wal_file = quant_hft::apps::GetArg(
        args, "wal-file", DefaultPathFromRoot("runtime/trading/wal/simnow/events.wal"));
    options.report_root = quant_hft::apps::GetArg(
        args, "report-root", DefaultPathFromRoot("runtime/trading/reports/simnow"));
    options.export_root = quant_hft::apps::GetArg(
        args, "export-root", DefaultPathFromRoot("runtime/trading/exports/simnow"));
    options.output_dir = quant_hft::apps::GetArg(
        args, "output-dir", DefaultPathFromRoot("runtime/trading/dashboard/simnow"));
    options.strict_exit = ParseBoolArg(args, "strict-exit", false);
    const std::string watch = quant_hft::apps::GetArg(args, "watch-seconds", "0");
    if (!ParseNonNegativeInt(watch, &options.watch_seconds)) {
        if (error != nullptr) {
            *error = "--watch-seconds must be a non-negative integer";
        }
    }
    return options;
}

DashboardState CollectState(const DashboardOptions& options) {
    DashboardState state;
    state.generated_ts_ns = UnixEpochNanosNow();
    state.generated_at_local = FormatLocalTime(state.generated_ts_ns);
    state.paths = {
        {"run_root", options.run_root},       {"market_data_dir", options.market_data_dir},
        {"wal_file", options.wal_file},       {"report_root", options.report_root},
        {"export_root", options.export_root}, {"output_dir", options.output_dir},
    };

    state.process = CollectProcessStatus(options);
    state.contracts = DiscoverContracts();
    state.markets = CollectMarketStatus(options);
    state.wal = CollectWalStatus(options);
    state.daily = CollectDailyStatus(options);
    state.recent_alerts = CollectRecentAlerts(state.process.core_log);

    bool market_healthy = true;
    if (!state.markets.empty()) {
        market_healthy = std::all_of(state.markets.begin(), state.markets.end(),
                                     [](const auto& item) { return item.healthy; });
    }
    state.overall_healthy = state.process.healthy && market_healthy;

    if (!state.process.supervisor_pids.empty() && state.process.status != "alive") {
        ++state.warning_count;
    }
    if (!state.wal.exists) {
        ++state.warning_count;
    }
    if (!state.daily.report_found && !state.daily.trading_day.empty()) {
        ++state.warning_count;
    }
    if (state.daily.parse_errors > 0) {
        ++state.warning_count;
    }
    for (const auto& item : state.markets) {
        if (!item.healthy) {
            ++state.warning_count;
        }
    }
    return state;
}

bool WriteDashboard(const DashboardOptions& options, const DashboardState& state,
                    std::string* error) {
    const fs::path output_dir(options.output_dir);
    return quant_hft::apps::WriteTextFile((output_dir / "dashboard_state.json").string(),
                                          RenderStateJson(state), error) &&
           quant_hft::apps::WriteTextFile((output_dir / "index.html").string(), RenderHtml(state),
                                          error);
}

}  // namespace

int main(int argc, char** argv) {
    std::string error;
    const DashboardOptions options = ParseOptions(argc, argv, &error);
    if (!error.empty()) {
        std::cerr << "simnow_dashboard_cli: " << error << '\n';
        return 2;
    }

    while (true) {
        const DashboardState state = CollectState(options);
        if (!WriteDashboard(options, state, &error)) {
            std::cerr << "simnow_dashboard_cli: " << error << '\n';
            return 1;
        }
        std::cout << "wrote " << (fs::path(options.output_dir) / "index.html").string()
                  << " overall_healthy=" << (state.overall_healthy ? "true" : "false") << '\n';

        if (options.watch_seconds == 0) {
            return options.strict_exit && !state.overall_healthy ? 2 : 0;
        }
        std::this_thread::sleep_for(std::chrono::seconds(options.watch_seconds));
    }
}
