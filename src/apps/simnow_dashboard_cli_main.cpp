#include <unistd.h>

#include <algorithm>
#include <cctype>
#include <chrono>
#include <cmath>
#include <csignal>
#include <cstdint>
#include <cstdlib>
#include <ctime>
#include <filesystem>
#include <fstream>
#include <functional>
#include <iomanip>
#include <iostream>
#include <map>
#include <optional>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include "quant_hft/apps/cli_support.h"
#include "quant_hft/core/ctp_text.h"

namespace {

namespace fs = std::filesystem;

constexpr std::int64_t kTickStaleSeconds = 180;
constexpr std::int64_t kBarStaleSeconds = 240;
constexpr int kCommodityMorningBreakMinute = 10 * 60 + 15;
constexpr int kCommodityLunchBreakMinute = 11 * 60 + 30;
constexpr int kCommodityDayCloseMinute = 15 * 60;
constexpr int kCommodityPostDayCloseLatestMinute = 15 * 60 + 30;
constexpr std::int64_t kCommodityMorningBreakSeconds = 15 * 60;
constexpr std::int64_t kCommodityLunchBreakSeconds = 2 * 60 * 60;
constexpr std::int64_t kCommodityDayCloseSeconds = 5 * 60 * 60 + 45 * 60;
constexpr std::size_t kRecentAlertLimit = 24;
constexpr std::size_t kRecentSignalMonitorLimit = 12;
constexpr std::size_t kRecentCtpEventLimit = 16;
constexpr std::size_t kHistoricalTradeFillIndex = static_cast<std::size_t>(-1);

struct DashboardOptions {
    std::string run_root;
    std::string market_data_dir;
    std::string wal_file;
    std::string report_root;
    std::string export_root;
    std::string monitor_root;
    std::string readiness_file;
    std::string ctp_instrument_dir;
    std::string probe_log_dir;
    std::string state_dir;
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
    std::string candidate_instrument_id;
    std::string exchange_id;
    std::string trading_day;
    std::string cache_trading_day;
    std::string phase{"legacy"};
    std::string selection_metric;
    std::string last_error;
    double lead_ratio{0.0};
    std::int64_t generation{0};
    std::int64_t eligible_count{0};
    std::int64_t baseline_count{0};
    std::int64_t broker_position{0};
    std::int64_t broker_frozen{0};
    std::int64_t active_open_orders{0};
    std::int64_t active_close_orders{0};
    std::int64_t warmup_observed_bars{0};
    std::int64_t warmup_required_bars{0};
    std::int64_t generation_rejections{0};
    std::optional<std::int64_t> phase_age_seconds;
    bool cache_trading_day_mismatch{false};
    bool healthy{false};
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

struct SignalMonitorStatus {
    std::string status{"missing"};
    bool healthy{false};
    std::string pid;
    std::string event_log_path;
    std::string incident_dir;
    std::string monitor_log_path;
    bool event_log_exists{false};
    std::optional<std::int64_t> event_log_age_seconds;
    std::int64_t signals{0};
    std::int64_t active{0};
    std::int64_t filled{0};
    std::int64_t incidents{0};
    std::string last_event;
    std::string last_message;
    std::vector<std::string> recent_events;
    std::vector<std::string> recent_incidents;
};

struct SignalMonitorEpoch {
    std::vector<std::string> lines;
    bool reset_by_core_engine{false};
};

struct SignalMonitorEpochStats {
    std::int64_t signals{0};
    std::int64_t active{0};
    std::int64_t filled{0};
    std::int64_t incidents{0};
    bool has_trace_events{false};
    bool has_summary{false};
    std::int64_t summary_signals{0};
    std::int64_t summary_active{0};
    std::int64_t summary_filled{0};
    std::int64_t summary_incidents{0};
    std::vector<std::string> incident_trace_ids;
};

struct CtpConnectionStatus {
    std::string status{"unknown"};
    bool healthy{false};
    std::string td_front{"unknown"};
    std::string md_front{"unknown"};
    std::string login_status{"unknown"};
    std::string auth_status{"unknown"};
    std::string settlement_status{"unknown"};
    std::string probe_status{"missing"};
    std::string latest_probe_log;
    std::optional<std::int64_t> probe_age_seconds;
    std::int64_t reconnect_attempts{0};
    std::int64_t ctp_errors{0};
    std::string last_error;
    std::vector<std::string> active_instruments;
    std::vector<std::string> recent_events;
};

struct ReadinessStatus {
    std::string path;
    std::string status{"missing"};
    bool found{false};
    bool fresh{false};
    bool healthy{false};
    std::optional<std::int64_t> age_seconds;
    std::string mode{"unknown"};
    std::int64_t generation{0};
    bool recovery_complete{false};
    bool trader_ready{false};
    bool gateway_healthy{false};
    bool settlement_confirmed{false};
    std::int64_t pending_exit_count{0};
    std::int64_t unresolved_mapping_count{0};
};

struct TradeFillDetail {
    std::string seq;
    std::string time;
    std::string ts_ns;
    std::string instrument_id;
    std::string exchange_id;
    std::string side;
    std::string offset;
    std::string volume;
    std::string price;
    std::string strategy_id;
    std::string client_order_id;
    std::string order_ref;
    std::string trade_id;
    std::string trace_id;
    std::string attribution;
    std::string trade_day_bucket;
    std::string replay_status{"unique"};
    std::int64_t replay_count{1};
};

struct CtpOrderFlowStatus {
    std::string status{"unknown"};
    bool healthy{false};
    std::int64_t signals{0};
    std::int64_t active{0};
    std::int64_t monitor_filled{0};
    std::int64_t monitor_incidents{0};
    std::int64_t order_submitted_logs{0};
    std::int64_t ctp_submitted{0};
    std::int64_t ctp_submit_rejected{0};
    std::int64_t ctp_callbacks{0};
    std::int64_t wal_rejected{0};
    std::int64_t wal_fills{0};
    std::int64_t wal_fills_raw{0};
    std::int64_t wal_duplicate_fills{0};
    std::string last_error_id;
    std::string last_reject_reason;
    std::vector<TradeFillDetail> trade_fills;
    std::vector<TradeFillDetail> replay_duplicate_fills;
    std::vector<std::string> recent_events;
    std::vector<std::string> recent_rejections;
    std::map<std::string, std::size_t> trade_fill_index_by_key;
    // Stores fill data from previous-run WAL records keyed by dedup key.
    // Used to recover the original exchange timestamp when the current run replays
    // the same trade_id via OnRtnTrade (whose ts_ns reflects recv time, not trade time).
    std::map<std::string, TradeFillDetail> historical_fill_by_key;
};

struct PositionRow {
    std::string instrument_id;
    std::int64_t net{0};
    std::string source;
    std::optional<double> avg_open;
    std::optional<double> initial_stop;
    std::optional<double> take_profit;
    std::optional<double> trailing_stop;
};

struct DashboardState {
    std::int64_t generated_ts_ns{0};
    std::string generated_at_local;
    bool overall_healthy{false};
    bool live_healthy{false};
    std::int64_t warning_count{0};
    std::int64_t live_warning_count{0};
    std::int64_t historical_risk_count{0};
    ProcessStatus process;
    std::vector<ContractStatus> contracts;
    std::vector<MarketFileStatus> markets;
    WalStatus wal;
    DailyStatus daily;
    SignalMonitorStatus signal_monitor;
    ReadinessStatus readiness;
    CtpConnectionStatus ctp_connection;
    CtpOrderFlowStatus ctp_order_flow;
    std::vector<PositionRow> positions;
    std::string positions_source;
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

std::string FormatLocalDateBucket(std::int64_t ts_ns) {
    if (ts_ns <= 0) {
        return "";
    }
    std::time_t seconds = static_cast<std::time_t>(ts_ns / 1'000'000'000LL);
    std::tm tm{};
#if defined(_WIN32)
    localtime_s(&tm, &seconds);
#else
    localtime_r(&seconds, &tm);
#endif
    char buffer[16] = {0};
    if (std::strftime(buffer, sizeof(buffer), "%Y%m%d", &tm) == 0) {
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

fs::path CanonicalOrAbsolute(const fs::path& path) {
    std::error_code ec;
    fs::path canonical = fs::weakly_canonical(path, ec);
    if (!ec) {
        return canonical;
    }
    return fs::absolute(path, ec);
}

std::vector<std::string> FindSupervisorPids(const std::string& run_root) {
    std::vector<std::string> pids;
    std::error_code ec;
    if (!fs::exists("/proc", ec)) {
        return pids;
    }
    const fs::path target_run_root = CanonicalOrAbsolute(run_root);
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
            const fs::path cwd = CanonicalOrAbsolute(entry.path() / "cwd");
            const fs::path default_run_root =
                CanonicalOrAbsolute(cwd / "runtime/trading/runs/simnow");
            if (text.find(run_root) == std::string::npos && default_run_root != target_run_root) {
                continue;
            }
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

std::optional<double> ExtractJsonDouble(const std::string& text, const std::string& key) {
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
    std::size_t parsed = 0;
    try {
        const double value = std::stod(text.substr(colon), &parsed);
        return parsed == 0 ? std::nullopt : std::optional<double>(value);
    } catch (...) {
        return std::nullopt;
    }
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

void PushLimited(std::vector<std::string>* items, std::string value, std::size_t limit) {
    if (items == nullptr || value.empty()) {
        return;
    }
    items->push_back(std::move(value));
    while (items->size() > limit) {
        items->erase(items->begin());
    }
}

std::string ExtractKvValue(const std::string& text, const std::string& key) {
    const std::string marker = key + "=";
    const auto pos = text.find(marker);
    if (pos == std::string::npos) {
        return "";
    }
    std::size_t value_start = pos + marker.size();
    while (value_start < text.size() &&
           std::isspace(static_cast<unsigned char>(text[value_start])) != 0) {
        ++value_start;
    }
    if (value_start >= text.size()) {
        return "";
    }
    if (text[value_start] == '"') {
        ++value_start;
        std::string value;
        bool escaped = false;
        for (std::size_t i = value_start; i < text.size(); ++i) {
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
        return value;
    }
    std::size_t value_end = value_start;
    while (value_end < text.size() &&
           std::isspace(static_cast<unsigned char>(text[value_end])) == 0) {
        ++value_end;
    }
    std::string value = text.substr(value_start, value_end - value_start);
    while (!value.empty() && (value.back() == ',' || value.back() == ';')) {
        value.pop_back();
    }
    return value;
}

std::string ExtractEventField(const std::string& line, const std::string& key) {
    std::string value = ExtractJsonString(line, key);
    if (!value.empty()) {
        return value;
    }
    if (const auto number = ExtractJsonInt(line, key); number.has_value()) {
        return std::to_string(*number);
    }
    return ExtractKvValue(line, key);
}

std::string SafeAscii(std::string text) {
    if (!quant_hft::ctp::IsValidUtf8(text)) {
        text = quant_hft::ctp::DecodeCtpText(text);
    }
    for (char& ch : text) {
        const unsigned char byte = static_cast<unsigned char>(ch);
        if (byte < 32 && byte != '\t') {
            ch = '?';
        }
    }
    return text;
}

bool HasAnyNeedle(const std::string& lower, const std::vector<std::string>& needles) {
    return std::any_of(needles.begin(), needles.end(),
                       [&](const auto& needle) { return lower.find(needle) != std::string::npos; });
}

std::string LatestRegularFile(const fs::path& root, const std::string& prefix,
                              const std::string& suffix) {
    std::error_code ec;
    if (!fs::exists(root, ec)) {
        return "";
    }
    fs::path latest;
    for (const auto& entry :
         fs::directory_iterator(root, fs::directory_options::skip_permission_denied, ec)) {
        if (ec) {
            break;
        }
        if (!entry.is_regular_file(ec)) {
            continue;
        }
        const std::string filename = entry.path().filename().string();
        if (!prefix.empty() && filename.rfind(prefix, 0) != 0) {
            continue;
        }
        if (!suffix.empty() && (filename.size() < suffix.size() ||
                                filename.substr(filename.size() - suffix.size()) != suffix)) {
            continue;
        }
        if (latest.empty()) {
            latest = entry.path();
            continue;
        }
        const auto latest_time = fs::last_write_time(latest, ec);
        if (ec) {
            latest = entry.path();
            continue;
        }
        const auto candidate_time = fs::last_write_time(entry.path(), ec);
        if (!ec && candidate_time > latest_time) {
            latest = entry.path();
        }
    }
    return latest.string();
}

std::string BriefStructuredEvent(const std::string& line, const std::string& source) {
    const std::string ts = ExtractEventField(line, "ts").empty() ? ExtractEventField(line, "ts_ns")
                                                                 : ExtractEventField(line, "ts");
    const std::string event = ExtractEventField(line, "event").empty()
                                  ? ExtractEventField(line, "event_type")
                                  : ExtractEventField(line, "event");
    const std::string trace_id = ExtractEventField(line, "trace_id");
    const std::string client_order_id = ExtractEventField(line, "client_order_id");
    const std::string order_ref = ExtractEventField(line, "order_ref");
    const std::string error_id = ExtractEventField(line, "error_id").empty()
                                     ? ExtractEventField(line, "ErrorID")
                                     : ExtractEventField(line, "error_id");
    const std::string message = ExtractEventField(line, "message").empty()
                                    ? ExtractEventField(line, "reason")
                                    : ExtractEventField(line, "message");
    std::string display_message = message;
    if (!error_id.empty()) {
        try {
            const std::string known = quant_hft::ctp::KnownCtpErrorMessage(std::stoi(error_id));
            if (!known.empty() && quant_hft::ctp::LooksLikePlaceholderText(display_message)) {
                display_message = known;
            }
        } catch (...) {
        }
    }
    std::ostringstream out;
    out << source;
    if (!ts.empty()) {
        out << ' ' << ts;
    }
    if (!event.empty()) {
        out << ' ' << event;
    }
    if (!trace_id.empty()) {
        out << " trace_id=" << trace_id;
    }
    if (!client_order_id.empty()) {
        out << " client_order_id=" << client_order_id;
    }
    if (!order_ref.empty()) {
        out << " order_ref=" << order_ref;
    }
    if (!error_id.empty()) {
        out << " error_id=" << error_id;
    }
    if (!display_message.empty()) {
        out << " - " << display_message;
    }
    return SafeAscii(out.str());
}

std::string ClassifyCtpIssue(const std::string& line) {
    const std::string lower = ToLower(line);
    if (lower.find("settlement_unconfirmed") != std::string::npos ||
        lower.find("errorid=42") != std::string::npos ||
        lower.find("error_id=42") != std::string::npos ||
        lower.find("\"error_id\":42") != std::string::npos ||
        lower.find("\"error_id\": 42") != std::string::npos) {
        return "settlement_unconfirmed";
    }
    if (lower.find("login") != std::string::npos &&
        HasAnyNeedle(lower, {"fail", "failed", "level=error", "reject"})) {
        return "login_failed";
    }
    if (lower.find("auth") != std::string::npos &&
        HasAnyNeedle(lower, {"fail", "failed", "level=error", "reject"})) {
        return "auth_failed";
    }
    if (lower.find("front_disconnected") != std::string::npos ||
        lower.find("onfrontdisconnected") != std::string::npos) {
        return "front_disconnected";
    }
    if (lower.find("reject") != std::string::npos || lower.find("rejected") != std::string::npos) {
        return "order_rejected";
    }
    if (lower.find("timeout") != std::string::npos) {
        return "timeout";
    }
    return "";
}

std::string RunIdFromRunDir(const std::string& run_dir) {
    if (run_dir.empty()) {
        return "";
    }
    return fs::path(run_dir).filename().string();
}

SignalMonitorEpoch CurrentSignalMonitorEpochFromLog(const std::string& event_log_path) {
    std::ifstream input(event_log_path);
    std::vector<std::string> all_lines;
    SignalMonitorEpoch epoch;
    std::string line;
    while (std::getline(input, line)) {
        if (line.empty()) {
            continue;
        }
        all_lines.push_back(line);
        const std::string event = ExtractJsonString(line, "event");
        if (event == "monitor_started" || event == "core_engine_running") {
            epoch.lines.clear();
            epoch.reset_by_core_engine = event == "core_engine_running";
        }
        epoch.lines.push_back(line);
    }
    if (epoch.lines.empty()) {
        epoch.lines = std::move(all_lines);
        epoch.reset_by_core_engine = false;
    }
    return epoch;
}

std::vector<std::string> CurrentSignalMonitorEpochLines(const std::string& event_log_path) {
    return CurrentSignalMonitorEpochFromLog(event_log_path).lines;
}

std::string ExtractSignalMonitorField(const std::string& line, const std::string& key) {
    std::string value = ExtractJsonString(line, key);
    if (!value.empty()) {
        return value;
    }
    if (const auto number = ExtractJsonInt(line, key); number.has_value()) {
        return std::to_string(*number);
    }
    return "";
}

bool IsPositiveIntegerText(const std::string& value) {
    if (value.empty()) {
        return false;
    }
    return std::all_of(value.begin(), value.end(),
                       [](unsigned char ch) { return std::isdigit(ch) != 0; }) &&
           value.find_first_not_of('0') != std::string::npos;
}

SignalMonitorEpochStats BuildSignalMonitorEpochStats(const SignalMonitorEpoch& epoch) {
    SignalMonitorEpochStats stats;
    std::map<std::string, std::string> trace_status;
    for (const std::string& line : epoch.lines) {
        const std::string event = ExtractJsonString(line, "event");
        const std::string trace_id = ExtractJsonString(line, "trace_id");
        if (event == "summary") {
            stats.has_summary = true;
            stats.summary_signals = ExtractJsonInt(line, "signals").value_or(stats.summary_signals);
            stats.summary_active = ExtractJsonInt(line, "active").value_or(stats.summary_active);
            stats.summary_filled = ExtractJsonInt(line, "filled").value_or(stats.summary_filled);
            stats.summary_incidents =
                ExtractJsonInt(line, "incidents").value_or(stats.summary_incidents);
        }
        if (trace_id.empty()) {
            continue;
        }
        stats.has_trace_events = true;
        auto& status = trace_status[trace_id];
        if (status.empty()) {
            status = "active";
        }

        if (event == "incident" || event == "order_rejected") {
            status = "incident";
            continue;
        }
        if (event == "trade_fill" || event == "wal_trade_fill") {
            status = "filled";
            continue;
        }
        if (event == "wal_order_update") {
            const std::string order_status = ExtractSignalMonitorField(line, "status");
            const std::string filled_volume = ExtractSignalMonitorField(line, "filled_volume");
            if ((order_status == "2" || order_status == "3") &&
                IsPositiveIntegerText(filled_volume)) {
                status = "filled";
            }
        }
    }

    if (!stats.has_trace_events) {
        if (stats.has_summary && !epoch.reset_by_core_engine) {
            stats.signals = stats.summary_signals;
            stats.active = stats.summary_active;
            stats.filled = stats.summary_filled;
            stats.incidents = stats.summary_incidents;
        }
        return stats;
    }

    for (const auto& [trace_id, status] : trace_status) {
        ++stats.signals;
        if (status == "filled") {
            ++stats.filled;
        } else if (status == "incident") {
            ++stats.incidents;
            stats.incident_trace_ids.push_back(trace_id);
        } else {
            ++stats.active;
        }
    }
    return stats;
}

bool SignalMonitorSummaryDiffers(const SignalMonitorEpochStats& stats) {
    return stats.has_summary &&
           (stats.signals != stats.summary_signals || stats.active != stats.summary_active ||
            stats.filled != stats.summary_filled || stats.incidents != stats.summary_incidents);
}

std::string SignalMonitorCountsMessage(const SignalMonitorEpochStats& stats) {
    std::ostringstream out;
    out << "current_epoch signals=" << stats.signals << " active=" << stats.active
        << " filled=" << stats.filled << " incidents=" << stats.incidents;
    return out.str();
}

std::string BriefSignalMonitorSummaryEvent(const std::string& line,
                                           const SignalMonitorEpochStats& stats) {
    const std::string ts = ExtractJsonString(line, "ts");
    std::ostringstream out;
    if (!ts.empty()) {
        out << ts << ' ';
    }
    out << "summary - " << SignalMonitorCountsMessage(stats);
    return out.str();
}

std::vector<ContractStatus> DiscoverContracts(const std::string& ctp_instrument_dir) {
    std::vector<ContractStatus> contracts;
    const fs::path root(ctp_instrument_dir);
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
        contract.instrument_id = ExtractJsonString(*payload, "current_instrument_id");
        if (contract.instrument_id.empty()) {
            contract.instrument_id = ExtractJsonString(*payload, "instrument_id");
        }
        contract.candidate_instrument_id = ExtractJsonString(*payload, "candidate_instrument_id");
        contract.exchange_id = ExtractJsonString(*payload, "exchange_id");
        contract.trading_day = ExtractJsonString(*payload, "trading_day");
        contract.phase = ExtractJsonString(*payload, "phase");
        if (contract.phase.empty()) {
            contract.phase = "legacy";
        }
        contract.selection_metric = ExtractJsonString(*payload, "selection_metric");
        contract.last_error = ExtractJsonString(*payload, "last_error");
        contract.lead_ratio = ExtractJsonDouble(*payload, "lead_ratio").value_or(0.0);
        contract.generation = ExtractJsonInt(*payload, "generation").value_or(0);
        contract.eligible_count = ExtractJsonInt(*payload, "eligible_count").value_or(0);
        contract.baseline_count = ExtractJsonInt(*payload, "baseline_count").value_or(0);
        contract.broker_position = ExtractJsonInt(*payload, "broker_position").value_or(0);
        contract.broker_frozen = ExtractJsonInt(*payload, "broker_frozen").value_or(0);
        contract.active_open_orders = ExtractJsonInt(*payload, "active_open_orders").value_or(0);
        contract.active_close_orders = ExtractJsonInt(*payload, "active_close_orders").value_or(0);
        contract.warmup_observed_bars =
            ExtractJsonInt(*payload, "warmup_observed_bars").value_or(0);
        contract.warmup_required_bars =
            ExtractJsonInt(*payload, "warmup_required_bars").value_or(0);
        contract.generation_rejections =
            ExtractJsonInt(*payload, "generation_rejections").value_or(0);
        const auto phase_started_ts_ns =
            ExtractJsonInt(*payload, "phase_started_ts_ns").value_or(0);
        if (phase_started_ts_ns > 0) {
            contract.phase_age_seconds = std::max<std::int64_t>(
                0, (UnixEpochNanosNow() - phase_started_ts_ns) / 1'000'000'000LL);
        }
        const fs::path cache_path = root / (contract.product + "_contracts.json");
        if (const auto cache_payload = ReadTextFile(cache_path); cache_payload.has_value()) {
            contract.cache_trading_day = ExtractJsonString(*cache_payload, "broker_trading_day");
        }
        contract.cache_trading_day_mismatch =
            !contract.trading_day.empty() && contract.cache_trading_day != contract.trading_day;
        const bool coverage_complete =
            contract.eligible_count > 0 && contract.baseline_count == contract.eligible_count;
        contract.healthy = !contract.instrument_id.empty() &&
                           (contract.phase == "ready" || contract.phase == "legacy") &&
                           !contract.cache_trading_day_mismatch &&
                           (contract.phase == "legacy" || coverage_complete) &&
                           contract.last_error.empty();
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

std::optional<int> ParseClockMinute(std::string value) {
    const std::size_t space_pos = value.find(' ');
    if (space_pos != std::string::npos) {
        value = value.substr(space_pos + 1);
    }
    if (value.size() < 5 || value[2] != ':') {
        return std::nullopt;
    }
    const auto is_digit = [](char ch) { return std::isdigit(static_cast<unsigned char>(ch)) != 0; };
    if (!is_digit(value[0]) || !is_digit(value[1]) || !is_digit(value[3]) || !is_digit(value[4])) {
        return std::nullopt;
    }
    const int hour = (value[0] - '0') * 10 + (value[1] - '0');
    const int minute = (value[3] - '0') * 10 + (value[4] - '0');
    if (hour < 0 || hour > 23 || minute < 0 || minute > 59) {
        return std::nullopt;
    }
    return hour * 60 + minute;
}

std::optional<std::int64_t> CommodityPauseWindowSeconds(int close_minute) {
    if (close_minute == kCommodityMorningBreakMinute) {
        return kCommodityMorningBreakSeconds;
    }
    if (close_minute == kCommodityLunchBreakMinute) {
        return kCommodityLunchBreakSeconds;
    }
    if (close_minute >= kCommodityDayCloseMinute &&
        close_minute <= kCommodityPostDayCloseLatestMinute) {
        return kCommodityDayCloseSeconds;
    }
    return std::nullopt;
}

bool IsWithinPauseAge(const std::optional<std::int64_t>& age, std::int64_t pause_seconds) {
    return age.has_value() && *age <= pause_seconds;
}

bool IsCommodityPauseObservation(const std::string& clock_text,
                                 const std::optional<std::int64_t>& age) {
    const auto minute = ParseClockMinute(clock_text);
    if (!minute.has_value()) {
        return false;
    }
    const auto pause_seconds = CommodityPauseWindowSeconds(*minute);
    if (!pause_seconds.has_value()) {
        return false;
    }
    return IsWithinPauseAge(age, *pause_seconds);
}

void ApplyCommodityPauseStatus(MarketFileStatus& status) {
    if (status.tick_status == "stale" &&
        IsCommodityPauseObservation(status.tick.update_time, status.tick_age_seconds)) {
        status.tick_status = "closed";
    }
    if (status.bar_status == "stale" &&
        IsCommodityPauseObservation(status.bar.minute, status.bar_age_seconds)) {
        status.bar_status = "closed";
    }
}

bool MarketObservationHealthy(const std::string& status) {
    return status == "fresh" || status == "closed";
}

ProcessStatus CollectProcessStatus(const DashboardOptions& options) {
    ProcessStatus status;
    const fs::path run_root(options.run_root);
    status.pid = ReadDigitsOnly(run_root / "current_core_engine.pid");
    status.run_dir = ReadFirstLine(run_root / "current_run_dir");
    status.core_log = ReadFirstLine(run_root / "current_core_engine_log");
    status.supervisor_pids = FindSupervisorPids(options.run_root);

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
    if (!status.supervisor_pids.empty()) {
        status.status = "waiting_for_trading_window";
        status.healthy = true;
        status.core_log.clear();
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
        ApplyCommodityPauseStatus(status);
        status.healthy = MarketObservationHealthy(status.tick_status) &&
                         MarketObservationHealthy(status.bar_status);
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

std::string BriefSignalMonitorEvent(const std::string& line) {
    const std::string ts = ExtractJsonString(line, "ts");
    const std::string event = ExtractJsonString(line, "event");
    const std::string message = ExtractJsonString(line, "message");
    const std::string trace_id = ExtractJsonString(line, "trace_id");
    std::ostringstream out;
    if (!ts.empty()) {
        out << ts << ' ';
    }
    out << (event.empty() ? "event" : event);
    if (!trace_id.empty()) {
        out << " trace_id=" << trace_id;
    }
    if (!message.empty()) {
        out << " - " << message;
    }
    return out.str();
}

SignalMonitorStatus CollectSignalMonitorStatus(const DashboardOptions& options) {
    SignalMonitorStatus status;
    const fs::path run_root(options.run_root);
    const fs::path monitor_root(options.monitor_root);
    const fs::path event_log = monitor_root / "signal_execution_watch.jsonl";
    const fs::path incident_dir = monitor_root / "incidents";
    const fs::path monitor_log = run_root / "signal_execution_monitor.log";

    status.pid = ReadDigitsOnly(run_root / "signal_execution_monitor.pid");
    status.event_log_path = event_log.string();
    status.incident_dir = incident_dir.string();
    status.monitor_log_path = monitor_log.string();
    status.event_log_age_seconds = FileAgeSeconds(event_log);
    status.event_log_exists = status.event_log_age_seconds.has_value();

    if (!status.pid.empty() && ProcessIsAlive(status.pid)) {
        status.status = "alive";
    } else if (status.event_log_exists) {
        status.status = "history_only";
    } else if (!status.pid.empty()) {
        status.status = "dead";
    } else {
        status.status = "missing";
    }

    const SignalMonitorEpoch epoch = CurrentSignalMonitorEpochFromLog(event_log.string());
    const SignalMonitorEpochStats epoch_stats = BuildSignalMonitorEpochStats(epoch);
    status.signals = epoch_stats.signals;
    status.active = epoch_stats.active;
    status.filled = epoch_stats.filled;
    status.incidents = epoch_stats.incidents;
    const bool summary_rebased = SignalMonitorSummaryDiffers(epoch_stats);

    for (const std::string& line : epoch.lines) {
        const std::string event = ExtractJsonString(line, "event");
        const std::string message = event == "summary" && summary_rebased
                                        ? SignalMonitorCountsMessage(epoch_stats)
                                        : ExtractJsonString(line, "message");
        if (!event.empty()) {
            status.last_event = event;
        }
        if (!message.empty()) {
            status.last_message = message;
        }
        status.recent_events.push_back(event == "summary" && summary_rebased
                                           ? BriefSignalMonitorSummaryEvent(line, epoch_stats)
                                           : BriefSignalMonitorEvent(line));
        if (status.recent_events.size() > kRecentSignalMonitorLimit) {
            status.recent_events.erase(status.recent_events.begin());
        }
    }

    std::error_code ec;
    if (status.incidents > 0 && fs::exists(incident_dir, ec)) {
        std::vector<std::string> incidents;
        for (const auto& entry : fs::directory_iterator(
                 incident_dir, fs::directory_options::skip_permission_denied, ec)) {
            if (ec) {
                break;
            }
            if (!entry.is_regular_file(ec)) {
                continue;
            }
            const std::string filename = entry.path().filename().string();
            if (!epoch_stats.incident_trace_ids.empty()) {
                const bool current_incident = std::any_of(
                    epoch_stats.incident_trace_ids.begin(), epoch_stats.incident_trace_ids.end(),
                    [&](const std::string& trace_id) {
                        return !trace_id.empty() && filename.find(trace_id) != std::string::npos;
                    });
                if (!current_incident) {
                    continue;
                }
            }
            incidents.push_back(filename);
        }
        std::sort(incidents.begin(), incidents.end(), std::greater<>());
        for (const auto& item : incidents) {
            status.recent_incidents.push_back(item);
            if (status.recent_incidents.size() >= kRecentSignalMonitorLimit) {
                break;
            }
        }
    }

    status.healthy = (status.status == "alive" || status.status == "history_only" ||
                      status.status == "missing") &&
                     status.incidents == 0;
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

std::string ReplaceKnownCtpPlaceholderError(std::string line) {
    if (!quant_hft::ctp::LooksLikePlaceholderText(line)) {
        return line;
    }
    std::string error_id = ExtractEventField(line, "ErrorID");
    if (error_id.empty()) {
        error_id = ExtractEventField(line, "error_id");
    }
    if (error_id.empty()) {
        return line;
    }
    std::string known;
    try {
        known = quant_hft::ctp::KnownCtpErrorMessage(std::stoi(error_id));
    } catch (...) {
        return line;
    }
    if (known.empty()) {
        return line;
    }

    const std::vector<std::string> markers = {"ErrorMsg=", "error_msg=", "status_msg="};
    for (const auto& marker : markers) {
        std::size_t pos = 0;
        while ((pos = line.find(marker, pos)) != std::string::npos) {
            const std::size_t value_start = pos + marker.size();
            std::size_t value_end = value_start;
            while (value_end < line.size() && line[value_end] != ')' && line[value_end] != '"' &&
                   line[value_end] != '\n' && line[value_end] != '\r') {
                ++value_end;
            }
            if (line.substr(value_start, value_end - value_start).find("????") !=
                std::string::npos) {
                line.replace(value_start, value_end - value_start, known);
                pos = value_start + known.size();
            } else {
                pos = value_end;
            }
        }
    }
    return line;
}

std::string SanitizeLogLine(std::string line) {
    const std::vector<std::string> quoted_keys = {
        "account_id=\"",      "investor_id=\"", "user_id=\"",       "balance=\"",
        "available=\"",       "curr_margin=\"", "frozen_margin=\"", "close_profit=\"",
        "position_profit=\"", "password=\"",    "auth_code=\"",     "authcode=\""};
    for (const auto& key : quoted_keys) {
        line = RedactAfterKey(std::move(line), key);
    }
    line = ReplaceKnownCtpPlaceholderError(std::move(line));
    return SafeAscii(line);
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

bool LineHasNonZeroError(const std::string& lower) {
    const bool has_error_key = lower.find("errorid=") != std::string::npos ||
                               lower.find("error_id=") != std::string::npos ||
                               lower.find("\"error_id\"") != std::string::npos ||
                               lower.find("\"errorid\"") != std::string::npos;
    if (!has_error_key) {
        return false;
    }
    return lower.find("errorid=0") == std::string::npos &&
           lower.find("error_id=0") == std::string::npos &&
           lower.find("\"error_id\":0") == std::string::npos &&
           lower.find("\"error_id\": 0") == std::string::npos;
}

bool LineLooksFailed(const std::string& lower) {
    return LineHasNonZeroError(lower) ||
           HasAnyNeedle(lower, {"level=error", " failed", " fail", "reject", "timeout"});
}

bool LineHasHealthyStatus(const std::string& lower) {
    return lower.find("state=\"healthy\"") != std::string::npos ||
           lower.find("state=healthy") != std::string::npos ||
           lower.find("status=\"healthy\"") != std::string::npos ||
           lower.find("status=healthy") != std::string::npos;
}

bool LineLooksCtpRecovered(const std::string& lower) {
    if (LineLooksFailed(lower)) {
        return false;
    }
    return HasAnyNeedle(lower, {"front_candidate_connect_success", "connect_success",
                                "ctp_trader_reconnect_ready", "session_snapshot",
                                "ctp_settlement_confirmed", "settlement_confirmed"}) ||
           (lower.find("health_status") != std::string::npos && LineHasHealthyStatus(lower));
}

void UpdateCtpConnectionFromLine(const std::string& line, const std::string& source,
                                 CtpConnectionStatus* status) {
    if (status == nullptr || line.empty()) {
        return;
    }
    const std::string lower = ToLower(line);
    const std::string issue = ClassifyCtpIssue(line);
    const bool connection_related = HasAnyNeedle(
        lower, {"onfront", "rspuserlogin", "authenticate", "settlement_confirm", "reqsettlement",
                "reconnect", "front_connected", "front_disconnected", "connect_success",
                "probe_completed", "session_snapshot", "health_status"});
    if (!connection_related) {
        return;
    }

    PushLimited(&status->recent_events, SanitizeLogLine(BriefStructuredEvent(line, source)),
                kRecentCtpEventLimit);

    if (!issue.empty()) {
        ++status->ctp_errors;
        status->last_error = issue;
    }
    if (lower.find("front_candidate_connect_success") != std::string::npos ||
        lower.find("ctp_front_candidate_connect_success") != std::string::npos) {
        status->td_front = "connected";
        status->md_front = "connected";
    }
    if (lower.find("ctp_td_front_connected") != std::string::npos ||
        lower.find("td_front_connected") != std::string::npos ||
        lower.find("trader_front_connected") != std::string::npos) {
        status->td_front = "connected";
    }
    if ((lower.find("ctp_front_disconnected") != std::string::npos &&
         lower.find("channel=\"td\"") != std::string::npos) ||
        lower.find("ctp_td_front_disconnected") != std::string::npos ||
        lower.find("td_front_disconnected") != std::string::npos ||
        lower.find("trader_front_disconnected") != std::string::npos) {
        status->td_front = "disconnected";
    }
    if (lower.find("ctp_md_front_connected") != std::string::npos ||
        lower.find("md_front_connected") != std::string::npos) {
        status->md_front = "connected";
    }
    if ((lower.find("ctp_front_disconnected") != std::string::npos &&
         lower.find("channel=\"md\"") != std::string::npos) ||
        lower.find("ctp_md_front_disconnected") != std::string::npos ||
        lower.find("md_front_disconnected") != std::string::npos) {
        status->md_front = "disconnected";
    }
    if (lower.find("auth") != std::string::npos ||
        lower.find("authenticate") != std::string::npos) {
        status->auth_status = LineLooksFailed(lower) ? "failed" : "authenticated";
    }
    if (lower.find("login") != std::string::npos ||
        lower.find("rspuserlogin") != std::string::npos ||
        lower.find("session_snapshot") != std::string::npos) {
        status->login_status = LineLooksFailed(lower) ? "failed" : "logged_in";
    }
    if (lower.find("settlement") != std::string::npos) {
        status->settlement_status = LineLooksFailed(lower) ? "unconfirmed" : "confirmed";
    }
    if (lower.find("reconnect") != std::string::npos) {
        ++status->reconnect_attempts;
    }
    if (lower.find("probe_completed") != std::string::npos) {
        status->probe_status = LineLooksFailed(lower) ? "failed" : "completed";
    }
    if (lower.find("health_status") != std::string::npos && LineHasHealthyStatus(lower)) {
        status->probe_status = "healthy";
    }
    if (LineLooksCtpRecovered(lower)) {
        status->ctp_errors = 0;
        status->last_error.clear();
    }
}

CtpConnectionStatus CollectCtpConnectionStatus(const DashboardOptions& options,
                                               const ProcessStatus& process,
                                               const std::vector<ContractStatus>& contracts) {
    CtpConnectionStatus status;
    for (const auto& contract : contracts) {
        if (!contract.instrument_id.empty()) {
            status.active_instruments.push_back(contract.instrument_id);
        }
    }

    if (process.status == "waiting_for_trading_window") {
        status.status = "waiting_for_trading_window";
        status.healthy = true;
        return status;
    }

    std::string probe_log = LatestRegularFile(options.probe_log_dir, "simnow_probe", ".log");
    if (probe_log.empty() && !process.run_dir.empty()) {
        const fs::path current_run_probe = fs::path(process.run_dir) / "simnow_probe.log";
        std::error_code ec;
        if (fs::is_regular_file(current_run_probe, ec)) {
            probe_log = current_run_probe.string();
        }
    }
    status.latest_probe_log = probe_log;
    if (!probe_log.empty()) {
        status.probe_status = "history_only";
        status.probe_age_seconds = FileAgeSeconds(probe_log);
        std::ifstream input(probe_log);
        std::string line;
        while (std::getline(input, line)) {
            UpdateCtpConnectionFromLine(line, "probe", &status);
        }
    }

    if (!process.core_log.empty()) {
        std::ifstream input(process.core_log);
        std::string line;
        while (std::getline(input, line)) {
            UpdateCtpConnectionFromLine(line, "core", &status);
        }
    }

    if (!process.healthy) {
        status.status = status.latest_probe_log.empty() ? "core_not_running" : "history_only";
        status.healthy = false;
        return status;
    }
    if (status.td_front == "disconnected" || status.md_front == "disconnected" ||
        status.login_status == "failed" || status.auth_status == "failed" ||
        status.settlement_status == "unconfirmed" || status.ctp_errors > 0) {
        status.status = "degraded";
        status.healthy = false;
        return status;
    }
    if (status.td_front == "connected" || status.md_front == "connected" ||
        status.login_status == "logged_in" || status.settlement_status == "confirmed" ||
        status.probe_status == "completed" || status.probe_status == "healthy") {
        status.status = "connected";
        status.healthy = true;
        return status;
    }
    status.status = "observing";
    status.healthy = true;
    return status;
}

ReadinessStatus CollectReadinessStatus(const DashboardOptions& options) {
    ReadinessStatus status;
    status.path = options.readiness_file;
    const auto payload = ReadTextFile(options.readiness_file);
    if (!payload.has_value()) {
        return status;
    }

    status.found = true;
    status.age_seconds = FileAgeSeconds(options.readiness_file);
    status.fresh = status.age_seconds.has_value() && *status.age_seconds <= 10;
    status.mode = ExtractJsonString(*payload, "mode");
    if (status.mode.empty()) {
        status.mode = "unknown";
    }
    status.generation = ExtractJsonInt(*payload, "generation").value_or(0);
    status.recovery_complete = ExtractJsonBool(*payload, "recovery_complete").value_or(false);
    status.trader_ready = ExtractJsonBool(*payload, "trader_ready").value_or(false);
    status.gateway_healthy = ExtractJsonBool(*payload, "gateway_healthy").value_or(false);
    status.settlement_confirmed = ExtractJsonBool(*payload, "settlement_confirmed").value_or(false);
    status.pending_exit_count = ExtractJsonInt(*payload, "pending_exit_count").value_or(0);
    status.unresolved_mapping_count =
        ExtractJsonInt(*payload, "unresolved_mapping_count").value_or(0);
    status.healthy = status.fresh && status.mode == "Ready" && status.recovery_complete &&
                     status.trader_ready && status.gateway_healthy && status.settlement_confirmed &&
                     status.unresolved_mapping_count == 0;
    status.status = status.fresh ? status.mode : "stale";
    return status;
}

void ApplyStructuredReadiness(const ReadinessStatus& readiness, CtpConnectionStatus* connection) {
    if (connection == nullptr || !readiness.found) {
        return;
    }
    connection->td_front = readiness.gateway_healthy ? "connected" : "disconnected";
    connection->md_front = readiness.gateway_healthy ? "connected" : "disconnected";
    connection->login_status = readiness.trader_ready ? "logged_in" : "not_ready";
    connection->settlement_status = readiness.settlement_confirmed ? "confirmed" : "unconfirmed";
    connection->status = readiness.healthy ? "connected" : "degraded";
    connection->healthy = readiness.healthy;
    if (!readiness.fresh) {
        connection->last_error = "structured readiness heartbeat is stale";
    } else if (readiness.unresolved_mapping_count > 0) {
        connection->last_error =
            "unresolved order/trade mappings=" + std::to_string(readiness.unresolved_mapping_count);
    } else if (readiness.mode != "Ready") {
        connection->last_error = "trading permission mode=" + readiness.mode;
    }
}

void UpdateCtpOrderFlowFromLine(const std::string& line, const std::string& source,
                                CtpOrderFlowStatus* flow) {
    if (flow == nullptr || line.empty()) {
        return;
    }
    const std::string lower = ToLower(line);
    const bool relevant = HasAnyNeedle(
        lower,
        {"signal_passed", "order_submitted", "ctp_order_submitted", "ctp_order_submit_rejected",
         "order_rejected", "order_update", "trade_fill", "onrsporderinsert", "onerrrtnorderinsert",
         "onrtnorder", "onrtntrade", "client_order_id", "order_ref", "settlement_unconfirmed"});
    if (!relevant) {
        return;
    }

    PushLimited(&flow->recent_events, SanitizeLogLine(BriefStructuredEvent(line, source)),
                kRecentCtpEventLimit);

    if (lower.find("event=order_submitted") != std::string::npos &&
        lower.find("event=ctp_order_submitted") == std::string::npos) {
        ++flow->order_submitted_logs;
    }
    if (lower.find("ctp_order_submitted") != std::string::npos) {
        ++flow->ctp_submitted;
    }
    if (lower.find("ctp_order_submit_rejected") != std::string::npos ||
        lower.find("order_rejected") != std::string::npos) {
        ++flow->ctp_submit_rejected;
    }

    const bool legacy_state_machine_reject =
        lower.find("legacy_state_machine_rejected") != std::string::npos;
    const std::string issue = ClassifyCtpIssue(line);
    if (!issue.empty() && issue != "front_disconnected" && !legacy_state_machine_reject) {
        flow->last_reject_reason = issue;
        PushLimited(&flow->recent_rejections, SanitizeLogLine(BriefStructuredEvent(line, source)),
                    kRecentCtpEventLimit);
    }
    std::string error_id = ExtractEventField(line, "error_id");
    if (error_id.empty()) {
        error_id = ExtractEventField(line, "ErrorID");
    }
    if (!error_id.empty() && error_id != "0") {
        flow->last_error_id = error_id;
    }
}

std::string TradeSideText(const std::string& side) {
    if (side == "0") {
        return "buy";
    }
    if (side == "1") {
        return "sell";
    }
    return side;
}

std::string TradeOffsetText(const std::string& offset) {
    if (offset == "0") {
        return "open";
    }
    if (offset == "1") {
        return "close";
    }
    if (offset == "2") {
        return "close_today";
    }
    if (offset == "3") {
        return "close_yesterday";
    }
    return offset;
}

std::string LocalTimeFromNanos(const std::string& ts_ns) {
    if (ts_ns.empty()) {
        return "";
    }
    try {
        return FormatLocalTime(std::stoll(ts_ns));
    } catch (...) {
        return "";
    }
}

std::string LocalDateBucketFromNanos(const std::string& ts_ns) {
    if (ts_ns.empty()) {
        return "";
    }
    try {
        return FormatLocalDateBucket(std::stoll(ts_ns));
    } catch (...) {
        return "";
    }
}

std::string TradeFillAttribution(const std::string& strategy_id, const std::string& trace_id) {
    if (!strategy_id.empty() || !trace_id.empty()) {
        return "strategy_matched";
    }
    return "external_or_private_stream";
}

TradeFillDetail ExtractTradeFillDetail(const std::string& line) {
    TradeFillDetail fill;
    fill.seq = ExtractEventField(line, "seq");
    fill.ts_ns = ExtractEventField(line, "ts_ns");
    fill.time = LocalTimeFromNanos(fill.ts_ns);
    fill.trade_day_bucket = LocalDateBucketFromNanos(fill.ts_ns);
    fill.instrument_id = ExtractEventField(line, "instrument_id");
    fill.exchange_id = ExtractEventField(line, "exchange_id");
    fill.side = TradeSideText(ExtractEventField(line, "side"));
    fill.offset = TradeOffsetText(ExtractEventField(line, "offset"));
    fill.volume = ExtractEventField(line, "last_trade_volume");
    if (fill.volume.empty()) {
        fill.volume = ExtractEventField(line, "filled_volume");
    }
    fill.price = ExtractEventField(line, "avg_fill_price");
    fill.strategy_id = ExtractEventField(line, "strategy_id");
    fill.client_order_id = ExtractEventField(line, "client_order_id");
    fill.order_ref = ExtractEventField(line, "order_ref");
    fill.trade_id = ExtractEventField(line, "trade_id");
    fill.trace_id = ExtractEventField(line, "trace_id");
    fill.attribution = TradeFillAttribution(fill.strategy_id, fill.trace_id);
    return fill;
}

std::string BuildTradeFillDedupKey(const TradeFillDetail& fill) {
    if (fill.trade_id.empty()) {
        return "";
    }
    // Exclude trade_day_bucket: replayed fills from previous sessions receive a new
    // ts_ns (recv time) that falls in a different calendar bucket, causing cross-session
    // duplicates to bypass deduplication. trade_id is globally unique per exchange trade.
    return fill.exchange_id + "|" + fill.instrument_id + "|" + fill.side + "|" + fill.offset + "|" +
           fill.trade_id;
}

void PushLimitedTradeFill(std::vector<TradeFillDetail>* items, TradeFillDetail value,
                          std::size_t limit) {
    if (items == nullptr) {
        return;
    }
    items->push_back(std::move(value));
    while (items->size() > limit) {
        items->erase(items->begin());
    }
}

void SeedTradeFillDedupKeyFromWalLine(const std::string& line, CtpOrderFlowStatus* flow) {
    if (flow == nullptr || line.empty()) {
        return;
    }
    const std::string lower = ToLower(line);
    const bool trade_line = lower.find("\"event_type\":\"trade_fill\"") != std::string::npos ||
                            lower.find("\"event_type\": \"trade_fill\"") != std::string::npos ||
                            lower.find("\"kind\":\"trade\"") != std::string::npos ||
                            lower.find("\"kind\": \"trade\"") != std::string::npos;
    if (!trade_line) {
        return;
    }
    TradeFillDetail fill = ExtractTradeFillDetail(line);
    const std::string key = BuildTradeFillDedupKey(fill);
    if (key.empty()) {
        return;
    }
    // Always keep the LATEST historical record for each trade_id (last writer wins).
    // This ensures the most recent pre-session fill data is used when recovering
    // the original exchange timestamp for replay detection.
    flow->trade_fill_index_by_key.insert_or_assign(key, kHistoricalTradeFillIndex);
    flow->historical_fill_by_key.insert_or_assign(key, std::move(fill));
}

void RecordTradeFillDetail(TradeFillDetail fill, CtpOrderFlowStatus* flow) {
    if (flow == nullptr) {
        return;
    }
    ++flow->wal_fills_raw;
    const std::string key = BuildTradeFillDedupKey(fill);
    if (!key.empty()) {
        const auto existing = flow->trade_fill_index_by_key.find(key);
        if (existing != flow->trade_fill_index_by_key.end()) {
            ++flow->wal_duplicate_fills;
            fill.replay_status = "ctp_replay_duplicate";
            PushLimitedTradeFill(&flow->replay_duplicate_fills, std::move(fill),
                                 kRecentCtpEventLimit);
            if (existing->second == kHistoricalTradeFillIndex) {
                // The original trade was in a previous run. Recover its data from
                // historical_fill_by_key so the correct exchange timestamp is displayed.
                // Do NOT increment wal_fills: this is not a new fill in the current session.
                const auto hist = flow->historical_fill_by_key.find(key);
                if (hist != flow->historical_fill_by_key.end()) {
                    TradeFillDetail recovered = hist->second;
                    recovered.replay_status = "replayed_historical";
                    recovered.replay_count = 2;
                    flow->trade_fill_index_by_key.insert_or_assign(key, flow->trade_fills.size());
                    PushLimitedTradeFill(&flow->trade_fills, std::move(recovered),
                                         kRecentCtpEventLimit);
                }
            } else if (existing->second < flow->trade_fills.size()) {
                TradeFillDetail& original = flow->trade_fills[existing->second];
                ++original.replay_count;
                original.replay_status = "replayed " + std::to_string(original.replay_count) + "x";
            }
            return;
        }
        flow->trade_fill_index_by_key.emplace(key, flow->trade_fills.size());
    }
    fill.replay_status = "unique";
    fill.replay_count = 1;
    ++flow->wal_fills;
    flow->trade_fills.push_back(std::move(fill));
}

void UpdateCtpOrderFlowFromWalLine(const std::string& line, CtpOrderFlowStatus* flow) {
    if (flow == nullptr || line.empty()) {
        return;
    }
    const std::string lower = ToLower(line);
    const bool order_event = lower.find("\"event_type\":\"order_update\"") != std::string::npos ||
                             lower.find("\"event_type\": \"order_update\"") != std::string::npos ||
                             lower.find("\"kind\":\"order\"") != std::string::npos ||
                             lower.find("\"kind\": \"order\"") != std::string::npos;
    const bool trade_event = lower.find("\"event_type\":\"trade_fill\"") != std::string::npos ||
                             lower.find("\"event_type\": \"trade_fill\"") != std::string::npos ||
                             lower.find("\"kind\":\"trade\"") != std::string::npos ||
                             lower.find("\"kind\": \"trade\"") != std::string::npos;
    if (!order_event && !trade_event) {
        return;
    }
    PushLimited(&flow->recent_events, SanitizeLogLine(BriefStructuredEvent(line, "wal")),
                kRecentCtpEventLimit);
    if (order_event) {
        ++flow->ctp_callbacks;
        const auto status = ExtractJsonInt(line, "status");
        if (status.has_value() && *status == 5) {
            ++flow->wal_rejected;
            std::string error_id = ExtractEventField(line, "error_id");
            if (error_id.empty()) {
                error_id = ExtractEventField(line, "ErrorID");
            }
            if (!error_id.empty() && error_id != "0") {
                flow->last_error_id = error_id;
            }
            const std::string reason = ClassifyCtpIssue(line);
            flow->last_reject_reason = reason.empty() ? "order_rejected" : reason;
            PushLimited(&flow->recent_rejections,
                        SanitizeLogLine(BriefStructuredEvent(line, "wal")), kRecentCtpEventLimit);
        }
    }
    if (trade_event) {
        RecordTradeFillDetail(ExtractTradeFillDetail(line), flow);
    }
}

CtpOrderFlowStatus CollectCtpOrderFlowStatus(const DashboardOptions& options,
                                             const SignalMonitorStatus& signal_monitor,
                                             const WalStatus& wal_status,
                                             const ProcessStatus& process) {
    CtpOrderFlowStatus flow;
    flow.signals = signal_monitor.signals;
    flow.active = signal_monitor.active;
    flow.monitor_filled = signal_monitor.filled;
    flow.monitor_incidents = signal_monitor.incidents;

    std::int64_t signal_passed_events = 0;
    for (const auto& line : CurrentSignalMonitorEpochLines(signal_monitor.event_log_path)) {
        if (line.find("\"event\":\"signal_passed\"") != std::string::npos ||
            line.find("\"event\": \"signal_passed\"") != std::string::npos) {
            ++signal_passed_events;
        }
        UpdateCtpOrderFlowFromLine(line, "watcher", &flow);
    }
    if (flow.signals == 0 && signal_passed_events > 0) {
        flow.signals = signal_passed_events;
    }

    if (!process.core_log.empty()) {
        std::ifstream core_input(process.core_log);
        std::string line;
        while (std::getline(core_input, line)) {
            UpdateCtpOrderFlowFromLine(line, "core", &flow);
        }
    }

    std::string current_run_id = RunIdFromRunDir(process.run_dir);
    if (current_run_id.empty()) {
        current_run_id =
            RunIdFromRunDir(ReadFirstLine(fs::path(options.run_root) / "current_run_dir"));
    }
    if (wal_status.exists) {
        std::ifstream wal_input(options.wal_file);
        std::string line;
        while (std::getline(wal_input, line)) {
            const std::string line_run_id = ExtractJsonString(line, "run_id");
            if (!current_run_id.empty() && line_run_id != current_run_id) {
                SeedTradeFillDedupKeyFromWalLine(line, &flow);
                continue;
            }
            UpdateCtpOrderFlowFromWalLine(line, &flow);
        }
    }

    const bool has_activity = flow.signals > 0 || flow.order_submitted_logs > 0 ||
                              flow.ctp_submitted > 0 || flow.ctp_callbacks > 0 ||
                              flow.wal_fills > 0;
    const bool has_problem =
        flow.monitor_incidents > 0 || flow.ctp_submit_rejected > 0 || flow.wal_rejected > 0;
    flow.healthy = !has_problem;
    if (has_problem) {
        flow.status = "attention";
    } else if (flow.active > 0) {
        flow.status = "active";
    } else if (has_activity) {
        flow.status = "flowing";
    } else {
        flow.status = "idle";
    }
    return flow;
}

std::string FormatPercent(std::int64_t numerator, std::int64_t denominator) {
    if (denominator <= 0) {
        return "n/a";
    }
    std::ostringstream out;
    out << std::fixed << std::setprecision(1)
        << (100.0 * static_cast<double>(numerator) / static_cast<double>(denominator)) << '%';
    return out.str();
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

std::string JsonOptionalDouble(const std::optional<double>& value) {
    if (!value.has_value() || !std::isfinite(*value)) {
        return "null";
    }
    std::ostringstream out;
    out << std::fixed << std::setprecision(4) << *value;
    return out.str();
}

// Render a price level for HTML cells: fixed precision, or a dash when absent.
std::string FormatPriceCell(const std::optional<double>& value) {
    if (!value.has_value() || !std::isfinite(*value)) {
        return "-";
    }
    std::ostringstream out;
    out << std::fixed << std::setprecision(2) << *value;
    return out.str();
}

std::string RenderStateJson(const DashboardState& state) {
    std::ostringstream out;
    out << "{\n";
    out << "  \"generated_ts_ns\": " << state.generated_ts_ns << ",\n";
    out << "  \"generated_at_local\": " << JsonString(state.generated_at_local) << ",\n";
    out << "  \"overall_healthy\": " << (state.overall_healthy ? "true" : "false") << ",\n";
    out << "  \"live_healthy\": " << (state.live_healthy ? "true" : "false") << ",\n";
    out << "  \"warning_count\": " << state.warning_count << ",\n";
    out << "  \"live_warning_count\": " << state.live_warning_count << ",\n";
    out << "  \"historical_risk_count\": " << state.historical_risk_count << ",\n";
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
            << ", \"candidate_instrument_id\": " << JsonString(item.candidate_instrument_id)
            << ", \"exchange_id\": " << JsonString(item.exchange_id)
            << ", \"trading_day\": " << JsonString(item.trading_day)
            << ", \"cache_trading_day\": " << JsonString(item.cache_trading_day)
            << ", \"phase\": " << JsonString(item.phase)
            << ", \"healthy\": " << (item.healthy ? "true" : "false")
            << ", \"generation\": " << item.generation
            << ", \"selection_metric\": " << JsonString(item.selection_metric)
            << ", \"lead_ratio\": " << item.lead_ratio
            << ", \"eligible_count\": " << item.eligible_count
            << ", \"baseline_count\": " << item.baseline_count
            << ", \"broker_position\": " << item.broker_position
            << ", \"broker_frozen\": " << item.broker_frozen
            << ", \"active_open_orders\": " << item.active_open_orders
            << ", \"active_close_orders\": " << item.active_close_orders
            << ", \"warmup_observed_bars\": " << item.warmup_observed_bars
            << ", \"warmup_required_bars\": " << item.warmup_required_bars
            << ", \"generation_rejections\": " << item.generation_rejections
            << ", \"phase_age_seconds\": "
            << (item.phase_age_seconds.has_value() ? std::to_string(*item.phase_age_seconds)
                                                   : "null")
            << ", \"cache_trading_day_mismatch\": "
            << (item.cache_trading_day_mismatch ? "true" : "false")
            << ", \"last_error\": " << JsonString(item.last_error)
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

    out << "  \"signal_monitor\": {\n";
    out << "    \"status\": " << JsonString(state.signal_monitor.status) << ",\n";
    out << "    \"healthy\": " << (state.signal_monitor.healthy ? "true" : "false") << ",\n";
    out << "    \"pid\": " << JsonString(state.signal_monitor.pid) << ",\n";
    out << "    \"event_log_path\": " << JsonString(state.signal_monitor.event_log_path) << ",\n";
    out << "    \"incident_dir\": " << JsonString(state.signal_monitor.incident_dir) << ",\n";
    out << "    \"monitor_log_path\": " << JsonString(state.signal_monitor.monitor_log_path)
        << ",\n";
    out << "    \"event_log_exists\": "
        << (state.signal_monitor.event_log_exists ? "true" : "false") << ",\n";
    out << "    \"event_log_age_seconds\": "
        << JsonOptionalInt(state.signal_monitor.event_log_age_seconds) << ",\n";
    out << "    \"signals\": " << state.signal_monitor.signals << ",\n";
    out << "    \"active\": " << state.signal_monitor.active << ",\n";
    out << "    \"filled\": " << state.signal_monitor.filled << ",\n";
    out << "    \"incidents\": " << state.signal_monitor.incidents << ",\n";
    out << "    \"last_event\": " << JsonString(state.signal_monitor.last_event) << ",\n";
    out << "    \"last_message\": " << JsonString(state.signal_monitor.last_message) << ",\n";
    out << "    \"recent_events\": [\n";
    for (std::size_t i = 0; i < state.signal_monitor.recent_events.size(); ++i) {
        out << "      " << JsonString(state.signal_monitor.recent_events[i]);
        if (i + 1 < state.signal_monitor.recent_events.size()) {
            out << ',';
        }
        out << "\n";
    }
    out << "    ],\n";
    out << "    \"recent_incidents\": [\n";
    for (std::size_t i = 0; i < state.signal_monitor.recent_incidents.size(); ++i) {
        out << "      " << JsonString(state.signal_monitor.recent_incidents[i]);
        if (i + 1 < state.signal_monitor.recent_incidents.size()) {
            out << ',';
        }
        out << "\n";
    }
    out << "    ]\n";
    out << "  },\n";

    out << "  \"readiness\": {\n";
    out << "    \"path\": " << JsonString(state.readiness.path) << ",\n";
    out << "    \"status\": " << JsonString(state.readiness.status) << ",\n";
    out << "    \"found\": " << (state.readiness.found ? "true" : "false") << ",\n";
    out << "    \"fresh\": " << (state.readiness.fresh ? "true" : "false") << ",\n";
    out << "    \"healthy\": " << (state.readiness.healthy ? "true" : "false") << ",\n";
    out << "    \"age_seconds\": " << JsonOptionalInt(state.readiness.age_seconds) << ",\n";
    out << "    \"mode\": " << JsonString(state.readiness.mode) << ",\n";
    out << "    \"generation\": " << state.readiness.generation << ",\n";
    out << "    \"recovery_complete\": " << (state.readiness.recovery_complete ? "true" : "false")
        << ",\n";
    out << "    \"trader_ready\": " << (state.readiness.trader_ready ? "true" : "false") << ",\n";
    out << "    \"gateway_healthy\": " << (state.readiness.gateway_healthy ? "true" : "false")
        << ",\n";
    out << "    \"settlement_confirmed\": "
        << (state.readiness.settlement_confirmed ? "true" : "false") << ",\n";
    out << "    \"pending_exit_count\": " << state.readiness.pending_exit_count << ",\n";
    out << "    \"unresolved_mapping_count\": " << state.readiness.unresolved_mapping_count << "\n";
    out << "  },\n";

    out << "  \"ctp_connection\": {\n";
    out << "    \"status\": " << JsonString(state.ctp_connection.status) << ",\n";
    out << "    \"healthy\": " << (state.ctp_connection.healthy ? "true" : "false") << ",\n";
    out << "    \"td_front\": " << JsonString(state.ctp_connection.td_front) << ",\n";
    out << "    \"md_front\": " << JsonString(state.ctp_connection.md_front) << ",\n";
    out << "    \"login_status\": " << JsonString(state.ctp_connection.login_status) << ",\n";
    out << "    \"auth_status\": " << JsonString(state.ctp_connection.auth_status) << ",\n";
    out << "    \"settlement_status\": " << JsonString(state.ctp_connection.settlement_status)
        << ",\n";
    out << "    \"probe_status\": " << JsonString(state.ctp_connection.probe_status) << ",\n";
    out << "    \"latest_probe_log\": " << JsonString(state.ctp_connection.latest_probe_log)
        << ",\n";
    out << "    \"probe_age_seconds\": " << JsonOptionalInt(state.ctp_connection.probe_age_seconds)
        << ",\n";
    out << "    \"reconnect_attempts\": " << state.ctp_connection.reconnect_attempts << ",\n";
    out << "    \"ctp_errors\": " << state.ctp_connection.ctp_errors << ",\n";
    out << "    \"last_error\": " << JsonString(state.ctp_connection.last_error) << ",\n";
    out << "    \"active_instruments\": [";
    for (std::size_t i = 0; i < state.ctp_connection.active_instruments.size(); ++i) {
        if (i > 0) {
            out << ", ";
        }
        out << JsonString(state.ctp_connection.active_instruments[i]);
    }
    out << "],\n";
    out << "    \"recent_events\": [\n";
    for (std::size_t i = 0; i < state.ctp_connection.recent_events.size(); ++i) {
        out << "      " << JsonString(state.ctp_connection.recent_events[i]);
        if (i + 1 < state.ctp_connection.recent_events.size()) {
            out << ',';
        }
        out << "\n";
    }
    out << "    ]\n";
    out << "  },\n";

    out << "  \"ctp_order_flow\": {\n";
    out << "    \"status\": " << JsonString(state.ctp_order_flow.status) << ",\n";
    out << "    \"healthy\": " << (state.ctp_order_flow.healthy ? "true" : "false") << ",\n";
    out << "    \"signals\": " << state.ctp_order_flow.signals << ",\n";
    out << "    \"active\": " << state.ctp_order_flow.active << ",\n";
    out << "    \"monitor_filled\": " << state.ctp_order_flow.monitor_filled << ",\n";
    out << "    \"monitor_incidents\": " << state.ctp_order_flow.monitor_incidents << ",\n";
    out << "    \"order_submitted_logs\": " << state.ctp_order_flow.order_submitted_logs << ",\n";
    out << "    \"ctp_submitted\": " << state.ctp_order_flow.ctp_submitted << ",\n";
    out << "    \"ctp_submit_rejected\": " << state.ctp_order_flow.ctp_submit_rejected << ",\n";
    out << "    \"ctp_callbacks\": " << state.ctp_order_flow.ctp_callbacks << ",\n";
    out << "    \"wal_rejected\": " << state.ctp_order_flow.wal_rejected << ",\n";
    out << "    \"wal_fills\": " << state.ctp_order_flow.wal_fills << ",\n";
    out << "    \"wal_fills_raw\": " << state.ctp_order_flow.wal_fills_raw << ",\n";
    out << "    \"wal_duplicate_fills\": " << state.ctp_order_flow.wal_duplicate_fills << ",\n";
    out << "    \"last_error_id\": " << JsonString(state.ctp_order_flow.last_error_id) << ",\n";
    out << "    \"last_reject_reason\": " << JsonString(state.ctp_order_flow.last_reject_reason)
        << ",\n";
    out << "    \"trade_fills\": [\n";
    for (std::size_t i = 0; i < state.ctp_order_flow.trade_fills.size(); ++i) {
        const auto& fill = state.ctp_order_flow.trade_fills[i];
        out << "      {\"seq\": " << JsonString(fill.seq) << ", \"time\": " << JsonString(fill.time)
            << ", \"ts_ns\": " << JsonString(fill.ts_ns)
            << ", \"instrument_id\": " << JsonString(fill.instrument_id)
            << ", \"exchange_id\": " << JsonString(fill.exchange_id)
            << ", \"side\": " << JsonString(fill.side)
            << ", \"offset\": " << JsonString(fill.offset)
            << ", \"volume\": " << JsonString(fill.volume)
            << ", \"price\": " << JsonString(fill.price)
            << ", \"strategy_id\": " << JsonString(fill.strategy_id)
            << ", \"client_order_id\": " << JsonString(fill.client_order_id)
            << ", \"order_ref\": " << JsonString(fill.order_ref)
            << ", \"trade_id\": " << JsonString(fill.trade_id)
            << ", \"trace_id\": " << JsonString(fill.trace_id)
            << ", \"attribution\": " << JsonString(fill.attribution)
            << ", \"replay_status\": " << JsonString(fill.replay_status)
            << ", \"replay_count\": " << fill.replay_count << "}";
        if (i + 1 < state.ctp_order_flow.trade_fills.size()) {
            out << ',';
        }
        out << "\n";
    }
    out << "    ],\n";
    out << "    \"replay_duplicate_fills\": [\n";
    for (std::size_t i = 0; i < state.ctp_order_flow.replay_duplicate_fills.size(); ++i) {
        const auto& fill = state.ctp_order_flow.replay_duplicate_fills[i];
        out << "      {\"seq\": " << JsonString(fill.seq) << ", \"time\": " << JsonString(fill.time)
            << ", \"instrument_id\": " << JsonString(fill.instrument_id)
            << ", \"exchange_id\": " << JsonString(fill.exchange_id)
            << ", \"side\": " << JsonString(fill.side)
            << ", \"offset\": " << JsonString(fill.offset)
            << ", \"volume\": " << JsonString(fill.volume)
            << ", \"price\": " << JsonString(fill.price)
            << ", \"strategy_id\": " << JsonString(fill.strategy_id)
            << ", \"client_order_id\": " << JsonString(fill.client_order_id)
            << ", \"order_ref\": " << JsonString(fill.order_ref)
            << ", \"trade_id\": " << JsonString(fill.trade_id)
            << ", \"trace_id\": " << JsonString(fill.trace_id)
            << ", \"attribution\": " << JsonString(fill.attribution)
            << ", \"replay_status\": " << JsonString(fill.replay_status) << "}";
        if (i + 1 < state.ctp_order_flow.replay_duplicate_fills.size()) {
            out << ',';
        }
        out << "\n";
    }
    out << "    ],\n";
    out << "    \"recent_events\": [\n";
    for (std::size_t i = 0; i < state.ctp_order_flow.recent_events.size(); ++i) {
        out << "      " << JsonString(state.ctp_order_flow.recent_events[i]);
        if (i + 1 < state.ctp_order_flow.recent_events.size()) {
            out << ',';
        }
        out << "\n";
    }
    out << "    ],\n";
    out << "    \"recent_rejections\": [\n";
    for (std::size_t i = 0; i < state.ctp_order_flow.recent_rejections.size(); ++i) {
        out << "      " << JsonString(state.ctp_order_flow.recent_rejections[i]);
        if (i + 1 < state.ctp_order_flow.recent_rejections.size()) {
            out << ',';
        }
        out << "\n";
    }
    out << "    ]\n";
    out << "  },\n";

    out << "  \"recent_alerts\": [\n";
    for (std::size_t i = 0; i < state.recent_alerts.size(); ++i) {
        out << "    " << JsonString(state.recent_alerts[i]);
        if (i + 1 < state.recent_alerts.size()) {
            out << ',';
        }
        out << "\n";
    }
    out << "  ],\n";

    out << "  \"positions_source\": " << JsonString(state.positions_source) << ",\n";
    out << "  \"positions\": [\n";
    for (std::size_t i = 0; i < state.positions.size(); ++i) {
        const auto& pos = state.positions[i];
        out << "    {\"instrument_id\": " << JsonString(pos.instrument_id)
            << ", \"net\": " << pos.net << ", \"source\": " << JsonString(pos.source)
            << ", \"avg_open\": " << JsonOptionalDouble(pos.avg_open)
            << ", \"initial_stop\": " << JsonOptionalDouble(pos.initial_stop)
            << ", \"take_profit\": " << JsonOptionalDouble(pos.take_profit)
            << ", \"trailing_stop\": " << JsonOptionalDouble(pos.trailing_stop) << "}";
        if (i + 1 < state.positions.size()) {
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

std::string AgeText(const std::optional<std::int64_t>& age_seconds) {
    return age_seconds.has_value() ? std::to_string(*age_seconds) + "s" : "n/a";
}

std::string InstrumentLabel(const MarketFileStatus& item) {
    return EmptyAsDash(item.tick.instrument_id.empty() ? item.bar.instrument_id
                                                       : item.tick.instrument_id);
}

std::string JoinOrDash(const std::vector<std::string>& values) {
    if (values.empty()) {
        return "-";
    }
    std::ostringstream out;
    for (std::size_t i = 0; i < values.size(); ++i) {
        if (i > 0) {
            out << ", ";
        }
        out << values[i];
    }
    return out.str();
}

std::string RenderLogList(const std::vector<std::string>& lines, const std::string& empty_text) {
    std::ostringstream out;
    out << "<ul class=\"logs\">";
    if (lines.empty()) {
        out << "<li>" << HtmlEscape(empty_text) << "</li>";
    } else {
        for (const auto& line : lines) {
            out << "<li>" << HtmlEscape(line) << "</li>";
        }
    }
    out << "</ul>";
    return out.str();
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
    const std::int64_t reject_count =
        state.ctp_order_flow.wal_rejected + state.ctp_order_flow.ctp_submit_rejected;
    const std::string active_instruments = JoinOrDash(state.ctp_connection.active_instruments);

    std::ostringstream html;
    html << "<!doctype html>\n<html lang=\"en\">\n<head>\n";
    html << "<meta charset=\"utf-8\">\n";
    html << "<meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">\n";
    html << "<meta http-equiv=\"refresh\" content=\"30\">\n";
    html << "<title>SimNow Dashboard</title>\n";
    html << "<style>\n";
    html << ":root{color-scheme:dark;--bg:#070807;--surface:#101312;--surface-2:#151a18;"
            "--surface-3:#1b211f;--ink:#edf4ef;--muted:#8c9a94;--line:#27312d;"
            "--line-strong:#3d4b45;--ok:#38d484;--bad:#ff6860;--warn:#f0b84a;"
            "--cyan:#43d8e8;--steel:#a6b5ae;--soft:#0c0f0e;}\n";
    html << "*{box-sizing:border-box}body{margin:0;background-color:var(--bg);"
            "background-image:linear-gradient(rgba(130,150,141,.08) 1px,transparent 1px),"
            "linear-gradient(90deg,rgba(130,150,141,.08) 1px,transparent 1px);"
            "background-size:28px 28px;color:var(--ink);font:13px/1.38 "
            "Inter,ui-sans-serif,system-ui,-apple-system,BlinkMacSystemFont,\"Segoe UI\","
            "sans-serif;font-feature-settings:\"tnum\" 1;}\n";
    html << ".topbar{height:56px;padding:0 28px;border-bottom:1px solid var(--line);"
            "background:rgba(7,8,7,.92);backdrop-filter:blur(14px);display:flex;"
            "align-items:center;justify-content:space-between;gap:16px;position:sticky;top:0;"
            "z-index:2}.brand{display:flex;align-items:center;gap:11px;font-weight:800;"
            "letter-spacing:0}.brand-mark{width:18px;height:18px;border-radius:4px;"
            "border:1px solid var(--cyan);background:linear-gradient(135deg,#163330,#0a1110);"
            "box-shadow:0 0 18px rgba(67,216,232,.26)}.nav{display:flex;gap:16px;"
            "align-items:center;color:var(--muted);font-size:12px}.nav span{white-space:nowrap}\n";
    html << "main{max-width:1540px;margin:0 auto;padding:22px 22px 34px}.hero{padding:10px 0 "
            "18px;margin-bottom:12px;display:grid;grid-template-columns:minmax(0,1fr) auto;"
            "gap:24px;align-items:end}.eyebrow{color:var(--cyan);font:800 11px/1.2 "
            "ui-monospace,SFMono-Regular,Menlo,monospace;text-transform:uppercase;"
            "letter-spacing:0}h1{font-size:36px;line-height:1.05;margin:7px 0 7px;"
            "font-weight:780;letter-spacing:0}.intro{margin:0;color:var(--muted);"
            "max-width:720px;font-size:14px}.hero-meta{display:grid;gap:8px;justify-items:end}"
            ".hero-meta .subtle{text-align:right;max-width:360px}.grid{display:grid;"
            "grid-template-columns:repeat(12,minmax(0,1fr));gap:12px}.panel{background:"
            "linear-gradient(180deg,rgba(21,26,24,.96),rgba(12,15,14,.96));border:1px solid "
            "var(--line);border-radius:8px;padding:14px;min-width:0;box-shadow:0 14px 42px "
            "rgba(0,0,0,.22)}.panel-head{display:flex;align-items:flex-start;"
            "justify-content:space-between;gap:12px;margin-bottom:12px}.span-3{grid-column:span 3}"
            ".span-4{grid-column:span 4}.span-5{grid-column:span 5}.span-6{grid-column:span 6}"
            ".span-7{grid-column:span 7}.span-8{grid-column:span 8}.span-12{grid-column:span 12}\n";
    html << "h2{font-size:12px;margin:0 0 10px;font-weight:850;letter-spacing:0;"
            "text-transform:uppercase;font-family:ui-monospace,SFMono-Regular,Menlo,monospace;"
            "color:var(--steel)}.caption,.section-note,.subtle{color:var(--muted)}"
            ".section-note{margin:-4px 0 10px;font-size:12px;overflow-wrap:anywhere}.badge{"
            "display:inline-flex;align-items:center;min-height:20px;border-radius:4px;padding:"
            "2px 7px;font-weight:850;font-size:10px;line-height:1.1;border:1px solid "
            "currentColor;background:transparent;font-family:ui-monospace,SFMono-Regular,Menlo,"
            "monospace;white-space:nowrap}.badge.ok{color:var(--ok);background:rgba(56,212,132,"
            ".09)}.badge.bad{color:var(--bad);background:rgba(255,104,96,.09)}.badge.warn{"
            "color:var(--warn);background:rgba(240,184,74,.1)}\n";
    html << ".metrics{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:8px}"
            ".metric{background:var(--surface-2);border:1px solid var(--line);border-radius:6px;"
            "padding:10px 11px;min-height:64px;min-width:0}.metric-label{color:var(--muted);"
            "font:800 10px/1.15 ui-monospace,SFMono-Regular,Menlo,monospace;text-transform:"
            "uppercase;margin-bottom:7px;white-space:nowrap;overflow:hidden;text-overflow:"
            "ellipsis;letter-spacing:0}.metric-value{font-size:18px;line-height:1.15;"
            "font-weight:820;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}"
            ".hero-stat .metrics,.session-panel .metrics{grid-template-columns:repeat(2,minmax(0,"
            "1fr))}.hero-stat .metric-value{font-size:21px}.price-grid{display:grid;"
            "grid-template-columns:repeat(2,minmax(0,1fr));gap:10px}.price-card{background:"
            "var(--soft);border:1px solid var(--line-strong);border-radius:8px;padding:12px;"
            "min-width:0}.price-card-head{display:flex;justify-content:space-between;gap:10px;"
            "align-items:flex-start}.symbol{display:grid;gap:3px;min-width:0}.symbol strong{"
            "font-size:18px;line-height:1.1;white-space:nowrap;overflow:hidden;text-overflow:"
            "ellipsis}.symbol span{color:var(--muted);font-size:12px;white-space:nowrap;"
            "overflow:hidden;text-overflow:ellipsis}.last-price{margin:12px 0 10px;color:"
            "var(--cyan);font-size:34px;line-height:1;font-weight:860;white-space:nowrap;"
            "overflow:hidden;text-overflow:ellipsis}.quote-row,.mini-grid{display:grid;gap:8px}"
            ".quote-row{grid-template-columns:repeat(2,minmax(0,1fr));margin-bottom:8px}"
            ".quote-box{border:1px solid var(--line);border-radius:6px;padding:8px;background:"
            "var(--surface)}.quote-label{display:block;color:var(--muted);font:800 10px/1.1 "
            "ui-monospace,SFMono-Regular,Menlo,monospace;text-transform:uppercase}.quote-value{"
            "display:block;margin-top:4px;font-size:16px;font-weight:760;white-space:nowrap;"
            "overflow:hidden;text-overflow:ellipsis}.mini-grid{grid-template-columns:repeat(3,"
            "minmax(0,1fr))}.mini{border-top:1px solid var(--line);padding-top:8px;min-width:0}"
            ".mini b{display:block;font-size:13px;white-space:nowrap;overflow:hidden;"
            "text-overflow:ellipsis}.mini span{display:block;color:var(--muted);font-size:11px;"
            "white-space:nowrap;overflow:hidden;text-overflow:ellipsis}\n";
    html << "table{width:100%;border-collapse:collapse;table-layout:fixed;border-top:1px solid "
            "var(--line);border-left:1px solid var(--line)}th,td{padding:8px 9px;border-right:"
            "1px solid var(--line);border-bottom:1px solid var(--line);text-align:left;"
            "vertical-align:top;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}"
            "th{font:850 10px/1.15 ui-monospace,SFMono-Regular,Menlo,monospace;text-transform:"
            "uppercase;color:var(--muted);background:var(--surface-2)}td{background:var(--soft)}"
            "code{font-family:ui-monospace,SFMono-Regular,Menlo,monospace;font-size:11px;color:"
            "var(--cyan);overflow-wrap:anywhere;word-break:break-word}.scroll{max-height:248px;"
            "overflow:auto;border:1px solid var(--line);border-radius:6px;background:var(--soft)}"
            ".scroll table{border-top:0;border-left:0}.logs{margin:0;padding:8px;list-style:"
            "none;display:grid;gap:7px}.logs li{background:var(--surface);border:1px solid "
            "var(--line);border-radius:6px;padding:9px 10px;font-family:ui-monospace,"
            "SFMono-Regular,Menlo,monospace;font-size:12px;overflow-wrap:anywhere}.empty-state{"
            "border:1px dashed var(--line-strong);border-radius:8px;padding:22px;color:"
            "var(--muted);background:var(--soft)}details.panel summary{cursor:pointer;color:"
            "var(--steel);font:850 12px/1.2 "
            "ui-monospace,SFMono-Regular,Menlo,monospace;text-transform:uppercase}\n";
    html << "@media(max-width:1100px){.span-3,.span-4,.span-5,.span-6,.span-7,.span-8{"
            "grid-column:span 12}.price-grid{grid-template-columns:1fr}.hero{grid-template-"
            "columns:1fr}.hero-meta{justify-items:start}.hero-meta .subtle{text-align:left}"
            ".metrics{grid-template-columns:repeat(2,minmax(0,1fr))}}"
            "@media(max-width:620px){.topbar,main{padding-left:"
            "14px;padding-right:14px}.nav{display:none}.metrics,.mini-grid,.quote-row{"
            "grid-template-columns:1fr}h1{font-size:29px}.last-price{font-size:29px}}\n";
    html << "</style>\n</head>\n<body>\n";

    html << "<header class=\"topbar\"><div class=\"brand\"><span class=\"brand-mark\"></span>"
            "<span>quant_hft / SimNow</span></div><nav class=\"nav\"><span>Status</span>"
            "<span>Positions</span><span>Prices</span><span>Fills</span>"
            "<span>30s refresh</span></nav></header>\n";
    html << "<main><section class=\"hero\"><div><div class=\"eyebrow\">SimNow live trading</div>"
            "<h1>SimNow Trading Cockpit</h1><p class=\"intro\">Key live state: system health, "
            "positions, prices, order flow, and fills.</p></div><div "
            "class=\"hero-meta\">"
         << RenderBadge(state.live_healthy ? "live healthy" : "live unhealthy", state.live_healthy)
         << "<div class=\"subtle\">Generated " << HtmlEscape(state.generated_at_local)
         << "</div></div></section><div class=\"grid\">\n";

    // Panel 0: Recent fills (newest first) — surfaced at the top for quick review.
    html << "<section class=\"panel span-12\"><h2>Recent Fills</h2>";
    html << "<p class=\"section-note\">Unique fills " << state.ctp_order_flow.wal_fills << " / raw "
         << state.ctp_order_flow.wal_fills_raw << "; " << state.ctp_order_flow.wal_duplicate_fills
         << " replay duplicates filtered. Newest first.</p>";
    html << "<div class=\"scroll\"><table><thead><tr><th>Time</th><th>Instrument</th>"
            "<th>Side</th><th>Offset</th><th>Volume</th><th>Price</th>"
            "<th>Strategy</th></tr></thead><tbody>";
    if (state.ctp_order_flow.trade_fills.empty()) {
        html << "<tr><td colspan=\"7\">No unique trade fills found</td></tr>";
    } else {
        for (auto it = state.ctp_order_flow.trade_fills.rbegin();
             it != state.ctp_order_flow.trade_fills.rend(); ++it) {
            const auto& fill = *it;
            html << "<tr><td>" << HtmlEscape(EmptyAsDash(fill.time)) << "</td><td>"
                 << HtmlEscape(EmptyAsDash(fill.instrument_id)) << "</td><td>"
                 << HtmlEscape(EmptyAsDash(fill.side)) << "</td><td>"
                 << HtmlEscape(EmptyAsDash(fill.offset)) << "</td><td>"
                 << HtmlEscape(EmptyAsDash(fill.volume)) << "</td><td>"
                 << HtmlEscape(EmptyAsDash(fill.price)) << "</td><td>"
                 << HtmlEscape(EmptyAsDash(fill.strategy_id)) << "</td></tr>";
        }
    }
    html << "</tbody></table></div></section>\n";

    // Panel 1: Status bar — consolidated system health.
    html << "<section class=\"panel span-12\"><h2>System Status</h2><div class=\"metrics\">";
    html << RenderMetric("core_engine", state.process.status);
    html << RenderMetric("permission", state.readiness.found ? state.readiness.mode : "legacy");
    html << RenderMetric("recovery gen", state.readiness.generation);
    html << RenderMetric("CTP", state.ctp_connection.status);
    html << RenderMetric("settlement", state.ctp_connection.settlement_status);
    html << RenderMetric("signal monitor", state.signal_monitor.status);
    html << RenderMetric("trading day", state.daily.trading_day);
    html << RenderMetric("warnings", state.live_warning_count);
    html << RenderMetric("rejects", reject_count);
    html << RenderMetric("incidents", state.signal_monitor.incidents);
    html << "</div><div class=\"section-note\">Active instruments: <code>"
         << HtmlEscape(active_instruments) << "</code></div></section>\n";

    html << "<section class=\"panel span-12\"><div class=\"panel-head\"><div><h2>"
            "Dominant Contract State</h2><div class=\"caption\">Structured flat-only switch "
            "phase, broker drain and no-signal warmup progress.</div></div></div>";
    html << "<div class=\"scroll\"><table><thead><tr><th>Product</th><th>Current</th>"
            "<th>Candidate</th><th>Phase</th><th>Generation</th><th>Coverage</th>"
            "<th>Broker Pos/Frozen</th><th>Active Open/Close</th><th>Warmup</th>"
            "<th>Generation Rejects</th><th>Phase Age</th><th>Last Error</th></tr></thead><tbody>";
    if (state.contracts.empty()) {
        html << "<tr><td colspan=\"12\">No dominant-contract status files found</td></tr>";
    } else {
        for (const auto& contract : state.contracts) {
            const std::string coverage = std::to_string(contract.baseline_count) + "/" +
                                         std::to_string(contract.eligible_count);
            const std::string broker = std::to_string(contract.broker_position) + "/" +
                                       std::to_string(contract.broker_frozen);
            const std::string orders = std::to_string(contract.active_open_orders) + "/" +
                                       std::to_string(contract.active_close_orders);
            const std::string warmup = std::to_string(contract.warmup_observed_bars) + "/" +
                                       std::to_string(contract.warmup_required_bars);
            html << "<tr><td>" << HtmlEscape(contract.product) << "</td><td>"
                 << HtmlEscape(EmptyAsDash(contract.instrument_id)) << "</td><td>"
                 << HtmlEscape(EmptyAsDash(contract.candidate_instrument_id)) << "</td><td>"
                 << RenderBadge(contract.phase, contract.healthy) << "</td><td>"
                 << contract.generation << "</td><td>" << HtmlEscape(coverage) << "</td><td>"
                 << HtmlEscape(broker) << "</td><td>" << HtmlEscape(orders) << "</td><td>"
                 << HtmlEscape(warmup) << "</td><td>" << contract.generation_rejections
                 << "</td><td>" << HtmlEscape(AgeText(contract.phase_age_seconds)) << "</td><td>"
                 << HtmlEscape(EmptyAsDash(contract.last_error)) << "</td></tr>";
        }
    }
    html << "</tbody></table></div></section>\n";

    // Panel 2: Current positions — strategy net positions persisted to disk.
    html << "<section class=\"panel span-12\"><div class=\"panel-head\"><div><h2>Current "
            "Positions</h2><div class=\"caption\">Strategy net positions from "
         << HtmlEscape(EmptyAsDash(state.positions_source))
         << "; open/stop/take-profit levels from strategy state, reconciled to CTP truth at "
            "session start.</div></div></div>";
    html << "<table><thead><tr><th>Instrument</th><th>Net</th><th>Direction</th>"
            "<th>Open</th><th>Init Stop</th><th>Take Profit</th><th>Trailing Stop</th>"
            "</tr></thead><tbody>";
    if (state.positions.empty()) {
        html << "<tr><td colspan=\"7\">Flat / no strategy positions found</td></tr>";
    } else {
        for (const auto& pos : state.positions) {
            const std::string direction = pos.net > 0 ? "LONG" : (pos.net < 0 ? "SHORT" : "FLAT");
            const bool is_long = pos.net > 0;
            html << "<tr><td>" << HtmlEscape(EmptyAsDash(pos.instrument_id)) << "</td><td>"
                 << pos.net << "</td><td>" << RenderBadge(direction, is_long) << "</td><td>"
                 << HtmlEscape(FormatPriceCell(pos.avg_open)) << "</td><td>"
                 << HtmlEscape(FormatPriceCell(pos.initial_stop)) << "</td><td>"
                 << HtmlEscape(FormatPriceCell(pos.take_profit)) << "</td><td>"
                 << HtmlEscape(FormatPriceCell(pos.trailing_stop)) << "</td></tr>";
        }
    }
    html << "</tbody></table></section>\n";

    // Panel 3: Price board.
    html << "<section class=\"panel span-8\"><div class=\"panel-head\"><div><h2>Price Board</h2>"
            "<div class=\"caption\">Latest trading_day market observations by product.</div></div>"
            "<div>"
         << RenderBadge(state.markets.empty() ? "missing" : "market feed",
                        !state.markets.empty() && state.live_healthy)
         << "</div></div><div class=\"price-grid\">";
    if (state.markets.empty()) {
        html << "<div class=\"empty-state\">No market CSV files found</div>";
    }
    for (const auto& item : state.markets) {
        html << "<article class=\"price-card\"><div class=\"price-card-head\"><div "
                "class=\"symbol\"><strong>"
             << HtmlEscape(EmptyAsDash(item.product)) << "</strong><span>"
             << HtmlEscape(InstrumentLabel(item)) << " / "
             << HtmlEscape(EmptyAsDash(item.tick.exchange_id.empty() ? item.bar.exchange_id
                                                                     : item.tick.exchange_id))
             << "</span></div>"
             << RenderBadge(item.tick_status, MarketObservationHealthy(item.tick_status))
             << "</div><div class=\"last-price\">" << HtmlEscape(EmptyAsDash(item.tick.last_price))
             << "</div><div class=\"quote-row\">"
             << "<div class=\"quote-box\"><span class=\"quote-label\">Bid</span><span "
                "class=\"quote-value\">"
             << HtmlEscape(EmptyAsDash(item.tick.bid_price_1)) << "</span></div>"
             << "<div class=\"quote-box\"><span class=\"quote-label\">Ask</span><span "
                "class=\"quote-value\">"
             << HtmlEscape(EmptyAsDash(item.tick.ask_price_1)) << "</span></div></div>"
             << "<div class=\"mini-grid\"><div class=\"mini\"><b>"
             << HtmlEscape(EmptyAsDash(item.bar.close)) << "</b><span>bar close</span></div>"
             << "<div class=\"mini\"><b>" << HtmlEscape(EmptyAsDash(item.tick.volume))
             << "</b><span>volume</span></div><div class=\"mini\"><b>"
             << HtmlEscape(AgeText(item.tick_age_seconds)) << "</b><span>tick age</span></div>"
             << "</div><div class=\"section-note\">Bar " << HtmlEscape(item.bar_status) << " at "
             << HtmlEscape(EmptyAsDash(item.bar.minute)) << "; age "
             << HtmlEscape(AgeText(item.bar_age_seconds)) << ".</div></article>";
    }
    html << "</div></section>\n";

    // Panel 5: Orders & fills summary (condensed metrics only).
    html << "<section class=\"panel span-4\"><h2>Orders And Fills</h2><div class=\"metrics\">";
    html << RenderMetric("WAL orders", state.wal.order_events);
    html << RenderMetric("unique fills", state.ctp_order_flow.wal_fills);
    html << RenderMetric("rejects", reject_count);
    html << RenderMetric("callbacks", state.ctp_order_flow.ctp_callbacks);
    html << RenderMetric("submit rate", FormatPercent(state.ctp_order_flow.ctp_submitted,
                                                      state.ctp_order_flow.signals));
    html << RenderMetric("export fills", state.daily.exported_trade_fills);
    html << "</div></section>\n";

    // Panel 6: Issues — alerts + signal incidents + CTP rejections combined.
    std::vector<std::string> issues;
    for (const auto& line : state.ctp_order_flow.recent_rejections) {
        issues.push_back("reject: " + line);
    }
    for (const auto& line : state.signal_monitor.recent_incidents) {
        issues.push_back("incident: " + line);
    }
    for (const auto& line : state.recent_alerts) {
        issues.push_back(line);
    }
    html << "<section class=\"panel span-12\"><h2>Issues And Alerts</h2><div class=\"scroll\">"
         << RenderLogList(issues, "No issues, rejections, or alerts") << "</div></section>\n";

    // Panel 7: Data paths (collapsible, ops use).
    html << "<details class=\"panel span-12\"><summary>Data Paths</summary>"
         << "<div class=\"section-note\">Source files backing this dashboard.</div>"
         << "<div class=\"scroll\">" << RenderPathTable(state.paths) << "</div></details>\n";

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
    options.monitor_root = quant_hft::apps::GetArg(
        args, "monitor-root", DefaultPathFromRoot("runtime/trading/monitor/simnow"));
    options.readiness_file = quant_hft::apps::GetArg(
        args, "readiness-file",
        GetEnvOrDefault("QUANT_HFT_READINESS_FILE",
                        (fs::path(options.monitor_root) / "readiness.json").string()));
    options.ctp_instrument_dir = quant_hft::apps::GetArg(
        args, "ctp-instrument-dir", DefaultPathFromRoot("runtime/ctp_instruments"));
    options.probe_log_dir = quant_hft::apps::GetArg(
        args, "probe-log-dir", DefaultPathFromRoot("runtime/verify_simnow_login"));
    options.state_dir = quant_hft::apps::GetArg(
        args, "state-dir", DefaultPathFromRoot("runtime/trading/state/simnow"));
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

// Parse the strategy net positions persisted to the strategy state files. The
// CompositeStrategy persists a flat `net_pos.<instrument>` map; both strategy
// files carry the merged map, so we take the entry from the file with the
// newest saved_epoch_seconds. Entries with a zero net are treated as flat and
// dropped. This source survives restarts and reflects the startup reconcile.
// Parse all `"<prefix><instrument>": "<value>"` entries in a strategy state JSON
// document into a per-instrument double map. The leading quote in the search
// token prevents matching nested atomic keys such as
// `atomic.<id>.trailing_stop.0.price`, which are never preceded by a quote.
std::map<std::string, double> ParsePrefixedQuotedDoubles(const std::string& text,
                                                         const std::string& prefix) {
    std::map<std::string, double> values;
    const std::string token = "\"" + prefix;
    std::size_t pos = 0;
    while ((pos = text.find(token, pos)) != std::string::npos) {
        const std::size_t key_start = pos + token.size();
        const std::size_t key_end = text.find('"', key_start);
        if (key_end == std::string::npos) {
            break;
        }
        const std::string instrument = text.substr(key_start, key_end - key_start);
        std::size_t colon = text.find(':', key_end);
        pos = key_end + 1;
        if (colon == std::string::npos) {
            continue;
        }
        ++colon;
        while (colon < text.size() && std::isspace(static_cast<unsigned char>(text[colon])) != 0) {
            ++colon;
        }
        // Values are serialized as quoted strings (e.g. "4500.500000").
        if (colon < text.size() && text[colon] == '"') {
            ++colon;
        }
        std::size_t value_end = colon;
        while (value_end < text.size() &&
               (std::isdigit(static_cast<unsigned char>(text[value_end])) != 0 ||
                text[value_end] == '-' || text[value_end] == '+' || text[value_end] == '.' ||
                text[value_end] == 'e' || text[value_end] == 'E')) {
            ++value_end;
        }
        if (value_end == colon || instrument.empty()) {
            continue;
        }
        try {
            values[instrument] = std::stod(text.substr(colon, value_end - colon));
        } catch (...) {
            continue;
        }
    }
    return values;
}

std::vector<PositionRow> CollectPositions(const DashboardOptions& options, std::string* source) {
    std::map<std::string, std::pair<std::int64_t, std::int64_t>> net_by_instrument;  // net, epoch
    // Per-instrument price levels keyed alongside the newest epoch seen, so a
    // later snapshot overrides an earlier one consistently with net positions.
    std::map<std::string, std::pair<double, std::int64_t>> avg_open_by_instrument;
    std::map<std::string, std::pair<double, std::int64_t>> init_stop_by_instrument;
    std::map<std::string, std::pair<double, std::int64_t>> take_profit_by_instrument;
    std::map<std::string, std::pair<double, std::int64_t>> trailing_stop_by_instrument;
    const fs::path state_dir(options.state_dir);
    std::error_code ec;
    if (!fs::is_directory(state_dir, ec)) {
        if (source != nullptr) {
            *source = "no state dir";
        }
        return {};
    }
    bool any_file = false;
    for (const auto& entry : fs::directory_iterator(state_dir, ec)) {
        if (ec) {
            break;
        }
        if (!entry.is_regular_file()) {
            continue;
        }
        const std::string name = entry.path().filename().string();
        if (name.rfind("strategy_state__", 0) != 0 ||
            entry.path().extension().string() != ".json" ||
            name.find("timeframe_state_fanout") != std::string::npos) {
            continue;
        }
        const auto content = ReadTextFile(entry.path());
        if (!content.has_value()) {
            continue;
        }
        any_file = true;
        const std::string& text = *content;
        std::int64_t saved_epoch = 0;
        if (const auto epoch = ExtractJsonInt(text, "saved_epoch_seconds")) {
            saved_epoch = *epoch;
        }
        const auto merge_prices =
            [saved_epoch](std::map<std::string, std::pair<double, std::int64_t>>& target,
                          const std::map<std::string, double>& parsed) {
                for (const auto& [instrument, value] : parsed) {
                    auto it = target.find(instrument);
                    if (it == target.end() || saved_epoch >= it->second.second) {
                        target[instrument] = {value, saved_epoch};
                    }
                }
            };
        merge_prices(avg_open_by_instrument, ParsePrefixedQuotedDoubles(text, "avg_open."));
        merge_prices(init_stop_by_instrument, ParsePrefixedQuotedDoubles(text, "init_stop."));
        merge_prices(take_profit_by_instrument, ParsePrefixedQuotedDoubles(text, "take_profit."));
        merge_prices(trailing_stop_by_instrument,
                     ParsePrefixedQuotedDoubles(text, "trailing_stop."));
        std::size_t pos = 0;
        const std::string token = "\"net_pos.";
        while ((pos = text.find(token, pos)) != std::string::npos) {
            const std::size_t key_start = pos + token.size();
            const std::size_t key_end = text.find('"', key_start);
            if (key_end == std::string::npos) {
                break;
            }
            const std::string instrument = text.substr(key_start, key_end - key_start);
            std::size_t colon = text.find(':', key_end);
            pos = key_end;
            if (colon == std::string::npos) {
                continue;
            }
            ++colon;
            while (colon < text.size() && (text[colon] == ' ' || text[colon] == '\t')) {
                ++colon;
            }
            // The state map serializes values as quoted strings (e.g. "19"); skip
            // an optional opening quote before reading the signed integer.
            if (colon < text.size() && text[colon] == '"') {
                ++colon;
            }
            std::size_t value_end = colon;
            while (value_end < text.size() &&
                   (std::isdigit(static_cast<unsigned char>(text[value_end])) != 0 ||
                    text[value_end] == '-' || text[value_end] == '+')) {
                ++value_end;
            }
            if (value_end == colon) {
                continue;
            }
            std::int64_t net = 0;
            try {
                net = std::stoll(text.substr(colon, value_end - colon));
            } catch (...) {
                continue;
            }
            auto it = net_by_instrument.find(instrument);
            if (it == net_by_instrument.end() || saved_epoch >= it->second.second) {
                net_by_instrument[instrument] = {net, saved_epoch};
            }
        }
    }
    if (source != nullptr) {
        *source = any_file ? "strategy_state" : "no state files";
    }
    std::vector<PositionRow> rows;
    for (const auto& [instrument, value] : net_by_instrument) {
        if (value.first == 0) {
            continue;
        }
        PositionRow row;
        row.instrument_id = instrument;
        row.net = value.first;
        row.source = "strategy_state";
        if (const auto it = avg_open_by_instrument.find(instrument);
            it != avg_open_by_instrument.end()) {
            row.avg_open = it->second.first;
        }
        if (const auto it = init_stop_by_instrument.find(instrument);
            it != init_stop_by_instrument.end()) {
            row.initial_stop = it->second.first;
        }
        if (const auto it = take_profit_by_instrument.find(instrument);
            it != take_profit_by_instrument.end()) {
            row.take_profit = it->second.first;
        }
        if (const auto it = trailing_stop_by_instrument.find(instrument);
            it != trailing_stop_by_instrument.end()) {
            row.trailing_stop = it->second.first;
        }
        rows.push_back(row);
    }
    return rows;
}

DashboardState CollectState(const DashboardOptions& options) {
    DashboardState state;
    state.generated_ts_ns = UnixEpochNanosNow();
    state.generated_at_local = FormatLocalTime(state.generated_ts_ns);
    state.paths = {
        {"run_root", options.run_root},
        {"market_data_dir", options.market_data_dir},
        {"wal_file", options.wal_file},
        {"report_root", options.report_root},
        {"export_root", options.export_root},
        {"output_dir", options.output_dir},
        {"monitor_root", options.monitor_root},
        {"readiness_file", options.readiness_file},
        {"ctp_instrument_dir", options.ctp_instrument_dir},
        {"probe_log_dir", options.probe_log_dir},
        {"state_dir", options.state_dir},
    };

    state.process = CollectProcessStatus(options);
    state.contracts = DiscoverContracts(options.ctp_instrument_dir);
    state.markets = CollectMarketStatus(options);
    state.wal = CollectWalStatus(options);
    state.daily = CollectDailyStatus(options);
    state.signal_monitor = CollectSignalMonitorStatus(options);
    state.readiness = CollectReadinessStatus(options);
    state.ctp_connection = CollectCtpConnectionStatus(options, state.process, state.contracts);
    ApplyStructuredReadiness(state.readiness, &state.ctp_connection);
    state.ctp_order_flow =
        CollectCtpOrderFlowStatus(options, state.signal_monitor, state.wal, state.process);
    state.positions = CollectPositions(options, &state.positions_source);
    state.recent_alerts = CollectRecentAlerts(state.process.core_log);

    const bool waiting_for_window = state.process.status == "waiting_for_trading_window";
    bool market_healthy = true;
    if (!state.markets.empty()) {
        market_healthy = std::all_of(state.markets.begin(), state.markets.end(),
                                     [](const auto& item) { return item.healthy; });
    }
    const bool structured_readiness_healthy =
        !state.readiness.found || state.readiness.healthy || waiting_for_window;
    const bool dominant_contracts_healthy =
        state.contracts.empty() ||
        std::all_of(state.contracts.begin(), state.contracts.end(),
                    [](const auto& contract) { return contract.healthy; });
    state.live_healthy = state.process.healthy && structured_readiness_healthy &&
                         (waiting_for_window || market_healthy) && state.signal_monitor.healthy &&
                         state.ctp_connection.healthy && dominant_contracts_healthy &&
                         state.ctp_order_flow.monitor_incidents == 0 &&
                         state.ctp_order_flow.ctp_submit_rejected == 0;
    state.overall_healthy = state.live_healthy;

    if (!state.process.supervisor_pids.empty() && state.process.status != "alive") {
        if (!waiting_for_window) {
            ++state.live_warning_count;
        }
    }
    if (!state.wal.exists) {
        ++state.live_warning_count;
    }
    if (!state.daily.report_found && !state.daily.trading_day.empty()) {
        ++state.historical_risk_count;
    }
    if (state.daily.parse_errors > 0) {
        ++state.historical_risk_count;
    }
    if (state.signal_monitor.status == "dead") {
        ++state.live_warning_count;
    }
    if (state.signal_monitor.incidents > 0) {
        ++state.live_warning_count;
    }
    if (state.ctp_connection.status == "degraded") {
        ++state.live_warning_count;
    }
    if (state.readiness.found && !state.readiness.healthy && !waiting_for_window) {
        ++state.live_warning_count;
    }
    if (state.ctp_order_flow.status == "attention") {
        if (state.ctp_order_flow.monitor_incidents > 0 ||
            state.ctp_order_flow.ctp_submit_rejected > 0) {
            ++state.live_warning_count;
        } else {
            ++state.historical_risk_count;
        }
    }
    for (const auto& item : state.markets) {
        if (!item.healthy && !waiting_for_window) {
            ++state.live_warning_count;
        }
    }
    for (const auto& contract : state.contracts) {
        if (!contract.healthy) {
            ++state.live_warning_count;
        }
        if (contract.phase == "pending_flat" && contract.phase_age_seconds.has_value() &&
            *contract.phase_age_seconds > 15 * 60) {
            state.recent_alerts.push_back("dominant_contract_pending_flat_stalled product=" +
                                          contract.product);
        }
        if (contract.phase == "warming" && contract.phase_age_seconds.has_value() &&
            *contract.phase_age_seconds > 30 * 60) {
            state.recent_alerts.push_back("dominant_contract_warming_stalled product=" +
                                          contract.product);
        }
        if (contract.cache_trading_day_mismatch) {
            state.recent_alerts.push_back("dominant_contract_cache_trading_day_mismatch product=" +
                                          contract.product);
        }
        if (contract.generation_rejections > 0) {
            state.recent_alerts.push_back(
                "dominant_contract_generation_rejected product=" + contract.product +
                " count=" + std::to_string(contract.generation_rejections));
        }
    }
    if (state.ctp_order_flow.wal_rejected > 0) {
        ++state.historical_risk_count;
    }
    if (state.daily.critical_alerts > 0 || state.daily.warn_alerts > 0) {
        ++state.historical_risk_count;
    }
    state.warning_count = state.live_warning_count + state.historical_risk_count;
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
