#pragma once

#include <algorithm>
#include <cctype>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <iomanip>
#include <map>
#include <optional>
#include <set>
#include <sstream>
#include <string>
#include <vector>

#include "quant_hft/apps/cli_support.h"

namespace quant_hft::apps {

namespace ops_detail {

inline std::string ToLower(std::string text) {
    std::transform(text.begin(), text.end(), text.begin(),
                   [](unsigned char ch) { return static_cast<char>(std::tolower(ch)); });
    return text;
}

inline std::string Trim(std::string text) {
    const auto is_space = [](unsigned char ch) { return std::isspace(ch) != 0; };
    while (!text.empty() && is_space(static_cast<unsigned char>(text.front()))) {
        text.erase(text.begin());
    }
    while (!text.empty() && is_space(static_cast<unsigned char>(text.back()))) {
        text.pop_back();
    }
    return text;
}

inline bool ParseBoolText(const std::string& raw, bool* out) {
    if (out == nullptr) {
        return false;
    }
    const std::string value = ToLower(Trim(raw));
    if (value == "1" || value == "true" || value == "yes" || value == "on") {
        *out = true;
        return true;
    }
    if (value == "0" || value == "false" || value == "no" || value == "off") {
        *out = false;
        return true;
    }
    return false;
}

inline bool ParseDoubleText(const std::string& raw, double* out) {
    if (out == nullptr) {
        return false;
    }
    const std::string value = Trim(raw);
    if (value.empty()) {
        return false;
    }
    try {
        std::size_t parsed = 0;
        const double number = std::stod(value, &parsed);
        if (parsed != value.size()) {
            return false;
        }
        *out = number;
        return true;
    } catch (...) {
        return false;
    }
}

inline bool ParseInt64Text(const std::string& raw, std::int64_t* out) {
    if (out == nullptr) {
        return false;
    }
    const std::string value = Trim(raw);
    if (value.empty()) {
        return false;
    }
    try {
        std::size_t parsed = 0;
        const std::int64_t number = std::stoll(value, &parsed);
        if (parsed != value.size()) {
            return false;
        }
        *out = number;
        return true;
    } catch (...) {
        return false;
    }
}

inline std::string JsonEscape(const std::string& text) {
    std::ostringstream oss;
    for (const char ch : text) {
        switch (ch) {
            case '"':
                oss << "\\\"";
                break;
            case '\\':
                oss << "\\\\";
                break;
            case '\n':
                oss << "\\n";
                break;
            case '\r':
                oss << "\\r";
                break;
            case '\t':
                oss << "\\t";
                break;
            default:
                oss << ch;
                break;
        }
    }
    return oss.str();
}

inline std::string WithPrefix(const std::string& name) {
    static const std::string kPrefix = "quant_hft_";
    if (name.rfind(kPrefix, 0) == 0) {
        return name;
    }
    return kPrefix + name;
}

inline std::string StripPrefix(const std::string& name) {
    static const std::string kPrefix = "quant_hft_";
    if (name.rfind(kPrefix, 0) == 0) {
        return name.substr(kPrefix.size());
    }
    return name;
}

inline std::optional<bool> NormalizeHealth(const std::string& raw) {
    const std::string value = ToLower(Trim(raw));
    if (value == "healthy" || value == "ok" || value == "true" || value == "1") {
        return true;
    }
    if (value == "unhealthy" || value == "failed" || value == "false" || value == "0") {
        return false;
    }
    return std::nullopt;
}

inline std::optional<bool> NormalizeChainStatus(const std::string& raw) {
    const std::string value = ToLower(Trim(raw));
    if (value == "complete" || value == "ok" || value == "healthy" || value == "true" ||
        value == "1") {
        return true;
    }
    if (value == "incomplete" || value == "broken" || value == "unhealthy" || value == "false" ||
        value == "0") {
        return false;
    }
    return std::nullopt;
}

inline std::string UpperSnake(const std::string& text) {
    std::ostringstream oss;
    bool last_was_underscore = false;
    for (const unsigned char ch : text) {
        if (std::isalnum(ch) != 0) {
            oss << static_cast<char>(std::toupper(ch));
            last_was_underscore = false;
            continue;
        }
        if (!last_was_underscore) {
            oss << '_';
            last_was_underscore = true;
        }
    }
    return oss.str();
}

inline std::string FormatNumber(double value) {
    std::ostringstream oss;
    oss << std::setprecision(12) << value;
    return oss.str();
}

inline bool ExtractJsonString(const std::string& json, const std::string& key, std::string* out) {
    if (out == nullptr) {
        return false;
    }
    const std::string quoted_key = "\"" + key + "\"";
    const std::size_t key_pos = json.find(quoted_key);
    if (key_pos == std::string::npos) {
        return false;
    }
    const std::size_t colon_pos = json.find(':', key_pos + quoted_key.size());
    if (colon_pos == std::string::npos) {
        return false;
    }

    std::size_t pos = colon_pos + 1;
    while (pos < json.size() && std::isspace(static_cast<unsigned char>(json[pos])) != 0) {
        ++pos;
    }
    if (pos >= json.size() || json[pos] != '"') {
        return false;
    }
    ++pos;

    std::string value;
    bool escaped = false;
    while (pos < json.size()) {
        const char ch = json[pos++];
        if (escaped) {
            switch (ch) {
                case '"':
                    value.push_back('"');
                    break;
                case '\\':
                    value.push_back('\\');
                    break;
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
            *out = value;
            return true;
        }
        value.push_back(ch);
    }
    return false;
}

inline bool ExtractJsonBool(const std::string& json, const std::string& key, bool* out) {
    if (out == nullptr) {
        return false;
    }
    const std::string quoted_key = "\"" + key + "\"";
    const std::size_t key_pos = json.find(quoted_key);
    if (key_pos == std::string::npos) {
        return false;
    }
    const std::size_t colon_pos = json.find(':', key_pos + quoted_key.size());
    if (colon_pos == std::string::npos) {
        return false;
    }
    std::size_t pos = colon_pos + 1;
    while (pos < json.size() && std::isspace(static_cast<unsigned char>(json[pos])) != 0) {
        ++pos;
    }
    if (pos + 4 <= json.size() && json.compare(pos, 4, "true") == 0) {
        *out = true;
        return true;
    }
    if (pos + 5 <= json.size() && json.compare(pos, 5, "false") == 0) {
        *out = false;
        return true;
    }
    return false;
}

inline bool ExtractJsonInt64(const std::string& json, const std::string& key, std::int64_t* out) {
    if (out == nullptr) {
        return false;
    }
    const std::string quoted_key = "\"" + key + "\"";
    const std::size_t key_pos = json.find(quoted_key);
    if (key_pos == std::string::npos) {
        return false;
    }
    const std::size_t colon_pos = json.find(':', key_pos + quoted_key.size());
    if (colon_pos == std::string::npos) {
        return false;
    }
    std::size_t start = colon_pos + 1;
    while (start < json.size() && std::isspace(static_cast<unsigned char>(json[start])) != 0) {
        ++start;
    }
    std::size_t end = start;
    while (end < json.size() &&
           (std::isdigit(static_cast<unsigned char>(json[end])) != 0 || json[end] == '-')) {
        ++end;
    }
    if (end <= start) {
        return false;
    }
    return ParseInt64Text(json.substr(start, end - start), out);
}

inline std::string NowUtcIso8601Z() {
    const auto now = std::chrono::system_clock::now();
    const std::time_t now_time = std::chrono::system_clock::to_time_t(now);
    std::tm tm = {};
#if defined(_WIN32)
    gmtime_s(&tm, &now_time);
#else
    gmtime_r(&now_time, &tm);
#endif
    std::ostringstream oss;
    oss << std::put_time(&tm, "%Y-%m-%dT%H:%M:%SZ");
    return oss.str();
}

}  // namespace ops_detail

struct OpsSliRecord {
    std::string name;
    std::string slo_name;
    std::string environment;
    std::string service;
    std::optional<double> value;
    std::optional<double> target;
    std::string unit;
    bool healthy{false};
    std::string detail;
};

struct OpsHealthReport {
    std::int64_t generated_ts_ns{0};
    std::string scope{"core_engine + strategy_engine + storage"};
    bool overall_healthy{false};
    std::map<std::string, std::string> metadata;
    std::vector<OpsSliRecord> slis;
};

struct OpsAlertItem {
    std::string code;
    std::string severity;
    std::string message;
    std::string sli_name;
};

struct OpsAlertReport {
    std::int64_t generated_ts_ns{0};
    bool overall_healthy{false};
    std::vector<OpsAlertItem> alerts;
};

struct OpsHealthBuildOptions {
    std::optional<double> strategy_engine_latency_ms;
    double strategy_engine_target_ms{1500.0};
    std::string strategy_engine_chain_status{"unknown"};
    bool core_process_alive{true};
    std::string redis_health{"unknown"};
    std::string timescale_health{"unknown"};
    std::string postgres_health;
    std::string scope{"core_engine + strategy_engine + storage"};
    std::string environment{"unknown"};
    std::string service{"core_engine"};
    std::map<std::string, std::string> metadata;
};

inline void AddBoolSli(std::vector<OpsSliRecord>* slis, const std::string& base_name,
                       const std::string& environment, const std::string& service,
                       const std::optional<bool>& health, const std::string& detail) {
    if (slis == nullptr) {
        return;
    }
    OpsSliRecord record;
    record.name = ops_detail::WithPrefix(base_name);
    record.slo_name = record.name;
    record.environment = environment;
    record.service = service;
    if (health.has_value()) {
        record.value = *health ? 1.0 : 0.0;
    }
    record.target = 1.0;
    record.unit = "bool";
    record.healthy = health.value_or(false);
    record.detail = detail;
    slis->push_back(std::move(record));
}

inline OpsHealthReport BuildOpsHealthReport(const OpsHealthBuildOptions& options) {
    OpsHealthReport report;
    report.generated_ts_ns = static_cast<std::int64_t>(
        std::chrono::time_point_cast<std::chrono::nanoseconds>(std::chrono::system_clock::now())
            .time_since_epoch()
            .count());
    report.scope = options.scope;
    report.metadata = options.metadata;
    report.metadata["environment"] = options.environment;
    report.metadata["service"] = options.service;

    AddBoolSli(&report.slis, "core_process_alive", options.environment, options.service,
               options.core_process_alive, "probe process stayed alive during collection");

    OpsSliRecord latency;
    latency.name = ops_detail::WithPrefix("strategy_engine_latency_p99_ms");
    latency.slo_name = latency.name;
    latency.environment = options.environment;
    latency.service = options.service;
    latency.value = options.strategy_engine_latency_ms;
    latency.target = options.strategy_engine_target_ms;
    latency.unit = "ms";
    latency.healthy = options.strategy_engine_latency_ms.has_value() &&
                      *options.strategy_engine_latency_ms <= options.strategy_engine_target_ms;
    latency.detail = "derived from reconnect recovery samples";
    report.slis.push_back(std::move(latency));

    AddBoolSli(&report.slis, "strategy_engine_chain_integrity", options.environment,
               options.service,
               ops_detail::NormalizeChainStatus(options.strategy_engine_chain_status),
               "input=" + options.strategy_engine_chain_status);

    AddBoolSli(&report.slis, "storage_redis_health", options.environment, options.service,
               ops_detail::NormalizeHealth(options.redis_health), "input=" + options.redis_health);

    AddBoolSli(&report.slis, "storage_timescale_health", options.environment, options.service,
               ops_detail::NormalizeHealth(options.timescale_health),
               "input=" + options.timescale_health);

    const std::string postgres_input = ops_detail::Trim(options.postgres_health).empty()
                                           ? options.timescale_health
                                           : options.postgres_health;
    AddBoolSli(&report.slis, "storage_postgres_health", options.environment, options.service,
               ops_detail::NormalizeHealth(postgres_input), "input=" + postgres_input);

    report.overall_healthy =
        !report.slis.empty() && std::all_of(report.slis.begin(), report.slis.end(),
                                            [](const OpsSliRecord& item) { return item.healthy; });
    return report;
}

inline OpsAlertReport EvaluateOpsAlertPolicy(const OpsHealthReport& report) {
    OpsAlertReport alert_report;
    alert_report.generated_ts_ns = report.generated_ts_ns;
    alert_report.overall_healthy = report.overall_healthy;

    const std::set<std::string> critical_sli_names = {
        "core_process_alive",
        "strategy_engine_chain_integrity",
        "storage_redis_health",
        "storage_timescale_health",
    };

    for (const OpsSliRecord& sli : report.slis) {
        if (sli.healthy) {
            continue;
        }
        const std::string base_name = ops_detail::StripPrefix(sli.name);
        const bool critical = critical_sli_names.find(base_name) != critical_sli_names.end();

        OpsAlertItem item;
        item.code = "OPS_" + ops_detail::UpperSnake(base_name) + "_UNHEALTHY";
        item.severity = critical ? "critical" : "warn";
        item.message = base_name + " unhealthy: " + sli.detail;
        item.sli_name = ops_detail::WithPrefix(base_name);
        alert_report.alerts.push_back(std::move(item));
    }

    if (alert_report.alerts.empty()) {
        OpsAlertItem info;
        info.code = "OPS_ALL_HEALTHY";
        info.severity = "info";
        info.message = "all SLI checks are healthy";
        info.sli_name = ops_detail::WithPrefix("core_process_alive");
        alert_report.alerts.push_back(std::move(info));
    }

    return alert_report;
}

inline std::string OpsHealthReportToJson(const OpsHealthReport& report) {
    std::ostringstream oss;
    oss << "{\n";
    oss << "  \"generated_ts_ns\": " << report.generated_ts_ns << ",\n";
    oss << "  \"scope\": \"" << ops_detail::JsonEscape(report.scope) << "\",\n";
    oss << "  \"overall_healthy\": " << (report.overall_healthy ? "true" : "false") << ",\n";

    oss << "  \"metadata\": {\n";
    std::size_t metadata_index = 0;
    for (const auto& [key, value] : report.metadata) {
        oss << "    \"" << ops_detail::JsonEscape(key) << "\": \"" << ops_detail::JsonEscape(value)
            << "\"";
        if (++metadata_index < report.metadata.size()) {
            oss << ',';
        }
        oss << "\n";
    }
    oss << "  },\n";

    oss << "  \"slis\": [\n";
    for (std::size_t index = 0; index < report.slis.size(); ++index) {
        const OpsSliRecord& sli = report.slis[index];
        oss << "    {\n";
        oss << "      \"name\": \"" << ops_detail::JsonEscape(sli.name) << "\",\n";
        oss << "      \"slo_name\": \"" << ops_detail::JsonEscape(sli.slo_name) << "\",\n";
        oss << "      \"environment\": \"" << ops_detail::JsonEscape(sli.environment) << "\",\n";
        oss << "      \"service\": \"" << ops_detail::JsonEscape(sli.service) << "\",\n";
        oss << "      \"value\": ";
        if (sli.value.has_value()) {
            oss << ops_detail::FormatNumber(*sli.value);
        } else {
            oss << "null";
        }
        oss << ",\n";
        oss << "      \"target\": ";
        if (sli.target.has_value()) {
            oss << ops_detail::FormatNumber(*sli.target);
        } else {
            oss << "null";
        }
        oss << ",\n";
        oss << "      \"unit\": \"" << ops_detail::JsonEscape(sli.unit) << "\",\n";
        oss << "      \"healthy\": " << (sli.healthy ? "true" : "false") << ",\n";
        oss << "      \"detail\": \"" << ops_detail::JsonEscape(sli.detail) << "\"\n";
        oss << "    }";
        if (index + 1 < report.slis.size()) {
            oss << ',';
        }
        oss << "\n";
    }
    oss << "  ]\n";
    oss << "}\n";
    return oss.str();
}

inline std::string RenderOpsHealthMarkdown(const OpsHealthReport& report) {
    std::ostringstream oss;
    oss << "# Ops Health Report\n\n";
    oss << "- Scope: " << report.scope << "\n";
    oss << "- Generated TS (ns): " << report.generated_ts_ns << "\n";
    oss << "- Overall healthy: " << (report.overall_healthy ? "yes" : "no") << "\n\n";
    oss << "## SLI\n";
    oss << "| Name | Value | Target | Healthy | Detail |\n";
    oss << "|---|---:|---:|---|---|\n";

    for (const OpsSliRecord& sli : report.slis) {
        const std::string value =
            sli.value.has_value() ? ops_detail::FormatNumber(*sli.value) : "n/a";
        const std::string target =
            sli.target.has_value() ? ops_detail::FormatNumber(*sli.target) : "n/a";
        oss << "| " << sli.name << " | " << value << " | " << target << " | "
            << (sli.healthy ? "yes" : "no") << " | " << sli.detail << " |\n";
    }

    if (!report.metadata.empty()) {
        oss << "\n## Metadata\n";
        for (const auto& [key, value] : report.metadata) {
            oss << "- " << key << ": " << value << "\n";
        }
    }
    return oss.str();
}

inline std::string OpsAlertReportToJson(const OpsAlertReport& report) {
    std::ostringstream oss;
    oss << "{\n";
    oss << "  \"generated_ts_ns\": " << report.generated_ts_ns << ",\n";
    oss << "  \"overall_healthy\": " << (report.overall_healthy ? "true" : "false") << ",\n";
    oss << "  \"alerts\": [\n";
    for (std::size_t index = 0; index < report.alerts.size(); ++index) {
        const OpsAlertItem& item = report.alerts[index];
        oss << "    {\n";
        oss << "      \"code\": \"" << ops_detail::JsonEscape(item.code) << "\",\n";
        oss << "      \"severity\": \"" << ops_detail::JsonEscape(item.severity) << "\",\n";
        oss << "      \"message\": \"" << ops_detail::JsonEscape(item.message) << "\",\n";
        oss << "      \"sli_name\": \"" << ops_detail::JsonEscape(item.sli_name) << "\"\n";
        oss << "    }";
        if (index + 1 < report.alerts.size()) {
            oss << ',';
        }
        oss << "\n";
    }
    oss << "  ]\n";
    oss << "}\n";
    return oss.str();
}

inline std::string RenderOpsAlertMarkdown(const OpsAlertReport& report) {
    std::ostringstream oss;
    oss << "# Ops Alert Report\n\n";
    oss << "- Generated TS (ns): " << report.generated_ts_ns << "\n";
    oss << "- Overall healthy: " << (report.overall_healthy ? "yes" : "no") << "\n\n";
    oss << "| Code | Severity | SLI | Message |\n";
    oss << "|---|---|---|---|\n";
    for (const OpsAlertItem& item : report.alerts) {
        oss << "| " << item.code << " | " << item.severity << " | " << item.sli_name << " | "
            << item.message << " |\n";
    }
    return oss.str();
}

inline bool ParseOpsHealthReportJson(const std::string& json, OpsHealthReport* out) {
    if (out == nullptr) {
        return false;
    }

    OpsHealthReport parsed;
    parsed.generated_ts_ns = UnixEpochMillisNow() * 1'000'000LL;
    parsed.scope = "core_engine + strategy_engine + storage";
    parsed.overall_healthy = false;

    std::int64_t generated_ts = 0;
    if (ops_detail::ExtractJsonInt64(json, "generated_ts_ns", &generated_ts)) {
        parsed.generated_ts_ns = generated_ts;
    }
    bool overall = false;
    if (ops_detail::ExtractJsonBool(json, "overall_healthy", &overall)) {
        parsed.overall_healthy = overall;
    }

    const std::size_t slis_key = json.find("\"slis\"");
    if (slis_key != std::string::npos) {
        const std::size_t open_bracket = json.find('[', slis_key);
        if (open_bracket != std::string::npos) {
            int depth = 0;
            std::size_t close_bracket = std::string::npos;
            for (std::size_t idx = open_bracket; idx < json.size(); ++idx) {
                if (json[idx] == '[') {
                    ++depth;
                } else if (json[idx] == ']') {
                    --depth;
                    if (depth == 0) {
                        close_bracket = idx;
                        break;
                    }
                }
            }

            if (close_bracket != std::string::npos) {
                const std::string slis_payload =
                    json.substr(open_bracket + 1, close_bracket - open_bracket - 1);
                std::size_t cursor = 0;
                while (cursor < slis_payload.size()) {
                    while (cursor < slis_payload.size() && slis_payload[cursor] != '{') {
                        ++cursor;
                    }
                    if (cursor >= slis_payload.size()) {
                        break;
                    }
                    const std::size_t object_start = cursor;
                    int object_depth = 0;
                    std::size_t object_end = std::string::npos;
                    for (; cursor < slis_payload.size(); ++cursor) {
                        if (slis_payload[cursor] == '{') {
                            ++object_depth;
                        } else if (slis_payload[cursor] == '}') {
                            --object_depth;
                            if (object_depth == 0) {
                                object_end = cursor;
                                ++cursor;
                                break;
                            }
                        }
                    }
                    if (object_end == std::string::npos) {
                        break;
                    }

                    const std::string object =
                        slis_payload.substr(object_start, object_end - object_start + 1);
                    OpsSliRecord sli;
                    if (!ops_detail::ExtractJsonString(object, "name", &sli.name)) {
                        continue;
                    }
                    ops_detail::ExtractJsonString(object, "slo_name", &sli.slo_name);
                    ops_detail::ExtractJsonString(object, "environment", &sli.environment);
                    ops_detail::ExtractJsonString(object, "service", &sli.service);
                    ops_detail::ExtractJsonString(object, "unit", &sli.unit);
                    ops_detail::ExtractJsonString(object, "detail", &sli.detail);
                    bool healthy = false;
                    ops_detail::ExtractJsonBool(object, "healthy", &healthy);
                    sli.healthy = healthy;
                    if (sli.slo_name.empty()) {
                        sli.slo_name = sli.name;
                    }
                    parsed.slis.push_back(std::move(sli));
                }
            }
        }
    }

    *out = std::move(parsed);
    return true;
}

}  // namespace quant_hft::apps
