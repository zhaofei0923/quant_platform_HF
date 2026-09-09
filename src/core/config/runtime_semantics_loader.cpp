#include "quant_hft/core/runtime_semantics_loader.h"

#include <algorithm>
#include <cctype>
#include <cmath>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <sstream>
#include <unordered_set>

#include "quant_hft/core/ctp_config_loader.h"

namespace quant_hft {
namespace {
std::string Trim(std::string value) {
    const auto first = value.find_first_not_of(" \t\r\n");
    if (first == std::string::npos) return {};
    value = value.substr(first, value.find_last_not_of(" \t\r\n") - first + 1);
    if (value.size() >= 2 && ((value.front() == '"' && value.back() == '"') ||
                              (value.front() == '\'' && value.back() == '\'')))
        value = value.substr(1, value.size() - 2);
    return value;
}
std::string Fingerprint(const std::string& value) {
    std::uint64_t hash = 14695981039346656037ULL;
    for (const unsigned char ch : value) {
        hash ^= ch;
        hash *= 1099511628211ULL;
    }
    std::ostringstream text;
    text << std::hex << hash;
    return text.str();
}
bool Parse(const std::string& value, int* out) {
    try {
        std::size_t used = 0;
        const auto parsed = std::stoi(value, &used);
        if (used != value.size() || parsed < 0) return false;
        *out = parsed;
        return true;
    } catch (const std::exception&) {
        return false;
    }
}
bool Parse(const std::string& value, double* out) {
    try {
        std::size_t used = 0;
        const auto parsed = std::stod(value, &used);
        if (used != value.size() || !std::isfinite(parsed) || parsed < 0) return false;
        *out = parsed;
        return true;
    } catch (const std::exception&) {
        return false;
    }
}
bool Parse(const std::string& value, bool* out) {
    if (value == "true" || value == "1") {
        *out = true;
        return true;
    }
    if (value == "false" || value == "0") {
        *out = false;
        return true;
    }
    return false;
}
bool Parse(const std::string& value, std::string* out) {
    if (value.find("${") != std::string::npos ||
        value.find_first_of("\"\n\r\\") != std::string::npos)
        return false;
    *out = value;
    return true;
}
}  // namespace

bool ValidateRuntimeSemanticsAgainstCtpConfig(const RuntimeSemanticsConfig& shared,
                                              const CtpFileConfig& live, std::string* error) {
    const auto same = [error](bool equal, const char* name) {
        if (!equal && error) *error = std::string("runtime semantics mismatch: ") + name;
        return equal;
    };
    if (!same(shared.dominant_contract_recheck_interval_ms ==
                  live.dominant_contract_recheck_interval_ms,
              "dominant_contract_recheck_interval_ms"))
        return false;
    if (!same(shared.dominant_contract_min_lead_ratio == live.dominant_contract_min_lead_ratio,
              "dominant_contract_min_lead_ratio"))
        return false;
    if (!same(shared.dominant_contract_min_lead_windows == live.dominant_contract_min_lead_windows,
              "dominant_contract_min_lead_windows"))
        return false;
    if (!same(shared.dominant_contract_min_hold_ms == live.dominant_contract_min_hold_ms,
              "dominant_contract_min_hold_ms"))
        return false;
    if (!same(shared.dominant_contract_max_tick_age_ms == live.dominant_contract_max_tick_age_ms,
              "dominant_contract_max_tick_age_ms"))
        return false;
    if (!same(shared.dominant_contract_warmup_bars == live.dominant_contract_warmup_bars,
              "dominant_contract_warmup_bars"))
        return false;
    if (!same(shared.dominant_contract_require_complete_baseline ==
                  live.dominant_contract_require_complete_baseline,
              "dominant_contract_require_complete_baseline"))
        return false;
    if (!same(shared.dominant_contract_switch_mode == live.dominant_contract_switch_mode,
              "dominant_contract_switch_mode"))
        return false;
    if (!same(shared.session_gate_enabled == live.execution.session_gate_enabled,
              "session_gate_enabled"))
        return false;
    if (!same(shared.max_signal_age_ms == live.execution.max_signal_age_ms, "max_signal_age_ms"))
        return false;
    if (!same(shared.max_market_tick_age_ms == live.execution.max_market_tick_age_ms,
              "max_market_tick_age_ms"))
        return false;
    if (!same(shared.open_session_end_guard_ms == live.execution.open_session_end_guard_ms,
              "open_session_end_guard_ms"))
        return false;
    if (!same(shared.cancel_after_ms == live.execution.cancel_after_ms, "cancel_after_ms"))
        return false;
    if (!same(shared.market_bar_allowed_lateness_ms == live.market_bar.allowed_lateness_ms,
              "market_bar_allowed_lateness_ms"))
        return false;
    if (!same(shared.market_bar_poll_interval_ms == live.market_bar.poll_interval_ms,
              "market_bar_poll_interval_ms"))
        return false;
    if (!same(shared.market_event_delay_hard_ms == live.market_bar.event_delay_hard_ms,
              "market_event_delay_hard_ms"))
        return false;
    if (!same(
            shared.require_complete_timeframe_bar == live.market_bar.require_complete_timeframe_bar,
            "require_complete_timeframe_bar"))
        return false;
    if (!same(shared.risk_default_max_order_volume == live.risk.default_max_order_volume,
              "risk_default_max_order_volume"))
        return false;
    if (!same(shared.risk_default_max_order_notional == live.risk.default_max_order_notional,
              "risk_default_max_order_notional"))
        return false;
    if (!same(shared.risk_default_max_active_orders == live.risk.default_max_active_orders,
              "risk_default_max_active_orders"))
        return false;
    if (!same(shared.risk_default_max_position_notional == live.risk.default_max_position_notional,
              "risk_default_max_position_notional"))
        return false;
    if (!same(shared.risk_max_margin_to_equity_ratio == live.risk.max_margin_to_equity_ratio,
              "risk_max_margin_to_equity_ratio"))
        return false;
    if (!same(shared.risk_sim_subaccount_enabled == live.risk.sim_subaccount_enabled,
              "risk_sim_subaccount_enabled") ||
        !same(shared.order_insert_rate_per_sec == live.runtime.order_insert_rate_per_sec,
              "order_insert_rate_per_sec") ||
        !same(shared.order_cancel_rate_per_sec == live.runtime.order_cancel_rate_per_sec,
              "order_cancel_rate_per_sec"))
        return false;
    const std::string mode = live.execution.mode == ExecutionMode::kDirect ? "direct" : "sliced";
    std::string algo;
    switch (live.execution.algo) {
        case ExecutionAlgo::kDirect:
            algo = "direct";
            break;
        case ExecutionAlgo::kSliced:
            algo = "sliced";
            break;
        case ExecutionAlgo::kTwap:
            algo = "twap";
            break;
        case ExecutionAlgo::kVwapLite:
            algo = "vwap_lite";
            break;
    }
    const std::string price_mode = live.execution.price_mode == ExecutionPriceMode::kSignalLimit
                                       ? "signal_limit"
                                       : "marketable_limit";
    if (!same(shared.execution_mode == mode, "execution_mode") ||
        !same(shared.execution_algo == algo, "execution_algo") ||
        !same(shared.execution_price_mode == price_mode, "execution_price_mode"))
        return false;
    std::vector<std::string> groups;
    std::istringstream csv(shared.risk_rule_groups);
    std::string group;
    while (std::getline(csv, group, ',')) {
        group = Trim(group);
        if (!group.empty()) groups.push_back(group);
    }
    if (!same(groups.size() == live.risk.rules.size(), "risk_rule_groups")) return false;
    for (std::size_t i = 0; i < groups.size(); ++i) {
        if (!same(groups[i] == live.risk.rules[i].rule_group, "risk_rule_groups")) return false;
    }
    return true;
}

std::string RenderRuntimeSemanticsJson(const RuntimeSemanticsConfig& config) {
    std::ostringstream out;
    out << std::setprecision(17) << "{";
    out << "\"dominant_contract_recheck_interval_ms\":"
        << config.dominant_contract_recheck_interval_ms;
    out << ",\"dominant_contract_min_lead_ratio\":" << config.dominant_contract_min_lead_ratio;
    out << ",\"dominant_contract_min_lead_windows\":" << config.dominant_contract_min_lead_windows;
    out << ",\"dominant_contract_min_hold_ms\":" << config.dominant_contract_min_hold_ms;
    out << ",\"dominant_contract_max_tick_age_ms\":" << config.dominant_contract_max_tick_age_ms;
    out << ",\"dominant_contract_warmup_bars\":" << config.dominant_contract_warmup_bars;
    out << ",\"dominant_contract_require_complete_baseline\":"
        << (config.dominant_contract_require_complete_baseline ? "true" : "false");
    out << ",\"dominant_contract_switch_mode\":\"" << config.dominant_contract_switch_mode << "\"";
    out << ",\"session_gate_enabled\":" << (config.session_gate_enabled ? "true" : "false");
    out << ",\"max_signal_age_ms\":" << config.max_signal_age_ms;
    out << ",\"max_market_tick_age_ms\":" << config.max_market_tick_age_ms;
    out << ",\"open_session_end_guard_ms\":" << config.open_session_end_guard_ms;
    out << ",\"market_bar_allowed_lateness_ms\":" << config.market_bar_allowed_lateness_ms;
    out << ",\"market_bar_poll_interval_ms\":" << config.market_bar_poll_interval_ms;
    out << ",\"market_event_delay_hard_ms\":" << config.market_event_delay_hard_ms;
    out << ",\"require_complete_timeframe_bar\":"
        << (config.require_complete_timeframe_bar ? "true" : "false");
    out << ",\"execution_mode\":\"" << config.execution_mode << "\"";
    out << ",\"execution_algo\":\"" << config.execution_algo << "\"";
    out << ",\"execution_price_mode\":\"" << config.execution_price_mode << "\"";
    out << ",\"cancel_after_ms\":" << config.cancel_after_ms;
    out << ",\"risk_default_max_order_volume\":" << config.risk_default_max_order_volume;
    out << ",\"risk_default_max_order_notional\":" << config.risk_default_max_order_notional;
    out << ",\"risk_default_max_active_orders\":" << config.risk_default_max_active_orders;
    out << ",\"risk_default_max_position_notional\":" << config.risk_default_max_position_notional;
    out << ",\"risk_max_margin_to_equity_ratio\":" << config.risk_max_margin_to_equity_ratio;
    out << ",\"risk_rule_groups\":\"" << config.risk_rule_groups << "\"";
    out << ",\"risk_rule_file_path\":\"" << config.risk_rule_file_path << "\"";
    out << ",\"risk_rule_content_fingerprint\":\"" << config.risk_rule_content_fingerprint << "\"";
    out << ",\"risk_sim_subaccount_enabled\":"
        << (config.risk_sim_subaccount_enabled ? "true" : "false");
    out << ",\"order_insert_rate_per_sec\":" << config.order_insert_rate_per_sec;
    out << ",\"order_cancel_rate_per_sec\":" << config.order_cancel_rate_per_sec;
    out << "}";
    return out.str();
}

bool LoadRuntimeSemanticsConfig(const std::string& path, RuntimeSemanticsConfig* out,
                                std::string* error) {
    if (!out) {
        if (error) *error = "runtime semantics output is null";
        return false;
    }
    std::ifstream input(path);
    if (!input) {
        if (error) *error = "unable to open runtime semantics config: " + path;
        return false;
    }
    RuntimeSemanticsConfig parsed;
    std::ostringstream source;
    std::unordered_set<std::string> seen;
    std::string line;
    while (std::getline(input, line)) {
        source << line << '\n';
        const auto comment = line.find('#');
        if (comment != std::string::npos) line.resize(comment);
        const auto colon = line.find(':');
        if (colon == std::string::npos) continue;
        const auto key = Trim(line.substr(0, colon));
        bool recognized = false;
        bool valid = true;
        if (key == "dominant_contract_recheck_interval_ms") {
            recognized = true;
            valid =
                Parse(Trim(line.substr(colon + 1)), &parsed.dominant_contract_recheck_interval_ms);
        }
        if (key == "dominant_contract_min_lead_ratio") {
            recognized = true;
            valid = Parse(Trim(line.substr(colon + 1)), &parsed.dominant_contract_min_lead_ratio);
        }
        if (key == "dominant_contract_min_lead_windows") {
            recognized = true;
            valid = Parse(Trim(line.substr(colon + 1)), &parsed.dominant_contract_min_lead_windows);
        }
        if (key == "dominant_contract_min_hold_ms") {
            recognized = true;
            valid = Parse(Trim(line.substr(colon + 1)), &parsed.dominant_contract_min_hold_ms);
        }
        if (key == "dominant_contract_max_tick_age_ms") {
            recognized = true;
            valid = Parse(Trim(line.substr(colon + 1)), &parsed.dominant_contract_max_tick_age_ms);
        }
        if (key == "dominant_contract_warmup_bars") {
            recognized = true;
            valid = Parse(Trim(line.substr(colon + 1)), &parsed.dominant_contract_warmup_bars);
        }
        if (key == "dominant_contract_require_complete_baseline") {
            recognized = true;
            valid = Parse(Trim(line.substr(colon + 1)),
                          &parsed.dominant_contract_require_complete_baseline);
        }
        if (key == "dominant_contract_switch_mode") {
            recognized = true;
            valid = Parse(Trim(line.substr(colon + 1)), &parsed.dominant_contract_switch_mode);
        }
        if (key == "session_gate_enabled") {
            recognized = true;
            valid = Parse(Trim(line.substr(colon + 1)), &parsed.session_gate_enabled);
        }
        if (key == "max_signal_age_ms") {
            recognized = true;
            valid = Parse(Trim(line.substr(colon + 1)), &parsed.max_signal_age_ms);
        }
        if (key == "max_market_tick_age_ms") {
            recognized = true;
            valid = Parse(Trim(line.substr(colon + 1)), &parsed.max_market_tick_age_ms);
        }
        if (key == "open_session_end_guard_ms") {
            recognized = true;
            valid = Parse(Trim(line.substr(colon + 1)), &parsed.open_session_end_guard_ms);
        }
        if (key == "market_bar_allowed_lateness_ms") {
            recognized = true;
            valid = Parse(Trim(line.substr(colon + 1)), &parsed.market_bar_allowed_lateness_ms);
        }
        if (key == "market_bar_poll_interval_ms") {
            recognized = true;
            valid = Parse(Trim(line.substr(colon + 1)), &parsed.market_bar_poll_interval_ms);
        }
        if (key == "market_event_delay_hard_ms") {
            recognized = true;
            valid = Parse(Trim(line.substr(colon + 1)), &parsed.market_event_delay_hard_ms);
        }
        if (key == "require_complete_timeframe_bar") {
            recognized = true;
            valid = Parse(Trim(line.substr(colon + 1)), &parsed.require_complete_timeframe_bar);
        }
        if (key == "execution_mode") {
            recognized = true;
            valid = Parse(Trim(line.substr(colon + 1)), &parsed.execution_mode);
        }
        if (key == "execution_algo") {
            recognized = true;
            valid = Parse(Trim(line.substr(colon + 1)), &parsed.execution_algo);
        }
        if (key == "execution_price_mode") {
            recognized = true;
            valid = Parse(Trim(line.substr(colon + 1)), &parsed.execution_price_mode);
        }
        if (key == "cancel_after_ms") {
            recognized = true;
            valid = Parse(Trim(line.substr(colon + 1)), &parsed.cancel_after_ms);
        }
        if (key == "risk_default_max_order_volume") {
            recognized = true;
            valid = Parse(Trim(line.substr(colon + 1)), &parsed.risk_default_max_order_volume);
        }
        if (key == "risk_default_max_order_notional") {
            recognized = true;
            valid = Parse(Trim(line.substr(colon + 1)), &parsed.risk_default_max_order_notional);
        }
        if (key == "risk_default_max_active_orders") {
            recognized = true;
            valid = Parse(Trim(line.substr(colon + 1)), &parsed.risk_default_max_active_orders);
        }
        if (key == "risk_default_max_position_notional") {
            recognized = true;
            valid = Parse(Trim(line.substr(colon + 1)), &parsed.risk_default_max_position_notional);
        }
        if (key == "risk_max_margin_to_equity_ratio") {
            recognized = true;
            valid = Parse(Trim(line.substr(colon + 1)), &parsed.risk_max_margin_to_equity_ratio);
        }
        if (key == "risk_rule_groups") {
            recognized = true;
            valid = Parse(Trim(line.substr(colon + 1)), &parsed.risk_rule_groups);
        }
        if (key == "risk_rule_file_path") {
            recognized = true;
            valid = Parse(Trim(line.substr(colon + 1)), &parsed.risk_rule_file_path);
        }
        if (key == "risk_sim_subaccount_enabled") {
            recognized = true;
            valid = Parse(Trim(line.substr(colon + 1)), &parsed.risk_sim_subaccount_enabled);
        }
        if (key == "order_insert_rate_per_sec") {
            recognized = true;
            valid = Parse(Trim(line.substr(colon + 1)), &parsed.order_insert_rate_per_sec);
        }
        if (key == "order_cancel_rate_per_sec") {
            recognized = true;
            valid = Parse(Trim(line.substr(colon + 1)), &parsed.order_cancel_rate_per_sec);
        }
        if (!recognized) continue;
        if (!valid || !seen.insert(key).second) {
            if (error) *error = "invalid or duplicate runtime semantics key: " + key;
            return false;
        }
    }
    if (input.bad()) {
        if (error) *error = "failed reading runtime semantics config";
        return false;
    }
    const auto lowercase = [](std::string value) {
        std::transform(value.begin(), value.end(), value.begin(),
                       [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
        return value;
    };
    parsed.dominant_contract_switch_mode = lowercase(parsed.dominant_contract_switch_mode);
    if (parsed.dominant_contract_switch_mode.empty())
        parsed.dominant_contract_switch_mode = "startup_only";
    parsed.execution_mode = lowercase(parsed.execution_mode);
    if (parsed.execution_mode.empty()) parsed.execution_mode = "direct";
    parsed.execution_algo = lowercase(parsed.execution_algo);
    if (!seen.count("execution_algo") || parsed.execution_algo.empty())
        parsed.execution_algo = parsed.execution_mode == "sliced" ? "sliced" : "direct";
    if (parsed.execution_algo == "vwap-lite" || parsed.execution_algo == "vwap")
        parsed.execution_algo = "vwap_lite";
    parsed.execution_price_mode = lowercase(parsed.execution_price_mode);
    if (parsed.execution_price_mode.empty() || parsed.execution_price_mode == "limit" ||
        parsed.execution_price_mode == "signal-limit")
        parsed.execution_price_mode = "signal_limit";
    if (parsed.execution_price_mode == "market" ||
        parsed.execution_price_mode == "marketable-limit")
        parsed.execution_price_mode = "marketable_limit";
    if (parsed.dominant_contract_min_lead_windows < 1 || parsed.dominant_contract_warmup_bars < 1 ||
        parsed.market_bar_poll_interval_ms < 1 || parsed.risk_default_max_order_volume < 1 ||
        parsed.order_insert_rate_per_sec < 1 || parsed.order_cancel_rate_per_sec < 1) {
        if (error)
            *error =
                "runtime semantics requires positive lead windows, warmup, poll, order limit and "
                "gateway rates";
        return false;
    }
    if (!std::isfinite(parsed.risk_max_margin_to_equity_ratio) ||
        parsed.risk_max_margin_to_equity_ratio < 0.0 ||
        parsed.risk_max_margin_to_equity_ratio > 1.0) {
        if (error) *error = "risk_max_margin_to_equity_ratio must be in [0, 1]";
        return false;
    }
    if (!parsed.risk_rule_file_path.empty()) {
        namespace fs = std::filesystem;
        fs::path rules(parsed.risk_rule_file_path);
        std::error_code ec;
        if (!fs::exists(rules, ec) && rules.is_relative()) {
            const auto beside_config = fs::path(path).parent_path() / rules;
            if (fs::exists(beside_config, ec))
                rules = beside_config;
            else
                rules = fs::path(__FILE__).parent_path().parent_path().parent_path().parent_path() /
                        rules;
        }
        std::ifstream rule_input(rules, std::ios::binary);
        if (!rule_input) {
            if (error) *error = "unable to open risk rule snapshot: " + rules.string();
            return false;
        }
        std::ostringstream rule_content;
        rule_content << rule_input.rdbuf();
        if (rule_input.bad()) {
            if (error) *error = "unable to read risk rule snapshot";
            return false;
        }
        parsed.risk_rule_file_path = fs::weakly_canonical(rules, ec).string();
        if (ec) {
            if (error) *error = "unable to resolve risk rule snapshot";
            return false;
        }
        parsed.risk_rule_content_fingerprint = Fingerprint(rule_content.str());
    }
    parsed.source_content_fingerprint = Fingerprint(source.str());
    parsed.effective_fingerprint = Fingerprint(RenderRuntimeSemanticsJson(parsed));
    *out = std::move(parsed);
    return true;
}
}  // namespace quant_hft
