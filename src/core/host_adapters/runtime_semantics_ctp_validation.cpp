#include "quant_hft/core/runtime_semantics_loader.h"
#include "quant_hft/core/ctp_config_loader.h"
#include <sstream>
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
}
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
}
