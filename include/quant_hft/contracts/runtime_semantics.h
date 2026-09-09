#pragma once

#include <cstdint>
#include <string>

namespace quant_hft {

// Pure decision parameters; no gateway endpoints, credentials, account IDs or environment lookup.
struct RuntimeSemanticsConfig {
    int dominant_contract_recheck_interval_ms{0};
    double dominant_contract_min_lead_ratio{0.15};
    int dominant_contract_min_lead_windows{3};
    int dominant_contract_min_hold_ms{900'000};
    int dominant_contract_max_tick_age_ms{6'000};
    int dominant_contract_warmup_bars{30};
    bool dominant_contract_require_complete_baseline{true};
    std::string dominant_contract_switch_mode{"startup_only"};
    bool session_gate_enabled{true};
    int max_signal_age_ms{2000};
    int max_market_tick_age_ms{6000};
    int open_session_end_guard_ms{30000};
    int market_bar_allowed_lateness_ms{3500};
    int market_bar_poll_interval_ms{100};
    int market_event_delay_hard_ms{5000};
    bool require_complete_timeframe_bar{true};
    std::string execution_mode{"direct"};
    std::string execution_algo{"direct"};
    std::string execution_price_mode{"signal_limit"};
    int cancel_after_ms{0};
    int risk_default_max_order_volume{200};
    double risk_default_max_order_notional{1'000'000};
    int risk_default_max_active_orders{0};
    double risk_default_max_position_notional{0};
    double risk_max_margin_to_equity_ratio{0};
    std::string risk_rule_groups{""};
    std::string risk_rule_file_path{"configs/risk_rules.yaml"};
    std::string risk_rule_content_fingerprint;
    bool risk_sim_subaccount_enabled{false};
    int order_insert_rate_per_sec{50};
    int order_cancel_rate_per_sec{50};
    std::string source_content_fingerprint;
    std::string effective_fingerprint;
};

}  // namespace quant_hft
