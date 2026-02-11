#pragma once

#include <string>
#include <vector>

namespace quant_hft {

enum class CtpEnvironment {
    kSimNow,
    kProduction,
};

struct CtpRuntimeConfig {
    CtpEnvironment environment{CtpEnvironment::kSimNow};
    bool is_production_mode{false};
    bool enable_real_api{false};
    bool enable_terminal_auth{true};
    int connect_timeout_ms{10'000};
    int reconnect_max_attempts{8};
    int reconnect_initial_backoff_ms{500};
    int reconnect_max_backoff_ms{8'000};

    std::string md_front;
    std::string td_front;
    std::string flow_path;

    std::string broker_id;
    std::string user_id;
    std::string investor_id;
    std::string password;
    std::string app_id;
    std::string auth_code;

    // v6.7.11 field support.
    std::string last_login_time;
    std::string reserve_info;
    char offset_apply_src{'0'};
};

struct CtpFrontPair {
    std::string md_front;
    std::string td_front;
};

enum class ExecutionMode {
    kDirect,
    kSliced,
};

enum class ExecutionAlgo {
    kDirect,
    kSliced,
    kTwap,
    kVwapLite,
};

struct ExecutionConfig {
    ExecutionMode mode{ExecutionMode::kDirect};
    ExecutionAlgo algo{ExecutionAlgo::kDirect};
    int slice_size{1};
    int slice_interval_ms{200};
    int twap_duration_ms{0};
    int vwap_lookback_bars{20};
    double throttle_reject_ratio{0.0};
    // 0 disables timeout-based cancel requests.
    int cancel_after_ms{0};
    int cancel_check_interval_ms{200};
};

struct RiskRuleConfig {
    std::string rule_id;
    std::string rule_group;
    std::string rule_version{"v1"};
    std::string policy_id;
    std::string policy_scope;
    std::string decision_tags;
    std::string account_id;
    std::string instrument_id;
    int window_start_hhmm{0};
    int window_end_hhmm{2359};
    int max_order_volume{200};
    double max_order_notional{1'000'000.0};
    int max_active_orders{0};
    double max_position_notional{0.0};
};

struct RiskConfig {
    int default_max_order_volume{200};
    double default_max_order_notional{1'000'000.0};
    int default_max_active_orders{0};
    double default_max_position_notional{0.0};
    std::string default_rule_group{"default"};
    std::string default_rule_version{"v1"};
    std::string default_policy_id{"policy.global"};
    std::string default_policy_scope{"global"};
    std::string default_decision_tags;
    std::vector<RiskRuleConfig> rules;
};

// Build candidate front pairs for connection retries.
// - always includes the configured pair first.
// - for known SimNow trading-hours groups (30001/11, 30002/12, 30003/13),
//   append alternate groups on the same host.
std::vector<CtpFrontPair> BuildCtpFrontCandidates(const std::string& md_front,
                                                  const std::string& td_front);

class CtpConfigValidator {
public:
    static bool Validate(const CtpRuntimeConfig& config, std::string* error);
};

}  // namespace quant_hft
