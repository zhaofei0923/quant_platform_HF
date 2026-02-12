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
    bool settlement_confirm_required{true};
    int connect_timeout_ms{10'000};
    int reconnect_max_attempts{8};
    int reconnect_initial_backoff_ms{500};
    int reconnect_max_backoff_ms{8'000};
    int recovery_quiet_period_ms{3'000};
    int query_retry_backoff_ms{200};
    int order_insert_rate_per_sec{50};
    int order_cancel_rate_per_sec{50};
    int query_rate_per_sec{5};
    int settlement_query_rate_per_sec{2};
    int order_bucket_capacity{20};
    int cancel_bucket_capacity{20};
    int query_bucket_capacity{5};
    int settlement_query_bucket_capacity{2};
    int settlement_retry_max{3};
    int settlement_retry_backoff_initial_ms{1'000};
    int settlement_retry_backoff_max_ms{5'000};
    int settlement_running_stale_timeout_ms{300'000};
    bool settlement_shadow_enabled{false};
    int cancel_retry_max{3};
    int cancel_retry_base_ms{1'000};
    int cancel_retry_max_delay_ms{5'000};
    int cancel_wait_ack_timeout_ms{1'200};
    int breaker_failure_threshold{5};
    int breaker_timeout_ms{1'000};
    int breaker_half_open_timeout_ms{5'000};
    bool breaker_strategy_enabled{true};
    bool breaker_account_enabled{true};
    bool breaker_system_enabled{true};
    int audit_hot_days{7};
    int audit_cold_days{180};

    std::string md_front;
    std::string td_front;
    std::string flow_path;

    std::string broker_id;
    std::string user_id;
    std::string investor_id;
    std::string password;
    std::string app_id;
    std::string auth_code;
    std::string kafka_bootstrap_servers;
    std::string kafka_topic_ticks{"market.ticks.v1"};
    std::string clickhouse_dsn;

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
    std::string preferred_venue{"SIM"};
    double participation_rate_limit{1.0};
    double impact_cost_bps{0.0};
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
    std::string exchange_id;
    int window_start_hhmm{0};
    int window_end_hhmm{2359};
    int max_order_volume{200};
    double max_order_notional{1'000'000.0};
    int max_active_orders{0};
    double max_position_notional{0.0};
    int max_cancel_count{0};
    double max_cancel_ratio{0.0};
};

struct RiskConfig {
    int default_max_order_volume{200};
    double default_max_order_notional{1'000'000.0};
    int default_max_active_orders{0};
    double default_max_position_notional{0.0};
    int default_max_cancel_count{0};
    double default_max_cancel_ratio{0.0};
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
