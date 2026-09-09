#pragma once

#include <cstdlib>
#include <regex>
#include <string>
#include <vector>

#include "quant_hft/contracts/execution_config.h"

namespace quant_hft {

inline std::string GetEnvOrDefault(const char* var, const std::string& def) {
    if (var == nullptr || std::string(var).empty()) {
        return def;
    }
    const char* value = std::getenv(var);
    return value == nullptr ? def : std::string(value);
}

inline std::string ResolveEnvVars(const std::string& input) {
    std::string result = input;
    static const std::regex env_pattern(R"(\$\{([^}]+)\})");
    std::smatch match;
    while (std::regex_search(result, match, env_pattern)) {
        const auto var_name = match[1].str();
        const char* env_val = std::getenv(var_name.c_str());
        const std::string replacement = env_val == nullptr ? "" : std::string(env_val);
        result.replace(match.position(0), match.length(0), replacement);
    }
    return result;
}

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
    int reconnect_cycle_cooldown_ms{8'000};
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
    bool metrics_enabled{false};
    int metrics_port{8080};

    std::string md_front;
    std::string log_level{"info"};
    std::string log_sink{"stderr"};
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

struct MarketBarRuntimeConfig {
    int allowed_lateness_ms{3'500};
    int poll_interval_ms{100};
    int checkpoint_interval_ms{1'000};
    int event_delay_hard_ms{5'000};
    bool require_complete_timeframe_bar{true};
};

struct MarketDataRecordingConfig {
    bool enabled{false};
    std::string output_dir{"runtime/market_data"};
    std::string run_id;
    bool flush_each_write{false};
    bool partition_by_product{false};
    bool write_global_copy{false};
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
