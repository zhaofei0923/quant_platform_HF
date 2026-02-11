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
