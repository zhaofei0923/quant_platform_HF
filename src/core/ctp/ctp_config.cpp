#include "quant_hft/core/ctp_config.h"

#include <array>

namespace quant_hft {
namespace {

struct ParsedTcpFront {
    std::string host;
    int port{0};
};

bool ParseTcpFront(const std::string& front, ParsedTcpFront* parsed) {
    if (parsed == nullptr) {
        return false;
    }
    constexpr const char* kPrefix = "tcp://";
    if (front.rfind(kPrefix, 0) != 0) {
        return false;
    }
    const auto payload = front.substr(6);
    const auto pos = payload.rfind(':');
    if (pos == std::string::npos) {
        return false;
    }
    const auto host = payload.substr(0, pos);
    const auto port_text = payload.substr(pos + 1);
    if (host.empty() || port_text.empty()) {
        return false;
    }
    int port = 0;
    try {
        port = std::stoi(port_text);
    } catch (...) {
        return false;
    }
    if (port <= 0 || port > 65535) {
        return false;
    }
    parsed->host = host;
    parsed->port = port;
    return true;
}

std::string MakeTcpFront(const std::string& host, int port) {
    return "tcp://" + host + ":" + std::to_string(port);
}

bool IsSimNowTradingHoursFrontPair(const std::string& md_front, const std::string& td_front) {
    ParsedTcpFront parsed_md;
    ParsedTcpFront parsed_td;
    if (!ParseTcpFront(md_front, &parsed_md) || !ParseTcpFront(td_front, &parsed_td)) {
        return false;
    }
    if (parsed_md.host != parsed_td.host) {
        return false;
    }
    // Official SimNow trading-hours fronts (look-through front_se) are hosted on 182.254.243.31.
    // These fronts use the monitoring center production secret key and require production-mode API
    // (CTP v6.7.11+ supports selecting key-mode via CreateFtdc*Api(..., blsProductionMode)).
    if (parsed_md.host != "182.254.243.31") {
        return false;
    }

    struct Pair {
        int td_port;
        int md_port;
    };
    constexpr std::array<Pair, 3> kPairs{{
        {30001, 30011},
        {30002, 30012},
        {30003, 30013},
    }};
    for (const auto& pair : kPairs) {
        if (parsed_td.port == pair.td_port && parsed_md.port == pair.md_port) {
            return true;
        }
    }
    return false;
}

}  // namespace

std::vector<CtpFrontPair> BuildCtpFrontCandidates(const std::string& md_front,
                                                  const std::string& td_front) {
    std::vector<CtpFrontPair> candidates;
    candidates.push_back(CtpFrontPair{md_front, td_front});

    ParsedTcpFront parsed_md;
    ParsedTcpFront parsed_td;
    if (!ParseTcpFront(md_front, &parsed_md) || !ParseTcpFront(td_front, &parsed_td)) {
        return candidates;
    }
    if (parsed_md.host != parsed_td.host) {
        return candidates;
    }

    struct SimNowTradingGroup {
        int group_id;
        int td_port;
        int md_port;
    };
    constexpr std::array<SimNowTradingGroup, 3> kGroups{{
        {1, 30001, 30011},
        {2, 30002, 30012},
        {3, 30003, 30013},
    }};

    int active_group = 0;
    for (const auto& group : kGroups) {
        if (parsed_td.port == group.td_port && parsed_md.port == group.md_port) {
            active_group = group.group_id;
            break;
        }
    }
    if (active_group == 0) {
        return candidates;
    }

    for (const auto& group : kGroups) {
        if (group.group_id == active_group) {
            continue;
        }
        candidates.push_back(CtpFrontPair{
            MakeTcpFront(parsed_md.host, group.md_port),
            MakeTcpFront(parsed_td.host, group.td_port),
        });
    }
    return candidates;
}

bool CtpConfigValidator::Validate(const CtpRuntimeConfig& config, std::string* error) {
    if (config.md_front.empty() || config.td_front.empty()) {
        if (error != nullptr) {
            *error = "md_front/td_front must be configured";
        }
        return false;
    }
    if (config.broker_id.empty() || config.user_id.empty() || config.password.empty()) {
        if (error != nullptr) {
            *error = "broker_id/user_id/password must be configured";
        }
        return false;
    }
    if (config.investor_id.empty()) {
        if (error != nullptr) {
            *error = "investor_id must be configured";
        }
        return false;
    }
    if (config.environment == CtpEnvironment::kSimNow) {
        const bool trading_hours_front = IsSimNowTradingHoursFrontPair(config.md_front, config.td_front);
        if (trading_hours_front && !config.is_production_mode) {
            if (error != nullptr) {
                *error =
                    "SimNow trading-hours fronts require is_production_mode=true (CTP v6.7.11 production secret key)";
            }
            return false;
        }
        if (!trading_hours_front && config.is_production_mode) {
            if (error != nullptr) {
                *error =
                    "SimNow requires is_production_mode=false unless using trading-hours fronts "
                    "(182.254.243.31:30001/30011, 30002/30012, 30003/30013)";
            }
            return false;
        }
    }
    if (config.environment == CtpEnvironment::kProduction && !config.is_production_mode) {
        if (error != nullptr) {
            *error = "Production requires is_production_mode=true explicitly";
        }
        return false;
    }
    if (config.is_production_mode && !config.enable_terminal_auth) {
        if (error != nullptr) {
            *error = "Production requires enable_terminal_auth=true";
        }
        return false;
    }
    if (config.is_production_mode && (config.app_id.empty() || config.auth_code.empty())) {
        if (error != nullptr) {
            *error = "ReqAuthenticate requires non-empty app_id/auth_code in production";
        }
        return false;
    }
    if (config.connect_timeout_ms <= 0) {
        if (error != nullptr) {
            *error = "connect_timeout_ms must be > 0";
        }
        return false;
    }
    if (config.reconnect_max_attempts <= 0) {
        if (error != nullptr) {
            *error = "reconnect_max_attempts must be > 0";
        }
        return false;
    }
    if (config.reconnect_initial_backoff_ms <= 0 || config.reconnect_max_backoff_ms <= 0 ||
        config.reconnect_initial_backoff_ms > config.reconnect_max_backoff_ms) {
        if (error != nullptr) {
            *error =
                "reconnect backoff must be > 0 and reconnect_initial_backoff_ms <= reconnect_max_backoff_ms";
        }
        return false;
    }
    if (config.recovery_quiet_period_ms < 0) {
        if (error != nullptr) {
            *error = "recovery_quiet_period_ms must be >= 0";
        }
        return false;
    }
    if (config.order_insert_rate_per_sec <= 0 || config.order_cancel_rate_per_sec <= 0 ||
        config.query_rate_per_sec <= 0 || config.settlement_query_rate_per_sec <= 0) {
        if (error != nullptr) {
            *error = "order/query rate limit must be > 0";
        }
        return false;
    }
    if (config.order_bucket_capacity <= 0 || config.cancel_bucket_capacity <= 0 ||
        config.query_bucket_capacity <= 0 || config.settlement_query_bucket_capacity <= 0) {
        if (error != nullptr) {
            *error = "bucket capacities must be > 0";
        }
        return false;
    }
    if (config.settlement_retry_max <= 0 || config.settlement_retry_backoff_initial_ms <= 0 ||
        config.settlement_retry_backoff_max_ms < config.settlement_retry_backoff_initial_ms ||
        config.settlement_running_stale_timeout_ms <= 0) {
        if (error != nullptr) {
            *error = "settlement retry/backoff/stale timeout configuration is invalid";
        }
        return false;
    }
    if (config.cancel_retry_max <= 0 || config.cancel_retry_base_ms <= 0 ||
        config.cancel_retry_max_delay_ms < config.cancel_retry_base_ms ||
        config.cancel_wait_ack_timeout_ms <= 0) {
        if (error != nullptr) {
            *error = "cancel retry configuration is invalid";
        }
        return false;
    }
    if (config.breaker_failure_threshold <= 0 || config.breaker_timeout_ms <= 0 ||
        config.breaker_half_open_timeout_ms <= 0) {
        if (error != nullptr) {
            *error = "breaker thresholds/timeouts must be > 0";
        }
        return false;
    }
    if (!config.breaker_strategy_enabled && !config.breaker_account_enabled &&
        !config.breaker_system_enabled) {
        if (error != nullptr) {
            *error = "at least one breaker scope must be enabled";
        }
        return false;
    }
    if (config.kafka_topic_ticks.empty()) {
        if (error != nullptr) {
            *error = "kafka_topic_ticks must not be empty";
        }
        return false;
    }
    if (config.audit_hot_days <= 0 || config.audit_cold_days <= 0 ||
        config.audit_cold_days < config.audit_hot_days) {
        if (error != nullptr) {
            *error = "audit retention days must be > 0 and cold >= hot";
        }
        return false;
    }
    return true;
}

}  // namespace quant_hft
