#pragma once

#include <string>
#include <vector>

#include "quant_hft/core/ctp_config.h"
#include "quant_hft/services/market_state_detector.h"

namespace quant_hft {

struct CtpFileConfig {
    CtpRuntimeConfig runtime;
    int query_rate_limit_qps{10};
    int account_query_interval_ms{2000};
    int position_query_interval_ms{2000};
    int instrument_query_interval_ms{30000};
    std::vector<std::string> instruments;
    std::vector<std::string> strategy_ids;
    std::string run_type{"live"};
    std::string strategy_factory{"demo"};
    std::string strategy_composite_config;
    int strategy_queue_capacity{8192};
    bool strategy_state_persist_enabled{false};
    int strategy_state_snapshot_interval_ms{60'000};
    int strategy_state_ttl_seconds{86'400};
    std::string strategy_state_key_prefix{"strategy_state"};
    int strategy_metrics_emit_interval_ms{1'000};
    std::string account_id;
    ExecutionConfig execution;
    RiskConfig risk;
    MarketStateDetectorConfig market_state_detector;
};

class CtpConfigLoader {
   public:
    static bool LoadFromYaml(const std::string& path, CtpFileConfig* config, std::string* error);
};

}  // namespace quant_hft
