#pragma once

#include <string>
#include <vector>

#include "quant_hft/core/ctp_config.h"

namespace quant_hft {

struct CtpFileConfig {
    CtpRuntimeConfig runtime;
    int query_rate_limit_qps{10};
    std::vector<std::string> instruments;
    std::vector<std::string> strategy_ids;
    int strategy_poll_interval_ms{200};
    std::string account_id;
    ExecutionConfig execution;
    RiskConfig risk;
};

class CtpConfigLoader {
public:
    static bool LoadFromYaml(const std::string& path,
                             CtpFileConfig* config,
                             std::string* error);
};

}  // namespace quant_hft
