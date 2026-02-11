#pragma once

#include <string>

#include "quant_hft/core/ctp_config.h"

namespace quant_hft {

struct CtpFileConfig {
    CtpRuntimeConfig runtime;
    int query_rate_limit_qps{10};
};

class CtpConfigLoader {
public:
    static bool LoadFromYaml(const std::string& path,
                             CtpFileConfig* config,
                             std::string* error);
};

}  // namespace quant_hft
