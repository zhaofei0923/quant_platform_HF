#pragma once

#include <filesystem>
#include <string>
#include <unordered_map>

#include "quant_hft/optim/optimization_algorithm.h"

namespace quant_hft::optim {

struct TrialConfigRequest {
    std::filesystem::path composite_config_path;
    std::filesystem::path target_sub_config_path;
    std::unordered_map<std::string, ParamValue> param_overrides;
    std::string trial_id;
};

struct TrialConfigArtifacts {
    std::filesystem::path working_dir;
    std::filesystem::path composite_config_path;
    std::filesystem::path sub_config_path;
};

bool GenerateTrialConfig(const TrialConfigRequest& request,
                         TrialConfigArtifacts* out,
                         std::string* error);

}  // namespace quant_hft::optim
