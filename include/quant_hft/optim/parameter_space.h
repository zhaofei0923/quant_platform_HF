#pragma once

#include <map>
#include <optional>
#include <string>
#include <vector>

#include "quant_hft/optim/optimization_algorithm.h"

namespace quant_hft::optim {

enum class ParameterType {
    kInt,
    kDouble,
    kString,
    kEnum,
};

struct ParameterDef {
    std::string name;
    ParameterType type{ParameterType::kString};
    std::vector<ParamValue> values;
    std::optional<ParamValue> min;
    std::optional<ParamValue> max;
    std::optional<double> step;
};

struct ParameterSpace {
    std::string backtest_cli_path;
    std::string composite_config_path;
    std::string target_sub_config_path;
    std::map<std::string, std::string> backtest_args;
    std::vector<ParameterDef> parameters;
    OptimizationConfig optimization;
};

bool LoadParameterSpace(const std::string& yaml_path, ParameterSpace* out, std::string* error);

}  // namespace quant_hft::optim
