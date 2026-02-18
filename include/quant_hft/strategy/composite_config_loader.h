#pragma once

#include <string>

#include "quant_hft/strategy/composite_strategy.h"

namespace quant_hft {

bool LoadCompositeStrategyDefinition(const std::string& path, CompositeStrategyDefinition* out,
                                     std::string* error);

}  // namespace quant_hft
