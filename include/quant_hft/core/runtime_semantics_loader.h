#pragma once

#include <string>

#include "quant_hft/contracts/runtime_semantics.h"

namespace quant_hft {
struct CtpFileConfig;
// Reads only an allowlist of pure YAML decisions. Never expands environment variables.
bool LoadRuntimeSemanticsConfig(const std::string& path, RuntimeSemanticsConfig* out,
                                std::string* error);
// Validates every shared decision against the live loader without reading runtime secrets.
bool ValidateRuntimeSemanticsAgainstCtpConfig(const RuntimeSemanticsConfig& shared,
                                              const CtpFileConfig& live, std::string* error);
std::string RenderRuntimeSemanticsJson(const RuntimeSemanticsConfig& config);
}  // namespace quant_hft
