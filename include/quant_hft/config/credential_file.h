#pragma once

#include <string>

namespace quant_hft {

// Bind CTP KEY=value assignments from an owner-only file without executing shell syntax.
void LoadCredentialFile(const std::string& path);

}  // namespace quant_hft
