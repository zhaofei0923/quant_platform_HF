#pragma once

#include <string>

#include "quant_hft/rolling/rolling_runner.h"

namespace quant_hft::rolling {

bool WriteRollingReportJson(const RollingReport& report,
                            const std::string& output_path,
                            std::string* error);

bool WriteRollingReportMarkdown(const RollingReport& report,
                                const std::string& output_path,
                                std::string* error);

}  // namespace quant_hft::rolling

