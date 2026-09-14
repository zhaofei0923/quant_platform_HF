#pragma once

#include <string>

namespace quant_hft {
struct StrategyReleaseMigrationOptions {
    std::string source_deployment;
    std::string target_deployment;
    std::string source_package_sha256;
    std::string account_ref;
    std::string instance_id;
    std::string source_state_directory;
    std::string output_directory;
    std::string key_prefix{"strategy_state"};
};

// Offline 1.0.0 -> 1.1.0 migration of one formal strategy checkpoint. The original
// directory is never modified. Other instance/pipeline files are not migrated.
bool MigrateStrategyReleaseState(const StrategyReleaseMigrationOptions& options,
                                 std::string* error);
}  // namespace quant_hft
