#pragma once

#include <string>

#include "quant_hft/config/deployment_config.h"

namespace quant_hft {
struct StrategyStateMigrationOptions {
    std::string account_ref;
    std::string instance_id;
    std::string legacy_instance_id;
    std::string legacy_state_directory;
    std::string key_prefix{"strategy_state"};
    std::string parameter_migration_report;
    std::string legacy_main_config;
    std::string output_directory;
};

// Offline only. Validates a frozen legacy snapshot and emits a new directory.
// Never modifies the old snapshot, a WAL, an execution journal or a live account.
bool MigrateStrategyState(const DeploymentConfig& deployment,
                          const StrategyStateMigrationOptions& options, std::string* error);
}  // namespace quant_hft
