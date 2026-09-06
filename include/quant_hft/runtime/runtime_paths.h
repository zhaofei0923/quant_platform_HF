#pragma once

#include <string>

#include "quant_hft/runtime/runtime_identity.h"

namespace quant_hft {

struct CtpFileConfig;

struct RuntimePathOptions {
    std::string runtime_root{"runtime"};
    std::string instance{"default"};
    std::string wal_file;
    std::string legacy_simnow_wal_file;
    std::string market_data_dir;
    std::string readiness_file;
    std::string pending_exit_wal;
};

struct RuntimePaths {
    RuntimeIdentity identity;
    std::string recovery_root;
    std::string wal_file;
    std::string market_data_dir;
    std::string state_dir;
    std::string flow_dir;
    std::string readiness_file;
    std::string pending_exit_wal;
    std::string run_root;
    std::string report_root;
    std::string export_root;
    std::string reconcile_root;
};

RuntimePathOptions RuntimePathOptionsFromEnvironment();
// Pure path resolution: no directory creation, identity binding, or broker connection.
bool ResolveRuntimePaths(const CtpFileConfig& config, const RuntimePathOptions& options,
                         RuntimePaths* paths, std::string* error);

}  // namespace quant_hft
