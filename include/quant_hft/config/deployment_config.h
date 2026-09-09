#pragma once

#include <map>
#include <string>
#include <unordered_map>
#include <vector>

#include "quant_hft/config/execution_profile.h"
#include "quant_hft/strategy/parameter_set.h"

namespace quant_hft {

struct DeploymentAccount {
    std::string account_ref;
    std::string environment;
    std::string broker_id;
    std::string account_id;
    std::string connection_config;
    std::string credential_ref;
    std::string runtime_root;
    std::string active_host;
    StrategyExecutionProfile risk;
};

struct ResolvedStrategyInstance {
    std::string instance_id;
    std::string account_ref;
    std::string strategy_release;
    std::string parameter_set;
    std::string parameter_hash;
    std::string product_id;
    // account_equity is allowed only for the sole instance of a physical account.
    std::string capital_mode{"fixed"};
    double initial_capital{0.0};
    std::string state_namespace;
    StrategyExecutionProfile risk;
    CompositeStrategyDefinition composite;
};

struct DeploymentConfig {
    std::string source_path;
    std::string package_version;
    std::string package_hash;
    std::string effective_hash;
    std::string resolved_json;
    std::map<std::string, DeploymentAccount> accounts;
    std::vector<ResolvedStrategyInstance> instances;
};

bool LoadDeploymentConfig(const std::string& path, DeploymentConfig* out, std::string* error);
std::string ConfigContentSha256(const std::string& content);
// Hashes materialized parameters, independent of YAML formatting and file newlines.
std::string ConfigParameterSha256(const ParameterSet& parameters);
// Entry points must bind a resolved deployment to the exact statically linked package.
bool VerifyDeploymentPackage(const DeploymentConfig& deployment, std::string* error);
// Only fixed budgets become independent strategy capital books in the online host.
std::unordered_map<std::string, double> FixedStrategyCapitalAllocations(
    const DeploymentConfig& deployment, const std::string& account_ref);
// Materializes the selected legacy mode once. It never changes source inputs.
bool MigrateLegacyParameterSet(const std::string& path, const std::string& mode,
                               const std::string& parameter_id, const std::string& release,
                               const std::string& schema_path, const std::string& output_directory,
                               std::string* error);

}  // namespace quant_hft
