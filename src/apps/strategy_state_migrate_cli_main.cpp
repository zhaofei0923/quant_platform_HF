#include <iostream>
#include <map>
#include <set>
#include <stdexcept>

#include "quant_hft/config/strategy_state_migration.h"
#include "quant_hft/core/host_adapters/filesystem_configuration_reader.h"
#include "quant_hft/core/host_adapters/host_clock.h"

namespace {
void Usage() {
    std::cerr << "strategy_state_migrate_cli --deployment FILE --account-ref ID --instance-id ID "
                 "--legacy-instance-id SAME_ID --legacy-state-dir DIR --legacy-main FILE "
                 "--parameter-migration-report FILE --output-dir NEW_DIR "
                 "[--key-prefix strategy_state]\n";
}
}  // namespace
int main(int argc, char** argv) {
    quant_hft::BindOnlineHostClocks();
    if (argc == 2 && std::string(argv[1]) == "--help") {
        Usage();
        return 0;
    }
    try {
        const std::set<std::string> allowed{"--deployment",
                                            "--account-ref",
                                            "--instance-id",
                                            "--legacy-instance-id",
                                            "--legacy-state-dir",
                                            "--legacy-main",
                                            "--parameter-migration-report",
                                            "--output-dir",
                                            "--key-prefix"};
        std::map<std::string, std::string> args;
        for (int i = 1; i < argc; i += 2) {
            if (i + 1 >= argc || !allowed.count(argv[i]) ||
                !args.emplace(argv[i], argv[i + 1]).second)
                throw std::runtime_error("missing, duplicate or unknown CLI option");
        }
        const auto required = [&](const char* key) {
            if (!args.count(key) || args.at(key).empty())
                throw std::runtime_error(std::string("required option: ") + key);
            return args.at(key);
        };
        quant_hft::BindFilesystemConfigurationReader();
        quant_hft::DeploymentConfig deployment;
        std::string error;
        if (!quant_hft::LoadDeploymentConfig(required("--deployment"), &deployment, &error) ||
            !quant_hft::VerifyDeploymentPackage(deployment, &error))
            throw std::runtime_error(error);
        quant_hft::StrategyStateMigrationOptions options;
        options.account_ref = required("--account-ref");
        options.instance_id = required("--instance-id");
        options.legacy_instance_id = required("--legacy-instance-id");
        options.legacy_state_directory = required("--legacy-state-dir");
        options.legacy_main_config = required("--legacy-main");
        options.parameter_migration_report = required("--parameter-migration-report");
        options.output_directory = required("--output-dir");
        if (args.count("--key-prefix")) options.key_prefix = args.at("--key-prefix");
        if (!quant_hft::MigrateStrategyState(deployment, options, &error))
            throw std::runtime_error(error);
        std::cout << "state migration written to " << options.output_directory
                  << "; original state and trading facts unchanged; no account activated\n";
        return 0;
    } catch (const std::exception& ex) {
        std::cerr << ex.what() << '\n';
        Usage();
        return 1;
    }
}
