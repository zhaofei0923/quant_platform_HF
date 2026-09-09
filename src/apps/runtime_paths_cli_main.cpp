#include "quant_hft/core/host_adapters/host_clock.h"
#include "quant_hft/core/host_adapters/filesystem_configuration_reader.h"
#include <iostream>
#include <string>

#include "quant_hft/core/ctp_config_loader.h"
#include "quant_hft/runtime/runtime_paths.h"

int main(int argc, char** argv) {
    quant_hft::BindOnlineHostClocks();
    quant_hft::BindFilesystemConfigurationReader();
    if (argc != 3 || std::string(argv[1]) != "--config") {
        std::cerr << "usage: runtime_paths_cli --config PATH\n";
        return 2;
    }
    quant_hft::CtpFileConfig config;
    quant_hft::RuntimePaths paths;
    std::string error;
    if (!quant_hft::CtpConfigLoader::LoadFromYaml(argv[2], &config, &error) ||
        !quant_hft::ResolveRuntimePaths(config, quant_hft::RuntimePathOptionsFromEnvironment(),
                                        &paths, &error)) {
        std::cerr << "runtime_paths_cli: " << error << '\n';
        return 2;
    }
    // This is a fixed key/value protocol, not shell code. Never emit credentials/config dumps.
    for (const auto& item :
         {std::pair<const char*, std::string>{"recovery_root", paths.recovery_root},
          {"wal_file", paths.wal_file},
          {"market_data_dir", paths.market_data_dir},
          {"readiness_file", paths.readiness_file},
          {"run_root", paths.run_root},
          {"report_root", paths.report_root},
          {"export_root", paths.export_root},
          {"reconcile_root", paths.reconcile_root}}) {
        if (item.second.find_first_of("\r\n") != std::string::npos) {
            std::cerr << "runtime_paths_cli: path contains a line break\n";
            return 2;
        }
        std::cout << item.first << '=' << item.second << '\n';
    }
    return 0;
}
