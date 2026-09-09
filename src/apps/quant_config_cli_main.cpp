#include <sys/stat.h>
#include <unistd.h>

#include <filesystem>
#include <fstream>
#include <iostream>
#include <regex>
#include <set>

#include "quant_hft/config/deployment_config.h"
#include "quant_hft/core/host_adapters/filesystem_configuration_reader.h"
#include "quant_hft/core/host_adapters/host_clock.h"

namespace {
void Usage() {
    std::cerr << "quant_config_cli validate|resolve|list <deployment.yaml> [output.json]\n"
                 "quant_config_cli migrate <legacy-main.yaml> <sim|live|backtest> <parameter-id> "
                 "<algorithm@version> <schema.yaml> <output-dir>\n"
                 "quant_config_cli launch <deployment.yaml> <account-ref>\n";
}

void LoadCredentials(const std::string& path) {
    struct stat info {};
    if (lstat(path.c_str(), &info) != 0 || !S_ISREG(info.st_mode) || (info.st_mode & 0077) != 0 ||
        info.st_uid != geteuid()) {
        throw std::runtime_error("credential_ref must be an owner-only regular file (0600)");
    }
    std::ifstream input(path);
    std::string line;
    std::set<std::string> seen;
    static const std::regex key_pattern("CTP_[A-Z0-9_]+");
    while (std::getline(input, line)) {
        if (!line.empty() && line.back() == '\r') line.pop_back();
        if (line.empty() || line.front() == '#') continue;
        const auto split = line.find('=');
        if (split == std::string::npos)
            throw std::runtime_error("credential file expects KEY=value, no shell syntax");
        const auto key = line.substr(0, split);
        auto value = line.substr(split + 1);
        if (!std::regex_match(key, key_pattern) || !seen.insert(key).second) {
            throw std::runtime_error("credential file contains invalid or duplicate variable");
        }
        if (value.size() >= 2 && (value.front() == '\'' || value.front() == '"') &&
            value.back() == value.front()) {
            value = value.substr(1, value.size() - 2);
        }
        if (setenv(key.c_str(), value.c_str(), 1) != 0)
            throw std::runtime_error("cannot bind credential environment");
    }
}
}  // namespace

int main(int argc, char** argv) {
    quant_hft::BindOnlineHostClocks();
    quant_hft::BindFilesystemConfigurationReader();
    if (argc < 3) {
        Usage();
        return 2;
    }
    const std::string command = argv[1];
    std::string error;
    if (command == "migrate") {
        if (argc != 8) {
            Usage();
            return 2;
        }
        if (!quant_hft::MigrateLegacyParameterSet(argv[2], argv[3], argv[4], argv[5], argv[6],
                                                  argv[7], &error)) {
            std::cerr << error << '\n';
            return 1;
        }
        std::cout << "migration written; original files and trading facts unchanged\n";
        return 0;
    }
    if (command != "validate" && command != "resolve" && command != "list" && command != "launch") {
        Usage();
        return 2;
    }
    quant_hft::DeploymentConfig deployment;
    if (!quant_hft::LoadDeploymentConfig(argv[2], &deployment, &error) ||
        !quant_hft::VerifyDeploymentPackage(deployment, &error)) {
        std::cerr << error << '\n';
        return 1;
    }
    if (command == "resolve") {
        if (argc == 4) {
            if (std::filesystem::exists(argv[3])) {
                std::cerr << "output already exists\n";
                return 1;
            }
            std::ofstream output(argv[3]);
            output << deployment.resolved_json;
            if (!output) {
                std::cerr << "cannot write output\n";
                return 1;
            }
        } else
            std::cout << deployment.resolved_json;
    } else if (command == "list") {
        std::cout << "account\tinstance\tstrategy_release\tparameter_set\tproduct/"
                     "timeframe\tcapital\tstate_namespace\n";
        for (const auto& instance : deployment.instances) {
            std::cout << instance.account_ref << '\t' << instance.instance_id << '\t'
                      << instance.strategy_release << '\t' << instance.parameter_set << '\t'
                      << instance.product_id << '/';
            for (const auto& sub : instance.composite.sub_strategies)
                std::cout << sub.timeframe_minutes << "m,";
            std::cout << '\t' << instance.initial_capital << '\t' << instance.state_namespace
                      << '\n';
        }
    } else if (command == "launch") {
        if (argc != 4) {
            Usage();
            return 2;
        }
        try {
            const auto& account = deployment.accounts.at(argv[3]);
            char host[256]{};
            if (gethostname(host, sizeof(host)) != 0 || account.active_host != host) {
                throw std::runtime_error("account active_host differs from this host");
            }
            LoadCredentials(account.credential_ref);
            setenv("QUANT_HFT_DEPLOYMENT_FILE", deployment.source_path.c_str(), 1);
            setenv("QUANT_HFT_DEPLOYMENT_ACCOUNT", account.account_ref.c_str(), 1);
            setenv("QUANT_HFT_RUNTIME_ROOT", account.runtime_root.c_str(), 1);
            const auto binary =
                std::filesystem::canonical("/proc/self/exe").parent_path() / "core_engine";
            std::cout << "launching account=" << account.account_ref
                      << " config=" << deployment.effective_hash << std::endl;
            execl(binary.c_str(), binary.c_str(), "--config", account.connection_config.c_str(),
                  nullptr);
            throw std::runtime_error("cannot exec sibling core_engine");
        } catch (const std::exception& ex) {
            std::cerr << ex.what() << '\n';
            return 1;
        }
    } else
        std::cout << "valid accounts=" << deployment.accounts.size()
                  << " instances=" << deployment.instances.size()
                  << " sha256=" << deployment.effective_hash << '\n';
    return 0;
}
