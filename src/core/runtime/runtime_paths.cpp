#include "quant_hft/runtime/runtime_paths.h"

#include <cstdlib>
#include <filesystem>

#include "quant_hft/core/ctp_config_loader.h"

namespace quant_hft {
namespace {
std::string Env(const char* name, std::string fallback = {}) {
    const auto* value = std::getenv(name);
    return value && *value ? value : std::move(fallback);
}
std::string Absolute(const std::filesystem::path& path) {
    return std::filesystem::weakly_canonical(std::filesystem::absolute(path)).string();
}
}  // namespace

RuntimePathOptions RuntimePathOptionsFromEnvironment() {
    RuntimePathOptions options;
    const auto root = Env("QUANT_ROOT");
    options.runtime_root =
        Env("QUANT_HFT_RUNTIME_ROOT", root.empty() ? "runtime" : root + "/runtime");
    options.instance = Env("QUANT_HFT_INSTANCE", "default");
    options.wal_file = Env("QUANT_HFT_WAL_FILE");
    options.legacy_simnow_wal_file = Env("SIMNOW_WAL_FILE");
    options.market_data_dir = Env("QUANT_HFT_MARKET_DATA_DIR");
    options.readiness_file = Env("QUANT_HFT_READINESS_FILE");
    options.pending_exit_wal = Env("QUANT_HFT_PENDING_EXIT_WAL");
    return options;
}

bool ResolveRuntimePaths(const CtpFileConfig& config, const RuntimePathOptions& options,
                         RuntimePaths* paths, std::string* error) {
    if (!paths) {
        if (error) *error = "runtime paths output is null";
        return false;
    }
    RuntimePaths result;
    result.identity.environment =
        !config.runtime.enable_real_api
            ? "sim"
            : (config.runtime.environment == CtpEnvironment::kProduction ? "prod" : "simnow");
    result.identity.broker_id = config.runtime.broker_id.empty() && !config.runtime.enable_real_api
                                    ? "sim"
                                    : config.runtime.broker_id;
    result.identity.account_id =
        config.account_id.empty() ? config.runtime.investor_id : config.account_id;
    result.identity.instance = options.instance;
    if (!result.identity.Validate(error)) return false;
    try {
        const auto suffix = std::filesystem::path(result.identity.environment) /
                            result.identity.broker_id / result.identity.account_id /
                            result.identity.instance;
        const auto root = std::filesystem::path(Absolute(options.runtime_root));
        result.recovery_root = Absolute(root / suffix);
        const auto path = [&](const char* name) {
            return Absolute(std::filesystem::path(result.recovery_root) / name);
        };
        if (!ResolveWalOverride(result.identity.environment, options.wal_file,
                                options.legacy_simnow_wal_file, path("wal/events.wal"),
                                &result.wal_file, error))
            return false;
        result.wal_file = Absolute(result.wal_file);
        result.market_data_dir =
            !options.market_data_dir.empty()
                ? Absolute(options.market_data_dir)
                : (config.market_data_recording.output_dir == "runtime/market_data"
                       ? path("market")
                       : Absolute(config.market_data_recording.output_dir));
        result.state_dir = config.strategy_state_file_dir == "runtime/trading/state"
                               ? path("state")
                               : Absolute(config.strategy_state_file_dir);
        result.flow_dir = config.runtime.flow_path.empty() ||
                                  config.runtime.flow_path == "./ctp_flow" ||
                                  config.runtime.flow_path == "ctp_flow"
                              ? path("flow")
                              : Absolute(config.runtime.flow_path);
        result.readiness_file = options.readiness_file.empty() ? path("monitor/readiness.json")
                                                               : Absolute(options.readiness_file);
        result.pending_exit_wal = options.pending_exit_wal.empty()
                                      ? path("pending_exit/pending_exit_v2.jsonl")
                                      : Absolute(options.pending_exit_wal);
        // Run artifacts must not populate a new recovery directory before identity binding.
        result.run_root = Absolute(root / "runs" / suffix);
        result.report_root = Absolute(root / "reports" / suffix);
        result.export_root = Absolute(root / "exports" / suffix);
        result.reconcile_root = Absolute(root / "reconcile" / suffix);
        *paths = std::move(result);
        return true;
    } catch (const std::filesystem::filesystem_error& ex) {
        if (error) *error = ex.what();
        return false;
    }
}
}  // namespace quant_hft
