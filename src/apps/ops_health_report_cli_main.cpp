#include <fstream>
#include <iostream>
#include <optional>
#include <string>

#include "quant_hft/apps/cli_support.h"
#include "quant_hft/apps/ops_report_support.h"

namespace {

std::optional<double> ParseOptionalDoubleArg(const quant_hft::apps::ArgMap& args,
                                             const std::string& key, std::string* error) {
    const std::string raw = quant_hft::apps::GetArg(args, key);
    if (raw.empty()) {
        return std::nullopt;
    }
    double value = 0.0;
    if (!quant_hft::apps::ops_detail::ParseDoubleText(raw, &value)) {
        if (error != nullptr) {
            *error = "invalid numeric value for --" + key + ": " + raw;
        }
        return std::nullopt;
    }
    return value;
}

bool ParseDoubleArg(const quant_hft::apps::ArgMap& args, const std::string& key, double fallback,
                    double* out, std::string* error) {
    if (out == nullptr) {
        return false;
    }
    const std::string raw = quant_hft::apps::GetArg(args, key);
    if (raw.empty()) {
        *out = fallback;
        return true;
    }
    double value = 0.0;
    if (!quant_hft::apps::ops_detail::ParseDoubleText(raw, &value)) {
        if (error != nullptr) {
            *error = "invalid numeric value for --" + key + ": " + raw;
        }
        return false;
    }
    *out = value;
    return true;
}

bool ParseBoolArg(const quant_hft::apps::ArgMap& args, const std::string& key, bool fallback,
                  bool* out, std::string* error) {
    if (out == nullptr) {
        return false;
    }
    const std::string raw = quant_hft::apps::GetArg(args, key);
    if (raw.empty()) {
        *out = fallback;
        return true;
    }
    bool value = false;
    if (!quant_hft::apps::ops_detail::ParseBoolText(raw, &value)) {
        if (error != nullptr) {
            *error = "invalid boolean value for --" + key + ": " + raw;
        }
        return false;
    }
    *out = value;
    return true;
}

}  // namespace

int main(int argc, char** argv) {
    using namespace quant_hft::apps;
    const ArgMap args = ParseArgs(argc, argv);

    std::string error;
    double strategy_engine_target_ms = 1500.0;
    if (!ParseDoubleArg(args, "strategy-engine-target-ms", 1500.0, &strategy_engine_target_ms,
                        &error)) {
        std::cerr << "ops_health_report_cli: " << error << '\n';
        return 1;
    }

    bool core_process_alive = true;
    if (!ParseBoolArg(args, "core-process-alive", true, &core_process_alive, &error)) {
        std::cerr << "ops_health_report_cli: " << error << '\n';
        return 1;
    }

    const std::optional<double> strategy_engine_latency_ms =
        ParseOptionalDoubleArg(args, "strategy-engine-latency-ms", &error);
    if (!error.empty()) {
        std::cerr << "ops_health_report_cli: " << error << '\n';
        return 1;
    }

    OpsHealthBuildOptions options;
    options.strategy_engine_latency_ms = strategy_engine_latency_ms;
    options.strategy_engine_target_ms = strategy_engine_target_ms;
    options.strategy_engine_chain_status = GetArg(args, "strategy-engine-chain-status", "unknown");
    options.core_process_alive = core_process_alive;
    options.redis_health = GetArg(args, "storage-redis-health", "unknown");
    options.timescale_health = GetArg(args, "storage-timescale-health", "unknown");
    options.postgres_health = GetArg(args, "storage-postgres-health", "");
    options.scope = GetArg(args, "scope", "core_engine + strategy_engine + storage");
    options.environment = GetArg(args, "environment", "unknown");
    options.service = GetArg(args, "service", "core_engine");

    const std::string operator_name = GetArg(args, "operator");
    const std::string host = GetArg(args, "host");
    const std::string build = GetArg(args, "build");
    const std::string config_profile = GetArg(args, "config-profile");
    const std::string network_interface = GetArg(args, "interface");

    if (!operator_name.empty()) {
        options.metadata["operator"] = operator_name;
    }
    if (!host.empty()) {
        options.metadata["host"] = host;
    }
    if (!build.empty()) {
        options.metadata["build"] = build;
    }
    if (!config_profile.empty()) {
        options.metadata["config_profile"] = config_profile;
    }
    if (!network_interface.empty()) {
        options.metadata["interface"] = network_interface;
    }

    const OpsHealthReport report = BuildOpsHealthReport(options);
    const std::string json_payload = OpsHealthReportToJson(report);
    const std::string markdown_payload = RenderOpsHealthMarkdown(report);

    const std::string json_output = GetArg(args, "output_json", "ops_health_report.json");
    const std::string markdown_output = GetArg(args, "output_md", "ops_health_report.md");

    if (!WriteTextFile(json_output, json_payload, &error) ||
        !WriteTextFile(markdown_output, markdown_payload, &error)) {
        std::cerr << "ops_health_report_cli: " << error << '\n';
        return 1;
    }

    std::cout << markdown_payload;
    return 0;
}
