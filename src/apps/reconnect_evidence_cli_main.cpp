#include <iostream>
#include <string>

#include "quant_hft/apps/cli_support.h"
#include "quant_hft/apps/ops_report_support.h"

namespace {

std::string BuildReconnectMarkdown(const std::string& chain_status, const std::string& chain_source,
                                   std::int64_t state_key_count, std::int64_t intent_count,
                                   std::int64_t order_key_count) {
    std::ostringstream oss;
    oss << "# Reconnect Evidence\n\n";
    oss << "- status: ok\n";
    oss << "- strategy_engine_chain_status: " << chain_status << "\n";
    oss << "- strategy_engine_chain_source: " << chain_source << "\n";
    oss << "- strategy_engine_state_key_count: " << state_key_count << "\n";
    oss << "- strategy_engine_intent_count: " << intent_count << "\n";
    oss << "- strategy_engine_order_key_count: " << order_key_count << "\n";
    return oss.str();
}

bool ParseInt64Arg(const quant_hft::apps::ArgMap& args, const std::string& key,
                   std::int64_t fallback, std::int64_t* out, std::string* error) {
    if (out == nullptr) {
        return false;
    }
    const std::string raw = quant_hft::apps::GetArg(args, key);
    if (raw.empty()) {
        *out = fallback;
        return true;
    }
    std::int64_t value = 0;
    if (!quant_hft::apps::ops_detail::ParseInt64Text(raw, &value)) {
        if (error != nullptr) {
            *error = "invalid integer value for --" + key + ": " + raw;
        }
        return false;
    }
    *out = value;
    return true;
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

std::string ResolveArgAlias(const quant_hft::apps::ArgMap& args, const std::string& primary,
                            const std::string& secondary, const std::string& fallback) {
    const std::string first = quant_hft::apps::GetArg(args, primary);
    if (!first.empty()) {
        return first;
    }
    const std::string second = quant_hft::apps::GetArg(args, secondary);
    if (!second.empty()) {
        return second;
    }
    return fallback;
}

}  // namespace

int main(int argc, char** argv) {
    using namespace quant_hft::apps;
    const ArgMap args = ParseArgs(argc, argv);

    std::string error;

    double strategy_engine_target_ms = 1500.0;
    if (!ParseDoubleArg(args, "strategy-engine-target-ms", 1500.0, &strategy_engine_target_ms,
                        &error)) {
        std::cerr << "reconnect_evidence_cli: " << error << '\n';
        return 1;
    }
    if (GetArg(args, "strategy-engine-target-ms").empty()) {
        const std::string bridge_target = GetArg(args, "strategy-bridge-target-ms");
        if (!bridge_target.empty() &&
            !ops_detail::ParseDoubleText(bridge_target, &strategy_engine_target_ms)) {
            std::cerr
                << "reconnect_evidence_cli: invalid numeric value for --strategy-bridge-target-ms: "
                << bridge_target << '\n';
            return 1;
        }
    }

    const std::string chain_status = ResolveArgAlias(args, "strategy-engine-chain-status",
                                                     "strategy-bridge-chain-status", "complete");
    const std::string chain_source = ResolveArgAlias(args, "strategy-engine-chain-source",
                                                     "strategy-bridge-chain-source", "in_process");

    std::int64_t state_key_count = 2;
    std::int64_t intent_count = 1;
    std::int64_t order_key_count = 1;
    if (!ParseInt64Arg(args, "strategy-engine-state-key-count", 2, &state_key_count, &error) ||
        !ParseInt64Arg(args, "strategy-engine-intent-count", 1, &intent_count, &error) ||
        !ParseInt64Arg(args, "strategy-engine-order-key-count", 1, &order_key_count, &error)) {
        std::cerr << "reconnect_evidence_cli: " << error << '\n';
        return 1;
    }

    if (GetArg(args, "strategy-engine-state-key-count").empty()) {
        ParseInt64Arg(args, "strategy-bridge-state-key-count", state_key_count, &state_key_count,
                      &error);
    }
    if (GetArg(args, "strategy-engine-intent-count").empty()) {
        ParseInt64Arg(args, "strategy-bridge-intent-count", intent_count, &intent_count, &error);
    }
    if (GetArg(args, "strategy-engine-order-key-count").empty()) {
        ParseInt64Arg(args, "strategy-bridge-order-key-count", order_key_count, &order_key_count,
                      &error);
    }
    if (!error.empty()) {
        std::cerr << "reconnect_evidence_cli: " << error << '\n';
        return 1;
    }

    OpsHealthBuildOptions options;
    options.strategy_engine_latency_ms = 0.0;
    options.strategy_engine_target_ms = strategy_engine_target_ms;
    options.strategy_engine_chain_status = chain_status;
    options.redis_health =
        ResolveArgAlias(args, "storage-redis-health", "storage-redis-health", "unknown");
    options.timescale_health =
        ResolveArgAlias(args, "storage-timescale-health", "storage-timescale-health", "unknown");
    options.metadata["strategy_engine_chain_source"] = chain_source;
    options.metadata["strategy_engine_state_key_count"] = std::to_string(state_key_count);
    options.metadata["strategy_engine_intent_count"] = std::to_string(intent_count);
    options.metadata["strategy_engine_order_key_count"] = std::to_string(order_key_count);

    const std::string operator_name = GetArg(args, "operator");
    const std::string host = GetArg(args, "host");
    const std::string build = GetArg(args, "build");
    const std::string config_profile = GetArg(args, "config-profile");
    const std::string interface_name = GetArg(args, "interface");
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
    if (!interface_name.empty()) {
        options.metadata["interface"] = interface_name;
    }

    const OpsHealthReport health_report = BuildOpsHealthReport(options);
    const OpsAlertReport alert_report = EvaluateOpsAlertPolicy(health_report);

    const std::string health_json_file =
        GetArg(args, "health_json_file", "docs/results/ops_health_report.json");
    const std::string health_markdown_file =
        GetArg(args, "health_markdown_file", "docs/results/ops_health_report.md");
    const std::string alert_json_file =
        GetArg(args, "alert_json_file", "docs/results/ops_alert_report.json");
    const std::string alert_markdown_file =
        GetArg(args, "alert_markdown_file", "docs/results/ops_alert_report.md");

    if (!WriteTextFile(health_json_file, OpsHealthReportToJson(health_report), &error) ||
        !WriteTextFile(health_markdown_file, RenderOpsHealthMarkdown(health_report), &error) ||
        !WriteTextFile(alert_json_file, OpsAlertReportToJson(alert_report), &error) ||
        !WriteTextFile(alert_markdown_file, RenderOpsAlertMarkdown(alert_report), &error)) {
        std::cerr << "reconnect_evidence_cli: " << error << '\n';
        return 1;
    }

    const std::string report_file = GetArg(args, "report_file");
    if (!report_file.empty()) {
        if (!WriteTextFile(report_file,
                           BuildReconnectMarkdown(chain_status, chain_source, state_key_count,
                                                  intent_count, order_key_count),
                           &error)) {
            std::cerr << "reconnect_evidence_cli: " << error << '\n';
            return 1;
        }
    }

    std::cout << "--strategy-engine-chain-status " << chain_status << '\n';
    std::cout << "--strategy-engine-chain-source " << chain_source << '\n';
    std::cout << "--strategy-engine-state-key-count " << state_key_count << '\n';
    std::cout << "--strategy-engine-intent-count " << intent_count << '\n';
    std::cout << "--strategy-engine-order-key-count " << order_key_count << '\n';
    return 0;
}
