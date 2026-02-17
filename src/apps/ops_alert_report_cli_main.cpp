#include <fstream>
#include <iostream>
#include <sstream>
#include <string>

#include "quant_hft/apps/cli_support.h"
#include "quant_hft/apps/ops_report_support.h"

namespace {

bool ReadFile(const std::string& path, std::string* out, std::string* error) {
    if (out == nullptr) {
        return false;
    }
    std::ifstream input(path);
    if (!input.is_open()) {
        if (error != nullptr) {
            *error = "unable to open input file: " + path;
        }
        return false;
    }
    std::ostringstream buffer;
    buffer << input.rdbuf();
    *out = buffer.str();
    return true;
}

}  // namespace

int main(int argc, char** argv) {
    using namespace quant_hft::apps;
    const ArgMap args = ParseArgs(argc, argv);

    std::string error;
    OpsHealthReport health_report;

    const std::string health_json_file = GetArg(args, "health-json-file");
    if (!health_json_file.empty()) {
        std::string health_json;
        if (!ReadFile(health_json_file, &health_json, &error)) {
            std::cerr << "ops_alert_report_cli: " << error << '\n';
            return 1;
        }
        if (!ParseOpsHealthReportJson(health_json, &health_report)) {
            std::cerr << "ops_alert_report_cli: failed to parse health report json: "
                      << health_json_file << '\n';
            return 1;
        }
    } else {
        OpsHealthBuildOptions default_options;
        default_options.strategy_engine_latency_ms = 0.0;
        default_options.strategy_engine_chain_status = "complete";
        default_options.redis_health = "healthy";
        default_options.timescale_health = "healthy";
        health_report = BuildOpsHealthReport(default_options);
    }

    const OpsAlertReport alert_report = EvaluateOpsAlertPolicy(health_report);
    const std::string json_payload = OpsAlertReportToJson(alert_report);
    const std::string markdown_payload = RenderOpsAlertMarkdown(alert_report);

    if (!WriteTextFile(GetArg(args, "output_json", "ops_alert_report.json"), json_payload,
                       &error) ||
        !WriteTextFile(GetArg(args, "output_md", "ops_alert_report.md"), markdown_payload,
                       &error)) {
        std::cerr << "ops_alert_report_cli: " << error << '\n';
        return 1;
    }

    std::cout << markdown_payload;
    return 0;
}
