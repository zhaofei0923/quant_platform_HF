#include <filesystem>
#include <iostream>
#include <string>

#include "quant_hft/apps/cli_support.h"
#include "quant_hft/rolling/rolling_config.h"
#include "quant_hft/rolling/rolling_report_writer.h"
#include "quant_hft/rolling/rolling_runner.h"

namespace {

using quant_hft::apps::ArgMap;
using quant_hft::apps::GetArg;
using quant_hft::apps::HasArg;
using quant_hft::apps::ParseArgs;
using quant_hft::rolling::LoadRollingConfig;
using quant_hft::rolling::RollingConfig;
using quant_hft::rolling::RollingReport;
using quant_hft::rolling::RunRollingBacktest;
using quant_hft::rolling::WriteRollingReportJson;
using quant_hft::rolling::WriteRollingReportMarkdown;

void PrintUsage(const char* argv0) {
    std::cout << "Usage: " << argv0 << " --config <rolling_config.yaml>\n";
}

bool EnsureParentDir(const std::string& output_path, std::string* error) {
    const std::filesystem::path path(output_path);
    const std::filesystem::path parent = path.parent_path();
    if (parent.empty()) {
        return true;
    }
    std::error_code ec;
    std::filesystem::create_directories(parent, ec);
    if (ec) {
        if (error != nullptr) {
            *error = "failed to create output directory: " + parent.string() + ": " + ec.message();
        }
        return false;
    }
    return true;
}

}  // namespace

int main(int argc, char** argv) {
    const ArgMap args = ParseArgs(argc, argv);
    if (HasArg(args, "help") || HasArg(args, "h")) {
        PrintUsage(argv[0]);
        return 0;
    }

    const std::string config_path = GetArg(args, "config", "");
    if (config_path.empty()) {
        PrintUsage(argv[0]);
        return 2;
    }

    RollingConfig config;
    std::string error;
    if (!LoadRollingConfig(config_path, &config, &error)) {
        std::cerr << "rolling_backtest_cli: failed to load config: " << error << '\n';
        return 2;
    }

    RollingReport report;
    if (!RunRollingBacktest(config, &report, &error)) {
        std::cerr << "rolling_backtest_cli: run failed: " << error << '\n';
        return 1;
    }

    if (!EnsureParentDir(config.output.report_json, &error) ||
        !EnsureParentDir(config.output.report_md, &error)) {
        std::cerr << "rolling_backtest_cli: " << error << '\n';
        return 1;
    }

    if (!WriteRollingReportJson(report, config.output.report_json, &error)) {
        std::cerr << "rolling_backtest_cli: failed to write JSON report: " << error << '\n';
        return 1;
    }
    if (!WriteRollingReportMarkdown(report, config.output.report_md, &error)) {
        std::cerr << "rolling_backtest_cli: failed to write Markdown report: " << error << '\n';
        return 1;
    }

    std::cout << "rolling_backtest_cli: mode=" << report.mode << " success=" << report.success_count
              << " failed=" << report.failed_count
              << " interrupted=" << (report.interrupted ? "true" : "false") << '\n';
    std::cout << "rolling_backtest_cli: json=" << config.output.report_json
              << " md=" << config.output.report_md << '\n';

    if (report.interrupted) {
        return 130;
    }
    if (report.success_count == 0) {
        return 1;
    }
    return 0;
}

