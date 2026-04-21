#include <iostream>
#include <string>

#include "quant_hft/apps/cli_support.h"
#include "quant_hft/rolling/oos_top10_validation.h"

namespace {

using quant_hft::apps::ArgMap;
using quant_hft::apps::GetArg;
using quant_hft::apps::HasArg;
using quant_hft::apps::ParseArgs;
using quant_hft::rolling::OosTop10ValidationReport;
using quant_hft::rolling::OosTop10ValidationRequest;
using quant_hft::rolling::RunOosTop10Validation;

void PrintUsage(const char* argv0) {
    std::cout << "Usage: " << argv0
              << " --train-report-json <parameter_optim_report.json>"
                 " --oos-start <YYYY-MM-DD|YYYYMMDD> --oos-end <YYYY-MM-DD|YYYYMMDD>"
                 " [--top-n <count>] [--output-dir <dir>] [--overwrite]\n";
}

bool ParsePositiveInt(const std::string& text, int* out) {
    if (out == nullptr || text.empty()) {
        return false;
    }
    try {
        std::size_t parsed = 0;
        const int value = std::stoi(text, &parsed);
        if (parsed != text.size() || value <= 0) {
            return false;
        }
        *out = value;
        return true;
    } catch (...) {
        return false;
    }
}

}  // namespace

int main(int argc, char** argv) {
    const ArgMap args = ParseArgs(argc, argv);
    if (HasArg(args, "help") || HasArg(args, "h")) {
        PrintUsage(argv[0]);
        return 0;
    }

    OosTop10ValidationRequest request;
    request.train_report_json = GetArg(args, "train-report-json", GetArg(args, "train_report_json"));
    request.oos_start_date = GetArg(args, "oos-start", GetArg(args, "start-date"));
    request.oos_end_date = GetArg(args, "oos-end", GetArg(args, "end-date"));
    request.output_dir = GetArg(args, "output-dir", GetArg(args, "output_dir"));
    request.overwrite = HasArg(args, "overwrite");

    const std::string top_n_raw = GetArg(args, "top-n", GetArg(args, "top_n", "10"));
    if (!ParsePositiveInt(top_n_raw, &request.top_n)) {
        std::cerr << "oos_top10_validation_cli: invalid --top-n: " << top_n_raw << '\n';
        PrintUsage(argv[0]);
        return 2;
    }

    if (request.train_report_json.empty() || request.oos_start_date.empty() ||
        request.oos_end_date.empty()) {
        PrintUsage(argv[0]);
        return 2;
    }

    OosTop10ValidationReport report;
    std::string error;
    if (!RunOosTop10Validation(request, &report, &error)) {
        std::cerr << "oos_top10_validation_cli: " << error << '\n';
        return 1;
    }

    std::cout << "oos_top10_validation_cli: selected=" << report.selected_count
              << " success=" << report.success_count << " failed=" << report.failed_count
              << " csv=" << report.output_csv;
    if (!report.final_recommended_params_yaml.empty()) {
        std::cout << " recommended=" << report.final_recommended_params_yaml;
    }
    std::cout << '\n';
    return report.selected_count > 0 ? 0 : 1;
}