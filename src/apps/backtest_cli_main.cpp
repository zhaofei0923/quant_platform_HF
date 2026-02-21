#include <iostream>
#include <string>

#include "quant_hft/apps/backtest_replay_support.h"

int main(int argc, char** argv) {
    using namespace quant_hft::apps;

    const auto args = ParseArgs(argc, argv);
    BacktestCliSpec spec;
    std::string error;
    if (!ParseBacktestCliSpec(args, &spec, &error)) {
        std::cerr << "backtest_cli: " << error << '\n';
        return 2;
    }
    if (!RequireParquetBacktestSpec(spec, &error)) {
        std::cerr << "backtest_cli: " << error << '\n';
        return 2;
    }

    BacktestCliResult result;
    if (!RunBacktestSpec(spec, &result, &error)) {
        std::cerr << "backtest_cli: " << error << '\n';
        return 1;
    }

    const std::string json = RenderBacktestJson(result);
    const std::string markdown = RenderBacktestMarkdown(result);

    const std::string output_json =
        detail::GetArgAny(args, {"output_json", "result_json", "report_json"});
    const std::string output_md = detail::GetArgAny(args, {"output_md", "report_md"});
    const std::string export_csv_dir =
        detail::GetArgAny(args, {"export_csv_dir", "export-csv-dir"});

    if (!WriteTextFile(output_json, json, &error)) {
        std::cerr << "backtest_cli: " << error << '\n';
        return 1;
    }
    if (!WriteTextFile(output_md, markdown, &error)) {
        std::cerr << "backtest_cli: " << error << '\n';
        return 1;
    }
    if (!ExportBacktestCsv(result, export_csv_dir, &error)) {
        std::cerr << "backtest_cli: " << error << '\n';
        return 1;
    }

    std::cout << json;
    return 0;
}
