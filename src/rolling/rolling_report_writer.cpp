#include "quant_hft/rolling/rolling_report_writer.h"

#include <iomanip>
#include <sstream>
#include <string>

#include "quant_hft/apps/cli_support.h"

namespace quant_hft::rolling {
namespace {

using quant_hft::apps::JsonEscape;
using quant_hft::apps::WriteTextFile;

std::string FormatDouble(double value) {
    std::ostringstream oss;
    oss << std::setprecision(12) << value;
    return oss.str();
}

}  // namespace

bool WriteRollingReportJson(const RollingReport& report,
                            const std::string& output_path,
                            std::string* error) {
    std::ostringstream json;
    json << "{\n"
         << "  \"mode\": \"" << JsonEscape(report.mode) << "\",\n"
         << "  \"interrupted\": " << (report.interrupted ? "true" : "false") << ",\n"
         << "  \"success_count\": " << report.success_count << ",\n"
         << "  \"failed_count\": " << report.failed_count << ",\n"
         << "  \"mean_objective\": " << FormatDouble(report.mean_objective) << ",\n"
         << "  \"std_objective\": " << FormatDouble(report.std_objective) << ",\n"
         << "  \"max_objective\": " << FormatDouble(report.max_objective) << ",\n"
         << "  \"min_objective\": " << FormatDouble(report.min_objective) << ",\n"
         << "  \"objectives\": [";

    for (std::size_t i = 0; i < report.objectives.size(); ++i) {
        if (i > 0) {
            json << ", ";
        }
        json << FormatDouble(report.objectives[i]);
    }

    json << "],\n"
         << "  \"windows\": [\n";

    for (std::size_t i = 0; i < report.windows.size(); ++i) {
        const WindowResult& window = report.windows[i];
        json << "    {\n"
             << "      \"index\": " << window.index << ",\n"
             << "      \"train_start\": \"" << JsonEscape(window.train_start) << "\",\n"
             << "      \"train_end\": \"" << JsonEscape(window.train_end) << "\",\n"
             << "      \"test_start\": \"" << JsonEscape(window.test_start) << "\",\n"
             << "      \"test_end\": \"" << JsonEscape(window.test_end) << "\",\n"
             << "      \"success\": " << (window.success ? "true" : "false") << ",\n"
             << "      \"objective\": " << FormatDouble(window.objective) << ",\n"
             << "      \"best_params_yaml\": \"" << JsonEscape(window.best_params_yaml)
             << "\",\n"
             << "      \"error_msg\": \"" << JsonEscape(window.error_msg) << "\",\n"
             << "      \"metrics\": {";

        bool first = true;
        for (const auto& [key, value] : window.metrics) {
            if (!first) {
                json << ", ";
            }
            first = false;
            json << "\"" << JsonEscape(key) << "\": " << FormatDouble(value);
        }

        json << "}\n"
             << "    }";
        if (i + 1 < report.windows.size()) {
            json << ',';
        }
        json << "\n";
    }

    json << "  ]\n"
         << "}\n";

    return WriteTextFile(output_path, json.str(), error);
}

bool WriteRollingReportMarkdown(const RollingReport& report,
                                const std::string& output_path,
                                std::string* error) {
    std::ostringstream md;
    md << "# Rolling Backtest Report\n\n"
       << "- Mode: `" << report.mode << "`\n"
       << "- Interrupted: `" << (report.interrupted ? "true" : "false") << "`\n"
       << "- Success: `" << report.success_count << "`\n"
       << "- Failed: `" << report.failed_count << "`\n"
       << "- Mean Objective: `" << FormatDouble(report.mean_objective) << "`\n"
       << "- Std Objective: `" << FormatDouble(report.std_objective) << "`\n"
       << "- Max Objective: `" << FormatDouble(report.max_objective) << "`\n"
       << "- Min Objective: `" << FormatDouble(report.min_objective) << "`\n\n"
       << "## Windows\n\n"
       << "| index | train | test | success | objective | best_params_yaml |\n"
       << "| --- | --- | --- | --- | --- | --- |\n";

    for (const WindowResult& window : report.windows) {
        md << "| " << window.index << " | " << window.train_start << "~" << window.train_end
           << " | " << window.test_start << "~" << window.test_end << " | "
           << (window.success ? "true" : "false") << " | " << FormatDouble(window.objective)
           << " | " << window.best_params_yaml << " |\n";
    }

    md << "\n## Failed Windows\n\n";
    for (const WindowResult& window : report.windows) {
        if (window.success) {
            continue;
        }
        md << "- window " << window.index << ": " << window.error_msg << "\n";
    }

    return WriteTextFile(output_path, md.str(), error);
}

}  // namespace quant_hft::rolling

