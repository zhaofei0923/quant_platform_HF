#include "quant_hft/optim/result_analyzer.h"

#include <gtest/gtest.h>

#include <chrono>
#include <filesystem>
#include <fstream>
#include <string>

namespace quant_hft::optim {
namespace {

std::filesystem::path WriteTempFile(const std::string& suffix, const std::string& content) {
    const auto stamp = std::chrono::steady_clock::now().time_since_epoch().count();
    const auto path =
        std::filesystem::temp_directory_path() /
        ("quant_hft_result_analyzer_test_" + std::to_string(stamp) + suffix);
    std::ofstream out(path);
    out << content;
    out.close();
    return path;
}

std::string ReadFile(const std::filesystem::path& path) {
    std::ifstream input(path);
    std::ostringstream buffer;
    buffer << input.rdbuf();
    return buffer.str();
}

TEST(ResultAnalyzerTest, ExtractMetricSupportsAliasMapping) {
    const auto json_path = WriteTempFile(
        ".json",
        "{\n"
        "  \"summary\": {\"total_pnl\": 12.3},\n"
        "  \"hf_standard\": {\n"
        "    \"advanced_summary\": {\"profit_factor\": 2.5}\n"
        "  }\n"
        "}\n");

    std::string error;
    const double value =
        ResultAnalyzer::ExtractMetricFromJson(json_path.string(), "hf_standard.profit_factor", &error);
    EXPECT_TRUE(error.empty()) << error;
    EXPECT_DOUBLE_EQ(value, 2.5);

    std::error_code ec;
    std::filesystem::remove(json_path, ec);
}

TEST(ResultAnalyzerTest, ExtractMetricFromJsonTextWorks) {
    const std::string json_text =
        "{\n"
        "  \"summary\": {\"total_pnl\": 12.3},\n"
        "  \"hf_standard\": {\n"
        "    \"advanced_summary\": {\"profit_factor\": 2.5}\n"
        "  }\n"
        "}\n";

    std::string error;
    const double value =
        ResultAnalyzer::ExtractMetricFromJsonText(json_text, "hf_standard.profit_factor", &error);
    EXPECT_TRUE(error.empty()) << error;
    EXPECT_DOUBLE_EQ(value, 2.5);
}

TEST(ResultAnalyzerTest, AnalyzeBuildsConvergenceAndObjectives) {
    Trial t1;
    t1.trial_id = "t1";
    t1.status = "completed";
    t1.objective = 1.0;

    Trial t2;
    t2.trial_id = "t2";
    t2.status = "failed";
    t2.error_msg = "oops";

    Trial t3;
    t3.trial_id = "t3";
    t3.status = "completed";
    t3.objective = 3.0;

    OptimizationConfig config;
    config.algorithm = "grid";
    config.metric_path = "hf_standard.profit_factor";
    config.maximize = true;

    const auto report = ResultAnalyzer::Analyze({t1, t2, t3}, config, false);
    EXPECT_EQ(report.total_trials, 3);
    EXPECT_EQ(report.completed_trials, 2);
    EXPECT_EQ(report.failed_trials, 1);
    EXPECT_EQ(report.best_trial.trial_id, "t3");
    ASSERT_EQ(report.convergence_curve.size(), 3U);
    EXPECT_DOUBLE_EQ(report.convergence_curve[0], 1.0);
    EXPECT_DOUBLE_EQ(report.convergence_curve[1], 1.0);
    EXPECT_DOUBLE_EQ(report.convergence_curve[2], 3.0);
    ASSERT_EQ(report.all_objectives.size(), 3U);
    EXPECT_DOUBLE_EQ(report.all_objectives[0], 1.0);
    EXPECT_DOUBLE_EQ(report.all_objectives[1], 0.0);
    EXPECT_DOUBLE_EQ(report.all_objectives[2], 3.0);
}

TEST(ResultAnalyzerTest, WritesReportAndBestParamsYaml) {
    Trial best;
    best.trial_id = "best";
    best.status = "completed";
    best.objective = 4.2;
    best.params.values["take_profit_atr_multiplier"] = 20.0;

    OptimizationReport report;
    report.algorithm = "grid";
    report.metric_path = "hf_standard.advanced_summary.profit_factor";
    report.maximize = true;
    report.total_trials = 1;
    report.completed_trials = 1;
    report.failed_trials = 0;
    report.best_trial = best;
    report.trials = {best};
    report.convergence_curve = {4.2};
    report.all_objectives = {4.2};

    const auto json_path = WriteTempFile(".report.json", "");
    const auto md_path = WriteTempFile(".report.md", "");
    const auto yaml_path = WriteTempFile(".best.yaml", "");

    std::string error;
    EXPECT_TRUE(ResultAnalyzer::WriteReport(report, json_path.string(), md_path.string(), &error))
        << error;
    EXPECT_TRUE(ResultAnalyzer::WriteBestParamsYaml(best.params, yaml_path.string(), &error))
        << error;

    const std::string json_text = ReadFile(json_path);
    EXPECT_NE(json_text.find("\"all_objectives\""), std::string::npos);
    EXPECT_NE(json_text.find("take_profit_atr_multiplier"), std::string::npos);

    const std::string yaml_text = ReadFile(yaml_path);
    EXPECT_NE(yaml_text.find("params:"), std::string::npos);
    EXPECT_NE(yaml_text.find("take_profit_atr_multiplier"), std::string::npos);

    std::error_code ec;
    std::filesystem::remove(json_path, ec);
    std::filesystem::remove(md_path, ec);
    std::filesystem::remove(yaml_path, ec);
}

}  // namespace
}  // namespace quant_hft::optim
