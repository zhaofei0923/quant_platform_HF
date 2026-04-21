#include "quant_hft/optim/result_analyzer.h"

#include <gtest/gtest.h>

#include <cmath>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <sstream>
#include <string>

#include "quant_hft/core/simple_json.h"
#include "quant_hft/optim/parameter_space.h"

namespace quant_hft::optim {
namespace {

using quant_hft::simple_json::Value;

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

std::filesystem::path MakeTempDirectory(const std::string& suffix) {
    const auto stamp = std::chrono::steady_clock::now().time_since_epoch().count();
    const auto path =
        std::filesystem::temp_directory_path() /
        ("quant_hft_result_analyzer_test_dir_" + std::to_string(stamp) + suffix);
    std::filesystem::create_directories(path);
    return path;
}

std::string ReadFile(const std::filesystem::path& path) {
    std::ifstream input(path);
    std::ostringstream buffer;
    buffer << input.rdbuf();
    return buffer.str();
}

Trial MakeTrial(const std::string& trial_id,
                double kama_filter,
                double stop_loss_atr_multiplier,
                double risk_per_trade_pct,
                double objective,
                const std::string& status) {
    Trial trial;
    trial.trial_id = trial_id;
    trial.status = status;
    trial.objective = objective;
    trial.params.values["kama_filter"] = kama_filter;
    trial.params.values["stop_loss_atr_multiplier"] = stop_loss_atr_multiplier;
    trial.params.values["risk_per_trade_pct"] = risk_per_trade_pct;
    trial.params.values["regime"] = std::string("trend");
    return trial;
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

TEST(ResultAnalyzerTest, ComputeObjectiveFromJsonTextSupportsWeightedObjectives) {
    OptimizationConfig config;
    config.maximize = true;

    OptimizationObjective profit;
    profit.metric_path = "summary.total_pnl";
    profit.weight = 0.6;
    profit.maximize = true;
    profit.scale_by_initial_equity = true;

    OptimizationObjective drawdown;
    drawdown.metric_path = "summary.max_drawdown";
    drawdown.weight = 0.4;
    drawdown.maximize = false;
    drawdown.scale_by_initial_equity = true;

    config.objectives = {profit, drawdown};

    const std::string json_text =
        "{\n"
        "  \"initial_equity\": 1000.0,\n"
        "  \"summary\": {\"total_pnl\": 120.0, \"max_drawdown\": 50.0}\n"
        "}\n";

    std::string error;
    const double score = ResultAnalyzer::ComputeObjectiveFromJsonText(json_text, config, &error);
    EXPECT_TRUE(error.empty()) << error;
    EXPECT_NEAR(score, 0.052, 1e-12);
}

TEST(ResultAnalyzerTest, AnalyzeCapturesWeightedObjectiveMetadata) {
    Trial trial;
    trial.trial_id = "weighted";
    trial.status = "completed";
    trial.objective = 0.1;

    OptimizationConfig config;
    config.maximize = true;

    OptimizationObjective profit;
    profit.metric_path = "total_pnl";
    profit.weight = 0.6;
    profit.maximize = true;
    profit.scale_by_initial_equity = true;

    OptimizationObjective drawdown;
    drawdown.metric_path = "max_drawdown";
    drawdown.weight = 0.4;
    drawdown.maximize = false;
    drawdown.scale_by_initial_equity = true;

    config.objectives = {profit, drawdown};

    const auto report = ResultAnalyzer::Analyze({trial}, config, false);
    EXPECT_EQ(report.metric_path, "weighted_objective");
    ASSERT_EQ(report.objectives.size(), 2U);
    EXPECT_EQ(report.objectives[0].metric_path, "summary.total_pnl");
    EXPECT_EQ(report.objectives[1].metric_path, "summary.max_drawdown");
}

TEST(ResultAnalyzerTest, ExtractTrialMetricsDerivesSnapshotMetrics) {
    const auto json_path = WriteTempFile(
        ".metrics.json",
        "{\n"
        "  \"summary\": {\"total_pnl\": 32.0, \"max_drawdown\": 80.0},\n"
        "  \"hf_standard\": {\n"
        "    \"advanced_summary\": {\"profit_factor\": 2.391304347826087},\n"
        "    \"daily\": [\n"
        "      {\"date\": \"2024-01-01\", \"daily_return_pct\": 0.10, \"cumulative_return_pct\": 0.10, \"drawdown_pct\": 0.05},\n"
        "      {\"date\": \"2024-01-02\", \"daily_return_pct\": -0.02, \"cumulative_return_pct\": 0.08, \"drawdown_pct\": 0.20}\n"
        "    ],\n"
        "    \"trades\": [\n"
        "      {\"fill_seq\": 1, \"trade_id\": \"rt1\", \"symbol\": \"rb\", \"side\": \"BUY\", \"offset\": \"OPEN\", \"volume\": 1, \"commission\": 2.0, \"realized_pnl\": 0.0, \"risk_budget_r\": 100.0, \"signal_type\": \"entry\"},\n"
        "      {\"fill_seq\": 2, \"trade_id\": \"rt1_close\", \"symbol\": \"rb\", \"side\": \"SELL\", \"offset\": \"CLOSE\", \"volume\": 1, \"commission\": 3.0, \"realized_pnl\": 60.0, \"risk_budget_r\": 0.0, \"signal_type\": \"exit\"},\n"
        "      {\"fill_seq\": 3, \"trade_id\": \"rt2\", \"symbol\": \"rb\", \"side\": \"SELL\", \"offset\": \"OPEN\", \"volume\": 1, \"commission\": 1.0, \"realized_pnl\": 0.0, \"risk_budget_r\": 100.0, \"signal_type\": \"entry\"},\n"
        "      {\"fill_seq\": 4, \"trade_id\": \"rt2_close\", \"symbol\": \"rb\", \"side\": \"BUY\", \"offset\": \"CLOSE\", \"volume\": 1, \"commission\": 2.0, \"realized_pnl\": -20.0, \"risk_budget_r\": 0.0, \"signal_type\": \"exit\"}\n"
        "    ]\n"
        "  },\n"
        "  \"deterministic\": {\"instrument_pnl\": {}}\n"
        "}\n");

    TrialMetricsSnapshot metrics;
    std::string error;
    EXPECT_TRUE(ResultAnalyzer::ExtractTrialMetricsFromJson(json_path.string(), &metrics, &error))
        << error;
    EXPECT_TRUE(error.empty()) << error;

    ASSERT_TRUE(metrics.total_pnl.has_value());
    EXPECT_DOUBLE_EQ(*metrics.total_pnl, 32.0);
    ASSERT_TRUE(metrics.max_drawdown.has_value());
    EXPECT_DOUBLE_EQ(*metrics.max_drawdown, 80.0);
    ASSERT_TRUE(metrics.max_drawdown_pct.has_value());
    EXPECT_DOUBLE_EQ(*metrics.max_drawdown_pct, 0.20);

    const double expected_annualized = (std::pow(1.0008, 126.0) - 1.0) * 100.0;
    ASSERT_TRUE(metrics.annualized_return_pct.has_value());
    EXPECT_NEAR(*metrics.annualized_return_pct, expected_annualized, 1e-9);

    const double expected_sharpe = (0.0004 / std::sqrt(7.2e-7)) * std::sqrt(252.0);
    ASSERT_TRUE(metrics.sharpe_ratio.has_value());
    EXPECT_NEAR(*metrics.sharpe_ratio, expected_sharpe, 1e-9);

    ASSERT_TRUE(metrics.calmar_ratio.has_value());
    EXPECT_NEAR(*metrics.calmar_ratio, expected_annualized / 0.20, 1e-9);
    ASSERT_TRUE(metrics.profit_factor.has_value());
    EXPECT_NEAR(*metrics.profit_factor, 55.0 / 23.0, 1e-12);
    ASSERT_TRUE(metrics.win_rate_pct.has_value());
    EXPECT_DOUBLE_EQ(*metrics.win_rate_pct, 50.0);
    ASSERT_TRUE(metrics.total_trades.has_value());
    EXPECT_EQ(*metrics.total_trades, 2);
    ASSERT_TRUE(metrics.expectancy_r.has_value());
    EXPECT_NEAR(*metrics.expectancy_r, 0.2, 1e-12);

    std::error_code ec;
    std::filesystem::remove(json_path, ec);
}

TEST(ResultAnalyzerTest, ExtractTrialMetricsAcceptsEmptyDailyAndTradesArrays) {
    const auto json_path = WriteTempFile(
        ".empty_metrics.json",
        "{\n"
        "  \"summary\": {\"total_pnl\": 0.0, \"max_drawdown\": 0.0},\n"
        "  \"hf_standard\": {\n"
        "    \"advanced_summary\": {\"profit_factor\": 0.0},\n"
        "    \"daily\": [],\n"
        "    \"trades\": []\n"
        "  },\n"
        "  \"deterministic\": {\"instrument_pnl\": {}}\n"
        "}\n");

    TrialMetricsSnapshot metrics;
    std::string error;
    EXPECT_TRUE(ResultAnalyzer::ExtractTrialMetricsFromJson(json_path.string(), &metrics, &error))
        << error;
    EXPECT_TRUE(error.empty()) << error;

    ASSERT_TRUE(metrics.total_pnl.has_value());
    EXPECT_DOUBLE_EQ(*metrics.total_pnl, 0.0);
    ASSERT_TRUE(metrics.max_drawdown.has_value());
    EXPECT_DOUBLE_EQ(*metrics.max_drawdown, 0.0);
    ASSERT_TRUE(metrics.profit_factor.has_value());
    EXPECT_DOUBLE_EQ(*metrics.profit_factor, 0.0);
    EXPECT_FALSE(metrics.annualized_return_pct.has_value());
    EXPECT_FALSE(metrics.sharpe_ratio.has_value());
    EXPECT_FALSE(metrics.calmar_ratio.has_value());

    std::error_code ec;
    std::filesystem::remove(json_path, ec);
}

TEST(ResultAnalyzerTest, EvaluateConstraintsDetectsViolationsUsingSupportedMetrics) {
    OptimizationConfig config;

    OptimizationConstraint profit_factor;
    OptimizationConstraint total_trades;
    OptimizationConstraint expectancy_r;
    OptimizationConstraint calmar_ratio;
    OptimizationConstraint sharpe_ratio;
    OptimizationConstraint max_drawdown_pct;
    std::string error;
    ASSERT_TRUE(ResultAnalyzer::ParseOptimizationConstraint("profit_factor > 2.0",
                                                            &profit_factor, &error))
        << error;
    ASSERT_TRUE(ResultAnalyzer::ParseOptimizationConstraint("total_trades >= 2",
                                                            &total_trades, &error))
        << error;
    ASSERT_TRUE(ResultAnalyzer::ParseOptimizationConstraint("expectancy_r >= 0.15",
                                                            &expectancy_r, &error))
        << error;
    ASSERT_TRUE(ResultAnalyzer::ParseOptimizationConstraint("calmar_ratio > 50.0",
                                                            &calmar_ratio, &error))
        << error;
    ASSERT_TRUE(ResultAnalyzer::ParseOptimizationConstraint("sharpe_ratio > 7.0",
                                                            &sharpe_ratio, &error))
        << error;
    ASSERT_TRUE(ResultAnalyzer::ParseOptimizationConstraint("max_drawdown_pct < 0.1",
                                                            &max_drawdown_pct, &error))
        << error;

    config.constraints = {profit_factor, total_trades, expectancy_r,
                          calmar_ratio, sharpe_ratio, max_drawdown_pct};

    const std::string json_text =
        "{\n"
        "  \"summary\": {\"total_pnl\": 32.0, \"max_drawdown\": 80.0},\n"
        "  \"hf_standard\": {\n"
        "    \"advanced_summary\": {\"profit_factor\": 2.391304347826087},\n"
        "    \"daily\": [\n"
        "      {\"date\": \"2024-01-01\", \"daily_return_pct\": 0.10, \"cumulative_return_pct\": 0.10, \"drawdown_pct\": 0.05},\n"
        "      {\"date\": \"2024-01-02\", \"daily_return_pct\": -0.02, \"cumulative_return_pct\": 0.08, \"drawdown_pct\": 0.20}\n"
        "    ],\n"
        "    \"trades\": [\n"
        "      {\"fill_seq\": 1, \"trade_id\": \"rt1\", \"symbol\": \"rb\", \"side\": \"BUY\", \"offset\": \"OPEN\", \"volume\": 1, \"commission\": 2.0, \"realized_pnl\": 0.0, \"risk_budget_r\": 100.0, \"signal_type\": \"entry\"},\n"
        "      {\"fill_seq\": 2, \"trade_id\": \"rt1_close\", \"symbol\": \"rb\", \"side\": \"SELL\", \"offset\": \"CLOSE\", \"volume\": 1, \"commission\": 3.0, \"realized_pnl\": 60.0, \"risk_budget_r\": 0.0, \"signal_type\": \"exit\"},\n"
        "      {\"fill_seq\": 3, \"trade_id\": \"rt2\", \"symbol\": \"rb\", \"side\": \"SELL\", \"offset\": \"OPEN\", \"volume\": 1, \"commission\": 1.0, \"realized_pnl\": 0.0, \"risk_budget_r\": 100.0, \"signal_type\": \"entry\"},\n"
        "      {\"fill_seq\": 4, \"trade_id\": \"rt2_close\", \"symbol\": \"rb\", \"side\": \"BUY\", \"offset\": \"CLOSE\", \"volume\": 1, \"commission\": 2.0, \"realized_pnl\": -20.0, \"risk_budget_r\": 0.0, \"signal_type\": \"exit\"}\n"
        "    ]\n"
        "  },\n"
        "  \"deterministic\": {\"instrument_pnl\": {}}\n"
        "}\n";

    std::vector<std::string> violations;
    EXPECT_TRUE(ResultAnalyzer::EvaluateConstraintsFromJsonText(json_text, config, &violations,
                                                                &error))
        << error;
    EXPECT_TRUE(error.empty()) << error;
    ASSERT_EQ(violations.size(), 1U);
    EXPECT_NE(violations[0].find("max_drawdown_pct < 0.1"), std::string::npos);
    EXPECT_NE(violations[0].find("actual=0.2"), std::string::npos);
}

TEST(ResultAnalyzerTest, WritesReportAndBestParamsYaml) {
    Trial best;
    best.trial_id = "best";
    best.status = "completed";
    best.objective = 4.2;
    best.stdout_log_path = "/tmp/stdout.log";
    best.stderr_log_path = "/tmp/stderr.log";
    best.working_dir = "/tmp/workdir";
    best.archived_artifact_dir = "/tmp/archive/best";
    best.metrics.total_pnl = 32.0;
    best.metrics.max_drawdown_pct = 0.2;
    best.metrics.total_trades = 2;
    best.params.values["take_profit_atr_multiplier"] = 20.0;

    OptimizationReport report;
    report.task_id = "task_123";
    report.started_at = "2024-01-01T00:00:00Z";
    report.finished_at = "2024-01-01T00:10:00Z";
    report.wall_clock_sec = 600.0;
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

    const auto temp_dir = MakeTempDirectory("_report");
    const auto json_path = temp_dir / "optimization_report.json";
    const auto md_path = temp_dir / "optimization_report.md";
    const auto yaml_path = temp_dir / "best_params.yaml";
    const auto top10_path = temp_dir / "top10_in_sample.md";

    std::string error;
    EXPECT_TRUE(ResultAnalyzer::WriteReport(report, json_path.string(), md_path.string(), &error))
        << error;
    EXPECT_TRUE(ResultAnalyzer::WriteBestParamsYaml(best.params, yaml_path.string(), &error))
        << error;

    const std::string json_text = ReadFile(json_path);
    EXPECT_NE(json_text.find("\"task_id\": \"task_123\""), std::string::npos);
    EXPECT_NE(json_text.find("\"all_objectives\""), std::string::npos);
    EXPECT_NE(json_text.find("\"metrics\""), std::string::npos);
    EXPECT_NE(json_text.find("\"archived_artifact_dir\""), std::string::npos);
    EXPECT_NE(json_text.find("take_profit_atr_multiplier"), std::string::npos);

    const std::string md_text = ReadFile(md_path);
    EXPECT_NE(md_text.find("## 任务元数据"), std::string::npos);
    EXPECT_NE(md_text.find("Top10 文件"), std::string::npos);

    const std::string top10_text = ReadFile(top10_path);
    EXPECT_NE(top10_text.find("# In-Sample Top 10"), std::string::npos);
    EXPECT_NE(top10_text.find("take_profit_atr_multiplier=20"), std::string::npos);

    const std::string yaml_text = ReadFile(yaml_path);
    EXPECT_NE(yaml_text.find("params:"), std::string::npos);
    EXPECT_NE(yaml_text.find("take_profit_atr_multiplier"), std::string::npos);

    std::error_code ec;
    std::filesystem::remove_all(temp_dir, ec);
}

TEST(ResultAnalyzerTest, AnalyzeTracksConstraintViolationsAndExcludesThemFromTop10) {
    Trial completed = MakeTrial("completed", 0.3, 3.0, 0.003, 2.0, "completed");
    completed.metrics.profit_factor = 2.0;

    Trial violated = MakeTrial("violated", 0.5, 3.0, 0.003, 9.0, "constraint_violated");
    violated.error_msg = "constraints violated: profit_factor > 10 (actual=9)";
    violated.metrics.profit_factor = 9.0;

    Trial failed = MakeTrial("failed", 0.5, 4.0, 0.003, 0.0, "failed");
    failed.error_msg = "backtest failed";

    OptimizationConfig config;
    config.metric_path = "profit_factor";
    config.maximize = true;

    const auto report = ResultAnalyzer::Analyze({completed, violated, failed}, config, false);
    EXPECT_EQ(report.completed_trials, 1);
    EXPECT_EQ(report.failed_trials, 1);
    EXPECT_EQ(report.constraint_stats.total_violations, 1);
    ASSERT_EQ(report.constraint_stats.violated_trials.size(), 1U);
    EXPECT_EQ(report.constraint_stats.violated_trials[0], "violated");
    EXPECT_EQ(report.best_trial.trial_id, "completed");
    ASSERT_EQ(report.all_objectives.size(), 3U);
    EXPECT_DOUBLE_EQ(report.all_objectives[1], 0.0);

    const auto temp_dir = MakeTempDirectory("_constraint_report");
    const auto json_path = temp_dir / "optimization_report.json";
    const auto md_path = temp_dir / "optimization_report.md";
    const auto top10_path = temp_dir / "top10_in_sample.md";

    std::string error;
    EXPECT_TRUE(ResultAnalyzer::WriteReport(report, json_path.string(), md_path.string(), &error))
        << error;

    const std::string json_text = ReadFile(json_path);
    EXPECT_NE(json_text.find("\"constraint_stats\""), std::string::npos);
    EXPECT_NE(json_text.find("\"total_violations\": 1"), std::string::npos);
    EXPECT_NE(json_text.find("\"status\": \"constraint_violated\""), std::string::npos);

    const std::string top10_text = ReadFile(top10_path);
    EXPECT_NE(top10_text.find("completed"), std::string::npos);
    EXPECT_EQ(top10_text.find("violated"), std::string::npos);

    std::error_code ec;
    std::filesystem::remove_all(temp_dir, ec);
}

TEST(ResultAnalyzerTest, WritesWeightedObjectivesToReport) {
    Trial best;
    best.trial_id = "best";
    best.status = "completed";
    best.objective = 0.052;

    OptimizationObjective profit;
    profit.metric_path = "summary.total_pnl";
    profit.weight = 0.6;
    profit.maximize = true;
    profit.scale_by_initial_equity = true;

    OptimizationObjective drawdown;
    drawdown.metric_path = "summary.max_drawdown";
    drawdown.weight = 0.4;
    drawdown.maximize = false;
    drawdown.scale_by_initial_equity = true;

    OptimizationReport report;
    report.algorithm = "grid";
    report.metric_path = "weighted_objective";
    report.objectives = {profit, drawdown};
    report.maximize = true;
    report.total_trials = 1;
    report.completed_trials = 1;
    report.failed_trials = 0;
    report.best_trial = best;
    report.trials = {best};
    report.convergence_curve = {0.052};
    report.all_objectives = {0.052};

    const auto json_path = WriteTempFile(".weighted.report.json", "");
    const auto md_path = WriteTempFile(".weighted.report.md", "");

    std::string error;
    EXPECT_TRUE(ResultAnalyzer::WriteReport(report, json_path.string(), md_path.string(), &error))
        << error;

    const std::string json_text = ReadFile(json_path);
    EXPECT_NE(json_text.find("\"objectives\""), std::string::npos);
    EXPECT_NE(json_text.find("summary.total_pnl"), std::string::npos);
    EXPECT_NE(json_text.find("scale_by_initial_equity"), std::string::npos);

    const std::string md_text = ReadFile(md_path);
    EXPECT_NE(md_text.find("## 目标构成"), std::string::npos);
    EXPECT_NE(md_text.find("summary.max_drawdown"), std::string::npos);

    std::error_code ec;
    std::filesystem::remove(json_path, ec);
    std::filesystem::remove(md_path, ec);
}

TEST(ResultAnalyzerTest, WritesHeatmapsForNumericPairsWithAveragesAndNulls) {
    ParameterSpace space;

    ParameterDef kama_filter;
    kama_filter.name = "kama_filter";
    kama_filter.type = ParameterType::kDouble;
    kama_filter.values = {0.3, 0.5};

    ParameterDef stop_loss_atr_multiplier;
    stop_loss_atr_multiplier.name = "stop_loss_atr_multiplier";
    stop_loss_atr_multiplier.type = ParameterType::kDouble;
    stop_loss_atr_multiplier.values = {3.0, 4.0};

    ParameterDef risk_per_trade_pct;
    risk_per_trade_pct.name = "risk_per_trade_pct";
    risk_per_trade_pct.type = ParameterType::kDouble;
    risk_per_trade_pct.values = {0.003, 0.005};

    ParameterDef regime;
    regime.name = "regime";
    regime.type = ParameterType::kString;
    regime.values = {std::string("trend"), std::string("mean_revert")};

    space.parameters = {kama_filter, stop_loss_atr_multiplier, risk_per_trade_pct, regime};

    OptimizationConfig config;
    config.metric_path = "hf_standard.risk_metrics.calmar_ratio";
    config.maximize = true;

    const auto report = ResultAnalyzer::Analyze(
        {
            MakeTrial("t1", 0.3, 3.0, 0.003, 1.0, "completed"),
            MakeTrial("t2", 0.3, 3.0, 0.005, 3.0, "completed"),
            MakeTrial("t3", 0.5, 3.0, 0.003, 2.0, "completed"),
            MakeTrial("t4", 0.5, 4.0, 0.003, 4.0, "completed"),
            MakeTrial("t5", 0.3, 4.0, 0.003, 100.0, "failed"),
        },
        config, false);

    const auto temp_dir = MakeTempDirectory("_heatmap");
    std::string error;
    EXPECT_TRUE(ResultAnalyzer::WriteHeatmaps(report, space, temp_dir.string(), &error)) << error;

    const auto priority_path = temp_dir / "heatmap_kama_filter_vs_stop_loss_atr_multiplier.json";
    const auto risk_path = temp_dir / "heatmap_kama_filter_vs_risk_per_trade_pct.json";
    const auto stop_loss_risk_path = temp_dir / "heatmap_stop_loss_atr_multiplier_vs_risk_per_trade_pct.json";
    EXPECT_TRUE(std::filesystem::exists(priority_path));
    EXPECT_TRUE(std::filesystem::exists(risk_path));
    EXPECT_TRUE(std::filesystem::exists(stop_loss_risk_path));

    Value root;
    ASSERT_TRUE(quant_hft::simple_json::Parse(ReadFile(priority_path), &root, &error)) << error;
    const Value* x_param = root.Find("x_param");
    const Value* y_param = root.Find("y_param");
    const Value* objective = root.Find("objective");
    ASSERT_NE(x_param, nullptr);
    ASSERT_NE(y_param, nullptr);
    ASSERT_NE(objective, nullptr);
    EXPECT_EQ(x_param->string_value, "kama_filter");
    EXPECT_EQ(y_param->string_value, "stop_loss_atr_multiplier");
    EXPECT_EQ(objective->string_value, "hf_standard.risk_metrics.calmar_ratio");

    const Value* x_values = root.Find("x_values");
    const Value* y_values = root.Find("y_values");
    const Value* z_values = root.Find("z_values");
    ASSERT_NE(x_values, nullptr);
    ASSERT_NE(y_values, nullptr);
    ASSERT_NE(z_values, nullptr);
    ASSERT_TRUE(x_values->IsArray());
    ASSERT_TRUE(y_values->IsArray());
    ASSERT_TRUE(z_values->IsArray());
    ASSERT_EQ(x_values->array_value.size(), 2U);
    ASSERT_EQ(y_values->array_value.size(), 2U);
    ASSERT_EQ(z_values->array_value.size(), 2U);
    EXPECT_DOUBLE_EQ(x_values->array_value[0].number_value, 0.3);
    EXPECT_DOUBLE_EQ(x_values->array_value[1].number_value, 0.5);
    EXPECT_DOUBLE_EQ(y_values->array_value[0].number_value, 3.0);
    EXPECT_DOUBLE_EQ(y_values->array_value[1].number_value, 4.0);

    ASSERT_TRUE(z_values->array_value[0].IsArray());
    ASSERT_TRUE(z_values->array_value[1].IsArray());
    ASSERT_EQ(z_values->array_value[0].array_value.size(), 2U);
    ASSERT_EQ(z_values->array_value[1].array_value.size(), 2U);
    EXPECT_DOUBLE_EQ(z_values->array_value[0].array_value[0].number_value, 2.0);
    EXPECT_DOUBLE_EQ(z_values->array_value[0].array_value[1].number_value, 2.0);
    EXPECT_TRUE(z_values->array_value[1].array_value[0].IsNull());
    EXPECT_DOUBLE_EQ(z_values->array_value[1].array_value[1].number_value, 4.0);

    std::error_code ec;
    std::filesystem::remove_all(temp_dir, ec);
}

}  // namespace
}  // namespace quant_hft::optim
