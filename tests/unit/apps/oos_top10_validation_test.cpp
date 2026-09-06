#include "quant_hft/rolling/oos_top10_validation.h"

#include <gtest/gtest.h>

#include <chrono>
#include <filesystem>
#include <fstream>
#include <optional>
#include <sstream>
#include <string>
#include <vector>

#include "quant_hft/apps/backtest_replay_support.h"
#include "quant_hft/optim/result_analyzer.h"

namespace quant_hft::rolling {
namespace {

using quant_hft::apps::DailyPerformance;
using quant_hft::apps::TradeRecord;
using quant_hft::backtest::BacktestCliResult;
using quant_hft::backtest::BacktestCliSpec;

std::filesystem::path MakeTempDir(const std::string& stem) {
    const auto stamp = std::chrono::steady_clock::now().time_since_epoch().count();
    const auto dir = std::filesystem::temp_directory_path() / (stem + "_" + std::to_string(stamp));
    std::filesystem::create_directories(dir);
    return dir;
}

std::filesystem::path WriteFile(const std::filesystem::path& path, const std::string& content) {
    std::filesystem::create_directories(path.parent_path());
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

BacktestCliSpec MakeArchivedSpec(const std::filesystem::path& archive_composite) {
    BacktestCliSpec spec;
    spec.behavior_profile = "research";
    spec.parameter_profile = "backtest";
    const auto inputs = archive_composite.parent_path() / "inputs";
    spec.dataset_root = (inputs / "dataset").string();
    spec.dataset_manifest = WriteFile(inputs / "dataset" / "manifest.json", "manifest-v1").string();
    WriteFile(inputs / "dataset" / "ticks.parquet", "data-v1");
    spec.engine_mode = "parquet";
    spec.rollover_mode = "expiry_close";
    spec.product_series_mode = "raw";
    spec.rollover_price_mode = "bbo";
    spec.symbols = {"c"};
    spec.start_date = "20240102";
    spec.end_date = "20240628";
    spec.max_ticks = 12000;
    spec.deterministic_fills = true;
    spec.streaming = true;
    spec.strict_parquet = true;
    spec.account_id = "sim-account";
    spec.run_id = "train-run";
    spec.initial_equity = 200000.0;
    spec.product_config_path = WriteFile(inputs / "fees.json", "fees-v1").string();
    spec.contract_expiry_calendar_path = WriteFile(inputs / "expiry.yaml", "expiry-v1").string();
    spec.strategy_main_config_path = WriteFile(inputs / "main.yaml", "main-v1").string();
    spec.strategy_factory = "composite";
    spec.strategy_composite_config = archive_composite.string();
    quant_hft::backtest::BacktestStrategyConfig resolved;
    resolved.strategy_factory = "composite";
    resolved.strategy_id = "archived_strategy";
    resolved.strategy_composite_config = (inputs / "deleted_trial_composite.yaml").string();
    spec.strategy_configs.push_back(resolved);
    spec.emit_trades = true;
    spec.emit_orders = false;
    spec.emit_position_history = false;
    return spec;
}

BacktestCliResult MakeBacktestResult(const BacktestCliSpec& spec, const std::string& run_id,
                                     double total_pnl, double max_drawdown, double profit_factor,
                                     const std::vector<DailyPerformance>& daily = {}) {
    BacktestCliResult result;
    result.run_id = run_id;
    result.mode = "deterministic";
    result.data_source = "parquet";
    result.engine_mode = spec.engine_mode;
    result.rollover_mode = spec.rollover_mode;
    result.spec = spec;
    result.initial_equity = spec.initial_equity;
    result.final_equity = spec.initial_equity + total_pnl;
    result.advanced_summary.profit_factor = profit_factor;
    result.daily = daily.empty()
                       ? std::vector<DailyPerformance>{
                             DailyPerformance{"2024-07-01", spec.initial_equity, 0.10, 0.10,
                                              0.05, 0.0, 1, 0.0, ""},
                             DailyPerformance{"2024-07-02", spec.initial_equity, -0.02, 0.08,
                                              0.20, 0.0, 1, 0.0, ""},
                         }
                       : daily;
    result.trades = {
        TradeRecord{
            "rt1",      "o1",       "c",        "SHFE", "BUY",          "OPEN",  1,  0.0, 0, "",
            2.0,        0.0,        0.0,        100.0,  "kama_trend_1", "entry", "", 1,   0, "",
            "20240701", "20240701", "09:01:00", ""},
        TradeRecord{
            "rt1_close", "o2",       "c",        "SHFE", "SELL",         "CLOSE", 1,  0.0, 0, "",
            3.0,         0.0,        60.0,       0.0,    "kama_trend_1", "exit",  "", 2,   0, "",
            "20240701",  "20240701", "09:05:00", ""},
        TradeRecord{
            "rt2",      "o3",       "c",        "SHFE", "SELL",         "OPEN",  1,  0.0, 0, "",
            1.0,        0.0,        0.0,        100.0,  "kama_trend_1", "entry", "", 3,   0, "",
            "20240702", "20240702", "09:10:00", ""},
        TradeRecord{
            "rt2_close", "o4",       "c",        "SHFE", "BUY",          "CLOSE", 1,  0.0, 0, "",
            2.0,         0.0,        -20.0,      0.0,    "kama_trend_1", "exit",  "", 4,   0, "",
            "20240702",  "20240702", "09:20:00", ""},
    };
    result.has_deterministic = true;
    result.deterministic.performance.total_pnl = total_pnl;
    result.deterministic.performance.max_drawdown = max_drawdown;
    return result;
}

std::vector<DailyPerformance> MakeDailySeries(double day1_return, double day1_cumulative,
                                              double day1_drawdown, double day2_return,
                                              double day2_cumulative, double day2_drawdown) {
    return {
        DailyPerformance{"2024-07-01", 200000.0, day1_return, day1_cumulative, day1_drawdown, 0.0,
                         1, 0.0, ""},
        DailyPerformance{"2024-07-02", 200000.0, day2_return, day2_cumulative, day2_drawdown, 0.0,
                         1, 0.0, ""},
    };
}

std::string StripLeadingCommentLines(const std::string& text) {
    std::istringstream input(text);
    std::ostringstream output;
    std::string line;
    bool first_body_line = true;
    while (std::getline(input, line)) {
        if (!line.empty() && line.back() == '\r') {
            line.pop_back();
        }
        if (line.rfind("#", 0) == 0) {
            continue;
        }
        if (!first_body_line) {
            output << '\n';
        }
        first_body_line = false;
        output << line;
    }
    if (!text.empty() && text.back() == '\n') {
        output << '\n';
    }
    return output.str();
}

std::filesystem::path WriteArchivedTrial(const std::filesystem::path& top_trials_dir,
                                         const std::string& dir_name, const std::string& trial_id) {
    const auto archived_dir = top_trials_dir / dir_name;
    const auto archived_composite = WriteFile(archived_dir / "composite.yaml",
                                              "composite:\n"
                                              "  run_type: backtest\n"
                                              "  merge_rule: kPriority\n"
                                              "  sub_strategies:\n"
                                              "    - id: kama_trend_1\n"
                                              "      enabled: true\n"
                                              "      type: KamaTrendStrategy\n"
                                              "      config_path: /tmp/old_" +
                                                  trial_id + "/target_sub_strategy.yaml\n");
    WriteFile(archived_dir / "target_sub_strategy.yaml",
              "params:\n"
              "  id: kama_trend_1\n"
              "  fast_period: 10\n");

    const BacktestCliSpec archived_spec = MakeArchivedSpec(archived_composite);
    const BacktestCliResult archived_result =
        MakeBacktestResult(archived_spec, "archived-" + trial_id, 32.0, 80.0, 2.3913043478);
    WriteFile(archived_dir / "result.json", quant_hft::apps::RenderBacktestJson(archived_result));
    return archived_dir;
}

std::filesystem::path WriteTrainReport(const std::filesystem::path& runtime_root) {
    const auto train_report =
        runtime_root / "train_reports" / "window_0000" / "parameter_optim_report.json";
    WriteFile(train_report,
              "{\n"
              "  \"trials\": [\n"
              "    {\n"
              "      \"trial_id\": \"trial_alpha\",\n"
              "      \"status\": \"completed\",\n"
              "      \"objective\": 2.0,\n"
              "      \"params\": {\"fast_period\": 10, \"threshold\": 0.3},\n"
              "      \"metrics\": {\"calmar_ratio\": 2.0, \"sharpe_ratio\": 1.4, "
              "\"max_drawdown_pct\": 0.18}\n"
              "    },\n"
              "    {\n"
              "      \"trial_id\": \"trial_beta\",\n"
              "      \"status\": \"completed\",\n"
              "      \"objective\": 2.0,\n"
              "      \"params\": {\"fast_period\": 12, \"threshold\": 0.25},\n"
              "      \"metrics\": {\"calmar_ratio\": 2.0, \"sharpe_ratio\": 2.1, "
              "\"max_drawdown_pct\": 0.16}\n"
              "    },\n"
              "    {\n"
              "      \"trial_id\": \"trial_gamma\",\n"
              "      \"status\": \"completed\",\n"
              "      \"objective\": 1.5,\n"
              "      \"params\": {\"fast_period\": 14, \"threshold\": 0.2},\n"
              "      \"metrics\": {\"calmar_ratio\": 1.5, \"sharpe_ratio\": 3.0, "
              "\"max_drawdown_pct\": 0.14}\n"
              "    }\n"
              "  ]\n"
              "}\n");
    return train_report;
}

TEST(OosTop10ValidationTest, RanksByCalmarThenSharpeAndReusesExistingResults) {
    const auto dir = MakeTempDir("oos_top10_validation");
    const auto runtime_root = dir / "runtime" / "rolling_optimize_kama";
    const auto top_trials_dir = runtime_root / "top_trials" / "window_0000";
    WriteArchivedTrial(top_trials_dir, "01_trial_alpha", "trial_alpha");
    WriteArchivedTrial(top_trials_dir, "02_trial_beta", "trial_beta");
    WriteArchivedTrial(top_trials_dir, "03_trial_gamma", "trial_gamma");
    const auto train_report = WriteTrainReport(runtime_root);

    const auto cached_dir = runtime_root / "oos_validation" / "window_0000" / "02_trial_alpha";
    const auto cached_composite = cached_dir / "composite.yaml";
    WriteFile(cached_composite,
              "composite:\n"
              "  sub_strategies:\n"
              "    - id: kama_trend_1\n"
              "      config_path: " +
                  (cached_dir / "target_sub_strategy.yaml").string() + "\n");
    WriteFile(cached_dir / "target_sub_strategy.yaml", "params:\n  id: kama_trend_1\n");
    const BacktestCliSpec cached_spec = MakeArchivedSpec(cached_composite);
    const BacktestCliResult cached_result =
        MakeBacktestResult(cached_spec, "cached-trial-alpha", 55.0, 80.0, 2.3913043478,
                           MakeDailySeries(0.02, 0.02, 0.10, 0.01, 0.03, 0.10));
    WriteFile(cached_dir / "result.json", quant_hft::apps::RenderBacktestJson(cached_result));

    int run_count = 0;
    OosBacktestRunFn run_fn = [&](const BacktestCliSpec& spec, BacktestCliResult* out,
                                  std::string* error) {
        (void)error;
        ++run_count;
        EXPECT_EQ(spec.start_date, "20240701");
        EXPECT_EQ(spec.end_date, "20241231");
        EXPECT_TRUE(std::filesystem::exists(spec.strategy_composite_config));
        EXPECT_EQ(spec.strategy_configs.size(), 1U);
        if (!spec.strategy_configs.empty()) {
            EXPECT_EQ(spec.strategy_configs.front().strategy_composite_config,
                      spec.strategy_composite_config);
        }
        const std::string composite_text = ReadFile(spec.strategy_composite_config);
        EXPECT_EQ(composite_text.find("/tmp/old_"), std::string::npos);
        EXPECT_NE(composite_text.find("target_sub_strategy.yaml"), std::string::npos);

        *out = MakeBacktestResult(spec, spec.run_id, 32.0, 80.0, 2.3913043478,
                                  MakeDailySeries(0.10, 0.10, 0.05, 0.10, 0.20, 0.05));
        return true;
    };

    OosTop10ValidationRequest request;
    request.train_report_json = train_report.string();
    request.oos_start_date = "2024-07-01";
    request.oos_end_date = "2024-12-31";
    request.top_n = 2;

    OosTop10ValidationReport report;
    std::string error;
    ASSERT_TRUE(RunOosTop10Validation(request, &report, &error, run_fn)) << error;

    ASSERT_EQ(report.rows.size(), 2U);
    EXPECT_EQ(report.rows[0].trial_id, "trial_beta");
    EXPECT_EQ(report.rows[1].trial_id, "trial_alpha");
    EXPECT_EQ(report.recommended_trial_id, "trial_beta");
    EXPECT_EQ(report.success_count, 2);
    EXPECT_EQ(report.failed_count, 0);
    EXPECT_EQ(run_count, 2);
    EXPECT_EQ(report.rows[0].status, "completed");
    EXPECT_EQ(report.rows[1].status, "completed");
    ASSERT_TRUE(report.rows[0].oos_total_pnl.has_value());
    EXPECT_DOUBLE_EQ(*report.rows[0].oos_total_pnl, 32.0);
    ASSERT_TRUE(report.rows[1].oos_total_pnl.has_value());
    EXPECT_DOUBLE_EQ(*report.rows[1].oos_total_pnl, 32.0);
    ASSERT_TRUE(report.rows[0].oos_trades.has_value());
    EXPECT_EQ(*report.rows[0].oos_trades, 2);

    // A legacy result with no content receipt is never trusted. Identical reruns are cached.
    ASSERT_TRUE(RunOosTop10Validation(request, &report, &error, run_fn)) << error;
    EXPECT_EQ(run_count, 2);
    EXPECT_TRUE(report.rows[0].reused_existing);
    EXPECT_TRUE(report.rows[1].reused_existing);
    const std::string csv_text = ReadFile(report.output_csv);
    EXPECT_NE(csv_text.find("Rank,Trial ID,Params"), std::string::npos);
    EXPECT_LT(csv_text.find("trial_beta"), csv_text.find("trial_alpha"));
    EXPECT_NE(csv_text.find("cached"), std::string::npos);

    ASSERT_FALSE(report.final_recommended_params_yaml.empty());
    const std::string recommended_yaml = ReadFile(report.final_recommended_params_yaml);
    EXPECT_NE(recommended_yaml.find("# Validation selection among training candidates; this "
                                    "interval is not an untouched final test."),
              std::string::npos);
    EXPECT_NE(recommended_yaml.find("# OOS Calmar: "), std::string::npos);
    EXPECT_NE(recommended_yaml.find("OOS Sharpe: "), std::string::npos);
    EXPECT_NE(recommended_yaml.find("OOS MaxDD: "), std::string::npos);

    const auto expected_yaml_path = dir / "expected_final_params.yaml";
    std::string local_error;
    ASSERT_TRUE(quant_hft::optim::ResultAnalyzer::WriteBestParamsYaml(
        report.rows[0].params, expected_yaml_path.string(), &local_error))
        << local_error;
    EXPECT_EQ(StripLeadingCommentLines(recommended_yaml), ReadFile(expected_yaml_path));

    const std::error_code ec{};
    std::filesystem::remove_all(dir);
}

TEST(OosTop10ValidationTest, CacheIdentityIncludesDatesAndContentEvenWithSameSizeAndMtime) {
    const auto dir = MakeTempDir("oos_cache_content");
    const auto runtime_root = dir / "runtime" / "rolling";
    const auto archive = WriteArchivedTrial(runtime_root / "top_trials" / "window_0000",
                                            "01_trial_beta", "trial_beta");
    OosTop10ValidationRequest request;
    request.train_report_json = WriteTrainReport(runtime_root).string();
    request.oos_start_date = "20240701";
    request.oos_end_date = "20240731";
    request.top_n = 1;
    int runs = 0;
    const OosBacktestRunFn run = [&](const BacktestCliSpec& spec, BacktestCliResult* result,
                                     std::string*) {
        ++runs;
        *result = MakeBacktestResult(spec, spec.run_id, 10, 80, 2);
        return true;
    };
    OosTop10ValidationReport report;
    std::string error;
    ASSERT_TRUE(RunOosTop10Validation(request, &report, &error, run)) << error;
    const auto first_result = report.rows[0].result_json_path;
    ASSERT_TRUE(RunOosTop10Validation(request, &report, &error, run)) << error;
    EXPECT_EQ(runs, 1);
    request.oos_end_date = "20240831";
    ASSERT_TRUE(RunOosTop10Validation(request, &report, &error, run)) << error;
    EXPECT_EQ(runs, 2);
    EXPECT_NE(first_result, report.rows[0].result_json_path);
    for (const auto& input :
         {archive / "target_sub_strategy.yaml", archive / "inputs" / "fees.json",
          archive / "inputs" / "dataset" / "ticks.parquet"}) {
        const auto original_time = std::filesystem::last_write_time(input);
        auto content = ReadFile(input);
        content.back() = content.back() == '1' ? '2' : '1';
        WriteFile(input, content);
        std::filesystem::last_write_time(input, original_time);
        ASSERT_TRUE(RunOosTop10Validation(request, &report, &error, run)) << error;
        EXPECT_FALSE(report.rows[0].reused_existing);
    }
    EXPECT_EQ(runs, 5);
    request.oos_start_date = "20240601";
    ASSERT_FALSE(RunOosTop10Validation(request, &report, &error, run));
    EXPECT_EQ(runs, 5);
    std::filesystem::remove_all(dir);
}

TEST(OosTop10ValidationTest, UsesTrainingObjectiveDirectionForCandidateRankingAndValidation) {
    const auto dir = MakeTempDir("oos_minimize_objective");
    const auto runtime_root = dir / "runtime" / "rolling";
    const auto archive_root = runtime_root / "top_trials" / "window_0000";
    WriteArchivedTrial(archive_root, "01_trial_alpha", "trial_alpha");
    WriteArchivedTrial(archive_root, "02_trial_beta", "trial_beta");
    WriteArchivedTrial(archive_root, "03_trial_gamma", "trial_gamma");
    const auto train = WriteTrainReport(runtime_root);
    auto text = ReadFile(train);
    text.insert(
        text.find('{') + 1,
        "\"metric_path\":\"hf_standard.risk_metrics.max_drawdown_pct\",\"maximize\":false,");
    WriteFile(train, text);
    OosTop10ValidationRequest request;
    request.train_report_json = train.string();
    request.oos_start_date = "20240701";
    request.oos_end_date = "20240731";
    request.top_n = 3;
    const OosBacktestRunFn run = [&](const BacktestCliSpec& spec, BacktestCliResult* result,
                                     std::string*) {
        const double dd = spec.run_id.find("alpha") != std::string::npos ? 0.02 : 0.10;
        *result = MakeBacktestResult(spec, spec.run_id, 10, 80, 2,
                                     MakeDailySeries(.1, .1, dd, .1, .2, dd));
        return true;
    };
    OosTop10ValidationReport report;
    std::string error;
    ASSERT_TRUE(RunOosTop10Validation(request, &report, &error, run)) << error;
    EXPECT_EQ(report.rows.front().trial_id, "trial_gamma");
    EXPECT_EQ(report.recommended_trial_id, "trial_alpha");
    EXPECT_FALSE(report.maximize);
    EXPECT_EQ(report.evaluation_role, "validation_selection");
    std::filesystem::remove_all(dir);
}

TEST(OosTop10ValidationTest, ContinuesAfterBacktestFailureAndRecordsError) {
    const auto dir = MakeTempDir("oos_top10_validation_failure");
    const auto runtime_root = dir / "runtime" / "rolling_optimize_kama";
    const auto top_trials_dir = runtime_root / "top_trials" / "window_0000";
    WriteArchivedTrial(top_trials_dir, "01_trial_alpha", "trial_alpha");
    WriteArchivedTrial(top_trials_dir, "02_trial_beta", "trial_beta");
    const auto train_report = WriteTrainReport(runtime_root);

    OosBacktestRunFn run_fn = [&](const BacktestCliSpec& spec, BacktestCliResult* out,
                                  std::string* error) {
        if (spec.run_id.find("trial_alpha") != std::string::npos) {
            *error = "synthetic oos failure";
            return false;
        }
        *out = MakeBacktestResult(spec, spec.run_id, 40.0, 80.0, 2.0,
                                  MakeDailySeries(0.05, 0.05, 0.08, 0.03, 0.08, 0.08));
        return true;
    };

    OosTop10ValidationRequest request;
    request.train_report_json = train_report.string();
    request.oos_start_date = "20240701";
    request.oos_end_date = "20241231";
    request.top_n = 2;

    OosTop10ValidationReport report;
    std::string error;
    ASSERT_TRUE(RunOosTop10Validation(request, &report, &error, run_fn)) << error;

    ASSERT_EQ(report.rows.size(), 2U);
    EXPECT_EQ(report.success_count, 1);
    EXPECT_EQ(report.failed_count, 1);
    EXPECT_EQ(report.rows[0].trial_id, "trial_beta");
    EXPECT_EQ(report.rows[1].trial_id, "trial_alpha");
    EXPECT_EQ(report.recommended_trial_id, "trial_beta");
    EXPECT_EQ(report.rows[1].status, "failed");
    EXPECT_EQ(report.rows[1].error_msg, "synthetic oos failure");
    EXPECT_FALSE(report.final_recommended_params_yaml.empty());
    EXPECT_TRUE(std::filesystem::exists(report.final_recommended_params_yaml));

    const std::string csv_text = ReadFile(report.output_csv);
    EXPECT_NE(csv_text.find("synthetic oos failure"), std::string::npos);

    std::filesystem::remove_all(dir);
}

}  // namespace
}  // namespace quant_hft::rolling