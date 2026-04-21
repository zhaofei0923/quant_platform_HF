#include "quant_hft/rolling/rolling_runner.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <csignal>
#include <cmath>
#include <ctime>
#include <filesystem>
#include <future>
#include <iomanip>
#include <iostream>
#include <limits>
#include <memory>
#include <numeric>
#include <sstream>
#include <string>
#include <thread>
#include <variant>
#include <vector>

#include "quant_hft/apps/cli_support.h"
#include "quant_hft/optim/grid_search.h"
#include "quant_hft/optim/random_search.h"
#include "quant_hft/optim/parameter_space.h"
#include "quant_hft/optim/result_analyzer.h"
#include "quant_hft/optim/task_scheduler.h"
#include "quant_hft/optim/temp_config_generator.h"
#include "quant_hft/rolling/metric_extractor.h"

namespace quant_hft::rolling {
namespace {

using quant_hft::apps::BacktestCliResult;
using quant_hft::apps::BacktestCliSpec;
using quant_hft::apps::RenderBacktestJson;
using quant_hft::apps::RunBacktestSpec;
using quant_hft::apps::SummarizeBacktest;
using quant_hft::apps::UnixEpochMillisNow;
using quant_hft::apps::WriteTextFile;
using quant_hft::optim::GridSearch;
using quant_hft::optim::IOptimizationAlgorithm;
using quant_hft::optim::LoadParameterSpace;
using quant_hft::optim::OptimizationConfig;
using quant_hft::optim::ParameterSpace;
using quant_hft::optim::ParamValueMap;
using quant_hft::optim::RandomSearch;
using quant_hft::optim::ResultAnalyzer;
using quant_hft::optim::TaskScheduler;
using quant_hft::optim::Trial;
using quant_hft::optim::TrialMetricsSnapshot;
using quant_hft::optim::TrialConfigArtifacts;
using quant_hft::optim::TrialConfigRequest;
using quant_hft::optim::GenerateTrialConfig;

std::atomic<bool> g_interrupted{false};

void HandleSignal(int /*signum*/) { g_interrupted.store(true); }

class TempArtifactManager {
   public:
    ~TempArtifactManager() { Cleanup(); }

    void MarkForCleanup(const std::filesystem::path& path) {
        if (!path.empty()) {
            cleanup_paths_.push_back(path);
        }
    }

    void MarkKeep(const std::filesystem::path& path) {
        if (!path.empty()) {
            keep_paths_.push_back(path);
        }
    }

    void Cleanup() {
        std::sort(cleanup_paths_.begin(), cleanup_paths_.end());
        cleanup_paths_.erase(std::unique(cleanup_paths_.begin(), cleanup_paths_.end()),
                            cleanup_paths_.end());
        std::sort(keep_paths_.begin(), keep_paths_.end());
        keep_paths_.erase(std::unique(keep_paths_.begin(), keep_paths_.end()), keep_paths_.end());

        for (const auto& path : cleanup_paths_) {
            if (std::binary_search(keep_paths_.begin(), keep_paths_.end(), path)) {
                continue;
            }
            std::error_code ec;
            std::filesystem::remove_all(path, ec);
        }
        cleanup_paths_.clear();
        keep_paths_.clear();
    }

   private:
    std::vector<std::filesystem::path> cleanup_paths_;
    std::vector<std::filesystem::path> keep_paths_;
};

int SafeMaxConcurrent(int requested) {
    const unsigned int hw = std::thread::hardware_concurrency();
    const int hw_cap = hw == 0 ? 1 : static_cast<int>(hw);
    return std::max(1, std::min(requested, hw_cap));
}

std::string FormatUtcTimestamp(std::chrono::system_clock::time_point time_point) {
    const std::time_t raw_time = std::chrono::system_clock::to_time_t(time_point);
    std::tm utc_tm{};
    gmtime_r(&raw_time, &utc_tm);

    std::ostringstream oss;
    oss << std::put_time(&utc_tm, "%Y-%m-%dT%H:%M:%SZ");
    return oss.str();
}

std::string MakeTaskId(std::chrono::system_clock::time_point time_point) {
    const auto millis =
        std::chrono::duration_cast<std::chrono::milliseconds>(time_point.time_since_epoch())
            .count();
    return "optim_" + std::to_string(millis);
}

std::string JoinMessages(const std::vector<std::string>& messages) {
    std::ostringstream oss;
    for (std::size_t index = 0; index < messages.size(); ++index) {
        if (index > 0) {
            oss << "; ";
        }
        oss << messages[index];
    }
    return oss.str();
}

std::filesystem::path ResolvePath(const std::filesystem::path& base_dir,
                                  const std::filesystem::path& raw) {
    if (raw.is_absolute()) {
        return std::filesystem::absolute(raw).lexically_normal();
    }
    return std::filesystem::absolute(base_dir / raw).lexically_normal();
}

BacktestRunFn DefaultRunFn() {
    return [](const BacktestCliSpec& spec, BacktestCliResult* out, std::string* error) {
        return RunBacktestSpec(spec, out, error);
    };
}

BacktestCliSpec BuildSpec(const RollingConfig& config,
                          const std::string& start_date,
                          const std::string& end_date,
                          const std::string& run_id,
                          const std::string& strategy_composite_config_override = "") {
    BacktestCliSpec spec;
    spec.engine_mode = "parquet";
    spec.dataset_root = config.backtest_base.dataset_root;
    spec.dataset_manifest = config.backtest_base.dataset_manifest;
    spec.start_date = start_date;
    spec.end_date = end_date;
    spec.symbols = config.backtest_base.symbols;
    spec.max_ticks = config.backtest_base.max_ticks;
    spec.deterministic_fills = config.backtest_base.deterministic_fills;
    spec.strict_parquet = config.backtest_base.strict_parquet;
    spec.rollover_mode = config.backtest_base.rollover_mode;
    spec.rollover_price_mode = config.backtest_base.rollover_price_mode;
    spec.rollover_slippage_bps = config.backtest_base.rollover_slippage_bps;
    spec.initial_equity = config.backtest_base.initial_equity;
    spec.emit_trades = config.backtest_base.emit_trades;
    spec.emit_orders = config.backtest_base.emit_orders;
    spec.emit_position_history = config.backtest_base.emit_position_history;
    spec.strategy_factory = config.backtest_base.strategy_factory;
    spec.strategy_main_config_path = config.backtest_base.strategy_composite_config;
    spec.strategy_composite_config = config.backtest_base.strategy_composite_config;
    spec.product_config_path = config.backtest_base.product_config_path;
    spec.contract_expiry_calendar_path = config.backtest_base.contract_expiry_calendar_path;
    spec.run_id = run_id;

    if (!strategy_composite_config_override.empty()) {
        spec.strategy_factory = "composite";
        spec.strategy_composite_config = strategy_composite_config_override;
    }

    return spec;
}

std::string BuildRunId(const std::string& mode,
                       int window_index,
                       const std::string& stage,
                       int seq) {
    std::ostringstream oss;
    oss << "rolling-" << mode << "-w" << window_index << "-" << stage << "-" << seq << "-"
        << UnixEpochMillisNow();
    return oss.str();
}

std::string WindowDirectoryName(int window_index) {
    std::ostringstream oss;
    oss << "window_" << std::setw(4) << std::setfill('0') << window_index;
    return oss.str();
}

std::string RankedTrialDirectoryName(std::size_t rank, const std::string& trial_id) {
    std::ostringstream oss;
    oss << std::setw(2) << std::setfill('0') << (rank + 1) << '_' << trial_id;
    return oss.str();
}

std::filesystem::path OutputRoot(const RollingConfig& config) {
    if (!config.output.root_dir.empty()) {
        return config.output.root_dir;
    }
    std::filesystem::path base_dir = std::filesystem::path(config.output.report_json).parent_path();
    if (base_dir.empty()) {
        base_dir = std::filesystem::path("runtime/rolling");
    }
    return base_dir;
}

std::filesystem::path WindowTopTrialsDir(const RollingConfig& config, int window_index) {
    return OutputRoot(config) / "top_trials" / WindowDirectoryName(window_index);
}

std::filesystem::path WindowTestResultPath(const RollingConfig& config, int window_index) {
    return OutputRoot(config) / "test_results" / WindowDirectoryName(window_index) / "result.json";
}

std::filesystem::path WindowTrainReportDir(const RollingConfig& config, int window_index) {
    return OutputRoot(config) / "train_reports" / WindowDirectoryName(window_index);
}

std::filesystem::path WindowTrainReportJsonPath(const RollingConfig& config, int window_index) {
    return WindowTrainReportDir(config, window_index) / "parameter_optim_report.json";
}

std::filesystem::path WindowTrainReportMdPath(const RollingConfig& config, int window_index) {
    return WindowTrainReportDir(config, window_index) / "parameter_optim_report.md";
}

bool CopyDirectoryRecursive(const std::filesystem::path& source,
                           const std::filesystem::path& destination,
                           std::string* error) {
    std::error_code ec;
    if (!std::filesystem::exists(source, ec) || !std::filesystem::is_directory(source, ec)) {
        if (error != nullptr) {
            *error = "archive source is not a directory: " + source.string();
        }
        return false;
    }

    std::filesystem::create_directories(destination.parent_path(), ec);
    if (ec) {
        if (error != nullptr) {
            *error = "failed to create archive parent directory: " + destination.parent_path().string() +
                     ", error=" + ec.message();
        }
        return false;
    }

    std::filesystem::remove_all(destination, ec);
    ec.clear();

    std::filesystem::copy(source, destination,
                          std::filesystem::copy_options::recursive |
                              std::filesystem::copy_options::overwrite_existing,
                          ec);
    if (ec) {
        if (error != nullptr) {
            *error = "failed to archive trial artifacts from " + source.string() + " to " +
                     destination.string() + ", error=" + ec.message();
        }
        return false;
    }
    return true;
}

bool PersistBacktestResultJson(const BacktestCliResult& result,
                               const std::filesystem::path& output_path,
                               std::string* error) {
    return WriteTextFile(output_path.string(), RenderBacktestJson(result), error);
}

void AppendDerivedMetrics(const BacktestCliResult& result,
                         std::unordered_map<std::string, double>* metrics) {
    if (metrics == nullptr) {
        return;
    }
    TrialMetricsSnapshot derived;
    std::string error;
    if (!ResultAnalyzer::ExtractTrialMetricsFromJsonText(RenderBacktestJson(result), &derived, &error)) {
        return;
    }
    if (derived.max_drawdown_pct.has_value()) {
        (*metrics)["hf_standard.risk_metrics.max_drawdown_pct"] = *derived.max_drawdown_pct;
    }
    if (derived.annualized_return_pct.has_value()) {
        (*metrics)["hf_standard.risk_metrics.annualized_return_pct"] =
            *derived.annualized_return_pct;
    }
    if (derived.sharpe_ratio.has_value()) {
        (*metrics)["hf_standard.risk_metrics.sharpe_ratio"] = *derived.sharpe_ratio;
    }
    if (derived.calmar_ratio.has_value()) {
        (*metrics)["hf_standard.risk_metrics.calmar_ratio"] = *derived.calmar_ratio;
    }
}

bool ArchiveTopKTrials(const RollingConfig& config,
                       const std::vector<Trial>& trials,
                       bool maximize,
                       int window_index,
                       int top_k,
                       std::string* archived_dir,
                       std::string* error) {
    if (top_k <= 0) {
        return true;
    }

    std::vector<const Trial*> completed;
    completed.reserve(trials.size());
    for (const Trial& trial : trials) {
        if (trial.status == "completed") {
            completed.push_back(&trial);
        }
    }
    if (completed.empty()) {
        return true;
    }

    std::stable_sort(completed.begin(), completed.end(), [&](const Trial* left, const Trial* right) {
        return maximize ? (left->objective > right->objective) : (left->objective < right->objective);
    });

    const std::filesystem::path archive_root = WindowTopTrialsDir(config, window_index);
    std::error_code ec;
    std::filesystem::remove_all(archive_root, ec);
    if (ec) {
        if (error != nullptr) {
            *error = "failed to reset top_trials directory: " + archive_root.string() + ", error=" +
                     ec.message();
        }
        return false;
    }
    std::filesystem::create_directories(archive_root, ec);
    if (ec) {
        if (error != nullptr) {
            *error = "failed to create top_trials directory: " + archive_root.string() + ", error=" +
                     ec.message();
        }
        return false;
    }

    const std::size_t limit =
        std::min<std::size_t>(static_cast<std::size_t>(top_k), completed.size());
    for (std::size_t rank = 0; rank < limit; ++rank) {
        const Trial& trial = *completed[rank];
        if (trial.working_dir.empty()) {
            if (error != nullptr) {
                *error = "completed trial missing working_dir: " + trial.trial_id;
            }
            return false;
        }
        const std::filesystem::path destination =
            archive_root / RankedTrialDirectoryName(rank, trial.trial_id);
        if (!CopyDirectoryRecursive(trial.working_dir, destination, error)) {
            return false;
        }
    }

    if (archived_dir != nullptr) {
        *archived_dir = archive_root.string();
    }
    return true;
}

std::unordered_map<std::string, double> BuildCoreMetrics(const BacktestCliResult& result) {
    std::unordered_map<std::string, double> metrics;
    const auto summary = SummarizeBacktest(result);
    metrics["summary.total_pnl"] = summary.total_pnl;
    metrics["summary.max_drawdown"] = summary.max_drawdown;
    metrics["hf_standard.advanced_summary.profit_factor"] = result.advanced_summary.profit_factor;
    metrics["hf_standard.risk_metrics.var_95"] = result.risk_metrics.var_95;
    metrics["final_equity"] = result.final_equity;
    AppendDerivedMetrics(result, &metrics);
    return metrics;
}

void FinalizeReportStats(RollingReport* report) {
    report->success_count = 0;
    report->failed_count = 0;
    report->objectives.clear();

    for (const auto& window : report->windows) {
        if (window.success) {
            ++report->success_count;
            report->objectives.push_back(window.objective);
        } else {
            ++report->failed_count;
        }
    }

    if (report->objectives.empty()) {
        report->mean_objective = 0.0;
        report->std_objective = 0.0;
        report->max_objective = 0.0;
        report->min_objective = 0.0;
        return;
    }

    const double sum = std::accumulate(report->objectives.begin(), report->objectives.end(), 0.0);
    report->mean_objective = sum / static_cast<double>(report->objectives.size());
    report->max_objective =
        *std::max_element(report->objectives.begin(), report->objectives.end());
    report->min_objective =
        *std::min_element(report->objectives.begin(), report->objectives.end());

    double variance = 0.0;
    for (double objective : report->objectives) {
        const double diff = objective - report->mean_objective;
        variance += diff * diff;
    }
    variance /= static_cast<double>(report->objectives.size());
    report->std_objective = std::sqrt(variance);
}

bool LoadAndValidateParamSpace(const RollingConfig& config,
                               ParameterSpace* out,
                               std::string* error) {
    if (out == nullptr) {
        if (error != nullptr) {
            *error = "parameter space output is null";
        }
        return false;
    }

    if (!LoadParameterSpace(config.optimization.param_space, out, error)) {
        return false;
    }

    const std::filesystem::path param_space_path =
        std::filesystem::absolute(config.optimization.param_space).lexically_normal();
    const std::filesystem::path param_space_dir = param_space_path.parent_path();

    const std::filesystem::path space_composite =
        ResolvePath(param_space_dir, std::filesystem::path(out->composite_config_path));
    if (!config.backtest_base.strategy_composite_config.empty()) {
        const auto expected =
            std::filesystem::absolute(config.backtest_base.strategy_composite_config).lexically_normal();
        if (space_composite != expected) {
            if (error != nullptr) {
                *error = "optimization.param_space composite_config_path does not match "
                         "backtest_base.strategy_composite_config";
            }
            return false;
        }
    }

    std::filesystem::path selected_target =
        ResolvePath(space_composite.parent_path(), std::filesystem::path(out->target_sub_config_path));

    if (!config.optimization.target_sub_config_path.empty()) {
        const std::filesystem::path rolling_target =
            std::filesystem::absolute(config.optimization.target_sub_config_path).lexically_normal();
        if (rolling_target != selected_target) {
            if (error != nullptr) {
                *error = "optimization.target_sub_config_path does not match param_space target_sub_config_path";
            }
            return false;
        }
        selected_target = rolling_target;
    }

    out->composite_config_path = space_composite.string();
    out->target_sub_config_path = selected_target.string();
    out->optimization.algorithm = config.optimization.algorithm;
    out->optimization.metric_path = config.optimization.metric;
    out->optimization.maximize = config.optimization.maximize;
    out->optimization.max_trials = config.optimization.max_trials;
    if (config.optimization.random_seed.has_value()) {
        out->optimization.random_seed = config.optimization.random_seed;
    }
    out->optimization.batch_size = config.optimization.parallel;
    if (config.optimization.preserve_top_k_trials.has_value()) {
        out->optimization.preserve_top_k_trials = *config.optimization.preserve_top_k_trials;
    }
    return true;
}

WindowResult RunFixedWindow(const RollingConfig& config,
                            const Window& window,
                            const BacktestRunFn& run_fn,
                            int seq,
                            const std::string& metric_path) {
    WindowResult out;
    out.index = window.index;
    out.train_start = window.train_start;
    out.train_end = window.train_end;
    out.test_start = window.test_start;
    out.test_end = window.test_end;

    BacktestCliResult result;
    std::string run_error;
    const BacktestCliSpec spec =
        BuildSpec(config, window.test_start, window.test_end,
                  BuildRunId(config.mode, window.index, "fixed", seq));

    if (!run_fn(spec, &result, &run_error)) {
        out.success = false;
        out.error_msg = run_error;
        return out;
    }

    double objective = 0.0;
    if (!ExtractMetricFromResult(result, metric_path, &objective, &run_error)) {
        out.success = false;
        out.error_msg = run_error;
        return out;
    }

    out.success = true;
    out.objective = objective;
    out.metrics = BuildCoreMetrics(result);
    return out;
}

WindowResult RunOptimizedWindow(const RollingConfig& config,
                                const Window& window,
                                const ParameterSpace& base_space,
                                const BacktestRunFn& run_fn,
                                int base_seq,
                                std::string* error) {
    WindowResult out;
    out.index = window.index;
    out.train_start = window.train_start;
    out.train_end = window.train_end;
    out.test_start = window.test_start;
    out.test_end = window.test_end;

    const auto task_started_system = std::chrono::system_clock::now();
    const auto task_started_steady = std::chrono::steady_clock::now();
    out.train_task_id = MakeTaskId(task_started_system);
    out.train_report_json = WindowTrainReportJsonPath(config, window.index).string();
    out.train_report_md = WindowTrainReportMdPath(config, window.index).string();

    ParameterSpace space = base_space;
    OptimizationConfig opt_config = space.optimization;

    std::unique_ptr<IOptimizationAlgorithm> algorithm;
    if (opt_config.algorithm == "grid") {
        algorithm = std::make_unique<GridSearch>();
    } else if (opt_config.algorithm == "random") {
        algorithm = std::make_unique<RandomSearch>();
    } else {
        out.success = false;
        out.error_msg = "unsupported optimization.algorithm: " + opt_config.algorithm;
        return out;
    }
    try {
        algorithm->Initialize(space, opt_config);
    } catch (const std::exception& ex) {
        out.success = false;
        out.error_msg = std::string("optimization initialize failed: ") + ex.what();
        return out;
    }

    TaskScheduler scheduler(SafeMaxConcurrent(opt_config.batch_size));
    TempArtifactManager artifact_manager;
    std::atomic<int> trial_counter{0};

    auto trial_task = [&](const ParamValueMap& params) -> Trial {
        Trial trial;
        const int trial_index = trial_counter.fetch_add(1);
        trial.trial_id = "window_" + std::to_string(window.index) + "_trial_" +
                         std::to_string(trial_index + 1);
        trial.params = params;

        TrialConfigRequest request;
        request.composite_config_path = space.composite_config_path;
        request.target_sub_config_path = space.target_sub_config_path;
        request.param_overrides = params.values;
        request.trial_id = trial.trial_id;

        TrialConfigArtifacts artifacts;
        std::string local_error;
        if (!GenerateTrialConfig(request, &artifacts, &local_error)) {
            trial.status = "failed";
            trial.error_msg = local_error;
            return trial;
        }
        trial.working_dir = artifacts.working_dir.string();

        BacktestCliResult train_result;
        BacktestCliSpec train_spec =
            BuildSpec(config, window.train_start, window.train_end,
                      BuildRunId(config.mode, window.index, "train", base_seq + trial_index),
                      artifacts.composite_config_path.string());

        if (!run_fn(train_spec, &train_result, &local_error)) {
            trial.status = "failed";
            trial.error_msg = local_error;
            return trial;
        }

        double objective = 0.0;
        if (!ExtractMetricFromResult(train_result, opt_config.metric_path, &objective, &local_error)) {
            trial.status = "failed";
            trial.error_msg = local_error;
            return trial;
        }

        const std::filesystem::path result_json_path = artifacts.working_dir / "result.json";
        if (!PersistBacktestResultJson(train_result, result_json_path, &local_error)) {
            trial.status = "failed";
            trial.error_msg = "failed to persist train result json: " + local_error;
            return trial;
        }

        trial.status = "completed";
        trial.objective = objective;
        trial.result_json_path = result_json_path.string();

        std::string metrics_error;
        if (!ResultAnalyzer::ExtractTrialMetricsFromJson(trial.result_json_path, &trial.metrics,
                                                         &metrics_error)) {
            trial.metrics_error = metrics_error;
        } else {
            trial.metrics_error = metrics_error;
        }

        std::vector<std::string> constraint_violations;
        std::string constraint_error;
        if (!ResultAnalyzer::EvaluateConstraintsFromJson(trial.result_json_path, opt_config,
                                                         &constraint_violations,
                                                         &constraint_error)) {
            trial.status = "failed";
            trial.error_msg = constraint_error;
            return trial;
        }
        if (!constraint_violations.empty()) {
            trial.status = "constraint_violated";
            trial.error_msg = "constraints violated: " + JoinMessages(constraint_violations);
        }
        return trial;
    };

    while (!algorithm->IsFinished()) {
        if (g_interrupted.load()) {
            break;
        }
        std::vector<ParamValueMap> batch = algorithm->GetNextBatch(scheduler.max_concurrent());
        if (batch.empty()) {
            break;
        }

        std::vector<Trial> results = scheduler.RunBatch(batch, trial_task);
        for (Trial& trial : results) {
            if (!trial.working_dir.empty()) {
                if (config.output.keep_temp_files || trial.status != "completed") {
                    artifact_manager.MarkKeep(trial.working_dir);
                } else {
                    artifact_manager.MarkForCleanup(trial.working_dir);
                }
            }
            algorithm->AddTrialResult(trial);
        }
    }

    const std::vector<Trial> trials = algorithm->GetAllTrials();
    out.train_trial_count = static_cast<int>(trials.size());
    out.completed_train_trial_count = static_cast<int>(std::count_if(
        trials.begin(), trials.end(), [](const Trial& trial) { return trial.status == "completed"; }));

    const auto task_finished_system = std::chrono::system_clock::now();
    auto train_report = ResultAnalyzer::Analyze(trials, opt_config, g_interrupted.load());
    train_report.task_id = out.train_task_id;
    train_report.started_at = FormatUtcTimestamp(task_started_system);
    train_report.finished_at = FormatUtcTimestamp(task_finished_system);
    train_report.wall_clock_sec =
        std::chrono::duration<double>(std::chrono::steady_clock::now() - task_started_steady)
            .count();

    std::error_code report_ec;
    const std::filesystem::path train_report_json_path(out.train_report_json);
    std::filesystem::create_directories(train_report_json_path.parent_path(), report_ec);
    if (report_ec) {
        out.success = false;
        out.error_msg = "failed to create train report directory: " + report_ec.message();
        artifact_manager.Cleanup();
        return out;
    }

    std::string report_error;
    if (!ResultAnalyzer::WriteReport(train_report, out.train_report_json, out.train_report_md,
                                     &report_error)) {
        out.success = false;
        out.error_msg = "failed to write train optimization report: " + report_error;
        artifact_manager.Cleanup();
        return out;
    }

    if (opt_config.export_heatmap) {
        std::filesystem::path heatmap_dir = std::filesystem::path(out.train_report_json).parent_path();
        if (heatmap_dir.empty()) {
            heatmap_dir = std::filesystem::current_path();
        }
        if (!ResultAnalyzer::WriteHeatmaps(train_report, space, heatmap_dir.string(), &report_error)) {
            out.success = false;
            out.error_msg = "failed to write train heatmap data: " + report_error;
            artifact_manager.Cleanup();
            return out;
        }
    }

    if (!ArchiveTopKTrials(config, trials, opt_config.maximize, window.index,
                           opt_config.preserve_top_k_trials, &out.top_trials_dir, error)) {
        out.success = false;
        out.error_msg = error != nullptr ? *error : "failed to archive top trials";
        artifact_manager.Cleanup();
        return out;
    }

    const Trial best = algorithm->GetBestTrial();
    if (best.status != "completed") {
        out.success = false;
        out.error_msg = best.error_msg.empty() ? "no successful trial in optimization" : best.error_msg;
        if (out.error_msg == "no completed trial") {
            const auto failed_it = std::find_if(trials.begin(), trials.end(), [](const Trial& trial) {
                return trial.status == "failed" && !trial.error_msg.empty();
            });
            if (failed_it != trials.end()) {
                out.error_msg = "no completed trial; first failed trial " + failed_it->trial_id +
                                ": " + failed_it->error_msg;
            } else {
                const auto violated_it =
                    std::find_if(trials.begin(), trials.end(), [](const Trial& trial) {
                        return trial.status == "constraint_violated" && !trial.error_msg.empty();
                    });
                if (violated_it != trials.end()) {
                    out.error_msg = "no completed trial; first constraint_violated trial " +
                                    violated_it->trial_id + ": " + violated_it->error_msg;
                }
            }
        }
        artifact_manager.Cleanup();
        return out;
    }

    if (!config.output.best_params_dir.empty()) {
        std::error_code ec;
        std::filesystem::create_directories(config.output.best_params_dir, ec);
        if (ec) {
            out.success = false;
            out.error_msg = "failed to create best_params_dir: " + ec.message();
            artifact_manager.Cleanup();
            return out;
        }

        std::ostringstream filename;
        filename << "window_" << std::setw(4) << std::setfill('0') << window.index << "_best.yaml";
        const std::filesystem::path best_yaml =
            std::filesystem::path(config.output.best_params_dir) / filename.str();

        std::string local_error;
        if (!ResultAnalyzer::WriteBestParamsYaml(best.params, best_yaml.string(), &local_error)) {
            out.success = false;
            out.error_msg = local_error;
            artifact_manager.Cleanup();
            return out;
        }
        out.best_params_yaml = best_yaml.string();
    }

    TrialConfigRequest best_request;
    best_request.composite_config_path = space.composite_config_path;
    best_request.target_sub_config_path = space.target_sub_config_path;
    best_request.param_overrides = best.params.values;
    best_request.trial_id = "window_" + std::to_string(window.index) + "_best_eval";

    TrialConfigArtifacts best_artifacts;
    std::string eval_error;
    if (!GenerateTrialConfig(best_request, &best_artifacts, &eval_error)) {
        out.success = false;
        out.error_msg = eval_error;
        artifact_manager.Cleanup();
        return out;
    }

    if (config.output.keep_temp_files) {
        artifact_manager.MarkKeep(best_artifacts.working_dir);
    } else {
        artifact_manager.MarkForCleanup(best_artifacts.working_dir);
    }

    BacktestCliResult test_result;
    BacktestCliSpec test_spec =
        BuildSpec(config, window.test_start, window.test_end,
                  BuildRunId(config.mode, window.index, "test", base_seq + 100000),
                  best_artifacts.composite_config_path.string());

    if (!run_fn(test_spec, &test_result, &eval_error)) {
        out.success = false;
        out.error_msg = eval_error;
        artifact_manager.MarkKeep(best_artifacts.working_dir);
        artifact_manager.Cleanup();
        return out;
    }

    double objective = 0.0;
    if (!ExtractMetricFromResult(test_result, opt_config.metric_path, &objective, &eval_error)) {
        out.success = false;
        out.error_msg = eval_error;
        artifact_manager.MarkKeep(best_artifacts.working_dir);
        artifact_manager.Cleanup();
        return out;
    }

    out.success = true;
    out.objective = objective;
    out.metrics = BuildCoreMetrics(test_result);
    const std::filesystem::path test_result_json = WindowTestResultPath(config, window.index);
    if (!PersistBacktestResultJson(test_result, test_result_json, &eval_error)) {
        out.success = false;
        out.error_msg = "failed to persist test result json: " + eval_error;
        artifact_manager.MarkKeep(best_artifacts.working_dir);
        artifact_manager.Cleanup();
        return out;
    }
    out.test_result_json = test_result_json.string();
    artifact_manager.Cleanup();
    if (error != nullptr) {
        error->clear();
    }
    return out;
}

}  // namespace

bool RunRollingBacktest(const RollingConfig& config,
                        RollingReport* report,
                        std::string* error,
                        BacktestRunFn run_fn) {
    if (report == nullptr) {
        if (error != nullptr) {
            *error = "rolling report output is null";
        }
        return false;
    }

    if (!run_fn) {
        run_fn = DefaultRunFn();
    }

    g_interrupted.store(false);
    std::signal(SIGINT, HandleSignal);
    std::signal(SIGTERM, HandleSignal);

    std::vector<std::string> trading_days;
    if (!BuildTradingDayCalendar(config, &trading_days, error)) {
        return false;
    }

    std::vector<Window> windows;
    if (!GenerateWindows(config, trading_days, &windows, error)) {
        return false;
    }

    RollingReport local;
    local.mode = config.mode;
    local.windows.resize(windows.size());

    if (config.mode == "fixed_params") {
        const std::size_t max_parallel =
            static_cast<std::size_t>(SafeMaxConcurrent(config.output.window_parallel));
        std::vector<std::future<WindowResult>> active;
        std::vector<std::size_t> active_index;
        active.reserve(max_parallel);
        active_index.reserve(max_parallel);

        std::size_t next_index = 0;
        while (next_index < windows.size() || !active.empty()) {
            while (!g_interrupted.load() && next_index < windows.size() &&
                   active.size() < max_parallel) {
                const std::size_t launch_index = next_index++;
                active.emplace_back(std::async(std::launch::async,
                                               [&, launch_index]() {
                                                   return RunFixedWindow(
                                                       config, windows[launch_index], run_fn,
                                                       static_cast<int>(launch_index) + 1,
                                                       config.optimization.metric);
                                               }));
                active_index.push_back(launch_index);
            }

            if (active.empty()) {
                break;
            }

            WindowResult result = active.front().get();
            const std::size_t result_index = active_index.front();
            local.windows[result_index] = std::move(result);
            active.erase(active.begin());
            active_index.erase(active_index.begin());
        }

        if (g_interrupted.load()) {
            local.interrupted = true;
        }
    } else {
        int window_parallel = config.output.window_parallel;
        if (window_parallel > 1) {
            std::cerr << "rolling_backtest_cli: warning mode=rolling_optimize forces "
                      << "output.window_parallel from " << window_parallel << " to 1\n";
            window_parallel = 1;
        }
        (void)window_parallel;

        ParameterSpace space;
        if (!LoadAndValidateParamSpace(config, &space, error)) {
            return false;
        }

        for (std::size_t i = 0; i < windows.size(); ++i) {
            if (g_interrupted.load()) {
                local.interrupted = true;
                break;
            }
            local.windows[i] = RunOptimizedWindow(config, windows[i], space, run_fn,
                                                  static_cast<int>(i) * 1000, error);
        }
    }

    if (g_interrupted.load()) {
        local.interrupted = true;
    }

    FinalizeReportStats(&local);
    *report = std::move(local);
    return true;
}

}  // namespace quant_hft::rolling
