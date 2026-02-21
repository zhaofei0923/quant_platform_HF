#include "quant_hft/rolling/rolling_runner.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <csignal>
#include <cmath>
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
#include "quant_hft/optim/parameter_space.h"
#include "quant_hft/optim/result_analyzer.h"
#include "quant_hft/optim/task_scheduler.h"
#include "quant_hft/optim/temp_config_generator.h"
#include "quant_hft/rolling/metric_extractor.h"

namespace quant_hft::rolling {
namespace {

using quant_hft::apps::BacktestCliResult;
using quant_hft::apps::BacktestCliSpec;
using quant_hft::apps::RunBacktestSpec;
using quant_hft::apps::SummarizeBacktest;
using quant_hft::apps::UnixEpochMillisNow;
using quant_hft::optim::GridSearch;
using quant_hft::optim::LoadParameterSpace;
using quant_hft::optim::OptimizationConfig;
using quant_hft::optim::ParameterSpace;
using quant_hft::optim::ParamValueMap;
using quant_hft::optim::ResultAnalyzer;
using quant_hft::optim::TaskScheduler;
using quant_hft::optim::Trial;
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
    spec.strategy_composite_config = config.backtest_base.strategy_composite_config;
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

std::unordered_map<std::string, double> BuildCoreMetrics(const BacktestCliResult& result) {
    std::unordered_map<std::string, double> metrics;
    const auto summary = SummarizeBacktest(result);
    metrics["summary.total_pnl"] = summary.total_pnl;
    metrics["summary.max_drawdown"] = summary.max_drawdown;
    metrics["hf_standard.advanced_summary.profit_factor"] = result.advanced_summary.profit_factor;
    metrics["hf_standard.risk_metrics.var_95"] = result.risk_metrics.var_95;
    metrics["final_equity"] = result.final_equity;
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
    out->optimization.batch_size = config.optimization.parallel;
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

    ParameterSpace space = base_space;
    OptimizationConfig opt_config = space.optimization;

    GridSearch algorithm;
    try {
        algorithm.Initialize(space, opt_config);
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

        trial.status = "completed";
        trial.objective = objective;
        return trial;
    };

    while (!algorithm.IsFinished()) {
        if (g_interrupted.load()) {
            break;
        }
        std::vector<ParamValueMap> batch = algorithm.GetNextBatch(scheduler.max_concurrent());
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
            algorithm.AddTrialResult(trial);
        }
    }

    const Trial best = algorithm.GetBestTrial();
    if (best.status != "completed") {
        out.success = false;
        out.error_msg = best.error_msg.empty() ? "no successful trial in optimization" : best.error_msg;
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
