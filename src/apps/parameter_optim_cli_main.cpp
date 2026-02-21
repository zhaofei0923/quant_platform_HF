#include <algorithm>
#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <memory>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include "quant_hft/apps/cli_support.h"
#include "quant_hft/optim/grid_search.h"
#include "quant_hft/optim/parameter_space.h"
#include "quant_hft/optim/random_search.h"
#include "quant_hft/optim/result_analyzer.h"
#include "quant_hft/optim/task_scheduler.h"
#include "quant_hft/optim/temp_config_generator.h"

namespace {

using quant_hft::apps::ArgMap;
using quant_hft::apps::GetArg;
using quant_hft::apps::HasArg;
using quant_hft::apps::ParseArgs;
using quant_hft::optim::IOptimizationAlgorithm;
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

std::string ShellQuote(const std::string& value) {
    std::string out;
    out.reserve(value.size() + 2);
    out.push_back('\'');
    for (char ch : value) {
        if (ch == '\'') {
            out += "'\\''";
        } else {
            out.push_back(ch);
        }
    }
    out.push_back('\'');
    return out;
}

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

std::string DetectBacktestCliPath(const ArgMap& args, const ParameterSpace& space) {
    const std::string cli_arg = GetArg(args, "backtest-cli-path", "");
    if (!cli_arg.empty()) {
        return cli_arg;
    }

    if (!space.backtest_cli_path.empty()) {
        return space.backtest_cli_path;
    }

    const std::vector<std::filesystem::path> candidates = {
        std::filesystem::path("build") / "backtest_cli",
        std::filesystem::path("build-gcc") / "backtest_cli",
    };
    for (const auto& candidate : candidates) {
        std::error_code ec;
        if (std::filesystem::exists(candidate, ec)) {
            return candidate.string();
        }
    }
    return "backtest_cli";
}

int SafeMaxConcurrent(int requested) {
    const unsigned int hw = std::thread::hardware_concurrency();
    const int hw_cap = hw == 0 ? 1 : static_cast<int>(hw);
    return std::max(1, std::min(requested, hw_cap));
}

std::string BuildBacktestCommand(const std::string& backtest_cli_path,
                                 const std::map<std::string, std::string>& backtest_args,
                                 const std::string& trial_id,
                                 const TrialConfigArtifacts& artifacts,
                                 const std::filesystem::path& output_json,
                                 const std::filesystem::path& stdout_log,
                                 const std::filesystem::path& stderr_log) {
    std::ostringstream cmd;
    cmd << ShellQuote(backtest_cli_path);

    for (const auto& [key, value] : backtest_args) {
        if (key == "strategy_factory" || key == "strategy_composite_config" || key == "output_json" ||
            key == "output_md" || key == "run_id") {
            continue;
        }
        cmd << " --" << key << ' ' << ShellQuote(value);
    }

    cmd << " --strategy_factory composite";
    cmd << " --strategy_composite_config " << ShellQuote(artifacts.composite_config_path.string());
    cmd << " --run_id " << ShellQuote(trial_id);
    cmd << " --output_json " << ShellQuote(output_json.string());
    cmd << " > " << ShellQuote(stdout_log.string()) << " 2> " << ShellQuote(stderr_log.string());
    return cmd.str();
}

void PrintUsage(const char* argv0) {
    std::cout << "Usage: " << argv0 << " --config <optim_config.yaml> [--backtest-cli-path <path>]\n";
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

    ParameterSpace space;
    std::string error;
    if (!LoadParameterSpace(config_path, &space, &error)) {
        std::cerr << "parameter_optim_cli: " << error << '\n';
        return 2;
    }

    std::unique_ptr<IOptimizationAlgorithm> algorithm;
    if (space.optimization.algorithm == "grid") {
        algorithm = std::make_unique<quant_hft::optim::GridSearch>();
    } else if (space.optimization.algorithm == "random") {
        algorithm = std::make_unique<quant_hft::optim::RandomSearch>();
    } else {
        std::cerr << "parameter_optim_cli: unsupported algorithm: " << space.optimization.algorithm
                  << '\n';
        return 2;
    }

    try {
        algorithm->Initialize(space, space.optimization);
    } catch (const std::exception& ex) {
        std::cerr << "parameter_optim_cli: initialize failed: " << ex.what() << '\n';
        return 2;
    }

    const std::string backtest_cli_path = DetectBacktestCliPath(args, space);
    const int max_concurrent = SafeMaxConcurrent(space.optimization.batch_size);
    TaskScheduler scheduler(max_concurrent);

    std::signal(SIGINT, HandleSignal);
    std::signal(SIGTERM, HandleSignal);

    std::atomic<int> trial_counter{0};
    TempArtifactManager artifact_manager;

    auto task = [&](const ParamValueMap& params) -> Trial {
        Trial trial;
        const int index = trial_counter.fetch_add(1);
        trial.trial_id = "trial_" + std::to_string(index + 1);
        trial.params = params;

        TrialConfigRequest request;
        request.composite_config_path = space.composite_config_path;
        request.target_sub_config_path = space.target_sub_config_path;
        request.param_overrides = params.values;
        request.trial_id = trial.trial_id;

        TrialConfigArtifacts artifacts;
        std::string trial_error;
        if (!GenerateTrialConfig(request, &artifacts, &trial_error)) {
            trial.status = "failed";
            trial.error_msg = trial_error;
            return trial;
        }
        trial.working_dir = artifacts.working_dir.string();

        const std::filesystem::path result_json = artifacts.working_dir / "result.json";
        const std::filesystem::path stdout_log = artifacts.working_dir / "stdout.log";
        const std::filesystem::path stderr_log = artifacts.working_dir / "stderr.log";

        const std::string command =
            BuildBacktestCommand(backtest_cli_path, space.backtest_args, trial.trial_id, artifacts,
                                 result_json, stdout_log, stderr_log);

        const auto start = std::chrono::steady_clock::now();
        const int rc = std::system(command.c_str());
        const auto end = std::chrono::steady_clock::now();
        trial.elapsed_sec = std::chrono::duration<double>(end - start).count();

        if (rc != 0) {
            trial.status = "failed";
            trial.error_msg = "backtest_cli exit code=" + std::to_string(rc) +
                              ", stderr=" + stderr_log.string();
            return trial;
        }

        trial.result_json_path = result_json.string();
        std::string metric_error;
        const double objective =
            ResultAnalyzer::ExtractMetricFromJson(trial.result_json_path, space.optimization.metric_path,
                                                  &metric_error);
        if (!metric_error.empty()) {
            trial.status = "failed";
            trial.error_msg = metric_error;
            return trial;
        }

        trial.status = "completed";
        trial.objective = objective;
        return trial;
    };

    while (!algorithm->IsFinished()) {
        if (g_interrupted.load()) {
            std::cerr << "parameter_optim_cli: interrupt signal received, stopping new dispatch\n";
            break;
        }

        std::vector<ParamValueMap> batch = algorithm->GetNextBatch(scheduler.max_concurrent());
        if (batch.empty()) {
            break;
        }

        std::vector<Trial> batch_results = scheduler.RunBatch(batch, task);
        for (Trial& trial : batch_results) {
            if (trial.status == "completed") {
                artifact_manager.MarkForCleanup(trial.working_dir);
            } else {
                artifact_manager.MarkKeep(trial.working_dir);
            }
            algorithm->AddTrialResult(trial);
            std::cout << "trial=" << trial.trial_id << " status=" << trial.status;
            if (trial.status == "completed") {
                std::cout << " objective=" << trial.objective;
            } else {
                std::cout << " error=" << trial.error_msg;
            }
            std::cout << '\n';
        }
    }

    const std::vector<Trial> trials = algorithm->GetAllTrials();
    const OptimizationConfig& config = space.optimization;
    const auto report = ResultAnalyzer::Analyze(trials, config, g_interrupted.load());

    if (!ResultAnalyzer::WriteReport(report, config.output_json, config.output_md, &error)) {
        std::cerr << "parameter_optim_cli: failed to write report: " << error << '\n';
        return 1;
    }

    if (report.best_trial.status == "completed") {
        if (!ResultAnalyzer::WriteBestParamsYaml(report.best_trial.params, config.best_params_yaml,
                                                 &error)) {
            std::cerr << "parameter_optim_cli: failed to write best params yaml: " << error << '\n';
            return 1;
        }
    }

    artifact_manager.Cleanup();

    std::cout << "optimization finished total=" << report.total_trials
              << " completed=" << report.completed_trials << " failed=" << report.failed_trials
              << " interrupted=" << (report.interrupted ? "true" : "false") << '\n';

    if (report.interrupted) {
        return 130;
    }
    if (report.completed_trials == 0) {
        return 1;
    }
    return 0;
}
