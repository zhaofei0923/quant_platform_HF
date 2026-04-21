#include <algorithm>
#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdlib>
#include <ctime>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <limits>
#include <map>
#include <memory>
#include <sstream>
#include <string>
#include <system_error>
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
using quant_hft::apps::DetectDefaultBacktestCliPath;
using quant_hft::apps::DefaultParameterOptimConfigPath;
using quant_hft::apps::GetArg;
using quant_hft::apps::HasArg;
using quant_hft::apps::ParseArgs;
using quant_hft::apps::ResolveConfigPathWithDefault;
using quant_hft::optim::IOptimizationAlgorithm;
using quant_hft::optim::LoadParameterSpace;
using quant_hft::optim::OptimizationConfig;
using quant_hft::optim::OptimizationReport;
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

std::string DetectBacktestCliPath(const ArgMap& args, const ParameterSpace& space,
                                  const std::string& argv0) {
    const std::string cli_arg = GetArg(args, "backtest-cli-path", "");
    if (!cli_arg.empty()) {
        return cli_arg;
    }

    if (!space.backtest_cli_path.empty()) {
        return space.backtest_cli_path;
    }

    return DetectDefaultBacktestCliPath(argv0);
}

int SafeMaxConcurrent(int requested) {
    const int normalized_requested = requested <= 0 ? 1 : requested;

    const unsigned int hw = std::thread::hardware_concurrency();
    const unsigned int half_hw = hw == 0 ? 1U : std::max(1U, hw / 2U);
    const int hw_cap = static_cast<int>(std::min(4U, half_hw));

    int memory_cap = std::numeric_limits<int>::max();
    std::ifstream meminfo("/proc/meminfo");
    if (meminfo.is_open()) {
        std::string key;
        long long value_kb = 0;
        std::string unit;
        while (meminfo >> key >> value_kb >> unit) {
            if (key == "MemAvailable:") {
                constexpr long long kReserveKb = 1024LL * 1024LL;
                constexpr long long kPerTrialKb = 1536LL * 1024LL;
                if (value_kb <= kReserveKb) {
                    memory_cap = 1;
                } else {
                    const long long usable_kb = value_kb - kReserveKb;
                    const long long cap = usable_kb / kPerTrialKb;
                    memory_cap = static_cast<int>(std::max(1LL, cap));
                }
                break;
            }
            meminfo.ignore(std::numeric_limits<std::streamsize>::max(), '\n');
        }
    }

    return std::max(1, std::min({normalized_requested, hw_cap, memory_cap}));
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

std::filesystem::path ArchiveRootForTask(const OptimizationConfig& config) {
    std::filesystem::path base_dir = std::filesystem::path(config.output_json).parent_path();
    if (base_dir.empty()) {
        base_dir = std::filesystem::path("runtime/optim");
    }
    return base_dir / "top_trials";
}

std::string RankedTrialDirectoryName(std::size_t rank, const std::string& trial_id) {
    std::ostringstream oss;
    oss << std::setw(2) << std::setfill('0') << (rank + 1) << '_' << trial_id;
    return oss.str();
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

bool PreserveTopKTrials(const OptimizationConfig& config,
                        OptimizationReport* report,
                        std::string* error) {
    if (report == nullptr) {
        if (error != nullptr) {
            *error = "optimization report is null";
        }
        return false;
    }
    if (config.preserve_top_k_trials <= 0) {
        return true;
    }

    std::vector<std::size_t> completed_indices;
    completed_indices.reserve(report->trials.size());
    for (std::size_t index = 0; index < report->trials.size(); ++index) {
        if (report->trials[index].status == "completed") {
            completed_indices.push_back(index);
        }
    }
    if (completed_indices.empty()) {
        return true;
    }

    std::stable_sort(completed_indices.begin(), completed_indices.end(), [&](std::size_t left,
                                                                             std::size_t right) {
        return report->maximize ? (report->trials[left].objective > report->trials[right].objective)
                                : (report->trials[left].objective < report->trials[right].objective);
    });

    const std::filesystem::path archive_root = ArchiveRootForTask(config);
    std::error_code ec;
    std::filesystem::remove_all(archive_root, ec);
    if (ec) {
        if (error != nullptr) {
            *error = "failed to reset archive root: " + archive_root.string() + ", error=" +
                     ec.message();
        }
        return false;
    }

    std::filesystem::create_directories(archive_root, ec);
    if (ec) {
        if (error != nullptr) {
            *error = "failed to create archive root: " + archive_root.string() + ", error=" +
                     ec.message();
        }
        return false;
    }

    const std::size_t top_n =
        std::min<std::size_t>(static_cast<std::size_t>(config.preserve_top_k_trials),
                              completed_indices.size());
    for (std::size_t rank = 0; rank < top_n; ++rank) {
        Trial& trial = report->trials[completed_indices[rank]];
        if (trial.working_dir.empty()) {
            if (error != nullptr) {
                *error = "completed trial missing working_dir: " + trial.trial_id;
            }
            return false;
        }

        const std::filesystem::path source_dir = trial.working_dir;
        const std::filesystem::path destination_dir =
            archive_root / RankedTrialDirectoryName(rank, trial.trial_id);
        // Archive before temp cleanup and use copy instead of move so top_trials keeps a
        // byte-for-byte snapshot of the original trial outputs at copy time. If a future
        // acceptance flow needs stronger post-run evidence, record per-file checksums here
        // and write them into report.json before artifact_manager.Cleanup() runs.
        if (!CopyDirectoryRecursive(source_dir, destination_dir, error)) {
            return false;
        }

        trial.archived_artifact_dir = destination_dir.string();
        if (report->best_trial.trial_id == trial.trial_id) {
            report->best_trial.archived_artifact_dir = trial.archived_artifact_dir;
        }
    }

    return true;
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
    std::cout << "Usage: " << argv0
              << " [--config <optim_config.yaml>] [--backtest-cli-path <path>]\n"
              << "Default config: " << DefaultParameterOptimConfigPath() << '\n';
}

}  // namespace

int main(int argc, char** argv) {
    const ArgMap args = ParseArgs(argc, argv);
    if (HasArg(args, "help") || HasArg(args, "h")) {
        PrintUsage(argv[0]);
        return 0;
    }

    const quant_hft::apps::ResolvedConfigPath resolved_config =
        ResolveConfigPathWithDefault(args, "config", DefaultParameterOptimConfigPath());
    if (resolved_config.path.empty()) {
        PrintUsage(argv[0]);
        return 2;
    }
    if (resolved_config.used_default) {
        std::cout << "parameter_optim_cli: using default config: " << resolved_config.path
                  << '\n';
    }
    const std::string config_path = resolved_config.path;

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

    const std::string backtest_cli_path = DetectBacktestCliPath(args, space, argv[0]);
    const int requested_concurrent = std::max(1, space.optimization.batch_size);
    const int max_concurrent = SafeMaxConcurrent(requested_concurrent);
    if (max_concurrent < requested_concurrent) {
        std::cout << "parameter_optim_cli: limiting concurrency from " << requested_concurrent
                  << " to " << max_concurrent
                  << " to reduce CPU and memory pressure\n";
    }
    TaskScheduler scheduler(max_concurrent);

    std::signal(SIGINT, HandleSignal);
    std::signal(SIGTERM, HandleSignal);

    std::atomic<int> trial_counter{0};
    TempArtifactManager artifact_manager;
    const auto task_started_system = std::chrono::system_clock::now();
    const auto task_started_steady = std::chrono::steady_clock::now();
    const std::string task_id = MakeTaskId(task_started_system);

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
        trial.result_json_path = result_json.string();
        trial.stdout_log_path = stdout_log.string();
        trial.stderr_log_path = stderr_log.string();

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

        std::string metric_error;
        const double objective =
            ResultAnalyzer::ComputeObjectiveFromJson(trial.result_json_path, space.optimization,
                                                     &metric_error);
        if (!metric_error.empty()) {
            trial.status = "failed";
            trial.error_msg = metric_error;
            return trial;
        }

        trial.status = "completed";
        trial.objective = objective;

        std::string metrics_error;
        if (!ResultAnalyzer::ExtractTrialMetricsFromJson(trial.result_json_path, &trial.metrics,
                                                         &metrics_error)) {
            trial.metrics_error = metrics_error;
        } else {
            trial.metrics_error = metrics_error;
        }

        std::vector<std::string> constraint_violations;
        std::string constraint_error;
        if (!ResultAnalyzer::EvaluateConstraintsFromJson(trial.result_json_path, space.optimization,
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
    auto report = ResultAnalyzer::Analyze(trials, config, g_interrupted.load());
    report.task_id = task_id;
    report.started_at = FormatUtcTimestamp(task_started_system);
    const auto task_finished_system = std::chrono::system_clock::now();
    report.finished_at = FormatUtcTimestamp(task_finished_system);
    report.wall_clock_sec =
        std::chrono::duration<double>(std::chrono::steady_clock::now() - task_started_steady)
            .count();

    if (!PreserveTopKTrials(config, &report, &error)) {
        std::cerr << "parameter_optim_cli: failed to archive top trials: " << error << '\n';
        return 1;
    }

    if (!ResultAnalyzer::WriteReport(report, config.output_json, config.output_md, &error)) {
        std::cerr << "parameter_optim_cli: failed to write report: " << error << '\n';
        return 1;
    }

    if (config.export_heatmap) {
        std::filesystem::path heatmap_dir = std::filesystem::path(config.output_json).parent_path();
        if (heatmap_dir.empty()) {
            heatmap_dir = std::filesystem::current_path();
        }
        if (!ResultAnalyzer::WriteHeatmaps(report, space, heatmap_dir.string(), &error)) {
            std::cerr << "parameter_optim_cli: failed to write heatmap data: " << error << '\n';
            return 1;
        }
    }

    if (report.best_trial.status == "completed") {
        if (!ResultAnalyzer::WriteBestParamsYaml(report.best_trial.params, config.best_params_yaml,
                                                 &error)) {
            std::cerr << "parameter_optim_cli: failed to write best params yaml: " << error << '\n';
            return 1;
        }
    }

    // Successful trial working directories are cleaned only after archiving/report writing.
    // This keeps runtime disk usage bounded while preserving copied artifacts under top_trials.
    artifact_manager.Cleanup();

    std::cout << "optimization finished total=" << report.total_trials
              << " completed=" << report.completed_trials << " failed=" << report.failed_trials
              << " constraint_violated=" << report.constraint_stats.total_violations
              << " interrupted=" << (report.interrupted ? "true" : "false") << '\n';

    if (report.interrupted) {
        return 130;
    }
    if (report.completed_trials == 0) {
        return 1;
    }
    return 0;
}
