#include <chrono>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>

#include "quant_hft/apps/backtest_replay_support.h"

int main(int argc, char** argv) {
    using namespace quant_hft::apps;
    const auto args = ParseArgs(argc, argv);

    const std::string baseline_file = detail::GetArgAny(
        args, {"baseline", "baseline_json"}, "configs/perf/backtest_benchmark_baseline.json");
    const std::string result_json = detail::GetArgAny(
        args, {"result_json", "output_json"}, "docs/results/backtest_benchmark_result.json");

    double baseline_old_p95_ms = 0.0;
    double baseline_max_ticks = 1200.0;
    double baseline_runs = 5.0;
    double baseline_warmup_runs = 1.0;
    double baseline_min_ticks = 1.0;

    if (!baseline_file.empty() && std::filesystem::exists(baseline_file)) {
        std::ifstream baseline_in(baseline_file);
        if (baseline_in.is_open()) {
            std::ostringstream content;
            content << baseline_in.rdbuf();
            const std::string baseline_json = content.str();
            detail::ExtractJsonNumber(baseline_json, "old_p95_ms", &baseline_old_p95_ms);
            if (baseline_old_p95_ms <= 0.0) {
                detail::ExtractJsonNumber(baseline_json, "max_p95_ms", &baseline_old_p95_ms);
            }
            detail::ExtractJsonNumber(baseline_json, "max_ticks", &baseline_max_ticks);
            detail::ExtractJsonNumber(baseline_json, "runs", &baseline_runs);
            detail::ExtractJsonNumber(baseline_json, "warmup_runs", &baseline_warmup_runs);
            detail::ExtractJsonNumber(baseline_json, "min_ticks_read", &baseline_min_ticks);
        }
    }

    const auto read_int_arg = [&](const std::initializer_list<const char*>& keys,
                                  std::int64_t fallback) {
        const std::string raw = detail::GetArgAny(args, keys);
        std::int64_t parsed = fallback;
        if (!raw.empty()) {
            detail::ParseInt64(raw, &parsed);
        }
        return parsed;
    };
    const auto read_double_arg = [&](const std::initializer_list<const char*>& keys,
                                     double fallback) {
        const std::string raw = detail::GetArgAny(args, keys);
        double parsed = fallback;
        if (!raw.empty()) {
            detail::ParseDouble(raw, &parsed);
        }
        return parsed;
    };

    const std::int64_t runs =
        std::max<std::int64_t>(1, read_int_arg({"runs"}, static_cast<std::int64_t>(baseline_runs)));
    const std::int64_t warmup_runs =
        std::max<std::int64_t>(0, read_int_arg({"warmup_runs", "warmup-runs"},
                                               static_cast<std::int64_t>(baseline_warmup_runs)));
    const std::int64_t max_ticks = std::max<std::int64_t>(
        1, read_int_arg({"max_ticks", "max-ticks"}, static_cast<std::int64_t>(baseline_max_ticks)));
    const std::int64_t min_ticks_required =
        std::max<std::int64_t>(1, read_int_arg({"min_ticks_read", "min-ticks-read"},
                                               static_cast<std::int64_t>(baseline_min_ticks)));

    baseline_old_p95_ms = read_double_arg(
        {"old_p95_ms", "baseline_p95_ms", "baseline-p95-ms", "max_p95_ms"}, baseline_old_p95_ms);
    if (baseline_old_p95_ms <= 0.0) {
        baseline_old_p95_ms = 1000.0;
    }

    ArgMap spec_args = args;
    if (detail::GetArgAny(spec_args, {"csv_path", "csv-path", "csv"}).empty()) {
        spec_args["csv_path"] = "runtime/benchmarks/backtest/rb_ci_sample.csv";
    }
    if (detail::GetArgAny(spec_args, {"engine_mode", "engine-mode"}).empty()) {
        spec_args["engine_mode"] = "csv";
    }
    spec_args["deterministic_fills"] = "true";
    spec_args["max_ticks"] = std::to_string(max_ticks);

    BacktestCliSpec base_spec;
    std::string error;
    if (!ParseBacktestCliSpec(spec_args, &base_spec, &error)) {
        std::cerr << "backtest_benchmark_cli: " << error << '\n';
        return 2;
    }

    std::vector<double> elapsed_ms_values;
    std::vector<std::int64_t> ticks_read_values;
    elapsed_ms_values.reserve(static_cast<std::size_t>(runs));
    ticks_read_values.reserve(static_cast<std::size_t>(runs));

    double sample_total_pnl = 0.0;
    const std::int64_t total_runs = warmup_runs + runs;
    for (std::int64_t idx = 0; idx < total_runs; ++idx) {
        BacktestCliSpec run_spec = base_spec;
        run_spec.run_id = "bench-" + std::to_string(idx);

        const auto started = std::chrono::steady_clock::now();
        BacktestCliResult run_result;
        if (!RunBacktestSpec(run_spec, &run_result, &error)) {
            std::cerr << "backtest_benchmark_cli: " << error << '\n';
            return 1;
        }
        const auto ended = std::chrono::steady_clock::now();
        const double elapsed_ms =
            std::chrono::duration_cast<std::chrono::duration<double, std::milli>>(ended - started)
                .count();

        if (idx < warmup_runs) {
            continue;
        }

        elapsed_ms_values.push_back(elapsed_ms);
        ticks_read_values.push_back(run_result.replay.ticks_read);
        if (run_result.has_deterministic) {
            sample_total_pnl = run_result.deterministic.performance.total_pnl;
        }
    }

    if (elapsed_ms_values.empty() || ticks_read_values.empty()) {
        std::cerr << "backtest_benchmark_cli: no benchmark samples collected\n";
        return 1;
    }

    std::vector<double> sorted_elapsed = elapsed_ms_values;
    std::sort(sorted_elapsed.begin(), sorted_elapsed.end());
    const std::size_t p95_index = detail::P95Index(sorted_elapsed.size());
    const double new_p95_ms = sorted_elapsed[p95_index];
    const double mean_ms = detail::Mean(elapsed_ms_values);
    const std::int64_t min_ticks_read =
        *std::min_element(ticks_read_values.begin(), ticks_read_values.end());
    const std::int64_t max_ticks_read =
        *std::max_element(ticks_read_values.begin(), ticks_read_values.end());

    const double allowed_p95_ms = baseline_old_p95_ms * 1.10;
    const bool passed = new_p95_ms <= allowed_p95_ms && min_ticks_read >= min_ticks_required;

    std::ostringstream json;
    json << "{\n"
         << "  \"benchmark\": \"backtest_deterministic\",\n"
         << "  \"runs\": " << runs << ",\n"
         << "  \"warmup_runs\": " << warmup_runs << ",\n"
         << "  \"max_ticks\": " << max_ticks << ",\n"
         << "  \"mean_ms\": " << detail::FormatDouble(mean_ms) << ",\n"
         << "  \"new_p95_ms\": " << detail::FormatDouble(new_p95_ms) << ",\n"
         << "  \"old_p95_ms\": " << detail::FormatDouble(baseline_old_p95_ms) << ",\n"
         << "  \"allowed_p95_ms\": " << detail::FormatDouble(allowed_p95_ms) << ",\n"
         << "  \"gate\": \"new_p95_ms <= old_p95_ms * 1.10\",\n"
         << "  \"min_ticks_read\": " << min_ticks_read << ",\n"
         << "  \"max_ticks_read\": " << max_ticks_read << ",\n"
         << "  \"min_ticks_required\": " << min_ticks_required << ",\n"
         << "  \"sample_total_pnl\": " << detail::FormatDouble(sample_total_pnl) << ",\n"
         << "  \"passed\": " << (passed ? "true" : "false") << ",\n"
         << "  \"status\": \"" << (passed ? "ok" : "failed") << "\"\n"
         << "}\n";

    if (!WriteTextFile(result_json, json.str(), &error)) {
        std::cerr << "backtest_benchmark_cli: " << error << '\n';
        return 1;
    }
    std::cout << json.str();
    return passed ? 0 : 2;
}
