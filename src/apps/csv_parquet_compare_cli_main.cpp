#include <algorithm>
#include <chrono>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>

#include "quant_hft/apps/backtest_replay_support.h"

namespace {

struct ModeSummary {
    std::string engine_mode;
    std::int64_t runs{0};
    std::int64_t warmup_runs{0};
    std::int64_t max_ticks{0};
    std::vector<double> elapsed_ms_values;
    double mean_ms{0.0};
    double p95_ms{0.0};
    double min_ms{0.0};
    double max_ms{0.0};
    std::int64_t ticks_read_min{0};
    std::int64_t ticks_read_max{0};
    double mean_ticks_per_sec{0.0};
    quant_hft::apps::BacktestCliResult sample_result;
};

bool RunModeSummary(const quant_hft::apps::BacktestCliSpec& base_spec, std::int64_t runs,
                    std::int64_t warmup_runs, ModeSummary* out_summary, std::string* error) {
    if (out_summary == nullptr) {
        if (error != nullptr) {
            *error = "mode summary output is null";
        }
        return false;
    }

    std::vector<double> elapsed_ms;
    std::vector<std::int64_t> ticks_read;
    elapsed_ms.reserve(static_cast<std::size_t>(runs));
    ticks_read.reserve(static_cast<std::size_t>(runs));

    quant_hft::apps::BacktestCliResult sample;
    const std::int64_t total_runs = runs + warmup_runs;
    for (std::int64_t idx = 0; idx < total_runs; ++idx) {
        quant_hft::apps::BacktestCliSpec run_spec = base_spec;
        run_spec.run_id = "compare-" + base_spec.engine_mode + "-" + std::to_string(idx);
        if (!quant_hft::apps::RequireParquetBacktestSpec(run_spec, error)) {
            return false;
        }

        const auto started = std::chrono::steady_clock::now();
        quant_hft::apps::BacktestCliResult result;
        if (!quant_hft::apps::RunBacktestSpec(run_spec, &result, error)) {
            return false;
        }
        const auto ended = std::chrono::steady_clock::now();
        const double elapsed =
            std::chrono::duration_cast<std::chrono::duration<double, std::milli>>(ended - started)
                .count();

        if (idx < warmup_runs) {
            continue;
        }
        elapsed_ms.push_back(elapsed);
        ticks_read.push_back(result.replay.ticks_read);
        sample = std::move(result);
    }

    if (elapsed_ms.empty() || ticks_read.empty()) {
        if (error != nullptr) {
            *error = "no benchmark samples collected";
        }
        return false;
    }

    std::vector<double> sorted_elapsed = elapsed_ms;
    std::sort(sorted_elapsed.begin(), sorted_elapsed.end());
    const std::size_t p95_index = quant_hft::apps::detail::P95Index(sorted_elapsed.size());
    const double mean_ms = quant_hft::apps::detail::Mean(elapsed_ms);
    const double mean_ticks = static_cast<double>(std::accumulate(
                                  ticks_read.begin(), ticks_read.end(), std::int64_t{0})) /
                              static_cast<double>(ticks_read.size());

    ModeSummary summary;
    summary.engine_mode = base_spec.engine_mode;
    summary.runs = runs;
    summary.warmup_runs = warmup_runs;
    summary.max_ticks = base_spec.max_ticks.has_value() ? base_spec.max_ticks.value() : 0;
    summary.elapsed_ms_values = std::move(elapsed_ms);
    summary.mean_ms = mean_ms;
    summary.p95_ms = sorted_elapsed[p95_index];
    summary.min_ms = *std::min_element(sorted_elapsed.begin(), sorted_elapsed.end());
    summary.max_ms = *std::max_element(sorted_elapsed.begin(), sorted_elapsed.end());
    summary.ticks_read_min = *std::min_element(ticks_read.begin(), ticks_read.end());
    summary.ticks_read_max = *std::max_element(ticks_read.begin(), ticks_read.end());
    summary.mean_ticks_per_sec = mean_ms > 0.0 ? (mean_ticks / (mean_ms / 1000.0)) : 0.0;
    summary.sample_result = std::move(sample);

    *out_summary = std::move(summary);
    return true;
}

}  // namespace

int main(int argc, char** argv) {
    using namespace quant_hft::apps;
    const auto args = ParseArgs(argc, argv);

    const std::string csv_path =
        detail::GetArgAny(args, {"csv_path", "csv-path"}, "backtest_data/c.csv");
    const std::string parquet_root =
        detail::GetArgAny(args, {"parquet_root", "parquet-root", "parquet_path", "parquet-path"},
                          "backtest_data/parquet/source=c");
    const std::string output_json =
        detail::GetArgAny(args, {"output_json", "result_json", "result-json"},
                          "docs/results/csv_parquet_speed_compare_c.json");

    std::int64_t max_ticks = 20000;
    std::int64_t runs = 3;
    std::int64_t warmup_runs = 1;
    bool deterministic_fills = false;
    const std::vector<std::string> symbols_filter =
        detail::SplitCommaList(detail::GetArgAny(args, {"symbols", "symbol"}));
    const std::string start_date =
        detail::NormalizeTradingDay(detail::GetArgAny(args, {"start_date", "start-date"}));
    const std::string end_date =
        detail::NormalizeTradingDay(detail::GetArgAny(args, {"end_date", "end-date"}));
    std::string error;

    {
        const std::string raw = detail::GetArgAny(args, {"max_ticks", "max-ticks"});
        if (!raw.empty() && (!detail::ParseInt64(raw, &max_ticks) || max_ticks <= 0)) {
            std::cerr << "csv_parquet_compare_cli: invalid max_ticks: " << raw << '\n';
            return 2;
        }
    }
    {
        const std::string raw = detail::GetArgAny(args, {"runs"});
        if (!raw.empty() && (!detail::ParseInt64(raw, &runs) || runs <= 0)) {
            std::cerr << "csv_parquet_compare_cli: invalid runs: " << raw << '\n';
            return 2;
        }
    }
    {
        const std::string raw = detail::GetArgAny(args, {"warmup_runs", "warmup-runs"});
        if (!raw.empty() && (!detail::ParseInt64(raw, &warmup_runs) || warmup_runs < 0)) {
            std::cerr << "csv_parquet_compare_cli: invalid warmup_runs: " << raw << '\n';
            return 2;
        }
    }
    {
        const std::string raw =
            detail::GetArgAny(args, {"deterministic_fills", "deterministic-fills"}, "false");
        if (!detail::ParseBool(raw, &deterministic_fills)) {
            std::cerr << "csv_parquet_compare_cli: invalid deterministic_fills: " << raw << '\n';
            return 2;
        }
    }

    BacktestCliSpec csv_spec;
    csv_spec.dataset_root = parquet_root;
    csv_spec.engine_mode = "parquet";
    csv_spec.rollover_mode = "strict";
    csv_spec.rollover_price_mode = "bbo";
    csv_spec.rollover_slippage_bps = 0.0;
    csv_spec.max_ticks = max_ticks;
    csv_spec.deterministic_fills = deterministic_fills;
    csv_spec.symbols = symbols_filter;
    csv_spec.start_date = start_date;
    csv_spec.end_date = end_date;
    csv_spec.account_id = "sim-account";
    csv_spec.run_id = "compare-csv";
    csv_spec.emit_state_snapshots = false;

    BacktestCliSpec parquet_spec = csv_spec;
    parquet_spec.run_id = "compare-parquet-b";

    ModeSummary csv_summary;
    ModeSummary parquet_summary;
    if (!RunModeSummary(csv_spec, runs, warmup_runs, &csv_summary, &error)) {
        std::cerr << "csv_parquet_compare_cli: " << error << '\n';
        return 1;
    }
    if (!RunModeSummary(parquet_spec, runs, warmup_runs, &parquet_summary, &error)) {
        std::cerr << "csv_parquet_compare_cli: " << error << '\n';
        return 1;
    }

    const BacktestSummary csv_backtest = SummarizeBacktest(csv_summary.sample_result);
    const BacktestSummary parquet_backtest = SummarizeBacktest(parquet_summary.sample_result);
    const double diff_total_pnl = std::fabs(csv_backtest.total_pnl - parquet_backtest.total_pnl);
    const double diff_drawdown =
        std::fabs(csv_backtest.max_drawdown - parquet_backtest.max_drawdown);
    const std::int64_t diff_intents =
        std::llabs(csv_backtest.intents_emitted - parquet_backtest.intents_emitted);
    const std::int64_t diff_order_events =
        std::llabs(csv_backtest.order_events - parquet_backtest.order_events);

    const bool ticks_consistent = csv_summary.ticks_read_min == parquet_summary.ticks_read_min &&
                                  csv_summary.ticks_read_max == parquet_summary.ticks_read_max;
    const double speedup =
        parquet_summary.mean_ms > 0.0 ? (csv_summary.mean_ms / parquet_summary.mean_ms) : 0.0;
    const double max_abs_diff = std::max(diff_total_pnl, diff_drawdown);
    const bool equal = ticks_consistent && diff_intents == 0 && diff_order_events == 0 &&
                       IsApproxEqual(diff_total_pnl, 0.0) && IsApproxEqual(diff_drawdown, 0.0);

    std::ostringstream json;
    json << "{\n"
         << "  \"benchmark\": \"csv_parquet_compare\",\n"
         << "  \"csv_path\": \"" << JsonEscape(csv_path) << "\",\n"
         << "  \"parquet_root\": \"" << JsonEscape(parquet_root) << "\",\n"
         << "  \"runs\": " << runs << ",\n"
         << "  \"warmup_runs\": " << warmup_runs << ",\n"
         << "  \"max_ticks\": " << max_ticks << ",\n"
         << "  \"deterministic_fills\": " << (deterministic_fills ? "true" : "false") << ",\n"
         << "  \"summary\": {\n"
         << "    \"csv\": {\n"
         << "      \"engine_mode\": \"parquet\",\n"
         << "      \"mean_ms\": " << detail::FormatDouble(csv_summary.mean_ms) << ",\n"
         << "      \"p95_ms\": " << detail::FormatDouble(csv_summary.p95_ms) << ",\n"
         << "      \"min_ms\": " << detail::FormatDouble(csv_summary.min_ms) << ",\n"
         << "      \"max_ms\": " << detail::FormatDouble(csv_summary.max_ms) << ",\n"
         << "      \"ticks_read_min\": " << csv_summary.ticks_read_min << ",\n"
         << "      \"ticks_read_max\": " << csv_summary.ticks_read_max << ",\n"
         << "      \"scan_rows\": " << csv_summary.sample_result.replay.scan_rows << ",\n"
         << "      \"scan_row_groups\": " << csv_summary.sample_result.replay.scan_row_groups
         << ",\n"
         << "      \"io_bytes\": " << csv_summary.sample_result.replay.io_bytes << ",\n"
         << "      \"early_stop_hit\": "
         << (csv_summary.sample_result.replay.early_stop_hit ? "true" : "false") << ",\n"
         << "      \"mean_ticks_per_sec\": " << detail::FormatDouble(csv_summary.mean_ticks_per_sec)
         << "\n"
         << "    },\n"
         << "    \"parquet\": {\n"
         << "      \"engine_mode\": \"parquet\",\n"
         << "      \"mean_ms\": " << detail::FormatDouble(parquet_summary.mean_ms) << ",\n"
         << "      \"p95_ms\": " << detail::FormatDouble(parquet_summary.p95_ms) << ",\n"
         << "      \"min_ms\": " << detail::FormatDouble(parquet_summary.min_ms) << ",\n"
         << "      \"max_ms\": " << detail::FormatDouble(parquet_summary.max_ms) << ",\n"
         << "      \"ticks_read_min\": " << parquet_summary.ticks_read_min << ",\n"
         << "      \"ticks_read_max\": " << parquet_summary.ticks_read_max << ",\n"
         << "      \"scan_rows\": " << parquet_summary.sample_result.replay.scan_rows << ",\n"
         << "      \"scan_row_groups\": " << parquet_summary.sample_result.replay.scan_row_groups
         << ",\n"
         << "      \"io_bytes\": " << parquet_summary.sample_result.replay.io_bytes << ",\n"
         << "      \"early_stop_hit\": "
         << (parquet_summary.sample_result.replay.early_stop_hit ? "true" : "false") << ",\n"
         << "      \"mean_ticks_per_sec\": "
         << detail::FormatDouble(parquet_summary.mean_ticks_per_sec) << "\n"
         << "    },\n"
         << "    \"parquet_vs_csv_speedup\": " << detail::FormatDouble(speedup) << ",\n"
         << "    \"ticks_read_consistent\": " << (ticks_consistent ? "true" : "false") << ",\n"
         << "    \"diff\": {\n"
         << "      \"intents_emitted\": " << diff_intents << ",\n"
         << "      \"order_events\": " << diff_order_events << ",\n"
         << "      \"total_pnl_abs\": " << detail::FormatDouble(diff_total_pnl) << ",\n"
         << "      \"max_drawdown_abs\": " << detail::FormatDouble(diff_drawdown) << "\n"
         << "    }\n"
         << "  },\n"
         << "  \"equal\": " << (equal ? "true" : "false") << ",\n"
         << "  \"max_abs_diff\": " << detail::FormatDouble(max_abs_diff) << ",\n"
         << "  \"status\": \"" << (equal ? "ok" : "diff_found") << "\"\n"
         << "}\n";

    if (!WriteTextFile(output_json, json.str(), &error)) {
        std::cerr << "csv_parquet_compare_cli: " << error << '\n';
        return 1;
    }
    std::cout << json.str();
    return 0;
}
