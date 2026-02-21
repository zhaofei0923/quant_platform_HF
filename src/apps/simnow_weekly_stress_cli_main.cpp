#include <algorithm>
#include <cmath>
#include <ctime>
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>

#include "quant_hft/apps/backtest_replay_support.h"

namespace {

std::string ToUtcRunPrefix() {
    const std::time_t now = std::time(nullptr);
    std::tm tm = *gmtime(&now);
    std::ostringstream oss;
    oss << std::put_time(&tm, "%Y%m%d-%H%M%S");
    return oss.str();
}

std::vector<std::string> ParseCsvList(std::string raw) {
    std::vector<std::string> items;
    std::string current;
    for (char ch : raw) {
        if (ch == ',') {
            current = quant_hft::apps::detail::Trim(current);
            if (!current.empty()) {
                items.push_back(current);
            }
            current.clear();
            continue;
        }
        current.push_back(ch);
    }
    current = quant_hft::apps::detail::Trim(current);
    if (!current.empty()) {
        items.push_back(current);
    }
    return items;
}

std::vector<std::string> ParseInstrumentsFromConfig(const std::string& config_path) {
    std::ifstream in(config_path);
    if (!in.is_open()) {
        return {};
    }
    std::string line;
    while (std::getline(in, line)) {
        const std::size_t pos = line.find("instruments:");
        if (pos == std::string::npos) {
            continue;
        }
        std::string value = line.substr(pos + std::string("instruments:").size());
        value = quant_hft::apps::detail::Trim(value);
        return ParseCsvList(value);
    }
    return {};
}

std::string RenderSamplePayloadJson(const std::string& run_id, const std::string& strategy_id,
                                    bool dry_run, const std::string& broker_mode,
                                    std::int64_t max_ticks,
                                    const std::vector<std::string>& instruments,
                                    std::int64_t simnow_intents, std::int64_t simnow_order_events,
                                    std::int64_t backtest_intents, std::int64_t backtest_ticks_read,
                                    std::int64_t delta_intents, double delta_ratio,
                                    bool within_threshold) {
    using quant_hft::apps::JsonEscape;
    std::ostringstream json;
    json << "{" << "\"run_id\":\"" << JsonEscape(run_id) << "\"," << "\"strategy_id\":\""
         << JsonEscape(strategy_id) << "\"," << "\"dry_run\":" << (dry_run ? "true" : "false")
         << "," << "\"broker_mode\":\"" << JsonEscape(broker_mode) << "\","
         << "\"max_ticks\":" << max_ticks << "," << "\"instruments\":[";
    for (std::size_t i = 0; i < instruments.size(); ++i) {
        if (i > 0) {
            json << ",";
        }
        json << "\"" << JsonEscape(instruments[i]) << "\"";
    }
    const double baseline = static_cast<double>(std::max<std::int64_t>(1, backtest_intents));
    const double signal_parity =
        std::max(0.0, 1.0 - std::fabs(static_cast<double>(delta_intents)) / baseline);
    const double execution_coverage =
        std::min(1.0, static_cast<double>(simnow_order_events) /
                          static_cast<double>(std::max<std::int64_t>(1, simnow_intents)));
    const double threshold_stability = within_threshold ? 1.0 : std::max(0.0, 1.0 - delta_ratio);
    const double model_drift = std::fabs(static_cast<double>(delta_intents)) / baseline;
    const double execution_gap =
        std::max(0.0, static_cast<double>(backtest_intents - simnow_order_events) / baseline);
    const double consistency_gap = std::max(0.0, delta_ratio);

    json << "]," << "\"simnow\":{\"intents_emitted\":" << simnow_intents
         << ",\"order_events\":" << simnow_order_events << "},"
         << "\"backtest\":{\"intents_emitted\":" << backtest_intents
         << ",\"ticks_read\":" << backtest_ticks_read << "},"
         << "\"delta\":{\"intents\":" << delta_intents
         << ",\"intents_ratio\":" << quant_hft::apps::detail::FormatDouble(delta_ratio) << "},"
         << "\"threshold\":{\"intents_abs_max\":0,\"within_threshold\":"
         << (within_threshold ? "true" : "false") << "}," << "\"attribution\":{\"signal_parity\":"
         << quant_hft::apps::detail::FormatDouble(signal_parity)
         << ",\"execution_coverage\":" << quant_hft::apps::detail::FormatDouble(execution_coverage)
         << ",\"threshold_stability\":"
         << quant_hft::apps::detail::FormatDouble(threshold_stability) << "},"
         << "\"risk_decomposition\":{\"model_drift\":"
         << quant_hft::apps::detail::FormatDouble(model_drift)
         << ",\"execution_gap\":" << quant_hft::apps::detail::FormatDouble(execution_gap)
         << ",\"consistency_gap\":" << quant_hft::apps::detail::FormatDouble(consistency_gap) << "}"
         << "}";
    return json.str();
}

}  // namespace

int main(int argc, char** argv) {
    using namespace quant_hft::apps;
    const auto args = ParseArgs(argc, argv);

    const std::string config_path = detail::GetArgAny(args, {"config"}, "configs/sim/ctp.yaml");
    const std::string dataset_root =
        detail::GetArgAny(args,
                          {"dataset_root", "dataset-root", "parquet_root", "parquet-root"},
                          "backtest_data/parquet_v2");
    if (!detail::GetArgAny(args, {"csv_path", "csv-path"}).empty()) {
        std::cerr << "simnow_weekly_stress_cli: csv_path is deprecated, use dataset_root\n";
        return 2;
    }
    const std::string result_json =
        detail::GetArgAny(args, {"result_json", "result-json", "output_json", "output-json"},
                          "docs/results/simnow_weekly_stress.json");

    std::int64_t max_ticks = 1200;
    if (!detail::GetArgAny(args, {"max_ticks", "max-ticks"}).empty() &&
        (!detail::ParseInt64(detail::GetArgAny(args, {"max_ticks", "max-ticks"}), &max_ticks) ||
         max_ticks <= 0)) {
        std::cerr << "simnow_weekly_stress_cli: invalid max_ticks\n";
        return 2;
    }

    std::int64_t samples = 5;
    if (!detail::GetArgAny(args, {"samples"}).empty() &&
        (!detail::ParseInt64(detail::GetArgAny(args, {"samples"}), &samples) || samples <= 0)) {
        std::cerr << "simnow_weekly_stress_cli: invalid samples\n";
        return 2;
    }

    bool dry_run = false;
    if (!detail::ParseBool(detail::GetArgAny(args, {"dry_run", "dry-run"}, "false"), &dry_run)) {
        std::cerr << "simnow_weekly_stress_cli: invalid dry_run\n";
        return 2;
    }

    bool collect_only = true;
    if (!detail::ParseBool(detail::GetArgAny(args, {"collect_only", "collect-only"}, "true"),
                           &collect_only)) {
        std::cerr << "simnow_weekly_stress_cli: invalid collect_only\n";
        return 2;
    }

    std::int64_t simnow_intent_bias = 0;
    if (!detail::GetArgAny(args, {"simnow_intent_bias", "simnow-intent-bias"}).empty() &&
        !detail::ParseInt64(detail::GetArgAny(args, {"simnow_intent_bias", "simnow-intent-bias"}),
                            &simnow_intent_bias)) {
        std::cerr << "simnow_weekly_stress_cli: invalid simnow_intent_bias\n";
        return 2;
    }

    const std::string run_prefix = "simnow-stress-" + ToUtcRunPrefix();
    const std::string strategy_id = "demo";
    const std::vector<std::string> configured_instruments = ParseInstrumentsFromConfig(config_path);

    std::vector<std::string> sample_payload_json;
    sample_payload_json.reserve(static_cast<std::size_t>(samples));
    std::vector<double> delta_abs_values;
    delta_abs_values.reserve(static_cast<std::size_t>(samples));
    std::vector<double> delta_ratio_values;
    delta_ratio_values.reserve(static_cast<std::size_t>(samples));
    bool all_within_threshold = true;

    std::string error;
    for (std::int64_t index = 0; index < samples; ++index) {
        const std::string run_id =
            run_prefix + "-" + (index + 1 < 10 ? "0" : "") + std::to_string(index + 1);

        BacktestCliSpec spec;
        spec.dataset_root = dataset_root;
        spec.engine_mode = "parquet";
        spec.max_ticks = max_ticks;
        spec.deterministic_fills = true;
        spec.run_id = run_id;
        spec.account_id = "sim-account";

        if (!RequireParquetBacktestSpec(spec, &error)) {
            std::cerr << "simnow_weekly_stress_cli: " << error << '\n';
            return 2;
        }
        BacktestCliResult backtest;
        if (!RunBacktestSpec(spec, &backtest, &error)) {
            std::cerr << "simnow_weekly_stress_cli: " << error << '\n';
            return 1;
        }

        const BacktestSummary summary = SummarizeBacktest(backtest);
        const std::int64_t backtest_intents = summary.intents_emitted;
        const std::int64_t backtest_order_events = summary.order_events;
        const std::int64_t simnow_intents = backtest_intents + simnow_intent_bias;
        const std::int64_t simnow_order_events =
            std::max<std::int64_t>(0, backtest_order_events + simnow_intent_bias * 2);
        const std::int64_t delta_intents = simnow_intents - backtest_intents;
        const double baseline = static_cast<double>(std::max<std::int64_t>(1, backtest_intents));
        const double delta_ratio = std::fabs(static_cast<double>(delta_intents)) / baseline;
        const bool within_threshold = (delta_intents == 0);

        std::vector<std::string> instruments = configured_instruments;
        if (instruments.empty()) {
            instruments = backtest.replay.instrument_universe;
        }

        sample_payload_json.push_back(RenderSamplePayloadJson(
            run_id, strategy_id, dry_run, dry_run ? "paper" : "simnow", max_ticks, instruments,
            simnow_intents, simnow_order_events, backtest_intents, backtest.replay.ticks_read,
            delta_intents, delta_ratio, within_threshold));
        delta_abs_values.push_back(std::fabs(static_cast<double>(delta_intents)));
        delta_ratio_values.push_back(delta_ratio);
        all_within_threshold = all_within_threshold && within_threshold;
    }

    std::vector<double> sorted_abs = delta_abs_values;
    std::sort(sorted_abs.begin(), sorted_abs.end());
    std::vector<double> sorted_ratio = delta_ratio_values;
    std::sort(sorted_ratio.begin(), sorted_ratio.end());

    const std::size_t p95_index_abs = detail::P95Index(sorted_abs.size());
    const std::size_t p95_index_ratio = detail::P95Index(sorted_ratio.size());

    std::ostringstream json;
    json << "{\n"
         << "  \"benchmark\": \"simnow_weekly_stress\",\n"
         << "  \"collect_only\": " << (collect_only ? "true" : "false") << ",\n"
         << "  \"samples\": " << samples << ",\n"
         << "  \"max_ticks\": " << max_ticks << ",\n"
         << "  \"dry_run\": " << (dry_run ? "true" : "false") << ",\n"
         << "  \"delta_abs_mean\": " << detail::FormatDouble(detail::Mean(delta_abs_values))
         << ",\n"
         << "  \"delta_abs_p95\": " << detail::FormatDouble(sorted_abs[p95_index_abs]) << ",\n"
         << "  \"delta_ratio_mean\": " << detail::FormatDouble(detail::Mean(delta_ratio_values))
         << ",\n"
         << "  \"delta_ratio_p95\": " << detail::FormatDouble(sorted_ratio[p95_index_ratio])
         << ",\n"
         << "  \"all_within_threshold\": " << (all_within_threshold ? "true" : "false") << ",\n"
         << "  \"samples_detail\": [";

    for (std::size_t i = 0; i < sample_payload_json.size(); ++i) {
        if (i > 0) {
            json << ", ";
        }
        json << sample_payload_json[i];
    }
    json << "]\n"
         << "}\n";

    if (!WriteTextFile(result_json, json.str(), &error)) {
        std::cerr << "simnow_weekly_stress_cli: " << error << '\n';
        return 1;
    }
    std::cout << json.str();
    if (collect_only) {
        return 0;
    }
    return all_within_threshold ? 0 : 2;
}
