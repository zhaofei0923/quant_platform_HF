#pragma once

#include <algorithm>
#include <array>
#include <cctype>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstdlib>
#include <ctime>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <limits>
#include <map>
#include <numeric>
#include <optional>
#include <queue>
#include <set>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <tuple>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "quant_hft/backtest/contract_expiry_calendar.h"
#include "quant_hft/backtest/indicator_trace_csv_writer.h"
#include "quant_hft/backtest/indicator_trace_parquet_writer.h"
#include "quant_hft/backtest/metrics.h"
#include "quant_hft/backtest/parquet_data_feed.h"
#include "quant_hft/backtest/product_fee_config_loader.h"
#include "quant_hft/backtest/sub_strategy_indicator_trace_csv_writer.h"
#include "quant_hft/backtest/sub_strategy_indicator_trace_parquet_writer.h"
#include "quant_hft/common/cli_support.h"
#include "quant_hft/common/timestamp.h"
#include "quant_hft/contracts/runtime_semantics.h"
#include "quant_hft/services/bar_aggregator.h"
#include "quant_hft/services/dominant_contract_coordinator.h"
#include "quant_hft/services/market_bar_pipeline.h"
#include "quant_hft/services/market_state_detector.h"
#include "quant_hft/services/timeframe_state_fanout.h"
#include "quant_hft/strategy/composite_config_loader.h"
#include "quant_hft/strategy/composite_strategy.h"
#include "quant_hft/strategy/demo_live_strategy.h"
#include "quant_hft/strategy/strategy_main_config_loader.h"
#include "quant_hft/strategy/strategy_registry.h"

namespace quant_hft::backtest {

inline constexpr char kBacktestComputationSemanticsVersion[] = "3.0.0";

using quant_hft::cli::ArgMap;
using quant_hft::cli::JsonEscape;
using quant_hft::cli::UnixEpochMillisNow;
using quant_hft::cli::WriteTextFile;

struct ReplayTick;

namespace detail {

constexpr EpochNanos kNanosPerSecond = 1'000'000'000LL;
constexpr EpochNanos kNanosPerMillisecond = 1'000'000LL;
constexpr EpochNanos kNanosPerMinute = 60LL * kNanosPerSecond;

std::string ToLower(std::string text);

std::string Trim(std::string text);

std::string StripUtf8Bom(std::string text);

std::string NormalizeCsvHeaderName(std::string text);

std::uint64_t Fnv1a64(std::uint64_t seed, std::string_view text);

std::string HexDigest64(std::uint64_t value);

std::string StableDigest(std::string_view text);

std::string GetArgAny(const ArgMap& args, std::initializer_list<const char*> keys,
                      const std::string& fallback = "");

bool HasArgAny(const ArgMap& args, std::initializer_list<const char*> keys);

bool ParseBool(const std::string& raw, bool* out);

bool ParseInt64(const std::string& raw, std::int64_t* out);

bool ParseDouble(const std::string& raw, double* out);

std::string StripInlineComment(const std::string& line);

bool LoadYamlScalarMap(const std::filesystem::path& path, std::map<std::string, std::string>* out,
                       std::string* error);

bool ResolveDetectorYamlValue(const std::map<std::string, std::string>& values,
                              const std::string& field, std::string* out);

inline constexpr std::array<const char*, 12> kMarketStateDetectorFields = {
    "adx_period",         "adx_strong_threshold", "adx_weak_lower",        "adx_weak_upper",
    "kama_er_period",     "kama_fast_period",     "kama_slow_period",      "kama_er_strong",
    "kama_er_weak_lower", "atr_period",           "require_adx_for_trend", "use_kama_er",
};

bool ApplyDetectorConfigField(MarketStateDetectorConfig* config, const std::string& field,
                              const std::string& raw, const std::string& context,
                              std::string* error);

bool LoadMarketStateDetectorConfigFile(const std::string& config_path,
                                       MarketStateDetectorConfig* out_config,
                                       MarketStateDetectorConfigByProduct* out_by_product,
                                       std::string* error);

bool LoadMarketStateDetectorConfigFile(const std::string& config_path,
                                       MarketStateDetectorConfig* out_config, std::string* error);

std::string NormalizeTradingDay(const std::string& raw);

bool BuildUtcTm(const std::string& normalized_day, int hour, int minute, int second,
                std::tm* out_tm);

bool ParseTimeHms(const std::string& raw, int* hour, int* minute, int* second);

bool IsDaySessionTick(const std::string& update_time);

EpochNanos ToEpochNs(const std::string& trading_day, const std::string& update_time,
                     int update_millisec);

std::string TradingDayFromEpochNs(EpochNanos ts_ns);

std::string UpdateTimeFromEpochNs(EpochNanos ts_ns);

std::string DateTimeFromEpochNs(EpochNanos ts_ns);

std::string TickDateTimeFromTickFields(const std::string& trading_day,
                                       const std::string& update_time, int update_millisec,
                                       EpochNanos fallback_ts_ns);

std::string ShiftTradingDay(const std::string& trading_day, int day_offset);

std::string DeriveActionDayFromTradingDayAndUpdateTime(const std::string& trading_day,
                                                       const std::string& update_time);

std::string ResolveActionDay(const std::string& trading_day, const std::string& action_day,
                             const std::string& update_time);

std::string DateTimeFromDayAndUpdateTime(const std::string& day, const std::string& update_time);

std::string LocalDateTimeFromTradingDayAndUpdateTime(const std::string& trading_day,
                                                     const std::string& action_day,
                                                     const std::string& update_time,
                                                     EpochNanos fallback_ts_ns);

BarAggregator& SharedSessionResolver();

int ResolveSessionOrderForOutput(const std::string& exchange, const std::string& symbol,
                                 const std::string& update_time);

std::string ResolveSessionKey(const std::string& exchange_id, const std::string& instrument_id,
                              const std::string& update_time);

std::string BuildReplayMinuteKey(const std::string& trading_day, const std::string& update_time);

std::string BuildReplayBarContextKey(const std::string& instrument_id,
                                     const std::string& minute_key);

std::filesystem::path AlternateProductConfigCandidate(const std::filesystem::path& configured_path);

void AppendUniqueProductConfigCandidate(std::vector<std::filesystem::path>* candidates,
                                        const std::filesystem::path& candidate);

bool ResolveBacktestProductConfigPath(const std::string& configured_path,
                                      const std::string& strategy_main_config_path,
                                      std::string* out_path, std::string* error);

bool ResolveTradingSessionsConfigPathForParquetBacktest(std::string* out_path, std::string* error);

std::vector<std::string> SplitCsvLine(const std::string& line);

std::vector<std::string> SplitCommaList(const std::string& raw);

std::string FindCell(const std::map<std::string, std::size_t>& header_index,
                     const std::vector<std::string>& cells,
                     std::initializer_list<const char*> candidates);

std::string InstrumentSymbolPrefix(const std::string& instrument_id);

double Clamp01(double value);

std::string FormatDouble(double value);

bool WriteWalLine(std::ofstream* out, const std::string& line);

bool ParseTraceOutputFormat(const std::string& raw, std::string* out_value);

bool TraceOutputWritesCsv(const std::string& format);

bool TraceOutputWritesParquet(const std::string& format);

struct TraceOutputPaths {
    std::string primary_path;
    std::string csv_path;
    std::string parquet_path;
};

std::filesystem::path TraceBasePath(std::filesystem::path path);

TraceOutputPaths ResolveTraceOutputPaths(const std::string& requested_path,
                                         const std::string& default_base_path,
                                         const std::string& format);

std::size_t P95Index(std::size_t count);

double Mean(const std::vector<double>& values);

bool ExtractJsonNumber(const std::string& json, const std::string& key, double* out_value);

bool ExtractJsonString(const std::string& json, const std::string& key, std::string* out_value);

bool ExtractJsonBool(const std::string& json, const std::string& key, bool* out_value);

}  // namespace detail

struct BacktestStrategyConfig {
    std::string strategy_id;
    std::string strategy_factory{"composite"};
    std::string strategy_main_config_path;
    std::string strategy_composite_config;
    std::string product_id;
    RiskManagementConfig risk_management;
};

struct BacktestCliSpec {
    std::string csv_path;
    std::string dataset_root;
    std::string dataset_manifest;
    std::string detector_config_path;
    std::string engine_mode{"csv"};
    std::string online_runtime_config_path{"configs/sim/ctp.yaml"};
    std::string behavior_profile{"online_parity"};
    std::string parameter_profile{"sim"};
    std::string initialization_policy{"cold_start"};
    std::string input_timestamp_basis{"legacy_exchange_local"};
    std::string rollover_mode{"flat_only"};
    std::string product_series_mode{"raw"};
    std::string rollover_price_mode{"bbo"};
    double rollover_slippage_bps{0.0};
    std::vector<std::string> symbols;
    std::string start_date;
    std::string end_date;
    std::optional<std::int64_t> max_ticks;
    bool deterministic_fills{true};
    bool streaming{true};
    bool strict_parquet{true};
    std::string wal_path;
    std::string account_id{"sim-account"};
    std::string run_id;
    double initial_equity{1'000'000.0};
    std::string product_config_path;
    std::string contract_expiry_calendar_path;
    std::string strategy_main_config_path;
    std::string strategy_factory{"demo"};
    std::string strategy_composite_config;
    std::vector<BacktestStrategyConfig> strategy_configs;
    bool emit_state_snapshots{false};
    bool emit_indicator_trace{false};
    std::string trace_output_format{"csv"};
    std::string indicator_trace_path;
    bool emit_sub_strategy_indicator_trace{false};
    std::string sub_strategy_indicator_trace_path;
    bool emit_trades{true};
    bool emit_orders{true};
    bool emit_position_history{false};
    bool emit_per_variety_outputs{false};
    MarketStateDetectorConfig detector_config{};
    MarketStateDetectorConfigByProduct detector_config_by_product{};
};

struct ReplayTick {
    std::string trading_day;
    std::string instrument_id;
    std::string exchange_id;
    std::string update_time;
    int update_millisec{0};
    EpochNanos ts_ns{0};
    double last_price{0.0};
    std::int64_t volume{0};
    double bid_price_1{0.0};
    std::int64_t bid_volume_1{0};
    double ask_price_1{0.0};
    std::int64_t ask_volume_1{0};
    double open_interest{0.0};
    std::string action_day;
};

namespace detail {

struct ReplayBarTickContext {
    ReplayTick first_tick;
    ReplayTick last_tick;
    bool initialized{false};
};

struct ReplaySignalTiming {
    EpochNanos signal_ts_ns{0};
    std::string signal_trading_day;
    std::string signal_action_day;
    std::string signal_update_time;
    int signal_update_millisec{0};
};

struct PendingBarIntent {
    SignalIntent intent;
    ReplaySignalTiming timing;
    std::string signal_session_key;
    MarketRegime market_regime{MarketRegime::kUnknown};
};

bool TrackReplayBarTickContext(const ReplayTick& tick,
                               std::unordered_map<std::string, ReplayBarTickContext>* contexts,
                               std::string* error);

bool ConsumeReplayBarTickContext(const BarSnapshot& bar,
                                 std::unordered_map<std::string, ReplayBarTickContext>* contexts,
                                 ReplayBarTickContext* out_context, std::string* error);

struct ReplayAggregatedBar {
    BarSnapshot bar;
    ReplayBarTickContext context;
    std::int32_t timeframe_minutes{1};
    StateSnapshot7D state;
    bool strategy_eligible{true};
    std::optional<double> kama;
    std::optional<double> atr;
    std::optional<double> adx;
    std::optional<double> er;
};

bool ParseReplayMinuteValue(const std::string& minute_key, std::string* trading_day,
                            int* minute_of_day);

std::string FormatReplayMinuteValue(const std::string& trading_day, int minute_of_day);

EpochNanos ReplayMinuteStartEpochNs(const std::string& minute_key);

std::string ReplayMinuteToDisplayDateTime(const std::string& minute_key,
                                          const std::string& action_day = "");

// Retains replay-only tick timing alongside the canonical shared market-state fanout.
// Indicator state and Bar eligibility are owned exclusively by TimeframeStateFanout.
class ReplayTimeframeFanout {
   public:
    explicit ReplayTimeframeFanout(std::vector<std::int32_t> timeframes,
                                   MarketStateDetectorConfig detector = {},
                                   MarketStateDetectorConfigByProduct detector_by_product = {})
        : fanout_(timeframes.empty() ? std::vector<std::int32_t>{1} : std::move(timeframes),
                  detector, std::move(detector_by_product)) {}

    void ObserveOneMinuteContext(const BarSnapshot& bar, const ReplayBarTickContext& context) {
        if (!context.initialized) return;
        std::string day;
        int minute = 0;
        if (!ParseReplayMinuteValue(bar.minute, &day, &minute)) return;
        for (const auto timeframe : fanout_.timeframes()) {
            const auto bucket_minute = FormatReplayMinuteValue(day, minute / timeframe * timeframe);
            auto& timing = contexts_[Key(bar.instrument_id, timeframe, bucket_minute)];
            if (!timing.initialized)
                timing = context;
            else
                timing.last_tick = context.last_tick;
        }
    }

    std::vector<ReplayAggregatedBar> OnOneMinuteBar(const BarSnapshot& bar,
                                                    const ReplayBarTickContext& context) {
        ObserveOneMinuteContext(bar, context);
        return Adapt(fanout_.OnOneMinuteBar(bar));
    }

    std::vector<ReplayAggregatedBar> Flush() { return Adapt(fanout_.Flush()); }

    std::vector<ReplayAggregatedBar> FlushFinished(
        const std::unordered_map<std::string, EpochNanos>& last_tick_by_instrument,
        EpochNanos cutoff) {
        std::vector<ReplayAggregatedBar> out;
        for (const auto& [instrument, last_tick] : last_tick_by_instrument) {
            if (last_tick > cutoff) {
                continue;
            }
            auto batch = Adapt(fanout_.FlushInstrument(instrument));
            out.insert(out.end(), batch.begin(), batch.end());
        }
        std::sort(out.begin(), out.end(), [](const auto& left, const auto& right) {
            return std::tie(left.bar.ts_ns, left.timeframe_minutes, left.bar.instrument_id) <
                   std::tie(right.bar.ts_ns, right.timeframe_minutes, right.bar.instrument_id);
        });
        return out;
    }

    void ResetInstrument(const std::string& instrument) {
        fanout_.ResetInstrument(instrument);
        const std::string prefix = instrument + "|";
        for (auto it = contexts_.begin(); it != contexts_.end();) {
            it = it->first.rfind(prefix, 0) == 0 ? contexts_.erase(it) : std::next(it);
        }
    }

   private:
    static std::string Key(const std::string& instrument, std::int32_t timeframe,
                           const std::string& minute) {
        return instrument + "|" + std::to_string(timeframe) + "|" + minute;
    }

   public:
    std::vector<ReplayAggregatedBar> Adapt(const std::vector<TimeframeStateEmission>& emissions) {
        std::vector<ReplayAggregatedBar> out;
        for (const auto& emission : emissions) {
            const auto key =
                Key(emission.bar.instrument_id, emission.timeframe_minutes, emission.bar.minute);
            auto it = contexts_.find(key);
            if (it == contexts_.end()) {
                continue;
            }
            ReplayAggregatedBar bar;
            bar.bar = emission.bar;
            bar.context = it->second;
            bar.timeframe_minutes = emission.timeframe_minutes;
            bar.state = emission.state;
            bar.strategy_eligible = emission.strategy_eligible;
            bar.kama = emission.kama;
            bar.atr = emission.atr;
            bar.adx = emission.adx;
            bar.er = emission.er;
            out.push_back(std::move(bar));
            contexts_.erase(it);
        }
        return out;
    }

   private:
    TimeframeStateFanout fanout_;
    std::unordered_map<std::string, ReplayBarTickContext> contexts_;
};

}  // namespace detail

struct ReplayReport {
    std::int64_t buffered_input_rows_high_water{0};
    std::int64_t ticks_read{0};
    std::int64_t scan_rows{0};
    std::int64_t scan_row_groups{0};
    std::int64_t io_bytes{0};
    bool early_stop_hit{false};
    std::int64_t bars_emitted{0};
    std::int64_t intents_emitted{0};
    std::string first_instrument;
    std::string last_instrument;
    std::int64_t instrument_count{0};
    std::vector<std::string> instrument_universe;
    EpochNanos first_ts_ns{0};
    EpochNanos last_ts_ns{0};
};

struct InstrumentPnlSnapshot {
    std::int32_t net_position{0};
    double avg_open_price{0.0};
    double realized_pnl{0.0};
    double unrealized_pnl{0.0};
    double last_price{0.0};
};

struct BacktestPerformanceSummary {
    double initial_equity{0.0};
    double final_equity{0.0};
    double total_commission{0.0};
    double total_pnl_after_cost{0.0};
    double max_margin_used{0.0};
    double final_margin_used{0.0};
    std::int64_t margin_clipped_orders{0};
    std::int64_t margin_rejected_orders{0};
    double total_realized_pnl{0.0};
    double total_unrealized_pnl{0.0};
    double total_pnl{0.0};
    double max_equity{0.0};
    double min_equity{0.0};
    double max_drawdown{0.0};
    std::map<std::string, std::int64_t> order_status_counts;
};

struct RolloverEvent {
    std::string symbol;
    std::string from_instrument;
    std::string to_instrument;
    std::string mode;
    std::int32_t position{0};
    std::string direction;
    double from_price{0.0};
    double to_price{0.0};
    std::int32_t canceled_orders{0};
    std::string price_mode;
    double slippage_bps{0.0};
    EpochNanos ts_ns{0};
};

struct RolloverAction {
    std::string symbol;
    std::string action;
    std::string from_instrument;
    std::string to_instrument;
    std::int32_t position{0};
    std::string side;
    double price{0.0};
    std::string mode;
    std::string price_mode;
    double slippage_bps{0.0};
    std::int32_t canceled_orders{0};
    EpochNanos ts_ns{0};
};

struct DeterministicReplayReport {
    ReplayReport replay;
    std::int64_t intents_processed{0};
    std::int64_t order_events_emitted{0};
    std::int64_t wal_records{0};
    std::map<std::string, std::int64_t> instrument_bars;
    std::map<std::string, InstrumentPnlSnapshot> instrument_pnl;
    double total_realized_pnl{0.0};
    double total_unrealized_pnl{0.0};
    BacktestPerformanceSummary performance;
    std::vector<std::string> invariant_violations;
    std::vector<RolloverEvent> rollover_events;
    std::vector<RolloverAction> rollover_actions;
    double rollover_slippage_cost{0.0};
    std::int64_t rollover_canceled_orders{0};
};

struct BacktestCliResult {
    RuntimeSemanticsConfig runtime_semantics;
    std::string event_clock{"legacy_exchange_local"};
    std::string initialization_result{"cold_start"};
    std::int64_t warmup_rejected_opens{0};
    std::int64_t runtime_gate_rejected_orders{0};
    std::string market_checkpoint_json;
    std::vector<std::string> runtime_limitations;
    std::vector<DominantContractStatus> contract_statuses;
    std::string run_id;
    std::string mode;
    std::string data_source;
    std::string engine_mode;
    std::string rollover_mode;
    double initial_equity{0.0};
    double final_equity{0.0};
    BacktestCliSpec spec;
    std::string input_signature;
    std::string data_signature;
    bool indicator_trace_enabled{false};
    std::string indicator_trace_path;
    std::int64_t indicator_trace_rows{0};
    bool sub_strategy_indicator_trace_enabled{false};
    std::string sub_strategy_indicator_trace_path;
    std::int64_t sub_strategy_indicator_trace_rows{0};
    ReplayReport replay;
    bool has_deterministic{false};
    DeterministicReplayReport deterministic;
    std::string version{"2.0"};
    Parameters parameters;
    AdvancedSummary advanced_summary;
    std::vector<DailyPerformance> daily;
    std::vector<TradeRecord> trades;
    std::vector<OrderRecord> orders;
    std::vector<RegimePerformance> regime_performance;
    std::vector<PositionSnapshot> position_history;
    ExecutionQuality execution_quality;
    RiskMetrics risk_metrics;
    RollingMetrics rolling_metrics;
    MonteCarloResult monte_carlo;
    std::vector<FactorExposure> factor_exposure;
};

struct BacktestSummary {
    std::int64_t intents_emitted{0};
    std::int64_t order_events{0};
    double total_pnl{0.0};
    double max_drawdown{0.0};
};

std::vector<TradeRecord> SortedTradesForOutput(std::vector<TradeRecord> trades);

std::vector<OrderRecord> SortedOrdersForOutput(std::vector<OrderRecord> orders);

bool ExportBacktestCsv(const BacktestCliResult& result, const std::string& out_dir,
                       std::string* error);

bool IsApproxEqual(double left, double right, double abs_tol = 1e-8, double rel_tol = 1e-6);

std::string ExtractSingleProductSymbol(const std::vector<std::string>& symbols);
bool IsParquetProductChainSelection(const std::vector<std::string>& symbols);

std::string DefaultBacktestStrategyId(const std::string& config_path, std::size_t index);

void AppendUniqueStrings(const std::vector<std::string>& values, std::vector<std::string>* out);

std::string PrimaryStrategyMainConfigPath(const BacktestCliSpec& spec);

bool PopulateStrategyConfigFromMainPath(const std::string& main_config_path,
                                        const std::string& strategy_id, BacktestStrategyConfig* out,
                                        StrategyMainConfig* loaded_main_config, std::string* error);

bool ResolveBacktestStrategyConfigs(const BacktestCliSpec& spec,
                                    std::vector<BacktestStrategyConfig>* out, std::string* error);

bool ParseBacktestCliSpec(const ArgMap& args, BacktestCliSpec* out, std::string* error);

bool RequireParquetBacktestSpec(const BacktestCliSpec& spec, std::string* error);

std::string BuildInputSignature(const BacktestCliSpec& spec);

std::string ComputeFileDigest(const std::filesystem::path& path, std::string* error);

std::string ComputeDatasetDigest(const std::filesystem::path& root, const std::string& start_date,
                                 const std::string& end_date, std::string* error);

bool ParseCsvTick(const std::map<std::string, std::size_t>& header_index,
                  const std::vector<std::string>& cells, ReplayTick* out_tick);

bool LoadCsvTicks(const BacktestCliSpec& spec, std::vector<ReplayTick>* out, std::string* error);

bool BuildTimestampRange(const BacktestCliSpec& spec, Timestamp* out_start, Timestamp* out_end,
                         std::string* error);

bool ValidatePartitionMetaFile(const std::filesystem::path& meta_path, std::string* error);

std::string SourceFilterFromSymbols(const std::vector<std::string>& symbols);

std::string ExtractSingleProductSymbol(const std::vector<std::string>& symbols);

struct ParquetSymbolSelection {
    std::vector<std::string> instrument_ids;
    std::vector<std::string> product_symbols;
};

ParquetSymbolSelection BuildParquetSymbolSelection(const std::vector<std::string>& symbols);

bool IsParquetProductChainSelection(const std::vector<std::string>& symbols);

class ProductSeriesAdjuster : public IBarAnalysisTransform {
   public:
    explicit ProductSeriesAdjuster(bool enabled) : enabled_(enabled) {}

    std::string Identity() const override { return enabled_ ? "continuous_adjusted_v1" : "raw_v1"; }
    std::unique_ptr<IBarAnalysisTransform> Clone() const override {
        return std::make_unique<ProductSeriesAdjuster>(*this);
    }
    bool SaveState(PersistenceState* out, std::string* error) const override {
        if (!out) {
            if (error) *error = "analysis transform state output is null";
            return false;
        }
        out->clear();
        (*out)["has_last_bar"] = has_last_bar_ ? "1" : "0";
        (*out)["last_instrument"] = last_instrument_id_;
        std::ostringstream close;
        close << std::setprecision(17) << last_analysis_close_;
        (*out)["last_analysis_close"] = close.str();
        for (const auto& [instrument, offset] : instrument_offsets_) {
            std::ostringstream value;
            value << std::setprecision(17) << offset;
            (*out)["offset." + instrument] = value.str();
        }
        return true;
    }
    bool LoadState(const PersistenceState& state, std::string* error) override {
        const auto last = state.find("last_instrument");
        const auto has = state.find("has_last_bar");
        const auto close = state.find("last_analysis_close");
        if (last == state.end() || has == state.end() || close == state.end() ||
            (has->second != "0" && has->second != "1")) {
            if (error) *error = "invalid analysis transform checkpoint";
            return false;
        }
        double parsed_close = 0;
        std::unordered_map<std::string, double> offsets;
        if (!detail::ParseDouble(close->second, &parsed_close) || !std::isfinite(parsed_close)) {
            if (error) *error = "invalid analysis transform last close";
            return false;
        }
        for (const auto& [key, value] : state) {
            if (key.rfind("offset.", 0) != 0) continue;
            double offset = 0;
            if (!detail::ParseDouble(value, &offset) || !std::isfinite(offset)) {
                if (error) *error = "invalid analysis transform offset";
                return false;
            }
            offsets[key.substr(7)] = offset;
        }
        has_last_bar_ = has->second == "1";
        last_instrument_id_ = last->second;
        last_analysis_close_ = parsed_close;
        instrument_offsets_ = std::move(offsets);
        return true;
    }

    BarSnapshot Apply(const BarSnapshot& raw_bar) override {
        BarSnapshot adjusted = raw_bar;
        if (!enabled_) {
            SetAnalysisFields(&adjusted, raw_bar, 0.0);
            return adjusted;
        }

        double offset = 0.0;
        const auto offset_it = instrument_offsets_.find(raw_bar.instrument_id);
        if (offset_it != instrument_offsets_.end()) {
            offset = offset_it->second;
        } else if (has_last_bar_ && raw_bar.instrument_id != last_instrument_id_) {
            offset = last_analysis_close_ - raw_bar.open;
            instrument_offsets_.emplace(raw_bar.instrument_id, offset);
        } else {
            instrument_offsets_.emplace(raw_bar.instrument_id, 0.0);
        }

        SetAnalysisFields(&adjusted, raw_bar, offset);
        has_last_bar_ = true;
        last_instrument_id_ = raw_bar.instrument_id;
        last_analysis_close_ = adjusted.analysis_close;
        return adjusted;
    }

   private:
    static void SetAnalysisFields(BarSnapshot* out, const BarSnapshot& raw_bar, double offset) {
        if (out == nullptr) {
            return;
        }
        out->analysis_open = raw_bar.open + offset;
        out->analysis_high = raw_bar.high + offset;
        out->analysis_low = raw_bar.low + offset;
        out->analysis_close = raw_bar.close + offset;
        out->analysis_price_offset = offset;
    }

    bool enabled_{false};
    bool has_last_bar_{false};
    std::string last_instrument_id_;
    double last_analysis_close_{0.0};
    std::unordered_map<std::string, double> instrument_offsets_;
};

std::vector<ParquetPartitionMeta> SelectParquetPartitionsForSymbols(
    ParquetDataFeed* feed, EpochNanos start_ts_ns, EpochNanos end_ts_ns,
    const std::vector<std::string>& symbols);

bool LoadParquetTicks(const BacktestCliSpec& spec, std::vector<ReplayTick>* out,
                      ReplayReport* report, std::string* error);

bool LoadTicksForSpec(const BacktestCliSpec& spec, std::vector<ReplayTick>* out,
                      std::string* out_data_source, ReplayReport* report, std::string* error);

StateSnapshot7D BuildStateSnapshotFromBar(const ReplayTick& /*first*/, const ReplayTick& last,
                                          const BarSnapshot& bar, EpochNanos ts_ns,
                                          std::int32_t timeframe_minutes = 1,
                                          MarketStateDetector* detector = nullptr);

StateSnapshot7D BuildStateSnapshotFromBar(const ReplayTick& first, const ReplayTick& last,
                                          double high, double low, std::int64_t volume_delta,
                                          EpochNanos ts_ns, std::int32_t timeframe_minutes = 1,
                                          MarketStateDetector* detector = nullptr);

struct PositionState {
    std::int32_t net_position{0};
    double avg_open_price{0.0};
    double realized_pnl{0.0};
};

double NormalizeContractMultiplier(double contract_multiplier);

double ResolveContractMultiplier(const ProductFeeEntry* fee_entry);

double ResolveContractMultiplier(const ProductFeeBook* product_fee_book,
                                 const std::string& instrument_id);

void ApplyTrade(PositionState* state, Side side, std::int32_t volume, double fill_price,
                double contract_multiplier = 1.0);

double ComputeUnrealized(std::int32_t net_position, double avg_open_price, double last_price,
                         double contract_multiplier = 1.0);

double ComputeTotalPnl(const std::map<std::string, PositionState>& state_by_instrument,
                       const std::map<std::string, double>& mark_price_by_instrument,
                       const ProductFeeBook* product_fee_book = nullptr);

double ComputeTotalEquity(double initial_equity,
                          const std::map<std::string, PositionState>& state_by_instrument,
                          const std::map<std::string, double>& mark_price_by_instrument,
                          double total_commission,
                          const ProductFeeBook* product_fee_book = nullptr);

double ComputeInstrumentMarginUsed(const std::string& instrument_id, const PositionState& state,
                                   const std::map<std::string, double>& mark_price_by_instrument,
                                   const ProductFeeBook& product_fee_book);

double ComputeTotalMarginUsed(const std::map<std::string, PositionState>& state_by_instrument,
                              const std::map<std::string, double>& mark_price_by_instrument,
                              const ProductFeeBook& product_fee_book);

bool ResolveSubscribedTimeframes(const BacktestCliSpec& spec,
                                 std::vector<std::int32_t>* out_timeframes, std::string* error);

std::pair<double, double> ComputeRolloverPrice(Side side, double last_price, double bid_price,
                                               double ask_price, const std::string& price_mode,
                                               double slippage_bps);

std::vector<std::string> ValidateInvariants(
    const std::map<std::string, InstrumentPnlSnapshot>& pnl);

std::string SideToString(Side side);

std::string SideToTitleString(Side side);

std::string OffsetFlagToString(OffsetFlag offset);

std::string OffsetFlagToTitleString(OffsetFlag offset);

std::string OrderStatusToString(OrderStatus status);

std::string SignalTypeToString(SignalType signal_type);

std::string MarketRegimeToString(MarketRegime regime);

std::string BuildDefaultIndicatorTraceBasePath(const std::string& run_id);

std::string BuildDefaultIndicatorTracePath(const std::string& run_id,
                                           const std::string& format = "csv");

std::string BuildDefaultSubStrategyIndicatorTraceBasePath(const std::string& run_id);

std::string BuildDefaultSubStrategyIndicatorTracePath(const std::string& run_id,
                                                      const std::string& format = "csv");

bool RunBacktestSpec(const BacktestCliSpec& spec, BacktestCliResult* out, std::string* error);

BacktestSummary SummarizeBacktest(const BacktestCliResult& result);

std::string RenderBacktestMarkdown(const BacktestCliResult& result);

std::string RenderBacktestJson(const BacktestCliResult& result);

}  // namespace quant_hft::backtest
