#include "quant_hft/backtest/replay_runtime.h"

#include "quant_hft/common/scope_exit.h"
#include "quant_hft/core/flow_controller.h"
#include "quant_hft/core/runtime_semantics_loader.h"
#include "quant_hft/risk/risk_manager.h"
#include "quant_hft/services/dominant_contract_coordinator.h"
#include "quant_hft/services/order_manager.h"
#include "quant_hft/services/trading_session_calendar.h"

namespace quant_hft::backtest {

std::vector<TradeRecord> SortedTradesForOutput(std::vector<TradeRecord> trades) {
    auto trade_day = [](const TradeRecord& row) {
        const std::string normalized = detail::NormalizeTradingDay(row.trading_day);
        if (!normalized.empty()) {
            return normalized;
        }
        return detail::TradingDayFromEpochNs(row.timestamp_ns);
    };
    auto trade_update_time = [](const TradeRecord& row) {
        if (!row.update_time.empty()) {
            return row.update_time;
        }
        return detail::UpdateTimeFromEpochNs(row.timestamp_ns);
    };
    std::stable_sort(
        trades.begin(), trades.end(), [&](const TradeRecord& left, const TradeRecord& right) {
            const std::string left_day = trade_day(left);
            const std::string right_day = trade_day(right);
            if (left_day != right_day) {
                return left_day < right_day;
            }

            const int left_session_order = detail::ResolveSessionOrderForOutput(
                left.exchange, left.symbol, trade_update_time(left));
            const int right_session_order = detail::ResolveSessionOrderForOutput(
                right.exchange, right.symbol, trade_update_time(right));
            if (left_session_order != right_session_order) {
                return left_session_order < right_session_order;
            }

            if (left.timestamp_ns != right.timestamp_ns) {
                return left.timestamp_ns < right.timestamp_ns;
            }

            const std::int64_t left_seq =
                left.fill_seq > 0 ? left.fill_seq : std::numeric_limits<std::int64_t>::max();
            const std::int64_t right_seq =
                right.fill_seq > 0 ? right.fill_seq : std::numeric_limits<std::int64_t>::max();
            return left_seq < right_seq;
        });
    return trades;
}

std::vector<OrderRecord> SortedOrdersForOutput(std::vector<OrderRecord> orders) {
    auto order_day = [](const OrderRecord& row) {
        const std::string normalized = detail::NormalizeTradingDay(row.trading_day);
        if (!normalized.empty()) {
            return normalized;
        }
        return detail::TradingDayFromEpochNs(row.created_at_ns);
    };
    auto order_update_time = [](const OrderRecord& row) {
        if (!row.update_time.empty()) {
            return row.update_time;
        }
        return detail::UpdateTimeFromEpochNs(row.created_at_ns);
    };
    std::stable_sort(
        orders.begin(), orders.end(), [&](const OrderRecord& left, const OrderRecord& right) {
            const std::string left_day = order_day(left);
            const std::string right_day = order_day(right);
            if (left_day != right_day) {
                return left_day < right_day;
            }

            const int left_session_order =
                detail::ResolveSessionOrderForOutput("", left.symbol, order_update_time(left));
            const int right_session_order =
                detail::ResolveSessionOrderForOutput("", right.symbol, order_update_time(right));
            if (left_session_order != right_session_order) {
                return left_session_order < right_session_order;
            }

            if (left.created_at_ns != right.created_at_ns) {
                return left.created_at_ns < right.created_at_ns;
            }

            const std::int64_t left_seq =
                left.order_seq > 0 ? left.order_seq : std::numeric_limits<std::int64_t>::max();
            const std::int64_t right_seq =
                right.order_seq > 0 ? right.order_seq : std::numeric_limits<std::int64_t>::max();
            return left_seq < right_seq;
        });
    return orders;
}

bool PopulateStrategyConfigFromMainPath(const std::string& main_config_path,
                                        const std::string& strategy_id, BacktestStrategyConfig* out,
                                        StrategyMainConfig* loaded_main_config,
                                        std::string* error) {
    if (out == nullptr) {
        if (error != nullptr) {
            *error = "strategy config output is null";
        }
        return false;
    }
    StrategyMainConfig main_config;
    if (!LoadStrategyMainConfig(main_config_path, &main_config, error)) {
        return false;
    }
    if (main_config.run_type != "backtest") {
        if (error != nullptr) {
            *error = "strategy_main_config run_type must be backtest for backtest replay";
        }
        return false;
    }

    out->strategy_id =
        strategy_id.empty() ? DefaultBacktestStrategyId(main_config_path, 0) : strategy_id;
    out->strategy_factory = "composite";
    out->strategy_main_config_path = main_config_path;
    out->strategy_composite_config = main_config_path;
    out->product_id = main_config.composite.product_id;
    out->risk_management = main_config.risk_management;
    if (loaded_main_config != nullptr) {
        *loaded_main_config = std::move(main_config);
    }
    return true;
}

bool ResolveBacktestStrategyConfigs(const BacktestCliSpec& spec,
                                    std::vector<BacktestStrategyConfig>* out, std::string* error) {
    if (out == nullptr) {
        if (error != nullptr) {
            *error = "strategy config output is null";
        }
        return false;
    }
    out->clear();
    if (!spec.strategy_configs.empty()) {
        *out = spec.strategy_configs;
        return true;
    }

    BacktestStrategyConfig config;
    config.strategy_id = spec.strategy_factory;
    config.strategy_factory = spec.strategy_factory;
    config.strategy_main_config_path = spec.strategy_main_config_path;
    config.strategy_composite_config = spec.strategy_composite_config;
    if (!spec.strategy_main_config_path.empty()) {
        StrategyMainConfig main_config;
        if (!LoadStrategyMainConfig(spec.strategy_main_config_path, &main_config, error)) {
            return false;
        }
        config.product_id = main_config.composite.product_id;
        config.risk_management = main_config.risk_management;
    }
    out->push_back(std::move(config));
    return true;
}

bool ParseBacktestCliSpec(const ArgMap& args, BacktestCliSpec* out, std::string* error) {
    if (out == nullptr) {
        if (error != nullptr) {
            *error = "spec output is null";
        }
        return false;
    }

    BacktestCliSpec spec;
    const bool has_symbols = detail::HasArgAny(args, {"symbols", "symbol"});
    const bool has_start_date = detail::HasArgAny(args, {"start_date", "start-date", "start"});
    const bool has_end_date = detail::HasArgAny(args, {"end_date", "end-date", "end"});
    const bool has_strategy_factory =
        detail::HasArgAny(args, {"strategy_factory", "strategy-factory"});
    const bool has_strategy_composite_config =
        detail::HasArgAny(args, {"strategy_composite_config", "strategy-composite-config"});
    const bool has_initial_equity = detail::HasArgAny(args, {"initial_equity", "initial-equity"});
    const bool has_product_series_mode =
        detail::HasArgAny(args, {"product_series_mode", "product-series-mode"});
    const bool has_product_config_path =
        detail::HasArgAny(args, {"product_config_path", "product-config-path"});
    const bool has_contract_expiry_calendar_path =
        detail::HasArgAny(args, {"contract_expiry_calendar_path", "contract-expiry-calendar-path"});
    const bool has_max_loss_percent =
        detail::HasArgAny(args, {"max_loss_percent", "max-loss-percent"});

    if (has_max_loss_percent) {
        if (error != nullptr) {
            *error =
                "max_loss_percent has been removed; configure risk_per_trade_pct in each "
                "sub strategy params";
        }
        return false;
    }

    spec.csv_path = detail::GetArgAny(args, {"csv_path", "csv-path", "csv"});
    spec.dataset_root =
        detail::GetArgAny(args, {"dataset_root", "dataset-root", "parquet_root", "parquet-root"});
    spec.dataset_manifest = detail::GetArgAny(
        args, {"dataset_manifest", "dataset-manifest", "manifest_path", "manifest-path"});
    spec.detector_config_path = detail::GetArgAny(args, {"detector_config", "detector-config"});
    spec.engine_mode =
        detail::ToLower(detail::GetArgAny(args, {"engine_mode", "engine-mode"}, "csv"));
    spec.online_runtime_config_path = detail::GetArgAny(
        args, {"online_runtime_config_path", "online-runtime-config-path"}, "configs/sim/ctp.yaml");
    spec.behavior_profile = detail::ToLower(
        detail::GetArgAny(args, {"behavior_profile", "behavior-profile"}, "online_parity"));
    spec.parameter_profile = detail::ToLower(
        detail::GetArgAny(args, {"parameter_profile", "parameter-profile"},
                          spec.behavior_profile == "research" ? "backtest" : "sim"));
    spec.input_timestamp_basis = detail::ToLower(detail::GetArgAny(
        args, {"input_timestamp_basis", "input-timestamp-basis"}, "legacy_exchange_local"));
    if (spec.input_timestamp_basis != "utc" &&
        spec.input_timestamp_basis != "legacy_exchange_local") {
        if (error) *error = "input_timestamp_basis must be utc or legacy_exchange_local";
        return false;
    }
    spec.initialization_policy = detail::ToLower(
        detail::GetArgAny(args, {"initialization_policy", "initialization-policy"}, "cold_start"));
    spec.rollover_mode = detail::ToLower(
        detail::GetArgAny(args, {"rollover_mode", "rollover-mode"},
                          spec.behavior_profile == "research" ? "strict" : "flat_only"));
    spec.product_series_mode = detail::ToLower(
        detail::GetArgAny(args, {"product_series_mode", "product-series-mode"}, "raw"));
    spec.rollover_price_mode = detail::ToLower(
        detail::GetArgAny(args, {"rollover_price_mode", "rollover-price-mode"}, "bbo"));
    spec.start_date =
        detail::NormalizeTradingDay(detail::GetArgAny(args, {"start_date", "start-date", "start"}));
    spec.end_date =
        detail::NormalizeTradingDay(detail::GetArgAny(args, {"end_date", "end-date", "end"}));
    spec.wal_path = detail::GetArgAny(args, {"wal_path", "wal-path"});
    spec.account_id = detail::GetArgAny(args, {"account_id", "account-id"}, "sim-account");
    spec.run_id = detail::GetArgAny(args, {"run_id", "run-id"},
                                    "backtest-" + std::to_string(UnixEpochMillisNow()));
    spec.initial_equity = 1'000'000.0;
    spec.product_config_path =
        detail::GetArgAny(args, {"product_config_path", "product-config-path"});
    spec.contract_expiry_calendar_path =
        detail::GetArgAny(args, {"contract_expiry_calendar_path", "contract-expiry-calendar-path"});
    spec.strategy_main_config_path = detail::GetArgAny(
        args, {"strategy_main_config_path", "strategy-main-config-path", "main_config",
               "main-config", "composite_config", "composite-config"});
    spec.strategy_factory =
        detail::ToLower(detail::GetArgAny(args, {"strategy_factory", "strategy-factory"}, "demo"));
    spec.strategy_composite_config =
        detail::GetArgAny(args, {"strategy_composite_config", "strategy-composite-config"});
    spec.symbols = detail::SplitCommaList(detail::GetArgAny(args, {"symbols", "symbol"}));

    const std::vector<std::string> strategy_main_config_paths = detail::SplitCommaList(
        detail::GetArgAny(args, {"strategy_main_config_paths", "strategy-main-config-paths",
                                 "main_configs", "main-configs"}));
    const std::vector<std::string> strategy_ids =
        detail::SplitCommaList(detail::GetArgAny(args, {"strategy_ids", "strategy-ids"}));
    if (!strategy_main_config_paths.empty() && !spec.strategy_main_config_path.empty()) {
        if (error != nullptr) {
            *error =
                "strategy_main_config_path and strategy_main_config_paths are mutually "
                "exclusive";
        }
        return false;
    }
    if (!strategy_main_config_paths.empty() && !strategy_ids.empty() &&
        strategy_ids.size() != strategy_main_config_paths.size()) {
        if (error != nullptr) {
            *error = "strategy_ids size must match strategy_main_config_paths size";
        }
        return false;
    }
    if (!strategy_main_config_paths.empty() && has_strategy_factory &&
        spec.strategy_factory != "composite") {
        if (error != nullptr) {
            *error = "strategy_main_config_paths currently supports strategy_factory=composite";
        }
        return false;
    }
    if (!strategy_main_config_paths.empty() && has_strategy_composite_config) {
        if (error != nullptr) {
            *error = "strategy_composite_config is not used with strategy_main_config_paths";
        }
        return false;
    }

    {
        const std::string raw =
            detail::GetArgAny(args, {"rollover_slippage_bps", "rollover-slippage-bps"}, "0");
        double parsed = 0.0;
        if (!detail::ParseDouble(raw, &parsed)) {
            if (error != nullptr) {
                *error = "invalid rollover_slippage_bps: " + raw;
            }
            return false;
        }
        spec.rollover_slippage_bps = parsed;
    }

    {
        const std::string raw_max_ticks = detail::GetArgAny(args, {"max_ticks", "max-ticks"});
        if (!raw_max_ticks.empty()) {
            std::int64_t parsed = 0;
            if (!detail::ParseInt64(raw_max_ticks, &parsed)) {
                if (error != nullptr) {
                    *error = "invalid max_ticks: " + raw_max_ticks;
                }
                return false;
            }
            if (parsed > 0) {
                spec.max_ticks = parsed;
            } else if (parsed < 0) {
                if (error != nullptr) {
                    *error = "max_ticks must be non-negative";
                }
                return false;
            }
        }
    }
    {
        const std::string raw_initial =
            detail::GetArgAny(args, {"initial_equity", "initial-equity"}, "1000000");
        double parsed = 0.0;
        if (!detail::ParseDouble(raw_initial, &parsed)) {
            if (error != nullptr) {
                *error = "invalid initial_equity: " + raw_initial;
            }
            return false;
        }
        spec.initial_equity = parsed;
    }
    {
        const std::string raw_det =
            detail::GetArgAny(args, {"deterministic_fills", "deterministic-fills"}, "true");
        bool parsed = true;
        if (!detail::ParseBool(raw_det, &parsed)) {
            if (error != nullptr) {
                *error = "invalid deterministic_fills: " + raw_det;
            }
            return false;
        }
        spec.deterministic_fills = parsed;
    }

    {
        const std::string raw_emit =
            detail::GetArgAny(args, {"emit_state_snapshots", "emit-state-snapshots"}, "false");
        bool parsed = false;
        if (!detail::ParseBool(raw_emit, &parsed)) {
            if (error != nullptr) {
                *error = "invalid emit_state_snapshots: " + raw_emit;
            }
            return false;
        }
        spec.emit_state_snapshots = parsed;
    }
    {
        const std::string raw_emit =
            detail::GetArgAny(args, {"emit_indicator_trace", "emit-indicator-trace"}, "false");
        bool parsed = false;
        if (!detail::ParseBool(raw_emit, &parsed)) {
            if (error != nullptr) {
                *error = "invalid emit_indicator_trace: " + raw_emit;
            }
            return false;
        }
        spec.emit_indicator_trace = parsed;
    }
    {
        const std::string raw_format =
            detail::GetArgAny(args, {"trace_output_format", "trace-output-format"}, "csv");
        std::string parsed;
        if (!detail::ParseTraceOutputFormat(raw_format, &parsed)) {
            if (error != nullptr) {
                *error = "invalid trace_output_format: " + raw_format;
            }
            return false;
        }
        spec.trace_output_format = parsed;
    }
    spec.indicator_trace_path =
        detail::GetArgAny(args, {"indicator_trace_path", "indicator-trace-path"});
    {
        const std::string raw_emit = detail::GetArgAny(
            args, {"emit_sub_strategy_indicator_trace", "emit-sub-strategy-indicator-trace"},
            "false");
        bool parsed = false;
        if (!detail::ParseBool(raw_emit, &parsed)) {
            if (error != nullptr) {
                *error = "invalid emit_sub_strategy_indicator_trace: " + raw_emit;
            }
            return false;
        }
        spec.emit_sub_strategy_indicator_trace = parsed;
    }
    spec.sub_strategy_indicator_trace_path = detail::GetArgAny(
        args, {"sub_strategy_indicator_trace_path", "sub-strategy-indicator-trace-path"});
    {
        const std::string raw_emit =
            detail::GetArgAny(args, {"emit_trades", "emit-trades"}, "true");
        bool parsed = true;
        if (!detail::ParseBool(raw_emit, &parsed)) {
            if (error != nullptr) {
                *error = "invalid emit_trades: " + raw_emit;
            }
            return false;
        }
        spec.emit_trades = parsed;
    }
    {
        const std::string raw_emit =
            detail::GetArgAny(args, {"emit_orders", "emit-orders"}, "true");
        bool parsed = true;
        if (!detail::ParseBool(raw_emit, &parsed)) {
            if (error != nullptr) {
                *error = "invalid emit_orders: " + raw_emit;
            }
            return false;
        }
        spec.emit_orders = parsed;
    }
    {
        const std::string raw_emit =
            detail::GetArgAny(args, {"emit_position_history", "emit-position-history"}, "false");
        bool parsed = false;
        if (!detail::ParseBool(raw_emit, &parsed)) {
            if (error != nullptr) {
                *error = "invalid emit_position_history: " + raw_emit;
            }
            return false;
        }
        spec.emit_position_history = parsed;
    }
    {
        const std::string raw_emit = detail::GetArgAny(
            args, {"emit_per_variety_outputs", "emit-per-variety-outputs"}, "false");
        bool parsed = false;
        if (!detail::ParseBool(raw_emit, &parsed)) {
            if (error != nullptr) {
                *error = "invalid emit_per_variety_outputs: " + raw_emit;
            }
            return false;
        }
        spec.emit_per_variety_outputs = parsed;
    }
    {
        const std::string raw_streaming =
            detail::GetArgAny(args, {"streaming", "streaming_mode", "streaming-mode"}, "true");
        bool parsed = true;
        if (!detail::ParseBool(raw_streaming, &parsed)) {
            if (error != nullptr) {
                *error = "invalid streaming: " + raw_streaming;
            }
            return false;
        }
        spec.streaming = parsed;
    }
    {
        const std::string raw_strict =
            detail::GetArgAny(args, {"strict_parquet", "strict-parquet"}, "true");
        bool parsed = true;
        if (!detail::ParseBool(raw_strict, &parsed)) {
            if (error != nullptr) {
                *error = "invalid strict_parquet: " + raw_strict;
            }
            return false;
        }
        spec.strict_parquet = parsed;
    }
    if (!strategy_main_config_paths.empty()) {
        spec.strategy_factory = "composite";
        spec.strategy_configs.clear();
        spec.strategy_configs.reserve(strategy_main_config_paths.size());
        bool applied_defaults = false;
        for (std::size_t index = 0; index < strategy_main_config_paths.size(); ++index) {
            StrategyMainConfig main_config;
            BacktestStrategyConfig strategy_config;
            const std::string strategy_id =
                index < strategy_ids.size()
                    ? strategy_ids[index]
                    : DefaultBacktestStrategyId(strategy_main_config_paths[index], index);
            if (!PopulateStrategyConfigFromMainPath(strategy_main_config_paths[index], strategy_id,
                                                    &strategy_config, &main_config, error)) {
                return false;
            }
            if (!applied_defaults) {
                if (!has_initial_equity) {
                    spec.initial_equity = main_config.backtest.initial_equity;
                }
                if (!has_product_series_mode && main_config.backtest.product_series_mode != "raw") {
                    spec.product_series_mode = main_config.backtest.product_series_mode;
                }
                if (!has_start_date && !main_config.backtest.start_date.empty()) {
                    spec.start_date = detail::NormalizeTradingDay(main_config.backtest.start_date);
                }
                if (!has_end_date && !main_config.backtest.end_date.empty()) {
                    spec.end_date = detail::NormalizeTradingDay(main_config.backtest.end_date);
                }
                if (!has_product_config_path && !main_config.backtest.product_config_path.empty()) {
                    spec.product_config_path = main_config.backtest.product_config_path;
                }
                if (!has_contract_expiry_calendar_path &&
                    !main_config.backtest.contract_expiry_calendar_path.empty()) {
                    spec.contract_expiry_calendar_path =
                        main_config.backtest.contract_expiry_calendar_path;
                }
                applied_defaults = true;
            }
            if (!has_symbols) {
                AppendUniqueStrings(main_config.backtest.symbols, &spec.symbols);
            }
            spec.strategy_configs.push_back(std::move(strategy_config));
        }
    } else if (!spec.strategy_main_config_path.empty()) {
        StrategyMainConfig main_config;
        if (!LoadStrategyMainConfig(spec.strategy_main_config_path, &main_config, error)) {
            return false;
        }
        if (main_config.run_type != "backtest") {
            if (error != nullptr) {
                *error = "strategy_main_config run_type must be backtest for backtest replay";
            }
            return false;
        }
        if (!has_initial_equity) {
            spec.initial_equity = main_config.backtest.initial_equity;
        }
        if (!has_product_series_mode && main_config.backtest.product_series_mode != "raw") {
            spec.product_series_mode = main_config.backtest.product_series_mode;
        }
        if (!has_symbols && !main_config.backtest.symbols.empty()) {
            spec.symbols = main_config.backtest.symbols;
        }
        if (!has_start_date && !main_config.backtest.start_date.empty()) {
            spec.start_date = detail::NormalizeTradingDay(main_config.backtest.start_date);
        }
        if (!has_end_date && !main_config.backtest.end_date.empty()) {
            spec.end_date = detail::NormalizeTradingDay(main_config.backtest.end_date);
        }
        if (!has_product_config_path && !main_config.backtest.product_config_path.empty()) {
            spec.product_config_path = main_config.backtest.product_config_path;
        }
        if (!has_contract_expiry_calendar_path &&
            !main_config.backtest.contract_expiry_calendar_path.empty()) {
            spec.contract_expiry_calendar_path = main_config.backtest.contract_expiry_calendar_path;
        }
        if (!has_strategy_factory) {
            spec.strategy_factory = "composite";
        }
        if (!has_strategy_composite_config) {
            spec.strategy_composite_config = spec.strategy_main_config_path;
        }
    }

    if (spec.engine_mode != "csv" && spec.engine_mode != "parquet" &&
        spec.engine_mode != "core_sim") {
        if (error != nullptr) {
            *error = "unsupported engine_mode: " + spec.engine_mode;
        }
        return false;
    }
    if (spec.behavior_profile != "online_parity" && spec.behavior_profile != "research") {
        if (error) *error = "behavior_profile must be online_parity or research";
        return false;
    }
    if (spec.parameter_profile != "sim" && spec.parameter_profile != "live" &&
        spec.parameter_profile != "backtest") {
        if (error) *error = "parameter_profile must be sim, live or backtest";
        return false;
    }
    if (spec.initialization_policy != "cold_start") {
        if (error)
            *error =
                "initialization_policy currently supports cold_start only; complete bars warm the "
                "reset strategy before opening";
        return false;
    }
    if (spec.behavior_profile == "online_parity" &&
        (spec.rollover_mode != "flat_only" || spec.product_series_mode != "raw")) {
        if (error)
            *error =
                "online_parity requires rollover_mode=flat_only and product_series_mode=raw; "
                "legacy rollover or adjusted series require explicit behavior_profile=research";
        return false;
    }
    if (spec.rollover_mode != "flat_only" && spec.rollover_mode != "strict" &&
        spec.rollover_mode != "carry" && spec.rollover_mode != "expiry_close") {
        if (error != nullptr) {
            *error = "unsupported rollover_mode: " + spec.rollover_mode;
        }
        return false;
    }
    if (spec.product_series_mode != "raw" && spec.product_series_mode != "continuous_adjusted") {
        if (error != nullptr) {
            *error = "unsupported product_series_mode: " + spec.product_series_mode;
        }
        return false;
    }
    if (spec.rollover_price_mode != "bbo" && spec.rollover_price_mode != "mid" &&
        spec.rollover_price_mode != "last") {
        if (error != nullptr) {
            *error = "unsupported rollover_price_mode: " + spec.rollover_price_mode;
        }
        return false;
    }
    if (spec.rollover_slippage_bps < 0.0) {
        if (error != nullptr) {
            *error = "rollover_slippage_bps must be non-negative";
        }
        return false;
    }
    if (!(spec.initial_equity > 0.0)) {
        if (error != nullptr) {
            *error = "initial_equity must be > 0";
        }
        return false;
    }
    if (spec.rollover_mode == "expiry_close") {
        if (spec.contract_expiry_calendar_path.empty()) {
            if (error != nullptr) {
                *error =
                    "contract_expiry_calendar_path is required when rollover_mode=expiry_close";
            }
            return false;
        }
        if (spec.engine_mode != "parquet") {
            if (error != nullptr) {
                *error = "rollover_mode=expiry_close requires engine_mode=parquet";
            }
            return false;
        }
        if (!spec.deterministic_fills) {
            if (error != nullptr) {
                *error = "rollover_mode=expiry_close requires deterministic_fills=true";
            }
            return false;
        }
        if (spec.product_series_mode != "raw") {
            if (error != nullptr) {
                *error = "rollover_mode=expiry_close requires product_series_mode=raw";
            }
            return false;
        }
        if (!IsParquetProductChainSelection(spec.symbols)) {
            if (error != nullptr) {
                *error = "rollover_mode=expiry_close requires product symbol selection";
            }
            return false;
        }
    }
    if (spec.strategy_configs.empty() && spec.strategy_factory != "demo" &&
        spec.strategy_factory != "composite") {
        if (error != nullptr) {
            *error = "unsupported strategy_factory: " + spec.strategy_factory;
        }
        return false;
    }
    if (spec.strategy_configs.empty() && spec.strategy_factory == "composite" &&
        spec.strategy_composite_config.empty()) {
        if (error != nullptr) {
            *error = "strategy_composite_config is required when strategy_factory=composite";
        }
        return false;
    }
    for (const BacktestStrategyConfig& strategy_config : spec.strategy_configs) {
        if (strategy_config.strategy_factory != "composite") {
            if (error != nullptr) {
                *error = "unsupported strategy_factory in strategy_main_config_paths: " +
                         strategy_config.strategy_factory;
            }
            return false;
        }
        if (strategy_config.strategy_composite_config.empty()) {
            if (error != nullptr) {
                *error = "strategy_composite_config is required for multi-strategy backtest";
            }
            return false;
        }
    }

    if (spec.engine_mode == "csv" && spec.csv_path.empty()) {
        if (error != nullptr) {
            *error = "csv_path is required when engine_mode=csv";
        }
        return false;
    }
    if (spec.engine_mode == "parquet" && spec.dataset_root.empty()) {
        if (error != nullptr) {
            *error = "dataset_root is required when engine_mode=parquet";
        }
        return false;
    }
    if (spec.engine_mode == "core_sim" && spec.dataset_root.empty() && spec.csv_path.empty()) {
        if (error != nullptr) {
            *error = "core_sim requires dataset_root or csv_path";
        }
        return false;
    }
    if (!spec.dataset_root.empty() && spec.dataset_manifest.empty()) {
        spec.dataset_manifest =
            (std::filesystem::path(spec.dataset_root) / "_manifest" / "partitions.jsonl").string();
    }
    if (!spec.detector_config_path.empty() &&
        !detail::LoadMarketStateDetectorConfigFile(spec.detector_config_path, &spec.detector_config,
                                                   &spec.detector_config_by_product, error)) {
        return false;
    }

    *out = std::move(spec);
    return true;
}

bool RequireParquetBacktestSpec(const BacktestCliSpec& spec, std::string* error) {
    if (spec.engine_mode != "parquet") {
        if (error != nullptr) {
            *error = "parquet-only policy: engine_mode must be parquet";
        }
        return false;
    }
    if (spec.dataset_root.empty()) {
        if (error != nullptr) {
            *error = "parquet-only policy: dataset_root is required";
        }
        return false;
    }
#if !QUANT_HFT_ENABLE_ARROW_PARQUET
    if (error != nullptr) {
        *error =
            "parquet-only policy: binary was built without Arrow/Parquet support "
            "(QUANT_HFT_ENABLE_ARROW_PARQUET=OFF). Rebuild with "
            "-DQUANT_HFT_ENABLE_ARROW_PARQUET=ON";
    }
    return false;
#endif
    return true;
}

std::string BuildInputSignature(const BacktestCliSpec& spec) {
    std::ostringstream symbols_stream;
    for (std::size_t index = 0; index < spec.symbols.size(); ++index) {
        if (index > 0) {
            symbols_stream << ',';
        }
        symbols_stream << spec.symbols[index];
    }
    std::ostringstream strategy_configs_stream;
    for (std::size_t index = 0; index < spec.strategy_configs.size(); ++index) {
        if (index > 0) {
            strategy_configs_stream << ',';
        }
        const BacktestStrategyConfig& config = spec.strategy_configs[index];
        strategy_configs_stream << config.strategy_id << ':' << config.strategy_factory << ':'
                                << config.strategy_main_config_path << ':'
                                << config.strategy_composite_config << ':' << config.product_id;
    }
    std::vector<std::string> detector_products;
    detector_products.reserve(spec.detector_config_by_product.size());
    for (const auto& entry : spec.detector_config_by_product) {
        detector_products.push_back(entry.first);
    }
    std::sort(detector_products.begin(), detector_products.end());
    std::ostringstream detector_by_product_stream;
    for (const std::string& product_id : detector_products) {
        const MarketStateDetectorConfig& detector = spec.detector_config_by_product.at(product_id);
        detector_by_product_stream
            << product_id << ':' << detector.adx_period << ':'
            << detail::FormatDouble(detector.adx_strong_threshold) << ':'
            << detail::FormatDouble(detector.adx_weak_lower) << ':'
            << detail::FormatDouble(detector.adx_weak_upper) << ':' << detector.kama_er_period
            << ':' << detector.kama_fast_period << ':' << detector.kama_slow_period << ':'
            << detail::FormatDouble(detector.kama_er_strong) << ':'
            << detail::FormatDouble(detector.kama_er_weak_lower) << ':' << detector.atr_period
            << ':' << (detector.require_adx_for_trend ? "true" : "false") << ':'
            << (detector.use_kama_er ? "true" : "false") << ';';
    }

    std::ostringstream oss;
    if (spec.behavior_profile == "online_parity") {
        RuntimeSemanticsConfig effective;
        std::string config_error;
        if (LoadRuntimeSemanticsConfig(spec.online_runtime_config_path, &effective, &config_error))
            oss << "runtime_effective=" << effective.effective_fingerprint << ';';
        else
            oss << "runtime_invalid=" << config_error << ';';
    }
    oss << "computation_semantics_version=" << kBacktestComputationSemanticsVersion << ';'
        << "csv_path=" << spec.csv_path << ';' << "dataset_root=" << spec.dataset_root << ';'
        << "dataset_manifest=" << spec.dataset_manifest << ';'
        << "detector_config_path=" << spec.detector_config_path << ';'
        << "detector_config.adx_period=" << spec.detector_config.adx_period << ';'
        << "detector_config.adx_strong_threshold="
        << detail::FormatDouble(spec.detector_config.adx_strong_threshold) << ';'
        << "detector_config.adx_weak_lower="
        << detail::FormatDouble(spec.detector_config.adx_weak_lower) << ';'
        << "detector_config.adx_weak_upper="
        << detail::FormatDouble(spec.detector_config.adx_weak_upper) << ';'
        << "detector_config.kama_er_period=" << spec.detector_config.kama_er_period << ';'
        << "detector_config.kama_fast_period=" << spec.detector_config.kama_fast_period << ';'
        << "detector_config.kama_slow_period=" << spec.detector_config.kama_slow_period << ';'
        << "detector_config.kama_er_strong="
        << detail::FormatDouble(spec.detector_config.kama_er_strong) << ';'
        << "detector_config.kama_er_weak_lower="
        << detail::FormatDouble(spec.detector_config.kama_er_weak_lower) << ';'
        << "detector_config.atr_period=" << spec.detector_config.atr_period << ';'
        << "detector_config.require_adx_for_trend="
        << (spec.detector_config.require_adx_for_trend ? "true" : "false") << ';'
        << "detector_config.use_kama_er=" << (spec.detector_config.use_kama_er ? "true" : "false")
        << ';' << "detector_config_by_product=" << detector_by_product_stream.str() << ';'
        << "online_runtime_config_path=" << spec.online_runtime_config_path << ';'
        << "behavior_profile=" << spec.behavior_profile << ';'
        << "parameter_profile=" << spec.parameter_profile << ';'
        << "initialization_policy=" << spec.initialization_policy << ';'
        << "input_timestamp_basis=" << spec.input_timestamp_basis << ';'
        << "engine_mode=" << spec.engine_mode << ';' << "rollover_mode=" << spec.rollover_mode
        << ';' << "product_series_mode=" << spec.product_series_mode << ';'
        << "contract_expiry_calendar_path=" << spec.contract_expiry_calendar_path << ';'
        << "rollover_price_mode=" << spec.rollover_price_mode << ';'
        << "rollover_slippage_bps=" << detail::FormatDouble(spec.rollover_slippage_bps) << ';'
        << "symbols=" << symbols_stream.str() << ';'
        << "streaming=" << (spec.streaming ? "true" : "false") << ';'
        << "strict_parquet=" << (spec.strict_parquet ? "true" : "false") << ';'
        << "start_date=" << spec.start_date << ';' << "end_date=" << spec.end_date << ';'
        << "max_ticks="
        << (spec.max_ticks.has_value() ? std::to_string(spec.max_ticks.value()) : "null") << ';'
        << "deterministic_fills=" << (spec.deterministic_fills ? "true" : "false") << ';'
        << "wal_path=" << spec.wal_path << ';' << "account_id=" << spec.account_id << ';'
        << "run_id=" << spec.run_id << ';'
        << "initial_equity=" << detail::FormatDouble(spec.initial_equity) << ';'
        << "product_config_path=" << spec.product_config_path << ';'
        << "strategy_main_config_path=" << spec.strategy_main_config_path << ';'
        << "strategy_factory=" << spec.strategy_factory << ';'
        << "strategy_composite_config=" << spec.strategy_composite_config << ';'
        << "strategy_configs=" << strategy_configs_stream.str() << ';'
        << "emit_state_snapshots=" << (spec.emit_state_snapshots ? "true" : "false") << ';'
        << "trace_output_format=" << spec.trace_output_format << ';'
        << "emit_indicator_trace=" << (spec.emit_indicator_trace ? "true" : "false") << ';'
        << "indicator_trace_path=" << spec.indicator_trace_path << ';'
        << "emit_sub_strategy_indicator_trace="
        << (spec.emit_sub_strategy_indicator_trace ? "true" : "false") << ';'
        << "sub_strategy_indicator_trace_path=" << spec.sub_strategy_indicator_trace_path << ';'
        << "emit_trades=" << (spec.emit_trades ? "true" : "false") << ';'
        << "emit_orders=" << (spec.emit_orders ? "true" : "false") << ';'
        << "emit_position_history=" << (spec.emit_position_history ? "true" : "false") << ';'
        << "emit_per_variety_outputs=" << (spec.emit_per_variety_outputs ? "true" : "false") << ';';
    return detail::StableDigest(oss.str());
}

std::string ComputeFileDigest(const std::filesystem::path& path, std::string* error) {
    std::ifstream input(path, std::ios::binary);
    if (!input.is_open()) {
        if (error != nullptr) {
            *error = "unable to open file for digest: " + path.string();
        }
        return "";
    }

    std::array<char, 1024 * 64> buffer{};
    std::uint64_t hash = 14695981039346656037ULL;
    while (input.good()) {
        input.read(buffer.data(), static_cast<std::streamsize>(buffer.size()));
        const std::streamsize count = input.gcount();
        if (count <= 0) {
            break;
        }
        hash =
            detail::Fnv1a64(hash, std::string_view(buffer.data(), static_cast<std::size_t>(count)));
    }

    if (input.bad()) {
        if (error != nullptr) {
            *error = "failed reading file for digest: " + path.string();
        }
        return "";
    }

    return detail::HexDigest64(hash);
}

std::string ComputeDatasetDigest(const std::filesystem::path& root, const std::string& start_date,
                                 const std::string& end_date, std::string* error) {
    if (!std::filesystem::exists(root)) {
        if (error != nullptr) {
            *error = "dataset root does not exist: " + root.string();
        }
        return "";
    }

    std::vector<std::filesystem::path> files;
    for (const auto& entry : std::filesystem::recursive_directory_iterator(root)) {
        if (!entry.is_regular_file()) {
            continue;
        }
        const std::string ext = detail::ToLower(entry.path().extension().string());
        if (ext == ".parquet" || ext == ".csv") {
            files.push_back(entry.path());
        }
    }
    std::sort(files.begin(), files.end());

    std::uint64_t hash = 14695981039346656037ULL;
    hash = detail::Fnv1a64(hash, root.string());
    hash = detail::Fnv1a64(hash, start_date);
    hash = detail::Fnv1a64(hash, end_date);

    for (const auto& path : files) {
        const std::string relative = std::filesystem::relative(path, root).string();
        const auto stat = std::filesystem::status(path);
        if (stat.type() != std::filesystem::file_type::regular) {
            continue;
        }
        const auto size = std::filesystem::file_size(path);
        const auto mtime = std::filesystem::last_write_time(path).time_since_epoch().count();
        hash = detail::Fnv1a64(hash, relative);
        hash = detail::Fnv1a64(hash, std::to_string(size));
        hash = detail::Fnv1a64(hash, std::to_string(mtime));
    }

    if (error != nullptr) {
        error->clear();
    }
    return detail::HexDigest64(hash);
}

bool ParseCsvTick(const std::map<std::string, std::size_t>& header_index,
                  const std::vector<std::string>& cells, ReplayTick* out_tick) {
    if (out_tick == nullptr) {
        return false;
    }

    ReplayTick tick;
    tick.trading_day = detail::NormalizeTradingDay(detail::FindCell(
        header_index, cells, {"TradingDay", "trading_day", "ActionDay", "action_day"}));
    tick.instrument_id = detail::FindCell(header_index, cells,
                                          {"InstrumentID", "instrument_id", "symbol", "Symbol"});
    tick.update_time = detail::FindCell(header_index, cells, {"UpdateTime", "update_time"});
    tick.action_day = detail::NormalizeTradingDay(
        detail::FindCell(header_index, cells, {"ActionDay", "action_day"}));

    {
        std::int64_t millis = 0;
        const std::string raw =
            detail::FindCell(header_index, cells, {"UpdateMillisec", "update_millisec"});
        if (!raw.empty()) {
            detail::ParseInt64(raw, &millis);
        }
        tick.update_millisec = static_cast<int>(std::max<std::int64_t>(0, millis));
    }

    {
        std::int64_t ts = 0;
        const std::string raw = detail::FindCell(header_index, cells, {"ts_ns", "TsNs", "ts"});
        if (!raw.empty() && detail::ParseInt64(raw, &ts)) {
            tick.ts_ns = ts;
            if (tick.trading_day.empty()) {
                tick.trading_day = detail::TradingDayFromEpochNs(ts);
            }
            if (tick.update_time.empty()) {
                tick.update_time = detail::UpdateTimeFromEpochNs(ts);
            }
        }
    }

    if (tick.ts_ns == 0) {
        if (!tick.action_day.empty()) {
            std::tm tm = {};
            int hour = 0, minute = 0, second = 0;
            if (!detail::ParseTimeHms(tick.update_time, &hour, &minute, &second) ||
                !detail::BuildUtcTm(tick.action_day, hour, minute, second, &tm))
                return false;
            tick.ts_ns = static_cast<EpochNanos>(timegm(&tm)) * detail::kNanosPerSecond +
                         tick.update_millisec * detail::kNanosPerMillisecond;
        } else {
            tick.ts_ns =
                detail::ToEpochNs(tick.trading_day, tick.update_time, tick.update_millisec);
        }
    }

    {
        const std::string raw = detail::FindCell(header_index, cells,
                                                 {"LastPrice", "last_price", "lastPrice", "close"});
        double value = 0.0;
        detail::ParseDouble(raw, &value);
        tick.last_price = value;
    }

    {
        const std::string raw = detail::FindCell(header_index, cells, {"Volume", "volume"});
        std::int64_t value = 0;
        detail::ParseInt64(raw, &value);
        tick.volume = value;
    }

    {
        const std::string raw =
            detail::FindCell(header_index, cells, {"BidPrice1", "bid_price1", "bid"});
        double value = 0.0;
        detail::ParseDouble(raw, &value);
        tick.bid_price_1 = value;
    }

    {
        const std::string raw =
            detail::FindCell(header_index, cells, {"BidVolume1", "bid_volume1"});
        std::int64_t value = 0;
        detail::ParseInt64(raw, &value);
        tick.bid_volume_1 = value;
    }

    {
        const std::string raw =
            detail::FindCell(header_index, cells, {"AskPrice1", "ask_price1", "ask"});
        double value = 0.0;
        detail::ParseDouble(raw, &value);
        tick.ask_price_1 = value;
    }

    {
        const std::string raw =
            detail::FindCell(header_index, cells, {"AskVolume1", "ask_volume1"});
        std::int64_t value = 0;
        detail::ParseInt64(raw, &value);
        tick.ask_volume_1 = value;
    }

    if (tick.instrument_id.empty() || tick.ts_ns <= 0) {
        return false;
    }

    if (tick.trading_day.empty()) {
        tick.trading_day = detail::TradingDayFromEpochNs(tick.ts_ns);
    }
    if (tick.update_time.empty()) {
        tick.update_time = detail::UpdateTimeFromEpochNs(tick.ts_ns);
    }

    *out_tick = std::move(tick);
    return true;
}

bool LoadCsvTicks(const BacktestCliSpec& spec, std::vector<ReplayTick>* out, std::string* error) {
    if (out == nullptr) {
        if (error != nullptr) {
            *error = "csv tick output is null";
        }
        return false;
    }

    const std::filesystem::path path(spec.csv_path);
    std::ifstream input(path);
    if (!input.is_open()) {
        if (error != nullptr) {
            *error = "unable to open csv file: " + path.string();
        }
        return false;
    }

    std::string header_line;
    if (!std::getline(input, header_line)) {
        if (error != nullptr) {
            *error = "csv file is empty: " + path.string();
        }
        return false;
    }

    const auto headers = detail::SplitCsvLine(header_line);
    std::map<std::string, std::size_t> header_index;
    for (std::size_t i = 0; i < headers.size(); ++i) {
        header_index[detail::NormalizeCsvHeaderName(headers[i])] = i;
    }
    std::unordered_set<std::string> instrument_filter;
    for (const std::string& symbol : spec.symbols) {
        if (!symbol.empty()) {
            instrument_filter.insert(symbol);
        }
    }

    out->clear();
    std::string line;
    while (std::getline(input, line)) {
        if (line.empty()) {
            continue;
        }

        ReplayTick tick;
        const auto cells = detail::SplitCsvLine(line);
        if (!ParseCsvTick(header_index, cells, &tick)) {
            continue;
        }
        if (spec.input_timestamp_basis == "utc" &&
            detail::FindCell(header_index, cells, {"ts_ns", "TsNs", "ts"}).empty()) {
            // Calendar fields are exchange local regardless of the selected epoch encoding.
            tick.ts_ns -= 8LL * 60LL * 60LL * detail::kNanosPerSecond;
        }

        if (!spec.start_date.empty()) {
            const std::string day = detail::NormalizeTradingDay(tick.trading_day);
            if (!day.empty() && day < spec.start_date) {
                continue;
            }
        }
        if (!spec.end_date.empty()) {
            const std::string day = detail::NormalizeTradingDay(tick.trading_day);
            if (!day.empty() && day > spec.end_date) {
                continue;
            }
        }
        if (!instrument_filter.empty() &&
            instrument_filter.find(tick.instrument_id) == instrument_filter.end()) {
            continue;
        }

        out->push_back(std::move(tick));
        if (spec.max_ticks.has_value() &&
            static_cast<std::int64_t>(out->size()) >= spec.max_ticks.value()) {
            break;
        }
    }

    std::sort(out->begin(), out->end(), [](const ReplayTick& left, const ReplayTick& right) {
        if (left.ts_ns != right.ts_ns) {
            return left.ts_ns < right.ts_ns;
        }
        return left.instrument_id < right.instrument_id;
    });
    return true;
}

bool BuildTimestampRange(const BacktestCliSpec& spec, Timestamp* out_start, Timestamp* out_end,
                         std::string* error) {
    if (out_start == nullptr || out_end == nullptr) {
        if (error != nullptr) {
            *error = "timestamp output is null";
        }
        return false;
    }

    try {
        if (spec.start_date.empty()) {
            *out_start = Timestamp(0);
        } else {
            const std::string text = spec.start_date.substr(0, 4) + "-" +
                                     spec.start_date.substr(4, 2) + "-" +
                                     spec.start_date.substr(6, 2) + " 00:00:00";
            *out_start = Timestamp::FromSql(text);
        }

        if (spec.end_date.empty()) {
            *out_end = Timestamp(4'102'444'799LL * detail::kNanosPerSecond);
        } else {
            const std::string text = spec.end_date.substr(0, 4) + "-" + spec.end_date.substr(4, 2) +
                                     "-" + spec.end_date.substr(6, 2) + " 23:59:59";
            *out_end = Timestamp::FromSql(text);
        }
    } catch (const std::exception& ex) {
        if (error != nullptr) {
            *error = ex.what();
        }
        return false;
    }

    return true;
}

bool ValidatePartitionMetaFile(const std::filesystem::path& meta_path, std::string* error) {
    std::ifstream input(meta_path);
    if (!input.is_open()) {
        if (error != nullptr) {
            *error = "unable to open parquet meta file: " + meta_path.string();
        }
        return false;
    }

    bool has_min = false;
    bool has_max = false;
    bool has_rows = false;
    bool has_schema = false;
    bool has_fingerprint = false;
    std::string schema_version;

    std::string line;
    while (std::getline(input, line)) {
        const std::size_t split = line.find('=');
        if (split == std::string::npos) {
            continue;
        }
        const std::string key = detail::Trim(line.substr(0, split));
        const std::string value = detail::Trim(line.substr(split + 1));
        if (key == "min_ts_ns") {
            has_min = true;
        } else if (key == "max_ts_ns") {
            has_max = true;
        } else if (key == "row_count") {
            has_rows = true;
        } else if (key == "schema_version") {
            has_schema = true;
            schema_version = value;
        } else if (key == "source_csv_fingerprint") {
            has_fingerprint = true;
        }
    }

    if (!has_min || !has_max || !has_rows || !has_schema || !has_fingerprint) {
        if (error != nullptr) {
            *error = "parquet meta missing required fields: " + meta_path.string();
        }
        return false;
    }
    if (schema_version != "v2" && schema_version != "v3") {
        if (error != nullptr) {
            *error = "unsupported schema_version in meta: " + meta_path.string();
        }
        return false;
    }
    return true;
}

std::string SourceFilterFromSymbols(const std::vector<std::string>& symbols) {
    std::set<std::string> product_prefixes;
    for (const std::string& symbol : symbols) {
        const std::string trimmed = detail::Trim(symbol);
        if (trimmed.empty()) {
            continue;
        }
        bool has_digit = false;
        for (unsigned char ch : trimmed) {
            if (std::isdigit(ch) != 0) {
                has_digit = true;
                break;
            }
        }
        if (has_digit) {
            continue;
        }
        const std::string prefix = detail::InstrumentSymbolPrefix(trimmed);
        if (!prefix.empty()) {
            product_prefixes.insert(prefix);
        }
    }
    if (product_prefixes.size() == 1U) {
        return *product_prefixes.begin();
    }
    return "";
}

std::string ExtractSingleProductSymbol(const std::vector<std::string>& symbols) {
    return SourceFilterFromSymbols(symbols);
}

ParquetSymbolSelection BuildParquetSymbolSelection(const std::vector<std::string>& symbols) {
    ParquetSymbolSelection selection;
    std::set<std::string> instrument_ids;
    std::set<std::string> product_symbols;
    for (const std::string& symbol : symbols) {
        const std::string trimmed = detail::Trim(symbol);
        if (trimmed.empty()) {
            continue;
        }

        bool has_digit = false;
        for (unsigned char ch : trimmed) {
            if (std::isdigit(ch) != 0) {
                has_digit = true;
                break;
            }
        }

        if (has_digit) {
            instrument_ids.insert(trimmed);
            continue;
        }

        const std::string product = detail::InstrumentSymbolPrefix(trimmed);
        if (!product.empty()) {
            product_symbols.insert(product);
        }
    }

    selection.instrument_ids.assign(instrument_ids.begin(), instrument_ids.end());
    selection.product_symbols.assign(product_symbols.begin(), product_symbols.end());
    return selection;
}

bool IsParquetProductChainSelection(const std::vector<std::string>& symbols) {
    const ParquetSymbolSelection selection = BuildParquetSymbolSelection(symbols);
    return selection.instrument_ids.empty() && !selection.product_symbols.empty();
}

std::vector<ParquetPartitionMeta> SelectParquetPartitionsForSymbols(
    ParquetDataFeed* feed, EpochNanos start_ts_ns, EpochNanos end_ts_ns,
    const std::vector<std::string>& symbols) {
    std::vector<ParquetPartitionMeta> selected;
    if (feed == nullptr) {
        return selected;
    }
    if (start_ts_ns > end_ts_ns) {
        return selected;
    }

    const ParquetSymbolSelection selection = BuildParquetSymbolSelection(symbols);
    std::unordered_set<std::string> seen_paths;
    auto append_unique = [&](const std::vector<ParquetPartitionMeta>& partitions) {
        for (const auto& partition : partitions) {
            if (seen_paths.insert(partition.file_path).second) {
                selected.push_back(partition);
            }
        }
    };

    if (selection.instrument_ids.empty() && selection.product_symbols.empty()) {
        append_unique(feed->QueryPartitions(start_ts_ns, end_ts_ns, std::vector<std::string>{},
                                            std::string{}));
    } else {
        if (!selection.instrument_ids.empty()) {
            append_unique(feed->QueryPartitions(start_ts_ns, end_ts_ns, selection.instrument_ids,
                                                std::string{}));
        }
        for (const std::string& product : selection.product_symbols) {
            append_unique(
                feed->QueryPartitions(start_ts_ns, end_ts_ns, std::vector<std::string>{}, product));
        }
    }

    std::sort(selected.begin(), selected.end(),
              [](const ParquetPartitionMeta& left, const ParquetPartitionMeta& right) {
                  if (left.min_ts_ns != right.min_ts_ns) {
                      return left.min_ts_ns < right.min_ts_ns;
                  }
                  return left.file_path < right.file_path;
              });
    return selected;
}

namespace {
std::vector<ParquetPartitionMeta> SelectReplayPartitions(ParquetDataFeed* feed,
                                                         const BacktestCliSpec& spec,
                                                         Timestamp fallback_start,
                                                         Timestamp fallback_end) {
    auto selected = SelectParquetPartitionsForSymbols(
        feed, 0, 4'102'444'799LL * detail::kNanosPerSecond, spec.symbols);
    selected.erase(
        std::remove_if(selected.begin(), selected.end(),
                       [&](const auto& partition) {
                           const auto day = detail::NormalizeTradingDay(partition.trading_day);
                           if (day.empty())
                               return partition.max_ts_ns < fallback_start.ToEpochNanos() ||
                                      partition.min_ts_ns > fallback_end.ToEpochNanos();
                           return (!spec.start_date.empty() && day < spec.start_date) ||
                                  (!spec.end_date.empty() && day > spec.end_date);
                       }),
        selected.end());
    return selected;
}

class MergedReplayCursor {
   public:
    bool Open(const BacktestCliSpec& spec, std::string* error) {
        spec_ = spec;
        if (!BuildTimestampRange(spec, &start_, &end_, error)) return false;
        ParquetDataFeed feed(spec.dataset_root);
        auto manifest = std::filesystem::path(spec.dataset_manifest);
        if (manifest.empty())
            manifest = std::filesystem::path(spec.dataset_root) / "_manifest/partitions.jsonl";
        else if (manifest.is_relative() && !std::filesystem::exists(manifest))
            manifest = std::filesystem::path(spec.dataset_root) / manifest;
        if (!feed.LoadManifestJsonl(manifest.string(), error)) return false;
        partitions_ = SelectReplayPartitions(&feed, spec, start_, end_);
        std::sort(partitions_.begin(), partitions_.end(), [](const auto& a, const auto& b) {
            return std::tie(a.min_ts_ns, a.instrument_id, a.file_path) <
                   std::tie(b.min_ts_ns, b.instrument_id, b.file_path);
        });
        for (const auto& partition : partitions_) {
            if (partition.min_ts_ns <= 0 || partition.max_ts_ns < partition.min_ts_ns) {
                if (error)
                    *error = "streaming requires valid partition timestamp bounds: " +
                             partition.file_path;
                return false;
            }
            if (spec.strict_parquet &&
                !ValidatePartitionMetaFile(partition.file_path + ".meta", error))
                return false;
            instruments_.insert(partition.instrument_id);
        }
        return true;
    }
    const std::set<std::string>& instruments() const { return instruments_; }
    bool Next(ReplayTick* out, bool* has, std::string* error) {
        *has = false;
        if (spec_.max_ticks && count_ >= *spec_.max_ticks) {
            totals_.early_stop_hit = true;
            return true;
        }
        if (pending_advance_) {
            const auto index = *pending_advance_;
            pending_advance_.reset();
            if (!Advance(index, error)) return false;
        }
        while (
            next_partition_ < partitions_.size() &&
            (heap_.empty() || partitions_[next_partition_].min_ts_ns <= heap_.top().tick.ts_ns)) {
            const auto index = next_partition_++;
            Active active;
            active.cursor = std::make_unique<ParquetTickCursor>();
            static const std::vector<std::string> columns = {
                "symbol",      "exchange",   "ts_ns",       "last_price",
                "last_volume", "bid_price1", "bid_volume1", "ask_price1",
                "ask_volume1", "volume",     "turnover",    "open_interest"};
            const auto& partition = partitions_[index];
            if (!active.cursor->Open(
                    partition,
                    partition.trading_day.empty() ? start_ : Timestamp(partition.min_ts_ns),
                    partition.trading_day.empty() ? end_ : Timestamp(partition.max_ts_ns), columns,
                    4096, error))
                return false;
            active_.emplace(index, std::move(active));
            if (!Advance(index, error)) return false;
        }
        if (heap_.empty()) return true;
        const auto node = heap_.top();
        heap_.pop();
        if (node.tick.ts_ns < last_emitted_ts_) {
            if (error) *error = "partition bounds cannot produce monotonic global replay";
            return false;
        }
        last_emitted_ts_ = node.tick.ts_ns;
        const auto& partition = partitions_[node.partition];
        *out = ReplayTick{};
        out->trading_day = detail::NormalizeTradingDay(partition.trading_day);
        out->instrument_id = node.tick.symbol;
        out->exchange_id = node.tick.exchange;
        out->ts_ns = node.tick.ts_ns;
        out->update_time = detail::UpdateTimeFromEpochNs(node.tick.ts_ns);
        out->update_millisec = static_cast<int>((node.tick.ts_ns % detail::kNanosPerSecond) /
                                                detail::kNanosPerMillisecond);
        out->last_price = node.tick.last_price;
        out->volume = node.tick.volume;
        out->bid_price_1 = node.tick.bid_price1;
        out->bid_volume_1 = node.tick.bid_volume1;
        out->ask_price_1 = node.tick.ask_price1;
        out->ask_volume_1 = node.tick.ask_volume1;
        out->open_interest = node.tick.open_interest;
        pending_advance_ = node.partition;
        ++count_;
        *has = true;
        return true;
    }
    ParquetScanMetrics metrics() const {
        auto out = totals_;
        for (const auto& [index, active] : active_) {
            (void)index;
            const auto& m = active.cursor->metrics();
            out.scan_rows += m.scan_rows;
            out.scan_row_groups += m.scan_row_groups;
            out.io_bytes += m.io_bytes;
            out.batches_read += m.batches_read;
        }
        return out;
    }

   private:
    struct Active {
        std::unique_ptr<ParquetTickCursor> cursor;
        std::uint64_t ordinal{0};
    };
    struct Node {
        Tick tick;
        std::size_t partition;
        std::uint64_t ordinal;
    };
    struct Compare {
        bool operator()(const Node& a, const Node& b) const {
            return std::tie(a.tick.ts_ns, a.tick.symbol, a.partition, a.ordinal) >
                   std::tie(b.tick.ts_ns, b.tick.symbol, b.partition, b.ordinal);
        }
    };
    bool Advance(std::size_t index, std::string* error) {
        auto it = active_.find(index);
        Tick tick;
        bool has = false;
        if (!it->second.cursor->Next(&tick, &has, error)) return false;
        if (has) {
            const auto& meta = partitions_[index];
            if (tick.ts_ns < meta.min_ts_ns || tick.ts_ns > meta.max_ts_ns ||
                (!meta.instrument_id.empty() && tick.symbol != meta.instrument_id)) {
                if (error)
                    *error = "parquet partition content does not match manifest bounds/identity: " +
                             meta.file_path;
                return false;
            }
            heap_.push(Node{std::move(tick), index, it->second.ordinal++});
        } else {
            const auto& m = it->second.cursor->metrics();
            totals_.scan_rows += m.scan_rows;
            totals_.scan_row_groups += m.scan_row_groups;
            totals_.io_bytes += m.io_bytes;
            totals_.batches_read += m.batches_read;
            active_.erase(it);
        }
        std::int64_t resident = 0;
        for (const auto& [active_index, active] : active_) {
            (void)active_index;
            resident += active.cursor->metrics().buffered_rows_high_water;
        }
        totals_.buffered_rows_high_water = std::max(totals_.buffered_rows_high_water, resident);
        return true;
    }
    BacktestCliSpec spec_;
    Timestamp start_, end_;
    std::vector<ParquetPartitionMeta> partitions_;
    std::set<std::string> instruments_;
    std::map<std::size_t, Active> active_;
    std::priority_queue<Node, std::vector<Node>, Compare> heap_;
    std::optional<std::size_t> pending_advance_;
    std::size_t next_partition_{0};
    std::int64_t count_{0};
    EpochNanos last_emitted_ts_{0};
    ParquetScanMetrics totals_;
};
}  // namespace

bool LoadParquetTicks(const BacktestCliSpec& spec, std::vector<ReplayTick>* out,
                      ReplayReport* report, std::string* error) {
    if (out == nullptr) {
        if (error != nullptr) {
            *error = "parquet tick output is null";
        }
        return false;
    }
    if (report == nullptr) {
        if (error != nullptr) {
            *error = "parquet replay report is null";
        }
        return false;
    }

    if (spec.streaming) {
        MergedReplayCursor cursor;
        if (!cursor.Open(spec, error)) return false;
        out->clear();
        for (;;) {
            ReplayTick tick;
            bool has = false;
            if (!cursor.Next(&tick, &has, error)) return false;
            if (!has) break;
            out->push_back(std::move(tick));
        }
        const auto metrics = cursor.metrics();
        report->scan_rows = metrics.scan_rows;
        report->scan_row_groups = metrics.scan_row_groups;
        report->io_bytes = metrics.io_bytes;
        report->early_stop_hit = metrics.early_stop_hit;
        report->buffered_input_rows_high_water = metrics.buffered_rows_high_water;
        return true;
    }
    const std::filesystem::path root(spec.dataset_root);
    if (!std::filesystem::exists(root)) {
        if (error != nullptr) {
            *error = "dataset_root does not exist: " + root.string();
        }
        return false;
    }

    Timestamp start;
    Timestamp end;
    if (!BuildTimestampRange(spec, &start, &end, error)) {
        return false;
    }

    ParquetDataFeed feed(root.string());
    std::filesystem::path manifest_path(spec.dataset_manifest);
    if (manifest_path.empty()) {
        manifest_path = root / "_manifest" / "partitions.jsonl";
    } else if (manifest_path.is_relative()) {
        if (!std::filesystem::exists(manifest_path)) {
            manifest_path = root / manifest_path;
        }
    }

    const bool manifest_exists = std::filesystem::exists(manifest_path);
    if (!manifest_exists && spec.strict_parquet) {
        if (error != nullptr) {
            *error =
                "missing parquet manifest, run csv_to_parquet_cli first: " + manifest_path.string();
        }
        return false;
    }
    if (manifest_exists) {
        std::string manifest_error;
        if (!feed.LoadManifestJsonl(manifest_path.string(), &manifest_error)) {
            if (error != nullptr) {
                *error = "failed to load parquet manifest: " + manifest_error;
            }
            return false;
        }
    }

    const auto selected = SelectReplayPartitions(&feed, spec, start, end);

    out->clear();

    ParquetScanMetrics totals;
    const std::vector<std::string> projected_columns = {
        "symbol",      "exchange",   "ts_ns",       "last_price", "last_volume", "bid_price1",
        "bid_volume1", "ask_price1", "ask_volume1", "volume",     "turnover",    "open_interest",
    };

    for (const ParquetPartitionMeta& partition : selected) {
        if (spec.strict_parquet) {
            const std::filesystem::path meta_path = partition.file_path + ".meta";
            if (!std::filesystem::exists(meta_path)) {
                if (error != nullptr) {
                    *error = "missing parquet meta sidecar: " + meta_path.string();
                }
                return false;
            }
            if (!ValidatePartitionMetaFile(meta_path, error)) {
                return false;
            }
        }

        const std::int64_t partition_limit = -1;

        std::vector<Tick> partition_ticks;
        ParquetScanMetrics partition_metrics;
        if (!feed.LoadPartitionTicks(
                partition, partition.trading_day.empty() ? start : Timestamp(partition.min_ts_ns),
                partition.trading_day.empty() ? end : Timestamp(partition.max_ts_ns),
                projected_columns, &partition_ticks, &partition_metrics, partition_limit, error)) {
            return false;
        }

        totals.scan_rows += partition_metrics.scan_rows;
        totals.scan_row_groups += partition_metrics.scan_row_groups;
        totals.io_bytes += partition_metrics.io_bytes;
        totals.early_stop_hit = totals.early_stop_hit || partition_metrics.early_stop_hit;

        std::vector<ReplayTick> replay_ticks;
        replay_ticks.reserve(partition_ticks.size());
        for (const Tick& tick : partition_ticks) {
            ReplayTick replay_tick;
            replay_tick.trading_day = detail::NormalizeTradingDay(partition.trading_day);
            if (replay_tick.trading_day.empty()) {
                replay_tick.trading_day = detail::TradingDayFromEpochNs(tick.ts_ns);
            }
            replay_tick.instrument_id = tick.symbol;
            replay_tick.exchange_id = tick.exchange;
            replay_tick.update_time = detail::UpdateTimeFromEpochNs(tick.ts_ns);
            replay_tick.update_millisec = static_cast<int>((tick.ts_ns % detail::kNanosPerSecond) /
                                                           detail::kNanosPerMillisecond);
            replay_tick.ts_ns = tick.ts_ns;
            replay_tick.open_interest = tick.open_interest;
            replay_tick.last_price = tick.last_price;
            replay_tick.volume = tick.volume;
            replay_tick.bid_price_1 = tick.bid_price1;
            replay_tick.bid_volume_1 = tick.bid_volume1;
            replay_tick.ask_price_1 = tick.ask_price1;
            replay_tick.ask_volume_1 = tick.ask_volume1;
            replay_ticks.push_back(std::move(replay_tick));
        }

        out->insert(out->end(), replay_ticks.begin(), replay_ticks.end());
    }
    // Materializing compatibility must select the same global prefix as the streaming merge.
    std::stable_sort(out->begin(), out->end(), [](const ReplayTick& left, const ReplayTick& right) {
        return std::tie(left.ts_ns, left.instrument_id) <
               std::tie(right.ts_ns, right.instrument_id);
    });

    if (spec.max_ticks.has_value() &&
        static_cast<std::int64_t>(out->size()) > spec.max_ticks.value()) {
        out->resize(static_cast<std::size_t>(spec.max_ticks.value()));
        totals.early_stop_hit = true;
    }

    report->scan_rows += totals.scan_rows;
    report->scan_row_groups += totals.scan_row_groups;
    report->io_bytes += totals.io_bytes;
    report->early_stop_hit = report->early_stop_hit || totals.early_stop_hit;
    return true;
}

bool LoadTicksForSpec(const BacktestCliSpec& spec, std::vector<ReplayTick>* out,
                      std::string* out_data_source, ReplayReport* report, std::string* error) {
    if (out == nullptr) {
        if (error != nullptr) {
            *error = "tick output is null";
        }
        return false;
    }
    if (out_data_source == nullptr) {
        if (error != nullptr) {
            *error = "data source output is null";
        }
        return false;
    }
    if (report == nullptr) {
        if (error != nullptr) {
            *error = "replay report output is null";
        }
        return false;
    }
    report->scan_rows = 0;
    report->scan_row_groups = 0;
    report->io_bytes = 0;
    report->early_stop_hit = false;

    if (spec.engine_mode == "parquet") {
        *out_data_source = "parquet";
        return LoadParquetTicks(spec, out, report, error);
    }

    if (spec.engine_mode == "core_sim") {
        if (!spec.dataset_root.empty()) {
            *out_data_source = "parquet";
            return LoadParquetTicks(spec, out, report, error);
        }
        *out_data_source = "csv";
        const bool ok = LoadCsvTicks(spec, out, error);
        if (ok && spec.max_ticks.has_value() &&
            static_cast<std::int64_t>(out->size()) >= spec.max_ticks.value()) {
            report->early_stop_hit = true;
        }
        return ok;
    }

    *out_data_source = "csv";
    const bool ok = LoadCsvTicks(spec, out, error);
    if (ok && spec.max_ticks.has_value() &&
        static_cast<std::int64_t>(out->size()) >= spec.max_ticks.value()) {
        report->early_stop_hit = true;
    }
    return ok;
}

bool ResolveSubscribedTimeframes(const BacktestCliSpec& spec,
                                 std::vector<std::int32_t>* out_timeframes, std::string* error) {
    if (out_timeframes == nullptr) {
        if (error != nullptr) {
            *error = "timeframe output is null";
        }
        return false;
    }
    std::set<std::int32_t> timeframe_set;
    const std::vector<BacktestStrategyConfig> strategy_configs = [&]() {
        if (!spec.strategy_configs.empty()) {
            return spec.strategy_configs;
        }
        BacktestStrategyConfig config;
        config.strategy_factory = spec.strategy_factory;
        config.strategy_composite_config = spec.strategy_composite_config;
        return std::vector<BacktestStrategyConfig>{config};
    }();
    for (const BacktestStrategyConfig& strategy_config : strategy_configs) {
        if (strategy_config.strategy_factory != "composite") {
            continue;
        }
        if (strategy_config.strategy_composite_config.empty()) {
            if (error != nullptr) {
                *error = "strategy_composite_config is required to resolve timeframe subscription";
            }
            return false;
        }

        CompositeStrategyDefinition definition;
        std::string load_error;
        if (!LoadCompositeStrategyDefinition(strategy_config.strategy_composite_config, &definition,
                                             &load_error)) {
            if (error != nullptr) {
                *error =
                    "failed to load composite config for timeframe subscription: " + load_error;
            }
            return false;
        }
        for (const auto& sub_strategy : definition.sub_strategies) {
            if (!sub_strategy.enabled) {
                continue;
            }
            const std::int32_t timeframe_minutes =
                sub_strategy.timeframe_minutes > 0 ? sub_strategy.timeframe_minutes : 1;
            if (timeframe_minutes <= 0) {
                if (error != nullptr) {
                    *error = "invalid timeframe_minutes in composite config for strategy_id `" +
                             sub_strategy.id + "`";
                }
                return false;
            }
            timeframe_set.insert(timeframe_minutes);
        }
    }

    if (timeframe_set.empty()) {
        timeframe_set.insert(1);
    }
    out_timeframes->assign(timeframe_set.begin(), timeframe_set.end());
    return true;
}

namespace {
class ReplayInputRange {
   public:
    bool Open(const BacktestCliSpec& spec, std::string* source, ReplayReport* report,
              std::string* error) {
        error_ = error;
        utc_ = spec.behavior_profile == "online_parity";
        input_utc_ = spec.input_timestamp_basis == "utc";
        if (!input_utc_ && spec.input_timestamp_basis != "legacy_exchange_local") {
            if (error) *error = "input_timestamp_basis must be utc or legacy_exchange_local";
            return false;
        }

        if (spec.engine_mode == "parquet" && spec.streaming) {
            cursor_ = std::make_unique<MergedReplayCursor>();
            if (!cursor_->Open(spec, error)) return false;
            instruments_ = cursor_->instruments();
            *source = "parquet";
        } else {
            if (!LoadTicksForSpec(spec, &materialized_, source, report, error)) return false;
            for (const auto& tick : materialized_) instruments_.insert(tick.instrument_id);
        }
        Advance();
        first_ = current_;
        return !failed_;
    }
    struct Iterator {
        ReplayInputRange* input;
        const ReplayTick& operator*() const { return input->current_; }
        Iterator& operator++() {
            input->Advance();
            return *this;
        }
        bool operator!=(const Iterator&) const { return input->has_; }
    };
    Iterator begin() { return {this}; }
    Iterator end() { return {this}; }
    bool empty() const { return !has_; }
    const ReplayTick& first() const { return first_; }
    const std::set<std::string>& instruments() const { return instruments_; }
    bool failed() const { return failed_; }
    void Finish(ReplayReport* report) const {
        if (!cursor_) return;
        const auto metrics = cursor_->metrics();
        report->scan_rows = metrics.scan_rows;
        report->scan_row_groups = metrics.scan_row_groups;
        report->io_bytes = metrics.io_bytes;
        report->early_stop_hit = metrics.early_stop_hit;
        report->buffered_input_rows_high_water = metrics.buffered_rows_high_water;
    }

   private:
    void Advance() {
        if (cursor_) {
            if (!cursor_->Next(&current_, &has_, error_)) {
                failed_ = true;
                has_ = false;
            }
        } else {
            has_ = offset_ < materialized_.size();
            if (has_) current_ = materialized_[offset_++];
        }
        if (has_) {
            constexpr EpochNanos offset = 8LL * 60LL * 60LL * detail::kNanosPerSecond;
            const auto local_encoded = current_.ts_ns + (input_utc_ ? offset : 0);
            current_.update_time = detail::UpdateTimeFromEpochNs(local_encoded);
            if (current_.action_day.empty())
                current_.action_day = detail::TradingDayFromEpochNs(local_encoded);
            if (utc_ != input_utc_) current_.ts_ns += utc_ ? -offset : offset;
        }
    }
    std::unique_ptr<MergedReplayCursor> cursor_;
    std::vector<ReplayTick> materialized_;
    std::set<std::string> instruments_;
    std::size_t offset_{0};
    ReplayTick current_, first_;
    bool has_{false}, failed_{false}, utc_{false}, input_utc_{false};
    std::string* error_{nullptr};
};
}  // namespace

bool RunBacktestSpec(const BacktestCliSpec& requested_spec, BacktestCliResult* out,
                     std::string* error) {
    if (out == nullptr) {
        if (error != nullptr) {
            *error = "result output is null";
        }
        return false;
    }

    BacktestCliSpec spec = requested_spec;
    ReplayInputRange ticks;
    std::string data_source;
    ReplayReport replay;

    const bool online_parity = spec.behavior_profile == "online_parity";
    if ((spec.behavior_profile != "research" && !online_parity) ||
        (online_parity &&
         (spec.rollover_mode != "flat_only" || spec.product_series_mode != "raw"))) {
        if (error) *error = "invalid behavior profile: online_parity requires flat_only/raw";
        return false;
    }
    RuntimeSemanticsConfig runtime_semantics;
    if (online_parity &&
        !LoadRuntimeSemanticsConfig(spec.online_runtime_config_path, &runtime_semantics, error))
        return false;
    if (online_parity && (runtime_semantics.dominant_contract_switch_mode != "flat_only" ||
                          runtime_semantics.execution_algo != "direct" ||
                          runtime_semantics.execution_mode != "direct" ||
                          !runtime_semantics.risk_rule_groups.empty())) {
        if (error)
            *error =
                "online_parity currently requires flat_only/direct runtime and default risk rules; "
                "requested custom execution or risk semantics are unsupported";
        return false;
    }
    if (online_parity && runtime_semantics.risk_sim_subaccount_enabled) {
        if (error)
            *error =
                "online_parity does not support enabled risk_sim_subaccount budgets; provide a "
                "disabled-budget runtime or explicit research profile";
        return false;
    }
    if (online_parity && runtime_semantics.risk_max_margin_to_equity_ratio > 0.0) {
        if (error)
            *error =
                "online_parity does not support broker-equity margin-ratio budgets; provide a "
                "runtime with risk_max_margin_to_equity_ratio disabled or use research profile";
        return false;
    }
    if (spec.initialization_policy != "cold_start") {
        if (error)
            *error = "unsupported initialization_policy; only flat cold_start is implemented";
        return false;
    }
    if (online_parity && spec.detector_config_path.empty()) {
        if (!detail::LoadMarketStateDetectorConfigFile(spec.online_runtime_config_path,
                                                       &spec.detector_config,
                                                       &spec.detector_config_by_product, error))
            return false;
    }
    std::int64_t runtime_gate_rejections = 0;
    std::int64_t warmup_rejected_opens = 0;
    if (online_parity && IsParquetProductChainSelection(spec.symbols)) {
        if (error)
            *error =
                "online_parity requires an explicit eligible instrument universe; stitched product "
                "main-chain data lacks candidate coverage";
        return false;
    }

    if (!ticks.Open(spec, &data_source, &replay, error)) {
        return false;
    }

    std::string register_error;
    if (!RegisterDemoLiveStrategy(&register_error)) {
        if (error != nullptr) {
            *error = "failed to register demo strategy: " + register_error;
        }
        return false;
    }
    if (!RegisterCompositeStrategy(&register_error)) {
        if (error != nullptr) {
            *error = "failed to register composite strategy: " + register_error;
        }
        return false;
    }

    std::vector<BacktestStrategyConfig> strategy_configs;
    if (!ResolveBacktestStrategyConfigs(spec, &strategy_configs, error)) {
        return false;
    }

    struct BacktestStrategyRuntime {
        BacktestStrategyConfig config;
        StrategyContext context;
        std::unique_ptr<ILiveStrategy> strategy;
        CompositeStrategy* composite{nullptr};
    };

    std::vector<BacktestStrategyRuntime> strategy_runtimes;
    strategy_runtimes.reserve(strategy_configs.size());

    auto initialize_strategy_runtime = [&](BacktestStrategyRuntime* runtime) -> bool {
        if (runtime == nullptr) {
            if (error != nullptr) {
                *error = "strategy runtime is null";
            }
            return false;
        }
        runtime->strategy = StrategyRegistry::Instance().Create(runtime->config.strategy_factory);
        if (runtime->strategy == nullptr) {
            if (error != nullptr) {
                *error = "strategy_factory not found: " + runtime->config.strategy_factory;
            }
            return false;
        }
        runtime->context.strategy_id = runtime->config.strategy_id;
        runtime->context.account_id = spec.account_id;
        runtime->context.metadata["run_type"] = "backtest";
        runtime->context.metadata["parameter_profile"] = spec.parameter_profile;
        runtime->context.metadata["behavior_profile"] = spec.behavior_profile;
        runtime->context.metadata["timestamp_basis"] =
            spec.behavior_profile == "online_parity" ? "utc" : "legacy_exchange_local";
        runtime->context.metadata["strategy_factory"] = runtime->config.strategy_factory;
        if (runtime->config.strategy_factory == "composite") {
            runtime->context.metadata["composite_config_path"] =
                runtime->config.strategy_composite_config;
        }
        try {
            runtime->strategy->Initialize(runtime->context);
        } catch (const std::exception& ex) {
            if (error != nullptr) {
                *error = std::string("strategy initialize failed: ") + ex.what();
            }
            return false;
        } catch (...) {
            if (error != nullptr) {
                *error = "strategy initialize failed: unknown exception";
            }
            return false;
        }
        runtime->composite = dynamic_cast<CompositeStrategy*>(runtime->strategy.get());
        return true;
    };

    for (const BacktestStrategyConfig& config : strategy_configs) {
        BacktestStrategyRuntime runtime;
        runtime.config = config;
        if (!initialize_strategy_runtime(&runtime)) {
            for (BacktestStrategyRuntime& initialized : strategy_runtimes) {
                if (initialized.strategy != nullptr) {
                    initialized.strategy->Shutdown();
                }
            }
            return false;
        }
        strategy_runtimes.push_back(std::move(runtime));
    }

    const auto has_composite_strategy = [&]() {
        return std::any_of(
            strategy_runtimes.begin(), strategy_runtimes.end(),
            [](const BacktestStrategyRuntime& runtime) { return runtime.composite != nullptr; });
    };
    if (spec.emit_sub_strategy_indicator_trace && !has_composite_strategy()) {
        if (error != nullptr) {
            *error =
                "emit_sub_strategy_indicator_trace requires strategy_factory=composite and a "
                "CompositeStrategy instance";
        }
        return false;
    }

    std::vector<std::int32_t> subscribed_timeframes;
    if (!ResolveSubscribedTimeframes(spec, &subscribed_timeframes, error)) {
        return false;
    }

    std::set<std::string> instrument_universe;

    EpochNanos virtual_now_ns = ticks.empty() ? 0 : ticks.first().ts_ns;
    DominantContractCoordinatorConfig coordinator_config;
    coordinator_config.min_lead_ratio = runtime_semantics.dominant_contract_min_lead_ratio;
    coordinator_config.min_lead_windows = runtime_semantics.dominant_contract_min_lead_windows;
    coordinator_config.min_hold_ms = runtime_semantics.dominant_contract_min_hold_ms;
    coordinator_config.max_tick_age_ms = runtime_semantics.dominant_contract_max_tick_age_ms;
    coordinator_config.min_warmup_bars = runtime_semantics.dominant_contract_warmup_bars;
    coordinator_config.require_complete_baseline =
        runtime_semantics.dominant_contract_require_complete_baseline;
    for (const auto& runtime : strategy_runtimes)
        coordinator_config.min_warmup_bars =
            std::max(coordinator_config.min_warmup_bars,
                     runtime.strategy->RequiredContractWarmupBars(ContractSwitchContext{}));
    DominantContractCoordinator contract_coordinator(coordinator_config);
    std::map<std::string, std::set<std::string>> eligible_by_product;
    if (online_parity) {
        const auto requested_universe = BuildParquetSymbolSelection(spec.symbols).instrument_ids;
        for (const auto& requested : requested_universe) {
            const auto canonical = CanonicalContractInstrumentId(requested);
            const bool covered = std::any_of(
                ticks.instruments().begin(), ticks.instruments().end(), [&](const auto& actual) {
                    return CanonicalContractInstrumentId(actual) == canonical;
                });
            if (!covered) {
                if (error)
                    *error = "online_parity candidate coverage missing requested instrument " +
                             requested;
                return false;
            }
        }
        for (const auto& instrument : ticks.instruments())
            eligible_by_product[detail::InstrumentSymbolPrefix(instrument)].insert(instrument);
        for (const auto& [product, eligible] : eligible_by_product) {
            if (!contract_coordinator.RegisterProduct(
                    product, ticks.empty() ? std::string{} : ticks.first().trading_day, "",
                    std::vector<std::string>(eligible.begin(), eligible.end()), virtual_now_ns,
                    error))
                return false;
            DominantContractBrokerState broker;
            broker.truth_complete = true;
            contract_coordinator.UpdateBrokerState(product, broker);
        }
    }

    std::map<std::string, PositionState> position_state;
    std::map<std::string, double> mark_price;
    std::map<std::string, std::int64_t> instrument_bars;
    std::map<std::string, std::int64_t> order_status_counts;
    std::vector<double> equity_points;
    std::vector<EquitySample> equity_history;
    std::map<std::string, EquitySample> latest_daily_equity_samples;
    std::vector<TradeRecord> trades;
    std::vector<OrderRecord> orders;
    std::vector<PositionSnapshot> position_history;
    std::int64_t trade_seq = 0;
    std::int64_t order_seq = 0;
    std::int64_t fill_record_seq = 0;
    std::int64_t order_record_seq = 0;
    double total_commission = 0.0;
    double latest_equity = spec.initial_equity;
    double used_margin_total = 0.0;
    double max_margin_used = 0.0;
    std::int64_t margin_clipped_orders = 0;
    std::int64_t margin_rejected_orders = 0;
    if (spec.deterministic_fills) {
        equity_points.push_back(spec.initial_equity);
        if (!ticks.empty()) {
            EquitySample seed;
            seed.ts_ns = ticks.first().ts_ns;
            seed.trading_day = detail::NormalizeTradingDay(ticks.first().trading_day);
            if (seed.trading_day.empty()) {
                seed.trading_day = detail::TradingDayFromEpochNs(seed.ts_ns);
            }
            seed.equity = spec.initial_equity;
            seed.position_value = 0.0;
            seed.market_regime = "kUnknown";
            equity_history.push_back(std::move(seed));
            latest_daily_equity_samples[equity_history.back().trading_day] = equity_history.back();
        }
    }

    ProductFeeBook product_fee_book;
    bool has_product_fee = false;
    std::string resolved_product_config_path;
    if (!detail::ResolveBacktestProductConfigPath(spec.product_config_path,
                                                  PrimaryStrategyMainConfigPath(spec),
                                                  &resolved_product_config_path, error)) {
        return false;
    }
    if (!resolved_product_config_path.empty()) {
        if (!LoadProductFeeConfig(resolved_product_config_path, &product_fee_book, error)) {
            return false;
        }
        has_product_fee = true;
    }

    auto runtime_matches_product = [](const BacktestStrategyRuntime& runtime,
                                      const std::string& product_id) {
        if (product_id.empty() || runtime.config.product_id.empty()) {
            return true;
        }
        return detail::ToLower(runtime.config.product_id) == detail::ToLower(product_id);
    };
    auto runtime_matches_instrument = [&](const BacktestStrategyRuntime& runtime,
                                          const std::string& instrument_id) {
        return runtime_matches_product(runtime, detail::InstrumentSymbolPrefix(instrument_id));
    };

    auto dispatch_order_event = [&](const OrderEvent& event) {
        for (BacktestStrategyRuntime& runtime : strategy_runtimes) {
            if (runtime.strategy != nullptr) {
                runtime.strategy->OnOrderEvent(event);
            }
        }
    };

    auto dispatch_state = [&](const StateSnapshot7D& state) {
        std::vector<SignalIntent> all_intents;
        if (online_parity && !contract_coordinator.CanDispatchToStrategy(state.instrument_id))
            return all_intents;
        for (BacktestStrategyRuntime& runtime : strategy_runtimes) {
            if (runtime.strategy == nullptr ||
                !runtime_matches_instrument(runtime, state.instrument_id)) {
                continue;
            }
            std::vector<SignalIntent> intents = runtime.strategy->OnState(state);
            for (auto& intent : intents) {
                intent.generated_ts_ns = virtual_now_ns;
                if (online_parity) {
                    intent.product_id = detail::InstrumentSymbolPrefix(intent.instrument_id);
                    intent.contract_generation =
                        contract_coordinator.GenerationForInstrument(intent.instrument_id)
                            .value_or(0);
                }
            }
            all_intents.insert(all_intents.end(), intents.begin(), intents.end());
        }
        return all_intents;
    };

    auto for_each_composite = [&](auto&& callback) {
        for (BacktestStrategyRuntime& runtime : strategy_runtimes) {
            if (runtime.composite != nullptr) {
                callback(runtime, *runtime.composite);
            }
        }
    };

    auto find_position_owner = [&](const std::string& instrument_id) {
        for (BacktestStrategyRuntime& runtime : strategy_runtimes) {
            if (runtime.composite == nullptr) {
                continue;
            }
            const std::string owner = runtime.composite->GetBacktestPositionOwner(instrument_id);
            if (!owner.empty()) {
                return owner;
            }
        }
        return std::string{};
    };

    auto apply_known_contract_multipliers_to = [&](CompositeStrategy* composite) {
        if (!has_product_fee || composite == nullptr) {
            return;
        }
        std::unordered_map<std::string, double> multipliers;
        if (!product_fee_book.ExportContractMultipliers(&multipliers)) {
            return;
        }
        for (const auto& [instrument_id, multiplier] : multipliers) {
            composite->SetBacktestContractMultiplier(instrument_id, multiplier);
        }
    };

    auto apply_known_contract_multipliers = [&]() {
        if (!has_product_fee) {
            return;
        }
        for_each_composite([&](BacktestStrategyRuntime& /*runtime*/, CompositeStrategy& composite) {
            apply_known_contract_multipliers_to(&composite);
        });
    };
    apply_known_contract_multipliers();

    RiskManagementConfig default_risk_management_config;
    std::unordered_map<std::string, RiskManagementConfig> risk_management_by_product;
    for (const BacktestStrategyConfig& strategy_config : strategy_configs) {
        if (strategy_config.product_id.empty()) {
            default_risk_management_config = strategy_config.risk_management;
            continue;
        }
        risk_management_by_product[detail::ToLower(strategy_config.product_id)] =
            strategy_config.risk_management;
        if (!default_risk_management_config.enabled) {
            default_risk_management_config = strategy_config.risk_management;
        }
    }

    auto calculate_risk_budget_r = [&](const SignalIntent& intent, double equity_before_fill) {
        const std::string product_id = detail::ToLower(detail::InstrumentSymbolPrefix(
            intent.instrument_id.empty() ? std::string{} : intent.instrument_id));
        const auto risk_it = risk_management_by_product.find(product_id);
        const RiskManagementConfig& risk_management_config =
            risk_it == risk_management_by_product.end() ? default_risk_management_config
                                                        : risk_it->second;
        if (!risk_management_config.enabled || intent.offset != OffsetFlag::kOpen) {
            return 0.0;
        }
        if (!std::isfinite(equity_before_fill) || equity_before_fill <= 0.0) {
            return 0.0;
        }
        const double pct_budget = equity_before_fill * risk_management_config.risk_per_trade_pct;
        if (!std::isfinite(pct_budget) || pct_budget <= 0.0) {
            return 0.0;
        }
        return std::min(pct_budget, risk_management_config.max_risk_per_trade);
    };

    const bool expiry_close_mode = spec.rollover_mode == "expiry_close";
    ContractExpiryCalendar contract_expiry_calendar;
    struct ExpiryCloseProductState {
        std::string product_symbol;
        std::vector<std::string> chain_contracts;
        std::size_t active_contract_index{0};
        std::unordered_set<std::string> retired_contracts;
    };
    std::map<std::string, ExpiryCloseProductState> expiry_close_products;
    if (expiry_close_mode) {
        if (!LoadContractExpiryCalendar(spec.contract_expiry_calendar_path,
                                        &contract_expiry_calendar, error)) {
            return false;
        }
        const ParquetSymbolSelection selection = BuildParquetSymbolSelection(spec.symbols);
        if (!selection.instrument_ids.empty() || selection.product_symbols.empty()) {
            if (error != nullptr) {
                *error = "rollover_mode=expiry_close requires product symbol selection";
            }
            return false;
        }
        for (const std::string& product : selection.product_symbols) {
            ExpiryCloseProductState state;
            state.product_symbol = product;
            expiry_close_products[product] = std::move(state);
        }

        for (const auto& instrument : ticks.instruments()) {
            const std::string product = detail::InstrumentSymbolPrefix(instrument);
            auto state_it = expiry_close_products.find(product);
            if (state_it == expiry_close_products.end()) {
                continue;
            }
            const std::string canonical = CanonicalContractInstrumentId(instrument);
            if (!state_it->second.retired_contracts.insert(canonical).second) {
                continue;
            }
            if (contract_expiry_calendar.Find(canonical) == nullptr) {
                if (error != nullptr) {
                    *error =
                        "missing contract expiry calendar entry for instrument_id: " + instrument;
                }
                return false;
            }
            state_it->second.chain_contracts.push_back(canonical);
        }
        for (auto& [product, state] : expiry_close_products) {
            state.retired_contracts.clear();
            if (state.chain_contracts.empty()) {
                if (error != nullptr) {
                    *error = "no contracts found for expiry_close product symbol: " + product;
                }
                return false;
            }
            std::sort(
                state.chain_contracts.begin(), state.chain_contracts.end(),
                [&](const std::string& left, const std::string& right) {
                    const ContractExpiryEntry* left_entry = contract_expiry_calendar.Find(left);
                    const ContractExpiryEntry* right_entry = contract_expiry_calendar.Find(right);
                    if (left_entry == nullptr || right_entry == nullptr) {
                        return left < right;
                    }
                    if (left_entry->last_trading_day != right_entry->last_trading_day) {
                        return left_entry->last_trading_day < right_entry->last_trading_day;
                    }
                    return left < right;
                });
        }
    }

    std::map<std::string, std::string> symbol_active_contract;
    std::vector<RolloverEvent> rollover_events;
    std::vector<RolloverAction> rollover_actions;
    double rollover_slippage_cost = 0.0;
    std::int64_t rollover_canceled_orders = 0;

    std::int64_t intents_processed = 0;
    std::int64_t order_events = 0;
    std::int64_t wal_records = 0;
    std::int64_t wal_seq = 1;

    std::ofstream wal_out;
    if (spec.deterministic_fills && !spec.wal_path.empty()) {
        std::filesystem::create_directories(std::filesystem::path(spec.wal_path).parent_path());
        wal_out.open(spec.wal_path, std::ios::out | std::ios::trunc);
        if (!wal_out.is_open()) {
            if (error != nullptr) {
                *error = "unable to open wal file: " + spec.wal_path;
            }
            return false;
        }
    }

    const detail::TraceOutputPaths indicator_trace_paths = detail::ResolveTraceOutputPaths(
        spec.indicator_trace_path, BuildDefaultIndicatorTraceBasePath(spec.run_id),
        spec.trace_output_format);
    std::string indicator_trace_path = indicator_trace_paths.primary_path;
    IndicatorTraceCsvWriter indicator_trace_csv_writer;
    IndicatorTraceParquetWriter indicator_trace_parquet_writer;
    const bool emit_indicator_trace_csv =
        spec.emit_indicator_trace && detail::TraceOutputWritesCsv(spec.trace_output_format);
    const bool emit_indicator_trace_parquet =
        spec.emit_indicator_trace && detail::TraceOutputWritesParquet(spec.trace_output_format);
    if (emit_indicator_trace_csv &&
        !indicator_trace_csv_writer.Open(indicator_trace_paths.csv_path, error)) {
        return false;
    }
    if (emit_indicator_trace_parquet &&
        !indicator_trace_parquet_writer.Open(indicator_trace_paths.parquet_path, error)) {
        return false;
    }

    const detail::TraceOutputPaths sub_strategy_indicator_trace_paths =
        detail::ResolveTraceOutputPaths(spec.sub_strategy_indicator_trace_path,
                                        BuildDefaultSubStrategyIndicatorTraceBasePath(spec.run_id),
                                        spec.trace_output_format);
    std::string sub_strategy_indicator_trace_path = sub_strategy_indicator_trace_paths.primary_path;
    SubStrategyIndicatorTraceCsvWriter sub_strategy_indicator_trace_csv_writer;
    SubStrategyIndicatorTraceParquetWriter sub_strategy_indicator_trace_parquet_writer;
    const bool emit_sub_strategy_indicator_trace_csv =
        spec.emit_sub_strategy_indicator_trace &&
        detail::TraceOutputWritesCsv(spec.trace_output_format);
    const bool emit_sub_strategy_indicator_trace_parquet =
        spec.emit_sub_strategy_indicator_trace &&
        detail::TraceOutputWritesParquet(spec.trace_output_format);
    if (emit_sub_strategy_indicator_trace_csv &&
        !sub_strategy_indicator_trace_csv_writer.Open(sub_strategy_indicator_trace_paths.csv_path,
                                                      error)) {
        return false;
    }
    if (emit_sub_strategy_indicator_trace_parquet &&
        !sub_strategy_indicator_trace_parquet_writer.Open(
            sub_strategy_indicator_trace_paths.parquet_path, error)) {
        return false;
    }

    const bool enable_rollover =
        !online_parity && spec.deterministic_fills &&
        (spec.engine_mode == "core_sim" ||
         (spec.engine_mode == "parquet" && IsParquetProductChainSelection(spec.symbols))) &&
        !expiry_close_mode;
    const bool enable_product_series_adjustment =
        spec.product_series_mode == "continuous_adjusted" && data_source == "parquet" &&
        IsParquetProductChainSelection(spec.symbols);
    const bool use_bar_aggregator = data_source == "csv" || data_source == "parquet";
    std::optional<BarAggregatorConfig> replay_bar_aggregator_config;
    std::unique_ptr<BarAggregator> replay_bar_aggregator;
    if (use_bar_aggregator) {
        std::string trading_sessions_config_path;
        if (!detail::ResolveTradingSessionsConfigPathForParquetBacktest(
                &trading_sessions_config_path, error)) {
            return false;
        }
        BarAggregatorConfig aggregator_config;
        aggregator_config.filter_non_trading_ticks = true;
        // Backtest mode keeps replay timestamp semantics consistent by preferring exchange time.
        aggregator_config.is_backtest_mode = !online_parity;
        if (online_parity)
            aggregator_config.allowed_lateness_ms =
                runtime_semantics.market_bar_allowed_lateness_ms;
        aggregator_config.trading_sessions_config_path = trading_sessions_config_path;
        aggregator_config.use_default_session_fallback = true;
        replay_bar_aggregator_config = aggregator_config;
        replay_bar_aggregator = std::make_unique<BarAggregator>(aggregator_config);
    }
    detail::ReplayTimeframeFanout timeframe_fanout(subscribed_timeframes, spec.detector_config,
                                                   spec.detector_config_by_product);
    MarketBarPipelineConfig pipeline_config;
    pipeline_config.bar_aggregator = *replay_bar_aggregator_config;
    pipeline_config.timeframes = subscribed_timeframes;
    pipeline_config.detector = spec.detector_config;
    pipeline_config.detector_by_product = spec.detector_config_by_product;
    if (enable_product_series_adjustment)
        pipeline_config.analysis_transform = std::make_shared<ProductSeriesAdjuster>(true);
    MarketBarPipeline market_pipeline(pipeline_config);
    TradingSessionCalendarConfig session_config;
    session_config.trading_sessions_config_path =
        pipeline_config.bar_aggregator.trading_sessions_config_path;
    TradingSessionCalendar session_calendar(session_config);
    auto compute_position_value = [&]() {
        double total = 0.0;
        for (const auto& [instrument_id, state] : position_state) {
            if (state.net_position == 0) {
                continue;
            }
            const auto mark_it = mark_price.find(instrument_id);
            const double last_price =
                mark_it != mark_price.end() ? mark_it->second : state.avg_open_price;
            total += std::fabs(static_cast<double>(state.net_position)) * last_price *
                     ResolveContractMultiplier(has_product_fee ? &product_fee_book : nullptr,
                                               instrument_id);
        }
        return total;
    };

    auto compute_current_equity = [&]() {
        latest_equity =
            ComputeTotalEquity(spec.initial_equity, position_state, mark_price, total_commission,
                               has_product_fee ? &product_fee_book : nullptr);
        return latest_equity;
    };

    auto upsert_latest_daily_equity_sample = [&](EpochNanos ts_ns, const std::string& trading_day,
                                                 double equity, double position_value,
                                                 const std::string& market_regime) {
        std::string normalized_day = detail::NormalizeTradingDay(trading_day);
        if (normalized_day.empty()) {
            normalized_day = detail::TradingDayFromEpochNs(ts_ns);
        }
        if (normalized_day.empty()) {
            return;
        }

        EquitySample& latest_sample = latest_daily_equity_samples[normalized_day];
        if (!latest_sample.trading_day.empty() && ts_ns < latest_sample.ts_ns) {
            return;
        }

        latest_sample.ts_ns = ts_ns;
        latest_sample.trading_day = normalized_day;
        latest_sample.equity = equity;
        latest_sample.position_value = position_value;
        if (!market_regime.empty()) {
            latest_sample.market_regime = market_regime;
        } else if (latest_sample.market_regime.empty()) {
            latest_sample.market_regime = "kUnknown";
        }
    };

    auto record_latest_daily_equity_for_tick = [&](const ReplayTick& tick) {
        const double current_equity = compute_current_equity();
        upsert_latest_daily_equity_sample(tick.ts_ns, tick.trading_day, current_equity,
                                          compute_position_value(), "");
    };

    auto record_position_snapshot = [&](const std::string& instrument_id, EpochNanos ts_ns) {
        if (!spec.emit_position_history) {
            return;
        }
        const auto state_it = position_state.find(instrument_id);
        if (state_it == position_state.end()) {
            return;
        }
        const PositionState& state = state_it->second;
        const auto mark_it = mark_price.find(instrument_id);
        const double last_price =
            mark_it != mark_price.end() ? mark_it->second : state.avg_open_price;

        PositionSnapshot snapshot;
        snapshot.timestamp_ns = ts_ns;
        snapshot.symbol = instrument_id;
        snapshot.net_position = state.net_position;
        snapshot.avg_price = state.avg_open_price;
        snapshot.unrealized_pnl =
            ComputeUnrealized(state.net_position, state.avg_open_price, last_price,
                              ResolveContractMultiplier(
                                  has_product_fee ? &product_fee_book : nullptr, instrument_id));
        position_history.push_back(std::move(snapshot));
    };

    auto handle_rollover = [&](const ReplayTick& tick) {
        const std::string symbol = detail::InstrumentSymbolPrefix(tick.instrument_id);
        if (symbol.empty()) {
            return;
        }
        const std::string rollover_trading_day = [&]() {
            const std::string normalized = detail::NormalizeTradingDay(tick.trading_day);
            if (!normalized.empty()) {
                return normalized;
            }
            return detail::TradingDayFromEpochNs(tick.ts_ns);
        }();
        const std::string rollover_action_day =
            detail::ResolveActionDay(rollover_trading_day, tick.action_day, tick.update_time);
        const std::string rollover_local_dt = detail::LocalDateTimeFromTradingDayAndUpdateTime(
            rollover_trading_day, rollover_action_day, tick.update_time, tick.ts_ns);

        const auto current_it = symbol_active_contract.find(symbol);
        if (current_it == symbol_active_contract.end()) {
            symbol_active_contract[symbol] = tick.instrument_id;
            return;
        }

        const std::string previous_contract = current_it->second;
        const std::string current_contract = tick.instrument_id;
        if (previous_contract == current_contract) {
            return;
        }

        PositionState& previous_state = position_state[previous_contract];
        const std::int32_t previous_position = std::abs(previous_state.net_position);
        if (previous_position == 0) {
            symbol_active_contract[symbol] = current_contract;
            return;
        }

        const std::int32_t canceled_orders = 0;
        rollover_canceled_orders += canceled_orders;

        std::string applied_mode = spec.rollover_mode;
        PositionState& next_state = position_state[current_contract];
        if (applied_mode == "carry" && next_state.net_position != 0) {
            applied_mode = "strict";
        }

        const std::string direction = previous_state.net_position > 0 ? "long" : "short";
        double from_price = tick.last_price;
        double to_price = tick.last_price;

        if (applied_mode == "strict") {
            const Side close_side = previous_state.net_position > 0 ? Side::kSell : Side::kBuy;
            const Side open_side = previous_state.net_position > 0 ? Side::kBuy : Side::kSell;

            const auto [close_price, close_slip] = ComputeRolloverPrice(
                close_side, tick.last_price, tick.bid_price_1, tick.ask_price_1,
                spec.rollover_price_mode, spec.rollover_slippage_bps);
            const auto [open_price, open_slip] =
                ComputeRolloverPrice(open_side, tick.last_price, tick.bid_price_1, tick.ask_price_1,
                                     spec.rollover_price_mode, spec.rollover_slippage_bps);
            from_price = close_price;
            to_price = open_price;

            const double prev_realized_before = previous_state.realized_pnl;
            const double next_realized_before = next_state.realized_pnl;
            ApplyTrade(&previous_state, close_side, previous_position, close_price,
                       ResolveContractMultiplier(has_product_fee ? &product_fee_book : nullptr,
                                                 previous_contract));
            const double equity_before_rollover_open = compute_current_equity();
            ApplyTrade(&next_state, open_side, previous_position, open_price,
                       ResolveContractMultiplier(has_product_fee ? &product_fee_book : nullptr,
                                                 current_contract));
            rollover_slippage_cost +=
                (close_slip + open_slip) * static_cast<double>(previous_position);
            const double close_realized_pnl = previous_state.realized_pnl - prev_realized_before;
            const double open_realized_pnl = next_state.realized_pnl - next_realized_before;

            if (spec.emit_orders) {
                OrderRecord close_order;
                close_order.order_seq = ++order_record_seq;
                close_order.order_id = "rollover-order-" + std::to_string(++order_seq);
                close_order.client_order_id = close_order.order_id;
                close_order.symbol = previous_contract;
                close_order.type = "Market";
                close_order.side = SideToTitleString(close_side);
                close_order.offset = "Close";
                close_order.price = close_price;
                close_order.volume = previous_position;
                close_order.status = "Accepted";
                close_order.filled_volume = 0;
                close_order.avg_fill_price = 0.0;
                close_order.created_at_ns = tick.ts_ns;
                close_order.created_at_dt_utc = detail::DateTimeFromEpochNs(tick.ts_ns);
                close_order.last_update_ns = tick.ts_ns;
                close_order.last_update_dt_utc = detail::DateTimeFromEpochNs(tick.ts_ns);
                close_order.trading_day = rollover_trading_day;
                close_order.action_day = rollover_action_day;
                close_order.update_time = tick.update_time;
                close_order.created_at_dt_local = rollover_local_dt;
                close_order.last_update_dt_local = rollover_local_dt;
                close_order.strategy_id = "rollover";
                orders.push_back(std::move(close_order));

                OrderRecord close_order_filled;
                close_order_filled.order_seq = ++order_record_seq;
                close_order_filled.order_id = "rollover-order-" + std::to_string(order_seq);
                close_order_filled.client_order_id = close_order_filled.order_id;
                close_order_filled.symbol = previous_contract;
                close_order_filled.type = "Market";
                close_order_filled.side = SideToTitleString(close_side);
                close_order_filled.offset = "Close";
                close_order_filled.price = close_price;
                close_order_filled.volume = previous_position;
                close_order_filled.status = "Filled";
                close_order_filled.filled_volume = previous_position;
                close_order_filled.avg_fill_price = close_price;
                close_order_filled.created_at_ns = tick.ts_ns;
                close_order_filled.created_at_dt_utc = detail::DateTimeFromEpochNs(tick.ts_ns);
                close_order_filled.last_update_ns = tick.ts_ns;
                close_order_filled.last_update_dt_utc = detail::DateTimeFromEpochNs(tick.ts_ns);
                close_order_filled.trading_day = rollover_trading_day;
                close_order_filled.action_day = rollover_action_day;
                close_order_filled.update_time = tick.update_time;
                close_order_filled.created_at_dt_local = rollover_local_dt;
                close_order_filled.last_update_dt_local = rollover_local_dt;
                close_order_filled.strategy_id = "rollover";
                orders.push_back(std::move(close_order_filled));

                OrderRecord open_order;
                open_order.order_seq = ++order_record_seq;
                open_order.order_id = "rollover-order-" + std::to_string(++order_seq);
                open_order.client_order_id = open_order.order_id;
                open_order.symbol = current_contract;
                open_order.type = "Market";
                open_order.side = SideToTitleString(open_side);
                open_order.offset = "Open";
                open_order.price = open_price;
                open_order.volume = previous_position;
                open_order.status = "Accepted";
                open_order.filled_volume = 0;
                open_order.avg_fill_price = 0.0;
                open_order.created_at_ns = tick.ts_ns;
                open_order.created_at_dt_utc = detail::DateTimeFromEpochNs(tick.ts_ns);
                open_order.last_update_ns = tick.ts_ns;
                open_order.last_update_dt_utc = detail::DateTimeFromEpochNs(tick.ts_ns);
                open_order.trading_day = rollover_trading_day;
                open_order.action_day = rollover_action_day;
                open_order.update_time = tick.update_time;
                open_order.created_at_dt_local = rollover_local_dt;
                open_order.last_update_dt_local = rollover_local_dt;
                open_order.strategy_id = "rollover";
                orders.push_back(std::move(open_order));

                OrderRecord open_order_filled;
                open_order_filled.order_seq = ++order_record_seq;
                open_order_filled.order_id = "rollover-order-" + std::to_string(order_seq);
                open_order_filled.client_order_id = open_order_filled.order_id;
                open_order_filled.symbol = current_contract;
                open_order_filled.type = "Market";
                open_order_filled.side = SideToTitleString(open_side);
                open_order_filled.offset = "Open";
                open_order_filled.price = open_price;
                open_order_filled.volume = previous_position;
                open_order_filled.status = "Filled";
                open_order_filled.filled_volume = previous_position;
                open_order_filled.avg_fill_price = open_price;
                open_order_filled.created_at_ns = tick.ts_ns;
                open_order_filled.created_at_dt_utc = detail::DateTimeFromEpochNs(tick.ts_ns);
                open_order_filled.last_update_ns = tick.ts_ns;
                open_order_filled.last_update_dt_utc = detail::DateTimeFromEpochNs(tick.ts_ns);
                open_order_filled.trading_day = rollover_trading_day;
                open_order_filled.action_day = rollover_action_day;
                open_order_filled.update_time = tick.update_time;
                open_order_filled.created_at_dt_local = rollover_local_dt;
                open_order_filled.last_update_dt_local = rollover_local_dt;
                open_order_filled.strategy_id = "rollover";
                orders.push_back(std::move(open_order_filled));
            }

            if (spec.emit_trades) {
                TradeRecord close_trade;
                close_trade.fill_seq = ++fill_record_seq;
                close_trade.trade_id = "rollover-trade-" + std::to_string(++trade_seq);
                close_trade.order_id = "rollover-order-close-" + std::to_string(trade_seq);
                close_trade.symbol = previous_contract;
                close_trade.exchange = "";
                close_trade.side = SideToTitleString(close_side);
                close_trade.offset = "Close";
                close_trade.volume = previous_position;
                close_trade.price = close_price;
                close_trade.timestamp_ns = tick.ts_ns;
                close_trade.timestamp_dt_utc = detail::DateTimeFromEpochNs(tick.ts_ns);
                close_trade.signal_ts_ns = tick.ts_ns;
                close_trade.trading_day = rollover_trading_day;
                close_trade.action_day = rollover_action_day;
                close_trade.update_time = tick.update_time;
                close_trade.timestamp_dt_local = rollover_local_dt;
                close_trade.signal_dt_local = rollover_local_dt;
                close_trade.commission = 0.0;
                close_trade.slippage = close_slip;
                close_trade.realized_pnl = close_realized_pnl;
                close_trade.strategy_id = "rollover";
                close_trade.signal_type = "rollover_close";
                close_trade.regime_at_entry = "rollover";
                trades.push_back(std::move(close_trade));

                TradeRecord open_trade;
                open_trade.fill_seq = ++fill_record_seq;
                open_trade.trade_id = "rollover-trade-" + std::to_string(++trade_seq);
                open_trade.order_id = "rollover-order-open-" + std::to_string(trade_seq);
                open_trade.symbol = current_contract;
                open_trade.exchange = "";
                open_trade.side = SideToTitleString(open_side);
                open_trade.offset = "Open";
                open_trade.volume = previous_position;
                open_trade.price = open_price;
                open_trade.timestamp_ns = tick.ts_ns;
                open_trade.timestamp_dt_utc = detail::DateTimeFromEpochNs(tick.ts_ns);
                open_trade.signal_ts_ns = tick.ts_ns;
                open_trade.trading_day = rollover_trading_day;
                open_trade.action_day = rollover_action_day;
                open_trade.update_time = tick.update_time;
                open_trade.timestamp_dt_local = rollover_local_dt;
                open_trade.signal_dt_local = rollover_local_dt;
                open_trade.commission = 0.0;
                open_trade.slippage = open_slip;
                open_trade.realized_pnl = open_realized_pnl;
                SignalIntent rollover_open_intent;
                rollover_open_intent.strategy_id = "rollover";
                rollover_open_intent.instrument_id = current_contract;
                rollover_open_intent.signal_type = SignalType::kOpen;
                rollover_open_intent.side = open_side;
                rollover_open_intent.offset = OffsetFlag::kOpen;
                rollover_open_intent.volume = previous_position;
                rollover_open_intent.limit_price = open_price;
                rollover_open_intent.ts_ns = tick.ts_ns;
                rollover_open_intent.trace_id =
                    "rollover-open-" + current_contract + "-" + std::to_string(tick.ts_ns);
                open_trade.risk_budget_r =
                    calculate_risk_budget_r(rollover_open_intent, equity_before_rollover_open);
                open_trade.strategy_id = "rollover";
                open_trade.signal_type = "rollover_open";
                open_trade.regime_at_entry = "rollover";
                trades.push_back(std::move(open_trade));
            }

            for_each_composite(
                [&](BacktestStrategyRuntime& /*runtime*/, CompositeStrategy& composite) {
                    composite.ApplyBacktestRollover(previous_contract, current_contract,
                                                    close_price, open_price, tick.ts_ns);
                });

            record_position_snapshot(previous_contract, tick.ts_ns);
            record_position_snapshot(current_contract, tick.ts_ns);

            RolloverAction close_action;
            close_action.symbol = symbol;
            close_action.action = "close";
            close_action.from_instrument = previous_contract;
            close_action.to_instrument = current_contract;
            close_action.position = previous_position;
            close_action.side = SideToString(close_side);
            close_action.price = close_price;
            close_action.mode = applied_mode;
            close_action.price_mode = spec.rollover_price_mode;
            close_action.slippage_bps = spec.rollover_slippage_bps;
            close_action.canceled_orders = canceled_orders;
            close_action.ts_ns = tick.ts_ns;

            RolloverAction open_action = close_action;
            open_action.action = "open";
            open_action.side = SideToString(open_side);
            open_action.price = open_price;

            rollover_actions.push_back(close_action);
            rollover_actions.push_back(open_action);

            if (wal_out.is_open()) {
                const std::string close_line =
                    "{\"seq\":" + std::to_string(wal_seq++) +
                    ",\"kind\":\"rollover\",\"action\":\"close\",\"symbol\":\"" +
                    JsonEscape(symbol) + "\",\"from_instrument\":\"" +
                    JsonEscape(previous_contract) + "\",\"to_instrument\":\"" +
                    JsonEscape(current_contract) +
                    "\",\"position\":" + std::to_string(previous_position) + "}";
                const std::string open_line =
                    "{\"seq\":" + std::to_string(wal_seq++) +
                    ",\"kind\":\"rollover\",\"action\":\"open\",\"symbol\":\"" +
                    JsonEscape(symbol) + "\",\"from_instrument\":\"" +
                    JsonEscape(previous_contract) + "\",\"to_instrument\":\"" +
                    JsonEscape(current_contract) +
                    "\",\"position\":" + std::to_string(previous_position) + "}";
                if (detail::WriteWalLine(&wal_out, close_line)) {
                    ++wal_records;
                }
                if (detail::WriteWalLine(&wal_out, open_line)) {
                    ++wal_records;
                }
            }
        } else {
            const double carry_price = mark_price.count(previous_contract) != 0
                                           ? mark_price[previous_contract]
                                           : tick.last_price;
            from_price = carry_price;
            to_price = carry_price;

            next_state.net_position = previous_state.net_position;
            next_state.avg_open_price = previous_state.avg_open_price;
            next_state.realized_pnl += previous_state.realized_pnl;

            previous_state.net_position = 0;
            previous_state.avg_open_price = 0.0;
            previous_state.realized_pnl = 0.0;

            record_position_snapshot(previous_contract, tick.ts_ns);
            record_position_snapshot(current_contract, tick.ts_ns);

            RolloverAction action;
            action.symbol = symbol;
            action.action = "carry";
            action.from_instrument = previous_contract;
            action.to_instrument = current_contract;
            action.position = previous_position;
            action.side = "";
            action.price = carry_price;
            action.mode = applied_mode;
            action.price_mode = spec.rollover_price_mode;
            action.slippage_bps = spec.rollover_slippage_bps;
            action.canceled_orders = canceled_orders;
            action.ts_ns = tick.ts_ns;
            rollover_actions.push_back(action);

            if (wal_out.is_open()) {
                const std::string line =
                    "{\"seq\":" + std::to_string(wal_seq++) +
                    ",\"kind\":\"rollover\",\"action\":\"carry\",\"symbol\":\"" +
                    JsonEscape(symbol) + "\",\"from_instrument\":\"" +
                    JsonEscape(previous_contract) + "\",\"to_instrument\":\"" +
                    JsonEscape(current_contract) +
                    "\",\"position\":" + std::to_string(previous_position) + "}";
                if (detail::WriteWalLine(&wal_out, line)) {
                    ++wal_records;
                }
            }
        }

        RolloverEvent event;
        event.symbol = symbol;
        event.from_instrument = previous_contract;
        event.to_instrument = current_contract;
        event.mode = applied_mode;
        event.position = previous_position;
        event.direction = direction;
        event.from_price = from_price;
        event.to_price = to_price;
        event.canceled_orders = canceled_orders;
        event.price_mode = spec.rollover_price_mode;
        event.slippage_bps = spec.rollover_slippage_bps;
        event.ts_ns = tick.ts_ns;
        rollover_events.push_back(event);

        symbol_active_contract[symbol] = current_contract;
    };

    std::unordered_map<std::string, detail::ReplayBarTickContext> replay_bar_contexts;
    std::unordered_map<std::string, std::vector<detail::PendingBarIntent>>
        pending_bar_intents_by_instrument;
    std::unordered_map<std::string, std::vector<detail::PendingBarIntent>>
        pending_tick_intents_by_instrument;

    std::map<std::string, MarketSnapshot> latest_runtime_market;
    auto simulated_orders = std::make_shared<OrderManager>();
    auto simulated_risk = CreateRiskManager(simulated_orders, nullptr);
    RiskManagerConfig simulated_risk_config;
    simulated_risk_config.default_max_order_volume =
        runtime_semantics.risk_default_max_order_volume;
    simulated_risk_config.default_max_order_notional =
        runtime_semantics.risk_default_max_order_notional;
    simulated_risk_config.default_max_position_notional =
        runtime_semantics.risk_default_max_position_notional;
    simulated_risk_config.default_max_order_rate = runtime_semantics.order_insert_rate_per_sec;
    simulated_risk_config.default_max_cancel_rate = runtime_semantics.order_cancel_rate_per_sec;
    simulated_risk_config.rule_file_path = runtime_semantics.risk_rule_file_path;
    simulated_risk_config.enable_dynamic_reload = false;
    simulated_risk_config.monotonic_now = [&] {
        return std::chrono::steady_clock::time_point(std::chrono::nanoseconds(virtual_now_ns));
    };
    if (online_parity && !simulated_risk->Initialize(simulated_risk_config)) {
        if (error) *error = "unable to initialize shared replay risk manager";
        return false;
    }
    TokenBucket simulated_insert_flow(runtime_semantics.order_insert_rate_per_sec,
                                      runtime_semantics.order_insert_rate_per_sec,
                                      simulated_risk_config.monotonic_now);
    TokenBucket simulated_cancel_flow(runtime_semantics.order_cancel_rate_per_sec,
                                      runtime_semantics.order_cancel_rate_per_sec,
                                      simulated_risk_config.monotonic_now);
    std::uint64_t simulated_order_seq = 0;
    std::string runtime_risk_error;
    const auto risk_context = [&](const SignalIntent& intent, double price) {
        OrderContext context;
        context.account_id = spec.account_id;
        context.strategy_id = intent.strategy_id;
        context.instrument_id = intent.instrument_id;
        const auto position = position_state.find(intent.instrument_id);
        if (position != position_state.end())
            context.current_position = position->second.net_position;
        context.current_margin = used_margin_total;
        const double equity =
            ComputeTotalEquity(spec.initial_equity, position_state, mark_price, total_commission,
                               has_product_fee ? &product_fee_book : nullptr);
        context.available_fund = equity - used_margin_total;
        context.today_pnl = equity - spec.initial_equity + total_commission;
        context.today_commission = total_commission;
        // ExecutionEngine uses the submitted price as its current-price reference.
        context.current_price = price;
        context.contract_multiplier = ResolveContractMultiplier(
            has_product_fee ? &product_fee_book : nullptr, intent.instrument_id);
        return context;
    };
    const auto finish_simulated_order = [&](const SignalIntent& intent, OrderStatus status,
                                            double price = 0.0, int volume = 0) {
        if (!online_parity || intent.trace_id.empty()) return;
        const auto order = simulated_orders->GetOrder(intent.trace_id);
        if (!order ||
            (order->status == OrderStatus::kFilled || order->status == OrderStatus::kCanceled ||
             order->status == OrderStatus::kRejected))
            return;
        if (status == OrderStatus::kCanceled) {
            const auto decision =
                simulated_risk->CheckCancel(intent.trace_id, risk_context(intent, order->price));
            if (!decision.allowed || !simulated_cancel_flow.TryAcquire()) {
                runtime_risk_error =
                    "online_parity cannot replay delayed cancel/backpressure without broker "
                    "acknowledgement history";
                return;
            }
        }
        OrderEvent event;
        event.order_ref = intent.trace_id;
        event.exchange_ts_ns = virtual_now_ns;
        event.account_id = spec.account_id;
        event.strategy_id = intent.strategy_id;
        event.instrument_id = intent.instrument_id;
        event.client_order_id = intent.trace_id;
        event.side = intent.side;
        event.offset = intent.offset;
        event.status = status;
        event.total_volume = intent.volume;
        event.filled_volume = volume;
        event.avg_fill_price = price;
        event.ts_ns = virtual_now_ns;
        Order updated;
        std::string order_error;
        if (!simulated_orders->OnOrderEvent(event, &updated, &order_error))
            runtime_risk_error = "replay order projection failed: " + order_error;
    };
    auto admit_runtime_intent = [&](SignalIntent& intent) {
        if (!online_parity) return true;
        const auto market = latest_runtime_market.find(intent.instrument_id);
        const auto age = virtual_now_ns - intent.generated_ts_ns;
        const bool opening = intent.offset == OffsetFlag::kOpen;
        std::size_t active = 0;
        for (const auto& item : pending_bar_intents_by_instrument) active += item.second.size();
        for (const auto& item : pending_tick_intents_by_instrument) active += item.second.size();
        const bool rejected =
            intent.volume <= 0 || age < 0 ||
            (runtime_semantics.max_signal_age_ms > 0 &&
             age > runtime_semantics.max_signal_age_ms * detail::kNanosPerMillisecond) ||
            (runtime_semantics.risk_default_max_active_orders > 0 &&
             active >=
                 static_cast<std::size_t>(runtime_semantics.risk_default_max_active_orders)) ||
            (opening &&
             (market == latest_runtime_market.end() ||
              (runtime_semantics.max_market_tick_age_ms > 0 &&
               virtual_now_ns - market->second.recv_ts_ns >
                   runtime_semantics.max_market_tick_age_ms * detail::kNanosPerMillisecond) ||
              market_pipeline.IsOpeningSuppressed(intent.instrument_id)));
        if (rejected) {
            ++runtime_gate_rejections;
            return false;
        }
        if (market == latest_runtime_market.end()) {
            ++runtime_gate_rejections;
            return false;
        }
        double submitted_price = intent.limit_price;
        if (runtime_semantics.execution_price_mode == "marketable_limit")
            submitted_price =
                intent.side == Side::kBuy ? market->second.ask_price_1 : market->second.bid_price_1;
        if (submitted_price <= 0) submitted_price = market->second.last_price;
        if (!std::isfinite(submitted_price) || submitted_price <= 0) {
            ++runtime_gate_rejections;
            return false;
        }
        for (const auto& rule : simulated_risk->GetActiveRules()) {
            if (!rule.enabled || rule.threshold <= 0 ||
                (!rule.account_id.empty() && rule.account_id != spec.account_id) ||
                (!rule.strategy_id.empty() && rule.strategy_id != intent.strategy_id) ||
                (!rule.instrument_id.empty() && rule.instrument_id != intent.instrument_id))
                continue;
            if (rule.type == RiskRuleType::DAILY_LOSS_LIMIT ||
                rule.type == RiskRuleType::MAX_TOTAL_POSITION ||
                rule.type == RiskRuleType::MAX_LEVERAGE || !rule.time_range.empty()) {
                runtime_risk_error = "online_parity cannot model active risk rule: " + rule.rule_id;
                return false;
            }
        }
        OrderIntent order;
        order.account_id = spec.account_id;
        order.strategy_id = intent.strategy_id;
        order.instrument_id = intent.instrument_id;
        order.side = intent.side;
        order.offset = intent.offset;
        order.price = submitted_price;
        order.volume = intent.volume;
        order.ts_ns = virtual_now_ns;
        order.client_order_id = "replay-admission-" + std::to_string(++simulated_order_seq);
        const auto decision =
            simulated_risk->CheckOrder(order, risk_context(intent, submitted_price));
        if (!decision.allowed) {
            ++runtime_gate_rejections;
            return false;
        }
        // A live gateway may wait under backpressure. No invented wait/drop semantics in parity.
        if (!simulated_insert_flow.TryAcquire()) {
            runtime_risk_error =
                "online_parity cannot replay gateway insert backpressure without arrival/ack "
                "history";
            return false;
        }
        intent.trace_id = order.client_order_id;
        simulated_orders->CreateOrder(order);
        finish_simulated_order(intent, OrderStatus::kAccepted);
        return runtime_risk_error.empty();
    };

    auto trading_day_from_tick = [&](const ReplayTick& tick) {
        const std::string normalized = detail::NormalizeTradingDay(tick.trading_day);
        if (!normalized.empty()) {
            return normalized;
        }
        return detail::TradingDayFromEpochNs(tick.ts_ns);
    };

    auto action_day_from_tick = [&](const ReplayTick& tick) {
        return detail::ResolveActionDay(trading_day_from_tick(tick), tick.action_day,
                                        tick.update_time);
    };

    auto local_dt_from_tick = [&](const ReplayTick& tick) {
        return detail::LocalDateTimeFromTradingDayAndUpdateTime(
            trading_day_from_tick(tick), action_day_from_tick(tick), tick.update_time, tick.ts_ns);
    };

    auto exchange_from_tick = [&](const ReplayTick& tick) {
        if (!tick.exchange_id.empty()) {
            return tick.exchange_id;
        }
        return detail::SharedSessionResolver().InferExchangeId(tick.instrument_id);
    };

    auto signal_timing_from_tick = [&](const ReplayTick& tick) {
        detail::ReplaySignalTiming timing;
        timing.signal_ts_ns = tick.ts_ns;
        timing.signal_trading_day = trading_day_from_tick(tick);
        timing.signal_action_day = action_day_from_tick(tick);
        timing.signal_update_time = tick.update_time;
        timing.signal_update_millisec = tick.update_millisec;
        return timing;
    };

    auto populate_order_timing = [&](OrderRecord* order, const ReplayTick& execution_tick) {
        if (order == nullptr) {
            return;
        }
        order->trading_day = trading_day_from_tick(execution_tick);
        order->action_day = action_day_from_tick(execution_tick);
        order->update_time = execution_tick.update_time;
        order->created_at_dt_local = local_dt_from_tick(execution_tick);
        order->last_update_dt_local = order->created_at_dt_local;
    };

    auto populate_trade_timing = [&](TradeRecord* trade,
                                     const detail::ReplaySignalTiming& signal_timing,
                                     const ReplayTick& execution_tick) {
        if (trade == nullptr) {
            return;
        }
        const EpochNanos signal_ts_ns =
            signal_timing.signal_ts_ns > 0 ? signal_timing.signal_ts_ns : execution_tick.ts_ns;
        const std::string signal_trading_day =
            detail::NormalizeTradingDay(signal_timing.signal_trading_day).empty()
                ? trading_day_from_tick(execution_tick)
                : detail::NormalizeTradingDay(signal_timing.signal_trading_day);
        const std::string signal_action_day = detail::ResolveActionDay(
            signal_trading_day, signal_timing.signal_action_day, signal_timing.signal_update_time);
        trade->signal_ts_ns = signal_ts_ns;
        trade->signal_dt_local = detail::LocalDateTimeFromTradingDayAndUpdateTime(
            signal_trading_day, signal_action_day,
            signal_timing.signal_update_time.empty() ? execution_tick.update_time
                                                     : signal_timing.signal_update_time,
            signal_ts_ns);
        trade->trading_day = trading_day_from_tick(execution_tick);
        trade->action_day = action_day_from_tick(execution_tick);
        trade->update_time = execution_tick.update_time;
        trade->timestamp_dt_local = local_dt_from_tick(execution_tick);
    };

    auto reinitialize_backtest_strategy = [&](const std::string& product_id) -> bool {
        bool reinitialized_any = false;
        for (BacktestStrategyRuntime& runtime : strategy_runtimes) {
            if (!runtime_matches_product(runtime, product_id)) {
                continue;
            }
            if (runtime.strategy != nullptr) {
                runtime.strategy->Shutdown();
            }
            if (!initialize_strategy_runtime(&runtime)) {
                if (error != nullptr && error->empty()) {
                    *error = "strategy initialize failed during expiry_close reset";
                }
                return false;
            }
            apply_known_contract_multipliers_to(runtime.composite);
            reinitialized_any = true;
        }
        if (!reinitialized_any && error != nullptr) {
            *error = "no strategy matched expiry_close product reset: " + product_id;
            return false;
        }
        return true;
    };

    auto process_deterministic_intent =
        [&](const SignalIntent& intent, const detail::ReplaySignalTiming& signal_timing,
            const ReplayTick& execution_tick, double fill_price, MarketRegime market_regime,
            const std::string& signal_type_label) -> bool {
        ScopeExit complete_projection(
            [&] { finish_simulated_order(intent, OrderStatus::kCanceled); });
        if (online_parity) {
            const auto position = position_state.find(intent.instrument_id);
            const int close_volume =
                position == position_state.end() ? 0 : std::abs(position->second.net_position);
            if (!contract_coordinator.ValidateSignal(intent, close_volume).allowed) {
                ++runtime_gate_rejections;
                return true;
            }
            const bool opening = intent.offset == OffsetFlag::kOpen;
            if (opening && runtime_semantics.session_gate_enabled &&
                !session_calendar
                     .EvaluateOrderTime(execution_tick.exchange_id, intent.instrument_id,
                                        execution_tick.trading_day, execution_tick.ts_ns,
                                        runtime_semantics.open_session_end_guard_ms, true)
                     .open_allowed) {
                ++runtime_gate_rejections;
                return true;
            }
            if (runtime_semantics.execution_price_mode == "marketable_limit") {
                const double executable = intent.side == Side::kBuy ? execution_tick.ask_price_1
                                                                    : execution_tick.bid_price_1;
                if (!std::isfinite(executable) || executable <= 0) {
                    ++runtime_gate_rejections;
                    return true;
                }
                fill_price = executable;
            } else if (runtime_semantics.execution_price_mode == "signal_limit") {
                if (intent.limit_price > 0) fill_price = intent.limit_price;
            } else {
                if (error) *error = "unsupported runtime execution_price_mode";
                return false;
            }
            const auto signal_age_ns = execution_tick.ts_ns - intent.generated_ts_ns;
            if ((runtime_semantics.cancel_after_ms > 0 &&
                 signal_age_ns > static_cast<EpochNanos>(runtime_semantics.cancel_after_ms) *
                                     detail::kNanosPerMillisecond) ||
                (opening && market_pipeline.IsOpeningSuppressed(intent.instrument_id))) {
                ++runtime_gate_rejections;
                finish_simulated_order(intent, OrderStatus::kCanceled);
                return true;
            }
        }
        ++intents_processed;
        const EpochNanos signal_ts_ns =
            signal_timing.signal_ts_ns > 0 ? signal_timing.signal_ts_ns : execution_tick.ts_ns;
        const std::string client_order_id =
            intent.trace_id.empty() ? ("det-order-" + std::to_string(intents_processed) + "-" +
                                       intent.instrument_id + "-" + std::to_string(signal_ts_ns))
                                    : intent.trace_id;
        const std::string order_id = "order-" + std::to_string(++order_seq);
        const ProductFeeEntry* fee_entry = nullptr;
        if (has_product_fee) {
            fee_entry = product_fee_book.Find(intent.instrument_id);
            if (fee_entry == nullptr) {
                if (error != nullptr) {
                    *error =
                        "missing product fee config for instrument_id: " + intent.instrument_id;
                }
                return false;
            }
        }

        std::int32_t exec_volume = intent.volume;
        if (fee_entry != nullptr && intent.offset == OffsetFlag::kOpen && exec_volume > 0) {
            const double account_equity =
                ComputeTotalEquity(spec.initial_equity, position_state, mark_price,
                                   total_commission, has_product_fee ? &product_fee_book : nullptr);
            const double available_margin = std::max(0.0, account_equity - used_margin_total);
            const double per_lot_margin =
                ProductFeeBook::ComputePerLotMargin(*fee_entry, intent.side, fill_price);
            std::int32_t max_openable = 0;
            if (std::isfinite(per_lot_margin) && per_lot_margin > 0.0) {
                const double raw_openable = std::floor(available_margin / per_lot_margin);
                if (std::isfinite(raw_openable) && raw_openable > 0.0) {
                    max_openable = static_cast<std::int32_t>(
                        std::min<double>(raw_openable, std::numeric_limits<std::int32_t>::max()));
                }
            }
            if (max_openable < exec_volume) {
                ++margin_clipped_orders;
                exec_volume = std::max<std::int32_t>(0, max_openable);
            }
            if (exec_volume <= 0) {
                ++margin_rejected_orders;
                ++order_events;
                order_status_counts["REJECTED"] += 1;
                if (spec.emit_orders) {
                    OrderRecord rejected_order;
                    rejected_order.order_seq = ++order_record_seq;
                    rejected_order.order_id = order_id;
                    rejected_order.client_order_id = client_order_id;
                    rejected_order.symbol = intent.instrument_id;
                    rejected_order.type = "Market";
                    rejected_order.side = SideToTitleString(intent.side);
                    rejected_order.offset = OffsetFlagToTitleString(intent.offset);
                    rejected_order.price = fill_price;
                    rejected_order.volume = intent.volume;
                    rejected_order.status = "Rejected";
                    rejected_order.filled_volume = 0;
                    rejected_order.avg_fill_price = 0.0;
                    rejected_order.created_at_ns = execution_tick.ts_ns;
                    rejected_order.created_at_dt_utc =
                        detail::DateTimeFromEpochNs(execution_tick.ts_ns);
                    rejected_order.last_update_ns = execution_tick.ts_ns;
                    rejected_order.last_update_dt_utc =
                        detail::DateTimeFromEpochNs(execution_tick.ts_ns);
                    rejected_order.strategy_id = intent.strategy_id;
                    rejected_order.cancel_reason = "margin_rejected";
                    populate_order_timing(&rejected_order, execution_tick);
                    orders.push_back(std::move(rejected_order));
                }
                finish_simulated_order(intent, OrderStatus::kRejected);
                return true;
            }
        }

        if (exec_volume <= 0) {
            finish_simulated_order(intent, OrderStatus::kRejected);
            return true;
        }

        const double equity_before_fill =
            ComputeTotalEquity(spec.initial_equity, position_state, mark_price, total_commission,
                               has_product_fee ? &product_fee_book : nullptr);
        PositionState& pnl_state = position_state[intent.instrument_id];
        const double realized_before = pnl_state.realized_pnl;
        ApplyTrade(&pnl_state, intent.side, exec_volume, fill_price,
                   ResolveContractMultiplier(fee_entry));
        const double realized_delta = pnl_state.realized_pnl - realized_before;

        double commission = 0.0;
        if (fee_entry != nullptr) {
            commission = ProductFeeBook::ComputeCommission(*fee_entry, intent.offset, exec_volume,
                                                           fill_price);
        }
        total_commission += commission;
        if (has_product_fee) {
            used_margin_total =
                ComputeTotalMarginUsed(position_state, mark_price, product_fee_book);
            max_margin_used = std::max(max_margin_used, used_margin_total);
        }

        order_events += 2;
        order_status_counts["ACCEPTED"] += 1;
        order_status_counts["FILLED"] += 1;

        OrderEvent filled_event;
        filled_event.account_id = spec.account_id;
        filled_event.strategy_id = intent.strategy_id;
        filled_event.client_order_id = client_order_id;
        filled_event.exchange_order_id = order_id;
        filled_event.instrument_id = intent.instrument_id;
        filled_event.side = intent.side;
        filled_event.offset = intent.offset;
        filled_event.status = OrderStatus::kFilled;
        filled_event.total_volume = exec_volume;
        filled_event.filled_volume = exec_volume;
        filled_event.avg_fill_price = fill_price;
        filled_event.ts_ns = execution_tick.ts_ns;
        finish_simulated_order(intent, OrderStatus::kFilled, fill_price, exec_volume);
        dispatch_order_event(filled_event);

        if (spec.emit_orders) {
            OrderRecord accepted_order;
            accepted_order.order_seq = ++order_record_seq;
            accepted_order.order_id = order_id;
            accepted_order.client_order_id = client_order_id;
            accepted_order.symbol = intent.instrument_id;
            accepted_order.type = "Market";
            accepted_order.side = SideToTitleString(intent.side);
            accepted_order.offset = OffsetFlagToTitleString(intent.offset);
            accepted_order.price = fill_price;
            accepted_order.volume = exec_volume;
            accepted_order.status = "Accepted";
            accepted_order.filled_volume = 0;
            accepted_order.avg_fill_price = 0.0;
            accepted_order.created_at_ns = execution_tick.ts_ns;
            accepted_order.created_at_dt_utc = detail::DateTimeFromEpochNs(execution_tick.ts_ns);
            accepted_order.last_update_ns = execution_tick.ts_ns;
            accepted_order.last_update_dt_utc = detail::DateTimeFromEpochNs(execution_tick.ts_ns);
            accepted_order.strategy_id = intent.strategy_id;
            populate_order_timing(&accepted_order, execution_tick);
            orders.push_back(std::move(accepted_order));

            OrderRecord filled_order;
            filled_order.order_seq = ++order_record_seq;
            filled_order.order_id = order_id;
            filled_order.client_order_id = client_order_id;
            filled_order.symbol = intent.instrument_id;
            filled_order.type = "Market";
            filled_order.side = SideToTitleString(intent.side);
            filled_order.offset = OffsetFlagToTitleString(intent.offset);
            filled_order.price = fill_price;
            filled_order.volume = exec_volume;
            filled_order.status = "Filled";
            filled_order.filled_volume = exec_volume;
            filled_order.avg_fill_price = fill_price;
            filled_order.created_at_ns = execution_tick.ts_ns;
            filled_order.created_at_dt_utc = detail::DateTimeFromEpochNs(execution_tick.ts_ns);
            filled_order.last_update_ns = execution_tick.ts_ns;
            filled_order.last_update_dt_utc = detail::DateTimeFromEpochNs(execution_tick.ts_ns);
            filled_order.strategy_id = intent.strategy_id;
            populate_order_timing(&filled_order, execution_tick);
            orders.push_back(std::move(filled_order));
        }
        if (spec.emit_trades) {
            double slippage = 0.0;
            if (intent.limit_price > 0.0) {
                slippage = intent.side == Side::kBuy ? fill_price - intent.limit_price
                                                     : intent.limit_price - fill_price;
            }

            TradeRecord trade;
            trade.fill_seq = ++fill_record_seq;
            trade.trade_id = "trade-" + std::to_string(++trade_seq);
            trade.order_id = order_id;
            trade.symbol = intent.instrument_id;
            trade.exchange = "";
            trade.side = SideToTitleString(intent.side);
            trade.offset = OffsetFlagToTitleString(intent.offset);
            trade.volume = exec_volume;
            trade.price = fill_price;
            trade.timestamp_ns = execution_tick.ts_ns;
            trade.timestamp_dt_utc = detail::DateTimeFromEpochNs(execution_tick.ts_ns);
            trade.commission = commission;
            trade.slippage = slippage;
            trade.realized_pnl = realized_delta;
            trade.risk_budget_r = calculate_risk_budget_r(intent, equity_before_fill);
            trade.strategy_id = intent.strategy_id;
            trade.signal_type = signal_type_label.empty() ? SignalTypeToString(intent.signal_type)
                                                          : signal_type_label;
            trade.regime_at_entry = MarketRegimeToString(market_regime);
            trade.exchange = exchange_from_tick(execution_tick);
            populate_trade_timing(&trade, signal_timing, execution_tick);
            trades.push_back(std::move(trade));
        }

        record_position_snapshot(intent.instrument_id, execution_tick.ts_ns);

        if (wal_out.is_open()) {
            const std::string accepted_line =
                "{\"seq\":" + std::to_string(wal_seq++) +
                ",\"kind\":\"order\",\"status\":1,\"instrument_id\":\"" +
                JsonEscape(intent.instrument_id) + "\",\"trace_id\":\"" +
                JsonEscape(client_order_id) +
                "\",\"ts_ns\":" + std::to_string(execution_tick.ts_ns) + "}";
            const std::string filled_line =
                "{\"seq\":" + std::to_string(wal_seq++) +
                ",\"kind\":\"trade\",\"status\":3,\"instrument_id\":\"" +
                JsonEscape(intent.instrument_id) + "\",\"trace_id\":\"" +
                JsonEscape(client_order_id) +
                "\",\"ts_ns\":" + std::to_string(execution_tick.ts_ns) +
                ",\"price\":" + detail::FormatDouble(fill_price) +
                ",\"filled_volume\":" + std::to_string(exec_volume) + "}";
            if (detail::WriteWalLine(&wal_out, accepted_line)) {
                ++wal_records;
            }
            if (detail::WriteWalLine(&wal_out, filled_line)) {
                ++wal_records;
            }
        }
        return true;
    };

    auto erase_replay_contexts_for_instrument = [&](const std::string& instrument_id) {
        if (instrument_id.empty()) {
            return;
        }
        const std::string prefix = instrument_id + "|";
        for (auto it = replay_bar_contexts.begin(); it != replay_bar_contexts.end();) {
            if (it->first.rfind(prefix, 0) == 0) {
                it = replay_bar_contexts.erase(it);
            } else {
                ++it;
            }
        }
    };

    auto reset_expiry_close_session = [&](const std::string& instrument_id,
                                          const std::string& product_id) -> bool {
        erase_replay_contexts_for_instrument(instrument_id);
        pending_bar_intents_by_instrument.erase(instrument_id);
        pending_tick_intents_by_instrument.erase(instrument_id);
        timeframe_fanout.ResetInstrument(instrument_id);
        market_pipeline.ResetInstrument(instrument_id, false);
        if (replay_bar_aggregator != nullptr) {
            replay_bar_aggregator->ResetInstrument(instrument_id);
        }
        return reinitialize_backtest_strategy(product_id);
    };

    auto current_expiry_active_contract = [&](const std::string& product_id) -> std::string {
        const auto state_it = expiry_close_products.find(product_id);
        if (!expiry_close_mode || state_it == expiry_close_products.end() ||
            state_it->second.active_contract_index >= state_it->second.chain_contracts.size()) {
            return "";
        }
        return state_it->second.chain_contracts[state_it->second.active_contract_index];
    };

    auto process_expiry_close_tick = [&](const ReplayTick& execution_tick,
                                         ExpiryCloseProductState* expiry_state) -> bool {
        if (expiry_state == nullptr) {
            if (error != nullptr) {
                *error = "expiry_close product state is null";
            }
            return false;
        }
        const std::string canonical_instrument =
            CanonicalContractInstrumentId(execution_tick.instrument_id);
        const ContractExpiryEntry* expiry_entry =
            contract_expiry_calendar.Find(canonical_instrument);
        if (expiry_entry == nullptr) {
            if (error != nullptr) {
                *error = "missing contract expiry calendar entry for instrument_id: " +
                         execution_tick.instrument_id;
            }
            return false;
        }

        PositionState& state = position_state[execution_tick.instrument_id];
        const std::int32_t close_volume = std::abs(state.net_position);
        if (close_volume > 0) {
            const Side close_side = state.net_position > 0 ? Side::kSell : Side::kBuy;
            const auto [close_price, close_slip] = ComputeRolloverPrice(
                close_side, execution_tick.last_price, execution_tick.bid_price_1,
                execution_tick.ask_price_1, spec.rollover_price_mode, spec.rollover_slippage_bps);

            SignalIntent expiry_intent;
            expiry_intent.strategy_id = find_position_owner(execution_tick.instrument_id);
            if (expiry_intent.strategy_id.empty()) {
                expiry_intent.strategy_id = "expiry";
            }
            expiry_intent.instrument_id = execution_tick.instrument_id;
            expiry_intent.signal_type = SignalType::kForceClose;
            expiry_intent.side = close_side;
            expiry_intent.offset = OffsetFlag::kClose;
            expiry_intent.volume = close_volume;
            expiry_intent.ts_ns = execution_tick.ts_ns;
            expiry_intent.trace_id =
                "expiry-close-" + canonical_instrument + "-" + std::to_string(execution_tick.ts_ns);

            mark_price[execution_tick.instrument_id] = execution_tick.last_price;
            rollover_slippage_cost += close_slip * static_cast<double>(close_volume);
            if (!process_deterministic_intent(
                    expiry_intent, signal_timing_from_tick(execution_tick), execution_tick,
                    close_price, MarketRegime::kUnknown, "expiry_close")) {
                return false;
            }
        }

        expiry_state->retired_contracts.insert(canonical_instrument);
        if (expiry_state->active_contract_index < expiry_state->chain_contracts.size() &&
            expiry_state->chain_contracts[expiry_state->active_contract_index] ==
                canonical_instrument) {
            ++expiry_state->active_contract_index;
        }
        return reset_expiry_close_session(execution_tick.instrument_id,
                                          expiry_state->product_symbol);
    };

    auto process_tick_intents = [&](const std::vector<SignalIntent>& intents,
                                    const ReplayTick& execution_tick, double fill_price,
                                    MarketRegime market_regime) -> bool {
        const detail::ReplaySignalTiming timing = signal_timing_from_tick(execution_tick);
        for (SignalIntent intent : intents) {
            if (!admit_runtime_intent(intent)) continue;
            if (online_parity || intent.signal_type == SignalType::kStopLoss ||
                intent.signal_type == SignalType::kTakeProfit) {
                auto& pending_list = pending_tick_intents_by_instrument[intent.instrument_id];
                const bool has_pending_close =
                    std::any_of(pending_list.begin(), pending_list.end(),
                                [](const detail::PendingBarIntent& pending) {
                                    return pending.intent.offset == OffsetFlag::kClose;
                                });
                if (has_pending_close) {
                    finish_simulated_order(intent, OrderStatus::kCanceled);
                    continue;
                }
                detail::PendingBarIntent pending;
                pending.intent = intent;
                pending.timing = timing;
                pending.market_regime = market_regime;
                pending_list.push_back(std::move(pending));
                continue;
            }
            if (!process_deterministic_intent(intent, timing, execution_tick, fill_price,
                                              market_regime, "")) {
                return false;
            }
        }
        return true;
    };

    auto process_pending_tick_intents = [&](const ReplayTick& execution_tick) -> bool {
        const auto pending_it =
            pending_tick_intents_by_instrument.find(execution_tick.instrument_id);
        if (pending_it == pending_tick_intents_by_instrument.end()) {
            return true;
        }

        std::vector<detail::PendingBarIntent> remaining;
        remaining.reserve(pending_it->second.size());
        for (const detail::PendingBarIntent& pending : pending_it->second) {
            if (execution_tick.ts_ns <=
                std::max(pending.timing.signal_ts_ns, pending.intent.generated_ts_ns)) {
                remaining.push_back(pending);
                continue;
            }
            if (pending.intent.offset == OffsetFlag::kClose) {
                const auto pos_it = position_state.find(pending.intent.instrument_id);
                if (pos_it == position_state.end() || pos_it->second.net_position == 0) {
                    finish_simulated_order(pending.intent, OrderStatus::kCanceled);
                    continue;
                }
            }
            if (!process_deterministic_intent(pending.intent, pending.timing, execution_tick,
                                              execution_tick.last_price, pending.market_regime,
                                              "")) {
                return false;
            }
        }

        if (remaining.empty()) {
            pending_tick_intents_by_instrument.erase(pending_it);
        } else {
            pending_it->second = std::move(remaining);
        }
        return true;
    };

    auto enqueue_bar_intents = [&](const std::vector<SignalIntent>& intents,
                                   const ReplayTick& signal_tick, MarketRegime market_regime) {
        if (intents.empty()) {
            return;
        }
        const detail::ReplaySignalTiming timing = signal_timing_from_tick(signal_tick);
        const std::string session_key = detail::ResolveSessionKey(
            signal_tick.exchange_id, signal_tick.instrument_id, signal_tick.update_time);
        for (SignalIntent intent : intents) {
            if (!admit_runtime_intent(intent)) continue;
            detail::PendingBarIntent pending;
            pending.intent = intent;
            pending.timing = timing;
            pending.signal_session_key = session_key;
            pending.market_regime = market_regime;
            pending_bar_intents_by_instrument[intent.instrument_id].push_back(std::move(pending));
        }
    };

    auto process_pending_bar_intents = [&](const ReplayTick& execution_tick) -> bool {
        const auto pending_it =
            pending_bar_intents_by_instrument.find(execution_tick.instrument_id);
        if (pending_it == pending_bar_intents_by_instrument.end()) {
            return true;
        }

        const std::string current_session_key = detail::ResolveSessionKey(
            execution_tick.exchange_id, execution_tick.instrument_id, execution_tick.update_time);
        std::vector<detail::PendingBarIntent> remaining;
        remaining.reserve(pending_it->second.size());
        for (const detail::PendingBarIntent& pending : pending_it->second) {
            if (!pending.signal_session_key.empty() && !current_session_key.empty() &&
                pending.signal_session_key != current_session_key) {
                finish_simulated_order(pending.intent, OrderStatus::kCanceled);
                continue;
            }
            if (execution_tick.ts_ns <=
                std::max(pending.timing.signal_ts_ns, pending.intent.generated_ts_ns)) {
                remaining.push_back(pending);
                continue;
            }
            if (!process_deterministic_intent(pending.intent, pending.timing, execution_tick,
                                              execution_tick.last_price, pending.market_regime,
                                              "")) {
                return false;
            }
        }

        if (remaining.empty()) {
            pending_bar_intents_by_instrument.erase(pending_it);
        } else {
            pending_it->second = std::move(remaining);
        }
        return true;
    };

    auto process_closed_bar = [&](const detail::ReplayAggregatedBar& aggregated) -> bool {
        const auto& last = aggregated.context.last_tick;
        const auto& bar = aggregated.bar;
        const StateSnapshot7D& state = aggregated.state;
        ++replay.bars_emitted;
        instrument_bars[last.instrument_id] += 1;
        mark_price[last.instrument_id] = bar.close;
        if (spec.emit_indicator_trace) {
            IndicatorTraceRow row;
            row.instrument_id = state.instrument_id;
            row.ts_ns = state.ts_ns;
            if (state.timeframe_minutes > 1) {
                row.dt_utc = detail::ReplayMinuteToDisplayDateTime(bar.minute);
            }
            if (row.dt_utc.empty()) {
                row.dt_utc = detail::TickDateTimeFromTickFields(last.trading_day, last.update_time,
                                                                last.update_millisec, state.ts_ns);
            }
            row.timeframe_minutes = state.timeframe_minutes;
            row.bar_open = state.bar_open;
            row.bar_high = state.bar_high;
            row.bar_low = state.bar_low;
            row.bar_close = state.bar_close;
            row.bar_volume = state.bar_volume;
            row.analysis_bar_open = state.analysis_bar_open;
            row.analysis_bar_high = state.analysis_bar_high;
            row.analysis_bar_low = state.analysis_bar_low;
            row.analysis_bar_close = state.analysis_bar_close;
            row.analysis_price_offset = state.analysis_price_offset;
            row.kama = aggregated.kama;
            row.atr = aggregated.atr;
            row.adx = aggregated.adx;
            row.er = aggregated.er;
            row.market_regime = state.market_regime;
            if (std::isfinite(state.market_state_adx)) {
                row.market_state_adx = state.market_state_adx;
            }
            if (std::isfinite(state.market_state_kama_er)) {
                row.market_state_kama_er = state.market_state_kama_er;
            }
            if (std::isfinite(state.market_state_atr_ratio)) {
                row.market_state_atr_ratio = state.market_state_atr_ratio;
            }
            row.market_state_bars_seen = state.market_state_bars_seen;
            row.market_state_decision_reason = state.market_state_decision_reason;
            if (emit_indicator_trace_csv && !indicator_trace_csv_writer.Append(row, error)) {
                return false;
            }
            if (emit_indicator_trace_parquet &&
                !indicator_trace_parquet_writer.Append(row, error)) {
                return false;
            }
        }

        if (has_product_fee) {
            used_margin_total =
                ComputeTotalMarginUsed(position_state, mark_price, product_fee_book);
            max_margin_used = std::max(max_margin_used, used_margin_total);
            for_each_composite(
                [&](BacktestStrategyRuntime& /*runtime*/, CompositeStrategy& composite) {
                    const ProductFeeEntry* entry = product_fee_book.Find(state.instrument_id);
                    if (entry != nullptr && entry->contract_multiplier > 0.0) {
                        composite.SetBacktestContractMultiplier(state.instrument_id,
                                                                entry->contract_multiplier);
                    }
                });
        }
        if (has_composite_strategy()) {
            const double equity =
                ComputeTotalEquity(spec.initial_equity, position_state, mark_price,
                                   total_commission, has_product_fee ? &product_fee_book : nullptr);
            for_each_composite(
                [&](BacktestStrategyRuntime& /*runtime*/, CompositeStrategy& composite) {
                    composite.SetBacktestAccountSnapshot(equity, equity - spec.initial_equity);
                });
        }
        std::vector<SignalIntent> intents;
        if (aggregated.strategy_eligible && bar.strategy_eligible && bar.is_complete) {
            intents = dispatch_state(state);
            if (online_parity) {
                const auto longest =
                    *std::max_element(subscribed_timeframes.begin(), subscribed_timeframes.end());
                if (state.timeframe_minutes == longest)
                    contract_coordinator.RecordWarmupBar(
                        detail::InstrumentSymbolPrefix(state.instrument_id), state.instrument_id,
                        bar.minute, virtual_now_ns);
                intents.erase(
                    std::remove_if(
                        intents.begin(), intents.end(),
                        [&](const SignalIntent& intent) {
                            const auto position = position_state.find(intent.instrument_id);
                            const int close_volume = position == position_state.end()
                                                         ? 0
                                                         : std::abs(position->second.net_position);
                            const bool rejected =
                                !contract_coordinator.ValidateSignal(intent, close_volume).allowed;
                            if (rejected && intent.offset == OffsetFlag::kOpen)
                                ++warmup_rejected_opens;
                            return rejected;
                        }),
                    intents.end());
            }
        }

        if (spec.emit_sub_strategy_indicator_trace && aggregated.strategy_eligible &&
            bar.strategy_eligible && bar.is_complete) {
            std::vector<CompositeAtomicTraceRow> atomic_trace_rows;
            for (BacktestStrategyRuntime& runtime : strategy_runtimes) {
                if (runtime.composite == nullptr ||
                    !runtime_matches_instrument(runtime, state.instrument_id)) {
                    continue;
                }
                const std::vector<CompositeAtomicTraceRow> runtime_rows =
                    runtime.composite->CollectAtomicIndicatorTrace();
                atomic_trace_rows.insert(atomic_trace_rows.end(), runtime_rows.begin(),
                                         runtime_rows.end());
            }
            for (const auto& atomic_trace : atomic_trace_rows) {
                SubStrategyIndicatorTraceRow row;
                row.instrument_id = state.instrument_id;
                row.ts_ns = state.ts_ns;
                row.trading_day = detail::NormalizeTradingDay(bar.trading_day);
                if (row.trading_day.empty()) {
                    row.trading_day = detail::NormalizeTradingDay(last.trading_day);
                }
                if (row.trading_day.empty()) {
                    row.trading_day = detail::TradingDayFromEpochNs(state.ts_ns);
                }
                row.action_day =
                    detail::ResolveActionDay(row.trading_day, bar.action_day, last.update_time);
                if (state.timeframe_minutes > 1) {
                    row.dt_utc = detail::ReplayMinuteToDisplayDateTime(bar.minute, row.action_day);
                }
                if (row.dt_utc.empty()) {
                    row.dt_utc = detail::TickDateTimeFromTickFields(
                        row.action_day, last.update_time, last.update_millisec, state.ts_ns);
                }
                row.timeframe_minutes = state.timeframe_minutes;
                row.strategy_id = atomic_trace.strategy_id;
                row.strategy_type = atomic_trace.strategy_type;
                row.bar_open = state.bar_open;
                row.bar_high = state.bar_high;
                row.bar_low = state.bar_low;
                row.bar_close = state.bar_close;
                row.bar_volume = state.bar_volume;
                row.analysis_bar_open = state.analysis_bar_open;
                row.analysis_bar_high = state.analysis_bar_high;
                row.analysis_bar_low = state.analysis_bar_low;
                row.analysis_bar_close = state.analysis_bar_close;
                row.analysis_price_offset = state.analysis_price_offset;
                row.kama = atomic_trace.kama;
                row.atr = atomic_trace.atr;
                row.adx = atomic_trace.adx;
                row.er = atomic_trace.er;
                row.stop_loss_price = atomic_trace.stop_loss_price;
                row.take_profit_price = atomic_trace.take_profit_price;
                row.market_regime = state.market_regime;
                if (std::isfinite(state.market_state_adx)) {
                    row.market_state_adx = state.market_state_adx;
                }
                if (std::isfinite(state.market_state_kama_er)) {
                    row.market_state_kama_er = state.market_state_kama_er;
                }
                if (std::isfinite(state.market_state_atr_ratio)) {
                    row.market_state_atr_ratio = state.market_state_atr_ratio;
                }
                row.market_state_bars_seen = state.market_state_bars_seen;
                row.market_state_decision_reason = state.market_state_decision_reason;
                if (emit_sub_strategy_indicator_trace_csv &&
                    !sub_strategy_indicator_trace_csv_writer.Append(row, error)) {
                    return false;
                }
                if (emit_sub_strategy_indicator_trace_parquet &&
                    !sub_strategy_indicator_trace_parquet_writer.Append(row, error)) {
                    return false;
                }
            }
        }

        replay.intents_emitted += static_cast<std::int64_t>(intents.size());

        if (spec.deterministic_fills) {
            enqueue_bar_intents(intents, last, state.market_regime);

            const double current_equity = compute_current_equity();
            equity_points.push_back(current_equity);
            EquitySample sample;
            sample.ts_ns = state.ts_ns;
            sample.trading_day = detail::NormalizeTradingDay(last.trading_day);
            if (sample.trading_day.empty()) {
                sample.trading_day = detail::TradingDayFromEpochNs(state.ts_ns);
            }
            sample.equity = current_equity;
            sample.position_value = compute_position_value();
            sample.market_regime = MarketRegimeToString(state.market_regime);
            upsert_latest_daily_equity_sample(sample.ts_ns, sample.trading_day, sample.equity,
                                              sample.position_value, sample.market_regime);
            equity_history.push_back(std::move(sample));
        }
        return true;
    };

    auto process_pipeline_result = [&](const MarketBarPipelineResult& batch) -> bool {
        for (const auto& bar : batch.one_minute_bars) {
            detail::ReplayBarTickContext context;
            if (!detail::ConsumeReplayBarTickContext(bar, &replay_bar_contexts, &context, error))
                return false;
            timeframe_fanout.ObserveOneMinuteContext(bar, context);
        }
        const auto aggregated_bars = timeframe_fanout.Adapt(batch.timeframe_emissions);
        if (aggregated_bars.size() != batch.timeframe_emissions.size()) {
            if (error) *error = "canonical market emission is missing its replay timing context";
            return false;
        }
        for (const auto& aggregated : aggregated_bars) {
            if (!process_closed_bar(aggregated)) return false;
        }
        return true;
    };

    std::unordered_map<std::string, EpochNanos> last_selection_evaluation_by_product;
    auto coordinate_contract = [&](const MarketSnapshot& snapshot) -> bool {
        if (!online_parity) return true;
        const std::string product = detail::InstrumentSymbolPrefix(snapshot.instrument_id);
        contract_coordinator.UpdateBaselineSnapshot(product, snapshot);
        contract_coordinator.UpdateLiveSnapshot(snapshot);
        DominantContractBrokerState broker;
        broker.truth_complete = true;
        for (const auto& [instrument, state] : position_state) {
            if (detail::InstrumentSymbolPrefix(instrument) != product) continue;
            broker.position += std::abs(state.net_position);
            if (state.net_position != 0) broker.held_instrument_ids.insert(instrument);
        }
        const auto count_pending = [&](const auto& pending_by_instrument) {
            for (const auto& [instrument, pending] : pending_by_instrument) {
                if (detail::InstrumentSymbolPrefix(instrument) != product) continue;
                for (const auto& order : pending) {
                    if (order.intent.offset == OffsetFlag::kOpen)
                        ++broker.active_open_orders;
                    else
                        ++broker.active_close_orders;
                }
            }
        };
        count_pending(pending_bar_intents_by_instrument);
        count_pending(pending_tick_intents_by_instrument);
        contract_coordinator.UpdateBrokerState(product, broker);
        auto& last_selection_evaluation_ns = last_selection_evaluation_by_product[product];
        const auto before = contract_coordinator.GetStatus(product);
        if (before.phase != DominantContractPhase::kSelecting &&
            (runtime_semantics.dominant_contract_recheck_interval_ms <= 0 ||
             virtual_now_ns - last_selection_evaluation_ns <
                 static_cast<EpochNanos>(runtime_semantics.dominant_contract_recheck_interval_ms) *
                     detail::kNanosPerMillisecond))
            return true;
        last_selection_evaluation_ns = virtual_now_ns;
        const auto decision = contract_coordinator.Evaluate(
            product, virtual_now_ns,
            session_calendar.IsOrderTime(snapshot.exchange_id, snapshot.instrument_id,
                                         session_calendar.TimeOfDay(virtual_now_ns)));
        if (decision.action == DominantContractAction::kCancelOpenOrders ||
            decision.action == DominantContractAction::kEnterPendingFlat) {
            const auto cancel_opens = [&](auto* pending_by_instrument) {
                for (auto& [instrument, pending] : *pending_by_instrument) {
                    if (detail::InstrumentSymbolPrefix(instrument) != product) continue;
                    pending.erase(std::remove_if(pending.begin(), pending.end(),
                                                 [&](const auto& order) {
                                                     if (order.intent.offset == OffsetFlag::kOpen) {
                                                         finish_simulated_order(
                                                             order.intent, OrderStatus::kCanceled);
                                                         return true;
                                                     }
                                                     return false;
                                                 }),
                                  pending.end());
                }
            };
            cancel_opens(&pending_bar_intents_by_instrument);
            cancel_opens(&pending_tick_intents_by_instrument);
            return true;
        }
        const bool initial = decision.action == DominantContractAction::kSelectInitial;
        if (!initial && decision.action != DominantContractAction::kBeginSwitch) return true;
        std::uint64_t generation = decision.generation;
        if (initial) {
            if (!contract_coordinator.CommitInitialSelection(
                    product, decision.candidate_instrument_id, virtual_now_ns, error))
                return false;
        } else if (!contract_coordinator.BeginSwitch(product, decision.previous_instrument_id,
                                                     decision.candidate_instrument_id,
                                                     virtual_now_ns, &generation, error))
            return false;
        if (!initial) {
            RolloverEvent event;
            event.symbol = product;
            event.from_instrument = decision.previous_instrument_id;
            event.to_instrument = decision.candidate_instrument_id;
            event.mode = "flat_only";
            event.ts_ns = virtual_now_ns;
            rollover_events.push_back(std::move(event));
        }
        const ContractSwitchContext context{product, decision.previous_instrument_id,
                                            decision.candidate_instrument_id, generation};
        std::vector<StateSnapshot7D> warmup;
        for (const auto timeframe : subscribed_timeframes) {
            auto states = market_pipeline.RecentCompleteStates(
                decision.candidate_instrument_id, timeframe,
                static_cast<std::size_t>(coordinator_config.min_warmup_bars));
            warmup.insert(warmup.end(), states.begin(), states.end());
        }
        std::sort(warmup.begin(), warmup.end(), [](const auto& left, const auto& right) {
            return std::tie(left.ts_ns, left.timeframe_minutes) <
                   std::tie(right.ts_ns, right.timeframe_minutes);
        });
        for (auto& runtime : strategy_runtimes) {
            if (!runtime_matches_product(runtime, product)) continue;
            if (!runtime.strategy->ResetForContractSwitch(context, error)) return false;
            for (const auto& state : warmup) (void)runtime.strategy->OnState(state);
        }
        const auto longest =
            *std::max_element(subscribed_timeframes.begin(), subscribed_timeframes.end());
        int complete_count = 0;
        for (const auto& state : warmup)
            if (state.timeframe_minutes == longest) ++complete_count;
        if (initial) {
            for (const auto& state : warmup) {
                if (state.timeframe_minutes == longest)
                    contract_coordinator.RecordWarmupBar(
                        product, state.instrument_id, std::to_string(state.ts_ns), virtual_now_ns);
            }
        } else if (!contract_coordinator.CommitSwitch(
                       product, decision.candidate_instrument_id, complete_count,
                       coordinator_config.min_warmup_bars, virtual_now_ns, error))
            return false;
        return true;
    };

    if (!use_bar_aggregator || replay_bar_aggregator == nullptr) {
        if (error != nullptr) {
            *error = "replay bar aggregator is not initialized";
        }
        return false;
    }

    const EpochNanos virtual_poll_ns =
        static_cast<EpochNanos>(online_parity ? runtime_semantics.market_bar_poll_interval_ms
                                              : 100) *
        detail::kNanosPerMillisecond;
    EpochNanos next_poll_ns = virtual_now_ns + virtual_poll_ns;
    auto advance_virtual_clock = [&](EpochNanos target_ns) -> bool {
        while (next_poll_ns < target_ns) {
            virtual_now_ns = next_poll_ns;
            if (!process_pipeline_result(market_pipeline.AdvanceWatermark(virtual_now_ns)))
                return false;
            for (const auto& [instrument, market] : latest_runtime_market) {
                (void)instrument;
                if (!coordinate_contract(market)) return false;
            }
            for (auto& runtime : strategy_runtimes) {
                auto intents = runtime.strategy->OnTimer(virtual_now_ns);
                for (auto& intent : intents) {
                    const auto market = latest_runtime_market.find(intent.instrument_id);
                    if (market == latest_runtime_market.end()) continue;
                    intent.generated_ts_ns = virtual_now_ns;
                    if (online_parity) {
                        intent.product_id = detail::InstrumentSymbolPrefix(intent.instrument_id);
                        intent.contract_generation =
                            contract_coordinator.GenerationForInstrument(intent.instrument_id)
                                .value_or(0);
                    }
                    ReplayTick timing_tick;
                    timing_tick.instrument_id = intent.instrument_id;
                    timing_tick.trading_day = market->second.trading_day;
                    timing_tick.update_time = market->second.update_time;
                    timing_tick.ts_ns = virtual_now_ns;
                    enqueue_bar_intents({intent}, timing_tick, MarketRegime::kUnknown);
                }
            }
            next_poll_ns += virtual_poll_ns;
        }
        return true;
    };

    for (const ReplayTick& tick : ticks) {
        if (!advance_virtual_clock(tick.ts_ns)) return false;
        virtual_now_ns = std::max(virtual_now_ns, tick.ts_ns);
        if (replay.ticks_read == 0) {
            replay.first_instrument = tick.instrument_id;
            replay.first_ts_ns = tick.ts_ns;
        }
        replay.last_instrument = tick.instrument_id;
        replay.last_ts_ns = tick.ts_ns;
        ++replay.ticks_read;
        instrument_universe.insert(tick.instrument_id);

        MarketSnapshot snapshot;
        snapshot.instrument_id = tick.instrument_id;
        snapshot.exchange_id = !tick.exchange_id.empty()
                                   ? tick.exchange_id
                                   : replay_bar_aggregator->InferExchangeId(tick.instrument_id);
        snapshot.trading_day = detail::NormalizeTradingDay(tick.trading_day);
        if (snapshot.trading_day.empty()) {
            snapshot.trading_day = detail::TradingDayFromEpochNs(tick.ts_ns);
        }
        snapshot.action_day =
            detail::ResolveActionDay(snapshot.trading_day, tick.action_day, tick.update_time);
        snapshot.update_time = tick.update_time;
        snapshot.update_millisec = tick.update_millisec;
        snapshot.last_price = tick.last_price;
        snapshot.bid_price_1 = tick.bid_price_1;
        snapshot.ask_price_1 = tick.ask_price_1;
        snapshot.bid_volume_1 = tick.bid_volume_1;
        snapshot.ask_volume_1 = tick.ask_volume_1;
        snapshot.volume = tick.volume;
        snapshot.open_interest = tick.open_interest;
        snapshot.exchange_ts_ns = tick.ts_ns;
        snapshot.recv_ts_ns = tick.ts_ns;
        latest_runtime_market[tick.instrument_id] = snapshot;

        if (!replay_bar_aggregator->ShouldProcessSnapshot(snapshot)) {
            if (!process_pipeline_result(market_pipeline.OnTick(snapshot))) return false;
            continue;
        }

        if (expiry_close_mode) {
            const std::string canonical_instrument =
                CanonicalContractInstrumentId(tick.instrument_id);
            const std::string product_id = detail::InstrumentSymbolPrefix(tick.instrument_id);
            auto expiry_state_it = expiry_close_products.find(product_id);
            if (expiry_state_it == expiry_close_products.end()) {
                continue;
            }
            if (expiry_state_it->second.retired_contracts.find(canonical_instrument) !=
                expiry_state_it->second.retired_contracts.end()) {
                continue;
            }

            const std::string active_contract = current_expiry_active_contract(product_id);
            if (active_contract.empty()) {
                continue;
            }
            if (canonical_instrument != active_contract) {
                continue;
            }

            const ContractExpiryEntry* expiry_entry =
                contract_expiry_calendar.Find(active_contract);
            if (expiry_entry == nullptr) {
                if (error != nullptr) {
                    *error = "missing contract expiry calendar entry for instrument_id: " +
                             tick.instrument_id;
                }
                return false;
            }

            const std::string tick_trading_day = trading_day_from_tick(tick);
            if (tick_trading_day == expiry_entry->last_trading_day &&
                detail::IsDaySessionTick(tick.update_time)) {
                if (!process_expiry_close_tick(tick, &expiry_state_it->second)) {
                    return false;
                }
                if (spec.deterministic_fills) {
                    record_latest_daily_equity_for_tick(tick);
                }
                continue;
            }
        }

        if (enable_rollover) {
            handle_rollover(tick);
        }

        mark_price[tick.instrument_id] = tick.last_price;
        if (spec.deterministic_fills) {
            if (!process_pending_bar_intents(tick)) {
                return false;
            }
            if (!process_pending_tick_intents(tick)) {
                return false;
            }
        }

        if (!coordinate_contract(snapshot)) return false;
        for_each_composite([&](BacktestStrategyRuntime&, CompositeStrategy& composite) {
            const double equity = compute_current_equity();
            composite.SetBacktestAccountSnapshot(equity, equity - spec.initial_equity);
        });
        const auto market_result = market_pipeline.OnTick(snapshot);
        if (!process_pipeline_result(market_result)) return false;
        if (!market_result.duplicate_tick && !market_result.late_tick) {
            ReplayTick context_tick = tick;
            context_tick.trading_day = snapshot.trading_day;
            if (!detail::TrackReplayBarTickContext(context_tick, &replay_bar_contexts, error))
                return false;
        }
        if (!process_pipeline_result(market_pipeline.AdvanceWatermark(tick.ts_ns))) return false;

        if (spec.deterministic_fills) {
            if (has_product_fee) {
                used_margin_total =
                    ComputeTotalMarginUsed(position_state, mark_price, product_fee_book);
                max_margin_used = std::max(max_margin_used, used_margin_total);
                const ProductFeeEntry* entry = product_fee_book.Find(tick.instrument_id);
                if (entry != nullptr && entry->contract_multiplier > 0.0) {
                    for_each_composite(
                        [&](BacktestStrategyRuntime& /*runtime*/, CompositeStrategy& composite) {
                            composite.SetBacktestContractMultiplier(tick.instrument_id,
                                                                    entry->contract_multiplier);
                        });
                }
            }
            const double equity =
                ComputeTotalEquity(spec.initial_equity, position_state, mark_price,
                                   total_commission, has_product_fee ? &product_fee_book : nullptr);
            for_each_composite(
                [&](BacktestStrategyRuntime& /*runtime*/, CompositeStrategy& composite) {
                    composite.SetBacktestAccountSnapshot(equity, equity - spec.initial_equity);
                });

            std::vector<SignalIntent> tick_intents;
            for (auto& runtime : strategy_runtimes) {
                if (!runtime_matches_instrument(runtime, tick.instrument_id)) continue;
                if (online_parity &&
                    !contract_coordinator.CanDispatchToStrategy(tick.instrument_id))
                    continue;
                auto runtime_intents = runtime.strategy->OnMarketTick(snapshot);
                for (auto& intent : runtime_intents) {
                    intent.generated_ts_ns = virtual_now_ns;
                    if (online_parity) {
                        intent.product_id = detail::InstrumentSymbolPrefix(intent.instrument_id);
                        intent.contract_generation =
                            contract_coordinator.GenerationForInstrument(intent.instrument_id)
                                .value_or(0);
                    }
                }
                tick_intents.insert(tick_intents.end(), runtime_intents.begin(),
                                    runtime_intents.end());
            }
            replay.intents_emitted += static_cast<std::int64_t>(tick_intents.size());
            if (!process_tick_intents(tick_intents, tick, tick.last_price,
                                      MarketRegime::kUnknown)) {
                return false;
            }
        }

        if (spec.deterministic_fills) {
            record_latest_daily_equity_for_tick(tick);
        }
    }

    if (ticks.failed()) return false;
    ticks.Finish(&replay);

    // The virtual clock stops with the input. Pending partial buckets remain a checkpoint;
    // no future event is invented to publish a final strategy bar.
    MarketBarPipeline::PersistenceState final_market_checkpoint;
    MarketBarPipelineResult shutdown_result;
    if (!market_pipeline.PrepareShutdown(replay.last_ts_ns, &final_market_checkpoint,
                                         &shutdown_result, error) ||
        !process_pipeline_result(shutdown_result))
        return false;
    replay_bar_contexts.clear();
    pending_bar_intents_by_instrument.clear();
    pending_tick_intents_by_instrument.clear();

    if (emit_indicator_trace_csv && !indicator_trace_csv_writer.Close(error)) {
        return false;
    }
    if (emit_indicator_trace_parquet && !indicator_trace_parquet_writer.Close(error)) {
        return false;
    }
    if (emit_sub_strategy_indicator_trace_csv &&
        !sub_strategy_indicator_trace_csv_writer.Close(error)) {
        return false;
    }
    if (emit_sub_strategy_indicator_trace_parquet &&
        !sub_strategy_indicator_trace_parquet_writer.Close(error)) {
        return false;
    }

    replay.instrument_count = static_cast<std::int64_t>(instrument_universe.size());
    replay.instrument_universe.assign(instrument_universe.begin(), instrument_universe.end());

    for (BacktestStrategyRuntime& runtime : strategy_runtimes) {
        if (runtime.strategy != nullptr) {
            runtime.strategy->Shutdown();
        }
    }

    BacktestCliResult result;
    result.run_id = spec.run_id;
    result.mode = spec.deterministic_fills ? "deterministic" : "bar_replay";
    result.data_source = data_source;
    result.engine_mode = spec.engine_mode;
    if (!runtime_risk_error.empty()) {
        if (error) *error = runtime_risk_error;
        return false;
    }
    if (online_parity) {
        RuntimeSemanticsConfig final_runtime;
        if (!LoadRuntimeSemanticsConfig(spec.online_runtime_config_path, &final_runtime, error))
            return false;
        if (final_runtime.effective_fingerprint != runtime_semantics.effective_fingerprint) {
            if (error)
                *error =
                    "online_parity risk/runtime snapshot changed during replay; historical "
                    "rule-update events are required";
            return false;
        }
    }
    result.runtime_semantics = runtime_semantics;
    result.event_clock = online_parity ? "virtual_utc" : "virtual_legacy_exchange_local";
    result.initialization_result =
        online_parity ? "cold_start_then_complete_bar_warmup" : "research_cold_start";
    result.warmup_rejected_opens = warmup_rejected_opens;
    result.runtime_gate_rejected_orders = runtime_gate_rejections;
    if (online_parity) {
        result.contract_statuses = contract_coordinator.GetAllStatuses();
        std::sort(result.contract_statuses.begin(), result.contract_statuses.end(),
                  [](const auto& a, const auto& b) { return a.product_id < b.product_id; });
        for (const auto& status : result.contract_statuses) {
            if (status.baseline_count < status.eligible_count) {
                if (error)
                    *error = "online_parity candidate coverage incomplete for product " +
                             status.product_id;
                return false;
            }
            if (status.phase == DominantContractPhase::kWarming)
                result.runtime_limitations.push_back(status.product_id +
                                                     ": incomplete warmup at end of input");
        }
        result.runtime_limitations.push_back(
            "Execution remains deterministic next-tick simulation using the configured price "
            "policy; "
            "no exchange queue or broker reconciliation is replayed");
    }
    if (online_parity)
        result.runtime_limitations.push_back(
            "Risk rules use an immutable content snapshot and the shared RiskManager with virtual "
            "time; enabled Sim subaccount budgets, unsupported applicable rules, and gateway "
            "backpressure without arrival/ack history fail explicitly");
    std::map<std::string, std::string> sorted_checkpoint(final_market_checkpoint.begin(),
                                                         final_market_checkpoint.end());
    std::ostringstream checkpoint_json;
    checkpoint_json << "{";
    bool checkpoint_first = true;
    for (const auto& [key, value] : sorted_checkpoint) {
        if (!checkpoint_first) checkpoint_json << ',';
        checkpoint_first = false;
        checkpoint_json << '\"' << JsonEscape(key) << "\":\"" << JsonEscape(value) << '\"';
    }
    checkpoint_json << "}";
    result.market_checkpoint_json = checkpoint_json.str();
    result.rollover_mode = spec.rollover_mode;
    result.initial_equity = spec.initial_equity;
    result.final_equity = spec.initial_equity;
    result.spec = spec;
    result.spec.strategy_factory = spec.strategy_factory;
    result.spec.strategy_composite_config = spec.strategy_composite_config;
    result.spec.trace_output_format = spec.trace_output_format;
    result.spec.indicator_trace_path = indicator_trace_path;
    result.spec.sub_strategy_indicator_trace_path = sub_strategy_indicator_trace_path;
    BacktestCliSpec signature_spec = spec;
    signature_spec.indicator_trace_path = indicator_trace_path;
    signature_spec.sub_strategy_indicator_trace_path = sub_strategy_indicator_trace_path;
    result.input_signature = BuildInputSignature(signature_spec);
    result.indicator_trace_enabled = spec.emit_indicator_trace;
    result.indicator_trace_path = indicator_trace_path;
    result.indicator_trace_rows = emit_indicator_trace_csv
                                      ? indicator_trace_csv_writer.rows_written()
                                      : indicator_trace_parquet_writer.rows_written();
    result.sub_strategy_indicator_trace_enabled = spec.emit_sub_strategy_indicator_trace;
    result.sub_strategy_indicator_trace_path = sub_strategy_indicator_trace_path;
    result.sub_strategy_indicator_trace_rows =
        emit_sub_strategy_indicator_trace_csv
            ? sub_strategy_indicator_trace_csv_writer.rows_written()
            : sub_strategy_indicator_trace_parquet_writer.rows_written();

    if (data_source == "csv") {
        result.data_signature = ComputeFileDigest(spec.csv_path, error);
    } else {
        result.data_signature =
            ComputeDatasetDigest(spec.dataset_root, spec.start_date, spec.end_date, error);
    }
    if (result.data_signature.empty()) {
        return false;
    }

    result.parameters.start_date = spec.start_date;
    result.parameters.end_date = spec.end_date;
    result.parameters.initial_capital = spec.initial_equity;
    result.parameters.engine_mode = spec.engine_mode;
    result.parameters.rollover_mode = spec.rollover_mode;
    result.parameters.product_series_mode = spec.product_series_mode;
    result.parameters.strategy_factory = spec.strategy_factory;
    if (spec.emit_trades) {
        result.trades = trades;
    }
    if (spec.emit_orders) {
        result.orders = orders;
    }
    if (spec.emit_position_history) {
        result.position_history = position_history;
    }
    std::vector<EquitySample> daily_equity_history;
    if (!latest_daily_equity_samples.empty()) {
        daily_equity_history.reserve(latest_daily_equity_samples.size());
        for (const auto& [day, sample] : latest_daily_equity_samples) {
            (void)day;
            daily_equity_history.push_back(sample);
        }
    }
    result.daily = ComputeDailyMetrics(daily_equity_history, result.trades, spec.initial_equity);
    result.risk_metrics = ComputeRiskMetrics(result.daily);
    result.execution_quality = ComputeExecutionQuality(result.orders, result.trades);
    result.rolling_metrics = ComputeRollingMetrics(result.daily, 63);
    result.regime_performance = ComputeRegimePerformance(result.trades);
    result.advanced_summary =
        ComputeAdvancedSummary(result.daily, result.trades, result.risk_metrics);
    result.monte_carlo = ComputeMonteCarloResult(result.daily, spec.initial_equity);
    result.factor_exposure = ComputeFactorExposure(result.daily);

    if (!spec.deterministic_fills) {
        result.replay = replay;
        *out = std::move(result);
        return true;
    }

    std::map<std::string, InstrumentPnlSnapshot> instrument_pnl;
    double total_realized_pnl = 0.0;
    double total_unrealized_pnl = 0.0;
    for (const auto& [instrument_id, state] : position_state) {
        const auto mark_it = mark_price.find(instrument_id);
        const double last_price =
            mark_it != mark_price.end() ? mark_it->second : state.avg_open_price;
        const double unrealized =
            ComputeUnrealized(state.net_position, state.avg_open_price, last_price,
                              ResolveContractMultiplier(
                                  has_product_fee ? &product_fee_book : nullptr, instrument_id));

        InstrumentPnlSnapshot snapshot;
        snapshot.net_position = state.net_position;
        snapshot.avg_open_price = state.avg_open_price;
        snapshot.realized_pnl = state.realized_pnl;
        snapshot.unrealized_pnl = unrealized;
        snapshot.last_price = last_price;
        instrument_pnl[instrument_id] = snapshot;

        total_realized_pnl += snapshot.realized_pnl;
        total_unrealized_pnl += snapshot.unrealized_pnl;
    }

    // EOF may follow a real fill without a later complete bar. Include its fees and mark
    // in terminal extrema rather than leaving the performance curve at the previous bar.
    equity_points.push_back(compute_current_equity());
    double max_equity = 0.0;
    double min_equity = 0.0;
    double max_drawdown = 0.0;
    if (!equity_points.empty()) {
        max_equity = equity_points.front();
        min_equity = equity_points.front();
        double running_peak = equity_points.front();
        for (double equity : equity_points) {
            max_equity = std::max(max_equity, equity);
            min_equity = std::min(min_equity, equity);
            running_peak = std::max(running_peak, equity);
            max_drawdown = std::max(max_drawdown, running_peak - equity);
        }
    }

    DeterministicReplayReport deterministic;
    deterministic.replay = replay;
    deterministic.intents_processed = intents_processed;
    deterministic.order_events_emitted = order_events;
    deterministic.wal_records = wal_records;
    deterministic.instrument_bars = instrument_bars;
    deterministic.instrument_pnl = instrument_pnl;
    deterministic.total_realized_pnl = total_realized_pnl;
    deterministic.total_unrealized_pnl = total_unrealized_pnl;
    deterministic.performance.total_realized_pnl = total_realized_pnl;
    deterministic.performance.total_unrealized_pnl = total_unrealized_pnl;
    deterministic.performance.total_pnl = total_realized_pnl + total_unrealized_pnl;
    deterministic.performance.initial_equity = spec.initial_equity;
    deterministic.performance.final_equity = latest_equity;
    deterministic.performance.total_commission = total_commission;
    deterministic.performance.total_pnl_after_cost =
        deterministic.performance.total_pnl - deterministic.performance.total_commission;
    deterministic.performance.max_margin_used = max_margin_used;
    deterministic.performance.final_margin_used =
        has_product_fee ? ComputeTotalMarginUsed(position_state, mark_price, product_fee_book)
                        : 0.0;
    deterministic.performance.margin_clipped_orders = margin_clipped_orders;
    deterministic.performance.margin_rejected_orders = margin_rejected_orders;
    deterministic.performance.max_equity = max_equity;
    deterministic.performance.min_equity = min_equity;
    deterministic.performance.max_drawdown = max_drawdown;
    deterministic.performance.order_status_counts = order_status_counts;
    deterministic.invariant_violations = ValidateInvariants(instrument_pnl);
    deterministic.rollover_events = rollover_events;
    deterministic.rollover_actions = rollover_actions;
    deterministic.rollover_slippage_cost = rollover_slippage_cost;
    deterministic.rollover_canceled_orders = rollover_canceled_orders;

    result.replay = replay;
    result.has_deterministic = true;
    result.deterministic = deterministic;
    result.final_equity = deterministic.performance.final_equity;

    *out = std::move(result);
    return true;
}

BacktestSummary SummarizeBacktest(const BacktestCliResult& result) {
    BacktestSummary summary;
    summary.intents_emitted = result.replay.intents_emitted;
    if (result.has_deterministic) {
        summary.order_events = result.deterministic.order_events_emitted;
        summary.total_pnl = result.deterministic.performance.total_pnl;
        summary.max_drawdown = result.deterministic.performance.max_drawdown;
    }
    return summary;
}

std::string RenderBacktestMarkdown(const BacktestCliResult& result) {
    std::ostringstream md;
    md << "# Backtest Replay Result\n\n"
       << "## Metadata\n"
       << "- Run ID: `" << result.run_id << "`\n"
       << "- Mode: `" << result.mode << "`\n"
       << "- Input Signature: `" << result.input_signature << "`\n"
       << "- Data Signature: `" << result.data_signature << "`\n\n"
       << "## Replay Overview\n"
       << "- Ticks Read: `" << result.replay.ticks_read << "`\n"
       << "- Scan Rows: `" << result.replay.scan_rows << "`\n"
       << "- Scan Row Groups: `" << result.replay.scan_row_groups << "`\n"
       << "- IO Bytes: `" << result.replay.io_bytes << "`\n"
       << "- Early Stop Hit: `" << (result.replay.early_stop_hit ? "true" : "false") << "`\n"
       << "- Bars Emitted: `" << result.replay.bars_emitted << "`\n"
       << "- Intents Emitted: `" << result.replay.intents_emitted << "`\n"
       << "- Instrument Count: `" << result.replay.instrument_count << "`\n"
       << "- Instrument Universe: `";

    for (std::size_t i = 0; i < result.replay.instrument_universe.size(); ++i) {
        if (i > 0) {
            md << ',';
        }
        md << result.replay.instrument_universe[i];
    }
    md << "`\n"
       << "- Time Range (ns): `" << result.replay.first_ts_ns << ':' << result.replay.last_ts_ns
       << "`\n\n";

    if (result.has_deterministic) {
        md << "## Deterministic Summary\n"
           << "- Order Events: `" << result.deterministic.order_events_emitted << "`\n"
           << "- WAL Records: `" << result.deterministic.wal_records << "`\n"
           << "- Total PnL: `" << detail::FormatDouble(result.deterministic.performance.total_pnl)
           << "`\n"
           << "- Max Drawdown: `"
           << detail::FormatDouble(result.deterministic.performance.max_drawdown) << "`\n";
    }

    md << "\n## HF Standard Summary\n"
       << "- Version: `" << result.version << "`\n"
       << "- Computation Semantics Version: `" << kBacktestComputationSemanticsVersion << "`\n"
       << "- Daily Rows: `" << result.daily.size() << "`\n"
       << "- Trades Rows: `" << result.trades.size() << "`\n"
       << "- Orders Rows: `" << result.orders.size() << "`\n"
       << "- Position Snapshot Rows: `" << result.position_history.size() << "`\n"
       << "- Emit Trades: `" << (result.spec.emit_trades ? "true" : "false") << "`\n"
       << "- Emit Orders: `" << (result.spec.emit_orders ? "true" : "false") << "`\n"
       << "- Emit Position History: `" << (result.spec.emit_position_history ? "true" : "false")
       << "`\n"
       << "- Product Series Mode: `" << result.spec.product_series_mode << "`\n"
       << "- VaR95 (%): `" << detail::FormatDouble(result.risk_metrics.var_95) << "`\n"
       << "- ES95 (%): `" << detail::FormatDouble(result.risk_metrics.expected_shortfall_95)
       << "`\n"
       << "- Fill Rate: `" << detail::FormatDouble(result.execution_quality.limit_order_fill_rate)
       << "`\n"
       << "- Cancel Rate: `" << detail::FormatDouble(result.execution_quality.cancel_rate) << "`\n";
    return md.str();
}

std::string RenderBacktestJson(const BacktestCliResult& result) {
    std::ostringstream limitations;
    limitations << '[';
    for (std::size_t i = 0; i < result.runtime_limitations.size(); ++i) {
        if (i != 0) limitations << ',';
        limitations << '\"' << JsonEscape(result.runtime_limitations[i]) << '\"';
    }
    limitations << ']';
    std::ostringstream contracts;
    contracts << '[';
    for (std::size_t i = 0; i < result.contract_statuses.size(); ++i) {
        const auto& status = result.contract_statuses[i];
        if (i != 0) contracts << ',';
        contracts << "{\"product_id\":\"" << JsonEscape(status.product_id)
                  << "\",\"current_instrument_id\":\"" << JsonEscape(status.current_instrument_id)
                  << "\",\"phase\":\"" << DominantContractPhaseName(status.phase)
                  << "\",\"generation\":" << status.generation
                  << ",\"eligible_count\":" << status.eligible_count
                  << ",\"baseline_count\":" << status.baseline_count
                  << ",\"warmup_observed_bars\":" << status.warmup_observed_bars
                  << ",\"warmup_required_bars\":" << status.warmup_required_bars
                  << ",\"generation_rejections\":" << status.generation_rejections << '}';
    }
    contracts << ']';
    std::ostringstream json;
    json << "{\n"
         << "  \"computation_semantics_version\": \"" << kBacktestComputationSemanticsVersion
         << "\",\n"
         << "  \"run_id\": \"" << JsonEscape(result.run_id) << "\",\n"
         << "  \"mode\": \"" << JsonEscape(result.mode) << "\",\n"
         << "  \"data_source\": \"" << JsonEscape(result.data_source) << "\",\n"
         << "  \"engine_mode\": \"" << JsonEscape(result.engine_mode) << "\",\n"
         << "  \"runtime_limitations\": " << limitations.str() << ",\n"
         << "  \"contract_statuses\": " << contracts.str() << ",\n"
         << "  \"event_clock\": \"" << result.event_clock << "\",\n"
         << "  \"initialization_result\": \"" << result.initialization_result << "\",\n"
         << "  \"warmup_rejected_opens\": " << result.warmup_rejected_opens << ",\n"
         << "  \"runtime_gate_rejected_orders\": " << result.runtime_gate_rejected_orders << ",\n"
         << "  \"runtime_semantics\": " << RenderRuntimeSemanticsJson(result.runtime_semantics)
         << ",\n"
         << "  \"runtime_source_fingerprint\": \""
         << result.runtime_semantics.source_content_fingerprint << "\",\n"
         << "  \"runtime_effective_fingerprint\": \""
         << result.runtime_semantics.effective_fingerprint << "\",\n"
         << "  \"market_checkpoint\": "
         << (result.market_checkpoint_json.empty() ? "{}" : result.market_checkpoint_json) << ",\n"
         << "  \"rollover_mode\": \"" << JsonEscape(result.rollover_mode) << "\",\n"
         << "  \"product_series_mode\": \"" << JsonEscape(result.spec.product_series_mode)
         << "\",\n"
         << "  \"initial_equity\": " << detail::FormatDouble(result.initial_equity) << ",\n"
         << "  \"final_equity\": " << detail::FormatDouble(result.final_equity) << ",\n"
         << "  \"metric_keys\": [\"total_pnl\", \"max_drawdown\", \"win_rate\", \"fill_rate\", "
            "\"capital_efficiency\"],\n"
         << "  \"spec\": {\n"
         << "    \"csv_path\": \"" << JsonEscape(result.spec.csv_path) << "\",\n"
         << "    \"dataset_root\": \"" << JsonEscape(result.spec.dataset_root) << "\",\n"
         << "    \"dataset_manifest\": \"" << JsonEscape(result.spec.dataset_manifest) << "\",\n"
         << "    \"detector_config\": \"" << JsonEscape(result.spec.detector_config_path) << "\",\n"
         << "    \"engine_mode\": \"" << JsonEscape(result.spec.engine_mode) << "\",\n"
         << "    \"online_runtime_config_path\": \""
         << JsonEscape(result.spec.online_runtime_config_path) << "\",\n"
         << "    \"behavior_profile\": \"" << JsonEscape(result.spec.behavior_profile) << "\",\n"
         << "    \"parameter_profile\": \"" << JsonEscape(result.spec.parameter_profile) << "\",\n"
         << "    \"initialization_policy\": \"" << JsonEscape(result.spec.initialization_policy)
         << "\",\n"
         << "    \"input_timestamp_basis\": \"" << JsonEscape(result.spec.input_timestamp_basis)
         << "\",\n"
         << "    \"rollover_mode\": \"" << JsonEscape(result.spec.rollover_mode) << "\",\n"
         << "    \"product_series_mode\": \"" << JsonEscape(result.spec.product_series_mode)
         << "\",\n"
         << "    \"rollover_price_mode\": \"" << JsonEscape(result.spec.rollover_price_mode)
         << "\",\n"
         << "    \"rollover_slippage_bps\": "
         << detail::FormatDouble(result.spec.rollover_slippage_bps) << ",\n"
         << "    \"start_date\": \"" << JsonEscape(result.spec.start_date) << "\",\n"
         << "    \"end_date\": \"" << JsonEscape(result.spec.end_date) << "\",\n"
         << "    \"max_ticks\": ";

    if (result.spec.max_ticks.has_value()) {
        json << result.spec.max_ticks.value();
    } else {
        json << "null";
    }

    json << ",\n"
         << "    \"symbols\": [";
    for (std::size_t i = 0; i < result.spec.symbols.size(); ++i) {
        if (i > 0) {
            json << ", ";
        }
        json << "\"" << JsonEscape(result.spec.symbols[i]) << "\"";
    }
    json << "],\n"
         << "    \"deterministic_fills\": " << (result.spec.deterministic_fills ? "true" : "false")
         << ",\n"
         << "    \"streaming\": " << (result.spec.streaming ? "true" : "false") << ",\n"
         << "    \"strict_parquet\": " << (result.spec.strict_parquet ? "true" : "false") << ",\n"
         << "    \"wal_path\": \"" << JsonEscape(result.spec.wal_path) << "\",\n"
         << "    \"account_id\": \"" << JsonEscape(result.spec.account_id) << "\",\n"
         << "    \"run_id\": \"" << JsonEscape(result.spec.run_id) << "\",\n"
         << "    \"initial_equity\": " << detail::FormatDouble(result.spec.initial_equity) << ",\n"
         << "    \"product_config_path\": \"" << JsonEscape(result.spec.product_config_path)
         << "\",\n"
         << "    \"contract_expiry_calendar_path\": \""
         << JsonEscape(result.spec.contract_expiry_calendar_path) << "\",\n"
         << "    \"strategy_main_config_path\": \""
         << JsonEscape(result.spec.strategy_main_config_path) << "\",\n"
         << "    \"strategy_factory\": \"" << JsonEscape(result.spec.strategy_factory) << "\",\n"
         << "    \"strategy_composite_config\": \""
         << JsonEscape(result.spec.strategy_composite_config) << "\",\n"
         << "    \"strategy_configs\": [";

    for (std::size_t i = 0; i < result.spec.strategy_configs.size(); ++i) {
        if (i > 0) {
            json << ", ";
        }
        const BacktestStrategyConfig& strategy_config = result.spec.strategy_configs[i];
        json << "{"
             << "\"strategy_id\": \"" << JsonEscape(strategy_config.strategy_id) << "\", "
             << "\"strategy_factory\": \"" << JsonEscape(strategy_config.strategy_factory) << "\", "
             << "\"strategy_main_config_path\": \""
             << JsonEscape(strategy_config.strategy_main_config_path) << "\", "
             << "\"strategy_composite_config\": \""
             << JsonEscape(strategy_config.strategy_composite_config) << "\", "
             << "\"product_id\": \"" << JsonEscape(strategy_config.product_id) << "\""
             << "}";
    }

    json << "],\n"
         << "    \"trace_output_format\": \"" << JsonEscape(result.spec.trace_output_format)
         << "\",\n"
         << "    \"market_state_detector\": {\n"
         << "      \"adx_period\": " << result.spec.detector_config.adx_period << ",\n"
         << "      \"adx_strong_threshold\": "
         << detail::FormatDouble(result.spec.detector_config.adx_strong_threshold) << ",\n"
         << "      \"adx_weak_lower\": "
         << detail::FormatDouble(result.spec.detector_config.adx_weak_lower) << ",\n"
         << "      \"adx_weak_upper\": "
         << detail::FormatDouble(result.spec.detector_config.adx_weak_upper) << ",\n"
         << "      \"kama_er_period\": " << result.spec.detector_config.kama_er_period << ",\n"
         << "      \"kama_fast_period\": " << result.spec.detector_config.kama_fast_period << ",\n"
         << "      \"kama_slow_period\": " << result.spec.detector_config.kama_slow_period << ",\n"
         << "      \"kama_er_strong\": "
         << detail::FormatDouble(result.spec.detector_config.kama_er_strong) << ",\n"
         << "      \"kama_er_weak_lower\": "
         << detail::FormatDouble(result.spec.detector_config.kama_er_weak_lower) << ",\n"
         << "      \"atr_period\": " << result.spec.detector_config.atr_period << ",\n"
         << "      \"require_adx_for_trend\": "
         << (result.spec.detector_config.require_adx_for_trend ? "true" : "false") << ",\n"
         << "      \"use_kama_er\": "
         << (result.spec.detector_config.use_kama_er ? "true" : "false") << "\n"
         << "    },\n"
         << "    \"emit_state_snapshots\": "
         << (result.spec.emit_state_snapshots ? "true" : "false") << ",\n"
         << "    \"emit_indicator_trace\": "
         << (result.spec.emit_indicator_trace ? "true" : "false") << ",\n"
         << "    \"indicator_trace_path\": \"" << JsonEscape(result.spec.indicator_trace_path)
         << "\",\n"
         << "    \"emit_sub_strategy_indicator_trace\": "
         << (result.spec.emit_sub_strategy_indicator_trace ? "true" : "false") << ",\n"
         << "    \"sub_strategy_indicator_trace_path\": \""
         << JsonEscape(result.spec.sub_strategy_indicator_trace_path) << "\",\n"
         << "    \"emit_trades\": " << (result.spec.emit_trades ? "true" : "false") << ",\n"
         << "    \"emit_orders\": " << (result.spec.emit_orders ? "true" : "false") << ",\n"
         << "    \"emit_position_history\": "
         << (result.spec.emit_position_history ? "true" : "false") << ",\n"
         << "    \"emit_per_variety_outputs\": "
         << (result.spec.emit_per_variety_outputs ? "true" : "false") << "\n"
         << "  },\n"
         << "  \"input_signature\": \"" << JsonEscape(result.input_signature) << "\",\n"
         << "  \"data_signature\": \"" << JsonEscape(result.data_signature) << "\",\n"
         << "  \"attribution\": {},\n"
         << "  \"risk_decomposition\": {},\n"
         << "  \"replay\": {\n"
         << "    \"ticks_read\": " << result.replay.ticks_read << ",\n"
         << "    \"buffered_input_rows_high_water\": "
         << result.replay.buffered_input_rows_high_water << ",\n"
         << "    \"scan_rows\": " << result.replay.scan_rows << ",\n"
         << "    \"scan_row_groups\": " << result.replay.scan_row_groups << ",\n"
         << "    \"io_bytes\": " << result.replay.io_bytes << ",\n"
         << "    \"early_stop_hit\": " << (result.replay.early_stop_hit ? "true" : "false") << ",\n"
         << "    \"bars_emitted\": " << result.replay.bars_emitted << ",\n"
         << "    \"intents_emitted\": " << result.replay.intents_emitted << ",\n"
         << "    \"first_instrument\": \"" << JsonEscape(result.replay.first_instrument) << "\",\n"
         << "    \"last_instrument\": \"" << JsonEscape(result.replay.last_instrument) << "\",\n"
         << "    \"instrument_count\": " << result.replay.instrument_count << ",\n"
         << "    \"instrument_universe\": [";

    for (std::size_t i = 0; i < result.replay.instrument_universe.size(); ++i) {
        if (i > 0) {
            json << ", ";
        }
        json << "\"" << JsonEscape(result.replay.instrument_universe[i]) << "\"";
    }

    json << "],\n"
         << "    \"first_ts_ns\": " << result.replay.first_ts_ns << ",\n"
         << "    \"last_ts_ns\": " << result.replay.last_ts_ns << "\n"
         << "  },\n"
         << "  \"indicator_trace\": {\n"
         << "    \"enabled\": " << (result.indicator_trace_enabled ? "true" : "false") << ",\n"
         << "    \"path\": \"" << JsonEscape(result.indicator_trace_path) << "\",\n"
         << "    \"rows\": " << result.indicator_trace_rows << "\n"
         << "  },\n"
         << "  \"sub_strategy_indicator_trace\": {\n"
         << "    \"enabled\": " << (result.sub_strategy_indicator_trace_enabled ? "true" : "false")
         << ",\n"
         << "    \"path\": \"" << JsonEscape(result.sub_strategy_indicator_trace_path) << "\",\n"
         << "    \"rows\": " << result.sub_strategy_indicator_trace_rows << "\n"
         << "  }";

    if (result.has_deterministic) {
        json << ",\n"
             << "  \"deterministic\": {\n"
             << "    \"intents_processed\": " << result.deterministic.intents_processed << ",\n"
             << "    \"order_events_emitted\": " << result.deterministic.order_events_emitted
             << ",\n"
             << "    \"wal_records\": " << result.deterministic.wal_records << ",\n"
             << "    \"instrument_bars\": {";

        bool first_entry = true;
        for (const auto& [instrument_id, count] : result.deterministic.instrument_bars) {
            if (!first_entry) {
                json << ", ";
            }
            first_entry = false;
            json << "\"" << JsonEscape(instrument_id) << "\": " << count;
        }

        json << "},\n"
             << "    \"instrument_pnl\": {";

        bool first_instrument = true;
        for (const auto& [instrument_id, snapshot] : result.deterministic.instrument_pnl) {
            if (!first_instrument) {
                json << ", ";
            }
            first_instrument = false;
            json << "\"" << JsonEscape(instrument_id) << "\": {"
                 << "\"net_position\": " << snapshot.net_position << ", "
                 << "\"avg_open_price\": " << detail::FormatDouble(snapshot.avg_open_price) << ", "
                 << "\"realized_pnl\": " << detail::FormatDouble(snapshot.realized_pnl) << ", "
                 << "\"unrealized_pnl\": " << detail::FormatDouble(snapshot.unrealized_pnl) << ", "
                 << "\"last_price\": " << detail::FormatDouble(snapshot.last_price) << "}";
        }

        json << "},\n"
             << "    \"total_realized_pnl\": "
             << detail::FormatDouble(result.deterministic.total_realized_pnl) << ",\n"
             << "    \"total_unrealized_pnl\": "
             << detail::FormatDouble(result.deterministic.total_unrealized_pnl) << ",\n"
             << "    \"performance\": {\n"
             << "      \"initial_equity\": "
             << detail::FormatDouble(result.deterministic.performance.initial_equity) << ",\n"
             << "      \"final_equity\": "
             << detail::FormatDouble(result.deterministic.performance.final_equity) << ",\n"
             << "      \"total_commission\": "
             << detail::FormatDouble(result.deterministic.performance.total_commission) << ",\n"
             << "      \"total_pnl_after_cost\": "
             << detail::FormatDouble(result.deterministic.performance.total_pnl_after_cost) << ",\n"
             << "      \"max_margin_used\": "
             << detail::FormatDouble(result.deterministic.performance.max_margin_used) << ",\n"
             << "      \"final_margin_used\": "
             << detail::FormatDouble(result.deterministic.performance.final_margin_used) << ",\n"
             << "      \"margin_clipped_orders\": "
             << result.deterministic.performance.margin_clipped_orders << ",\n"
             << "      \"margin_rejected_orders\": "
             << result.deterministic.performance.margin_rejected_orders << ",\n"
             << "      \"total_realized_pnl\": "
             << detail::FormatDouble(result.deterministic.performance.total_realized_pnl) << ",\n"
             << "      \"total_unrealized_pnl\": "
             << detail::FormatDouble(result.deterministic.performance.total_unrealized_pnl) << ",\n"
             << "      \"total_pnl\": "
             << detail::FormatDouble(result.deterministic.performance.total_pnl) << ",\n"
             << "      \"max_equity\": "
             << detail::FormatDouble(result.deterministic.performance.max_equity) << ",\n"
             << "      \"min_equity\": "
             << detail::FormatDouble(result.deterministic.performance.min_equity) << ",\n"
             << "      \"max_drawdown\": "
             << detail::FormatDouble(result.deterministic.performance.max_drawdown) << ",\n"
             << "      \"order_status_counts\": {";

        bool first_status = true;
        for (const auto& [status, count] : result.deterministic.performance.order_status_counts) {
            if (!first_status) {
                json << ", ";
            }
            first_status = false;
            json << "\"" << JsonEscape(status) << "\": " << count;
        }

        json << "}\n"
             << "    },\n"
             << "    \"invariant_violations\": [";

        for (std::size_t i = 0; i < result.deterministic.invariant_violations.size(); ++i) {
            if (i > 0) {
                json << ", ";
            }
            json << "\"" << JsonEscape(result.deterministic.invariant_violations[i]) << "\"";
        }

        json << "],\n"
             << "    \"rollover_events\": [";

        for (std::size_t i = 0; i < result.deterministic.rollover_events.size(); ++i) {
            if (i > 0) {
                json << ", ";
            }
            const RolloverEvent& event = result.deterministic.rollover_events[i];
            json << "{"
                 << "\"symbol\": \"" << JsonEscape(event.symbol) << "\", "
                 << "\"from_instrument\": \"" << JsonEscape(event.from_instrument) << "\", "
                 << "\"to_instrument\": \"" << JsonEscape(event.to_instrument) << "\", "
                 << "\"mode\": \"" << JsonEscape(event.mode) << "\", "
                 << "\"position\": " << event.position << ", "
                 << "\"direction\": \"" << JsonEscape(event.direction) << "\", "
                 << "\"from_price\": " << detail::FormatDouble(event.from_price) << ", "
                 << "\"to_price\": " << detail::FormatDouble(event.to_price) << ", "
                 << "\"canceled_orders\": " << event.canceled_orders << ", "
                 << "\"price_mode\": \"" << JsonEscape(event.price_mode) << "\", "
                 << "\"slippage_bps\": " << detail::FormatDouble(event.slippage_bps) << ", "
                 << "\"ts_ns\": " << event.ts_ns << "}";
        }

        json << "],\n"
             << "    \"rollover_actions\": [";

        for (std::size_t i = 0; i < result.deterministic.rollover_actions.size(); ++i) {
            if (i > 0) {
                json << ", ";
            }
            const RolloverAction& action = result.deterministic.rollover_actions[i];
            json << "{"
                 << "\"symbol\": \"" << JsonEscape(action.symbol) << "\", "
                 << "\"action\": \"" << JsonEscape(action.action) << "\", "
                 << "\"from_instrument\": \"" << JsonEscape(action.from_instrument) << "\", "
                 << "\"to_instrument\": \"" << JsonEscape(action.to_instrument) << "\", "
                 << "\"position\": " << action.position << ", "
                 << "\"side\": \"" << JsonEscape(action.side) << "\", "
                 << "\"price\": " << detail::FormatDouble(action.price) << ", "
                 << "\"mode\": \"" << JsonEscape(action.mode) << "\", "
                 << "\"price_mode\": \"" << JsonEscape(action.price_mode) << "\", "
                 << "\"slippage_bps\": " << detail::FormatDouble(action.slippage_bps) << ", "
                 << "\"canceled_orders\": " << action.canceled_orders << ", "
                 << "\"ts_ns\": " << action.ts_ns << "}";
        }

        json << "],\n"
             << "    \"rollover_slippage_cost\": "
             << detail::FormatDouble(result.deterministic.rollover_slippage_cost) << ",\n"
             << "    \"rollover_canceled_orders\": "
             << result.deterministic.rollover_canceled_orders << "\n"
             << "  }";
    }

    const BacktestSummary summary = SummarizeBacktest(result);
    json << ",\n"
         << "  \"summary\": {\n"
         << "    \"intents_emitted\": " << summary.intents_emitted << ",\n"
         << "    \"order_events\": " << summary.order_events << ",\n"
         << "    \"total_pnl\": " << detail::FormatDouble(summary.total_pnl) << ",\n"
         << "    \"max_drawdown\": " << detail::FormatDouble(summary.max_drawdown) << "\n"
         << "  },\n"
         << "  \"hf_standard\": {\n"
         << "    \"version\": \"" << JsonEscape(result.version) << "\",\n"
         << "    \"parameters\": {\n"
         << "      \"start_date\": \"" << JsonEscape(result.parameters.start_date) << "\",\n"
         << "      \"end_date\": \"" << JsonEscape(result.parameters.end_date) << "\",\n"
         << "      \"initial_capital\": " << detail::FormatDouble(result.parameters.initial_capital)
         << ",\n"
         << "      \"engine_mode\": \"" << JsonEscape(result.parameters.engine_mode) << "\",\n"
         << "      \"rollover_mode\": \"" << JsonEscape(result.parameters.rollover_mode) << "\",\n"
         << "      \"product_series_mode\": \"" << JsonEscape(result.parameters.product_series_mode)
         << "\",\n"
         << "      \"strategy_factory\": \"" << JsonEscape(result.parameters.strategy_factory)
         << "\"\n"
         << "    },\n"
         << "    \"metadata\": {\n"
         << "      \"emit_trades\": " << (result.spec.emit_trades ? "true" : "false") << ",\n"
         << "      \"emit_orders\": " << (result.spec.emit_orders ? "true" : "false") << ",\n"
         << "      \"emit_position_history\": "
         << (result.spec.emit_position_history ? "true" : "false") << ",\n"
         << "      \"emit_per_variety_outputs\": "
         << (result.spec.emit_per_variety_outputs ? "true" : "false") << ",\n"
         << "      \"position_sampling\": \"on_trade\",\n"
         << "      \"trade_datetime_fields\": [\"timestamp_dt_utc\"],\n"
         << "      \"order_datetime_fields\": [\"created_at_dt_utc\", \"last_update_dt_utc\"]\n"
         << "    },\n"
         << "    \"advanced_summary\": {\n"
         << "      \"rolling_sharpe_3m_last\": "
         << detail::FormatDouble(result.advanced_summary.rolling_sharpe_3m_last) << ",\n"
         << "      \"rolling_max_dd_3m_last\": "
         << detail::FormatDouble(result.advanced_summary.rolling_max_dd_3m_last) << ",\n"
         << "      \"information_ratio\": "
         << detail::FormatDouble(result.advanced_summary.information_ratio) << ",\n"
         << "      \"beta\": " << detail::FormatDouble(result.advanced_summary.beta) << ",\n"
         << "      \"alpha\": " << detail::FormatDouble(result.advanced_summary.alpha) << ",\n"
         << "      \"tail_ratio\": " << detail::FormatDouble(result.advanced_summary.tail_ratio)
         << ",\n"
         << "      \"gain_to_pain_ratio\": "
         << detail::FormatDouble(result.advanced_summary.gain_to_pain_ratio) << ",\n"
         << "      \"avg_win_loss_duration_ratio\": "
         << detail::FormatDouble(result.advanced_summary.avg_win_loss_duration_ratio) << ",\n"
         << "      \"profit_factor\": "
         << detail::FormatDouble(result.advanced_summary.profit_factor) << "\n"
         << "    },\n"
         << "    \"execution_quality\": {\n"
         << "      \"limit_order_fill_rate\": "
         << detail::FormatDouble(result.execution_quality.limit_order_fill_rate) << ",\n"
         << "      \"avg_wait_time_ms\": "
         << detail::FormatDouble(result.execution_quality.avg_wait_time_ms) << ",\n"
         << "      \"cancel_rate\": " << detail::FormatDouble(result.execution_quality.cancel_rate)
         << ",\n"
         << "      \"slippage_mean\": "
         << detail::FormatDouble(result.execution_quality.slippage_mean) << ",\n"
         << "      \"slippage_std\": "
         << detail::FormatDouble(result.execution_quality.slippage_std) << ",\n"
         << "      \"slippage_percentiles\": [";
    for (std::size_t i = 0; i < result.execution_quality.slippage_percentiles.size(); ++i) {
        if (i > 0) {
            json << ", ";
        }
        json << detail::FormatDouble(result.execution_quality.slippage_percentiles[i]);
    }
    json << "]\n"
         << "    },\n"
         << "    \"risk_metrics\": {\n"
         << "      \"var_95\": " << detail::FormatDouble(result.risk_metrics.var_95) << ",\n"
         << "      \"expected_shortfall_95\": "
         << detail::FormatDouble(result.risk_metrics.expected_shortfall_95) << ",\n"
         << "      \"ulcer_index\": " << detail::FormatDouble(result.risk_metrics.ulcer_index)
         << ",\n"
         << "      \"recovery_factor\": "
         << detail::FormatDouble(result.risk_metrics.recovery_factor) << ",\n"
         << "      \"tail_loss\": " << detail::FormatDouble(result.risk_metrics.tail_loss) << "\n"
         << "    },\n"
         << "    \"rolling_metrics\": {\n"
         << "      \"rolling_sharpe_3m\": [";
    for (std::size_t i = 0; i < result.rolling_metrics.rolling_sharpe_3m.size(); ++i) {
        if (i > 0) {
            json << ", ";
        }
        json << detail::FormatDouble(result.rolling_metrics.rolling_sharpe_3m[i]);
    }
    json << "],\n"
         << "      \"rolling_max_dd_3m\": [";
    for (std::size_t i = 0; i < result.rolling_metrics.rolling_max_dd_3m.size(); ++i) {
        if (i > 0) {
            json << ", ";
        }
        json << detail::FormatDouble(result.rolling_metrics.rolling_max_dd_3m[i]);
    }
    json << "]\n"
         << "    },\n"
         << "    \"daily\": [";
    for (std::size_t i = 0; i < result.daily.size(); ++i) {
        if (i > 0) {
            json << ", ";
        }
        const DailyPerformance& row = result.daily[i];
        json << "{\"date\":\"" << JsonEscape(row.date)
             << "\",\"capital\":" << detail::FormatDouble(row.capital)
             << ",\"daily_return_pct\":" << detail::FormatDouble(row.daily_return_pct)
             << ",\"cumulative_return_pct\":" << detail::FormatDouble(row.cumulative_return_pct)
             << ",\"drawdown_pct\":" << detail::FormatDouble(row.drawdown_pct)
             << ",\"position_value\":" << detail::FormatDouble(row.position_value)
             << ",\"trades_count\":" << row.trades_count
             << ",\"turnover\":" << detail::FormatDouble(row.turnover) << ",\"market_regime\":\""
             << JsonEscape(row.market_regime) << "\"}";
    }
    const std::vector<TradeRecord> sorted_trades = SortedTradesForOutput(result.trades);
    const std::vector<OrderRecord> sorted_orders = SortedOrdersForOutput(result.orders);

    json << "],\n"
         << "    \"trades\": [";
    for (std::size_t i = 0; i < sorted_trades.size(); ++i) {
        if (i > 0) {
            json << ", ";
        }
        const TradeRecord& row = sorted_trades[i];
        json << "{\"fill_seq\":" << row.fill_seq << ",\"trade_id\":\"" << JsonEscape(row.trade_id)
             << "\",\"order_id\":\"" << JsonEscape(row.order_id) << "\",\"symbol\":\""
             << JsonEscape(row.symbol) << "\",\"exchange\":\"" << JsonEscape(row.exchange)
             << "\",\"side\":\"" << JsonEscape(row.side) << "\",\"offset\":\""
             << JsonEscape(row.offset) << "\",\"volume\":" << row.volume
             << ",\"price\":" << detail::FormatDouble(row.price)
             << ",\"timestamp_ns\":" << row.timestamp_ns << ",\"signal_ts_ns\":" << row.signal_ts_ns
             << ",\"trading_day\":\"" << JsonEscape(row.trading_day) << "\",\"action_day\":\""
             << JsonEscape(row.action_day) << "\",\"update_time\":\"" << JsonEscape(row.update_time)
             << "\",\"timestamp_dt_local\":\"" << JsonEscape(row.timestamp_dt_local)
             << "\",\"signal_dt_local\":\"" << JsonEscape(row.signal_dt_local) << "\""
             << ",\"timestamp_dt_utc\":\"" << JsonEscape(row.timestamp_dt_utc) << "\""
             << ",\"commission\":" << detail::FormatDouble(row.commission)
             << ",\"slippage\":" << detail::FormatDouble(row.slippage)
             << ",\"realized_pnl\":" << detail::FormatDouble(row.realized_pnl)
             << ",\"risk_budget_r\":" << detail::FormatDouble(row.risk_budget_r)
             << ",\"strategy_id\":\"" << JsonEscape(row.strategy_id) << "\",\"signal_type\":\""
             << JsonEscape(row.signal_type) << "\",\"regime_at_entry\":\""
             << JsonEscape(row.regime_at_entry) << "\"}";
    }
    json << "],\n"
         << "    \"orders\": [";
    for (std::size_t i = 0; i < sorted_orders.size(); ++i) {
        if (i > 0) {
            json << ", ";
        }
        const OrderRecord& row = sorted_orders[i];
        json << "{\"order_seq\":" << row.order_seq << ",\"order_id\":\"" << JsonEscape(row.order_id)
             << "\",\"client_order_id\":\"" << JsonEscape(row.client_order_id) << "\",\"symbol\":\""
             << JsonEscape(row.symbol) << "\",\"type\":\"" << JsonEscape(row.type)
             << "\",\"side\":\"" << JsonEscape(row.side) << "\",\"offset\":\""
             << JsonEscape(row.offset) << "\",\"price\":" << detail::FormatDouble(row.price)
             << ",\"volume\":" << row.volume << ",\"status\":\"" << JsonEscape(row.status)
             << "\",\"filled_volume\":" << row.filled_volume
             << ",\"avg_fill_price\":" << detail::FormatDouble(row.avg_fill_price)
             << ",\"created_at_ns\":" << row.created_at_ns << ",\"created_at_dt_utc\":\""
             << JsonEscape(row.created_at_dt_utc) << "\",\"last_update_ns\":" << row.last_update_ns
             << ",\"last_update_dt_utc\":\"" << JsonEscape(row.last_update_dt_utc)
             << "\",\"trading_day\":\"" << JsonEscape(row.trading_day) << "\",\"action_day\":\""
             << JsonEscape(row.action_day) << "\",\"update_time\":\"" << JsonEscape(row.update_time)
             << "\",\"created_at_dt_local\":\"" << JsonEscape(row.created_at_dt_local)
             << "\",\"last_update_dt_local\":\"" << JsonEscape(row.last_update_dt_local) << "\""
             << ",\"strategy_id\":\"" << JsonEscape(row.strategy_id) << "\",\"cancel_reason\":\""
             << JsonEscape(row.cancel_reason) << "\"}";
    }
    json << "],\n"
         << "    \"regime_performance\": [";
    for (std::size_t i = 0; i < result.regime_performance.size(); ++i) {
        if (i > 0) {
            json << ", ";
        }
        const RegimePerformance& row = result.regime_performance[i];
        json << "{\"regime\":\"" << JsonEscape(row.regime) << "\",\"total_days\":" << row.total_days
             << ",\"trades_count\":" << row.trades_count
             << ",\"win_rate\":" << detail::FormatDouble(row.win_rate)
             << ",\"average_return_pct\":" << detail::FormatDouble(row.average_return_pct)
             << ",\"total_pnl\":" << detail::FormatDouble(row.total_pnl)
             << ",\"sharpe\":" << detail::FormatDouble(row.sharpe)
             << ",\"max_drawdown_pct\":" << detail::FormatDouble(row.max_drawdown_pct) << "}";
    }
    json << "],\n"
         << "    \"position_history\": [";
    for (std::size_t i = 0; i < result.position_history.size(); ++i) {
        if (i > 0) {
            json << ", ";
        }
        const PositionSnapshot& row = result.position_history[i];
        json << "{\"timestamp_ns\":" << row.timestamp_ns << ",\"symbol\":\""
             << JsonEscape(row.symbol) << "\",\"net_position\":" << row.net_position
             << ",\"avg_price\":" << detail::FormatDouble(row.avg_price)
             << ",\"unrealized_pnl\":" << detail::FormatDouble(row.unrealized_pnl) << "}";
    }
    json << "],\n"
         << "    \"monte_carlo\": {\n"
         << "      \"simulations\": " << result.monte_carlo.simulations << ",\n"
         << "      \"mean_final_capital\": "
         << detail::FormatDouble(result.monte_carlo.mean_final_capital) << ",\n"
         << "      \"ci_95_lower\": " << detail::FormatDouble(result.monte_carlo.ci_95_lower)
         << ",\n"
         << "      \"ci_95_upper\": " << detail::FormatDouble(result.monte_carlo.ci_95_upper)
         << ",\n"
         << "      \"prob_loss\": " << detail::FormatDouble(result.monte_carlo.prob_loss) << ",\n"
         << "      \"max_drawdown_95\": "
         << detail::FormatDouble(result.monte_carlo.max_drawdown_95) << "\n"
         << "    },\n"
         << "    \"factor_exposure\": [";
    for (std::size_t i = 0; i < result.factor_exposure.size(); ++i) {
        if (i > 0) {
            json << ", ";
        }
        const FactorExposure& row = result.factor_exposure[i];
        json << "{\"factor\":\"" << JsonEscape(row.factor)
             << "\",\"exposure\":" << detail::FormatDouble(row.exposure)
             << ",\"t_stat\":" << detail::FormatDouble(row.t_stat) << "}";
    }
    json << "]\n"
         << "  }\n"
         << "}\n";
    return json.str();
}

}  // namespace quant_hft::backtest
