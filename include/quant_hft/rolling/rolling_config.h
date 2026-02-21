#pragma once

#include <cstdint>
#include <filesystem>
#include <optional>
#include <string>
#include <vector>

namespace quant_hft::rolling {

struct RollingBacktestBase {
    std::string engine_mode{"parquet"};
    std::string dataset_root;
    std::string dataset_manifest;
    std::vector<std::string> symbols;
    std::string strategy_factory{"composite"};
    std::string strategy_composite_config;
    std::optional<std::int64_t> max_ticks;
    bool deterministic_fills{true};
    bool strict_parquet{true};
    std::string rollover_mode{"strict"};
    std::string rollover_price_mode{"bbo"};
    double rollover_slippage_bps{0.0};
    double initial_equity{1'000'000.0};
    bool emit_trades{false};
    bool emit_orders{false};
    bool emit_position_history{false};
};

struct RollingWindowSpec {
    std::string type{"rolling"};
    int train_length_days{180};
    int test_length_days{30};
    int step_days{30};
    int min_train_days{180};
    std::string start_date;
    std::string end_date;
};

struct RollingOptimizationSpec {
    std::string algorithm{"grid"};
    std::string metric{"hf_standard.profit_factor"};
    bool maximize{true};
    int max_trials{100};
    int parallel{1};
    std::string param_space;
    std::string target_sub_config_path;
};

struct RollingOutputSpec {
    std::string report_json;
    std::string report_md;
    std::string best_params_dir;
    bool keep_temp_files{false};
    int window_parallel{1};
};

struct RollingConfig {
    std::filesystem::path config_path;
    std::filesystem::path config_dir;
    std::string mode{"fixed_params"};
    RollingBacktestBase backtest_base;
    RollingWindowSpec window;
    RollingOptimizationSpec optimization;
    RollingOutputSpec output;
};

bool LoadRollingConfig(const std::string& yaml_path, RollingConfig* out, std::string* error);

}  // namespace quant_hft::rolling

