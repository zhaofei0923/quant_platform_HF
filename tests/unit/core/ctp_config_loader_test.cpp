#include "quant_hft/core/ctp_config_loader.h"

#include <gtest/gtest.h>

#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <string>

namespace quant_hft {
namespace {

class ScopedEnvVar {
   public:
    ScopedEnvVar(std::string key, const char* value) : key_(std::move(key)) {
        const char* previous = std::getenv(key_.c_str());
        if (previous != nullptr) {
            had_previous_ = true;
            previous_value_ = previous;
        }
        if (value == nullptr) {
            unsetenv(key_.c_str());
        } else {
            setenv(key_.c_str(), value, 1);
        }
    }

    ~ScopedEnvVar() {
        if (had_previous_) {
            setenv(key_.c_str(), previous_value_.c_str(), 1);
            return;
        }
        unsetenv(key_.c_str());
    }

   private:
    std::string key_;
    bool had_previous_{false};
    std::string previous_value_;
};

std::filesystem::path WriteTempConfig(const std::string& body) {
    const auto token = std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
    const auto path = std::filesystem::temp_directory_path() /
                      ("quant_hft_ctp_config_loader_test_" + token + ".yaml");
    std::ofstream out(path);
    out << body;
    return path;
}

TEST(CtpConfigLoaderTest, ResolveEnvVarsReplacesExistingEnvVar) {
    const ScopedEnvVar env("CTP_TEST_ENV_KEY", "resolved-value");
    const auto resolved = ResolveEnvVars("prefix-${CTP_TEST_ENV_KEY}-suffix");
    EXPECT_EQ(resolved, "prefix-resolved-value-suffix");
}

TEST(CtpConfigLoaderTest, ResolveEnvVarsLeavesUnknownVarEmpty) {
    const ScopedEnvVar env("CTP_TEST_UNKNOWN_KEY", nullptr);
    const auto resolved = ResolveEnvVars("left-${CTP_TEST_UNKNOWN_KEY}-right");
    EXPECT_EQ(resolved, "left--right");
}

TEST(CtpConfigLoaderTest, LoadConfigWithEnvVarsSuccess) {
    const ScopedEnvVar broker_id("CTP_TEST_BROKER_ID", "9999");
    const ScopedEnvVar user_id("CTP_TEST_USER_ID", "191202");
    const ScopedEnvVar investor_id("CTP_TEST_INVESTOR_ID", "191202");
    const ScopedEnvVar market_front("CTP_TEST_MARKET_FRONT", "tcp://127.0.0.1:40011");
    const ScopedEnvVar trader_front("CTP_TEST_TRADER_FRONT", "tcp://127.0.0.1:40001");
    const ScopedEnvVar password("CTP_TEST_PASSWORD_VAR", "env-secret");

    const auto config_path = WriteTempConfig(
        "ctp:\n"
        "  environment: sim\n"
        "  is_production_mode: false\n"
        "  broker_id: \"${CTP_TEST_BROKER_ID}\"\n"
        "  user_id: \"${CTP_TEST_USER_ID}\"\n"
        "  investor_id: \"${CTP_TEST_INVESTOR_ID}\"\n"
        "  market_front: \"${CTP_TEST_MARKET_FRONT}\"\n"
        "  trader_front: \"${CTP_TEST_TRADER_FRONT}\"\n"
        "  password: \"${CTP_TEST_PASSWORD_VAR}\"\n");

    CtpFileConfig config;
    std::string error;
    ASSERT_TRUE(CtpConfigLoader::LoadFromYaml(config_path.string(), &config, &error)) << error;
    EXPECT_EQ(config.runtime.broker_id, "9999");
    EXPECT_EQ(config.runtime.user_id, "191202");
    EXPECT_EQ(config.runtime.investor_id, "191202");
    EXPECT_EQ(config.runtime.md_front, "tcp://127.0.0.1:40011");
    EXPECT_EQ(config.runtime.td_front, "tcp://127.0.0.1:40001");
    EXPECT_EQ(config.runtime.password, "env-secret");

    std::filesystem::remove(config_path);
}

TEST(CtpConfigLoaderTest, LoadsValidSimConfigWithPasswordEnv) {
    const ScopedEnvVar password_env("CTP_TEST_PASSWORD", "env-secret");
    const auto config_path = WriteTempConfig(
        "ctp:\n"
        "  environment: sim\n"
        "  is_production_mode: false\n"
        "  enable_real_api: false\n"
        "  broker_id: \"9999\"\n"
        "  user_id: \"191202\"\n"
        "  investor_id: \"191202\"\n"
        "  market_front: \"tcp://182.254.243.31:40011\"\n"
        "  trader_front: \"tcp://182.254.243.31:40001\"\n"
        "  password_env: \"CTP_TEST_PASSWORD\"\n"
        "  auth_code: \"0000000000000000\"\n"
        "  app_id: \"simnow_client_test\"\n"
        "  query_rate_limit_qps: 12\n");

    CtpFileConfig config;
    std::string error;
    ASSERT_TRUE(CtpConfigLoader::LoadFromYaml(config_path.string(), &config, &error)) << error;

    EXPECT_EQ(config.runtime.environment, CtpEnvironment::kSimNow);
    EXPECT_FALSE(config.runtime.is_production_mode);
    EXPECT_FALSE(config.runtime.enable_real_api);
    EXPECT_TRUE(config.runtime.enable_terminal_auth);
    EXPECT_EQ(config.runtime.md_front, "tcp://182.254.243.31:40011");
    EXPECT_EQ(config.runtime.td_front, "tcp://182.254.243.31:40001");
    EXPECT_EQ(config.runtime.password, "env-secret");
    EXPECT_EQ(config.query_rate_limit_qps, 12);

    std::filesystem::remove(config_path);
}

TEST(CtpConfigLoaderTest, RejectsUnknownEnvironment) {
    const auto config_path = WriteTempConfig(
        "ctp:\n"
        "  environment: sandbox\n"
        "  is_production_mode: false\n"
        "  broker_id: \"9999\"\n"
        "  user_id: \"191202\"\n"
        "  investor_id: \"191202\"\n"
        "  market_front: \"tcp://127.0.0.1:40011\"\n"
        "  trader_front: \"tcp://127.0.0.1:40001\"\n"
        "  password: \"plain-secret\"\n");

    CtpFileConfig config;
    std::string error;
    EXPECT_FALSE(CtpConfigLoader::LoadFromYaml(config_path.string(), &config, &error));
    EXPECT_NE(error.find("environment"), std::string::npos);

    std::filesystem::remove(config_path);
}

TEST(CtpConfigLoaderTest, SupportsDisablingTerminalAuthInNonProduction) {
    const ScopedEnvVar password_env("CTP_TEST_PASSWORD", "env-secret");
    const auto config_path = WriteTempConfig(
        "ctp:\n"
        "  environment: sim\n"
        "  is_production_mode: false\n"
        "  enable_real_api: true\n"
        "  enable_terminal_auth: false\n"
        "  broker_id: \"9999\"\n"
        "  user_id: \"191202\"\n"
        "  investor_id: \"191202\"\n"
        "  market_front: \"tcp://182.254.243.31:40012\"\n"
        "  trader_front: \"tcp://182.254.243.31:40002\"\n"
        "  password_env: \"CTP_TEST_PASSWORD\"\n");

    CtpFileConfig config;
    std::string error;
    ASSERT_TRUE(CtpConfigLoader::LoadFromYaml(config_path.string(), &config, &error)) << error;
    EXPECT_TRUE(config.runtime.enable_real_api);
    EXPECT_FALSE(config.runtime.enable_terminal_auth);

    std::filesystem::remove(config_path);
}

TEST(CtpConfigLoaderTest, RejectsProductionConfigWhenTerminalAuthDisabled) {
    const ScopedEnvVar password_env("CTP_TEST_PASSWORD", "env-secret");
    const auto config_path = WriteTempConfig(
        "ctp:\n"
        "  environment: production\n"
        "  is_production_mode: true\n"
        "  enable_real_api: true\n"
        "  enable_terminal_auth: false\n"
        "  broker_id: \"9999\"\n"
        "  user_id: \"191202\"\n"
        "  investor_id: \"191202\"\n"
        "  market_front: \"tcp://180.168.146.187:10231\"\n"
        "  trader_front: \"tcp://180.168.146.187:10201\"\n"
        "  password_env: \"CTP_TEST_PASSWORD\"\n"
        "  app_id: \"prod_app\"\n"
        "  auth_code: \"prod_auth\"\n");

    CtpFileConfig config;
    std::string error;
    EXPECT_FALSE(CtpConfigLoader::LoadFromYaml(config_path.string(), &config, &error));
    EXPECT_NE(error.find("enable_terminal_auth"), std::string::npos);

    std::filesystem::remove(config_path);
}

TEST(CtpConfigLoaderTest, RejectsWhenPasswordCannotBeResolved) {
    const ScopedEnvVar missing_password("CTP_TEST_MISSING_PASSWORD", nullptr);
    const auto config_path = WriteTempConfig(
        "ctp:\n"
        "  environment: sim\n"
        "  is_production_mode: false\n"
        "  broker_id: \"9999\"\n"
        "  user_id: \"191202\"\n"
        "  investor_id: \"191202\"\n"
        "  market_front: \"tcp://127.0.0.1:40011\"\n"
        "  trader_front: \"tcp://127.0.0.1:40001\"\n"
        "  password_env: \"CTP_TEST_MISSING_PASSWORD\"\n");

    CtpFileConfig config;
    std::string error;
    EXPECT_FALSE(CtpConfigLoader::LoadFromYaml(config_path.string(), &config, &error));
    EXPECT_NE(error.find("password"), std::string::npos);

    std::filesystem::remove(config_path);
}

TEST(CtpConfigLoaderTest, LoadsStrategyEngineKeysAndSplitsLists) {
    const auto config_path = WriteTempConfig(
        "ctp:\n"
        "  environment: sim\n"
        "  is_production_mode: false\n"
        "  broker_id: \"9999\"\n"
        "  user_id: \"191202\"\n"
        "  investor_id: \"191202\"\n"
        "  market_front: \"tcp://127.0.0.1:40011\"\n"
        "  trader_front: \"tcp://127.0.0.1:40001\"\n"
        "  password: \"plain-secret\"\n"
        "  instruments: \"SHFE.ag2406, SHFE.rb2405\"\n"
        "  strategy_ids: \" demo, alpha \"\n"
        "  run_type: \"sim\"\n"
        "  strategy_factory: \"demo\"\n"
        "  strategy_queue_capacity: 4096\n"
        "  account_id: \"sim-account\"\n");

    CtpFileConfig config;
    std::string error;
    ASSERT_TRUE(CtpConfigLoader::LoadFromYaml(config_path.string(), &config, &error)) << error;

    ASSERT_EQ(config.instruments.size(), 2U);
    EXPECT_EQ(config.instruments[0], "SHFE.ag2406");
    EXPECT_EQ(config.instruments[1], "SHFE.rb2405");
    ASSERT_EQ(config.strategy_ids.size(), 2U);
    EXPECT_EQ(config.strategy_ids[0], "demo");
    EXPECT_EQ(config.strategy_ids[1], "alpha");
    EXPECT_EQ(config.run_type, "sim");
    EXPECT_EQ(config.strategy_factory, "demo");
    EXPECT_EQ(config.strategy_queue_capacity, 4096);
    EXPECT_FALSE(config.strategy_state_persist_enabled);
    EXPECT_EQ(config.strategy_state_backend, "redis");
    EXPECT_EQ(config.strategy_state_snapshot_interval_ms, 60000);
    EXPECT_EQ(config.strategy_state_ttl_seconds, 86400);
    EXPECT_EQ(config.strategy_state_key_prefix, "strategy_state");
    EXPECT_EQ(config.strategy_state_file_dir, "runtime/trading/state");
    EXPECT_EQ(config.strategy_metrics_emit_interval_ms, 1000);
    EXPECT_EQ(config.account_id, "sim-account");

    std::filesystem::remove(config_path);
}

TEST(CtpConfigLoaderTest, LoadsDominantRuntimeRecheckKeys) {
    const auto config_path = WriteTempConfig(
        "ctp:\n"
        "  environment: sim\n"
        "  is_production_mode: false\n"
        "  broker_id: \"9999\"\n"
        "  user_id: \"191202\"\n"
        "  investor_id: \"191202\"\n"
        "  market_front: \"tcp://127.0.0.1:40011\"\n"
        "  trader_front: \"tcp://127.0.0.1:40001\"\n"
        "  password: \"plain-secret\"\n"
        "  product_ids: \"c,rb\"\n"
        "  active_contract_mode: \"dominant_open_interest\"\n"
        "  dominant_contract_wait_ms: 4000\n"
        "  dominant_contract_recheck_interval_ms: 60000\n"
        "  dominant_contract_min_lead_ratio: 0.15\n"
        "  dominant_contract_min_lead_windows: 4\n"
        "  dominant_contract_min_hold_ms: 900000\n"
        "  dominant_contract_cache_max_age_ms: 3600000\n"
        "  dominant_contract_max_tick_age_ms: 5500\n"
        "  dominant_contract_warmup_bars: 40\n"
        "  dominant_contract_switch_timeout_ms: 45000\n"
        "  dominant_contract_require_complete_baseline: true\n"
        "  dominant_contract_switch_mode: \"flat_only\"\n");

    CtpFileConfig config;
    std::string error;
    ASSERT_TRUE(CtpConfigLoader::LoadFromYaml(config_path.string(), &config, &error)) << error;

    EXPECT_EQ(config.product_ids.size(), 2U);
    EXPECT_EQ(config.active_contract_mode, "dominant_open_interest");
    EXPECT_EQ(config.dominant_contract_wait_ms, 4000);
    EXPECT_EQ(config.dominant_contract_recheck_interval_ms, 60000);
    EXPECT_DOUBLE_EQ(config.dominant_contract_min_lead_ratio, 0.15);
    EXPECT_EQ(config.dominant_contract_min_lead_windows, 4);
    EXPECT_EQ(config.dominant_contract_min_hold_ms, 900000);
    EXPECT_EQ(config.dominant_contract_cache_max_age_ms, 3600000);
    EXPECT_EQ(config.dominant_contract_max_tick_age_ms, 5500);
    EXPECT_EQ(config.dominant_contract_warmup_bars, 40);
    EXPECT_EQ(config.dominant_contract_switch_timeout_ms, 45000);
    EXPECT_TRUE(config.dominant_contract_require_complete_baseline);
    EXPECT_EQ(config.dominant_contract_switch_mode, "flat_only");

    std::filesystem::remove(config_path);
}

TEST(CtpConfigLoaderTest, LoadsDominantContractSelectionKeys) {
    const auto config_path = WriteTempConfig(
        "ctp:\n"
        "  environment: sim\n"
        "  is_production_mode: false\n"
        "  broker_id: \"9999\"\n"
        "  user_id: \"191202\"\n"
        "  investor_id: \"191202\"\n"
        "  market_front: \"tcp://127.0.0.1:40011\"\n"
        "  trader_front: \"tcp://127.0.0.1:40001\"\n"
        "  password: \"plain-secret\"\n"
        "  product_ids: \"c\"\n"
        "  active_contract_mode: \"dominant_open_interest\"\n"
        "  dominant_contract_wait_ms: 3500\n");

    CtpFileConfig config;
    std::string error;
    ASSERT_TRUE(CtpConfigLoader::LoadFromYaml(config_path.string(), &config, &error)) << error;

    ASSERT_EQ(config.product_ids.size(), 1U);
    EXPECT_EQ(config.product_ids[0], "c");
    EXPECT_EQ(config.active_contract_mode, "dominant_open_interest");
    EXPECT_EQ(config.dominant_contract_wait_ms, 3500);

    std::filesystem::remove(config_path);
}

TEST(CtpConfigLoaderTest, LoadsStrategyStateAndMetricsConfigKeys) {
    const auto config_path = WriteTempConfig(
        "ctp:\n"
        "  environment: sim\n"
        "  is_production_mode: false\n"
        "  broker_id: \"9999\"\n"
        "  user_id: \"191202\"\n"
        "  investor_id: \"191202\"\n"
        "  market_front: \"tcp://127.0.0.1:40011\"\n"
        "  trader_front: \"tcp://127.0.0.1:40001\"\n"
        "  password: \"plain-secret\"\n"
        "  strategy_state_persist_enabled: true\n"
        "  strategy_state_backend: \"file\"\n"
        "  strategy_state_snapshot_interval_ms: 5000\n"
        "  strategy_state_ttl_seconds: 3600\n"
        "  strategy_state_key_prefix: \"hf_strategy_state\"\n"
        "  strategy_state_file_dir: \"runtime/trading/state/simnow\"\n"
        "  strategy_metrics_emit_interval_ms: 2000\n");

    CtpFileConfig config;
    std::string error;
    ASSERT_TRUE(CtpConfigLoader::LoadFromYaml(config_path.string(), &config, &error)) << error;
    EXPECT_TRUE(config.strategy_state_persist_enabled);
    EXPECT_EQ(config.strategy_state_backend, "file");
    EXPECT_EQ(config.strategy_state_snapshot_interval_ms, 5000);
    EXPECT_EQ(config.strategy_state_ttl_seconds, 3600);
    EXPECT_EQ(config.strategy_state_key_prefix, "hf_strategy_state");
    EXPECT_EQ(config.strategy_state_file_dir, "runtime/trading/state/simnow");
    EXPECT_EQ(config.strategy_metrics_emit_interval_ms, 2000);

    std::filesystem::remove(config_path);
}

TEST(CtpConfigLoaderTest, RejectsNegativeStrategyMetricsEmitInterval) {
    const auto config_path = WriteTempConfig(
        "ctp:\n"
        "  environment: sim\n"
        "  is_production_mode: false\n"
        "  broker_id: \"9999\"\n"
        "  user_id: \"191202\"\n"
        "  investor_id: \"191202\"\n"
        "  market_front: \"tcp://127.0.0.1:40011\"\n"
        "  trader_front: \"tcp://127.0.0.1:40001\"\n"
        "  password: \"plain-secret\"\n"
        "  strategy_metrics_emit_interval_ms: -1\n");

    CtpFileConfig config;
    std::string error;
    EXPECT_FALSE(CtpConfigLoader::LoadFromYaml(config_path.string(), &config, &error));
    EXPECT_NE(error.find("strategy_metrics_emit_interval_ms"), std::string::npos);

    std::filesystem::remove(config_path);
}

TEST(CtpConfigLoaderTest, RejectsInvalidRunType) {
    const auto config_path = WriteTempConfig(
        "ctp:\n"
        "  environment: sim\n"
        "  is_production_mode: false\n"
        "  broker_id: \"9999\"\n"
        "  user_id: \"191202\"\n"
        "  investor_id: \"191202\"\n"
        "  market_front: \"tcp://127.0.0.1:40011\"\n"
        "  trader_front: \"tcp://127.0.0.1:40001\"\n"
        "  password: \"plain-secret\"\n"
        "  run_type: \"paper\"\n");

    CtpFileConfig config;
    std::string error;
    EXPECT_FALSE(CtpConfigLoader::LoadFromYaml(config_path.string(), &config, &error));
    EXPECT_NE(error.find("run_type"), std::string::npos);

    std::filesystem::remove(config_path);
}

TEST(CtpConfigLoaderTest, RequiresCompositeConfigPathWhenFactoryIsComposite) {
    const auto config_path = WriteTempConfig(
        "ctp:\n"
        "  environment: sim\n"
        "  is_production_mode: false\n"
        "  broker_id: \"9999\"\n"
        "  user_id: \"191202\"\n"
        "  investor_id: \"191202\"\n"
        "  market_front: \"tcp://127.0.0.1:40011\"\n"
        "  trader_front: \"tcp://127.0.0.1:40001\"\n"
        "  password: \"plain-secret\"\n"
        "  strategy_factory: \"composite\"\n");

    CtpFileConfig config;
    std::string error;
    EXPECT_FALSE(CtpConfigLoader::LoadFromYaml(config_path.string(), &config, &error));
    EXPECT_NE(error.find("strategy_composite_config"), std::string::npos);

    std::filesystem::remove(config_path);
}

TEST(CtpConfigLoaderTest, ResolvesRelativeCompositeConfigPathFromYamlDirectory) {
    const auto token = std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
    const auto temp_dir =
        std::filesystem::temp_directory_path() / ("quant_hft_ctp_loader_composite_" + token);
    const auto nested_dir = temp_dir / "configs";
    const auto strategy_dir = temp_dir / "strategies";
    std::filesystem::create_directories(nested_dir);
    std::filesystem::create_directories(strategy_dir);

    const auto config_path = nested_dir / "ctp.yaml";
    {
        std::ofstream out(config_path);
        out << "ctp:\n"
               "  environment: sim\n"
               "  is_production_mode: false\n"
               "  broker_id: \"9999\"\n"
               "  user_id: \"191202\"\n"
               "  investor_id: \"191202\"\n"
               "  market_front: \"tcp://127.0.0.1:40011\"\n"
               "  trader_front: \"tcp://127.0.0.1:40001\"\n"
               "  password: \"plain-secret\"\n"
               "  strategy_factory: \"composite\"\n"
               "  strategy_composite_config: \"../strategies/composite_strategy.yaml\"\n";
    }

    CtpFileConfig config;
    std::string error;
    ASSERT_TRUE(CtpConfigLoader::LoadFromYaml(config_path.string(), &config, &error)) << error;

    const std::filesystem::path expected =
        (nested_dir / "../strategies/composite_strategy.yaml").lexically_normal();
    EXPECT_EQ(std::filesystem::path(config.strategy_composite_config), expected);

    std::filesystem::remove_all(temp_dir);
}

TEST(CtpConfigLoaderTest, RejectsDeprecatedStrategyPollIntervalSetting) {
    const auto config_path = WriteTempConfig(
        "ctp:\n"
        "  environment: sim\n"
        "  is_production_mode: false\n"
        "  broker_id: \"9999\"\n"
        "  user_id: \"191202\"\n"
        "  investor_id: \"191202\"\n"
        "  market_front: \"tcp://127.0.0.1:40011\"\n"
        "  trader_front: \"tcp://127.0.0.1:40001\"\n"
        "  password: \"plain-secret\"\n"
        "  strategy_poll_interval_ms: 200\n");

    CtpFileConfig config;
    std::string error;
    EXPECT_FALSE(CtpConfigLoader::LoadFromYaml(config_path.string(), &config, &error));
    EXPECT_NE(error.find("strategy_poll_interval_ms is removed"), std::string::npos);

    std::filesystem::remove(config_path);
}

TEST(CtpConfigLoaderTest, LoadsExecutionAndRiskRuleConfigs) {
    const auto config_path = WriteTempConfig(
        "ctp:\n"
        "  environment: sim\n"
        "  is_production_mode: false\n"
        "  broker_id: \"9999\"\n"
        "  user_id: \"191202\"\n"
        "  investor_id: \"191202\"\n"
        "  market_front: \"tcp://127.0.0.1:40011\"\n"
        "  trader_front: \"tcp://127.0.0.1:40001\"\n"
        "  password: \"plain-secret\"\n"
        "  execution_mode: \"sliced\"\n"
        "  execution_algo: \"twap\"\n"
        "  execution_price_mode: \"marketable_limit\"\n"
        "  slice_size: 3\n"
        "  slice_interval_ms: 120\n"
        "  twap_duration_ms: 2500\n"
        "  vwap_lookback_bars: 30\n"
        "  throttle_reject_ratio: 0.25\n"
        "  preferred_venue: \"SIM\"\n"
        "  participation_rate_limit: 0.35\n"
        "  impact_cost_bps: 7.5\n"
        "  cancel_after_ms: 1500\n"
        "  cancel_check_interval_ms: 250\n"
        "  market_data_recording_enabled: true\n"
        "  market_data_recording_dir: \"runtime/test_market_data\"\n"
        "  market_data_recording_run_id: \"unit-run\"\n"
        "  market_data_recording_flush_each_write: true\n"
        "  market_data_recording_partition_by_product: true\n"
        "  market_data_recording_write_global_copy: true\n"
        "  risk_default_max_order_volume: 12\n"
        "  risk_default_max_order_notional: 200000\n"
        "  risk_default_max_active_orders: 4\n"
        "  risk_default_max_position_notional: 900000\n"
        "  risk_sim_subaccount_enabled: true\n"
        "  risk_sim_subaccount_id: \"sim-subaccount-200000\"\n"
        "  risk_sim_subaccount_initial_equity: 200000\n"
        "  risk_sim_subaccount_max_margin: 180000\n"
        "  risk_sim_subaccount_order_margin_rate: 0.12\n"
        "  risk_sim_subaccount_contract_multiplier: 10\n"
        "  risk_default_max_cancel_count: 8\n"
        "  risk_default_max_cancel_ratio: 0.45\n"
        "  risk_default_rule_group: \"global-default\"\n"
        "  risk_default_rule_version: \"2026.02\"\n"
        "  risk_default_policy_id: \"policy.global\"\n"
        "  risk_default_policy_scope: \"global\"\n"
        "  risk_default_decision_tags: \"default-risk\"\n"
        "  risk_rule_groups: \"ag_open,acc_guard\"\n"
        "  risk_rule_ag_open_id: \"risk.ag.open\"\n"
        "  risk_rule_ag_open_policy_id: \"policy.ag.open\"\n"
        "  risk_rule_ag_open_policy_scope: \"instrument\"\n"
        "  risk_rule_ag_open_decision_tags: \"ag,risk\"\n"
        "  risk_rule_ag_open_instrument_id: \"SHFE.ag2406\"\n"
        "  risk_rule_ag_open_exchange_id: \"SHFE\"\n"
        "  risk_rule_ag_open_max_order_volume: 2\n"
        "  risk_rule_ag_open_max_order_notional: 12000\n"
        "  risk_rule_ag_open_max_active_orders: 1\n"
        "  risk_rule_ag_open_max_position_notional: 80000\n"
        "  risk_rule_ag_open_max_cancel_count: 2\n"
        "  risk_rule_ag_open_max_cancel_ratio: 0.25\n"
        "  risk_rule_ag_open_version: \"2026.03\"\n"
        "  risk_rule_acc_guard_account_id: \"sim-account\"\n"
        "  risk_rule_acc_guard_max_order_volume: 5\n"
        "  risk_rule_acc_guard_max_order_notional: 50000\n");

    CtpFileConfig config;
    std::string error;
    ASSERT_TRUE(CtpConfigLoader::LoadFromYaml(config_path.string(), &config, &error)) << error;

    EXPECT_EQ(config.execution.mode, ExecutionMode::kSliced);
    EXPECT_EQ(config.execution.algo, ExecutionAlgo::kTwap);
    EXPECT_EQ(config.execution.price_mode, ExecutionPriceMode::kMarketableLimit);
    EXPECT_EQ(config.execution.slice_size, 3);
    EXPECT_EQ(config.execution.slice_interval_ms, 120);
    EXPECT_EQ(config.execution.twap_duration_ms, 2500);
    EXPECT_EQ(config.execution.vwap_lookback_bars, 30);
    EXPECT_DOUBLE_EQ(config.execution.throttle_reject_ratio, 0.25);
    EXPECT_EQ(config.execution.preferred_venue, "SIM");
    EXPECT_DOUBLE_EQ(config.execution.participation_rate_limit, 0.35);
    EXPECT_DOUBLE_EQ(config.execution.impact_cost_bps, 7.5);
    EXPECT_EQ(config.execution.cancel_after_ms, 1500);
    EXPECT_EQ(config.execution.cancel_check_interval_ms, 250);

    EXPECT_TRUE(config.market_data_recording.enabled);
    EXPECT_EQ(config.market_data_recording.output_dir, "runtime/test_market_data");
    EXPECT_EQ(config.market_data_recording.run_id, "unit-run");
    EXPECT_TRUE(config.market_data_recording.flush_each_write);
    EXPECT_TRUE(config.market_data_recording.partition_by_product);
    EXPECT_TRUE(config.market_data_recording.write_global_copy);

    EXPECT_EQ(config.risk.default_max_order_volume, 12);
    EXPECT_DOUBLE_EQ(config.risk.default_max_order_notional, 200000.0);
    EXPECT_EQ(config.risk.default_max_active_orders, 4);
    EXPECT_DOUBLE_EQ(config.risk.default_max_position_notional, 900000.0);
    EXPECT_TRUE(config.risk.sim_subaccount_enabled);
    EXPECT_EQ(config.risk.sim_subaccount_id, "sim-subaccount-200000");
    EXPECT_DOUBLE_EQ(config.risk.sim_subaccount_initial_equity, 200000.0);
    EXPECT_DOUBLE_EQ(config.risk.sim_subaccount_max_margin, 180000.0);
    EXPECT_DOUBLE_EQ(config.risk.sim_subaccount_order_margin_rate, 0.12);
    EXPECT_DOUBLE_EQ(config.risk.sim_subaccount_contract_multiplier, 10.0);
    EXPECT_EQ(config.risk.default_max_cancel_count, 8);
    EXPECT_DOUBLE_EQ(config.risk.default_max_cancel_ratio, 0.45);
    EXPECT_EQ(config.risk.default_rule_group, "global-default");
    EXPECT_EQ(config.risk.default_rule_version, "2026.02");
    EXPECT_EQ(config.risk.default_policy_id, "policy.global");
    EXPECT_EQ(config.risk.default_policy_scope, "global");
    EXPECT_EQ(config.risk.default_decision_tags, "default-risk");

    ASSERT_EQ(config.risk.rules.size(), 2U);
    EXPECT_EQ(config.risk.rules[0].rule_id, "risk.ag.open");
    EXPECT_EQ(config.risk.rules[0].rule_group, "ag_open");
    EXPECT_EQ(config.risk.rules[0].policy_id, "policy.ag.open");
    EXPECT_EQ(config.risk.rules[0].policy_scope, "instrument");
    EXPECT_EQ(config.risk.rules[0].decision_tags, "ag,risk");
    EXPECT_EQ(config.risk.rules[0].instrument_id, "SHFE.ag2406");
    EXPECT_EQ(config.risk.rules[0].exchange_id, "SHFE");
    EXPECT_EQ(config.risk.rules[0].max_order_volume, 2);
    EXPECT_EQ(config.risk.rules[0].max_active_orders, 1);
    EXPECT_DOUBLE_EQ(config.risk.rules[0].max_position_notional, 80000.0);
    EXPECT_EQ(config.risk.rules[0].max_cancel_count, 2);
    EXPECT_DOUBLE_EQ(config.risk.rules[0].max_cancel_ratio, 0.25);
    EXPECT_EQ(config.risk.rules[1].account_id, "sim-account");
    EXPECT_EQ(config.risk.rules[1].rule_group, "acc_guard");
    EXPECT_EQ(config.risk.rules[1].policy_id, "policy.global");
    EXPECT_EQ(config.risk.rules[1].max_cancel_count, 8);
    EXPECT_DOUBLE_EQ(config.risk.rules[1].max_cancel_ratio, 0.45);

    std::filesystem::remove(config_path);
}

TEST(CtpConfigLoaderTest, LoadsPerStrategyCompositeConfigMap) {
    const auto config_path = WriteTempConfig(
        "ctp:\n"
        "  environment: sim\n"
        "  is_production_mode: false\n"
        "  broker_id: \"9999\"\n"
        "  user_id: \"191202\"\n"
        "  investor_id: \"191202\"\n"
        "  market_front: \"tcp://127.0.0.1:40011\"\n"
        "  trader_front: \"tcp://127.0.0.1:40001\"\n"
        "  password: \"plain-secret\"\n"
        "  strategy_factory: \"composite\"\n"
        "  strategy_ids: \"c_alpha, rb_alpha\"\n"
        "  strategy_composite_config_map:\n"
        "    c_alpha: \"./composites/c.yaml\"\n"
        "    rb_alpha: \"./composites/rb.yaml\"\n");

    CtpFileConfig config;
    std::string error;
    ASSERT_TRUE(CtpConfigLoader::LoadFromYaml(config_path.string(), &config, &error)) << error;

    ASSERT_EQ(config.strategy_composite_config_map.size(), 2U);
    const std::filesystem::path config_dir = config_path.parent_path();
    EXPECT_EQ(config.strategy_composite_config_map.at("c_alpha"),
              (config_dir / "composites/c.yaml").lexically_normal().string());
    EXPECT_EQ(config.strategy_composite_config_map.at("rb_alpha"),
              (config_dir / "composites/rb.yaml").lexically_normal().string());

    std::filesystem::remove(config_path);
}

TEST(CtpConfigLoaderTest, DefaultsAccountIdToUserIdWhenNotConfigured) {
    const auto config_path = WriteTempConfig(
        "ctp:\n"
        "  environment: sim\n"
        "  is_production_mode: false\n"
        "  broker_id: \"9999\"\n"
        "  user_id: \"191202\"\n"
        "  investor_id: \"191202\"\n"
        "  market_front: \"tcp://127.0.0.1:40011\"\n"
        "  trader_front: \"tcp://127.0.0.1:40001\"\n"
        "  password: \"plain-secret\"\n");

    CtpFileConfig config;
    std::string error;
    ASSERT_TRUE(CtpConfigLoader::LoadFromYaml(config_path.string(), &config, &error)) << error;

    EXPECT_EQ(config.account_id, "191202");

    std::filesystem::remove(config_path);
}

TEST(CtpConfigLoaderTest, RejectsInvalidCancelExecutionConfigs) {
    const auto negative_cancel_after = WriteTempConfig(
        "ctp:\n"
        "  environment: sim\n"
        "  is_production_mode: false\n"
        "  broker_id: \"9999\"\n"
        "  user_id: \"191202\"\n"
        "  investor_id: \"191202\"\n"
        "  market_front: \"tcp://127.0.0.1:40011\"\n"
        "  trader_front: \"tcp://127.0.0.1:40001\"\n"
        "  password: \"plain-secret\"\n"
        "  cancel_after_ms: -1\n");

    CtpFileConfig config;
    std::string error;
    EXPECT_FALSE(CtpConfigLoader::LoadFromYaml(negative_cancel_after.string(), &config, &error));
    EXPECT_NE(error.find("cancel_after_ms"), std::string::npos);
    std::filesystem::remove(negative_cancel_after);

    const auto invalid_scan_interval = WriteTempConfig(
        "ctp:\n"
        "  environment: sim\n"
        "  is_production_mode: false\n"
        "  broker_id: \"9999\"\n"
        "  user_id: \"191202\"\n"
        "  investor_id: \"191202\"\n"
        "  market_front: \"tcp://127.0.0.1:40011\"\n"
        "  trader_front: \"tcp://127.0.0.1:40001\"\n"
        "  password: \"plain-secret\"\n"
        "  cancel_check_interval_ms: 0\n");
    error.clear();
    EXPECT_FALSE(CtpConfigLoader::LoadFromYaml(invalid_scan_interval.string(), &config, &error));
    EXPECT_NE(error.find("cancel_check_interval_ms"), std::string::npos);
    std::filesystem::remove(invalid_scan_interval);

    const auto bad_execution_algo = WriteTempConfig(
        "ctp:\n"
        "  environment: sim\n"
        "  is_production_mode: false\n"
        "  broker_id: \"9999\"\n"
        "  user_id: \"191202\"\n"
        "  investor_id: \"191202\"\n"
        "  market_front: \"tcp://127.0.0.1:40011\"\n"
        "  trader_front: \"tcp://127.0.0.1:40001\"\n"
        "  password: \"plain-secret\"\n"
        "  execution_algo: \"invalid_algo\"\n");
    error.clear();
    EXPECT_FALSE(CtpConfigLoader::LoadFromYaml(bad_execution_algo.string(), &config, &error));
    EXPECT_NE(error.find("execution_algo"), std::string::npos);
    std::filesystem::remove(bad_execution_algo);

    const auto bad_execution_price_mode = WriteTempConfig(
        "ctp:\n"
        "  environment: sim\n"
        "  is_production_mode: false\n"
        "  broker_id: \"9999\"\n"
        "  user_id: \"191202\"\n"
        "  investor_id: \"191202\"\n"
        "  market_front: \"tcp://127.0.0.1:40011\"\n"
        "  trader_front: \"tcp://127.0.0.1:40001\"\n"
        "  password: \"plain-secret\"\n"
        "  execution_price_mode: \"invalid_mode\"\n");
    error.clear();
    EXPECT_FALSE(CtpConfigLoader::LoadFromYaml(bad_execution_price_mode.string(), &config, &error));
    EXPECT_NE(error.find("execution_price_mode"), std::string::npos);
    std::filesystem::remove(bad_execution_price_mode);

    const auto bad_reject_ratio = WriteTempConfig(
        "ctp:\n"
        "  environment: sim\n"
        "  is_production_mode: false\n"
        "  broker_id: \"9999\"\n"
        "  user_id: \"191202\"\n"
        "  investor_id: \"191202\"\n"
        "  market_front: \"tcp://127.0.0.1:40011\"\n"
        "  trader_front: \"tcp://127.0.0.1:40001\"\n"
        "  password: \"plain-secret\"\n"
        "  throttle_reject_ratio: 1.5\n");
    error.clear();
    EXPECT_FALSE(CtpConfigLoader::LoadFromYaml(bad_reject_ratio.string(), &config, &error));
    EXPECT_NE(error.find("throttle_reject_ratio"), std::string::npos);
    std::filesystem::remove(bad_reject_ratio);
}

TEST(CtpConfigLoaderTest, LoadsAndValidatesCtpQueryIntervals) {
    const auto config_path = WriteTempConfig(
        "ctp:\n"
        "  environment: sim\n"
        "  is_production_mode: false\n"
        "  broker_id: \"9999\"\n"
        "  user_id: \"191202\"\n"
        "  investor_id: \"191202\"\n"
        "  market_front: \"tcp://127.0.0.1:40011\"\n"
        "  trader_front: \"tcp://127.0.0.1:40001\"\n"
        "  password: \"plain-secret\"\n"
        "  account_query_interval_ms: 1500\n"
        "  position_query_interval_ms: 1700\n"
        "  instrument_query_interval_ms: 25000\n"
        "  query_retry_backoff_ms: 300\n");

    CtpFileConfig config;
    std::string error;
    ASSERT_TRUE(CtpConfigLoader::LoadFromYaml(config_path.string(), &config, &error)) << error;

    EXPECT_EQ(config.account_query_interval_ms, 1500);
    EXPECT_EQ(config.position_query_interval_ms, 1700);
    EXPECT_EQ(config.instrument_query_interval_ms, 25000);
    EXPECT_EQ(config.runtime.query_retry_backoff_ms, 300);
    std::filesystem::remove(config_path);

    const auto invalid = WriteTempConfig(
        "ctp:\n"
        "  environment: sim\n"
        "  is_production_mode: false\n"
        "  broker_id: \"9999\"\n"
        "  user_id: \"191202\"\n"
        "  investor_id: \"191202\"\n"
        "  market_front: \"tcp://127.0.0.1:40011\"\n"
        "  trader_front: \"tcp://127.0.0.1:40001\"\n"
        "  password: \"plain-secret\"\n"
        "  position_query_interval_ms: 0\n");
    error.clear();
    EXPECT_FALSE(CtpConfigLoader::LoadFromYaml(invalid.string(), &config, &error));
    EXPECT_NE(error.find("position_query_interval_ms"), std::string::npos);
    std::filesystem::remove(invalid);
}

TEST(CtpConfigLoaderTest, LoadsFlowBreakerAndAuditSettings) {
    const auto config_path = WriteTempConfig(
        "ctp:\n"
        "  environment: sim\n"
        "  is_production_mode: false\n"
        "  broker_id: \"9999\"\n"
        "  user_id: \"191202\"\n"
        "  investor_id: \"191202\"\n"
        "  market_front: \"tcp://127.0.0.1:40011\"\n"
        "  trader_front: \"tcp://127.0.0.1:40001\"\n"
        "  password: \"plain-secret\"\n"
        "  settlement_confirm_required: true\n"
        "  metrics_enabled: true\n"
        "  metrics_port: 18080\n"
        "  order_insert_rate_per_sec: 60\n"
        "  order_cancel_rate_per_sec: 55\n"
        "  query_rate_per_sec: 6\n"
        "  order_bucket_capacity: 30\n"
        "  cancel_bucket_capacity: 25\n"
        "  query_bucket_capacity: 7\n"
        "  breaker_failure_threshold: 9\n"
        "  breaker_timeout_ms: 1200\n"
        "  breaker_half_open_timeout_ms: 6000\n"
        "  breaker_strategy_enabled: true\n"
        "  breaker_account_enabled: true\n"
        "  breaker_system_enabled: true\n"
        "  recovery_quiet_period_ms: 3500\n"
        "  gateway_reconnect_cycle_cooldown_ms: 9000\n"
        "  session_gate_enabled: true\n"
        "  max_signal_age_ms: 2100\n"
        "  max_market_tick_age_ms: 6100\n"
        "  open_session_end_guard_ms: 31000\n"
        "  recovery_query_timeout_ms: 32000\n"
        "  open_reenable_stability_ms: 33000\n"
        "  market_bar_allowed_lateness_ms: 3500\n"
        "  market_bar_poll_interval_ms: 100\n"
        "  market_bar_checkpoint_interval_ms: 1000\n"
        "  market_event_delay_hard_ms: 5000\n"
        "  require_complete_timeframe_bar: true\n"
        "  kafka_bootstrap_servers: \"127.0.0.1:9092\"\n"
        "  kafka_topic_ticks: \"market.ticks.v1\"\n"
        "  clickhouse_dsn: \"clickhouse://localhost:9000/default\"\n"
        "  audit_hot_days: 7\n"
        "  audit_cold_days: 180\n");

    CtpFileConfig config;
    std::string error;
    ASSERT_TRUE(CtpConfigLoader::LoadFromYaml(config_path.string(), &config, &error)) << error;

    EXPECT_TRUE(config.runtime.settlement_confirm_required);
    EXPECT_TRUE(config.runtime.metrics_enabled);
    EXPECT_EQ(config.runtime.metrics_port, 18080);
    EXPECT_EQ(config.runtime.order_insert_rate_per_sec, 60);
    EXPECT_EQ(config.runtime.order_cancel_rate_per_sec, 55);
    EXPECT_EQ(config.runtime.query_rate_per_sec, 6);
    EXPECT_EQ(config.query_rate_limit_qps, 6);
    EXPECT_EQ(config.runtime.order_bucket_capacity, 30);
    EXPECT_EQ(config.runtime.cancel_bucket_capacity, 25);
    EXPECT_EQ(config.runtime.query_bucket_capacity, 7);
    EXPECT_EQ(config.runtime.breaker_failure_threshold, 9);
    EXPECT_EQ(config.runtime.breaker_timeout_ms, 1200);
    EXPECT_EQ(config.runtime.breaker_half_open_timeout_ms, 6000);
    EXPECT_EQ(config.runtime.recovery_quiet_period_ms, 3500);
    EXPECT_EQ(config.runtime.reconnect_cycle_cooldown_ms, 9000);
    EXPECT_TRUE(config.execution.session_gate_enabled);
    EXPECT_EQ(config.execution.max_signal_age_ms, 2100);
    EXPECT_EQ(config.execution.max_market_tick_age_ms, 6100);
    EXPECT_EQ(config.execution.open_session_end_guard_ms, 31000);
    EXPECT_EQ(config.execution.recovery_query_timeout_ms, 32000);
    EXPECT_EQ(config.execution.open_reenable_stability_ms, 33000);
    EXPECT_EQ(config.market_bar.allowed_lateness_ms, 3500);
    EXPECT_EQ(config.market_bar.poll_interval_ms, 100);
    EXPECT_EQ(config.market_bar.checkpoint_interval_ms, 1000);
    EXPECT_EQ(config.market_bar.event_delay_hard_ms, 5000);
    EXPECT_TRUE(config.market_bar.require_complete_timeframe_bar);
    EXPECT_EQ(config.runtime.kafka_bootstrap_servers, "127.0.0.1:9092");
    EXPECT_EQ(config.runtime.kafka_topic_ticks, "market.ticks.v1");
    EXPECT_EQ(config.runtime.clickhouse_dsn, "clickhouse://localhost:9000/default");
    EXPECT_EQ(config.runtime.audit_hot_days, 7);
    EXPECT_EQ(config.runtime.audit_cold_days, 180);

    std::filesystem::remove(config_path);
}

TEST(CtpConfigLoaderTest, LoadsAndValidatesLoggingSettings) {
    const auto config_path = WriteTempConfig(
        "ctp:\n"
        "  environment: sim\n"
        "  is_production_mode: false\n"
        "  broker_id: \"9999\"\n"
        "  user_id: \"191202\"\n"
        "  investor_id: \"191202\"\n"
        "  market_front: \"tcp://127.0.0.1:40011\"\n"
        "  trader_front: \"tcp://127.0.0.1:40001\"\n"
        "  password: \"plain-secret\"\n"
        "  log_level: \"warn\"\n"
        "  log_sink: \"stdout\"\n");

    CtpFileConfig config;
    std::string error;
    ASSERT_TRUE(CtpConfigLoader::LoadFromYaml(config_path.string(), &config, &error)) << error;
    EXPECT_EQ(config.runtime.log_level, "warn");
    EXPECT_EQ(config.runtime.log_sink, "stdout");
    std::filesystem::remove(config_path);

    const auto invalid_level = WriteTempConfig(
        "ctp:\n"
        "  environment: sim\n"
        "  is_production_mode: false\n"
        "  broker_id: \"9999\"\n"
        "  user_id: \"191202\"\n"
        "  investor_id: \"191202\"\n"
        "  market_front: \"tcp://127.0.0.1:40011\"\n"
        "  trader_front: \"tcp://127.0.0.1:40001\"\n"
        "  password: \"plain-secret\"\n"
        "  log_level: \"verbose\"\n");
    error.clear();
    EXPECT_FALSE(CtpConfigLoader::LoadFromYaml(invalid_level.string(), &config, &error));
    EXPECT_NE(error.find("log_level"), std::string::npos);
    std::filesystem::remove(invalid_level);

    const auto invalid_sink = WriteTempConfig(
        "ctp:\n"
        "  environment: sim\n"
        "  is_production_mode: false\n"
        "  broker_id: \"9999\"\n"
        "  user_id: \"191202\"\n"
        "  investor_id: \"191202\"\n"
        "  market_front: \"tcp://127.0.0.1:40011\"\n"
        "  trader_front: \"tcp://127.0.0.1:40001\"\n"
        "  password: \"plain-secret\"\n"
        "  log_sink: \"file\"\n");
    error.clear();
    EXPECT_FALSE(CtpConfigLoader::LoadFromYaml(invalid_sink.string(), &config, &error));
    EXPECT_NE(error.find("log_sink"), std::string::npos);
    std::filesystem::remove(invalid_sink);
}

TEST(CtpConfigLoaderTest, LoadsNestedMarketStateDetectorConfig) {
    const auto config_path = WriteTempConfig(
        "ctp:\n"
        "  environment: sim\n"
        "  is_production_mode: false\n"
        "  broker_id: \"9999\"\n"
        "  user_id: \"191202\"\n"
        "  investor_id: \"191202\"\n"
        "  market_front: \"tcp://127.0.0.1:40011\"\n"
        "  trader_front: \"tcp://127.0.0.1:40001\"\n"
        "  password: \"plain-secret\"\n"
        "  market_state_detector:\n"
        "    adx_period: 7\n"
        "    adx_strong_threshold: 45\n"
        "    adx_weak_lower: 20\n"
        "    adx_weak_upper: 35\n"
        "    kama_er_period: 8\n"
        "    kama_fast_period: 2\n"
        "    kama_slow_period: 21\n"
        "    kama_er_strong: 0.7\n"
        "    kama_er_weak_lower: 0.25\n"
        "    atr_period: 9\n"
        "    require_adx_for_trend: false\n"
        "    use_kama_er: true\n");

    CtpFileConfig config;
    std::string error;
    ASSERT_TRUE(CtpConfigLoader::LoadFromYaml(config_path.string(), &config, &error)) << error;
    EXPECT_EQ(config.market_state_detector.adx_period, 7);
    EXPECT_DOUBLE_EQ(config.market_state_detector.adx_strong_threshold, 45.0);
    EXPECT_EQ(config.market_state_detector.kama_er_period, 8);
    EXPECT_FALSE(config.market_state_detector.require_adx_for_trend);
    std::filesystem::remove(config_path);
}

TEST(CtpConfigLoaderTest, IgnoresDeprecatedMarketStateFlatGateKeys) {
    const auto config_path = WriteTempConfig(
        "ctp:\n"
        "  environment: sim\n"
        "  is_production_mode: false\n"
        "  broker_id: \"9999\"\n"
        "  user_id: \"191202\"\n"
        "  investor_id: \"191202\"\n"
        "  market_front: \"tcp://127.0.0.1:40011\"\n"
        "  trader_front: \"tcp://127.0.0.1:40001\"\n"
        "  password: \"plain-secret\"\n"
        "  adx_period: 6\n"
        "  atr_flat_ratio: 0.003\n"
        "  min_bars_for_flat: 5\n"
        "  market_state_detector:\n"
        "    adx_period: 11\n"
        "    atr_flat_ratio: 0.0015\n"
        "    min_bars_for_flat: 12\n");

    CtpFileConfig config;
    std::string error;
    ASSERT_TRUE(CtpConfigLoader::LoadFromYaml(config_path.string(), &config, &error)) << error;
    EXPECT_EQ(config.market_state_detector.adx_period, 11);
    std::filesystem::remove(config_path);
}

TEST(CtpConfigLoaderTest, RejectsInvalidMarketStateDetectorConfig) {
    const auto invalid_period = WriteTempConfig(
        "ctp:\n"
        "  environment: sim\n"
        "  is_production_mode: false\n"
        "  broker_id: \"9999\"\n"
        "  user_id: \"191202\"\n"
        "  investor_id: \"191202\"\n"
        "  market_front: \"tcp://127.0.0.1:40011\"\n"
        "  trader_front: \"tcp://127.0.0.1:40001\"\n"
        "  password: \"plain-secret\"\n"
        "  market_state_detector:\n"
        "    adx_period: 0\n");

    CtpFileConfig config;
    std::string error;
    EXPECT_FALSE(CtpConfigLoader::LoadFromYaml(invalid_period.string(), &config, &error));
    EXPECT_NE(error.find("market_state_detector"), std::string::npos);
    std::filesystem::remove(invalid_period);
}

TEST(CtpConfigLoaderTest, LoadsMarketStateDetectorByProductOverrides) {
    const auto config_path = WriteTempConfig(
        "ctp:\n"
        "  environment: sim\n"
        "  is_production_mode: false\n"
        "  broker_id: \"9999\"\n"
        "  user_id: \"191202\"\n"
        "  investor_id: \"191202\"\n"
        "  market_front: \"tcp://127.0.0.1:40011\"\n"
        "  trader_front: \"tcp://127.0.0.1:40001\"\n"
        "  password: \"plain-secret\"\n"
        "  market_state_detector:\n"
        "    adx_period: 9\n"
        "    require_adx_for_trend: false\n"
        "  market_state_detector_by_product:\n"
        "    c:\n"
        "      use_kama_er: false\n"
        "    HC:\n"
        "      adx_period: 21\n"
        "      require_adx_for_trend: true\n");

    CtpFileConfig config;
    std::string error;
    ASSERT_TRUE(CtpConfigLoader::LoadFromYaml(config_path.string(), &config, &error)) << error;
    EXPECT_EQ(config.market_state_detector.adx_period, 9);
    EXPECT_FALSE(config.market_state_detector.require_adx_for_trend);
    ASSERT_EQ(config.market_state_detector_by_product.size(), 2U);

    const auto c_it = config.market_state_detector_by_product.find("c");
    ASSERT_NE(c_it, config.market_state_detector_by_product.end());
    EXPECT_EQ(c_it->second.adx_period, 9);
    EXPECT_FALSE(c_it->second.require_adx_for_trend);
    EXPECT_FALSE(c_it->second.use_kama_er);

    const auto hc_it = config.market_state_detector_by_product.find("hc");
    ASSERT_NE(hc_it, config.market_state_detector_by_product.end());
    EXPECT_EQ(hc_it->second.adx_period, 21);
    EXPECT_TRUE(hc_it->second.require_adx_for_trend);

    const MarketStateDetectorConfig& resolved_c = ResolveMarketStateDetectorConfig(
        "DCE.c2607", config.market_state_detector, config.market_state_detector_by_product);
    const MarketStateDetectorConfig& resolved_hc = ResolveMarketStateDetectorConfig(
        "SHFE.hc2610", config.market_state_detector, config.market_state_detector_by_product);
    EXPECT_FALSE(resolved_c.use_kama_er);
    EXPECT_EQ(resolved_hc.adx_period, 21);
    std::filesystem::remove(config_path);
}

TEST(CtpConfigLoaderTest, RejectsInvalidMarketStateDetectorByProductConfig) {
    const auto invalid_product = WriteTempConfig(
        "ctp:\n"
        "  environment: sim\n"
        "  is_production_mode: false\n"
        "  broker_id: \"9999\"\n"
        "  user_id: \"191202\"\n"
        "  investor_id: \"191202\"\n"
        "  market_front: \"tcp://127.0.0.1:40011\"\n"
        "  trader_front: \"tcp://127.0.0.1:40001\"\n"
        "  password: \"plain-secret\"\n"
        "  market_state_detector_by_product:\n"
        "    c2607:\n"
        "      require_adx_for_trend: true\n");

    CtpFileConfig config;
    std::string error;
    EXPECT_FALSE(CtpConfigLoader::LoadFromYaml(invalid_product.string(), &config, &error));
    EXPECT_NE(error.find("market_state_detector_by_product product"), std::string::npos);
    std::filesystem::remove(invalid_product);

    const auto invalid_threshold = WriteTempConfig(
        "ctp:\n"
        "  environment: sim\n"
        "  is_production_mode: false\n"
        "  broker_id: \"9999\"\n"
        "  user_id: \"191202\"\n"
        "  investor_id: \"191202\"\n"
        "  market_front: \"tcp://127.0.0.1:40011\"\n"
        "  trader_front: \"tcp://127.0.0.1:40001\"\n"
        "  password: \"plain-secret\"\n"
        "  market_state_detector_by_product:\n"
        "    hc:\n"
        "      kama_er_weak_lower: 0.8\n"
        "      kama_er_strong: 0.6\n");

    error.clear();
    EXPECT_FALSE(CtpConfigLoader::LoadFromYaml(invalid_threshold.string(), &config, &error));
    EXPECT_NE(error.find("market_state_detector_by_product config"), std::string::npos);
    std::filesystem::remove(invalid_threshold);
}

}  // namespace
}  // namespace quant_hft
