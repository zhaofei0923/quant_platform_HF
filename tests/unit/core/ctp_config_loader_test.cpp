#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <string>

#include <gtest/gtest.h>

#include "quant_hft/core/ctp_config_loader.h"

namespace quant_hft {
namespace {

class ScopedEnvVar {
public:
    ScopedEnvVar(std::string key, const char* value)
        : key_(std::move(key)) {
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
    const auto token = std::to_string(
        std::chrono::steady_clock::now().time_since_epoch().count());
    const auto path = std::filesystem::temp_directory_path() /
                      ("quant_hft_ctp_config_loader_test_" + token + ".yaml");
    std::ofstream out(path);
    out << body;
    return path;
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
    ASSERT_TRUE(CtpConfigLoader::LoadFromYaml(config_path.string(), &config, &error))
        << error;

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

TEST(CtpConfigLoaderTest, SupportsDisablingTerminalAuth) {
    const ScopedEnvVar password_env("CTP_TEST_PASSWORD", "env-secret");
    const auto config_path = WriteTempConfig(
        "ctp:\n"
        "  environment: sim\n"
        "  is_production_mode: true\n"
        "  enable_real_api: true\n"
        "  enable_terminal_auth: false\n"
        "  broker_id: \"9999\"\n"
        "  user_id: \"191202\"\n"
        "  investor_id: \"191202\"\n"
        "  market_front: \"tcp://182.254.243.31:30012\"\n"
        "  trader_front: \"tcp://182.254.243.31:30002\"\n"
        "  password_env: \"CTP_TEST_PASSWORD\"\n");

    CtpFileConfig config;
    std::string error;
    ASSERT_TRUE(CtpConfigLoader::LoadFromYaml(config_path.string(), &config, &error))
        << error;
    EXPECT_TRUE(config.runtime.enable_real_api);
    EXPECT_FALSE(config.runtime.enable_terminal_auth);

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

TEST(CtpConfigLoaderTest, LoadsStrategyBridgeKeysAndSplitsLists) {
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
        "  strategy_poll_interval_ms: 350\n"
        "  account_id: \"sim-account\"\n");

    CtpFileConfig config;
    std::string error;
    ASSERT_TRUE(CtpConfigLoader::LoadFromYaml(config_path.string(), &config, &error))
        << error;

    ASSERT_EQ(config.instruments.size(), 2U);
    EXPECT_EQ(config.instruments[0], "SHFE.ag2406");
    EXPECT_EQ(config.instruments[1], "SHFE.rb2405");
    ASSERT_EQ(config.strategy_ids.size(), 2U);
    EXPECT_EQ(config.strategy_ids[0], "demo");
    EXPECT_EQ(config.strategy_ids[1], "alpha");
    EXPECT_EQ(config.strategy_poll_interval_ms, 350);
    EXPECT_EQ(config.account_id, "sim-account");

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
        "  risk_default_max_order_volume: 12\n"
        "  risk_default_max_order_notional: 200000\n"
        "  risk_default_max_active_orders: 4\n"
        "  risk_default_max_position_notional: 900000\n"
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
        "  risk_rule_ag_open_max_order_volume: 2\n"
        "  risk_rule_ag_open_max_order_notional: 12000\n"
        "  risk_rule_ag_open_max_active_orders: 1\n"
        "  risk_rule_ag_open_max_position_notional: 80000\n"
        "  risk_rule_ag_open_version: \"2026.03\"\n"
        "  risk_rule_acc_guard_account_id: \"sim-account\"\n"
        "  risk_rule_acc_guard_max_order_volume: 5\n"
        "  risk_rule_acc_guard_max_order_notional: 50000\n");

    CtpFileConfig config;
    std::string error;
    ASSERT_TRUE(CtpConfigLoader::LoadFromYaml(config_path.string(), &config, &error))
        << error;

    EXPECT_EQ(config.execution.mode, ExecutionMode::kSliced);
    EXPECT_EQ(config.execution.algo, ExecutionAlgo::kTwap);
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

    EXPECT_EQ(config.risk.default_max_order_volume, 12);
    EXPECT_DOUBLE_EQ(config.risk.default_max_order_notional, 200000.0);
    EXPECT_EQ(config.risk.default_max_active_orders, 4);
    EXPECT_DOUBLE_EQ(config.risk.default_max_position_notional, 900000.0);
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
    EXPECT_EQ(config.risk.rules[0].max_order_volume, 2);
    EXPECT_EQ(config.risk.rules[0].max_active_orders, 1);
    EXPECT_DOUBLE_EQ(config.risk.rules[0].max_position_notional, 80000.0);
    EXPECT_EQ(config.risk.rules[1].account_id, "sim-account");
    EXPECT_EQ(config.risk.rules[1].rule_group, "acc_guard");
    EXPECT_EQ(config.risk.rules[1].policy_id, "policy.global");

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
    ASSERT_TRUE(CtpConfigLoader::LoadFromYaml(config_path.string(), &config, &error))
        << error;

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
    EXPECT_FALSE(
        CtpConfigLoader::LoadFromYaml(negative_cancel_after.string(), &config, &error));
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
    EXPECT_FALSE(
        CtpConfigLoader::LoadFromYaml(invalid_scan_interval.string(), &config, &error));
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

}  // namespace
}  // namespace quant_hft
