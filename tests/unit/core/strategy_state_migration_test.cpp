#include "quant_hft/config/strategy_state_migration.h"

#include <gtest/gtest.h>

#include <chrono>
#include <filesystem>
#include <fstream>

#include "quant_hft/common/position_projection.h"
#include "quant_hft/config/strict_yaml.h"
#include "quant_hft/core/host_adapters/filesystem_configuration_reader.h"
#include "quant_hft/strategy/state_envelope.h"
#include "quant_hft/strategy/state_persistence.h"
#include "quant_hft/strategy/strategy_main_config_loader.h"

namespace quant_hft {
namespace {
namespace fs = std::filesystem;
class StateMigrationFixture : public ::testing::Test {
   protected:
    fs::path directory;
    DeploymentConfig deployment;
    StrategyStateMigrationOptions options;
    StrategyState original;
    std::string source_bytes;
    void Write(const fs::path& path, const std::string& content) {
        fs::create_directories(path.parent_path());
        std::ofstream output(path);
        output << content;
        ASSERT_TRUE(output.good());
    }
    std::string Read(const fs::path& path) {
        std::ifstream input(path);
        return std::string(std::istreambuf_iterator<char>(input), {});
    }
    fs::path SourceFile() {
        return directory / "old/strategy_state__account__stable_instance.json";
    }
    void SetUp() override {
        BindFilesystemConfigurationReader();
        directory = fs::temp_directory_path() /
                    ("quant-state-migration-unit-" +
                     std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
        fs::create_directories(directory);
        Write(directory / "schema.yaml", R"(version: 1
algorithms:
  KamaTrendStrategy:
    parameters:
      er_period: {type: int, min: 1, default: 10}
      risk_per_trade_pct: {type: float, min: 0, max: 1, default: 0.005}
)");
        Write(directory / "main.yaml", R"(run_type: sim
market_state_mode: false
backtest:
  initial_equity: 100000
composite:
  product_id: hc
  merge_rule: kPriority
  enable_non_backtest: true
  sub_strategies:
    - id: kama
      enabled: true
      type: KamaTrendStrategy
      timeframe_minutes: 5
      config_path: sub.yaml
)");
        Write(directory / "sub.yaml",
              "params:\n  id: kama\n  er_period: 10\n  risk_per_trade_pct: 0.005\n");
        std::string error;
        ASSERT_TRUE(MigrateLegacyParameterSet(
            (directory / "main.yaml").string(), "sim", "test_v001", "kama_trend@1.0.0",
            (directory / "schema.yaml").string(), (directory / "parameters").string(), &error))
            << error;
        ParameterSet parameters;
        const auto param_bytes = Read(directory / "parameters/test_v001.yaml");
        ASSERT_TRUE(ParseParameterSetYaml(param_bytes, Read(directory / "schema.yaml"), &parameters,
                                          &error))
            << error;
        // Exercise the real configuration resolver: its materialized parameter hash
        // deliberately differs from the hash of the formatted parameter file.
        YAML::Node manifest;
        manifest["schema_version"] = 1;
        manifest["version"] = "1.0.0";
        manifest["strategy_releases"].push_back(parameters.strategy_release);
        manifest["files"]["share/quant_strategies/1.0.0/schemas/atomic_parameters.yaml"] =
            ConfigContentSha256(Read(directory / "schema.yaml"));
        Write(directory / "manifest.json", YAML::Dump(manifest));
        Write(directory / "connection.yaml", "runtime: {enable_real_api: false}\n");
        YAML::Node root;
        root["schema_version"] = 1;
        root["package"]["version"] = "1.0.0";
        const auto reference = [&](const std::string& path) {
            YAML::Node node;
            node["path"] = path;
            node["sha256"] = ConfigContentSha256(Read(directory / path));
            return node;
        };
        root["package"]["manifest"] = reference("manifest.json");
        root["package"]["parameter_schema"] = reference("schema.yaml");
        root["parameter_sets"]["test_v001"] = reference("parameters/test_v001.yaml");
        root["risk_profiles"]["trial"] = YAML::Load(
            "{max_order_volume: 2, max_order_notional: 1000000, "
            "max_margin_to_equity_ratio: 0.4, forbid_open_windows: ''}");
        root["accounts"]["sim_a"] = YAML::Load(
            "{environment: simnow, broker_id: '9999', account_id: account, "
            "connection_config: connection.yaml, credential_ref: outside.env, "
            "runtime_root: runtime, active_host: test-host, risk_profile_ref: trial}");
        root["capital_allocations"]["trial"] =
            YAML::Load("{account_ref: sim_a, initial_capital: 25000}");
        root["instances"].push_back(
            YAML::Load("{instance_id: stable_instance, account_ref: sim_a, "
                       "strategy_release: 'kama_trend@1.0.0', parameter_set: test_v001, "
                       "capital_allocation_ref: trial, risk_profile_ref: trial}"));
        Write(directory / "deployment.yaml", YAML::Dump(root));
        ASSERT_TRUE(
            LoadDeploymentConfig((directory / "deployment.yaml").string(), &deployment, &error))
            << error;
        EXPECT_NE(deployment.instances.front().parameter_hash, ConfigContentSha256(param_bytes));
        StrategyMainConfig legacy;
        ASSERT_TRUE(LoadStrategyMainConfig((directory / "main.yaml").string(), &legacy, &error))
            << error;
        CompositeStrategy strategy(legacy.composite);
        strategy.Initialize({"stable_instance", "account", {{"run_type", "sim"}}, {}});
        ASSERT_TRUE(strategy.SaveState(&original, &error)) << error;
        original["last_committed_trade_seq"] = "827";
        original["last_bar_end_ns"] = "1788937260000000000";
        FileStrategyStatePersistence source((directory / "old").string(), "strategy_state", 0);
        ASSERT_TRUE(source.SaveStrategyState("account", "stable_instance", original, &error))
            << error;
        source_bytes = Read(SourceFile());
        options.account_ref = "sim_a";
        options.instance_id = "stable_instance";
        options.legacy_instance_id = "stable_instance";
        options.legacy_state_directory = (directory / "old").string();
        options.parameter_migration_report =
            (directory / "parameters/test_v001.migration.json").string();
        options.legacy_main_config = (directory / "main.yaml").string();
        options.output_directory = (directory / "new").string();
    }
    void TearDown() override {
        if (!directory.empty() && directory.parent_path() == fs::temp_directory_path() &&
            directory.filename().string().rfind("quant-state-migration-unit-", 0) == 0)
            fs::remove_all(directory);
    }
};
TEST_F(StateMigrationFixture, PreservesOriginalPayloadWatermarksAndProducesAuditableNewDirectory) {
    std::string error;
    ASSERT_TRUE(MigrateStrategyState(deployment, options, &error)) << error;
    EXPECT_EQ(Read(SourceFile()), source_bytes);
    FileStrategyStatePersistence destination(options.output_directory, options.key_prefix, 0);
    StrategyState wrapped, restored;
    ASSERT_TRUE(destination.LoadStrategyState("account", "stable_instance", &wrapped, &error));
    const auto& instance = deployment.instances.front();
    ASSERT_TRUE(UnwrapStrategyState(
        wrapped,
        {"stable_instance", "account", instance.strategy_release, instance.parameter_hash, "1"},
        &restored, &error));
    EXPECT_EQ(restored, original);
    const auto report = config_detail::Parse(Read(directory / "new/migration_report.json"));
    EXPECT_EQ(report["payload_sha256_before"].as<std::string>(),
              report["payload_sha256_after"].as<std::string>());
    EXPECT_EQ(report["old_state_sha256"].as<std::string>(), ConfigContentSha256(source_bytes));
    EXPECT_FALSE(report["owner_renamed"].as<bool>());
    EXPECT_FALSE(report["trade_facts_changed"].as<bool>());
    EXPECT_FALSE(MigrateStrategyState(deployment, options, &error));
    EXPECT_NE(error.find("new directory"), std::string::npos);
    EXPECT_EQ(Read(SourceFile()), source_bytes);
}
TEST_F(StateMigrationFixture, RejectsOwnerRenameAndUnknownAccountWithoutOutput) {
    std::string error;
    options.legacy_instance_id = "old_owner";
    EXPECT_FALSE(MigrateStrategyState(deployment, options, &error));
    EXPECT_NE(error.find("renaming"), std::string::npos);
    options.legacy_instance_id = options.instance_id;
    options.account_ref = "other_account";
    EXPECT_FALSE(MigrateStrategyState(deployment, options, &error));
    EXPECT_FALSE(fs::exists(options.output_directory));
    EXPECT_EQ(Read(SourceFile()), source_bytes);
}
TEST_F(StateMigrationFixture, RejectsMissingEvidenceAndChangedSubfile) {
    std::string error;
    auto original_path = options.parameter_migration_report;
    options.parameter_migration_report = (directory / "missing-report.json").string();
    EXPECT_FALSE(MigrateStrategyState(deployment, options, &error));
    options.parameter_migration_report = original_path;
    Write(directory / "sub.yaml", Read(directory / "sub.yaml") + "# changed\n");
    EXPECT_FALSE(MigrateStrategyState(deployment, options, &error));
    EXPECT_NE(error.find("subfile hash"), std::string::npos);
    EXPECT_FALSE(fs::exists(options.output_directory));
    EXPECT_EQ(Read(SourceFile()), source_bytes);
}
TEST_F(StateMigrationFixture, RejectsForgedReportWhichCannotReproduceFormalParameters) {
    std::string error;
    auto report = config_detail::Parse(Read(options.parameter_migration_report));
    deployment.instances.front().parameter_hash = std::string(64, 'a');
    report["parameter_sha256"] = deployment.instances.front().parameter_hash;
    Write(options.parameter_migration_report, YAML::Dump(report));
    EXPECT_FALSE(MigrateStrategyState(deployment, options, &error));
    EXPECT_NE(error.find("recomputed legacy parameters"), std::string::npos);
    EXPECT_FALSE(fs::exists(options.output_directory));
    EXPECT_EQ(Read(SourceFile()), source_bytes);
}
TEST_F(StateMigrationFixture, RejectsUnmappedPositionOwnerAndUnknownComponent) {
    std::string error;
    FileStrategyStatePersistence source((directory / "old").string(), "strategy_state", 0);
    Position position;
    position.account_id = "account";
    position.strategy_id = "component_owner";
    position.symbol = "hc2610";
    position.exchange = "SHFE";
    position.long_qty = 1;
    position.avg_long_price = 3500;
    auto state = original;
    state["committed_position." + PositionProjectionKey(position)] =
        EncodePositionProjection(position);
    ASSERT_TRUE(source.SaveStrategyState("account", "stable_instance", state, &error));
    EXPECT_FALSE(MigrateStrategyState(deployment, options, &error));
    EXPECT_NE(error.find("economic position owner"), std::string::npos);
    state = original;
    state["atomic.removed_component.state"] = "1";
    ASSERT_TRUE(source.SaveStrategyState("account", "stable_instance", state, &error));
    EXPECT_FALSE(MigrateStrategyState(deployment, options, &error));
    EXPECT_NE(error.find("atomic state component"), std::string::npos);
    EXPECT_FALSE(fs::exists(options.output_directory));
}
TEST_F(StateMigrationFixture, RejectsCorruptStateAndNestedOutput) {
    std::string error;
    auto state = config_detail::Parse(source_bytes);
    state["state"]["composite.schema_version"] = "999999";
    // A pre-existing envelope cannot be silently re-attributed by this migration.
    state["state"]["__host.instance_id"] = "foreign";
    Write(SourceFile(), config_detail::CanonicalJson(state));
    EXPECT_FALSE(MigrateStrategyState(deployment, options, &error));
    EXPECT_FALSE(fs::exists(options.output_directory));
    Write(SourceFile(), source_bytes);
    options.output_directory = (directory / "old/new").string();
    EXPECT_FALSE(MigrateStrategyState(deployment, options, &error));
    EXPECT_NE(error.find("isolated"), std::string::npos);
    EXPECT_EQ(Read(SourceFile()), source_bytes);
}
}  // namespace
}  // namespace quant_hft
