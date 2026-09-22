#include "quant_hft/config/strategy_release_migration.h"

#include <fcntl.h>
#include <gtest/gtest.h>
#include <sys/wait.h>
#include <unistd.h>

#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <string>
#include <utility>
#include <vector>

#include "quant_hft/common/position_projection.h"
#include "quant_hft/config/deployment_config.h"
#include "quant_hft/config/strict_yaml.h"
#include "quant_hft/core/host_adapters/filesystem_configuration_reader.h"
#include "quant_hft/strategy/state_envelope.h"
#include "quant_hft/strategy/state_persistence.h"

namespace quant_hft {
namespace {
namespace fs = std::filesystem;

TEST(ReleaseMigrationConfigTest, RejectsUnsupportedVersionBeforeReadingDeployment) {
    const auto missing =
        fs::temp_directory_path() /
        ("quant-release-unsupported-version-" +
         std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()) + ".yaml");
    ASSERT_FALSE(fs::exists(missing));
    DeploymentConfig deployment;
    std::string error;
    EXPECT_FALSE(LoadDeploymentConfigForMigration(missing.string(), "2.0.0", &deployment, &error));
    EXPECT_EQ(error, "unsupported offline migration package version");
    EXPECT_FALSE(fs::exists(missing));
}

class ReleaseMigrationCliTest : public ::testing::Test {
   protected:
    void SetUp() override {
        auto pattern = (fs::temp_directory_path() / "quant-release-cli-XXXXXX").string();
        ASSERT_NE(::mkdtemp(pattern.data()), nullptr);
        root_ = pattern;
        binary_ = fs::canonical("/proc/self/exe").parent_path() / "strategy_state_migrate_cli";
        ASSERT_TRUE(fs::is_regular_file(binary_));
    }

    void TearDown() override { fs::remove_all(root_); }

    std::pair<int, std::string> Run(std::vector<std::string> args) {
        const auto output_path = root_ / "cli.log";
        const pid_t child = ::fork();
        if (child < 0) return {-1, "fork failed"};
        if (child == 0) {
            ::clearenv();
            if (::chdir(root_.c_str()) != 0) ::_exit(125);
            const int output = ::open(output_path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0600);
            if (output < 0 || ::dup2(output, STDOUT_FILENO) < 0 ||
                ::dup2(output, STDERR_FILENO) < 0)
                ::_exit(126);
            ::close(output);
            std::vector<char*> argv{const_cast<char*>(binary_.c_str())};
            for (auto& arg : args) argv.push_back(arg.data());
            argv.push_back(nullptr);
            ::execv(binary_.c_str(), argv.data());
            ::_exit(127);
        }
        int status = 0;
        if (::waitpid(child, &status, 0) < 0) return {-1, "waitpid failed"};
        std::ifstream input(output_path);
        const std::string output((std::istreambuf_iterator<char>(input)), {});
        return {WIFEXITED(status) ? WEXITSTATUS(status) : -1, output};
    }

    fs::path root_;
    fs::path binary_;
};

TEST_F(ReleaseMigrationCliTest, RejectsMixedLegacyAndFormalReleaseModesBeforeFileAccess) {
    const auto result =
        Run({"--source-deployment", "missing-old.yaml", "--deployment", "missing-new.yaml",
             "--source-package-sha256", std::string(64, 'a'), "--account-ref", "sim_a",
             "--instance-id", "stable_instance", "--source-state-dir", "missing-frozen",
             "--output-dir", "output", "--legacy-state-dir", "missing-legacy"});
    EXPECT_EQ(result.first, 1) << result.second;
    EXPECT_NE(result.second.find("cannot mix migration modes"), std::string::npos) << result.second;
    EXPECT_FALSE(fs::exists(root_ / "output"));
}

TEST_F(ReleaseMigrationCliTest, RejectsFormalReleaseArgumentsWithoutSourceDeployment) {
    const auto result =
        Run({"--deployment", "missing-new.yaml", "--source-package-sha256", std::string(64, 'a'),
             "--account-ref", "sim_a", "--instance-id", "stable_instance", "--source-state-dir",
             "missing-frozen", "--output-dir", "output"});
    EXPECT_EQ(result.first, 1) << result.second;
    EXPECT_NE(result.second.find("formal release migration requires --source-deployment"),
              std::string::npos)
        << result.second;
    EXPECT_FALSE(fs::exists(root_ / "output"));
}

class ReleaseMigrationFixture : public ::testing::Test {
   protected:
    fs::path directory;
    StrategyReleaseMigrationOptions options;
    DeploymentConfig source, target;
    StrategyState original;
    std::string source_bytes;
    std::string Read(const fs::path& path) {
        std::ifstream input(path);
        return std::string(std::istreambuf_iterator<char>(input), {});
    }
    void Write(const fs::path& path, const std::string& text) {
        fs::create_directories(path.parent_path());
        std::ofstream output(path);
        output << text;
        ASSERT_TRUE(output.good());
    }
    fs::path SourceFile() const {
        return directory / "frozen/strategy_state__account__stable_instance.json";
    }
    YAML::Node Reference(const fs::path& path) {
        YAML::Node result;
        result["path"] = path.string();
        result["sha256"] = ConfigContentSha256(Read(path));
        return result;
    }
    void SaveOriginal() {
        const auto& instance = source.instances.front();
        StrategyState wrapped;
        std::string error;
        ASSERT_TRUE(WrapStrategyState(
            original,
            {"stable_instance", "account", instance.strategy_release, instance.parameter_hash, "1"},
            &wrapped, &error))
            << error;
        FileStrategyStatePersistence persistence((directory / "frozen").string(), "strategy_state",
                                                 0);
        ASSERT_TRUE(persistence.SaveStrategyState("account", "stable_instance", wrapped, &error))
            << error;
        source_bytes = Read(SourceFile());
    }
    void ConfigureIdentityOnlyMigration() {
        options.source_deployment = (directory / "1.1.0/deployment.yaml").string();
        options.target_deployment = (directory / "1.1.1/deployment.yaml").string();
        options.source_package_sha256 =
            ConfigContentSha256(Read(directory / "intermediate_manifest.json"));
        std::string error;
        ASSERT_TRUE(
            LoadDeploymentConfigForMigration(options.source_deployment, "1.1.0", &source, &error))
            << error;
        ASSERT_TRUE(LoadDeploymentConfig(options.target_deployment, &target, &error)) << error;
        ASSERT_TRUE(VerifyDeploymentPackage(target, &error)) << error;

        CompositeStrategy strategy(target.instances.front().composite);
        StrategyContext context;
        context.account_id = "account";
        context.strategy_id = "stable_instance";
        context.metadata["ownership_mode"] = "instance";
        strategy.Initialize(context);
        ASSERT_TRUE(strategy.LoadState(original, &error)) << error;
        StrategyState upgraded;
        ASSERT_TRUE(strategy.SaveState(&upgraded, &error)) << error;
        strategy.Shutdown();
        original = std::move(upgraded);
        original["last_committed_trade_seq"] = "827";
        original["last_bar_end_ns"] = "1788937260000000000";
        SaveOriginal();
    }
    void ConfigureFlatIdentityOnlyMigration() {
        ConfigureIdentityOnlyMigration();
        CompositeStrategy strategy(target.instances.front().composite);
        StrategyContext context;
        context.account_id = "account";
        context.strategy_id = "stable_instance";
        context.metadata["ownership_mode"] = "instance";
        strategy.Initialize(context);
        StrategyState flat;
        std::string error;
        ASSERT_TRUE(strategy.SaveState(&flat, &error)) << error;
        Position position;
        position.account_id = "account";
        position.strategy_id = "stable_instance";
        position.symbol = "hc2701";
        position.exchange = "SHFE";
        position.version = 2;
        flat["committed_position." + PositionProjectionKey(position)] =
            EncodePositionProjection(position);
        ASSERT_TRUE(strategy.LoadState(flat, &error)) << error;
        ASSERT_TRUE(strategy.SaveState(&original, &error)) << error;
        strategy.Shutdown();
        original["last_committed_trade_seq"] = "827";
        original["last_bar_end_ns"] = "1788937260000000000";
        original["application_watermark"] = "preserve-exactly";
        SaveOriginal();
    }
    void SetUp() override {
        BindFilesystemConfigurationReader();
        directory = fs::temp_directory_path() /
                    ("quant-release-migration-unit-" +
                     std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
        fs::create_directories(directory);
        const fs::path package = QUANT_STRATEGIES_TEST_DATA_DIR;
        ASSERT_TRUE(fs::exists(package / "manifest.json"));
        const auto schema = package / "schemas/atomic_parameters.yaml";
        YAML::Node old_manifest;
        old_manifest["schema_version"] = 1;
        old_manifest["version"] = "1.0.0";
        old_manifest["strategy_releases"].push_back("kama_trend@1.0.0");
        old_manifest["files"]["share/quant_strategies/1.0.0/schemas/atomic_parameters.yaml"] =
            ConfigContentSha256(Read(schema));
        Write(directory / "old_manifest.json", YAML::Dump(old_manifest));
        YAML::Node intermediate_manifest;
        intermediate_manifest["schema_version"] = 1;
        intermediate_manifest["version"] = "1.1.0";
        intermediate_manifest["strategy_releases"].push_back("kama_trend@1.1.0");
        intermediate_manifest["files"]
                             ["share/quant_strategies/1.1.0/schemas/atomic_parameters.yaml"] =
                                 ConfigContentSha256(Read(schema));
        Write(directory / "intermediate_manifest.json", YAML::Dump(intermediate_manifest));
        Write(directory / "connection.yaml", "runtime: {enable_real_api: false}\n");
        for (const auto* version : {"1.0.0", "1.1.0", "1.1.1"}) {
            const std::string v = version;
            const auto parameter_file = directory / (v + "/parameters.yaml");
            Write(parameter_file,
                  "schema_version: 1\nparameter_set_id: test_v001\nstrategy_release: kama_trend@" +
                      v +
                      "\nproduct_id: hc\nmarket_state_mode: false\nmerge_rule: "
                      "kPriority\ncomponents:\n"
                      "  - component_id: kama\n    type: KamaTrendStrategy\n    enabled: true\n"
                      "    timeframe_minutes: 5\n    entry_market_regimes: []\n    params: {}\n");
            YAML::Node root;
            root["schema_version"] = 1;
            root["package"]["version"] = v;
            const auto manifest = v == "1.0.0"   ? directory / "old_manifest.json"
                                  : v == "1.1.0" ? directory / "intermediate_manifest.json"
                                                 : package / "manifest.json";
            root["package"]["manifest"] = Reference(manifest);
            root["package"]["parameter_schema"] = Reference(schema);
            root["parameter_sets"]["test_v001"] = Reference(parameter_file);
            root["risk_profiles"]["trial"] = YAML::Load(
                "{max_order_volume: 2, max_order_notional: 1000000, "
                "max_margin_to_equity_ratio: 0.3, forbid_open_windows: '21:00-23:05'}");
            root["accounts"]["sim_a"] = YAML::Load(
                "{environment: simnow, broker_id: '9999', account_id: account, "
                "credential_ref: outside.env, runtime_root: runtime, active_host: test-host, "
                "risk_profile_ref: trial}");
            root["accounts"]["sim_a"]["connection_config"] =
                (directory / "connection.yaml").string();
            root["accounts"]["sim_a"]["credential_ref"] = (directory / "outside.env").string();
            root["accounts"]["sim_a"]["runtime_root"] = (directory / "runtime").string();
            root["capital_allocations"]["trial"] =
                YAML::Load("{account_ref: sim_a, initial_capital: 25000}");
            root["instances"].push_back(
                YAML::Load("{instance_id: stable_instance, account_ref: sim_a, strategy_release: "
                           "'kama_trend@" +
                           v +
                           "', parameter_set: test_v001, capital_allocation_ref: trial, "
                           "risk_profile_ref: trial}"));
            Write(directory / (v + "/deployment.yaml"), YAML::Dump(root));
        }
        options.source_deployment = (directory / "1.0.0/deployment.yaml").string();
        options.target_deployment = (directory / "1.1.0/deployment.yaml").string();
        options.source_package_sha256 = ConfigContentSha256(Read(directory / "old_manifest.json"));
        options.account_ref = "sim_a";
        options.instance_id = "stable_instance";
        options.source_state_directory = (directory / "frozen").string();
        options.output_directory = (directory / "new").string();
        std::string error;
        ASSERT_TRUE(
            LoadDeploymentConfigForMigration(options.source_deployment, "1.0.0", &source, &error))
            << error;
        ASSERT_TRUE(
            LoadDeploymentConfigForMigration(options.target_deployment, "1.1.0", &target, &error))
            << error;
        CompositeStrategy strategy(target.instances.front().composite);
        StrategyContext context;
        context.account_id = "account";
        context.strategy_id = "stable_instance";
        context.metadata["ownership_mode"] = "instance";
        strategy.Initialize(context);
        ASSERT_TRUE(strategy.SaveState(&original, &error)) << error;
        strategy.Shutdown();
        for (auto it = original.begin(); it != original.end();) {
            if (it->first.rfind("atomic.kama.take_atr_by_instrument.", 0) == 0)
                it = original.erase(it);
            else
                ++it;
        }
        original["atomic.kama.version"] = "1";
        original["atomic.kama.take_profit.count"] = "1";
        original["atomic.kama.take_profit.0.instrument"] = "hc2701";
        original["atomic.kama.take_profit.0.price"] = "3510.125";
        original["atomic.kama.trailing_stop.count"] = "1";
        original["atomic.kama.trailing_stop.0.instrument"] = "hc2701";
        original["atomic.kama.trailing_stop.0.price"] = "3380.25";
        original["atomic.kama.trailing_direction.count"] = "1";
        original["atomic.kama.trailing_direction.0.instrument"] = "hc2701";
        original["atomic.kama.trailing_direction.0.direction"] = "1";
        original["net_pos.hc2701"] = "1";
        original["avg_open.hc2701"] = "3400.000000";
        original["multiplier.hc2701"] = "10.000000";
        original["owner.hc2701"] = "kama";
        // The legacy merged display value is not an authoritative owner target.
        original["take_profit.hc2701"] = "9999.000000";
        original["last_committed_trade_seq"] = "827";
        original["last_bar_end_ns"] = "1788937260000000000";
        Position position;
        position.account_id = "account";
        position.strategy_id = "stable_instance";
        position.symbol = "hc2701";
        position.exchange = "SHFE";
        position.long_qty = 1;
        position.avg_long_price = 3400;
        original["committed_position." + PositionProjectionKey(position)] =
            EncodePositionProjection(position);
        SaveOriginal();
    }
    void TearDown() override {
        if (directory.parent_path() == fs::temp_directory_path() &&
            directory.filename().string().rfind("quant-release-migration-unit-", 0) == 0)
            fs::remove_all(directory);
    }
};

TEST_F(ReleaseMigrationFixture, PreservesHeldTargetDirectionFactsAndWatermarksWithoutOverwriting) {
    std::string error;
    ASSERT_TRUE(MigrateStrategyReleaseState(options, &error)) << error;
    EXPECT_EQ(Read(SourceFile()), source_bytes);
    FileStrategyStatePersistence persistence(options.output_directory, options.key_prefix, 0);
    StrategyState wrapped, restored;
    ASSERT_TRUE(persistence.LoadStrategyState("account", "stable_instance", &wrapped, &error));
    const auto& instance = target.instances.front();
    ASSERT_TRUE(UnwrapStrategyState(
        wrapped,
        {"stable_instance", "account", instance.strategy_release, instance.parameter_hash, "1"},
        &restored, &error));
    for (const auto& entry : original)
        if (entry.first.rfind("atomic.", 0) != 0) EXPECT_EQ(restored.at(entry.first), entry.second);
    EXPECT_EQ(restored.at("atomic.kama.version"), "2");
    EXPECT_EQ(restored.at("atomic.kama.take_profit.0.price"), "3510.125");
    EXPECT_EQ(restored.at("atomic.kama.take_profit.0.direction"), "1");
    EXPECT_EQ(restored.at("atomic.kama.take_atr_by_instrument.count"), "0");
    const auto report = config_detail::Parse(Read(directory / "new/migration_report.json"));
    EXPECT_EQ(report["facts_sha256_before"].as<std::string>(),
              report["facts_sha256_after"].as<std::string>());
    EXPECT_NE(report["payload_sha256_before"].as<std::string>(),
              report["payload_sha256_after"].as<std::string>());
    EXPECT_FALSE(MigrateStrategyReleaseState(options, &error));
    EXPECT_EQ(Read(SourceFile()), source_bytes);
}
TEST_F(ReleaseMigrationFixture, IdentityOnlyUpgradePreservesExactPayloadFactsAndWatermarks) {
    ConfigureIdentityOnlyMigration();
    const auto expected_payload = original;
    const auto expected_source = source_bytes;
    std::string error;
    ASSERT_TRUE(MigrateStrategyReleaseState(options, &error)) << error;
    EXPECT_EQ(Read(SourceFile()), expected_source);
    FileStrategyStatePersistence persistence(options.output_directory, options.key_prefix, 0);
    StrategyState wrapped, restored;
    ASSERT_TRUE(persistence.LoadStrategyState("account", "stable_instance", &wrapped, &error));
    const auto& instance = target.instances.front();
    ASSERT_TRUE(UnwrapStrategyState(
        wrapped,
        {"stable_instance", "account", instance.strategy_release, instance.parameter_hash, "1"},
        &restored, &error));
    EXPECT_EQ(restored, expected_payload);
    EXPECT_EQ(restored.at("last_committed_trade_seq"), "827");
    EXPECT_EQ(restored.at("last_bar_end_ns"), "1788937260000000000");
    const auto report = config_detail::Parse(Read(directory / "new/migration_report.json"));
    EXPECT_EQ(report["migration"].as<std::string>(), "identity_only_1.1.0_to_1.1.1");
    EXPECT_TRUE(report["payload_preserved"].as<bool>());
    EXPECT_EQ(report["payload_sha256_before"].as<std::string>(),
              report["payload_sha256_after"].as<std::string>());
    EXPECT_EQ(report["facts_sha256_before"].as<std::string>(),
              report["facts_sha256_after"].as<std::string>());
    EXPECT_FALSE(report["orphan_take_profit_cleanup_allowed"].as<bool>());
    EXPECT_FALSE(MigrateStrategyReleaseState(options, &error));
    EXPECT_EQ(Read(SourceFile()), expected_source);
}
TEST_F(ReleaseMigrationFixture, IdentityOnlyUpgradePreservesFlatZeroProjectionAndAllWatermarks) {
    ConfigureFlatIdentityOnlyMigration();
    const auto expected_payload = original;
    const auto expected_source = source_bytes;
    std::string error;
    ASSERT_TRUE(MigrateStrategyReleaseState(options, &error)) << error;
    EXPECT_EQ(Read(SourceFile()), expected_source);
    FileStrategyStatePersistence persistence(options.output_directory, options.key_prefix, 0);
    StrategyState wrapped, restored;
    ASSERT_TRUE(persistence.LoadStrategyState("account", "stable_instance", &wrapped, &error));
    const auto& instance = target.instances.front();
    ASSERT_TRUE(UnwrapStrategyState(
        wrapped,
        {"stable_instance", "account", instance.strategy_release, instance.parameter_hash, "1"},
        &restored, &error));
    EXPECT_EQ(restored, expected_payload);
    EXPECT_EQ(restored.at("last_committed_trade_seq"), "827");
    EXPECT_EQ(restored.at("last_bar_end_ns"), "1788937260000000000");
    EXPECT_EQ(restored.at("application_watermark"), "preserve-exactly");
    const auto projection = std::find_if(restored.begin(), restored.end(), [](const auto& entry) {
        return entry.first.rfind("committed_position.", 0) == 0;
    });
    ASSERT_NE(projection, restored.end());
    Position position;
    ASSERT_TRUE(DecodePositionProjection(projection->second, &position));
    EXPECT_EQ(position.long_qty, 0);
    EXPECT_EQ(position.short_qty, 0);
    const auto report = config_detail::Parse(Read(directory / "new/migration_report.json"));
    EXPECT_TRUE(report["payload_preserved"].as<bool>());
    EXPECT_EQ(report["payload_sha256_before"].as<std::string>(),
              report["payload_sha256_after"].as<std::string>());
}
TEST_F(ReleaseMigrationFixture, IdentityOnlyUpgradeRejectsTargetNotLinkedIntoHost) {
    ConfigureIdentityOnlyMigration();
    const auto original_source = source_bytes;
    auto deployment = config_detail::Parse(Read(options.target_deployment));
    const auto manifest_path = directory / "1.1.1/unlinked_manifest.json";
    Write(manifest_path, Read(QUANT_STRATEGIES_TEST_DATA_DIR "/manifest.json") + "\n");
    deployment["package"]["manifest"] = Reference(manifest_path);
    Write(options.target_deployment, YAML::Dump(deployment));
    std::string error;
    EXPECT_FALSE(MigrateStrategyReleaseState(options, &error));
    EXPECT_NE(error.find("statically linked package"), std::string::npos);
    EXPECT_FALSE(fs::exists(options.output_directory));
    EXPECT_EQ(Read(SourceFile()), original_source);
}
TEST_F(ReleaseMigrationFixture, IdentityOnlyUpgradeRejectsEffectiveParameterDrift) {
    ConfigureIdentityOnlyMigration();
    const auto path = directory / "1.1.1/parameters.yaml";
    auto parameters = config_detail::Parse(Read(path));
    parameters["components"][0]["params"]["take_profit_atr_multiplier"] = 4;
    Write(path, YAML::Dump(parameters));
    auto deployment = config_detail::Parse(Read(options.target_deployment));
    deployment["parameter_sets"]["test_v001"] = Reference(path);
    Write(options.target_deployment, YAML::Dump(deployment));
    std::string error;
    EXPECT_FALSE(MigrateStrategyReleaseState(options, &error));
    EXPECT_NE(error.find("effective parameters"), std::string::npos);
    EXPECT_FALSE(fs::exists(options.output_directory));
    EXPECT_EQ(Read(SourceFile()), source_bytes);
}
TEST_F(ReleaseMigrationFixture, IdentityOnlyUpgradeRejectsRiskDrift) {
    ConfigureIdentityOnlyMigration();
    auto deployment = config_detail::Parse(Read(options.target_deployment));
    deployment["risk_profiles"]["trial"]["max_order_volume"] = 3;
    Write(options.target_deployment, YAML::Dump(deployment));
    std::string error;
    EXPECT_FALSE(MigrateStrategyReleaseState(options, &error));
    EXPECT_NE(error.find("capital or risk limits"), std::string::npos);
    EXPECT_FALSE(fs::exists(options.output_directory));
    EXPECT_EQ(Read(SourceFile()), source_bytes);
}
TEST_F(ReleaseMigrationFixture, IdentityOnlyUpgradeRejectsRuntimeBindingDrift) {
    ConfigureIdentityOnlyMigration();
    auto deployment = config_detail::Parse(Read(options.target_deployment));
    deployment["accounts"]["sim_a"]["runtime_root"] = (directory / "other-runtime").string();
    Write(options.target_deployment, YAML::Dump(deployment));
    std::string error;
    EXPECT_FALSE(MigrateStrategyReleaseState(options, &error));
    EXPECT_NE(error.find("runtime bindings"), std::string::npos);
    EXPECT_FALSE(fs::exists(options.output_directory));
    EXPECT_EQ(Read(SourceFile()), source_bytes);
}
TEST_F(ReleaseMigrationFixture, RejectsTargetWithoutOldDirectionAndLeavesNoOutput) {
    original.erase("atomic.kama.trailing_direction.0.direction");
    SaveOriginal();
    std::string error;
    EXPECT_FALSE(MigrateStrategyReleaseState(options, &error));
    EXPECT_FALSE(fs::exists(options.output_directory));
    EXPECT_EQ(Read(SourceFile()), source_bytes);
}
TEST_F(ReleaseMigrationFixture, RejectsNonzeroPositionWithoutAtomicCheckpoint) {
    for (auto it = original.begin(); it != original.end();) {
        if (it->first.rfind("atomic.", 0) == 0)
            it = original.erase(it);
        else
            ++it;
    }
    SaveOriginal();
    std::string error;
    EXPECT_FALSE(MigrateStrategyReleaseState(options, &error));
    EXPECT_NE(error.find("held position requires saved atomic state"), std::string::npos);
    EXPECT_FALSE(fs::exists(options.output_directory));
}
TEST_F(ReleaseMigrationFixture, RejectsPackagePinAndStrictEnvelopeMismatch) {
    std::string error;
    const auto hash = options.source_package_sha256;
    options.source_package_sha256 = std::string(64, 'a');
    EXPECT_FALSE(MigrateStrategyReleaseState(options, &error));
    EXPECT_NE(error.find("explicitly pinned hash"), std::string::npos);
    options.source_package_sha256 = hash;
    auto state = config_detail::Parse(Read(SourceFile()));
    state["state"]["__host.parameter_hash"] = std::string(64, 'b');
    Write(SourceFile(), config_detail::CanonicalJson(state));
    EXPECT_FALSE(MigrateStrategyReleaseState(options, &error));
    EXPECT_NE(error.find("formal checkpoint invalid"), std::string::npos);
    EXPECT_FALSE(fs::exists(options.output_directory));
}
TEST_F(ReleaseMigrationFixture, RejectsEffectiveParameterChangesEvenWithUpdatedFileHashes) {
    const auto path = directory / "1.1.0/parameters.yaml";
    auto parameters = config_detail::Parse(Read(path));
    parameters["components"][0]["params"]["take_profit_atr_multiplier"] = 4;
    Write(path, YAML::Dump(parameters));
    auto deployment = config_detail::Parse(Read(options.target_deployment));
    deployment["parameter_sets"]["test_v001"] = Reference(path);
    Write(options.target_deployment, YAML::Dump(deployment));
    std::string error;
    EXPECT_FALSE(MigrateStrategyReleaseState(options, &error));
    EXPECT_NE(error.find("effective parameters"), std::string::npos);
    EXPECT_FALSE(fs::exists(options.output_directory));
}
TEST_F(ReleaseMigrationFixture, RejectsOwnerDriftCorruptStateAndOutputInsideSource) {
    original["owner.hc2701"] = "unknown_component";
    SaveOriginal();
    std::string error;
    EXPECT_FALSE(MigrateStrategyReleaseState(options, &error));
    EXPECT_FALSE(fs::exists(options.output_directory));
    Write(SourceFile(), "{\"state\":{},\"state\":{}}");
    EXPECT_FALSE(MigrateStrategyReleaseState(options, &error));
    options.output_directory = (directory / "frozen/new").string();
    EXPECT_FALSE(MigrateStrategyReleaseState(options, &error));
    EXPECT_NE(error.find("new isolated directory"), std::string::npos);
}
TEST_F(ReleaseMigrationFixture, RejectsChangedRuntimeBindingWithoutReadingCredentials) {
    auto deployment = config_detail::Parse(Read(options.target_deployment));
    deployment["accounts"]["sim_a"]["credential_ref"] =
        (directory / "a_different_unread_secret.env").string();
    Write(options.target_deployment, YAML::Dump(deployment));
    std::string error;
    EXPECT_FALSE(MigrateStrategyReleaseState(options, &error));
    EXPECT_NE(error.find("runtime bindings"), std::string::npos);
    EXPECT_FALSE(fs::exists(options.output_directory));
    EXPECT_EQ(Read(SourceFile()), source_bytes);
}
TEST_F(ReleaseMigrationFixture, RejectsEconomicProjectionHiddenByFlatNetPosition) {
    original["net_pos.hc2701"] = "0";
    SaveOriginal();
    std::string error;
    EXPECT_FALSE(MigrateStrategyReleaseState(options, &error));
    EXPECT_NE(error.find("committed position differs"), std::string::npos);
    EXPECT_FALSE(fs::exists(options.output_directory));
}
TEST_F(ReleaseMigrationFixture, ClearsLegacyFlatTargetAndPreservesAllOriginalFacts) {
    original["net_pos.hc2701"] = "0";
    original.erase("owner.hc2701");
    original.erase("avg_open.hc2701");
    for (auto& entry : original) {
        if (entry.first.rfind("committed_position.", 0) != 0) continue;
        Position position;
        ASSERT_TRUE(DecodePositionProjection(entry.second, &position));
        position.long_qty = 0;
        position.avg_long_price = 0;
        entry.second = EncodePositionProjection(position);
    }
    SaveOriginal();
    std::string error;
    ASSERT_TRUE(MigrateStrategyReleaseState(options, &error)) << error;
    FileStrategyStatePersistence persistence(options.output_directory, options.key_prefix, 0);
    StrategyState wrapped, restored;
    ASSERT_TRUE(persistence.LoadStrategyState("account", "stable_instance", &wrapped, &error));
    const auto& instance = target.instances.front();
    ASSERT_TRUE(UnwrapStrategyState(
        wrapped,
        {"stable_instance", "account", instance.strategy_release, instance.parameter_hash, "1"},
        &restored, &error));
    EXPECT_EQ(restored.at("atomic.kama.take_profit.count"), "0");
    for (const auto& entry : original)
        if (entry.first.rfind("atomic.", 0) != 0) EXPECT_EQ(restored.at(entry.first), entry.second);
    EXPECT_EQ(Read(SourceFile()), source_bytes);
}
}  // namespace
}  // namespace quant_hft
