#include "quant_hft/config/deployment_config.h"

#include <gtest/gtest.h>
#include <yaml-cpp/yaml.h>

#include <chrono>
#include <filesystem>
#include <fstream>
#include <string>

namespace quant_hft {
namespace {
namespace fs = std::filesystem;

class DeploymentFixture : public ::testing::Test {
   protected:
    fs::path directory;
    YAML::Node root;
    const std::string schema = R"(version: 1
algorithms:
  KamaTrendStrategy:
    parameters:
      er_period: {type: int, min: 1, default: 10}
      risk_per_trade_pct: {type: float, min: 0, max: 1, default: 0.005}
)";
    std::string parameters = R"(schema_version: 1
parameter_set_id: hc_test_v001
strategy_release: kama_trend@1.0.0
product_id: hc
market_state_mode: false
merge_rule: kPriority
components:
  - component_id: kama
    type: KamaTrendStrategy
    timeframe_minutes: 5
    enabled: true
    params: {}
)";

    void Write(const std::string& name, const std::string& content) {
        const auto path = directory / name;
        fs::create_directories(path.parent_path());
        std::ofstream output(path, std::ios::binary);
        output << content;
        ASSERT_TRUE(output.good());
    }
    YAML::Node Ref(const std::string& path, const std::string& content) {
        YAML::Node node;
        node["path"] = path;
        node["sha256"] = ConfigContentSha256(content);
        return node;
    }
    void SetUp() override {
        directory = fs::temp_directory_path() /
                    ("quant-deployment-unit-" +
                     std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
        fs::create_directories(directory);
        Write("package/schema.yaml", schema);
        YAML::Node manifest;
        manifest["schema_version"] = 1;
        manifest["version"] = "1.0.0";
        manifest["strategy_releases"].push_back("kama_trend@1.0.0");
        manifest["files"]["share/quant_strategies/1.0.0/schemas/atomic_parameters.yaml"] =
            ConfigContentSha256(schema);
        const auto manifest_text = YAML::Dump(manifest);
        Write("package/manifest.json", manifest_text);
        Write("params.yaml", parameters);
        Write("connection.yaml", "runtime: {enable_real_api: false}\n");
        Write("secret.env", "CTP_PASSWORD=not-for-resolved-output\n");
        root["schema_version"] = 1;
        root["package"]["version"] = "1.0.0";
        root["package"]["manifest"] = Ref("package/manifest.json", manifest_text);
        root["package"]["parameter_schema"] = Ref("package/schema.yaml", schema);
        root["parameter_sets"]["hc_test_v001"] = Ref("params.yaml", parameters);
        auto profile = YAML::Load(
            "{max_order_volume: 2, max_order_notional: 1000000, "
            "max_margin_to_equity_ratio: 0.4, forbid_open_windows: ''}");
        root["risk_profiles"]["trial"] = profile;
        AddAccount("a", "10001");
        AddAllocation("capital_a", "a");
        AddInstance("instance", "a", "capital_a");
    }
    void TearDown() override {
        if (!directory.empty() && directory.parent_path() == fs::temp_directory_path() &&
            directory.filename().string().rfind("quant-deployment-unit-", 0) == 0)
            fs::remove_all(directory);
    }
    void AddAccount(const std::string& reference, const std::string& account) {
        auto node = root["accounts"][reference];
        node["environment"] = "simnow";
        node["broker_id"] = "9999";
        node["account_id"] = account;
        node["connection_config"] = "connection.yaml";
        node["credential_ref"] = "secret.env";
        node["runtime_root"] = "runtime";
        node["active_host"] = "wsl-test";
        node["risk_profile_ref"] = "trial";
    }
    void AddAllocation(const std::string& reference, const std::string& account) {
        root["capital_allocations"][reference]["account_ref"] = account;
        root["capital_allocations"][reference]["initial_capital"] = 25000;
    }
    void AddInstance(const std::string& id, const std::string& account,
                     const std::string& allocation) {
        YAML::Node node;
        node["instance_id"] = id;
        node["account_ref"] = account;
        node["strategy_release"] = "kama_trend@1.0.0";
        node["parameter_set"] = "hc_test_v001";
        node["capital_allocation_ref"] = allocation;
        node["risk_profile_ref"] = "trial";
        root["instances"].push_back(node);
    }
    void ReplaceParameters(const std::string& content) {
        Write("params.yaml", content);
        root["parameter_sets"]["hc_test_v001"] = Ref("params.yaml", content);
    }
    bool Load(DeploymentConfig* output, std::string* error) {
        Write("deployment.yaml", YAML::Dump(root));
        return LoadDeploymentConfig((directory / "deployment.yaml").string(), output, error);
    }
};

TEST_F(DeploymentFixture, ResolvesDeterministicallyWithoutReadingCredentialContents) {
    DeploymentConfig first, second;
    std::string error;
    ASSERT_TRUE(Load(&first, &error)) << error;
    ASSERT_TRUE(Load(&second, &error)) << error;
    EXPECT_EQ(first.effective_hash, second.effective_hash);
    EXPECT_EQ(first.resolved_json, second.resolved_json);
    EXPECT_EQ(first.instances.size(), 1U);
    EXPECT_EQ(first.instances[0].initial_capital, 25000);
    EXPECT_EQ(first.instances[0].capital_mode, "fixed");
    EXPECT_EQ(FixedStrategyCapitalAllocations(first, "a").at("instance"), 25000);
    EXPECT_EQ(first.resolved_json.find("not-for-resolved-output"), std::string::npos);
    fs::remove(directory / "secret.env");
    ASSERT_TRUE(Load(&second, &error)) << error;
    EXPECT_EQ(first.effective_hash, second.effective_hash);
}
TEST_F(DeploymentFixture, RejectsDuplicateYamlKey) {
    Write("deployment.yaml", YAML::Dump(root) + "\nschema_version: 1\n");
    DeploymentConfig output;
    std::string error;
    EXPECT_FALSE(LoadDeploymentConfig((directory / "deployment.yaml").string(), &output, &error));
    EXPECT_NE(error.find("duplicate YAML key"), std::string::npos);
}
TEST_F(DeploymentFixture, RejectsDeploymentForDifferentStaticallyLinkedPackage) {
    DeploymentConfig output;
    std::string error;
    ASSERT_TRUE(Load(&output, &error)) << error;
    EXPECT_FALSE(VerifyDeploymentPackage(output, &error));
    EXPECT_NE(error.find("statically linked package"), std::string::npos);
}
TEST_F(DeploymentFixture, RejectsUnknownParameterAndOldModeOverride) {
    DeploymentConfig output;
    std::string error;
    auto param = YAML::Load(parameters);
    param["components"][0]["params"]["typo_period"] = 12;
    ReplaceParameters(YAML::Dump(param));
    EXPECT_FALSE(Load(&output, &error));
    EXPECT_NE(error.find("unknown algorithm parameter"), std::string::npos);
    param = YAML::Load(parameters);
    param["components"][0]["overrides"]["live"]["er_period"] = 12;
    ReplaceParameters(YAML::Dump(param));
    EXPECT_FALSE(Load(&output, &error));
    EXPECT_NE(error.find("unknown field"), std::string::npos);
}
TEST_F(DeploymentFixture, RejectsDuplicateInstanceAndCapitalReuse) {
    DeploymentConfig output;
    std::string error;
    AddAllocation("capital_b", "a");
    AddInstance("instance", "a", "capital_b");
    EXPECT_FALSE(Load(&output, &error));
    EXPECT_NE(error.find("duplicate instance_id"), std::string::npos);
    root["instances"][1]["instance_id"] = "other_instance";
    root["instances"][1]["capital_allocation_ref"] = "capital_a";
    EXPECT_FALSE(Load(&output, &error));
    EXPECT_NE(error.find("allocation reused"), std::string::npos);
}
TEST_F(DeploymentFixture, AllowsSameInstanceNameAcrossDistinctAccounts) {
    AddAccount("b", "10002");
    AddAllocation("capital_b", "b");
    AddInstance("instance", "b", "capital_b");
    DeploymentConfig output;
    std::string error;
    ASSERT_TRUE(Load(&output, &error)) << error;
    ASSERT_EQ(output.instances.size(), 2U);
    EXPECT_NE(output.instances[0].state_namespace, output.instances[1].state_namespace);
    root["capital_allocations"]["capital_b"]["account_ref"] = "a";
    EXPECT_FALSE(Load(&output, &error));
}
TEST_F(DeploymentFixture, RejectsDuplicatePhysicalAccountAndParameterRange) {
    DeploymentConfig output;
    std::string error;
    AddAccount("b", "10001");
    EXPECT_FALSE(Load(&output, &error));
    EXPECT_NE(error.find("duplicate physical account"), std::string::npos);
    root["accounts"].remove("b");
    auto param = YAML::Load(parameters);
    param["components"][0]["params"]["er_period"] = 0;
    ReplaceParameters(YAML::Dump(param));
    EXPECT_FALSE(Load(&output, &error));
    EXPECT_NE(error.find("below minimum"), std::string::npos);
}
TEST_F(DeploymentFixture, RejectsHashMismatchWithoutReplacingPreviousOutput) {
    DeploymentConfig output;
    output.effective_hash = "previous";
    std::string error;
    Write("params.yaml", parameters + "# changed after snapshot\n");
    EXPECT_FALSE(Load(&output, &error));
    EXPECT_NE(error.find("SHA256 mismatch"), std::string::npos);
    EXPECT_EQ(output.effective_hash, "previous");
}
TEST_F(DeploymentFixture, RejectsInlineDeploymentSecretAndUnknownFields) {
    DeploymentConfig output;
    std::string error;
    root["accounts"]["a"]["password"] = "forbidden-inline-secret";
    EXPECT_FALSE(Load(&output, &error));
    EXPECT_NE(error.find("unknown field"), std::string::npos);
    root["accounts"]["a"].remove("password");
    root["instances"][0]["params"] = YAML::Node(YAML::NodeType::Map);
    EXPECT_FALSE(Load(&output, &error));
}
TEST_F(DeploymentFixture, RejectsInlineConnectionSecret) {
    Write("connection.yaml", "runtime: {password: forbidden-inline-secret}\n");
    DeploymentConfig output;
    std::string error;
    EXPECT_FALSE(Load(&output, &error));
}
TEST_F(DeploymentFixture, RejectsSchemaOutsidePackageManifest) {
    const auto drifted_schema = schema + "# independent drift\n";
    Write("package/schema.yaml", drifted_schema);
    root["package"]["parameter_schema"] = Ref("package/schema.yaml", drifted_schema);
    DeploymentConfig output;
    std::string error;
    EXPECT_FALSE(Load(&output, &error));
}
TEST_F(DeploymentFixture, ResolvesSingleAccountEquityWithoutInventingAnInitialBudget) {
    auto allocation = root["capital_allocations"]["capital_a"];
    allocation.remove("initial_capital");
    allocation["mode"] = "account_equity";
    DeploymentConfig output;
    std::string error;
    ASSERT_TRUE(Load(&output, &error)) << error;
    ASSERT_EQ(output.instances.size(), 1U);
    EXPECT_EQ(output.instances[0].capital_mode, "account_equity");
    EXPECT_TRUE(FixedStrategyCapitalAllocations(output, "a").empty());
    const auto resolved = YAML::Load(output.resolved_json);
    EXPECT_FALSE(resolved["instances"][0]["initial_capital"]);
    EXPECT_EQ(resolved["instances"][0]["capital_source"].as<std::string>(),
              "confirmed_broker_account_snapshot");
}
TEST_F(DeploymentFixture, AccountEquityRejectsSpecifiedInitialCapitalAndUnknownMode) {
    auto allocation = root["capital_allocations"]["capital_a"];
    allocation["mode"] = "account_equity";
    DeploymentConfig output;
    std::string error;
    EXPECT_FALSE(Load(&output, &error));
    EXPECT_NE(error.find("forbids initial_capital"), std::string::npos);
    allocation["mode"] = "broker_equity_typo";
    EXPECT_FALSE(Load(&output, &error));
}
TEST_F(DeploymentFixture, AccountEquityRejectsMixedOrUnusedAllocationsForSameAccount) {
    auto allocation = root["capital_allocations"]["capital_a"];
    allocation.remove("initial_capital");
    allocation["mode"] = "account_equity";
    AddAllocation("unused_fixed", "a");
    DeploymentConfig output;
    std::string error;
    EXPECT_FALSE(Load(&output, &error));
    EXPECT_NE(error.find("cannot mix account budgets"), std::string::npos);
    root["capital_allocations"]["unused_fixed"].remove("initial_capital");
    root["capital_allocations"]["unused_fixed"]["mode"] = "account_equity";
    EXPECT_FALSE(Load(&output, &error));
}
TEST_F(DeploymentFixture, AccountEquityRejectsTwoInstancesAndAnUnassignedAccount) {
    auto allocation = root["capital_allocations"]["capital_a"];
    allocation.remove("initial_capital");
    allocation["mode"] = "account_equity";
    AddInstance("other", "a", "capital_a");
    DeploymentConfig output;
    std::string error;
    EXPECT_FALSE(Load(&output, &error));
    EXPECT_NE(error.find("allocation reused"), std::string::npos);
    root["instances"].remove(1);
    AddAccount("b", "10002");
    AddAllocation("unused_equity", "b");
    root["capital_allocations"]["unused_equity"].remove("initial_capital");
    root["capital_allocations"]["unused_equity"]["mode"] = "account_equity";
    EXPECT_FALSE(Load(&output, &error));
    EXPECT_NE(error.find("exactly one strategy instance"), std::string::npos);
}
TEST_F(DeploymentFixture, AccountEquityAndFixedModesRemainIsolatedAcrossPhysicalAccounts) {
    root["capital_allocations"]["capital_a"].remove("initial_capital");
    root["capital_allocations"]["capital_a"]["mode"] = "account_equity";
    AddAccount("b", "10002");
    AddAllocation("capital_b", "b");
    root["capital_allocations"]["capital_b"]["mode"] = "fixed";
    AddInstance("instance", "b", "capital_b");
    DeploymentConfig output;
    std::string error;
    ASSERT_TRUE(Load(&output, &error)) << error;
    EXPECT_TRUE(FixedStrategyCapitalAllocations(output, "a").empty());
    EXPECT_EQ(FixedStrategyCapitalAllocations(output, "b").at("instance"), 25000);
}
}  // namespace
}  // namespace quant_hft
