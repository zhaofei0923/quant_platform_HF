#include <gtest/gtest.h>
#include <sys/wait.h>
#include <unistd.h>

#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <string>

#include "quant_hft/core/ctp_config_loader.h"
#include "quant_hft/core/runtime_semantics_loader.h"
namespace quant_hft {
namespace {
class PackagedRuntimeSemanticsTest : public ::testing::Test {
   protected:
    void SetUp() override {
        original_cwd_ = std::filesystem::current_path();
        auto pattern = (std::filesystem::temp_directory_path() / "quant-package-XXXXXX").string();
        ASSERT_NE(::mkdtemp(pattern.data()), nullptr);
        root_ = pattern;
    }

    void TearDown() override {
        std::filesystem::current_path(original_cwd_);
        std::filesystem::remove_all(root_);
    }

    static std::string Quote(const std::filesystem::path& path) {
        std::string result = "'";
        for (const auto ch : path.string()) result += ch == '\'' ? "'\\''" : std::string(1, ch);
        return result + "'";
    }

    static void Write(const std::filesystem::path& path, const std::string& text) {
        std::filesystem::create_directories(path.parent_path());
        std::ofstream(path) << text;
    }

    static std::string Read(const std::filesystem::path& path) {
        std::ifstream input(path);
        return std::string(std::istreambuf_iterator<char>(input), {});
    }

    int RunLauncher(const std::filesystem::path& launcher,
                    const std::filesystem::path& external_directory,
                    const std::filesystem::path& observed_cwd) {
        const pid_t child = ::fork();
        if (child < 0) return -1;
        if (child == 0) {
            ::clearenv();
            ::setenv("PATH", "/usr/bin:/bin", 1);
            ::setenv("QUANT_HFT_DEPLOYMENT_FILE", "deployment.yaml", 1);
            ::setenv("QUANT_HFT_DEPLOYMENT_ACCOUNT", "synthetic-account", 1);
            ::setenv("QUANT_PACKAGE_TEST_CWD_FILE", observed_cwd.c_str(), 1);
            if (::chdir(external_directory.c_str()) != 0) ::_exit(125);
            ::execl("/bin/bash", "bash", launcher.c_str(), nullptr);
            ::_exit(127);
        }
        int status = 0;
        if (::waitpid(child, &status, 0) != child || !WIFEXITED(status)) return -1;
        return WEXITSTATUS(status);
    }

    std::filesystem::path root_;
    std::filesystem::path original_cwd_;
};
}  // namespace

TEST(ReplayMarketParityTest, SharedRuntimeDefaultsMatchLiveLoaderAndRejectDecisionDrift) {
    RuntimeSemanticsConfig shared;
    CtpFileConfig live;
    std::string error;
    EXPECT_TRUE(ValidateRuntimeSemanticsAgainstCtpConfig(shared, live, &error)) << error;
    live.execution.cancel_after_ms = 2000;
    EXPECT_FALSE(ValidateRuntimeSemanticsAgainstCtpConfig(shared, live, &error));
    EXPECT_NE(error.find("cancel_after_ms"), std::string::npos);
    shared.cancel_after_ms = 2000;
    EXPECT_TRUE(ValidateRuntimeSemanticsAgainstCtpConfig(shared, live, &error)) << error;
    live.market_bar.allowed_lateness_ms = 700;
    EXPECT_FALSE(ValidateRuntimeSemanticsAgainstCtpConfig(shared, live, &error));
    EXPECT_NE(error.find("market_bar_allowed_lateness_ms"), std::string::npos);
    shared.market_bar_allowed_lateness_ms = 700;
    live.dominant_contract_min_hold_ms += 1;
    EXPECT_FALSE(ValidateRuntimeSemanticsAgainstCtpConfig(shared, live, &error));
    EXPECT_NE(error.find("dominant_contract_min_hold_ms"), std::string::npos);
    shared.dominant_contract_min_hold_ms = live.dominant_contract_min_hold_ms;
    live.risk.max_margin_to_equity_ratio = 0.30;
    EXPECT_FALSE(ValidateRuntimeSemanticsAgainstCtpConfig(shared, live, &error));
    EXPECT_NE(error.find("risk_max_margin_to_equity_ratio"), std::string::npos);
    shared.risk_max_margin_to_equity_ratio = 0.30;
    shared.risk_rule_groups = "group_a, group_b";
    live.risk.rules.resize(2);
    live.risk.rules[0].rule_group = "group_a";
    live.risk.rules[1].rule_group = "group_b";
    EXPECT_TRUE(ValidateRuntimeSemanticsAgainstCtpConfig(shared, live, &error)) << error;
    live.risk.rules[1].rule_group = "other";
    EXPECT_FALSE(ValidateRuntimeSemanticsAgainstCtpConfig(shared, live, &error));
    EXPECT_NE(error.find("risk_rule_groups"), std::string::npos);
}

TEST_F(PackagedRuntimeSemanticsTest, ReleaseLaunchersLoadRiskRulesWithExternalConnectionConfig) {
    const std::filesystem::path source = QUANT_HFT_SOURCE_DIR;
    const auto build = root_ / "stub-build";
    const auto output = root_ / "archives";
    const auto extracted = root_ / "extracted";
    const auto external = root_ / "external-account";
    const auto connection = external / "connection.yaml";
    Write(build / "CMakeCache.txt", "QUANT_HFT_ENABLE_CTP_REAL_API:BOOL=OFF\n");
    // Real package construction, with inert executables so no gateway or account can be started.
    for (const auto* name :
         {"core_engine", "quant_config_cli", "strategy_state_migrate_cli", "daily_settlement",
          "wal_replay_tool", "simnow_probe", "runtime_paths_cli",
          "simnow_accounting_policy_check_cli", "simnow_wal_export_cli", "simnow_dashboard_cli",
          "reconnect_evidence_cli", "ops_health_report_cli", "ops_alert_report_cli",
          "dashboard_publish_cli", "simnow_flatten_positions"}) {
        const auto binary = build / name;
        Write(binary,
              "#!/bin/bash\nprintf '%s' \"$PWD\" > \"${QUANT_PACKAGE_TEST_CWD_FILE:?}\"\n"
              "printf '%s' \"$QUANT_HFT_DEPLOYMENT_FILE\" > "
              "\"$QUANT_PACKAGE_TEST_CWD_FILE.manifest\"\n");
        std::filesystem::permissions(binary, std::filesystem::perms::owner_exec,
                                     std::filesystem::perm_options::add);
    }
    const std::string package_command =
        "bash " + Quote(source / "scripts/build/package_nonhotpath_release.sh") + " v0.0.0-test " +
        Quote(output) + " " + Quote(build) + " > " + Quote(root_ / "package.log") + " 2>&1";
    ASSERT_EQ(std::system(package_command.c_str()), 0) << Read(root_ / "package.log");
    std::filesystem::create_directories(extracted);
    const std::string extract_command = "tar -xzf " +
                                        Quote(output / "quant-platform-hf-v0.0.0-test.tar.gz") +
                                        " -C " + Quote(extracted);
    ASSERT_EQ(std::system(extract_command.c_str()), 0);
    const auto package = extracted / "quant-platform-hf-v0.0.0-test";
    const auto rules = package / "configs/risk_rules.yaml";
    ASSERT_TRUE(std::filesystem::is_regular_file(rules));
    EXPECT_EQ(Read(rules), Read(source / "configs/risk_rules.yaml"));
    EXPECT_NE(Read(package / "SHA256SUMS").find("./configs/risk_rules.yaml"), std::string::npos);

    Write(connection, "ctp:\n  environment: simnow\n");
    std::filesystem::current_path(external);
    RuntimeSemanticsConfig semantics;
    std::string error;
    ASSERT_FALSE(LoadRuntimeSemanticsConfig(connection.string(), &semantics, &error));
    EXPECT_NE(error.find("unable to open risk rule snapshot"), std::string::npos);
    ASSERT_FALSE(std::filesystem::exists(external / "configs/risk_rules.yaml"));

    for (const auto* script : {"run_packaged_account.sh", "run_packaged_supervisor.sh"}) {
        const auto observed_cwd = root_ / (std::string(script) + ".cwd");
        ASSERT_EQ(RunLauncher(package / "scripts/ops" / script, external, observed_cwd), 0);
        ASSERT_EQ(Read(observed_cwd), package.string());
        EXPECT_EQ(Read(observed_cwd.string() + ".manifest"),
                  (external / "deployment.yaml").string());
        std::filesystem::current_path(Read(observed_cwd));
        ASSERT_TRUE(LoadRuntimeSemanticsConfig(connection.string(), &semantics, &error)) << error;
        EXPECT_EQ(std::filesystem::canonical(semantics.risk_rule_file_path), rules);
        EXPECT_FALSE(semantics.risk_rule_content_fingerprint.empty());
        EXPECT_FALSE(std::filesystem::exists(external / "configs/risk_rules.yaml"));
    }

    // Omitting this public artifact must remain a clear startup error, never bypass risk rules.
    std::filesystem::remove(rules);
    EXPECT_FALSE(LoadRuntimeSemanticsConfig(connection.string(), &semantics, &error));
    EXPECT_NE(error.find("unable to open risk rule snapshot"), std::string::npos);
}

}  // namespace quant_hft
