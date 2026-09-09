#include "quant_hft/runtime/runtime_paths.h"

#include <fcntl.h>
#include <gtest/gtest.h>
#include <sys/wait.h>
#include <unistd.h>

#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iterator>

#include "quant_hft/core/ctp_config_loader.h"

namespace quant_hft {
namespace {
class RuntimePathsTest : public ::testing::Test {
   protected:
    void SetUp() override {
        auto pattern = (std::filesystem::temp_directory_path() / "quant-paths-XXXXXX").string();
        ASSERT_NE(::mkdtemp(pattern.data()), nullptr);
        root = pattern;
        options.runtime_root = (root / "runtime").string();
        config.runtime.enable_real_api = true;
        config.runtime.environment = CtpEnvironment::kSimNow;
        config.runtime.broker_id = "b";
        config.account_id = "a";
    }
    void TearDown() override { std::filesystem::remove_all(root); }
    std::filesystem::path root;
    RuntimePathOptions options;
    CtpFileConfig config;
};

TEST_F(RuntimePathsTest, ResolutionHasNoSideEffectsAndKeepsRunArtifactsOutsideRecovery) {
    RuntimePaths first;
    RuntimePaths restart;
    std::string error;
    ASSERT_TRUE(ResolveRuntimePaths(config, options, &first, &error)) << error;
    ASSERT_TRUE(ResolveRuntimePaths(config, options, &restart, &error)) << error;
    EXPECT_EQ(first.wal_file, restart.wal_file);
    EXPECT_EQ(first.recovery_root, (root / "runtime/simnow/b/a/default").string());
    EXPECT_EQ(first.run_root, (root / "runtime/runs/simnow/b/a/default").string());
    EXPECT_EQ(first.market_data_dir, first.recovery_root + "/market");
    EXPECT_EQ(first.state_dir, first.recovery_root + "/state");
    EXPECT_FALSE(std::filesystem::exists(options.runtime_root));
    config.account_id = "other";
    ASSERT_TRUE(ResolveRuntimePaths(config, options, &restart, &error)) << error;
    EXPECT_NE(first.wal_file, restart.wal_file);
    EXPECT_NE(first.run_root, restart.run_root);
    EXPECT_NE(first.market_data_dir, restart.market_data_dir);
    EXPECT_NE(first.state_dir, restart.state_dir);
}

TEST_F(RuntimePathsTest, ExplicitLegacyArtifactsRemainUnclaimedAndKeepTheirPaths) {
    const auto legacy = root / "legacy.wal";
    std::ofstream(legacy) << "old data\n";
    options.wal_file = legacy.string();
    options.market_data_dir = (root / "custom-market").string();
    RuntimePaths paths;
    std::string error;
    ASSERT_TRUE(ResolveRuntimePaths(config, options, &paths, &error)) << error;
    EXPECT_EQ(paths.wal_file, legacy.string());
    EXPECT_EQ(paths.market_data_dir, options.market_data_dir);
    EXPECT_FALSE(std::filesystem::exists(options.runtime_root));
    RuntimeDirectory directory;
    ASSERT_TRUE(directory.Acquire(options.runtime_root, paths.identity, &error)) << error;
    EXPECT_FALSE(directory.BindArtifact(paths.wal_file, false, &error));
    EXPECT_NE(error.find("migration"), std::string::npos);
}

TEST_F(RuntimePathsTest, ConflictingOverridesAndUnsafeIdentityFailBeforeCreatingDirectories) {
    options.wal_file = "one";
    options.legacy_simnow_wal_file = "two";
    RuntimePaths paths;
    std::string error;
    EXPECT_FALSE(ResolveRuntimePaths(config, options, &paths, &error));
    options.legacy_simnow_wal_file.clear();
    options.instance = "../other";
    EXPECT_FALSE(ResolveRuntimePaths(config, options, &paths, &error));
    EXPECT_FALSE(std::filesystem::exists(options.runtime_root));
}

TEST_F(RuntimePathsTest, CliResolvesFormalConnectionWithoutLegacyStrategyDefinition) {
    const auto connection = root / "connection.yaml";
    std::ofstream(connection) << "ctp:\n"
                                 "  environment: simnow\n"
                                 "  enable_real_api: true\n"
                                 "  is_production_mode: false\n"
                                 "  broker_id: b\n"
                                 "  user_id: a\n"
                                 "  investor_id: a\n"
                                 "  account_id: a\n"
                                 "  md_front: tcp://127.0.0.1:1\n"
                                 "  td_front: tcp://127.0.0.1:2\n"
                                 "  strategy_factory: composite\n"
                                 "  password: ${QUANT_PATHS_TEST_SECRET}\n";
    const auto binary =
        std::filesystem::canonical("/proc/self/exe").parent_path() / "runtime_paths_cli";
    ASSERT_TRUE(std::filesystem::is_regular_file(binary));
    const auto output_path = root / "paths.txt";
    const pid_t child = ::fork();
    ASSERT_GE(child, 0);
    if (child == 0) {
        ::clearenv();
        ::setenv("QUANT_HFT_RUNTIME_ROOT", options.runtime_root.c_str(), 1);
        ::setenv("QUANT_PATHS_TEST_SECRET", "synthetic-test-secret", 1);
        const int output = ::open(output_path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0600);
        if (output < 0 || ::dup2(output, STDOUT_FILENO) < 0 || ::dup2(output, STDERR_FILENO) < 0)
            ::_exit(126);
        ::close(output);
        ::execl(binary.c_str(), binary.c_str(), "--config", connection.c_str(), nullptr);
        ::_exit(127);
    }
    int status = 0;
    ASSERT_EQ(::waitpid(child, &status, 0), child);
    std::ifstream input(output_path);
    const std::string output((std::istreambuf_iterator<char>(input)), {});
    ASSERT_TRUE(WIFEXITED(status)) << output;
    ASSERT_EQ(WEXITSTATUS(status), 0) << output;
    EXPECT_NE(output.find("recovery_root=" + options.runtime_root + "/simnow/b/a/default\n"),
              std::string::npos);
    EXPECT_NE(output.find("wal_file="), std::string::npos);
    EXPECT_EQ(output.find("synthetic-test-secret"), std::string::npos);
    EXPECT_EQ(output.find("password"), std::string::npos);
    EXPECT_FALSE(std::filesystem::exists(options.runtime_root));
}
}  // namespace
}  // namespace quant_hft
