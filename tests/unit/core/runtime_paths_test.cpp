#include "quant_hft/runtime/runtime_paths.h"

#include <gtest/gtest.h>
#include <unistd.h>

#include <filesystem>
#include <fstream>

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
    EXPECT_FALSE(std::filesystem::exists(options.runtime_root));
    config.account_id = "other";
    ASSERT_TRUE(ResolveRuntimePaths(config, options, &restart, &error)) << error;
    EXPECT_NE(first.wal_file, restart.wal_file);
    EXPECT_NE(first.run_root, restart.run_root);
    EXPECT_NE(first.market_data_dir, restart.market_data_dir);
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
}  // namespace
}  // namespace quant_hft
