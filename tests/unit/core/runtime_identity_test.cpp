#include "quant_hft/runtime/runtime_identity.h"

#include <gtest/gtest.h>
#include <sys/wait.h>
#include <unistd.h>

#include <cstdlib>
#include <filesystem>
#include <fstream>

#include "quant_hft/runtime/scoped_redis_client.h"

namespace quant_hft {
namespace {

std::string QuotePath(const std::filesystem::path& path) {
    std::string result = "'";
    for (char ch : path.string()) {
        result += ch == '\'' ? "'\\''" : std::string(1, ch);
    }
    return result + "'";
}

int StageMigration(const std::filesystem::path& source, const std::filesystem::path& parent) {
    const auto repo =
        std::filesystem::path(__FILE__).parent_path().parent_path().parent_path().parent_path();
    const auto command = "bash " + QuotePath(repo / "scripts/ops/stage_runtime_migration.sh") +
                         " --source " + QuotePath(source) + " --staging-parent " +
                         QuotePath(parent) + " --environment simnow --broker b --account a > " +
                         QuotePath(parent.parent_path() / "migration.log") + " 2>&1";
    const int status = std::system(command.c_str());
    return WIFEXITED(status) ? WEXITSTATUS(status) : -1;
}

class RuntimeIdentityTest : public ::testing::Test {
   protected:
    void SetUp() override {
        auto pattern = (std::filesystem::temp_directory_path() / "quant-runtime-XXXXXX").string();
        ASSERT_NE(::mkdtemp(pattern.data()), nullptr);
        root = pattern;
    }
    void TearDown() override { std::filesystem::remove_all(root); }
    std::filesystem::path root;
};

TEST_F(RuntimeIdentityTest, RestartUsesStableStateAndRejectsConcurrentOwner) {
    const RuntimeIdentity identity{"simnow", "test-broker", "test-account", "default"};
    std::string path;
    std::string error;
    {
        RuntimeDirectory first;
        ASSERT_TRUE(first.Acquire(root.string(), identity, &error)) << error;
        path = first.Path("wal/events.wal");
        RuntimeDirectory second;
        EXPECT_FALSE(second.Acquire(root.string(), identity, &error));
    }
    RuntimeDirectory restart;
    ASSERT_TRUE(restart.Acquire(root.string(), identity, &error)) << error;
    EXPECT_EQ(restart.Path("wal/events.wal"), path);
}

TEST_F(RuntimeIdentityTest, DifferentEnvironmentsNeverShareDefaultRecoveryFiles) {
    RuntimeDirectory simnow;
    RuntimeDirectory production;
    std::string error;
    ASSERT_TRUE(simnow.Acquire(root.string(), {"simnow", "b", "a", "default"}, &error));
    ASSERT_TRUE(production.Acquire(root.string(), {"prod", "b", "a", "default"}, &error));
    EXPECT_NE(simnow.Path("wal/events.wal"), production.Path("wal/events.wal"));
    const auto external = (root / "explicit.wal").string();
    ASSERT_TRUE(simnow.BindArtifact(external, false, &error));
    EXPECT_FALSE(production.BindArtifact(external, false, &error));
}

TEST_F(RuntimeIdentityTest, SiblingInstancesCannotBypassAccountAdmission) {
    RuntimeDirectory first;
    RuntimeDirectory sibling;
    RuntimeDirectory other_account;
    std::string error;
    ASSERT_TRUE(first.Acquire(root.string(), {"simnow", "b", "a", "first"}, &error));
    EXPECT_FALSE(sibling.Acquire(root.string(), {"simnow", "b", "a", "second"}, &error));
    EXPECT_NE(error.find("account"), std::string::npos);
    EXPECT_TRUE(other_account.Acquire(root.string(), {"simnow", "b", "other", "first"}, &error));
}

TEST_F(RuntimeIdentityTest, NonemptyLegacyFilesRequireAnExplicitMigration) {
    const auto legacy = root / "old.wal";
    std::ofstream(legacy) << "old state\n";
    RuntimeDirectory runtime;
    std::string error;
    ASSERT_TRUE(runtime.Acquire(root.string(), {"simnow", "b", "a", "default"}, &error));
    EXPECT_FALSE(runtime.BindArtifact(legacy.string(), false, &error));
    EXPECT_NE(error.find("migration"), std::string::npos);
}

TEST_F(RuntimeIdentityTest, MigrationCopiesLegacyForReviewWithoutAssigningIdentity) {
    const auto source = root / "legacy";
    const auto parent = root / "review";
    std::filesystem::create_directory(source);
    std::ofstream(source / "events.wal") << "legacy payload\n";
    EXPECT_EQ(StageMigration(source, parent), 3);
    ASSERT_TRUE(std::filesystem::exists(source / "events.wal"));
    EXPECT_FALSE(std::filesystem::exists(source / "identity.manifest"));
    const auto review = std::filesystem::directory_iterator(parent)->path();
    EXPECT_TRUE(std::filesystem::exists(review / "copy/events.wal"));
    EXPECT_FALSE(std::filesystem::exists(review / "copy/identity.manifest"));
    EXPECT_TRUE(std::filesystem::exists(review / "copy.sha256"));
    EXPECT_EQ(StageMigration(source, source / "review"), 2);
}

TEST_F(RuntimeIdentityTest, MigrationRejectsActiveAccountAndChecksCopiedIdentity) {
    const auto parent = root / "review";
    std::filesystem::path source;
    {
        RuntimeDirectory runtime;
        std::string error;
        ASSERT_TRUE(
            runtime.Acquire((root / "runtime").string(), {"simnow", "b", "a", "default"}, &error));
        source = runtime.root();
        EXPECT_EQ(StageMigration(source, parent), 2);
    }
    EXPECT_EQ(StageMigration(source, parent), 0);
    std::ofstream(source / "identity.manifest") << "wrong identity\n";
    EXPECT_EQ(StageMigration(source, parent), 2);
}

TEST_F(RuntimeIdentityTest, RejectsTraversalAndConflictingLegacyOverrides) {
    RuntimeDirectory runtime;
    std::string error;
    EXPECT_FALSE(runtime.Acquire(root.string(), {"simnow", "b", "../a", "default"}, &error));
    std::string path;
    EXPECT_FALSE(ResolveWalOverride("prod", "", "old", "new", &path, &error));
    EXPECT_FALSE(ResolveWalOverride("simnow", "new", "old", "default", &path, &error));
    EXPECT_TRUE(ResolveWalOverride("simnow", "new", "new", "default", &path, &error));
    EXPECT_EQ(path, "new");
}

TEST_F(RuntimeIdentityTest, CacheAndCheckpointKeysAreScopedTogether) {
    auto raw = std::make_shared<InMemoryRedisHashClient>();
    ScopedRedisClient sim(raw, "simnow:b:a:default");
    ScopedRedisClient prod(raw, "prod:b:a:default");
    std::string error;
    ASSERT_TRUE(sim.HSetVersioned("position:a:rb", {{"qty", "3"}}, 2, &error));
    ASSERT_TRUE(prod.HSetVersioned("position:a:rb", {{"qty", "8"}}, 1, &error));
    std::unordered_map<std::string, std::string> result;
    ASSERT_TRUE(sim.HGetAll("position:a:rb", &result, &error));
    EXPECT_EQ(result["qty"], "3");
    ASSERT_TRUE(prod.HGetAll("position:a:rb", &result, &error));
    EXPECT_EQ(result["qty"], "8");
}

}  // namespace
}  // namespace quant_hft
