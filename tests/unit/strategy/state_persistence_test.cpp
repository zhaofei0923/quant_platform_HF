#include "quant_hft/strategy/state_persistence.h"

#include <gtest/gtest.h>

#include <chrono>
#include <filesystem>
#include <memory>
#include <thread>

#if defined(__linux__)
#include <sys/stat.h>
#include <sys/syscall.h>
#include <unistd.h>

#include <cerrno>

namespace {
thread_local bool fail_state_file_sync = false;
thread_local bool fail_state_directory_sync = false;
}  // namespace

// Test-binary interposition exercises actual filesystem failures without production hooks.
extern "C" int fsync(int fd) {
    struct stat status {};
    if (::fstat(fd, &status) == 0 && ((fail_state_file_sync && S_ISREG(status.st_mode)) ||
                                      (fail_state_directory_sync && S_ISDIR(status.st_mode)))) {
        errno = EIO;
        return -1;
    }
    return static_cast<int>(::syscall(SYS_fsync, fd));
}
#endif

namespace quant_hft {
namespace {

TEST(StatePersistenceTest, SavesAndLoadsStrategyState) {
    auto redis = std::make_shared<InMemoryRedisHashClient>();
    RedisStrategyStatePersistence persistence(redis, "strategy_state", 60);

    StrategyState state;
    state["k1"] = "v1";
    state["k2"] = "v2";

    std::string error;
    ASSERT_TRUE(persistence.SaveStrategyState("acct", "alpha", state, &error)) << error;

    StrategyState loaded;
    ASSERT_TRUE(persistence.LoadStrategyState("acct", "alpha", &loaded, &error)) << error;
    EXPECT_EQ(loaded.at("k1"), "v1");
    EXPECT_EQ(loaded.at("k2"), "v2");
}

TEST(StatePersistenceTest, ExpiresWhenTtlElapsed) {
    auto redis = std::make_shared<InMemoryRedisHashClient>();
    RedisStrategyStatePersistence persistence(redis, "strategy_state", 1);

    StrategyState state;
    state["k"] = "v";
    std::string error;
    ASSERT_TRUE(persistence.SaveStrategyState("acct", "beta", state, &error)) << error;

    std::this_thread::sleep_for(std::chrono::seconds(2));

    StrategyState loaded;
    EXPECT_FALSE(persistence.LoadStrategyState("acct", "beta", &loaded, &error));
}

TEST(StatePersistenceTest, FileBackendSavesAndLoadsStrategyState) {
    const auto token = std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
    const auto root = std::filesystem::temp_directory_path() / ("quant_hft_state_" + token);
    FileStrategyStatePersistence persistence(root.string(), "strategy_state", 60);

    StrategyState state;
    state["kama.initialized"] = "true";
    state["raw"] = "buy";

    std::string error;
    ASSERT_TRUE(persistence.SaveStrategyState("acct/demo", "kama:alpha", state, &error)) << error;

    StrategyState loaded;
    ASSERT_TRUE(persistence.LoadStrategyState("acct/demo", "kama:alpha", &loaded, &error)) << error;
    EXPECT_EQ(loaded.at("kama.initialized"), "true");
    EXPECT_EQ(loaded.at("raw"), "buy");

    std::filesystem::remove_all(root);
}

TEST(StatePersistenceTest, FailedReplacePreservesExistingDestination) {
    const auto token = std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
    const auto root = std::filesystem::temp_directory_path() / ("quant_hft_state_replace_" + token);
    const auto destination = root / "strategy_state__acct__alpha.json";
    std::filesystem::create_directories(destination);
    FileStrategyStatePersistence persistence(root.string(), "strategy_state", 0);
    std::string error;
    EXPECT_FALSE(persistence.SaveStrategyState("acct", "alpha", {{"version", "2"}}, &error));
    EXPECT_TRUE(std::filesystem::is_directory(destination));
    std::filesystem::remove_all(root);
}

#if defined(__linux__)
TEST(StatePersistenceTest, FileSyncFailureKeepsPreviousSnapshotAndAllowsRetry) {
    const auto token = std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
    const auto root = std::filesystem::temp_directory_path() / ("quant_hft_state_sync_" + token);
    FileStrategyStatePersistence persistence(root.string(), "strategy_state", 0);
    std::string error;
    ASSERT_TRUE(persistence.SaveStrategyState("acct", "alpha", {{"version", "1"}}, &error));
    fail_state_file_sync = true;
    const bool saved = persistence.SaveStrategyState("acct", "alpha", {{"version", "2"}}, &error);
    fail_state_file_sync = false;
    EXPECT_FALSE(saved);
    StrategyState loaded;
    ASSERT_TRUE(persistence.LoadStrategyState("acct", "alpha", &loaded, &error)) << error;
    EXPECT_EQ(loaded.at("version"), "1");
    ASSERT_TRUE(persistence.SaveStrategyState("acct", "alpha", {{"version", "2"}}, &error));
    ASSERT_TRUE(persistence.LoadStrategyState("acct", "alpha", &loaded, &error));
    EXPECT_EQ(loaded.at("version"), "2");
    std::filesystem::remove_all(root);
}

TEST(StatePersistenceTest, DirectorySyncFailureDoesNotReportDurableSuccess) {
    const auto token = std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
    const auto root = std::filesystem::temp_directory_path() / ("quant_hft_state_dirsync_" + token);
    FileStrategyStatePersistence persistence(root.string(), "strategy_state", 0);
    std::string error;
    fail_state_directory_sync = true;
    const bool saved = persistence.SaveStrategyState("acct", "alpha", {{"version", "1"}}, &error);
    fail_state_directory_sync = false;
    EXPECT_FALSE(saved);
    ASSERT_TRUE(persistence.SaveStrategyState("acct", "alpha", {{"version", "1"}}, &error));
    std::filesystem::remove_all(root);
}
#endif

}  // namespace
}  // namespace quant_hft
