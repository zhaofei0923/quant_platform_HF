#include <grp.h>
#include <gtest/gtest.h>
#include <sys/stat.h>
#include <unistd.h>

#include <array>
#include <filesystem>
#include <fstream>
#include <memory>
#include <string>

#include "quant_hft/core/local_wal_regulatory_sink.h"

namespace quant_hft {
namespace {

class DashboardWalPermissionsTest : public ::testing::Test {
   protected:
    void SetUp() override {
        char pattern[] = "/tmp/dashboard-wal-permissions-XXXXXX";
        const char* created = ::mkdtemp(pattern);
        ASSERT_NE(created, nullptr);
        directory_ = created;
        path_ = directory_ + "/events.wal";
        sink_ = std::make_unique<LocalWalRegulatorySink>(path_);
        ASSERT_TRUE(sink_->LastError().empty());
        struct stat info {};
        ASSERT_EQ(::stat(path_.c_str(), &info), 0);
        gid_ = info.st_gid;
        std::array<char, 16384> buffer{};
        struct group entry {};
        struct group* found = nullptr;
        ASSERT_EQ(::getgrgid_r(gid_, &entry, buffer.data(), buffer.size(), &found), 0);
        ASSERT_NE(found, nullptr);
        group_ = found->gr_name;
    }
    void TearDown() override {
        sink_.reset();
        // Only the exact private directory returned by mkdtemp is removed.
        if (!directory_.empty()) std::filesystem::remove_all(directory_);
    }
    mode_t Mode(const std::string& path) {
        struct stat info {};
        EXPECT_EQ(::stat(path.c_str(), &info), 0);
        return info.st_mode & 0777U;
    }
    std::string directory_, path_, group_;
    gid_t gid_{};
    std::unique_ptr<LocalWalRegulatorySink> sink_;
};

TEST_F(DashboardWalPermissionsTest, DefaultIsPrivateAndMatchingExplicitGroupGetsReadOnly) {
    EXPECT_EQ(Mode(path_), 0600U);
    std::string error;
    ASSERT_TRUE(sink_->EnableObserverReadAccess(group_, &error)) << error;
    EXPECT_EQ(Mode(path_), 0640U);
    EXPECT_TRUE(sink_->LastError().empty());
}

TEST_F(DashboardWalPermissionsTest, WrongOrUnknownGroupDoesNotChangePermissionsOrTradingError) {
    std::string other_group;
    for (const char* name : {"root", "nogroup", "daemon"}) {
        const auto* entry = ::getgrnam(name);
        if (entry != nullptr && entry->gr_gid != gid_) {
            other_group = name;
            break;
        }
    }
    ASSERT_FALSE(other_group.empty());
    std::string error;
    EXPECT_FALSE(sink_->EnableObserverReadAccess(other_group, &error));
    EXPECT_EQ(error, "observer WAL group mismatch");
    EXPECT_FALSE(sink_->EnableObserverReadAccess("quant-dashboard-no-such-group-test", &error));
    EXPECT_FALSE(sink_->EnableObserverReadAccess("", &error));
    EXPECT_EQ(Mode(path_), 0600U);
    EXPECT_TRUE(sink_->LastError().empty());
    OrderEvent event;
    event.account_id = "test-account";
    event.instrument_id = "hc2610";
    event.avg_fill_price = 3200;
    EXPECT_TRUE(sink_->CommitOrderEvent(event).durable);
    EXPECT_TRUE(sink_->LastError().empty());
}

TEST_F(DashboardWalPermissionsTest, PermissionChangeUsesOpenedInodeNotReplacementPath) {
    const auto retained = directory_ + "/events.wal.retained";
    std::filesystem::rename(path_, retained);
    std::ofstream replacement(path_);
    replacement << "unrelated replacement";
    replacement.close();
    ASSERT_EQ(::chmod(path_.c_str(), 0600), 0);
    std::string error;
    ASSERT_TRUE(sink_->EnableObserverReadAccess(group_, &error)) << error;
    EXPECT_EQ(Mode(retained), 0640U);
    EXPECT_EQ(Mode(path_), 0600U);
}

}  // namespace
}  // namespace quant_hft
