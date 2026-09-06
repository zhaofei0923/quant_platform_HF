#include "quant_hft/core/durable_order_event_inbox.h"

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <future>
#include <mutex>
#include <vector>

#include "quant_hft/core/local_wal_regulatory_sink.h"

namespace quant_hft {
namespace {

std::filesystem::path InboxWalPath() {
    return std::filesystem::temp_directory_path() /
           ("quant_hft_inbox_" +
            std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()) + ".wal");
}

OrderEvent InboxEvent(const std::string& id) {
    OrderEvent event;
    event.account_id = "account";
    event.client_order_id = id;
    event.instrument_id = "ag2609";
    event.status = OrderStatus::kAccepted;
    event.total_volume = 1;
    event.ts_ns = 1;
    return event;
}

TEST(DurableOrderEventInboxTest, FullQueuePersistsSuffixAndReplaysBarrierAtItsFifoPosition) {
    const auto path = InboxWalPath();
    LocalWalRegulatorySink sink(path.string());
    DurableOrderEventInbox inbox(1);
    std::promise<void> entered;
    std::promise<void> release;
    auto released = release.get_future().share();
    std::mutex seen_mutex;
    std::vector<std::string> seen;
    ASSERT_TRUE(inbox.Configure(&sink, [&](const auto& event, const auto& receipt) {
        EXPECT_TRUE(receipt.durable);
        if (event.client_order_id == "first") {
            entered.set_value();
            released.wait();
        }
        std::lock_guard<std::mutex> lock(seen_mutex);
        seen.push_back(event.client_order_id);
        return true;
    }));
    ASSERT_TRUE(inbox.Start());
    ASSERT_TRUE(inbox.Accept(InboxEvent("first")));
    entered.get_future().wait();
    EXPECT_TRUE(inbox.Accept(InboxEvent("second")));
    EXPECT_TRUE(inbox.PostBarrier([&] {
        std::lock_guard<std::mutex> lock(seen_mutex);
        seen.push_back("barrier");
    }));
    EXPECT_TRUE(inbox.Accept(InboxEvent("third")));
    EXPECT_FALSE(inbox.Healthy());
    release.set_value();
    ASSERT_TRUE(inbox.Recover(path.string()));
    ASSERT_TRUE(inbox.Drain(1000));
    EXPECT_TRUE(inbox.Healthy());
    EXPECT_EQ(seen, (std::vector<std::string>{"first", "second", "barrier", "third"}));
    inbox.Stop();
    std::filesystem::remove(path);
}

TEST(DurableOrderEventInboxTest, ConsumerFailureBlocksLaterEventsUntilSuccessfulReplay) {
    const auto path = InboxWalPath();
    LocalWalRegulatorySink sink(path.string());
    DurableOrderEventInbox inbox;
    std::atomic<bool> fail{true};
    std::promise<void> failed;
    std::vector<std::string> seen;
    ASSERT_TRUE(inbox.Configure(&sink, [&](const auto& event, const auto&) {
        if (fail.load()) {
            failed.set_value();
            return false;
        }
        seen.push_back(event.client_order_id);
        return true;
    }));
    ASSERT_TRUE(inbox.Start());
    ASSERT_TRUE(inbox.Accept(InboxEvent("first")));
    failed.get_future().wait();
    ASSERT_TRUE(inbox.Accept(InboxEvent("second")));
    EXPECT_FALSE(inbox.Drain(5));
    fail.store(false);
    ASSERT_TRUE(inbox.Recover(path.string()));
    EXPECT_EQ(seen, (std::vector<std::string>{"first", "second"}));
    EXPECT_TRUE(inbox.Healthy());
    inbox.Stop();
    std::filesystem::remove(path);
}

TEST(DurableOrderEventInboxTest, DamagedRecoverySnapshotInvokesNoConsumer) {
    const auto path = InboxWalPath();
    {
        LocalWalRegulatorySink sink(path.string());
        ASSERT_TRUE(sink.CommitOrderEvent(InboxEvent("old")));
    }
    {
        std::ofstream out(path, std::ios::app);
        out << "{\"seq\":1";
    }
    LocalWalRegulatorySink sink(path.string());
    DurableOrderEventInbox inbox;
    int calls = 0;
    ASSERT_TRUE(inbox.Configure(&sink, [&](const auto&, const auto&) {
        ++calls;
        return true;
    }));
    ASSERT_TRUE(inbox.Start());
    EXPECT_FALSE(inbox.Recover(path.string()));
    EXPECT_EQ(calls, 0);
    EXPECT_FALSE(inbox.Healthy());
    inbox.Stop();
    std::filesystem::remove(path);
}

}  // namespace
}  // namespace quant_hft
