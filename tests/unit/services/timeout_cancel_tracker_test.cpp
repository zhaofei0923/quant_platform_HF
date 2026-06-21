#include "quant_hft/services/timeout_cancel_tracker.h"

#include <gtest/gtest.h>

namespace quant_hft {
namespace {

TimeoutCancelTrackerConfig TestConfig() {
    TimeoutCancelTrackerConfig config;
    config.max_attempts = 2;
    config.retry_base_ns = 100;
    config.retry_max_ns = 500;
    return config;
}

OrderEvent CancelRejected(const std::string& client_order_id, const std::string& reason) {
    OrderEvent event;
    event.client_order_id = client_order_id;
    event.status = OrderStatus::kRejected;
    event.event_source = "OnErrRtnOrderAction";
    event.reason = reason;
    return event;
}

}  // namespace

TEST(TimeoutCancelTrackerTest, FirstTimeoutRequestsCancel) {
    TimeoutCancelTracker tracker;
    const auto decision = tracker.OnOrderTimedOut("ord-1", 1'000, TestConfig());

    EXPECT_EQ(decision.action, TimeoutCancelAction::kRequestCancel);
    EXPECT_EQ(decision.attempts, 1);
    EXPECT_EQ(tracker.Attempts("ord-1"), 1);
}

TEST(TimeoutCancelTrackerTest, AwaitingFeedbackDoesNotRepeatCancel) {
    TimeoutCancelTracker tracker;
    ASSERT_EQ(tracker.OnOrderTimedOut("ord-1", 1'000, TestConfig()).action,
              TimeoutCancelAction::kRequestCancel);

    const auto second = tracker.OnOrderTimedOut("ord-1", 1'200, TestConfig());

    EXPECT_EQ(second.action, TimeoutCancelAction::kNone);
    EXPECT_EQ(second.attempts, 1);
}

TEST(TimeoutCancelTrackerTest, FailedCancelSchedulesFiniteRetryThenSuppresses) {
    TimeoutCancelTracker tracker;
    const auto config = TestConfig();
    ASSERT_EQ(tracker.OnOrderTimedOut("ord-1", 1'000, config).action,
              TimeoutCancelAction::kRequestCancel);

    const auto failed =
        tracker.OnCancelRequestCompleted("ord-1", false, 1'100, config, "cancel_request_failed");
    EXPECT_EQ(failed.action, TimeoutCancelAction::kNone);
    EXPECT_TRUE(failed.retry_scheduled);
    EXPECT_EQ(failed.next_retry_ts_ns, 1'200);

    EXPECT_EQ(tracker.OnOrderTimedOut("ord-1", 1'150, config).action, TimeoutCancelAction::kNone);
    const auto retry = tracker.OnOrderTimedOut("ord-1", 1'200, config);
    EXPECT_EQ(retry.action, TimeoutCancelAction::kRequestCancel);
    EXPECT_EQ(retry.attempts, 2);

    const auto exhausted =
        tracker.OnCancelRequestCompleted("ord-1", false, 1'300, config, "cancel_request_failed");
    EXPECT_EQ(exhausted.action, TimeoutCancelAction::kSuppressAndReconcile);
    EXPECT_EQ(exhausted.reason, "cancel_attempts_exhausted");
    EXPECT_TRUE(tracker.IsSuppressed("ord-1"));
}

TEST(TimeoutCancelTrackerTest, Error26SuppressesAndRequestsSingleReconcile) {
    TimeoutCancelTracker tracker;
    const auto config = TestConfig();
    ASSERT_EQ(tracker.OnOrderTimedOut("ord-1", 1'000, config).action,
              TimeoutCancelAction::kRequestCancel);

    const auto first = tracker.OnOrderEvent(
        CancelRejected("ord-1", "cancel_error (ErrorID=26, ErrorMsg=order terminal unknown)"),
        1'100, config);
    const auto duplicate = tracker.OnOrderEvent(
        CancelRejected("ord-1", "cancel_request_rejected (ErrorID=26)"), 1'200, config);

    EXPECT_EQ(first.action, TimeoutCancelAction::kSuppressAndReconcile);
    EXPECT_EQ(first.reason, "cancel_terminal_unknown");
    EXPECT_EQ(duplicate.action, TimeoutCancelAction::kNone);
    EXPECT_TRUE(tracker.IsSuppressed("ord-1"));
}

TEST(TimeoutCancelTrackerTest, TerminalOrderClearsState) {
    TimeoutCancelTracker tracker;
    ASSERT_EQ(tracker.OnOrderTimedOut("ord-1", 1'000, TestConfig()).action,
              TimeoutCancelAction::kRequestCancel);

    OrderEvent filled;
    filled.client_order_id = "ord-1";
    filled.status = OrderStatus::kFilled;
    filled.event_source = "OnRtnTrade";
    tracker.OnOrderEvent(filled, 1'100, TestConfig());

    EXPECT_EQ(tracker.TrackedCount(), 0U);
    EXPECT_EQ(tracker.Attempts("ord-1"), 0);
}

TEST(TimeoutCancelTrackerTest, CancelRejectedIsNotTerminalOrderEvent) {
    TimeoutCancelTracker tracker;
    ASSERT_EQ(tracker.OnOrderTimedOut("ord-1", 1'000, TestConfig()).action,
              TimeoutCancelAction::kRequestCancel);

    tracker.OnOrderEvent(CancelRejected("ord-1", "cancel_request_rejected"), 1'100, TestConfig());

    EXPECT_EQ(tracker.TrackedCount(), 1U);
}

}  // namespace quant_hft
