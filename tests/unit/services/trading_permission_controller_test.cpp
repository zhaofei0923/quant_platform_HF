#include "quant_hft/services/trading_permission_controller.h"

#include <gtest/gtest.h>

namespace quant_hft {

TEST(TradingPermissionControllerTest, StartsBlockedAndRejectsAllOrders) {
    TradingPermissionController controller(30);

    const auto snapshot = controller.GetSnapshot(100);
    EXPECT_EQ(snapshot.mode, TradingPermissionMode::kBlocked);
    EXPECT_FALSE(controller.CanSubmit(OffsetFlag::kOpen, 100));
    EXPECT_FALSE(controller.CanSubmit(OffsetFlag::kClose, 100));
    EXPECT_FALSE(controller.CanCancel(100));
}

TEST(TradingPermissionControllerTest, RecoveryEntersCloseOnlyUntilContinuousStabilityElapses) {
    TradingPermissionController controller(30);
    controller.BeginRecovery(7, 100);
    std::string error;
    ASSERT_TRUE(controller.MarkRecoveryComplete(7, 100, &error)) << error;

    EXPECT_EQ(controller.GetSnapshot(129).mode, TradingPermissionMode::kCloseOnly);
    EXPECT_TRUE(controller.CanSubmit(OffsetFlag::kCloseToday, 129));
    EXPECT_TRUE(controller.CanCancel(129));
    EXPECT_FALSE(controller.CanSubmit(OffsetFlag::kOpen, 129));
    EXPECT_EQ(controller.GetSnapshot(130).mode, TradingPermissionMode::kReady);
    EXPECT_TRUE(controller.CanSubmit(OffsetFlag::kOpen, 130));
}

TEST(TradingPermissionControllerTest, ConstraintResetsReadyStabilityWindow) {
    TradingPermissionController controller(30);
    controller.BeginRecovery(1, 100);
    ASSERT_TRUE(controller.MarkRecoveryComplete(1, 100));
    ASSERT_EQ(controller.GetSnapshot(130).mode, TradingPermissionMode::kReady);

    ASSERT_TRUE(controller.SetCloseOnly("market_data_stale", 140));
    EXPECT_EQ(controller.GetSnapshot(200).mode, TradingPermissionMode::kCloseOnly);
    EXPECT_TRUE(controller.CanSubmit(SignalType::kStopLoss, 200));
    EXPECT_FALSE(controller.CanSubmit(SignalType::kOpen, 200));

    ASSERT_TRUE(controller.ClearReason("market_data_stale", 200));
    EXPECT_EQ(controller.GetSnapshot(229).mode, TradingPermissionMode::kCloseOnly);
    EXPECT_EQ(controller.GetSnapshot(230).mode, TradingPermissionMode::kReady);
}

TEST(TradingPermissionControllerTest, BlockedConstraintRejectsCloseAndCanDowngrade) {
    TradingPermissionController controller(0);
    controller.BeginRecovery(2, 100);
    ASSERT_TRUE(controller.MarkRecoveryComplete(2, 100));
    ASSERT_EQ(controller.GetSnapshot(100).mode, TradingPermissionMode::kReady);

    ASSERT_TRUE(controller.SetBlocked("td_unavailable", 101));
    EXPECT_FALSE(controller.CanSubmit(OffsetFlag::kClose, 101));
    ASSERT_TRUE(controller.SetCloseOnly("td_unavailable", 102));
    EXPECT_TRUE(controller.CanSubmit(OffsetFlag::kClose, 102));
    EXPECT_FALSE(controller.CanSubmit(OffsetFlag::kOpen, 102));
}

TEST(TradingPermissionControllerTest, StaleRecoveryGenerationCannotOpenTrading) {
    TradingPermissionController controller(0);
    controller.BeginRecovery(9, 100);
    std::string error;
    EXPECT_FALSE(controller.MarkRecoveryComplete(8, 101, &error));
    EXPECT_FALSE(error.empty());
    EXPECT_EQ(controller.GetSnapshot(200).mode, TradingPermissionMode::kBlocked);
}

TEST(TradingPermissionControllerTest, ReasonsAreDeterministicAndExplainRejection) {
    TradingPermissionController controller(0);
    controller.BeginRecovery(1, 100);
    ASSERT_TRUE(controller.MarkRecoveryComplete(1, 100));
    ASSERT_TRUE(controller.SetCloseOnly("position_diff", 101));
    ASSERT_TRUE(controller.SetBlocked("settlement_missing", 101));

    const auto snapshot = controller.GetSnapshot(101);
    ASSERT_EQ(snapshot.reasons.size(), 2U);
    EXPECT_EQ(snapshot.reasons[0], "position_diff");
    EXPECT_EQ(snapshot.reasons[1], "settlement_missing");
    std::string reason;
    EXPECT_FALSE(controller.CanSubmit(OffsetFlag::kClose, 101, &reason));
    EXPECT_NE(reason.find("settlement_missing"), std::string::npos);
}

}  // namespace quant_hft
