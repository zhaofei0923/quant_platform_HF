#include <gtest/gtest.h>

#include "quant_hft/contracts/types.h"
#include "quant_hft/services/order_state_machine.h"

namespace quant_hft {

TEST(OrderStateMachineTest, AppliesValidLifecycleTransitions) {
    OrderStateMachine machine;

    OrderIntent intent;
    intent.client_order_id = "ord-1";
    intent.account_id = "a1";
    intent.instrument_id = "SHFE.ag2406";
    intent.volume = 2;
    intent.price = 4500.0;
    intent.ts_ns = 1;

    ASSERT_TRUE(machine.OnOrderIntent(intent));

    OrderEvent accepted;
    accepted.client_order_id = "ord-1";
    accepted.account_id = "a1";
    accepted.instrument_id = "SHFE.ag2406";
    accepted.status = OrderStatus::kAccepted;
    accepted.total_volume = 2;
    accepted.filled_volume = 0;
    accepted.ts_ns = 2;
    ASSERT_TRUE(machine.OnOrderEvent(accepted));

    OrderEvent part_fill = accepted;
    part_fill.status = OrderStatus::kPartiallyFilled;
    part_fill.filled_volume = 1;
    part_fill.ts_ns = 3;
    ASSERT_TRUE(machine.OnOrderEvent(part_fill));

    OrderEvent all_fill = part_fill;
    all_fill.status = OrderStatus::kFilled;
    all_fill.filled_volume = 2;
    all_fill.ts_ns = 4;
    ASSERT_TRUE(machine.OnOrderEvent(all_fill));

    const auto snapshot = machine.GetOrderSnapshot("ord-1");
    EXPECT_EQ(snapshot.status, OrderStatus::kFilled);
    EXPECT_EQ(snapshot.filled_volume, 2);
    EXPECT_TRUE(snapshot.is_terminal);
}

TEST(OrderStateMachineTest, RejectsInvalidTransitionFromTerminalState) {
    OrderStateMachine machine;

    OrderIntent intent;
    intent.client_order_id = "ord-2";
    intent.account_id = "a1";
    intent.instrument_id = "SHFE.ag2406";
    intent.volume = 1;
    intent.price = 4500.0;
    intent.ts_ns = 1;

    ASSERT_TRUE(machine.OnOrderIntent(intent));

    OrderEvent canceled;
    canceled.client_order_id = "ord-2";
    canceled.account_id = "a1";
    canceled.instrument_id = "SHFE.ag2406";
    canceled.status = OrderStatus::kCanceled;
    canceled.total_volume = 1;
    canceled.filled_volume = 0;
    canceled.ts_ns = 2;
    ASSERT_TRUE(machine.OnOrderEvent(canceled));

    OrderEvent late_fill = canceled;
    late_fill.status = OrderStatus::kFilled;
    late_fill.filled_volume = 1;
    late_fill.ts_ns = 3;
    EXPECT_FALSE(machine.OnOrderEvent(late_fill));
}

TEST(OrderStateMachineTest, TreatsDuplicateEventAsIdempotent) {
    OrderStateMachine machine;

    OrderIntent intent;
    intent.client_order_id = "ord-3";
    intent.account_id = "a1";
    intent.instrument_id = "SHFE.ag2406";
    intent.volume = 3;
    intent.price = 4500.0;
    intent.ts_ns = 1;

    ASSERT_TRUE(machine.OnOrderIntent(intent));

    OrderEvent part_fill;
    part_fill.client_order_id = "ord-3";
    part_fill.account_id = "a1";
    part_fill.instrument_id = "SHFE.ag2406";
    part_fill.status = OrderStatus::kPartiallyFilled;
    part_fill.total_volume = 3;
    part_fill.filled_volume = 2;
    part_fill.ts_ns = 2;

    ASSERT_TRUE(machine.OnOrderEvent(part_fill));
    ASSERT_TRUE(machine.OnOrderEvent(part_fill));

    const auto snapshot = machine.GetOrderSnapshot("ord-3");
    EXPECT_EQ(snapshot.status, OrderStatus::kPartiallyFilled);
    EXPECT_EQ(snapshot.filled_volume, 2);
}

TEST(OrderStateMachineTest, RecoveryCanBootstrapOrderFromWalEvent) {
    OrderStateMachine machine;

    OrderEvent recovered;
    recovered.client_order_id = "ord-recover-1";
    recovered.account_id = "a1";
    recovered.instrument_id = "SHFE.ag2406";
    recovered.status = OrderStatus::kPartiallyFilled;
    recovered.total_volume = 4;
    recovered.filled_volume = 1;
    recovered.ts_ns = 100;

    ASSERT_TRUE(machine.RecoverFromOrderEvent(recovered));
    const auto snapshot = machine.GetOrderSnapshot("ord-recover-1");
    EXPECT_EQ(snapshot.status, OrderStatus::kPartiallyFilled);
    EXPECT_EQ(snapshot.total_volume, 4);
    EXPECT_EQ(snapshot.filled_volume, 1);
}

}  // namespace quant_hft
