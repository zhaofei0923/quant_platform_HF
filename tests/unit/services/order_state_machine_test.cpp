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

TEST(OrderStateMachineTest, HandlesMultipleSlicedOrdersIndependently) {
    OrderStateMachine machine;

    OrderIntent slice1;
    slice1.client_order_id = "trace-1#slice-1";
    slice1.account_id = "a1";
    slice1.instrument_id = "SHFE.ag2406";
    slice1.volume = 2;
    slice1.price = 4500.0;
    slice1.ts_ns = 1;

    OrderIntent slice2 = slice1;
    slice2.client_order_id = "trace-1#slice-2";
    slice2.ts_ns = 2;

    ASSERT_TRUE(machine.OnOrderIntent(slice1));
    ASSERT_TRUE(machine.OnOrderIntent(slice2));

    OrderEvent fill1;
    fill1.client_order_id = "trace-1#slice-1";
    fill1.account_id = "a1";
    fill1.instrument_id = "SHFE.ag2406";
    fill1.status = OrderStatus::kFilled;
    fill1.total_volume = 2;
    fill1.filled_volume = 2;
    fill1.ts_ns = 3;
    ASSERT_TRUE(machine.OnOrderEvent(fill1));

    OrderEvent cancel2 = fill1;
    cancel2.client_order_id = "trace-1#slice-2";
    cancel2.status = OrderStatus::kCanceled;
    cancel2.filled_volume = 0;
    cancel2.ts_ns = 4;
    ASSERT_TRUE(machine.OnOrderEvent(cancel2));

    const auto snapshot1 = machine.GetOrderSnapshot("trace-1#slice-1");
    const auto snapshot2 = machine.GetOrderSnapshot("trace-1#slice-2");
    EXPECT_EQ(snapshot1.status, OrderStatus::kFilled);
    EXPECT_EQ(snapshot2.status, OrderStatus::kCanceled);
    EXPECT_EQ(machine.ActiveOrderCount(), 0U);
}

TEST(OrderStateMachineTest, ReturnsOnlyNonTerminalOrdersFromActiveView) {
    OrderStateMachine machine;

    OrderIntent active_intent;
    active_intent.client_order_id = "ord-active";
    active_intent.account_id = "a1";
    active_intent.instrument_id = "SHFE.ag2406";
    active_intent.volume = 2;
    active_intent.price = 4500.0;
    active_intent.ts_ns = 100;
    ASSERT_TRUE(machine.OnOrderIntent(active_intent));

    OrderEvent active_event;
    active_event.client_order_id = "ord-active";
    active_event.account_id = "a1";
    active_event.instrument_id = "SHFE.ag2406";
    active_event.status = OrderStatus::kAccepted;
    active_event.total_volume = 2;
    active_event.filled_volume = 0;
    active_event.ts_ns = 120;
    ASSERT_TRUE(machine.OnOrderEvent(active_event));

    OrderIntent terminal_intent = active_intent;
    terminal_intent.client_order_id = "ord-terminal";
    terminal_intent.ts_ns = 130;
    ASSERT_TRUE(machine.OnOrderIntent(terminal_intent));

    OrderEvent terminal_event = active_event;
    terminal_event.client_order_id = "ord-terminal";
    terminal_event.status = OrderStatus::kCanceled;
    terminal_event.ts_ns = 140;
    ASSERT_TRUE(machine.OnOrderEvent(terminal_event));

    const auto active_orders = machine.GetActiveOrders();
    ASSERT_EQ(active_orders.size(), 1U);
    EXPECT_EQ(active_orders[0].client_order_id, "ord-active");
    EXPECT_EQ(active_orders[0].status, OrderStatus::kAccepted);
    EXPECT_EQ(active_orders[0].last_update_ts_ns, 120);
}

}  // namespace quant_hft
