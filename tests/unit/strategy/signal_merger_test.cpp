#include "quant_hft/strategy/signal_merger.h"

#include <gtest/gtest.h>

namespace quant_hft {
namespace {

SignalIntent MakeIntent(const std::string& instrument_id, SignalType signal_type, int volume,
                        EpochNanos ts_ns, std::string trace_id) {
    SignalIntent signal;
    signal.strategy_id = "s";
    signal.instrument_id = instrument_id;
    signal.signal_type = signal_type;
    signal.volume = volume;
    signal.ts_ns = ts_ns;
    signal.trace_id = std::move(trace_id);
    return signal;
}

TEST(SignalMergerTest, PriorityMergerFollowsLegacyPrecedence) {
    PrioritySignalMerger merger;
    const std::vector<SignalIntent> merged = merger.Merge({
        MakeIntent("A", SignalType::kOpen, 10, 100, "open"),
        MakeIntent("A", SignalType::kClose, 1, 101, "close"),
        MakeIntent("A", SignalType::kStopLoss, 1, 102, "stop"),
        MakeIntent("A", SignalType::kForceClose, 1, 103, "force"),
        MakeIntent("B", SignalType::kOpen, 5, 100, "b-open-1"),
        MakeIntent("B", SignalType::kOpen, 6, 100, "b-open-2"),
        MakeIntent("C", SignalType::kOpen, 5, 100, "c-open-a"),
        MakeIntent("C", SignalType::kOpen, 5, 100, "c-open-b"),
    });

    ASSERT_EQ(merged.size(), 3U);
    EXPECT_EQ(merged[0].instrument_id, "A");
    EXPECT_EQ(merged[0].signal_type, SignalType::kForceClose);
    EXPECT_EQ(merged[1].instrument_id, "B");
    EXPECT_EQ(merged[1].trace_id, "b-open-2");
    EXPECT_EQ(merged[2].instrument_id, "C");
    EXPECT_EQ(merged[2].trace_id, "c-open-a");
}

TEST(SignalMergerTest, FactoryCreatesPriorityMerger) {
    std::string error;
    auto merger = CreateSignalMerger(SignalMergeRule::kPriority, &error);
    ASSERT_NE(merger, nullptr) << error;
}

}  // namespace
}  // namespace quant_hft
