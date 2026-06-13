#include "quant_hft/services/ctp_close_offset_resolver.h"

#include <gtest/gtest.h>

#include <string>
#include <vector>

namespace quant_hft {
namespace {

InvestorPositionSnapshot MakePositionSnapshot(std::int32_t position,
                                              const std::string& instrument_id,
                                              const std::string& exchange_id,
                                              PositionDirection direction,
                                              const std::string& position_date) {
    InvestorPositionSnapshot snapshot;
    snapshot.account_id = "acc-1";
    snapshot.investor_id = "acc-1";
    snapshot.instrument_id = instrument_id;
    snapshot.exchange_id = exchange_id;
    snapshot.posi_direction = direction == PositionDirection::kLong ? "2" : "3";
    snapshot.position_date = position_date;
    snapshot.position = position;
    snapshot.today_position = position_date == "today" ? position : 0;
    snapshot.yd_position = position_date == "yesterday" ? position : 0;
    snapshot.ts_ns = 1;
    snapshot.source = "ctp";
    return snapshot;
}

PlannedOrder MakePlannedClose(std::int32_t volume, const std::string& instrument_id = "hc2610",
                              Side side = Side::kBuy) {
    PlannedOrder planned;
    planned.execution_algo_id = "direct";
    planned.slice_index = 1;
    planned.slice_total = 1;
    planned.intent.account_id = "acc-1";
    planned.intent.client_order_id = "trace-1";
    planned.intent.trace_id = "trace-1";
    planned.intent.strategy_id = "kama_candidate_hc";
    planned.intent.instrument_id = instrument_id;
    planned.intent.side = side;
    planned.intent.offset = OffsetFlag::kClose;
    planned.intent.volume = volume;
    planned.intent.price = 3372.0;
    planned.intent.ts_ns = 100;
    return planned;
}

}  // namespace

TEST(CtpCloseOffsetResolverTest, ConvertsShfeTodayShortCloseToCloseToday) {
    CtpPositionLedger ledger;
    std::string error;
    ASSERT_TRUE(ledger.ApplyInvestorPositionSnapshot(
        MakePositionSnapshot(21, "hc2610", "SHFE", PositionDirection::kShort, "today"), &error))
        << error;

    std::vector<PlannedOrder> resolved;
    ASSERT_TRUE(ResolveCtpCloseOffsets({MakePlannedClose(21)}, ledger, &resolved, &error)) << error;

    ASSERT_EQ(resolved.size(), 1U);
    EXPECT_EQ(resolved[0].intent.offset, OffsetFlag::kCloseToday);
    EXPECT_EQ(resolved[0].intent.volume, 21);
    EXPECT_EQ(resolved[0].intent.client_order_id, "trace-1");
    EXPECT_EQ(resolved[0].slice_index, 1);
    EXPECT_EQ(resolved[0].slice_total, 1);
}

TEST(CtpCloseOffsetResolverTest, SplitsShfeCloseAcrossTodayAndYesterday) {
    CtpPositionLedger ledger;
    std::string error;
    ASSERT_TRUE(ledger.ApplyInvestorPositionSnapshot(
        MakePositionSnapshot(3, "rb2405", "SHFE", PositionDirection::kLong, "today"), &error))
        << error;
    ASSERT_TRUE(ledger.ApplyInvestorPositionSnapshot(
        MakePositionSnapshot(4, "rb2405", "SHFE", PositionDirection::kLong, "yesterday"), &error))
        << error;

    std::vector<PlannedOrder> resolved;
    ASSERT_TRUE(ResolveCtpCloseOffsets({MakePlannedClose(6, "rb2405", Side::kSell)}, ledger,
                                       &resolved, &error))
        << error;

    ASSERT_EQ(resolved.size(), 2U);
    EXPECT_EQ(resolved[0].intent.offset, OffsetFlag::kCloseToday);
    EXPECT_EQ(resolved[0].intent.volume, 3);
    EXPECT_EQ(resolved[0].intent.client_order_id, "trace-1#close-today");
    EXPECT_EQ(resolved[0].slice_index, 1);
    EXPECT_EQ(resolved[0].slice_total, 2);
    EXPECT_EQ(resolved[1].intent.offset, OffsetFlag::kCloseYesterday);
    EXPECT_EQ(resolved[1].intent.volume, 3);
    EXPECT_EQ(resolved[1].intent.client_order_id, "trace-1#close-yesterday");
    EXPECT_EQ(resolved[1].slice_index, 2);
    EXPECT_EQ(resolved[1].slice_total, 2);
}

TEST(CtpCloseOffsetResolverTest, SlicedPlansConsumeDatedClosableAcrossSlices) {
    CtpPositionLedger ledger;
    std::string error;
    ASSERT_TRUE(ledger.ApplyInvestorPositionSnapshot(
        MakePositionSnapshot(3, "rb2405", "SHFE", PositionDirection::kLong, "today"), &error))
        << error;
    ASSERT_TRUE(ledger.ApplyInvestorPositionSnapshot(
        MakePositionSnapshot(3, "rb2405", "SHFE", PositionDirection::kLong, "yesterday"), &error))
        << error;

    std::vector<PlannedOrder> raw;
    for (int i = 0; i < 3; ++i) {
        PlannedOrder planned = MakePlannedClose(2, "rb2405", Side::kSell);
        planned.intent.client_order_id = "trace-1#slice-" + std::to_string(i + 1);
        planned.intent.trace_id = planned.intent.client_order_id;
        planned.slice_index = i + 1;
        planned.slice_total = 3;
        raw.push_back(planned);
    }

    std::vector<PlannedOrder> resolved;
    ASSERT_TRUE(ResolveCtpCloseOffsets(raw, ledger, &resolved, &error)) << error;

    ASSERT_EQ(resolved.size(), 4U);
    EXPECT_EQ(resolved[0].intent.offset, OffsetFlag::kCloseToday);
    EXPECT_EQ(resolved[0].intent.volume, 2);
    EXPECT_EQ(resolved[1].intent.offset, OffsetFlag::kCloseToday);
    EXPECT_EQ(resolved[1].intent.volume, 1);
    EXPECT_EQ(resolved[2].intent.offset, OffsetFlag::kCloseYesterday);
    EXPECT_EQ(resolved[2].intent.volume, 1);
    EXPECT_EQ(resolved[3].intent.offset, OffsetFlag::kCloseYesterday);
    EXPECT_EQ(resolved[3].intent.volume, 2);
    EXPECT_EQ(resolved[3].slice_index, 4);
    EXPECT_EQ(resolved[3].slice_total, 4);
}

TEST(CtpCloseOffsetResolverTest, LeavesDceGenericCloseUnchanged) {
    CtpPositionLedger ledger;
    std::string error;

    std::vector<PlannedOrder> resolved;
    ASSERT_TRUE(ResolveCtpCloseOffsets({MakePlannedClose(5, "c2607", Side::kSell)}, ledger,
                                       &resolved, &error))
        << error;

    ASSERT_EQ(resolved.size(), 1U);
    EXPECT_EQ(resolved[0].intent.offset, OffsetFlag::kClose);
    EXPECT_EQ(resolved[0].intent.instrument_id, "c2607");
    EXPECT_EQ(resolved[0].intent.client_order_id, "trace-1");
}

TEST(CtpCloseOffsetResolverTest, RejectsShfeCloseWhenDatedClosableIsInsufficient) {
    CtpPositionLedger ledger;
    std::string error;
    ASSERT_TRUE(ledger.ApplyInvestorPositionSnapshot(
        MakePositionSnapshot(2, "hc2610", "SHFE", PositionDirection::kShort, "today"), &error))
        << error;
    ASSERT_TRUE(ledger.ApplyInvestorPositionSnapshot(
        MakePositionSnapshot(1, "hc2610", "SHFE", PositionDirection::kShort, "yesterday"), &error))
        << error;

    std::vector<PlannedOrder> resolved;
    EXPECT_FALSE(ResolveCtpCloseOffsets({MakePlannedClose(4)}, ledger, &resolved, &error));
    EXPECT_TRUE(resolved.empty());
    EXPECT_NE(error.find("today_closable=2"), std::string::npos);
    EXPECT_NE(error.find("yesterday_closable=1"), std::string::npos);
    EXPECT_NE(error.find("total_closable=3"), std::string::npos);
}

TEST(CtpCloseOffsetResolverTest, InfersExchangeForCommonProducts) {
    EXPECT_EQ(InferCtpExchangeIdFromInstrumentId("hc2610"), "SHFE");
    EXPECT_EQ(InferCtpExchangeIdFromInstrumentId("c2607"), "DCE");
    EXPECT_EQ(InferCtpExchangeIdFromInstrumentId("SHFE.rb2405"), "SHFE");
    EXPECT_TRUE(RequiresExplicitCtpCloseOffset("shfe"));
    EXPECT_FALSE(RequiresExplicitCtpCloseOffset("DCE"));
}

}  // namespace quant_hft
