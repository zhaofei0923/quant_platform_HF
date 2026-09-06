#include "quant_hft/core/query_batch_collector.h"

#include <gtest/gtest.h>

namespace quant_hft {
namespace {

QueryResultMetadata Metadata(int request, std::uint64_t generation = 1) {
    QueryResultMetadata metadata;
    metadata.request_id = request;
    metadata.generation = generation;
    metadata.query_name = "investor_position";
    metadata.full_account = true;
    return metadata;
}

TEST(QueryBatchCollectorTest, NextRequestCannotClearCompletedRows) {
    QueryBatchCollector<int> collector;
    ASSERT_TRUE(collector.Begin(Metadata(10)));
    const int first = 2;
    const int second = 3;
    EXPECT_FALSE(collector.Accept(10, 1, &first, 0, "", false));
    EXPECT_FALSE(collector.Accept(10, 1, &second, 0, "", false));
    auto result = collector.Accept(10, 1, nullptr, 0, "", true);
    ASSERT_TRUE(result);
    ASSERT_TRUE(collector.Begin(Metadata(11)));
    EXPECT_EQ(result->rows, (std::vector<int>{2, 3}));
    EXPECT_TRUE(result->metadata.success);
    EXPECT_TRUE(result->metadata.complete);
}

TEST(QueryBatchCollectorTest, ErrorOnAnIntermediatePacketCannotBecomeSuccessfulEmptyTruth) {
    QueryBatchCollector<int> collector;
    ASSERT_TRUE(collector.Begin(Metadata(10)));
    collector.Accept(10, 1, nullptr, 42, "query failed", false);
    auto result = collector.Accept(10, 1, nullptr, 0, "", true);
    ASSERT_TRUE(result);
    EXPECT_FALSE(result->metadata.success);
    EXPECT_EQ(result->metadata.error_code, 42);
    EXPECT_EQ(result->metadata.error, "query failed");
}

TEST(QueryBatchCollectorTest, OldGenerationCannotCompleteReusedRequestNumber) {
    QueryBatchCollector<int> collector;
    ASSERT_TRUE(collector.Begin(Metadata(10)));
    collector.Reset();
    ASSERT_TRUE(collector.Begin(Metadata(10, 2)));
    EXPECT_FALSE(collector.Accept(10, 1, nullptr, 0, "", true));
    const int row = 5;
    auto result = collector.Accept(10, 2, &row, 0, "", true);
    ASSERT_TRUE(result);
    EXPECT_EQ(result->rows, (std::vector<int>{5}));
}

TEST(QueryBatchCollectorTest, SuccessfulEmptyFullBatchIsExplicit) {
    QueryBatchCollector<int> collector;
    ASSERT_TRUE(collector.Begin(Metadata(10)));
    auto result = collector.Accept(10, 1, nullptr, 0, "", true);
    ASSERT_TRUE(result);
    EXPECT_TRUE(result->metadata.success);
    EXPECT_TRUE(result->metadata.full_account);
    EXPECT_TRUE(result->rows.empty());
    EXPECT_FALSE(collector.Accept(10, 1, nullptr, 0, "", true));
}

TEST(QueryBatchCollectorTest, CapacityFailureRemainsFailureUntilLastPacket) {
    QueryBatchCollector<int> collector(1, 1);
    ASSERT_TRUE(collector.Begin(Metadata(1)));
    EXPECT_FALSE(collector.Begin(Metadata(2)));
    int row = 7;
    collector.Accept(1, 1, &row, 0, "", false);
    collector.Accept(1, 1, &row, 0, "", false);
    const auto result = collector.Accept(1, 1, nullptr, 0, "", true);
    ASSERT_TRUE(result);
    EXPECT_FALSE(result->metadata.success);
    EXPECT_EQ(result->rows.size(), 1U);
    EXPECT_EQ(result->metadata.error, "query batch row limit exceeded");
    EXPECT_TRUE(collector.Begin(Metadata(2)));
}

}  // namespace
}  // namespace quant_hft
