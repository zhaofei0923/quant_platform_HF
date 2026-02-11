#include <memory>
#include <string>
#include <unordered_map>

#include <gtest/gtest.h>

#include "quant_hft/core/redis_hash_client.h"
#include "quant_hft/core/strategy_intent_inbox.h"

namespace quant_hft {

TEST(StrategyIntentInboxTest, DecodesLatestBatchAndAppliesSeqGate) {
    auto redis = std::make_shared<InMemoryRedisHashClient>();
    std::string error;
    ASSERT_TRUE(redis->HSet(
        "strategy:intent:demo:latest",
        {
            {"seq", "1"},
            {"count", "1"},
            {"intent_0", "SHFE.ag2406|BUY|OPEN|2|4500.0|123|trace-1"},
            {"ts_ns", "999"},
        },
        &error));

    StrategyIntentInbox inbox(redis);
    StrategyIntentBatch batch;
    ASSERT_TRUE(inbox.ReadLatest("demo", &batch, &error)) << error;
    ASSERT_EQ(batch.seq, 1);
    ASSERT_EQ(batch.ts_ns, 999);
    ASSERT_EQ(batch.intents.size(), 1U);
    EXPECT_EQ(batch.intents[0].strategy_id, "demo");
    EXPECT_EQ(batch.intents[0].instrument_id, "SHFE.ag2406");
    EXPECT_EQ(batch.intents[0].volume, 2);
    EXPECT_EQ(batch.intents[0].trace_id, "trace-1");

    StrategyIntentBatch duplicate;
    ASSERT_TRUE(inbox.ReadLatest("demo", &duplicate, &error)) << error;
    EXPECT_EQ(duplicate.seq, 1);
    EXPECT_TRUE(duplicate.intents.empty());

    ASSERT_TRUE(redis->HSet(
        "strategy:intent:demo:latest",
        {
            {"seq", "2"},
            {"count", "1"},
            {"intent_0", "SHFE.ag2406|SELL|CLOSE|1|4499.0|124|trace-2"},
            {"ts_ns", "1000"},
        },
        &error));

    StrategyIntentBatch next_batch;
    ASSERT_TRUE(inbox.ReadLatest("demo", &next_batch, &error)) << error;
    ASSERT_EQ(next_batch.seq, 2);
    ASSERT_EQ(next_batch.intents.size(), 1U);
    EXPECT_EQ(next_batch.intents[0].trace_id, "trace-2");
}

TEST(StrategyIntentInboxTest, RejectsInvalidEncodedIntent) {
    auto redis = std::make_shared<InMemoryRedisHashClient>();
    std::string error;
    ASSERT_TRUE(redis->HSet(
        "strategy:intent:demo:latest",
        {
            {"seq", "1"},
            {"count", "1"},
            {"intent_0", "bad|format"},
        },
        &error));

    StrategyIntentInbox inbox(redis);
    StrategyIntentBatch batch;
    EXPECT_FALSE(inbox.ReadLatest("demo", &batch, &error));
    EXPECT_NE(error.find("decode"), std::string::npos);
}

}  // namespace quant_hft
