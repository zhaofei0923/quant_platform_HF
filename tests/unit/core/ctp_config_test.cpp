#include <gtest/gtest.h>

#include "quant_hft/core/ctp_config.h"

namespace quant_hft {

TEST(CtpConfigTest, RejectsSimNowWithProductionMode) {
    CtpRuntimeConfig cfg;
    cfg.environment = CtpEnvironment::kSimNow;
    cfg.is_production_mode = true;
    cfg.md_front = "tcp://sim-md";
    cfg.td_front = "tcp://sim-td";
    cfg.broker_id = "9999";
    cfg.user_id = "191202";
    cfg.investor_id = "191202";
    cfg.password = "pwd";

    std::string error;
    EXPECT_FALSE(CtpConfigValidator::Validate(cfg, &error));
    EXPECT_NE(error.find("is_production_mode"), std::string::npos);
}

TEST(CtpConfigTest, AcceptsSimNowTradingHoursWithProductionMode) {
    CtpRuntimeConfig cfg;
    cfg.environment = CtpEnvironment::kSimNow;
    cfg.is_production_mode = true;
    cfg.md_front = "tcp://182.254.243.31:30011";
    cfg.td_front = "tcp://182.254.243.31:30001";
    cfg.broker_id = "9999";
    cfg.user_id = "191202";
    cfg.investor_id = "191202";
    cfg.password = "pwd";

    std::string error;
    EXPECT_TRUE(CtpConfigValidator::Validate(cfg, &error));
}

TEST(CtpConfigTest, RejectsSimNowTradingHoursWithEvaluationMode) {
    CtpRuntimeConfig cfg;
    cfg.environment = CtpEnvironment::kSimNow;
    cfg.is_production_mode = false;
    cfg.md_front = "tcp://182.254.243.31:30011";
    cfg.td_front = "tcp://182.254.243.31:30001";
    cfg.broker_id = "9999";
    cfg.user_id = "191202";
    cfg.investor_id = "191202";
    cfg.password = "pwd";

    std::string error;
    EXPECT_FALSE(CtpConfigValidator::Validate(cfg, &error));
    EXPECT_NE(error.find("trading-hours"), std::string::npos);
}

TEST(CtpConfigTest, RejectsMissingInvestorId) {
    CtpRuntimeConfig cfg;
    cfg.environment = CtpEnvironment::kSimNow;
    cfg.is_production_mode = false;
    cfg.md_front = "tcp://sim-md";
    cfg.td_front = "tcp://sim-td";
    cfg.broker_id = "9999";
    cfg.user_id = "191202";
    cfg.password = "pwd";

    std::string error;
    EXPECT_FALSE(CtpConfigValidator::Validate(cfg, &error));
    EXPECT_NE(error.find("investor_id"), std::string::npos);
}

TEST(CtpConfigTest, AcceptsValidSimNowConfig) {
    CtpRuntimeConfig cfg;
    cfg.environment = CtpEnvironment::kSimNow;
    cfg.is_production_mode = false;
    cfg.md_front = "tcp://sim-md";
    cfg.td_front = "tcp://sim-td";
    cfg.broker_id = "9999";
    cfg.user_id = "191202";
    cfg.investor_id = "191202";
    cfg.password = "pwd";

    std::string error;
    EXPECT_TRUE(CtpConfigValidator::Validate(cfg, &error));
}

TEST(CtpConfigTest, RejectsInvalidReconnectAttemptLimit) {
    CtpRuntimeConfig cfg;
    cfg.environment = CtpEnvironment::kSimNow;
    cfg.is_production_mode = false;
    cfg.md_front = "tcp://sim-md";
    cfg.td_front = "tcp://sim-td";
    cfg.broker_id = "9999";
    cfg.user_id = "191202";
    cfg.investor_id = "191202";
    cfg.password = "pwd";
    cfg.reconnect_max_attempts = 0;

    std::string error;
    EXPECT_FALSE(CtpConfigValidator::Validate(cfg, &error));
    EXPECT_NE(error.find("reconnect_max_attempts"), std::string::npos);
}

TEST(CtpConfigTest, RejectsInvalidReconnectBackoffRange) {
    CtpRuntimeConfig cfg;
    cfg.environment = CtpEnvironment::kSimNow;
    cfg.is_production_mode = false;
    cfg.md_front = "tcp://sim-md";
    cfg.td_front = "tcp://sim-td";
    cfg.broker_id = "9999";
    cfg.user_id = "191202";
    cfg.investor_id = "191202";
    cfg.password = "pwd";
    cfg.reconnect_initial_backoff_ms = 5000;
    cfg.reconnect_max_backoff_ms = 1000;

    std::string error;
    EXPECT_FALSE(CtpConfigValidator::Validate(cfg, &error));
    EXPECT_NE(error.find("reconnect backoff"), std::string::npos);
}

TEST(CtpConfigTest, FrontCandidatesReturnsPrimaryOnlyForUnknownPattern) {
    const auto candidates = BuildCtpFrontCandidates("tcp://127.0.0.1:10131",
                                                    "tcp://127.0.0.1:10130");
    ASSERT_EQ(candidates.size(), 1U);
    EXPECT_EQ(candidates[0].md_front, "tcp://127.0.0.1:10131");
    EXPECT_EQ(candidates[0].td_front, "tcp://127.0.0.1:10130");
}

TEST(CtpConfigTest, FrontCandidatesAddsGroup2AndGroup3ForGroup1) {
    const auto candidates = BuildCtpFrontCandidates("tcp://182.254.243.31:30011",
                                                    "tcp://182.254.243.31:30001");
    ASSERT_EQ(candidates.size(), 3U);
    EXPECT_EQ(candidates[0].md_front, "tcp://182.254.243.31:30011");
    EXPECT_EQ(candidates[0].td_front, "tcp://182.254.243.31:30001");
    EXPECT_EQ(candidates[1].md_front, "tcp://182.254.243.31:30012");
    EXPECT_EQ(candidates[1].td_front, "tcp://182.254.243.31:30002");
    EXPECT_EQ(candidates[2].md_front, "tcp://182.254.243.31:30013");
    EXPECT_EQ(candidates[2].td_front, "tcp://182.254.243.31:30003");
}

TEST(CtpConfigTest, FrontCandidatesRotatesFromGroup3) {
    const auto candidates = BuildCtpFrontCandidates("tcp://182.254.243.31:30013",
                                                    "tcp://182.254.243.31:30003");
    ASSERT_EQ(candidates.size(), 3U);
    EXPECT_EQ(candidates[0].md_front, "tcp://182.254.243.31:30013");
    EXPECT_EQ(candidates[0].td_front, "tcp://182.254.243.31:30003");
    EXPECT_EQ(candidates[1].md_front, "tcp://182.254.243.31:30011");
    EXPECT_EQ(candidates[1].td_front, "tcp://182.254.243.31:30001");
    EXPECT_EQ(candidates[2].md_front, "tcp://182.254.243.31:30012");
    EXPECT_EQ(candidates[2].td_front, "tcp://182.254.243.31:30002");
}

}  // namespace quant_hft
