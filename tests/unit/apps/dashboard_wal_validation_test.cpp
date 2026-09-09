#include "quant_hft/apps/dashboard_wal_validation.h"

#include <gtest/gtest.h>

#include <string>

namespace quant_hft::dashboard {
namespace {

std::string Modern(const std::string& fields = "", const std::string& broker = "broker-test",
                   const std::string& kind = "trade") {
    const bool mapping = kind == "ctp_order_submit_mapping";
    std::string record =
        "{\"schema_version\":4,\"stream_id\":\"stream-test\",\"stream_first_sequence\":0,"
        "\"seq\":2,\"account_id\":\"account-test\",\"kind\":\"" +
        kind + "\",\"event_type\":\"" + (mapping ? "ctp_submit_mapping" : "trade_fill") +
        "\",\"trading_day\":\"20260907\",\"instrument_id\":\"hc2610\",\"side\":0,\"offset\":0";
    if (!mapping)
        record += ",\"broker_id\":\"" + broker +
                  "\",\"trade_id\":\"test-trade\",\"exchange_id\":\"SHFE\",\"client_order_id\":"
                  "\"client-test\","
                  "\"status\":3,\"total_volume\":2,\"filled_volume\":2,\"last_trade_volume\":2,"
                  "\"avg_fill_price\":3200";
    record += fields + "}";
    return record.substr(0, record.size() - 1) + ",\"checksum\":\"" +
           WalChecksumHex(WalChecksum(record)) + "\"}";
}

bool Validate(const std::string& line, std::string* error) {
    simple_json::Value parsed;
    if (!simple_json::ParseStrict(line, &parsed, error)) return false;
    return ValidateDashboardWalRecord(line, parsed, "broker-test", "account-test", error);
}

TEST(DashboardWalValidationTest, ValidProductionTradeAndMappingAreAccepted) {
    std::string error;
    EXPECT_TRUE(Validate(Modern(), &error)) << error;
    EXPECT_TRUE(error.empty());
    EXPECT_TRUE(Validate(Modern("", "", "ctp_order_submit_mapping"), &error)) << error;
}

TEST(DashboardWalValidationTest, ChangedPriceWithOldChecksumIsRejected) {
    auto line = Modern();
    line.replace(line.find("3200"), 4, "9200");
    std::string error;
    EXPECT_FALSE(Validate(line, &error));
    EXPECT_EQ(error, "wal_checksum_mismatch");
}

TEST(DashboardWalValidationTest, SameAccountFromDifferentBrokerIsRejected) {
    std::string error;
    EXPECT_FALSE(Validate(Modern("", "other-broker"), &error));
    EXPECT_EQ(error, "wal_broker_mismatch");
}

TEST(DashboardWalValidationTest, FutureAndMalformedVersionsAreRejected) {
    std::string error;
    auto future = Modern();
    future.replace(future.find("version\":4") + 9, 1, "5");
    EXPECT_FALSE(Validate(future, &error));
    EXPECT_EQ(error, "wal_unsupported_schema");
    EXPECT_FALSE(Validate("{\"schema_version\":4.5}", &error));
    EXPECT_EQ(error, "wal_invalid_schema");
}

TEST(DashboardWalValidationTest, LegacyNeedsCorrectAccountAndIsAlwaysMarkedUnverified) {
    std::string error;
    const std::string legacy =
        "{\"schema_version\":2,\"account_id\":\"account-test\",\"kind\":\"order\","
        "\"trading_day\":\"20260907\",\"instrument_id\":\"hc2610\",\"side\":0,\"offset\":0,"
        "\"client_order_id\":\"legacy-client\",\"status\":1,\"avg_fill_price\":0}";
    EXPECT_TRUE(Validate(legacy, &error));
    EXPECT_EQ(error, "legacy_unverified");
    auto foreign = legacy;
    foreign.replace(foreign.find("account-test"), 12, "other-person");
    EXPECT_FALSE(Validate(foreign, &error));
    EXPECT_EQ(error, "wal_account_mismatch");
}

TEST(DashboardWalValidationTest, DuplicatedKeysCannotBypassIdentityOrChecksumChecks) {
    std::string error;
    EXPECT_FALSE(Validate(Modern(",\"account_id\":\"foreign-account\""), &error));
}

}  // namespace
}  // namespace quant_hft::dashboard
