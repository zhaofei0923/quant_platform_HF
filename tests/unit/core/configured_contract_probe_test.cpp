#include "quant_hft/core/configured_contract_probe.h"

#include <gtest/gtest.h>

#include <limits>
#include <string>
#include <vector>

namespace quant_hft {
namespace {

ConfiguredContractProbeContext Context(const std::string& instrument_id,
                                       const std::string& product_id) {
    ConfiguredContractProbeContext context;
    context.instrument_id = instrument_id;
    context.product_id = product_id;
    context.account_id = "account";
    context.investor_id = "account";
    context.trading_day = "20260908";
    return context;
}

QueryResultMetadata Metadata(const ConfiguredContractProbeContext& context, int request_id,
                             std::uint64_t generation, const std::string& query_name) {
    QueryResultMetadata metadata;
    metadata.request_id = request_id;
    metadata.generation = generation;
    metadata.query_name = query_name;
    metadata.instrument_id = context.instrument_id;
    metadata.account_id = context.account_id;
    metadata.trading_day = context.trading_day;
    metadata.source = context.source;
    metadata.complete = true;
    metadata.success = true;
    return metadata;
}

QueryResult<InstrumentMetaSnapshot> InstrumentResult(
    const ConfiguredContractProbeContext& context, int request_id, std::uint64_t generation,
    const std::string& exchange_id) {
    QueryResult<InstrumentMetaSnapshot> result;
    result.metadata = Metadata(context, request_id, generation, "Instrument");
    InstrumentMetaSnapshot row;
    row.instrument_id = context.instrument_id;
    row.product_id = context.product_id;
    row.exchange_id = exchange_id;
    row.volume_multiple = 10;
    row.price_tick = 1.0;
    row.is_trading = true;
    row.product_class = "1";
    row.ts_ns = 1;
    row.source = context.source;
    result.rows.push_back(row);
    return result;
}

QueryResult<InstrumentCommissionRateSnapshot> CommissionResult(
    const ConfiguredContractProbeContext& context, int request_id, std::uint64_t generation,
    const std::string& exchange_id, bool product_scope) {
    QueryResult<InstrumentCommissionRateSnapshot> result;
    result.metadata = Metadata(context, request_id, generation, "InstrumentCommissionRate");
    InstrumentCommissionRateSnapshot row;
    row.account_id = context.account_id;
    row.investor_id = context.investor_id;
    row.instrument_id = product_scope ? context.product_id : context.instrument_id;
    row.exchange_id = exchange_id;
    row.open_ratio_by_money = 0.0001;
    row.close_ratio_by_money = 0.0001;
    row.close_today_ratio_by_money = 0.0001;
    row.ts_ns = 2;
    row.source = context.source;
    result.rows.push_back(row);
    return result;
}

QueryResult<InstrumentOrderCommRateSnapshot> OrderCommissionResult(
    const ConfiguredContractProbeContext& context, int request_id, std::uint64_t generation,
    const std::string& exchange_id, bool include_row) {
    QueryResult<InstrumentOrderCommRateSnapshot> result;
    result.metadata = Metadata(context, request_id, generation, "InstrumentOrderCommRate");
    if (include_row) {
        InstrumentOrderCommRateSnapshot row;
        row.account_id = context.account_id;
        row.investor_id = context.investor_id;
        row.instrument_id = context.instrument_id;
        row.exchange_id = exchange_id;
        row.hedge_flag = "1";
        row.order_comm_by_volume = 0.25;
        row.order_action_comm_by_volume = 0.5;
        row.ts_ns = 3;
        row.source = context.source;
        result.rows.push_back(row);
    }
    return result;
}

ConfiguredContractProbeEvidence ValidEvidence(const std::string& instrument_id,
                                              const std::string& product_id,
                                              const std::string& exchange_id, bool order_row,
                                              bool product_commission = false) {
    const auto context = Context(instrument_id, product_id);
    const auto instrument = InstrumentResult(context, 11, 4, exchange_id);
    const auto commission =
        CommissionResult(context, 12, 4, exchange_id, product_commission);
    const auto order_comm = OrderCommissionResult(context, 13, 4, exchange_id, order_row);
    ConfiguredContractProbeEvidence evidence;
    std::string error;
    EXPECT_TRUE(ValidateConfiguredContractProbeEvidence(context, 11, instrument, 12, commission,
                                                        13, order_comm, &evidence, &error))
        << error;
    return evidence;
}

TEST(ConfiguredContractProbeTest, ListEnvironmentIsAuthoritativeAndSupportsMultipleContracts) {
    std::vector<std::string> instruments;
    std::string error;
    ASSERT_TRUE(ResolveConfiguredProbeInstruments(" hc2701, c2701 ,SR701 ", "rb2701",
                                                  {"ag2701"}, "fallback2701", &instruments,
                                                  &error))
        << error;
    EXPECT_EQ(instruments, (std::vector<std::string>{"hc2701", "c2701", "SR701"}));
}

TEST(ConfiguredContractProbeTest, LegacySingleContractRemainsSupportedWhenListIsAbsent) {
    std::vector<std::string> instruments;
    std::string error;
    ASSERT_TRUE(ResolveConfiguredProbeInstruments("", "hc2701", {"c2701", "m2701"},
                                                  "fallback2701", &instruments, &error));
    EXPECT_EQ(instruments, (std::vector<std::string>{"hc2701"}));
}

TEST(ConfiguredContractProbeTest, DuplicateOrUnparseableConfiguredContractsFailClosed) {
    std::vector<std::string> instruments;
    std::string error;
    EXPECT_FALSE(ResolveConfiguredProbeInstruments("hc2701,hc2701", "", {}, "", &instruments,
                                                   &error));
    EXPECT_NE(error.find("duplicate"), std::string::npos);
    EXPECT_FALSE(ResolveConfiguredProbeInstruments("2701", "", {}, "", &instruments, &error));
    EXPECT_NE(error.find("product"), std::string::npos);
}

TEST(ConfiguredContractProbeTest, ControlledUniverseRequiresUniqueCompleteProductScope) {
    const std::vector<std::string> instruments{"hc2701", "hc2705", "c2701", "SR701"};
    std::vector<ConfiguredProbeProductScope> scopes;
    std::string error;
    ASSERT_TRUE(ResolveConfiguredProbeProductScopes(
        "hc:SHFE,c:DCE,SR:CZCE", "4", instruments, &scopes, &error))
        << error;
    ASSERT_EQ(scopes.size(), 3U);
    EXPECT_EQ(ExpectedExchangeForConfiguredProduct(scopes, "hc"), "SHFE");
    EXPECT_EQ(ExpectedExchangeForConfiguredProduct(scopes, "sr"), "CZCE");

    EXPECT_FALSE(ResolveConfiguredProbeProductScopes("", "4", instruments, &scopes, &error));
    EXPECT_NE(error.find("SIMNOW_PRODUCT_SCOPE"), std::string::npos);
    EXPECT_FALSE(ResolveConfiguredProbeProductScopes("hc:SHFE,c:DCE", "4", instruments, &scopes,
                                                    &error));
    EXPECT_NE(error.find("cover"), std::string::npos);
    EXPECT_FALSE(ResolveConfiguredProbeProductScopes(
        "hc:SHFE,hc:DCE,c:DCE,SR:CZCE", "4", instruments, &scopes, &error));
    EXPECT_NE(error.find("duplicate"), std::string::npos);
    EXPECT_FALSE(ResolveConfiguredProbeProductScopes(
        "hc:SHFE,c:DCE,SR:CZCE", "3", instruments, &scopes, &error));
    EXPECT_NE(error.find("count"), std::string::npos);
}

TEST(ConfiguredContractProbeTest, IndependentLegacyModeAllowsMissingProductScope) {
    std::vector<ConfiguredProbeProductScope> scopes;
    std::string error;
    EXPECT_TRUE(ResolveConfiguredProbeProductScopes("", "", {"hc2701"}, &scopes, &error));
    EXPECT_TRUE(scopes.empty());
}

TEST(ConfiguredContractProbeTest, AcceptsExactOrProductCommissionAndExplicitOrEmptyOrderFee) {
    const auto hc = ValidEvidence("hc2701", "hc", "SHFE", true, true);
    EXPECT_EQ(hc.commission_scope, "product");
    EXPECT_EQ(hc.order_comm_status, "verified");
    EXPECT_EQ(hc.order_comm_rows, 1U);

    const auto c = ValidEvidence("c2701", "c", "DCE", false);
    EXPECT_EQ(c.commission_scope, "contract");
    EXPECT_EQ(c.order_comm_status, "successful_empty");
    EXPECT_EQ(c.order_comm_rows, 0U);
}

TEST(ConfiguredContractProbeTest,
     OmittedFeeExchangeBindsOnlyToSameSessionExactInstrumentMetadata) {
    auto context = Context("hc2701", "hc");
    context.expected_exchange_id = "SHFE";
    auto instrument = InstrumentResult(context, 11, 4, "SHFE");
    auto commission = CommissionResult(context, 12, 4, "", false);
    auto order_comm = OrderCommissionResult(context, 13, 4, "", true);
    ConfiguredContractProbeEvidence evidence;
    std::string error;

    ASSERT_TRUE(ValidateConfiguredContractProbeEvidence(context, 11, instrument, 12, commission,
                                                        13, order_comm, &evidence, &error))
        << error;
    EXPECT_EQ(evidence.exchange_id, "SHFE");

    commission.rows.front().exchange_id = "DCE";
    EXPECT_FALSE(ValidateConfiguredContractProbeEvidence(context, 11, instrument, 12, commission,
                                                         13, order_comm, &evidence, &error));
    commission.rows.front().exchange_id.clear();
    order_comm.rows.front().exchange_id = "DCE";
    EXPECT_FALSE(ValidateConfiguredContractProbeEvidence(context, 11, instrument, 12, commission,
                                                         13, order_comm, &evidence, &error));
}

TEST(ConfiguredContractProbeTest, RejectsIncompleteFailedOrCrossSessionQueries) {
    const auto context = Context("hc2701", "hc");
    auto instrument = InstrumentResult(context, 11, 4, "SHFE");
    auto commission = CommissionResult(context, 12, 4, "SHFE", false);
    auto order_comm = OrderCommissionResult(context, 13, 4, "SHFE", true);
    ConfiguredContractProbeEvidence evidence;
    std::string error;

    instrument.metadata.complete = false;
    EXPECT_FALSE(ValidateConfiguredContractProbeEvidence(context, 11, instrument, 12, commission,
                                                         13, order_comm, &evidence, &error));
    instrument.metadata.complete = true;
    commission.metadata.success = false;
    commission.metadata.error_code = 7;
    commission.metadata.error = "broker rejected query";
    EXPECT_FALSE(ValidateConfiguredContractProbeEvidence(context, 11, instrument, 12, commission,
                                                         13, order_comm, &evidence, &error));
    commission = CommissionResult(context, 12, 5, "SHFE", false);
    EXPECT_FALSE(ValidateConfiguredContractProbeEvidence(context, 11, instrument, 12, commission,
                                                         13, order_comm, &evidence, &error));
}

TEST(ConfiguredContractProbeTest, RejectsProductIdentityAccountAndNonFiniteRateMismatches) {
    const auto context = Context("c2701", "c");
    auto instrument = InstrumentResult(context, 11, 4, "DCE");
    auto commission = CommissionResult(context, 12, 4, "DCE", false);
    auto order_comm = OrderCommissionResult(context, 13, 4, "DCE", true);
    ConfiguredContractProbeEvidence evidence;
    std::string error;

    instrument.rows.front().product_id = "m";
    EXPECT_FALSE(ValidateConfiguredContractProbeEvidence(context, 11, instrument, 12, commission,
                                                         13, order_comm, &evidence, &error));
    instrument.rows.front().product_id = "c";
    commission.rows.front().account_id = "another-account";
    EXPECT_FALSE(ValidateConfiguredContractProbeEvidence(context, 11, instrument, 12, commission,
                                                         13, order_comm, &evidence, &error));
    commission = CommissionResult(context, 12, 4, "DCE", false);
    order_comm.rows.front().order_comm_by_volume =
        std::numeric_limits<double>::quiet_NaN();
    EXPECT_FALSE(ValidateConfiguredContractProbeEvidence(context, 11, instrument, 12, commission,
                                                         13, order_comm, &evidence, &error));
}

TEST(ConfiguredContractProbeTest, DeclaredExchangeMustMatchCtpInstrumentExchange) {
    auto context = Context("c2701", "c");
    context.expected_exchange_id = "DCE";
    auto instrument = InstrumentResult(context, 11, 4, "CZCE");
    auto commission = CommissionResult(context, 12, 4, "CZCE", false);
    auto order_comm = OrderCommissionResult(context, 13, 4, "CZCE", true);
    ConfiguredContractProbeEvidence evidence;
    std::string error;
    EXPECT_FALSE(ValidateConfiguredContractProbeEvidence(context, 11, instrument, 12, commission,
                                                         13, order_comm, &evidence, &error));
    EXPECT_NE(error.find("tradable configured future"), std::string::npos);

    context.expected_exchange_id.clear();
    EXPECT_TRUE(ValidateConfiguredContractProbeEvidence(context, 11, instrument, 12, commission,
                                                        13, order_comm, &evidence, &error))
        << error;
}

TEST(ConfiguredContractProbeTest, UniverseIsReadyOnlyAfterEveryConfiguredContractValidates) {
    const std::vector<std::string> configured{"hc2701", "c2701", "SR701"};
    std::vector<ConfiguredContractProbeEvidence> evidence;
    evidence.push_back(ValidEvidence("hc2701", "hc", "SHFE", false, true));
    evidence.push_back(ValidEvidence("c2701", "c", "DCE", true));
    std::string error;
    EXPECT_FALSE(ConfiguredUniverseProbeReady(configured, evidence, &error));

    evidence.push_back(ValidEvidence("SR701", "sr", "CZCE", true));
    EXPECT_TRUE(ConfiguredUniverseProbeReady(configured, evidence, &error)) << error;

    evidence.back().instrument_id = "c2701";
    EXPECT_FALSE(ConfiguredUniverseProbeReady(configured, evidence, &error));
}

TEST(ConfiguredContractProbeTest, MonitorHealthFailureLatchesAcrossLaterRecovery) {
    ConfiguredUniverseProbeHealthLatch health;
    EXPECT_TRUE(health.all_healthy());
    health.Observe(true);
    EXPECT_TRUE(health.all_healthy());
    health.Observe(false);
    EXPECT_FALSE(health.all_healthy());
    health.Observe(true);
    EXPECT_FALSE(health.all_healthy());
}

}  // namespace
}  // namespace quant_hft
