#include "quant_hft/services/verified_trade_accounting_policy.h"

#include <gtest/gtest.h>
#include <unistd.h>

#include <chrono>
#include <filesystem>
#include <fstream>
#include <functional>
#include <string>
#include <utility>
#include <vector>

namespace quant_hft {
namespace {

constexpr EpochNanos kQueryTime = 1'788'656'400'000'000'000LL;
constexpr const char* kDay = "20260906";

std::string ValidPolicyJson() {
    return R"json({"schema_version":1,"environment":"simnow","broker_id":"TEST_BROKER","account_id":"TEST_ACCOUNT","records":[{
"verified":true,"instrument_id":"m2701","exchange_id":"DCE","hedge_flag":0,"trading_day":"20260906",
"commission_instrument_id":"m2701","policy_source":"sample","policy_version":"v1",
"sample_sha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
"contract_multiplier":10,"multiplier_source":"meta-sample","commission_source":"fee-sample",
"profit_basis":"opening_lot_v1","profit_source":"fill-sample","fee_model":"money_plus_volume_v1",
"fee_date_basis":"close_allocation_v1","fee_allocation_source":"sample","fee_allocation_version":"v1",
"generic_close_priority":"today_first","close_priority_source":"sample","close_priority_version":"v1",
"order_fee_policy":"verified_zero_v1","order_fee_source":"orderfee-sample",
"open_fee":{"by_money":0.001,"by_volume":2},"close_fee":{"by_money":0.002,"by_volume":3},
"close_today_fee":{"by_money":0.003,"by_volume":4}}]})json";
}

std::string Replace(std::string text, const std::string& from, const std::string& to) {
    const auto found = text.find(from);
    EXPECT_NE(found, std::string::npos) << from;
    if (found != std::string::npos) text.replace(found, from.size(), to);
    return text;
}

QueryResultMetadata Metadata(const std::string& name) {
    QueryResultMetadata metadata;
    metadata.request_id = 11;
    metadata.generation = 3;
    metadata.query_name = name;
    metadata.instrument_id = "m2701";
    metadata.account_id = "TEST_ACCOUNT";
    metadata.trading_day = kDay;
    metadata.source = "ctp";
    metadata.success = metadata.complete = true;
    return metadata;
}

struct PolicyEvidence {
    QueryResult<InstrumentMetaSnapshot> instrument;
    QueryResult<InstrumentCommissionRateSnapshot> commission;
    QueryResult<InstrumentOrderCommRateSnapshot> order_commission;

    PolicyEvidence() {
        instrument.metadata = Metadata("instrument");
        InstrumentMetaSnapshot meta;
        meta.instrument_id = "m2701";
        meta.exchange_id = "DCE";
        meta.volume_multiple = 10;
        meta.source = "ctp";
        meta.ts_ns = kQueryTime;
        instrument.rows.push_back(meta);
        commission.metadata = Metadata("instrument_commission_rate");
        InstrumentCommissionRateSnapshot fee;
        fee.account_id = fee.investor_id = "TEST_ACCOUNT";
        fee.instrument_id = "m2701";
        fee.exchange_id = "DCE";
        fee.open_ratio_by_money = 0.001;
        fee.open_ratio_by_volume = 2;
        fee.close_ratio_by_money = 0.002;
        fee.close_ratio_by_volume = 3;
        fee.close_today_ratio_by_money = 0.003;
        fee.close_today_ratio_by_volume = 4;
        fee.source = "ctp";
        fee.ts_ns = kQueryTime;
        commission.rows.push_back(fee);
        order_commission.metadata = Metadata("instrument_order_comm_rate");
        InstrumentOrderCommRateSnapshot order_fee;
        order_fee.account_id = order_fee.investor_id = "TEST_ACCOUNT";
        order_fee.instrument_id = "m2701";
        order_fee.exchange_id = "DCE";
        order_fee.hedge_flag = "1";
        order_fee.source = "ctp";
        order_fee.ts_ns = kQueryTime;
        order_commission.rows.push_back(order_fee);
    }

    void Observe(VerifiedTradeAccountingPolicyRegistry* registry, int omit = -1) const {
        if (omit != 0) registry->ObserveInstrumentQuery(instrument);
        if (omit != 1) registry->ObserveCommissionQuery(commission);
        if (omit != 2) registry->ObserveOrderCommissionQuery(order_commission);
    }
};

class VerifiedTradeAccountingPolicyTest : public ::testing::Test {
   protected:
    AccountingPolicyRuntimeIdentity identity{"simnow", "TEST_BROKER", "TEST_ACCOUNT"};
    AccountingPolicyInstrument instrument{"m2701", "DCE", HedgeFlag::kSpeculation};
    std::filesystem::path dir;

    void SetUp() override {
        dir = std::filesystem::temp_directory_path() /
              ("verified_accounting_policy_" + std::to_string(getpid()) + "_" +
               std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
        std::filesystem::create_directories(dir);
    }
    void TearDown() override { std::filesystem::remove_all(dir); }

    Trade MatchingTrade() const {
        Trade trade;
        trade.broker_id = identity.broker_id;
        trade.account_id = identity.account_id;
        trade.symbol = instrument.instrument_id;
        trade.exchange = instrument.exchange_id;
        trade.hedge_flag = instrument.hedge_flag;
        trade.trading_day = kDay;
        trade.price = 100;
        trade.quantity = 2;
        trade.trade_ts_ns = kQueryTime;
        return trade;
    }
};

TEST_F(VerifiedTradeAccountingPolicyTest, MissingFileAndMalformedJsonNeverBecomeReady) {
    VerifiedTradeAccountingPolicyRegistry registry;
    std::string error;
    EXPECT_FALSE(registry.LoadFromFile((dir / "missing.json").string(), identity, &error));
    EXPECT_FALSE(error.empty());
    EXPECT_FALSE(registry.ReadyFor({instrument}, kDay, &error));
    for (const auto& json :
         {std::string{}, std::string("{"), std::string("[]"), std::string("null")}) {
        SCOPED_TRACE(json);
        EXPECT_FALSE(registry.LoadFromJson(json, identity, &error));
        EXPECT_FALSE(registry.ReadyFor({instrument}, kDay, &error));
    }
}

TEST_F(VerifiedTradeAccountingPolicyTest, UnknownAndDuplicateJsonKeysAreRejectedAtEveryLevel) {
    const auto valid = ValidPolicyJson();
    const std::vector<std::string> malformed = {
        Replace(valid, "\"schema_version\":1", "\"unknown\":1,\"schema_version\":1"),
        Replace(valid, "\"verified\":true", "\"unknown\":1,\"verified\":true"),
        Replace(valid, "\"open_fee\":{", "\"open_fee\":{\"unknown\":1,"),
        Replace(valid, "\"schema_version\":1", "\"schema_version\":1,\"schema_version\":1"),
        Replace(valid, "\"verified\":true", "\"verified\":true,\"verified\":false"),
        Replace(valid, "\"by_money\":0.001", "\"by_money\":0.001,\"by_money\":0.001")};
    for (std::size_t i = 0; i < malformed.size(); ++i) {
        SCOPED_TRACE(i);
        VerifiedTradeAccountingPolicyRegistry registry;
        std::string error;
        EXPECT_FALSE(registry.LoadFromJson(malformed[i], identity, &error));
        EXPECT_FALSE(error.empty());
        PolicyEvidence{}.Observe(&registry);
        EXPECT_FALSE(registry.ReadyFor({instrument}, kDay, &error));
    }
}

TEST_F(VerifiedTradeAccountingPolicyTest,
       UnverifiedInvalidDatesAndUnsupportedConventionsFailClosed) {
    const auto valid = ValidPolicyJson();
    const std::vector<std::pair<std::string, std::string>> changes = {
        {"\"verified\":true", "\"verified\":false"},
        {"\"trading_day\":\"20260906\"", "\"trading_day\":\"20261306\""},
        {"\"trading_day\":\"20260906\"", "\"trading_day\":\"20260230\""},
        {"\"trading_day\":\"20260906\"", "\"trading_day\":\"2026-09-06\""},
        {"\"generic_close_priority\":\"today_first\"", "\"generic_close_priority\":\"guess\""},
        {"\"fee_model\":\"money_plus_volume_v1\"", "\"fee_model\":\"unknown\""},
        {"\"order_fee_policy\":\"verified_zero_v1\"", "\"order_fee_policy\":\"ignored\""}};
    for (const auto& [from, to] : changes) {
        SCOPED_TRACE(to);
        VerifiedTradeAccountingPolicyRegistry registry;
        std::string error;
        EXPECT_FALSE(registry.LoadFromJson(Replace(valid, from, to), identity, &error));
        EXPECT_FALSE(registry.ReadyFor({instrument}, kDay, &error));
    }
}

TEST_F(VerifiedTradeAccountingPolicyTest, DuplicateInstrumentDayScopeIsRejected) {
    auto json = ValidPolicyJson();
    const auto first = json.find('[');
    const auto last = json.rfind(']');
    ASSERT_NE(first, std::string::npos);
    ASSERT_NE(last, std::string::npos);
    json.insert(last, "," + json.substr(first + 1, last - first - 1));
    VerifiedTradeAccountingPolicyRegistry registry;
    std::string error;
    EXPECT_FALSE(registry.LoadFromJson(json, identity, &error));
    PolicyEvidence{}.Observe(&registry);
    EXPECT_FALSE(registry.ReadyFor({instrument}, kDay, &error));
}

TEST_F(VerifiedTradeAccountingPolicyTest, RuntimeEnvelopeMustMatchEnvironmentBrokerAndAccount) {
    for (int field = 0; field < 3; ++field) {
        SCOPED_TRACE(field);
        auto other = identity;
        if (field == 0) other.environment = "live";
        if (field == 1) other.broker_id = "OTHER_BROKER";
        if (field == 2) other.account_id = "OTHER_ACCOUNT";
        VerifiedTradeAccountingPolicyRegistry registry;
        std::string error;
        EXPECT_FALSE(registry.LoadFromJson(ValidPolicyJson(), other, &error));
        EXPECT_FALSE(registry.ReadyFor({instrument}, kDay, &error));
    }
}

TEST_F(VerifiedTradeAccountingPolicyTest, ExactSameDayEvidenceReturnsExplicitFeeAndClosePolicy) {
    const auto file = dir / "policy.json";
    std::ofstream(file) << ValidPolicyJson();
    VerifiedTradeAccountingPolicyRegistry registry;
    std::string error;
    ASSERT_TRUE(registry.LoadFromFile(file.string(), identity, &error)) << error;
    PolicyEvidence{}.Observe(&registry);
    ASSERT_TRUE(registry.ReadyFor({instrument}, kDay, &error)) << error;
    const auto policy = registry.Resolve(MatchingTrade(), &error);
    EXPECT_TRUE(policy.valuation_inputs_verified) << error;
    EXPECT_DOUBLE_EQ(policy.contract_multiplier, 10);
    EXPECT_EQ(policy.fee_model, TradeFeeModel::kMoneyPlusVolumeV1);
    EXPECT_EQ(policy.generic_close_priority, GenericClosePriority::kTodayFirst);
    EXPECT_EQ(policy.fee_date_basis, "close_allocation_v1");
    EXPECT_DOUBLE_EQ(policy.open_fee.by_money, 0.001);
    EXPECT_DOUBLE_EQ(policy.open_fee.by_volume, 2);
    EXPECT_DOUBLE_EQ(policy.close_fee.by_money, 0.002);
    EXPECT_DOUBLE_EQ(policy.close_fee.by_volume, 3);
    EXPECT_DOUBLE_EQ(policy.close_today_fee.by_money, 0.003);
    EXPECT_DOUBLE_EQ(policy.close_today_fee.by_volume, 4);
    EXPECT_FALSE(policy.close_rule_source.empty());
    EXPECT_FALSE(policy.close_rule_version.empty());
    EXPECT_FALSE(policy.fee_allocation_source.empty());
}

TEST_F(VerifiedTradeAccountingPolicyTest, ResolveRejectsEveryMismatchedTradeScopeAndExpiredDay) {
    VerifiedTradeAccountingPolicyRegistry registry;
    std::string error;
    ASSERT_TRUE(registry.LoadFromJson(ValidPolicyJson(), identity, &error));
    PolicyEvidence{}.Observe(&registry);
    ASSERT_TRUE(registry.ReadyFor({instrument}, kDay, &error)) << error;
    for (int field = 0; field < 6; ++field) {
        SCOPED_TRACE(field);
        auto trade = MatchingTrade();
        if (field == 0) trade.broker_id = "OTHER_BROKER";
        if (field == 1) trade.account_id = "OTHER_ACCOUNT";
        if (field == 2) trade.symbol = "m2705";
        if (field == 3) trade.exchange = "SHFE";
        if (field == 4) trade.hedge_flag = HedgeFlag::kHedge;
        if (field == 5) trade.trading_day = "20260907";
        EXPECT_FALSE(registry.Resolve(trade, &error).valuation_inputs_verified);
        EXPECT_FALSE(error.empty());
    }
    EXPECT_FALSE(registry.ReadyFor({instrument}, "20260907", &error));
    auto wrong = instrument;
    wrong.exchange_id = "SHFE";
    EXPECT_FALSE(registry.ReadyFor({wrong}, kDay, &error));
}

TEST_F(VerifiedTradeAccountingPolicyTest, EachOfTheThreeCompletedQueriesIsRequired) {
    for (int omit = 0; omit < 3; ++omit) {
        SCOPED_TRACE(omit);
        VerifiedTradeAccountingPolicyRegistry registry;
        std::string error;
        ASSERT_TRUE(registry.LoadFromJson(ValidPolicyJson(), identity, &error));
        PolicyEvidence{}.Observe(&registry, omit);
        EXPECT_FALSE(registry.ReadyFor({instrument}, kDay, &error));
        EXPECT_FALSE(registry.Resolve(MatchingTrade(), &error).valuation_inputs_verified);
    }
}

TEST_F(VerifiedTradeAccountingPolicyTest, FailedIncompleteForeignOrUncorrelatedQueryIsNotEvidence) {
    const std::vector<std::function<void(QueryResultMetadata&)>> faults = {
        [](auto& m) { m.success = false; },
        [](auto& m) { m.complete = false; },
        [](auto& m) { m.error_code = 7; },
        [](auto& m) { m.generation = 0; },
        [](auto& m) { m.account_id = "OTHER_ACCOUNT"; },
        [](auto& m) { m.account_id.clear(); },
        [](auto& m) { m.source = "cache"; },
        [](auto& m) { m.source = "simulated"; },
        [](auto& m) { m.trading_day = "20260905"; },
        [](auto& m) { m.instrument_id = "m2705"; }};
    for (int query = 0; query < 3; ++query) {
        for (std::size_t fault = 0; fault < faults.size(); ++fault) {
            SCOPED_TRACE(::testing::Message() << "query=" << query << " fault=" << fault);
            PolicyEvidence evidence;
            if (query == 0) faults[fault](evidence.instrument.metadata);
            if (query == 1) faults[fault](evidence.commission.metadata);
            if (query == 2) faults[fault](evidence.order_commission.metadata);
            VerifiedTradeAccountingPolicyRegistry registry;
            std::string error;
            ASSERT_TRUE(registry.LoadFromJson(ValidPolicyJson(), identity, &error));
            evidence.Observe(&registry);
            EXPECT_FALSE(registry.ReadyFor({instrument}, kDay, &error));
            EXPECT_FALSE(registry.Resolve(MatchingTrade(), &error).valuation_inputs_verified);
        }
    }
}

TEST_F(VerifiedTradeAccountingPolicyTest, WrongRowIdentityOrSourceCannotAuthorizeValuation) {
    const std::vector<std::function<void(PolicyEvidence&)>> faults = {
        [](auto& e) { e.instrument.rows[0].instrument_id = "m2705"; },
        [](auto& e) { e.instrument.rows[0].exchange_id = "SHFE"; },
        [](auto& e) { e.instrument.rows[0].source = "cache"; },
        [](auto& e) { e.commission.rows[0].account_id = "OTHER_ACCOUNT"; },
        [](auto& e) { e.commission.rows[0].investor_id = "OTHER_ACCOUNT"; },
        [](auto& e) { e.commission.rows[0].instrument_id = "m2705"; },
        [](auto& e) { e.commission.rows[0].exchange_id = "SHFE"; },
        [](auto& e) { e.commission.rows[0].source = "cache"; },
        [](auto& e) { e.order_commission.rows[0].account_id = "OTHER_ACCOUNT"; },
        [](auto& e) { e.order_commission.rows[0].investor_id = "OTHER_ACCOUNT"; },
        [](auto& e) { e.order_commission.rows[0].instrument_id = "m2705"; },
        [](auto& e) { e.order_commission.rows[0].exchange_id = "SHFE"; },
        [](auto& e) { e.order_commission.rows[0].hedge_flag = "3"; },
        [](auto& e) { e.order_commission.rows[0].source = "cache"; }};
    for (std::size_t fault = 0; fault < faults.size(); ++fault) {
        SCOPED_TRACE(fault);
        PolicyEvidence evidence;
        faults[fault](evidence);
        VerifiedTradeAccountingPolicyRegistry registry;
        std::string error;
        ASSERT_TRUE(registry.LoadFromJson(ValidPolicyJson(), identity, &error));
        evidence.Observe(&registry);
        EXPECT_FALSE(registry.ReadyFor({instrument}, kDay, &error));
    }
}

TEST_F(VerifiedTradeAccountingPolicyTest,
       EveryQueriedFeeLegAndMultiplierMustMatchTheVerifiedRecord) {
    const std::vector<std::function<void(PolicyEvidence&)>> faults = {
        [](auto& e) { e.instrument.rows[0].volume_multiple = 11; },
        [](auto& e) { e.commission.rows[0].open_ratio_by_money += 0.0001; },
        [](auto& e) { e.commission.rows[0].open_ratio_by_volume += 1; },
        [](auto& e) { e.commission.rows[0].close_ratio_by_money += 0.0001; },
        [](auto& e) { e.commission.rows[0].close_ratio_by_volume += 1; },
        [](auto& e) { e.commission.rows[0].close_today_ratio_by_money += 0.0001; },
        [](auto& e) { e.commission.rows[0].close_today_ratio_by_volume += 1; }};
    for (std::size_t fault = 0; fault < faults.size(); ++fault) {
        SCOPED_TRACE(fault);
        PolicyEvidence evidence;
        faults[fault](evidence);
        VerifiedTradeAccountingPolicyRegistry registry;
        std::string error;
        ASSERT_TRUE(registry.LoadFromJson(ValidPolicyJson(), identity, &error));
        evidence.Observe(&registry);
        EXPECT_FALSE(registry.ReadyFor({instrument}, kDay, &error));
    }
}

TEST_F(VerifiedTradeAccountingPolicyTest, NonzeroOrderOrCancellationFeeIsUnsupported) {
    for (int field = 0; field < 2; ++field) {
        SCOPED_TRACE(field);
        PolicyEvidence evidence;
        if (field == 0) evidence.order_commission.rows[0].order_comm_by_volume = 0.01;
        if (field == 1) evidence.order_commission.rows[0].order_action_comm_by_volume = 0.01;
        VerifiedTradeAccountingPolicyRegistry registry;
        std::string error;
        ASSERT_TRUE(registry.LoadFromJson(ValidPolicyJson(), identity, &error));
        evidence.Observe(&registry);
        EXPECT_FALSE(registry.ReadyFor({instrument}, kDay, &error));
        EXPECT_FALSE(registry.Resolve(MatchingTrade(), &error).valuation_inputs_verified);
    }
}

TEST_F(VerifiedTradeAccountingPolicyTest, MissingAndConflictingDuplicateQueryRowsAreRejected) {
    for (int query = 0; query < 3; ++query) {
        for (bool duplicate : {false, true}) {
            SCOPED_TRACE(::testing::Message() << query << " duplicate=" << duplicate);
            PolicyEvidence evidence;
            if (query == 0) {
                if (duplicate) {
                    evidence.instrument.rows.push_back(evidence.instrument.rows.front());
                    ++evidence.instrument.rows.back().volume_multiple;
                } else
                    evidence.instrument.rows.clear();
            }
            if (query == 1) {
                if (duplicate) {
                    evidence.commission.rows.push_back(evidence.commission.rows.front());
                    ++evidence.commission.rows.back().open_ratio_by_volume;
                } else
                    evidence.commission.rows.clear();
            }
            if (query == 2) {
                if (duplicate) {
                    evidence.order_commission.rows.push_back(
                        evidence.order_commission.rows.front());
                    ++evidence.order_commission.rows.back().order_comm_by_volume;
                } else
                    evidence.order_commission.rows.clear();
            }
            VerifiedTradeAccountingPolicyRegistry registry;
            std::string error;
            ASSERT_TRUE(registry.LoadFromJson(ValidPolicyJson(), identity, &error));
            evidence.Observe(&registry);
            EXPECT_FALSE(registry.ReadyFor({instrument}, kDay, &error));
        }
    }
}

TEST_F(VerifiedTradeAccountingPolicyTest, ExplicitProductCommissionMappingRequiresAFilteredQuery) {
    const auto json = Replace(ValidPolicyJson(), "\"commission_instrument_id\":\"m2701\"",
                              "\"commission_instrument_id\":\"m\"");
    PolicyEvidence evidence;
    evidence.commission.rows[0].instrument_id = "m";
    VerifiedTradeAccountingPolicyRegistry registry;
    std::string error;
    ASSERT_TRUE(registry.LoadFromJson(json, identity, &error)) << error;
    evidence.Observe(&registry);
    EXPECT_TRUE(registry.ReadyFor({instrument}, kDay, &error)) << error;
    evidence.commission.metadata.instrument_id.clear();
    registry.ObserveCommissionQuery(evidence.commission);
    EXPECT_FALSE(registry.ReadyFor({instrument}, kDay, &error));
}

TEST_F(VerifiedTradeAccountingPolicyTest, FailedLatestQueryRevokesPreviousReadiness) {
    PolicyEvidence evidence;
    VerifiedTradeAccountingPolicyRegistry registry;
    std::string error;
    ASSERT_TRUE(registry.LoadFromJson(ValidPolicyJson(), identity, &error));
    evidence.Observe(&registry);
    ASSERT_TRUE(registry.ReadyFor({instrument}, kDay, &error));
    ++evidence.commission.metadata.generation;
    evidence.commission.metadata.success = false;
    evidence.commission.metadata.error_code = 1;
    registry.ObserveCommissionQuery(evidence.commission);
    EXPECT_FALSE(registry.ReadyFor({instrument}, kDay, &error));
}

TEST_F(VerifiedTradeAccountingPolicyTest, YesterdayFirstMustBeAnExplicitVerifiedChoice) {
    auto json = Replace(ValidPolicyJson(), "\"generic_close_priority\":\"today_first\"",
                        "\"generic_close_priority\":\"yesterday_first\"");
    VerifiedTradeAccountingPolicyRegistry registry;
    std::string error;
    ASSERT_TRUE(registry.LoadFromJson(json, identity, &error));
    PolicyEvidence{}.Observe(&registry);
    ASSERT_TRUE(registry.ReadyFor({instrument}, kDay, &error));
    EXPECT_EQ(registry.Resolve(MatchingTrade(), &error).generic_close_priority,
              GenericClosePriority::kYesterdayFirst);
}

TEST_F(VerifiedTradeAccountingPolicyTest, ShfeAndIneRequireExchangeExplicitCloseConvention) {
    for (const auto* exchange : {"SHFE", "INE"}) {
        SCOPED_TRACE(exchange);
        auto json = Replace(ValidPolicyJson(), "\"exchange_id\":\"DCE\"",
                            std::string("\"exchange_id\":\"") + exchange + "\"");
        VerifiedTradeAccountingPolicyRegistry registry;
        std::string error;
        EXPECT_FALSE(registry.LoadFromJson(json, identity, &error));
        json = Replace(json, "\"generic_close_priority\":\"today_first\"",
                       "\"generic_close_priority\":\"exchange_explicit\"");
        EXPECT_TRUE(registry.LoadFromJson(json, identity, &error)) << error;
    }
}

TEST_F(VerifiedTradeAccountingPolicyTest, SimulatedEvidenceOnlyMatchesTheSimulationEnvironment) {
    identity.environment = "sim";
    auto json = Replace(ValidPolicyJson(), "\"environment\":\"simnow\"", "\"environment\":\"sim\"");
    VerifiedTradeAccountingPolicyRegistry registry;
    std::string error;
    ASSERT_TRUE(registry.LoadFromJson(json, identity, &error));
    PolicyEvidence evidence;
    evidence.instrument.metadata.source = evidence.instrument.rows[0].source = "simulated";
    evidence.commission.metadata.source = evidence.commission.rows[0].source = "simulated";
    evidence.order_commission.metadata.source = evidence.order_commission.rows[0].source =
        "simulated";
    evidence.Observe(&registry);
    EXPECT_TRUE(registry.ReadyFor({instrument}, kDay, &error)) << error;
    evidence.instrument.metadata.source = "ctp";
    registry.ObserveInstrumentQuery(evidence.instrument);
    EXPECT_FALSE(registry.ReadyFor({instrument}, kDay, &error));
}

TEST_F(VerifiedTradeAccountingPolicyTest, RestoredEvidenceOnlyAuthorizesIdenticalHistoricalPolicy) {
    VerifiedTradeAccountingPolicyRegistry verified;
    std::string error;
    ASSERT_TRUE(verified.LoadFromJson(ValidPolicyJson(), identity, &error));
    PolicyEvidence{}.Observe(&verified);
    ASSERT_TRUE(verified.ReadyFor({instrument}, kDay, &error));
    VerifiedTradeAccountingPolicyRegistry::EvidenceState saved;
    ASSERT_TRUE(verified.ExportValidatedEvidence(&saved, &error)) << error;
    ASSERT_FALSE(saved.empty());

    VerifiedTradeAccountingPolicyRegistry restored;
    ASSERT_TRUE(restored.LoadFromJson(ValidPolicyJson(), identity, &error));
    ASSERT_TRUE(restored.RestoreValidatedEvidence(saved, &error)) << error;
    EXPECT_TRUE(restored.ResolveHistorical(MatchingTrade(), &error).valuation_inputs_verified)
        << error;
    EXPECT_FALSE(restored.ReadyFor({instrument}, kDay, &error));
    EXPECT_FALSE(restored.Resolve(MatchingTrade(), &error).valuation_inputs_verified);
    PolicyEvidence{}.Observe(&restored);
    EXPECT_TRUE(restored.ReadyFor({instrument}, kDay, &error)) << error;
    EXPECT_TRUE(restored.Resolve(MatchingTrade(), &error).valuation_inputs_verified) << error;

    const std::vector<std::pair<std::string, std::string>> changes = {
        {"\"by_money\":0.001", "\"by_money\":0.0011"},
        {"\"policy_source\":\"sample\"", "\"policy_source\":\"another_sample\""},
        {"\"policy_version\":\"v1\"", "\"policy_version\":\"v2\""}};
    for (const auto& [from, to] : changes) {
        SCOPED_TRACE(to);
        VerifiedTradeAccountingPolicyRegistry changed;
        ASSERT_TRUE(changed.LoadFromJson(Replace(ValidPolicyJson(), from, to), identity, &error));
        ASSERT_TRUE(changed.RestoreValidatedEvidence(saved, &error)) << error;
        EXPECT_FALSE(changed.ResolveHistorical(MatchingTrade(), &error).valuation_inputs_verified);
        EXPECT_FALSE(changed.ReadyFor({instrument}, kDay, &error));
        EXPECT_FALSE(changed.Resolve(MatchingTrade(), &error).valuation_inputs_verified);
    }
}

TEST_F(VerifiedTradeAccountingPolicyTest, NewSessionRequiresMatchingFreshQueryGeneration) {
    VerifiedTradeAccountingPolicyRegistry registry;
    std::string error;
    ASSERT_TRUE(registry.LoadFromJson(ValidPolicyJson(), identity, &error));
    PolicyEvidence evidence;
    evidence.instrument.metadata.generation = 1;
    evidence.commission.metadata.generation = 1;
    evidence.order_commission.metadata.generation = 1;
    evidence.Observe(&registry);
    ASSERT_TRUE(registry.ReadyFor({instrument}, kDay, &error)) << error;
    VerifiedTradeAccountingPolicyRegistry::EvidenceState saved;
    ASSERT_TRUE(registry.ExportValidatedEvidence(&saved, &error));
    ASSERT_TRUE(registry.RestoreValidatedEvidence(saved, &error));

    registry.BeginSession(2);
    EXPECT_FALSE(registry.ReadyFor({instrument}, kDay, &error));
    EXPECT_FALSE(registry.Resolve(MatchingTrade(), &error).valuation_inputs_verified);
    EXPECT_TRUE(registry.ResolveHistorical(MatchingTrade(), &error).valuation_inputs_verified)
        << error;
    evidence.Observe(&registry);  // Late callbacks from the previous same-day session.
    EXPECT_FALSE(registry.ReadyFor({instrument}, kDay, &error));

    evidence.instrument.metadata.generation = 2;
    evidence.commission.metadata.generation = 2;
    evidence.Observe(&registry);  // The order-fee query still belongs to generation 1.
    EXPECT_FALSE(registry.ReadyFor({instrument}, kDay, &error));
    evidence.order_commission.metadata.generation = 2;
    registry.ObserveOrderCommissionQuery(evidence.order_commission);
    EXPECT_TRUE(registry.ReadyFor({instrument}, kDay, &error)) << error;
    EXPECT_TRUE(registry.Resolve(MatchingTrade(), &error).valuation_inputs_verified) << error;

    VerifiedTradeAccountingPolicyRegistry implicit_session;
    ASSERT_TRUE(implicit_session.LoadFromJson(ValidPolicyJson(), identity, &error));
    evidence.commission.metadata.generation = 3;
    evidence.Observe(&implicit_session);
    EXPECT_FALSE(implicit_session.ReadyFor({instrument}, kDay, &error));
    evidence.instrument.metadata.generation = 3;
    evidence.order_commission.metadata.generation = 3;
    evidence.Observe(&implicit_session);
    EXPECT_TRUE(implicit_session.ReadyFor({instrument}, kDay, &error)) << error;
}

}  // namespace
}  // namespace quant_hft
