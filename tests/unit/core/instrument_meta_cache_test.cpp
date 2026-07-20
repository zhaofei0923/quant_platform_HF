#include "quant_hft/core/instrument_meta_cache.h"

#include <gtest/gtest.h>

#include <chrono>
#include <filesystem>
#include <fstream>
#include <string>

namespace quant_hft {
namespace {

InstrumentMetaSnapshot Meta(const std::string& instrument_id,
                            const std::string& open_date = "20250101",
                            const std::string& expire_date = "20261231") {
    InstrumentMetaSnapshot meta;
    meta.instrument_id = instrument_id;
    meta.exchange_id = "DCE";
    meta.product_id = ExtractFuturesProductId(instrument_id);
    meta.volume_multiple = 10;
    meta.price_tick = 1.0;
    meta.open_date = open_date;
    meta.expire_date = expire_date;
    meta.is_trading = true;
    meta.product_class = "1";
    meta.source = "OnRspQryInstrument";
    return meta;
}

std::filesystem::path TempPath(const std::string& suffix) {
    const auto token = std::chrono::steady_clock::now().time_since_epoch().count();
    return std::filesystem::temp_directory_path() /
           ("quant_hft_instrument_meta_cache_" + std::to_string(token) + suffix);
}

TEST(InstrumentMetaCacheTest, FiltersLifecycleTradingAndProductClass) {
    auto stopped = Meta("c2611");
    stopped.is_trading = false;
    auto option = Meta("c2611-C-2000");
    option.product_class = "2";
    auto wrong_product = Meta("m2609");

    const std::vector<InstrumentMetaSnapshot> queried{Meta("c2609"),
                                                      Meta("c2607", "20250101", "20260630"),
                                                      Meta("c2701", "20260801", "20271231"),
                                                      stopped,
                                                      option,
                                                      wrong_product};
    const auto product_metadata = CollectProductInstrumentMetadata(queried, "c");
    EXPECT_EQ(product_metadata.size(), 5U);

    const auto eligible = FilterEligibleFuturesContracts(queried, "c", "20260719");

    ASSERT_EQ(eligible.size(), 1U);
    EXPECT_EQ(eligible.front().instrument_id, "c2609");
    EXPECT_TRUE(IsEligibleFuturesContract(eligible.front(), "c", "20260719"));
}

TEST(InstrumentMetaCacheTest, WritesAndLoadsAtomicV2Document) {
    const auto path = TempPath(".json");
    std::string error;
    ASSERT_TRUE(WriteInstrumentMetaCacheV2Atomically(
        path.string(), "c", "20260719", {Meta("c2609"), Meta("c2611")}, 1'000'000'000LL, &error))
        << error;

    InstrumentMetaCacheDocument document;
    ASSERT_TRUE(LoadInstrumentMetaCacheDocument(path.string(), &document, &error)) << error;
    EXPECT_EQ(document.schema_version, 2);
    EXPECT_FALSE(document.legacy);
    EXPECT_EQ(document.product_id, "c");
    EXPECT_EQ(document.broker_trading_day, "20260719");
    ASSERT_EQ(document.instruments.size(), 2U);

    std::string reason;
    EXPECT_TRUE(
        IsInstrumentMetaCacheCurrent(document, "20260719", 1'500'000'000LL, 1'000, &reason));
    EXPECT_FALSE(
        IsInstrumentMetaCacheCurrent(document, "20260720", 1'500'000'000LL, 1'000, &reason));
    EXPECT_EQ(reason, "broker_trading_day_mismatch");
    std::filesystem::remove(path);
}

TEST(InstrumentMetaCacheTest, LegacyArrayIsReadableButNeverCurrent) {
    const auto path = TempPath("_legacy.json");
    {
        std::ofstream out(path);
        out << "[\n"
               "  {\n"
               "    \"instrument_id\": \"c2609\",\n"
               "    \"exchange_id\": \"DCE\",\n"
               "    \"product_id\": \"c\",\n"
               "    \"volume_multiple\": 10,\n"
               "    \"price_tick\": 1.0\n"
               "  }\n"
               "]\n";
    }
    InstrumentMetaCacheDocument document;
    std::string error;
    ASSERT_TRUE(LoadInstrumentMetaCacheDocument(path.string(), &document, &error)) << error;
    EXPECT_TRUE(document.legacy);
    ASSERT_EQ(document.instruments.size(), 1U);
    std::string reason;
    EXPECT_FALSE(
        IsInstrumentMetaCacheCurrent(document, "20260719", 1'000'000'000LL, 86'400'000, &reason));
    EXPECT_EQ(reason, "legacy_schema_requires_refresh");
    std::filesystem::remove(path);
}

TEST(InstrumentMetaCacheTest, RejectsCorruptDocument) {
    const auto path = TempPath("_corrupt.json");
    {
        std::ofstream out(path);
        out << "{not-json}";
    }
    InstrumentMetaCacheDocument document;
    std::string error;
    EXPECT_FALSE(LoadInstrumentMetaCacheDocument(path.string(), &document, &error));
    EXPECT_FALSE(error.empty());
    std::filesystem::remove(path);
}

TEST(InstrumentMetaCacheTest, RejectsTruncatedV2Document) {
    const auto path = TempPath("_truncated.json");
    {
        std::ofstream out(path);
        out << "{\n"
               "  \"schema_version\": 2,\n"
               "  \"product_id\": \"c\",\n"
               "  \"broker_trading_day\": \"20260719\",\n"
               "  \"generated_ts_ns\": 1000000000,\n"
               "  \"instruments\": [\n";
    }
    InstrumentMetaCacheDocument document;
    std::string error;
    EXPECT_FALSE(LoadInstrumentMetaCacheDocument(path.string(), &document, &error));
    EXPECT_EQ(error, "instrument cache is truncated");
    std::filesystem::remove(path);
}

TEST(InstrumentMetaCacheTest, WritesLoadsAndValidatesUniverseManifest) {
    const auto path = TempPath("_manifest.json");
    InstrumentUniverseManifest manifest;
    manifest.schema_version = 1;
    manifest.broker_trading_day = "20260720";
    manifest.generated_ts_ns = 2'000'000'000LL;
    manifest.generation = 2;
    manifest.complete = true;
    manifest.cache_dir = "/tmp/universe/20260720.2";
    manifest.product_ids = {"hc", "c"};
    std::string error;
    ASSERT_TRUE(WriteInstrumentUniverseManifestAtomically(path.string(), manifest, &error))
        << error;

    InstrumentUniverseManifest loaded;
    ASSERT_TRUE(LoadInstrumentUniverseManifest(path.string(), &loaded, &error)) << error;
    EXPECT_EQ(loaded.schema_version, 1);
    EXPECT_EQ(loaded.broker_trading_day, "20260720");
    EXPECT_EQ(loaded.generation, 2U);
    EXPECT_EQ(loaded.product_ids, (std::vector<std::string>{"c", "hc"}));
    std::string reason;
    EXPECT_TRUE(IsInstrumentUniverseManifestCurrent(loaded, "20260720", {"c", "hc"},
                                                    2'500'000'000LL, 1'000, &reason));
    EXPECT_FALSE(IsInstrumentUniverseManifestCurrent(loaded, "20260721", {"c", "hc"},
                                                     2'500'000'000LL, 1'000, &reason));
    EXPECT_EQ(reason, "broker_trading_day_mismatch");
    EXPECT_FALSE(IsInstrumentUniverseManifestCurrent(loaded, "20260720", {"c", "hc", "m"},
                                                     2'500'000'000LL, 1'000, &reason));
    EXPECT_EQ(reason, "required_product_missing:m");
    std::filesystem::remove(path);
}

TEST(InstrumentMetaCacheTest, RejectsIncompleteUniverseManifest) {
    const auto path = TempPath("_manifest_incomplete.json");
    {
        std::ofstream out(path);
        out << "{\n"
               "  \"schema_version\": 1,\n"
               "  \"broker_trading_day\": \"20260720\",\n"
               "  \"generated_ts_ns\": 2000000000,\n"
               "  \"generation\": 2,\n"
               "  \"complete\": false,\n"
               "  \"cache_dir\": \"/tmp/universe\",\n"
               "  \"products_csv\": \"c,hc\"\n"
               "}\n";
    }
    InstrumentUniverseManifest loaded;
    std::string error;
    EXPECT_FALSE(LoadInstrumentUniverseManifest(path.string(), &loaded, &error));
    EXPECT_EQ(error, "instrument universe manifest is incomplete");
    std::filesystem::remove(path);
}

}  // namespace
}  // namespace quant_hft
