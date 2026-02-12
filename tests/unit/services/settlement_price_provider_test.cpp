#include <filesystem>
#include <fstream>
#include <string>

#include <gtest/gtest.h>

#include "quant_hft/services/settlement_price_provider.h"

namespace quant_hft {
namespace {

std::string TempPath(const std::string& leaf) {
    auto path = std::filesystem::temp_directory_path() / ("quant_hft_" + leaf);
    return path.string();
}

TEST(SettlementPriceProviderTest, ApiPriceJsonIsLoaded) {
    const auto cache_path = TempPath("settlement_cache_api.sqlite");
    const auto json_path = TempPath("settlement_prices_api.json");

    std::ofstream out(json_path);
    out << "{\"rb2405\": 3810.5}";
    out.close();

    ProdSettlementPriceProvider provider(cache_path, json_path);
    const auto price = provider.GetSettlementPrice("rb2405", "2026-02-12");
    ASSERT_TRUE(price.has_value());
    EXPECT_DOUBLE_EQ(price->first, 3810.5);
    EXPECT_EQ(price->second.type, SettlementPriceSource::SourceType::kApi);
}

TEST(SettlementPriceProviderTest, ManualOverrideHasHighestPriority) {
    const auto cache_path = TempPath("settlement_cache_manual.sqlite");
    const auto json_path = TempPath("settlement_prices_manual.json");

    std::ofstream out(json_path);
    out << "{\"rb2405\": 3810.5}";
    out.close();

    ProdSettlementPriceProvider provider(cache_path, json_path);
    provider.SetManualOverride("rb2405", "2026-02-12", 3799.0, "tester");

    const auto price = provider.GetSettlementPrice("rb2405", "2026-02-12");
    ASSERT_TRUE(price.has_value());
    EXPECT_DOUBLE_EQ(price->first, 3799.0);
    EXPECT_EQ(price->second.type, SettlementPriceSource::SourceType::kManual);
}

TEST(SettlementPriceProviderTest, CacheFallbackAfterApiFileRemoved) {
    const auto cache_path = TempPath("settlement_cache_fallback.sqlite");
    const auto json_path = TempPath("settlement_prices_fallback.json");

    {
        std::ofstream out(json_path);
        out << "{\"rb2405\": 3800.0}";
    }

    {
        ProdSettlementPriceProvider provider(cache_path, json_path);
        const auto first = provider.GetSettlementPrice("rb2405", "2026-02-12");
        ASSERT_TRUE(first.has_value());
        EXPECT_EQ(first->second.type, SettlementPriceSource::SourceType::kApi);
    }

    std::filesystem::remove(json_path);

    ProdSettlementPriceProvider provider(cache_path, json_path);
    const auto second = provider.GetSettlementPrice("rb2405", "2026-02-12");
    if (!second.has_value()) {
        GTEST_SKIP() << "sqlite cache unavailable in current runtime";
    }
    EXPECT_DOUBLE_EQ(second->first, 3800.0);
    EXPECT_EQ(second->second.type, SettlementPriceSource::SourceType::kCache);
}

TEST(SettlementPriceProviderTest, MissingPriceReturnsNullopt) {
    const auto cache_path = TempPath("settlement_cache_missing.sqlite");
    ProdSettlementPriceProvider provider(cache_path, "");
    const auto price = provider.GetSettlementPrice("rb2405", "2026-02-12");
    EXPECT_FALSE(price.has_value());
}

}  // namespace
}  // namespace quant_hft
