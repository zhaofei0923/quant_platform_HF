#include "quant_hft/backtest/product_fee_config_loader.h"

#include <gtest/gtest.h>

#include <chrono>
#include <filesystem>
#include <fstream>
#include <string>

namespace quant_hft {
namespace {

std::filesystem::path WriteTempFile(const std::string& stem, const std::string& suffix,
                                    const std::string& content) {
    const auto stamp = std::chrono::steady_clock::now().time_since_epoch().count();
    const auto path =
        std::filesystem::temp_directory_path() / (stem + "_" + std::to_string(stamp) + suffix);
    std::ofstream out(path);
    out << content;
    out.close();
    return path;
}

TEST(ProductFeeConfigLoaderTest, LoadsYamlAndSupportsInstrumentAndSymbolLookup) {
    const auto path = WriteTempFile("quant_hft_product_fee", ".yaml",
                                    "products:\n"
                                    "  rb2405:\n"
                                    "    symbol: rb\n"
                                    "    contract_multiplier: 10\n"
                                    "    long_margin_ratio: 0.16\n"
                                    "    short_margin_ratio: 0.17\n"
                                    "    open_mode: rate\n"
                                    "    open_value: 0.0001\n"
                                    "    close_mode: per_lot\n"
                                    "    close_value: 2\n"
                                    "    close_today_mode: per_lot\n"
                                    "    close_today_value: 3\n");

    ProductFeeBook book;
    std::string error;
    ASSERT_TRUE(LoadProductFeeConfig(path.string(), &book, &error)) << error;

    const ProductFeeEntry* exact = book.Find("rb2405");
    ASSERT_NE(exact, nullptr);
    EXPECT_DOUBLE_EQ(exact->contract_multiplier, 10.0);
    EXPECT_DOUBLE_EQ(exact->long_margin_ratio, 0.16);
    EXPECT_DOUBLE_EQ(exact->short_margin_ratio, 0.17);

    const ProductFeeEntry* fallback = book.Find("rb2406");
    ASSERT_NE(fallback, nullptr);
    EXPECT_EQ(fallback->instrument_id, "rb2405");

    EXPECT_NEAR(ProductFeeBook::ComputeCommission(*exact, OffsetFlag::kOpen, 2, 100.0), 0.2, 1e-12);
    EXPECT_NEAR(ProductFeeBook::ComputeCommission(*exact, OffsetFlag::kClose, 3, 100.0), 6.0,
                1e-12);
    EXPECT_NEAR(ProductFeeBook::ComputeCommission(*exact, OffsetFlag::kCloseToday, 1, 100.0), 3.0,
                1e-12);
    EXPECT_NEAR(ProductFeeBook::ComputePerLotMargin(*exact, Side::kBuy, 100.0), 160.0, 1e-12);
    EXPECT_NEAR(ProductFeeBook::ComputePerLotMargin(*exact, Side::kSell, 100.0), 170.0, 1e-12);
    EXPECT_NEAR(ProductFeeBook::ComputeRequiredMargin(*exact, Side::kSell, 3, 100.0), 510.0, 1e-12);

    std::filesystem::remove(path);
}

TEST(ProductFeeConfigLoaderTest, LoadsJsonConfig) {
    const auto path = WriteTempFile("quant_hft_product_fee", ".json",
                                    "{\n"
                                    "  \"products\": {\n"
                                    "    \"ag2406\": {\n"
                                    "      \"symbol\": \"ag\",\n"
                                    "      \"contract_multiplier\": 15,\n"
                                    "      \"long_margin_ratio\": 0.12,\n"
                                    "      \"short_margin_ratio\": 0.13,\n"
                                    "      \"open_mode\": \"per_lot\",\n"
                                    "      \"open_value\": 1.5,\n"
                                    "      \"close_mode\": \"rate\",\n"
                                    "      \"close_value\": 0.0002,\n"
                                    "      \"close_today_mode\": \"rate\",\n"
                                    "      \"close_today_value\": 0.0003\n"
                                    "    }\n"
                                    "  }\n"
                                    "}\n");

    ProductFeeBook book;
    std::string error;
    ASSERT_TRUE(LoadProductFeeConfig(path.string(), &book, &error)) << error;
    const ProductFeeEntry* exact = book.Find("ag2406");
    ASSERT_NE(exact, nullptr);
    EXPECT_DOUBLE_EQ(exact->contract_multiplier, 15.0);
    EXPECT_DOUBLE_EQ(exact->long_margin_ratio, 0.12);
    EXPECT_DOUBLE_EQ(exact->short_margin_ratio, 0.13);
    EXPECT_NEAR(ProductFeeBook::ComputeCommission(*exact, OffsetFlag::kOpen, 2, 5000.0), 3.0,
                1e-12);
    EXPECT_NEAR(ProductFeeBook::ComputeCommission(*exact, OffsetFlag::kClose, 1, 5000.0), 15.0,
                1e-12);

    std::filesystem::remove(path);
}

TEST(ProductFeeConfigLoaderTest, RejectsInvalidMode) {
    const auto path = WriteTempFile("quant_hft_product_fee", ".yaml",
                                    "products:\n"
                                    "  rb2405:\n"
                                    "    symbol: rb\n"
                                    "    contract_multiplier: 10\n"
                                    "    long_margin_ratio: 0.16\n"
                                    "    short_margin_ratio: 0.16\n"
                                    "    open_mode: bad\n"
                                    "    open_value: 0.1\n"
                                    "    close_mode: per_lot\n"
                                    "    close_value: 1\n"
                                    "    close_today_mode: per_lot\n"
                                    "    close_today_value: 1\n");

    ProductFeeBook book;
    std::string error;
    EXPECT_FALSE(LoadProductFeeConfig(path.string(), &book, &error));
    EXPECT_NE(error.find("mode"), std::string::npos);

    std::filesystem::remove(path);
}

TEST(ProductFeeConfigLoaderTest, LoadsInstrumentInfoSchemaAndMapsCommissionModes) {
    const auto path = WriteTempFile("quant_hft_product_fee", ".json",
                                    "{\n"
                                    "  \"products\": {\n"
                                    "    \"RB\": {\n"
                                    "      \"product\": \"RB\",\n"
                                    "      \"volume_multiple\": 10,\n"
                                    "      \"long_margin_ratio\": 0.16,\n"
                                    "      \"short_margin_ratio\": 0.16,\n"
                                    "      \"commission\": {\n"
                                    "        \"open_ratio_by_money\": 0.0001,\n"
                                    "        \"open_ratio_by_volume\": 0,\n"
                                    "        \"close_ratio_by_money\": 0,\n"
                                    "        \"close_ratio_by_volume\": 1.5,\n"
                                    "        \"close_today_ratio_by_money\": 0,\n"
                                    "        \"close_today_ratio_by_volume\": 3\n"
                                    "      }\n"
                                    "    }\n"
                                    "  }\n"
                                    "}\n");

    ProductFeeBook book;
    std::string error;
    ASSERT_TRUE(LoadProductFeeConfig(path.string(), &book, &error)) << error;
    const ProductFeeEntry* entry = book.Find("rb2405");
    ASSERT_NE(entry, nullptr);
    EXPECT_EQ(entry->instrument_id, "RB");
    EXPECT_DOUBLE_EQ(entry->contract_multiplier, 10.0);
    EXPECT_EQ(entry->open_mode, ProductFeeMode::kRate);
    EXPECT_DOUBLE_EQ(entry->open_value, 0.0001);
    EXPECT_EQ(entry->close_mode, ProductFeeMode::kPerLot);
    EXPECT_DOUBLE_EQ(entry->close_value, 1.5);
    EXPECT_EQ(entry->close_today_mode, ProductFeeMode::kPerLot);
    EXPECT_DOUBLE_EQ(entry->close_today_value, 3.0);

    std::filesystem::remove(path);
}

TEST(ProductFeeConfigLoaderTest, LoadsInstrumentInfoYamlSchemaAndMapsCommissionModes) {
    const auto path = WriteTempFile("quant_hft_product_fee", ".yaml",
                                    "products:\n"
                                    "  RB:\n"
                                    "    product: RB\n"
                                    "    volume_multiple: 10\n"
                                    "    long_margin_ratio: 0.16\n"
                                    "    short_margin_ratio: 0.16\n"
                                    "    commission:\n"
                                    "      open_ratio_by_money: 0.0001\n"
                                    "      open_ratio_by_volume: 0\n"
                                    "      close_ratio_by_money: 0\n"
                                    "      close_ratio_by_volume: 1.5\n"
                                    "      close_today_ratio_by_money: 0\n"
                                    "      close_today_ratio_by_volume: 3\n");

    ProductFeeBook book;
    std::string error;
    ASSERT_TRUE(LoadProductFeeConfig(path.string(), &book, &error)) << error;
    const ProductFeeEntry* entry = book.Find("rb2405");
    ASSERT_NE(entry, nullptr);
    EXPECT_EQ(entry->instrument_id, "RB");
    EXPECT_DOUBLE_EQ(entry->contract_multiplier, 10.0);
    EXPECT_EQ(entry->open_mode, ProductFeeMode::kRate);
    EXPECT_DOUBLE_EQ(entry->open_value, 0.0001);
    EXPECT_EQ(entry->close_mode, ProductFeeMode::kPerLot);
    EXPECT_DOUBLE_EQ(entry->close_value, 1.5);
    EXPECT_EQ(entry->close_today_mode, ProductFeeMode::kPerLot);
    EXPECT_DOUBLE_EQ(entry->close_today_value, 3.0);

    std::filesystem::remove(path);
}

TEST(ProductFeeConfigLoaderTest, LoadsRawInstrumentInfoJsonRootWithoutProducts) {
    const auto path = WriteTempFile("quant_hft_instrument_info", ".json",
                                    "{\n"
                                    "  \"RB\": {\n"
                                    "    \"commission\": {\n"
                                    "      \"open_ratio_by_money\": 0.0001,\n"
                                    "      \"open_ratio_by_volume\": 0,\n"
                                    "      \"close_ratio_by_money\": 0,\n"
                                    "      \"close_ratio_by_volume\": 1.5,\n"
                                    "      \"close_today_ratio_by_money\": 0,\n"
                                    "      \"close_today_ratio_by_volume\": 3\n"
                                    "    },\n"
                                    "    \"long_margin_ratio\": 0.16,\n"
                                    "    \"short_margin_ratio\": 0.16,\n"
                                    "    \"product\": \"RB\",\n"
                                    "    \"trading_sessions\": [\"21:00:00-23:00:00\"],\n"
                                    "    \"volume_multiple\": 10\n"
                                    "  }\n"
                                    "}\n");

    ProductFeeBook book;
    std::string error;
    ASSERT_TRUE(LoadProductFeeConfig(path.string(), &book, &error)) << error;
    const ProductFeeEntry* entry = book.Find("rb2405");
    ASSERT_NE(entry, nullptr);
    EXPECT_EQ(entry->instrument_id, "RB");
    EXPECT_DOUBLE_EQ(entry->contract_multiplier, 10.0);
    EXPECT_EQ(entry->open_mode, ProductFeeMode::kRate);
    EXPECT_EQ(entry->close_mode, ProductFeeMode::kPerLot);

    std::filesystem::remove(path);
}

TEST(ProductFeeConfigLoaderTest, LoadsYamlWithTradingSessionsListIgnored) {
    const auto path = WriteTempFile("quant_hft_product_fee", ".yaml",
                                    "products:\n"
                                    "  RB:\n"
                                    "    product: RB\n"
                                    "    volume_multiple: 10\n"
                                    "    long_margin_ratio: 0.16\n"
                                    "    short_margin_ratio: 0.16\n"
                                    "    trading_sessions:\n"
                                    "      - 21:00:00-23:00:00\n"
                                    "      - 09:00:00-10:15:00\n"
                                    "    commission:\n"
                                    "      open_ratio_by_money: 0.0001\n"
                                    "      open_ratio_by_volume: 0\n"
                                    "      close_ratio_by_money: 0\n"
                                    "      close_ratio_by_volume: 1.5\n"
                                    "      close_today_ratio_by_money: 0\n"
                                    "      close_today_ratio_by_volume: 3\n");

    ProductFeeBook book;
    std::string error;
    ASSERT_TRUE(LoadProductFeeConfig(path.string(), &book, &error)) << error;
    const ProductFeeEntry* entry = book.Find("rb2405");
    ASSERT_NE(entry, nullptr);
    EXPECT_EQ(entry->instrument_id, "RB");
    EXPECT_DOUBLE_EQ(entry->contract_multiplier, 10.0);

    std::filesystem::remove(path);
}

TEST(ProductFeeConfigLoaderTest, RejectsCommissionMoneyAndVolumeBothPositive) {
    const auto path = WriteTempFile("quant_hft_product_fee", ".json",
                                    "{\n"
                                    "  \"products\": {\n"
                                    "    \"RB\": {\n"
                                    "      \"product\": \"RB\",\n"
                                    "      \"volume_multiple\": 10,\n"
                                    "      \"long_margin_ratio\": 0.16,\n"
                                    "      \"short_margin_ratio\": 0.16,\n"
                                    "      \"commission\": {\n"
                                    "        \"open_ratio_by_money\": 0.0001,\n"
                                    "        \"open_ratio_by_volume\": 0.1,\n"
                                    "        \"close_ratio_by_money\": 0,\n"
                                    "        \"close_ratio_by_volume\": 1,\n"
                                    "        \"close_today_ratio_by_money\": 0,\n"
                                    "        \"close_today_ratio_by_volume\": 1\n"
                                    "      }\n"
                                    "    }\n"
                                    "  }\n"
                                    "}\n");

    ProductFeeBook book;
    std::string error;
    EXPECT_FALSE(LoadProductFeeConfig(path.string(), &book, &error));
    EXPECT_NE(error.find("both"), std::string::npos);

    std::filesystem::remove(path);
}

TEST(ProductFeeConfigLoaderTest, RejectsMissingMarginRatio) {
    const auto path = WriteTempFile("quant_hft_product_fee", ".yaml",
                                    "products:\n"
                                    "  rb2405:\n"
                                    "    symbol: rb\n"
                                    "    contract_multiplier: 10\n"
                                    "    open_mode: rate\n"
                                    "    open_value: 0.0001\n"
                                    "    close_mode: per_lot\n"
                                    "    close_value: 2\n"
                                    "    close_today_mode: per_lot\n"
                                    "    close_today_value: 3\n");

    ProductFeeBook book;
    std::string error;
    EXPECT_FALSE(LoadProductFeeConfig(path.string(), &book, &error));
    EXPECT_NE(error.find("margin"), std::string::npos);

    std::filesystem::remove(path);
}

}  // namespace
}  // namespace quant_hft
