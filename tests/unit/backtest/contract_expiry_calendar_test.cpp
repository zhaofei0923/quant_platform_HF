#include "quant_hft/backtest/contract_expiry_calendar.h"

#include <gtest/gtest.h>

#include <chrono>
#include <filesystem>
#include <fstream>
#include <string>

namespace quant_hft {
namespace {

std::filesystem::path MakeTempDir(const std::string& stem) {
    const auto stamp = std::chrono::steady_clock::now().time_since_epoch().count();
    const auto path = std::filesystem::temp_directory_path() / (stem + "_" + std::to_string(stamp));
    std::filesystem::create_directories(path);
    return path;
}

std::filesystem::path WriteFile(const std::filesystem::path& path, const std::string& content) {
    std::filesystem::create_directories(path.parent_path());
    std::ofstream out(path);
    out << content;
    out.close();
    return path;
}

TEST(ContractExpiryCalendarTest, LoadsYamlAndCanonicalizesInstrumentIds) {
    const std::filesystem::path root = MakeTempDir("quant_hft_contract_expiry_calendar");
    const std::filesystem::path path =
        WriteFile(root / "contract_expiry_calendar.yaml",
                  "contracts:\n"
                  "  c2405:\n"
                  "    last_trading_day: 20240329\n"
                  "  ma501:\n"
                  "    last_trading_day: 20241129\n");

    ContractExpiryCalendar calendar;
    std::string error;
    ASSERT_TRUE(LoadContractExpiryCalendar(path.string(), &calendar, &error)) << error;

    const ContractExpiryEntry* c2405 = calendar.Find("C2405");
    ASSERT_NE(c2405, nullptr);
    EXPECT_EQ(c2405->instrument_id, "c2405");
    EXPECT_EQ(c2405->last_trading_day, "20240329");

    const ContractExpiryEntry* ma501 = calendar.Find("MA501");
    ASSERT_NE(ma501, nullptr);
    EXPECT_EQ(ma501->instrument_id, "ma501");
    EXPECT_EQ(ma501->last_trading_day, "20241129");

    std::error_code ec;
    std::filesystem::remove_all(root, ec);
}

TEST(ContractExpiryCalendarTest, RejectsInvalidLastTradingDay) {
    const std::filesystem::path root = MakeTempDir("quant_hft_contract_expiry_calendar_invalid");
    const std::filesystem::path path =
        WriteFile(root / "contract_expiry_calendar.yaml",
                  "contracts:\n"
                  "  c2405:\n"
                  "    last_trading_day: 2024-03-29\n");

    ContractExpiryCalendar calendar;
    std::string error;
    EXPECT_FALSE(LoadContractExpiryCalendar(path.string(), &calendar, &error));
    EXPECT_NE(error.find("last_trading_day"), std::string::npos);

    std::error_code ec;
    std::filesystem::remove_all(root, ec);
}

}  // namespace
}  // namespace quant_hft
