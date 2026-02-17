#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <sstream>
#include <string>

#include <gtest/gtest.h>

namespace {

int RunCommand(const std::string& command) {
    return std::system(command.c_str());
}

std::filesystem::path MakeTempDir(const std::string& suffix) {
    const auto path = std::filesystem::temp_directory_path() /
                      ("quant_hft_consistency_gate_test_" + suffix);
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);
    return path;
}

std::string ReadFile(const std::filesystem::path& path) {
    std::ifstream input(path);
    std::ostringstream buffer;
    buffer << input.rdbuf();
    return buffer.str();
}

void WriteFile(const std::filesystem::path& path, const std::string& payload) {
    if (!path.parent_path().empty()) {
        std::filesystem::create_directories(path.parent_path());
    }
    std::ofstream out(path);
    out << payload;
}

std::string EscapePathForShell(const std::filesystem::path& path) {
    std::string text = path.string();
    std::string escaped;
    escaped.reserve(text.size() + 8);
    for (const char ch : text) {
        if (ch == '\'') {
            escaped += "'\\''";
        } else {
            escaped.push_back(ch);
        }
    }
    return escaped;
}

TEST(ConsistencyGateScriptTest, GeneratesConsistencyReports) {
    const auto temp_root = MakeTempDir("generate");
    const auto results_dir = temp_root / "results";
    const auto csv_path = temp_root / "sample.csv";

    const std::string command = "bash scripts/build/run_consistency_gates.sh --build-dir build " +
                                std::string("--csv-path '") + EscapePathForShell(csv_path) +
                                "' --results-dir '" + EscapePathForShell(results_dir) + "'";
    const int rc = RunCommand(command);
    EXPECT_EQ(rc, 0);

    const auto shadow = results_dir / "shadow_consistency_report.json";
    const auto backtest = results_dir / "backtest_consistency_report.json";
    const auto summary = results_dir / "consistency_gate_summary.json";

    EXPECT_TRUE(std::filesystem::exists(shadow));
    EXPECT_TRUE(std::filesystem::exists(backtest));
    EXPECT_TRUE(std::filesystem::exists(summary));

    const std::string backtest_payload = ReadFile(backtest);
    EXPECT_NE(backtest_payload.find("\"status\": \"pass\""), std::string::npos);

    const std::string summary_payload = ReadFile(summary);
    EXPECT_NE(summary_payload.find("\"shadow_consistency\": \"pass\""), std::string::npos);
    EXPECT_NE(summary_payload.find("\"backtest_consistency\": \"pass\""), std::string::npos);
}

TEST(ConsistencyGateScriptTest, FailsWhenBaselineIsMissing) {
    const auto temp_root = MakeTempDir("missing_baseline");
    const auto results_dir = temp_root / "results";
    const auto csv_path = temp_root / "sample.csv";
    const auto missing_baseline = temp_root / "missing_baseline.json";
    const auto missing_provenance = temp_root / "missing_provenance.json";

    const std::string command =
        "bash scripts/build/run_consistency_gates.sh --build-dir build " +
        std::string("--csv-path '") + EscapePathForShell(csv_path) + "' --results-dir '" +
        EscapePathForShell(results_dir) + "' --baseline-json '" +
        EscapePathForShell(missing_baseline) + "' --provenance-json '" +
        EscapePathForShell(missing_provenance) + "'";
    const int rc = RunCommand(command);
    EXPECT_NE(rc, 0);

    const auto summary = results_dir / "consistency_gate_summary.json";
    EXPECT_TRUE(std::filesystem::exists(summary));
    const std::string summary_payload = ReadFile(summary);
    EXPECT_NE(summary_payload.find("\"backtest_consistency\": \"fail\""), std::string::npos);
}

TEST(ConsistencyGateScriptTest, FailsWhenConsistencyExceedsTolerance) {
    const auto temp_root = MakeTempDir("diff_fail");
    const auto results_dir = temp_root / "results";
    const auto csv_path = temp_root / "diff.csv";
    WriteFile(
        csv_path,
        "symbol,exchange,ts_ns,last_price,last_volume,bid_price1,bid_volume1,ask_price1,ask_volume1,"
        "volume,turnover,open_interest\n"
        "rb2405,SHFE,1704067200000000000,100.0,1,99.9,5,100.1,5,10,1000,100\n"
        "rb2405,SHFE,1704067201000000000,101.0,1,100.9,5,101.1,5,11,1111,100\n"
        "rb2405,SHFE,1704067260000000000,98.0,1,97.9,5,98.1,5,12,1176,100\n"
        "rb2405,SHFE,1704067261000000000,97.0,1,96.9,5,97.1,5,13,1261,100\n");

    const std::string command = "bash scripts/build/run_consistency_gates.sh --build-dir build " +
                                std::string("--csv-path '") + EscapePathForShell(csv_path) +
                                "' --results-dir '" + EscapePathForShell(results_dir) +
                                "' --abs-tol 1e-8 --rel-tol 1e-6";
    const int rc = RunCommand(command);
    EXPECT_NE(rc, 0);

    const auto summary = results_dir / "consistency_gate_summary.json";
    EXPECT_TRUE(std::filesystem::exists(summary));
    const std::string summary_payload = ReadFile(summary);
    EXPECT_NE(summary_payload.find("\"backtest_consistency\": \"fail\""), std::string::npos);
}

}  // namespace
