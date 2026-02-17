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

}  // namespace
