#include <gtest/gtest.h>

#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <sstream>
#include <string>

namespace {

int RunCommand(const std::string& command) { return std::system(command.c_str()); }

std::filesystem::path MakeTempDir(const std::string& suffix) {
    const auto path = std::filesystem::temp_directory_path() /
                      ("quant_hft_preprod_rehearsal_gate_test_" + suffix);
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

TEST(PreprodRehearsalGateScriptTest, GeneratesPreprodAndRollbackEvidence) {
    const auto temp_root = MakeTempDir("generate");
    const auto results_dir = temp_root / "results";

    const std::string command =
        "bash scripts/build/run_preprod_rehearsal_gate.sh --build-dir build --results-dir '" +
        EscapePathForShell(results_dir) + "'";
    const int rc = RunCommand(command);
    EXPECT_EQ(rc, 0);

    const auto report_json = results_dir / "preprod_rehearsal_report.json";
    const auto report_md = results_dir / "preprod_rehearsal_report.md";
    const auto cutover_env = results_dir / "preprod_cutover_result.env";
    const auto rollback_env = results_dir / "preprod_rollback_result.env";

    EXPECT_TRUE(std::filesystem::exists(report_json));
    EXPECT_TRUE(std::filesystem::exists(report_md));
    EXPECT_TRUE(std::filesystem::exists(cutover_env));
    EXPECT_TRUE(std::filesystem::exists(rollback_env));

    const std::string report_payload = ReadFile(report_json);
    EXPECT_NE(report_payload.find("\"status\": \"pass\""), std::string::npos);
    EXPECT_NE(report_payload.find("\"rollback_drill\": \"pass\""), std::string::npos);

    const std::string rollback_payload = ReadFile(rollback_env);
    EXPECT_NE(rollback_payload.find("ROLLBACK_TRIGGERED=true"), std::string::npos);
    EXPECT_NE(rollback_payload.find("ROLLBACK_SUCCESS=true"), std::string::npos);
}

}  // namespace
