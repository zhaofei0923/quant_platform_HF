#include <gtest/gtest.h>

#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <sstream>
#include <string>

namespace {

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

int RunCommand(const std::string& command) { return std::system(command.c_str()); }

std::filesystem::path MakeTempDir(const std::string& suffix) {
    const auto path =
        std::filesystem::temp_directory_path() / ("quant_hft_quality_gate_test_" + suffix);
    std::filesystem::remove_all(path);
    std::filesystem::create_directories(path);
    return path;
}

void WriteFile(const std::filesystem::path& path, const std::string& payload) {
    if (!path.parent_path().empty()) {
        std::filesystem::create_directories(path.parent_path());
    }
    std::ofstream out(path);
    out << payload;
}

TEST(QualityGateScriptsTest, RepoPurityCheckPassesCurrentRepository) {
    const int rc = RunCommand("bash scripts/build/repo_purity_check.sh --repo-root .");
    EXPECT_EQ(rc, 0);
}

TEST(QualityGateScriptsTest, RepoPurityCheckFailsWhenPythonAssetExists) {
    const auto root = MakeTempDir("purity_fail");
    WriteFile(root / "README.md", "# sample\n");
    WriteFile(root / "sample.py", "print('x')\n");

    const std::string cmd =
        "bash scripts/build/repo_purity_check.sh --repo-root '" + EscapePathForShell(root) + "'";
    const int rc = RunCommand(cmd);
    EXPECT_NE(rc, 0);
}

TEST(QualityGateScriptsTest, DependencyAuditPassesCurrentBuild) {
    const int rc = RunCommand("bash scripts/build/dependency_audit.sh --build-dir build");
    EXPECT_EQ(rc, 0);
}

TEST(QualityGateScriptsTest, BacktestBaselineCheckPassesCurrentRepository) {
    const int rc = RunCommand(
        "bash scripts/build/check_backtest_baseline.sh "
        "--baseline-json "
        "tests/regression/backtest_consistency/baseline/legacy_python/backtest_baseline.json "
        "--provenance-json "
        "tests/regression/backtest_consistency/baseline/legacy_python/provenance.json");
    EXPECT_EQ(rc, 0);
}

TEST(QualityGateScriptsTest, BacktestBaselineCheckFailsForMissingProvenanceFields) {
    const auto root = MakeTempDir("baseline_fail");
    const auto baseline = root / "baseline.json";
    const auto provenance = root / "provenance.json";
    WriteFile(baseline,
              "{\n"
              "  \"run_id\": \"r1\",\n"
              "  \"mode\": \"deterministic\",\n"
              "  \"spec\": {},\n"
              "  \"replay\": {},\n"
              "  \"deterministic\": {},\n"
              "  \"summary\": {}\n"
              "}\n");
    WriteFile(provenance,
              "{\n"
              "  \"source\": \"legacy_python\"\n"
              "}\n");

    const std::string cmd = "bash scripts/build/check_backtest_baseline.sh --baseline-json '" +
                            EscapePathForShell(baseline) + "' --provenance-json '" +
                            EscapePathForShell(provenance) + "'";
    const int rc = RunCommand(cmd);
    EXPECT_NE(rc, 0);
}

TEST(QualityGateScriptsTest, CsvParquetSpeedupGateFailsWhenSpeedupBelowThreshold) {
    const auto root = MakeTempDir("speedup_gate_fail");
    const auto compare_json = root / "compare.json";
    WriteFile(compare_json,
              "{\n"
              "  \"equal\": true,\n"
              "  \"summary\": {\n"
              "    \"parquet_vs_csv_speedup\": 4.5\n"
              "  }\n"
              "}\n");

    const std::string cmd = "bash scripts/build/run_csv_parquet_speedup_gate.sh --input-json '" +
                            EscapePathForShell(compare_json) + "' --min-speedup 5.0";
    const int rc = RunCommand(cmd);
    EXPECT_NE(rc, 0);
}

TEST(QualityGateScriptsTest, CsvParquetSpeedupGatePassesWhenSpeedupMeetsThreshold) {
    const auto root = MakeTempDir("speedup_gate_pass");
    const auto compare_json = root / "compare.json";
    WriteFile(compare_json,
              "{\n"
              "  \"equal\": true,\n"
              "  \"summary\": {\n"
              "    \"parquet_vs_csv_speedup\": 6.2\n"
              "  }\n"
              "}\n");

    const std::string cmd = "bash scripts/build/run_csv_parquet_speedup_gate.sh --input-json '" +
                            EscapePathForShell(compare_json) + "' --min-speedup 5.0";
    const int rc = RunCommand(cmd);
    EXPECT_EQ(rc, 0);
}

}  // namespace
