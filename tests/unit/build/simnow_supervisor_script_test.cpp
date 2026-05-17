#include <gtest/gtest.h>

#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <sstream>
#include <string>

namespace {

int RunCommand(const std::string& command) { return std::system(command.c_str()); }

std::filesystem::path MakeTempDir(const std::string& suffix) {
    const auto path =
        std::filesystem::temp_directory_path() / ("quant_hft_simnow_supervisor_test_" + suffix);
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

std::string EscapeForShell(const std::string& text) {
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

std::string EscapePathForShell(const std::filesystem::path& path) {
    return EscapeForShell(path.string());
}

std::string RunSupervisorDryRun(const std::string& suffix, const std::string& fake_now) {
    const auto temp_root = MakeTempDir(suffix);
    const auto env_file = temp_root / "simnow.env";
    const auto output_file = temp_root / "dry_run.out";
    WriteFile(env_file, "# test env intentionally empty\n");

    const std::string command =
        "SIMNOW_FAKE_NOW='" + EscapeForShell(fake_now) + "' " + "SIMNOW_LOCK_DIR='" +
        EscapePathForShell(temp_root / "locks") + "' " +
        "bash scripts/ops/supervise_simnow_trading.sh " + "--env-file '" +
        EscapePathForShell(env_file) + "' " + "--run-root '" +
        EscapePathForShell(temp_root / "runs") + "' " + "--market-data-dir '" +
        EscapePathForShell(temp_root / "market") + "' " + "--wal-file '" +
        EscapePathForShell(temp_root / "wal" / "events.wal") + "' " + "--report-root '" +
        EscapePathForShell(temp_root / "reports") + "' " + "--export-root '" +
        EscapePathForShell(temp_root / "exports") + "' " + "--reconcile-root '" +
        EscapePathForShell(temp_root / "reconcile") + "' " +
        "--windows 'night=20:50-02:35,day_am=08:50-11:35,day_pm=13:20-15:20' " +
        "--no-eod --dry-run > '" + EscapePathForShell(output_file) + "' 2>&1";

    const int rc = RunCommand(command);
    EXPECT_EQ(rc, 0) << ReadFile(output_file);
    return ReadFile(output_file);
}

TEST(SimnowSupervisorScriptTest, FridayNightMapsToNextAllowedTradingDay) {
    const std::string output = RunSupervisorDryRun("friday_night", "2026-05-15 21:01:00");

    EXPECT_NE(
        output.find("[dry-run] decision=start_or_keep_alive session=night trading_day=20260518 "
                    "range=20:50-02:35"),
        std::string::npos)
        << output;
}

TEST(SimnowSupervisorScriptTest, SaturdayEarlyMorningContinuesFridayNightSession) {
    const std::string output = RunSupervisorDryRun("saturday_early", "2026-05-16 01:01:00");

    EXPECT_NE(
        output.find("[dry-run] decision=start_or_keep_alive session=night trading_day=20260518 "
                    "range=20:50-02:35"),
        std::string::npos)
        << output;
}

TEST(SimnowSupervisorScriptTest, WeekendNightDoesNotStartANewSession) {
    const std::string output = RunSupervisorDryRun("weekend_night", "2026-05-16 21:01:00");

    EXPECT_NE(output.find("[dry-run] decision=outside_trading_window"), std::string::npos)
        << output;
}

}  // namespace
