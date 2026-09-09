#include <fcntl.h>
#include <gtest/gtest.h>
#include <signal.h>
#include <sys/wait.h>
#include <unistd.h>

#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <string>
#include <thread>
#include <vector>

namespace {
class FormalConnectionToolsTest : public ::testing::Test {
   protected:
    void SetUp() override {
        auto pattern = (std::filesystem::temp_directory_path() / "quant-tools-XXXXXX").string();
        ASSERT_NE(::mkdtemp(pattern.data()), nullptr);
        root_ = pattern;
        connection_ = root_ / "connection.yaml";
        std::ofstream(connection_) << "ctp:\n"
                                      "  environment: simnow\n"
                                      "  enable_real_api: true\n"
                                      "  is_production_mode: false\n"
                                      "  broker_id: test_broker\n"
                                      "  user_id: test_account\n"
                                      "  investor_id: test_account\n"
                                      "  account_id: test_account\n"
                                      "  enable_terminal_auth: false\n"
                                      "  md_front: tcp://127.0.0.1:1\n"
                                      "  td_front: tcp://127.0.0.1:2\n"
                                      "  strategy_factory: composite\n"
                                      "  instruments: hc2701\n"
                                      "  password: ${QUANT_TOOLS_TEST_SECRET}\n";
    }
    void TearDown() override { std::filesystem::remove_all(root_); }

    void AddLateValidationSentinel() {
        // This field is parsed after the legacy strategy-map requirement. It proves
        // deferral while forcing a deterministic exit before any gateway is created.
        std::ofstream(connection_, std::ios::app)
            << "  strategy_state_persist_enabled: invalid_bool_sentinel\n";
    }

    std::pair<int, std::string> Run(const std::string& name, std::vector<std::string> args) {
        const auto binary = std::filesystem::canonical("/proc/self/exe").parent_path() / name;
        const auto output_path = root_ / (name + ".log");
        const pid_t child = ::fork();
        if (child < 0) return {-1, "fork failed"};
        if (child == 0) {
            ::clearenv();
            ::setenv("QUANT_TOOLS_TEST_SECRET", "synthetic-test-secret", 1);
            // The probe rejects this scope after parsing, before constructing its gateway.
            ::setenv("SIMNOW_PRODUCT_SCOPE", "invalid:scope:sentinel", 1);
            if (::chdir(root_.c_str()) != 0) ::_exit(125);
            const int output = ::open(output_path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0600);
            if (output < 0 || ::dup2(output, STDOUT_FILENO) < 0 ||
                ::dup2(output, STDERR_FILENO) < 0)
                ::_exit(126);
            ::close(output);
            std::vector<char*> argv;
            argv.push_back(const_cast<char*>(binary.c_str()));
            for (auto& arg : args) argv.push_back(arg.data());
            argv.push_back(nullptr);
            ::execv(binary.c_str(), argv.data());
            ::_exit(127);
        }
        int status = 0;
        const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
        while (::waitpid(child, &status, WNOHANG) == 0) {
            if (std::chrono::steady_clock::now() > deadline) {
                ::kill(child, SIGKILL);
                ::waitpid(child, &status, 0);
                break;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(5));
        }
        std::ifstream input(output_path);
        const std::string output((std::istreambuf_iterator<char>(input)), {});
        return {WIFEXITED(status) ? WEXITSTATUS(status) : -1, output};
    }
    std::filesystem::path root_;
    std::filesystem::path connection_;
};

TEST_F(FormalConnectionToolsTest, ProbeParsesFormalConnectionBeforeOfflineScopeRejection) {
    const auto result = Run("simnow_probe", {connection_.string(), "--monitor-seconds", "0"});
    if (result.second.find("ctp_real_api_disabled") != std::string::npos)
        GTEST_SKIP() << "Query-only probe is unavailable in a stub SDK build";
    EXPECT_EQ(result.first, 3) << result.second;
    EXPECT_NE(result.second.find("configured_product_scope_invalid"), std::string::npos)
        << result.second;
    EXPECT_EQ(result.second.find("config_load_failed"), std::string::npos);
    EXPECT_EQ(result.second.find("probe_started"), std::string::npos);
    EXPECT_EQ(result.second.find("synthetic-test-secret"), std::string::npos);
    EXPECT_FALSE(std::filesystem::exists(root_ / "ctp_flow"));
}

TEST_F(FormalConnectionToolsTest, SettlementDefersLegacyStrategyGateButValidatesConnectionFields) {
    AddLateValidationSentinel();
    const auto result = Run("daily_settlement", {"--config", connection_.string(), "--trading-day",
                                                 "20260909", "--shadow"});
    EXPECT_EQ(result.first, 1) << result.second;
    EXPECT_NE(result.second.find("strategy_state_persist_enabled must be bool"), std::string::npos)
        << result.second;
    EXPECT_EQ(result.second.find("strategy_composite_config_map is required"), std::string::npos);
    EXPECT_EQ(result.second.find("trader_connect_failed"), std::string::npos);
    EXPECT_EQ(result.second.find("ctp_front_candidate_connect_attempt"), std::string::npos);
    EXPECT_EQ(result.second.find("synthetic-test-secret"), std::string::npos);
    EXPECT_FALSE(std::filesystem::exists(root_ / "ctp_flow"));
}

TEST_F(FormalConnectionToolsTest, SnapshotOnlyFlattenToolDefersLegacyStrategyGate) {
    AddLateValidationSentinel();
    const auto result = Run("simnow_flatten_positions", {"--config", connection_.string()});
    EXPECT_EQ(result.first, 1) << result.second;
    EXPECT_NE(result.second.find("strategy_state_persist_enabled must be bool"), std::string::npos)
        << result.second;
    EXPECT_EQ(result.second.find("strategy_composite_config_map is required"), std::string::npos);
    EXPECT_EQ(result.second.find("run_started"), std::string::npos);
    EXPECT_EQ(result.second.find("ctp_front_candidate_connect_attempt"), std::string::npos);
    EXPECT_EQ(result.second.find("synthetic-test-secret"), std::string::npos);
    EXPECT_FALSE(std::filesystem::exists(root_ / "ctp_flow"));
}

TEST_F(FormalConnectionToolsTest, ExecutingFlattenToolRetainsLegacyStrategyRequirements) {
    AddLateValidationSentinel();
    const auto result =
        Run("simnow_flatten_positions", {"--config", connection_.string(), "--execute"});
    EXPECT_EQ(result.first, 1) << result.second;
    EXPECT_NE(result.second.find("strategy_composite_config_map is required"), std::string::npos)
        << result.second;
    EXPECT_EQ(result.second.find("strategy_state_persist_enabled must be bool"), std::string::npos);
    EXPECT_EQ(result.second.find("run_started"), std::string::npos);
    EXPECT_EQ(result.second.find("ctp_front_candidate_connect_attempt"), std::string::npos);
    EXPECT_EQ(result.second.find("synthetic-test-secret"), std::string::npos);
    EXPECT_FALSE(std::filesystem::exists(root_ / "ctp_flow"));
}
}  // namespace
