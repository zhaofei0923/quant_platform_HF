#include <gtest/gtest.h>

#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <sstream>
#include <string>

#ifndef QUANT_HFT_BUILD_DIR
#error "QUANT_HFT_BUILD_DIR is required"
#endif

namespace {

std::filesystem::path BuildDir() { return std::filesystem::path(QUANT_HFT_BUILD_DIR); }

std::filesystem::path RepoRoot() { return BuildDir().parent_path(); }

std::filesystem::path BinaryPath(const std::string& name) { return BuildDir() / name; }

std::string ReadFile(const std::filesystem::path& path) {
    std::ifstream input(path);
    std::ostringstream buffer;
    buffer << input.rdbuf();
    return buffer.str();
}

int RunCommandCapture(const std::string& command, const std::filesystem::path& output_file) {
    const std::string shell_command = command + " > \"" + output_file.string() + "\" 2>&1";
    return std::system(shell_command.c_str());
}

std::filesystem::path MakeTempDir(const std::string& suffix) {
    const auto base = std::filesystem::temp_directory_path() / ("quant_hft_ops_cli_test_" + suffix);
    std::filesystem::remove_all(base);
    std::filesystem::create_directories(base);
    return base;
}

void WriteFile(const std::filesystem::path& path, const std::string& content) {
    std::filesystem::create_directories(path.parent_path());
    std::ofstream out(path);
    out << content;
}

TEST(OpsCli, HealthReportWritesStructuredSchema) {
    const auto dir = MakeTempDir("health");
    const auto output_log = dir / "health_stdout.log";
    const auto output_json = dir / "health.json";
    const auto output_md = dir / "health.md";

    const std::string command = "\"" + BinaryPath("ops_health_report_cli").string() +
                                "\" --output_json \"" + output_json.string() + "\" --output_md \"" +
                                output_md.string() +
                                "\" --strategy-engine-latency-ms 320"
                                " --strategy-engine-target-ms 1000"
                                " --strategy-engine-chain-status complete"
                                " --storage-redis-health healthy"
                                " --storage-timescale-health healthy"
                                " --operator kevin"
                                " --host localhost"
                                " --build test-build"
                                " --config-profile sim"
                                " --interface eth0";
    const int rc = RunCommandCapture(command, output_log);

    EXPECT_EQ(rc, 0);
    const std::string json = ReadFile(output_json);
    const std::string markdown = ReadFile(output_md);
    EXPECT_NE(json.find("\"generated_ts_ns\""), std::string::npos);
    EXPECT_NE(json.find("\"overall_healthy\": true"), std::string::npos);
    EXPECT_NE(json.find("quant_hft_strategy_engine_latency_p99_ms"), std::string::npos);
    EXPECT_NE(json.find("quant_hft_strategy_engine_chain_integrity"), std::string::npos);
    EXPECT_NE(markdown.find("# Ops Health Report"), std::string::npos);
    EXPECT_NE(markdown.find("quant_hft_strategy_engine_chain_integrity"), std::string::npos);
}

TEST(OpsCli, AlertReportEvaluatesCriticalAlertsFromHealthReport) {
    const auto dir = MakeTempDir("alert");
    const auto health_json = dir / "health.json";
    const auto health_md = dir / "health.md";
    const auto health_log = dir / "health_stdout.log";
    const auto alert_json = dir / "alert.json";
    const auto alert_md = dir / "alert.md";
    const auto alert_log = dir / "alert_stdout.log";

    const std::string health_command = "\"" + BinaryPath("ops_health_report_cli").string() +
                                       "\" --output_json \"" + health_json.string() +
                                       "\" --output_md \"" + health_md.string() +
                                       "\" --strategy-engine-latency-ms 3200"
                                       " --strategy-engine-target-ms 1000"
                                       " --strategy-engine-chain-status incomplete"
                                       " --storage-redis-health unhealthy"
                                       " --storage-timescale-health healthy";
    EXPECT_EQ(RunCommandCapture(health_command, health_log), 0);

    const std::string alert_command = "\"" + BinaryPath("ops_alert_report_cli").string() +
                                      "\" --health-json-file \"" + health_json.string() +
                                      "\" --output_json \"" + alert_json.string() +
                                      "\" --output_md \"" + alert_md.string() + "\"";
    const int rc = RunCommandCapture(alert_command, alert_log);

    EXPECT_EQ(rc, 0);
    const std::string json = ReadFile(alert_json);
    const std::string markdown = ReadFile(alert_md);
    EXPECT_NE(json.find("\"overall_healthy\": false"), std::string::npos);
    EXPECT_NE(json.find("\"severity\": \"critical\""), std::string::npos);
    EXPECT_NE(json.find("OPS_STRATEGY_ENGINE_CHAIN_INTEGRITY_UNHEALTHY"), std::string::npos);
    EXPECT_NE(markdown.find("quant_hft_strategy_engine_chain_integrity"), std::string::npos);
}

TEST(OpsCli, CtpCutoverOrchestratorDryRunProducesEvidenceFiles) {
    const auto dir = MakeTempDir("cutover_dry");
    const auto cutover_template = dir / "cutover.env";
    const auto rollback_template = dir / "rollback.env";
    const auto cutover_output = dir / "cutover_result.env";
    const auto rollback_output = dir / "rollback_result.env";
    const auto stdout_log = dir / "stdout.log";

    WriteFile(cutover_template,
              "CUTOVER_ENV_NAME=prodlike\n"
              "CUTOVER_WINDOW_LOCAL=2026-02-13T09:00:00+08:00\n"
              "CTP_CONFIG_PATH=configs/prod/ctp.yaml\n"
              "OLD_CORE_ENGINE_STOP_CMD=echo stop-old-core\n"
              "PRECHECK_CMD=echo precheck\n"
              "BOOTSTRAP_INFRA_CMD=echo bootstrap\n"
              "INIT_KAFKA_TOPIC_CMD=echo init-kafka\n"
              "INIT_CLICKHOUSE_SCHEMA_CMD=echo init-clickhouse\n"
              "INIT_DEBEZIUM_CONNECTOR_CMD=echo init-debezium\n"
              "NEW_CORE_ENGINE_START_CMD=echo start-new-core\n"
              "WARMUP_QUERY_CMD=echo warmup\n"
              "POST_SWITCH_MONITOR_MINUTES=30\n"
              "MONITOR_KEYS=order_latency_p99_ms,breaker_state\n"
              "CUTOVER_EVIDENCE_OUTPUT=docs/results/ctp_cutover_result.env\n");

    WriteFile(rollback_template,
              "ROLLBACK_ENV_NAME=prodlike\n"
              "ROLLBACK_TRIGGER_CONDITION=order_latency_gt_5ms\n"
              "NEW_CORE_ENGINE_STOP_CMD=echo stop-new-core\n"
              "RESTORE_PREVIOUS_BINARIES_CMD=echo restore-binaries\n"
              "RESTORE_STRATEGY_ENGINE_COMPAT_CMD=echo restore-engine\n"
              "PREVIOUS_CORE_ENGINE_START_CMD=echo start-prev-core\n"
              "POST_ROLLBACK_VALIDATE_CMD=echo validate-rollback\n"
              "MAX_ROLLBACK_SECONDS=180\n"
              "ROLLBACK_EVIDENCE_OUTPUT=docs/results/ctp_rollback_result.env\n");

    const std::string command = "\"" + BinaryPath("ctp_cutover_orchestrator_cli").string() +
                                "\" --cutover-template \"" + cutover_template.string() +
                                "\" --rollback-template \"" + rollback_template.string() +
                                "\" --cutover-output \"" + cutover_output.string() +
                                "\" --rollback-output \"" + rollback_output.string() + "\"";

    const int rc = RunCommandCapture(command, stdout_log);
    EXPECT_EQ(rc, 0);

    const std::string cutover_payload = ReadFile(cutover_output);
    const std::string rollback_payload = ReadFile(rollback_output);
    EXPECT_NE(cutover_payload.find("CUTOVER_DRY_RUN=1"), std::string::npos);
    EXPECT_NE(cutover_payload.find("CUTOVER_SUCCESS=true"), std::string::npos);
    EXPECT_NE(cutover_payload.find("CUTOVER_TRIGGERED_ROLLBACK=false"), std::string::npos);
    EXPECT_NE(rollback_payload.find("ROLLBACK_TRIGGERED=false"), std::string::npos);
    EXPECT_NE(rollback_payload.find("ROLLBACK_TOTAL_STEPS=0"), std::string::npos);
}

TEST(OpsCli, CtpCutoverOrchestratorExecuteFailureTriggersRollback) {
    const auto dir = MakeTempDir("cutover_failure");
    const auto cutover_template = dir / "cutover.env";
    const auto rollback_template = dir / "rollback.env";
    const auto cutover_output = dir / "cutover_result.env";
    const auto rollback_output = dir / "rollback_result.env";
    const auto stdout_log = dir / "stdout.log";

    WriteFile(cutover_template,
              "CUTOVER_ENV_NAME=prodlike\n"
              "CUTOVER_WINDOW_LOCAL=2026-02-13T09:00:00+08:00\n"
              "CTP_CONFIG_PATH=configs/prod/ctp.yaml\n"
              "OLD_CORE_ENGINE_STOP_CMD=echo stop-old-core\n"
              "PRECHECK_CMD=false\n"
              "BOOTSTRAP_INFRA_CMD=echo bootstrap\n"
              "INIT_KAFKA_TOPIC_CMD=echo init-kafka\n"
              "INIT_CLICKHOUSE_SCHEMA_CMD=echo init-clickhouse\n"
              "INIT_DEBEZIUM_CONNECTOR_CMD=echo init-debezium\n"
              "NEW_CORE_ENGINE_START_CMD=echo start-new-core\n"
              "WARMUP_QUERY_CMD=echo warmup\n"
              "POST_SWITCH_MONITOR_MINUTES=30\n"
              "MONITOR_KEYS=order_latency_p99_ms,breaker_state\n"
              "CUTOVER_EVIDENCE_OUTPUT=docs/results/ctp_cutover_result.env\n");

    WriteFile(rollback_template,
              "ROLLBACK_ENV_NAME=prodlike\n"
              "ROLLBACK_TRIGGER_CONDITION=order_latency_gt_5ms\n"
              "NEW_CORE_ENGINE_STOP_CMD=echo stop-new-core\n"
              "RESTORE_PREVIOUS_BINARIES_CMD=echo restore-binaries\n"
              "RESTORE_STRATEGY_ENGINE_COMPAT_CMD=echo restore-engine\n"
              "PREVIOUS_CORE_ENGINE_START_CMD=echo start-prev-core\n"
              "POST_ROLLBACK_VALIDATE_CMD=echo validate-rollback\n"
              "MAX_ROLLBACK_SECONDS=180\n"
              "ROLLBACK_EVIDENCE_OUTPUT=docs/results/ctp_rollback_result.env\n");

    const std::string command =
        "\"" + BinaryPath("ctp_cutover_orchestrator_cli").string() + "\" --cutover-template \"" +
        cutover_template.string() + "\" --rollback-template \"" + rollback_template.string() +
        "\" --cutover-output \"" + cutover_output.string() + "\" --rollback-output \"" +
        rollback_output.string() + "\" --execute";

    const int rc = RunCommandCapture(command, stdout_log);
    EXPECT_NE(rc, 0);

    const std::string cutover_payload = ReadFile(cutover_output);
    const std::string rollback_payload = ReadFile(rollback_output);
    EXPECT_NE(cutover_payload.find("CUTOVER_DRY_RUN=0"), std::string::npos);
    EXPECT_NE(cutover_payload.find("CUTOVER_SUCCESS=false"), std::string::npos);
    EXPECT_NE(cutover_payload.find("CUTOVER_FAILED_STEP=precheck"), std::string::npos);
    EXPECT_NE(cutover_payload.find("CUTOVER_TRIGGERED_ROLLBACK=true"), std::string::npos);
    EXPECT_NE(rollback_payload.find("ROLLBACK_TRIGGERED=true"), std::string::npos);
    EXPECT_NE(rollback_payload.find("ROLLBACK_SUCCESS=true"), std::string::npos);
}

TEST(OpsCli, VerifyContractSyncCliReportsSuccessOnRepositoryContracts) {
    const auto dir = MakeTempDir("verify_contract");
    const auto stdout_log = dir / "stdout.log";

    const std::string command = "\"" + BinaryPath("verify_contract_sync_cli").string() + "\"";
    const int rc = RunCommandCapture(command, stdout_log);
    EXPECT_EQ(rc, 0);

    const std::string output = ReadFile(stdout_log);
    EXPECT_NE(output.find("contract sync verification passed"), std::string::npos);

    const auto cpp_contract_path = RepoRoot() / "include/quant_hft/contracts/types.h";
    const auto proto_contract_path = RepoRoot() / "proto/quant_hft/v1/contracts.proto";
    EXPECT_NE(ReadFile(cpp_contract_path).find("signal_type"), std::string::npos);
    EXPECT_NE(ReadFile(proto_contract_path).find("signal_type"), std::string::npos);
}

TEST(OpsCli, VerifyDevelopRequirementsCliRejectsMissingPath) {
    const auto dir = MakeTempDir("verify_requirements");
    const auto develop_root = dir / "develop";
    const auto requirements_file = dir / "requirements.json";
    const auto stdout_log = dir / "stdout.log";

    WriteFile(develop_root / "doc.md", "# sample\nall requirements are implemented\n");
    WriteFile(requirements_file,
              "{\n"
              "  \"requirements\": [\n"
              "    {\n"
              "      \"id\": \"REQ-FAIL-001\",\n"
              "      \"doc\": \"develop/doc.md\",\n"
              "      \"description\": \"intentional missing path\",\n"
              "      \"code_paths\": [\"src/not_found.cpp\"],\n"
              "      \"test_paths\": [\"tests/not_found.cpp\"],\n"
              "      \"evidence_paths\": [\"docs/results/not_found.md\"]\n"
              "    }\n"
              "  ]\n"
              "}\n");

    const std::string command = "\"" + BinaryPath("verify_develop_requirements_cli").string() +
                                "\" --requirements-file \"" + requirements_file.string() +
                                "\" --develop-root \"" + develop_root.string() + "\"";
    const int rc = RunCommandCapture(command, stdout_log);

    EXPECT_NE(rc, 0);
    const std::string output = ReadFile(stdout_log);
    EXPECT_NE(output.find("missing path"), std::string::npos);
}

TEST(OpsCli, CsvToParquetCliWritesManifestAndMeta) {
    const auto dir = MakeTempDir("csv_to_parquet");
    const auto input_csv = dir / "rb_sample.csv";
    const auto output_root = dir / "parquet_v2";
    const auto stdout_log = dir / "stdout.log";
    const auto manifest = output_root / "_manifest" / "partitions.jsonl";

    WriteFile(input_csv,
              "TradingDay,InstrumentID,UpdateTime,UpdateMillisec,LastPrice,Volume,BidPrice1,"
              "BidVolume1,AskPrice1,AskVolume1,AveragePrice,Turnover,OpenInterest\n"
              "20230103,rb2305,08:59:00,500,4100.0,100,4099.0,3,4101.0,4,41000.0,1234500.0,1000\n"
              "20230103,rb2305,09:00:00,0,4102.0,101,4101.0,3,4103.0,4,41020.0,1239900.0,1001\n");

    const std::string command = "\"" + BinaryPath("csv_to_parquet_cli").string() +
                                "\" --input_csv \"" + input_csv.string() + "\" --output_root \"" +
                                output_root.string() + "\" --source rb --resume true";
    const int rc = RunCommandCapture(command, stdout_log);

    EXPECT_EQ(rc, 0);
    const std::filesystem::path part = output_root / "source=rb" / "trading_day=20230103" /
                                       "instrument_id=rb2305" / "part-0000.parquet";
    const std::filesystem::path meta = std::filesystem::path(part.string() + ".meta");
    EXPECT_TRUE(std::filesystem::exists(part));
    EXPECT_TRUE(std::filesystem::exists(meta));
    EXPECT_TRUE(std::filesystem::exists(manifest));

    {
        std::ifstream parquet_stream(part, std::ios::binary);
        ASSERT_TRUE(parquet_stream.is_open());
        std::string bytes((std::istreambuf_iterator<char>(parquet_stream)),
                          std::istreambuf_iterator<char>());
        ASSERT_GE(bytes.size(), 8U);
        EXPECT_EQ(bytes.substr(0, 4), "PAR1");
        EXPECT_EQ(bytes.substr(bytes.size() - 4), "PAR1");
    }

    const std::string meta_text = ReadFile(meta);
    EXPECT_NE(meta_text.find("min_ts_ns="), std::string::npos);
    EXPECT_NE(meta_text.find("max_ts_ns="), std::string::npos);
    EXPECT_NE(meta_text.find("row_count="), std::string::npos);
    EXPECT_NE(meta_text.find("schema_version="), std::string::npos);
    EXPECT_NE(meta_text.find("source_csv_fingerprint="), std::string::npos);

    const std::string manifest_text = ReadFile(manifest);
    EXPECT_NE(manifest_text.find("\"source\":\"rb\""), std::string::npos);
    EXPECT_NE(manifest_text.find("\"instrument_id\":\"rb2305\""), std::string::npos);
    EXPECT_NE(manifest_text.find("\"trading_day\":\"20230103\""), std::string::npos);
}

TEST(OpsCli, CsvParquetCompareIncludesScanMetrics) {
    const auto dir = MakeTempDir("csv_parquet_metrics");
    const auto input_csv = dir / "rb_sample.csv";
    const auto output_root = dir / "parquet_v2";
    const auto convert_log = dir / "convert_stdout.log";
    const auto compare_log = dir / "compare_stdout.log";
    const auto compare_json = dir / "compare.json";
    const auto manifest = output_root / "_manifest" / "partitions.jsonl";

    WriteFile(input_csv,
              "TradingDay,InstrumentID,UpdateTime,UpdateMillisec,LastPrice,Volume,BidPrice1,"
              "BidVolume1,AskPrice1,AskVolume1,AveragePrice,Turnover,OpenInterest\n"
              "20230103,rb2305,08:59:00,500,4100.0,100,4099.0,3,4101.0,4,41000.0,1234500.0,1000\n"
              "20230103,rb2305,09:00:00,0,4102.0,101,4101.0,3,4103.0,4,41020.0,1239900.0,1001\n"
              "20230103,rb2305,09:00:01,0,4103.0,102,4102.0,3,4104.0,4,41030.0,1245300.0,1002\n");

    const std::string convert_command = "\"" + BinaryPath("csv_to_parquet_cli").string() +
                                        "\" --input_csv \"" + input_csv.string() +
                                        "\" --output_root \"" + output_root.string() +
                                        "\" --source rb --resume true";
    ASSERT_EQ(RunCommandCapture(convert_command, convert_log), 0);
    ASSERT_TRUE(std::filesystem::exists(manifest));

    const std::string compare_command =
        "\"" + BinaryPath("csv_parquet_compare_cli").string() + "\" --csv_path \"" +
        input_csv.string() + "\" --parquet_root \"" + output_root.string() +
        "\" --runs 1 --warmup_runs 0 --max_ticks 2 --output_json \"" + compare_json.string() + "\"";
    const int rc = RunCommandCapture(compare_command, compare_log);
    EXPECT_EQ(rc, 0);

    const std::string payload = ReadFile(compare_json);
    EXPECT_NE(payload.find("\"scan_rows\""), std::string::npos);
    EXPECT_NE(payload.find("\"scan_row_groups\""), std::string::npos);
    EXPECT_NE(payload.find("\"io_bytes\""), std::string::npos);
    EXPECT_NE(payload.find("\"early_stop_hit\""), std::string::npos);
}

TEST(OpsCli, CsvParquetCompareSupportsSymbolsFilter) {
    const auto dir = MakeTempDir("csv_parquet_symbols");
    const auto input_csv = dir / "rb_sample.csv";
    const auto output_root = dir / "parquet_v2";
    const auto convert_log = dir / "convert_stdout.log";
    const auto compare_log = dir / "compare_stdout.log";
    const auto compare_json = dir / "compare.json";

    WriteFile(input_csv,
              "TradingDay,InstrumentID,UpdateTime,UpdateMillisec,LastPrice,Volume,BidPrice1,"
              "BidVolume1,AskPrice1,AskVolume1,AveragePrice,Turnover,OpenInterest\n"
              "20230103,rb2305,08:59:00,500,4100.0,100,4099.0,3,4101.0,4,41000.0,1234500.0,1000\n"
              "20230103,rb2305,09:00:00,0,4102.0,101,4101.0,3,4103.0,4,41020.0,1239900.0,1001\n"
              "20230103,rb2306,09:00:00,0,4202.0,201,4201.0,3,4203.0,4,42020.0,2239900.0,2001\n"
              "20230103,rb2306,09:00:01,0,4203.0,202,4202.0,3,4204.0,4,42030.0,2245300.0,2002\n");

    const std::string convert_command = "\"" + BinaryPath("csv_to_parquet_cli").string() +
                                        "\" --input_csv \"" + input_csv.string() +
                                        "\" --output_root \"" + output_root.string() +
                                        "\" --source rb --resume true";
    ASSERT_EQ(RunCommandCapture(convert_command, convert_log), 0);

    const std::string compare_command =
        "\"" + BinaryPath("csv_parquet_compare_cli").string() + "\" --csv_path \"" +
        input_csv.string() + "\" --parquet_root \"" + output_root.string() +
        "\" --symbols rb2305 --runs 1 --warmup_runs 0 --output_json \"" + compare_json.string() +
        "\"";
    const int rc = RunCommandCapture(compare_command, compare_log);
    EXPECT_EQ(rc, 0);

    const std::string payload = ReadFile(compare_json);
    EXPECT_NE(payload.find("\"ticks_read_min\": 2"), std::string::npos);
    EXPECT_NE(payload.find("\"ticks_read_max\": 2"), std::string::npos);
}

TEST(OpsCli, FactorEvalCliIncludesDetectorConfigInOutputJson) {
    const auto dir = MakeTempDir("factor_eval_detector");
    const auto input_csv = dir / "rb_sample.csv";
    const auto detector_yaml = dir / "detector.yaml";
    const auto output_jsonl = dir / "tracker.jsonl";
    const auto output_json = dir / "factor_eval.json";
    const auto stdout_log = dir / "factor_eval_stdout.log";

    WriteFile(input_csv,
              "TradingDay,InstrumentID,UpdateTime,UpdateMillisec,LastPrice,Volume,BidPrice1,"
              "BidVolume1,AskPrice1,AskVolume1,AveragePrice,Turnover,OpenInterest\n"
              "20230103,rb2305,08:59:00,500,4100.0,100,4099.0,3,4101.0,4,41000.0,1234500.0,1000\n"
              "20230103,rb2305,09:00:00,0,4102.0,101,4101.0,3,4103.0,4,41020.0,1239900.0,1001\n"
              "20230103,rb2305,09:00:01,0,4103.0,102,4102.0,3,4104.0,4,41030.0,1245300.0,1002\n");
    WriteFile(detector_yaml,
              "market_state_detector:\n"
              "  adx_period: 7\n"
              "  atr_period: 5\n");

    const std::string command = "\"" + BinaryPath("factor_eval_cli").string() +
                                "\" --factor_id factor-test --template trend"
                                " --csv_path \"" +
                                input_csv.string() + "\" --detector_config \"" +
                                detector_yaml.string() + "\" --max_ticks 100 --output_jsonl \"" +
                                output_jsonl.string() + "\" --output_json \"" +
                                output_json.string() + "\"";
    const int rc = RunCommandCapture(command, stdout_log);
    EXPECT_EQ(rc, 0);

    const std::string payload = ReadFile(output_json);
    EXPECT_NE(payload.find("\"detector_config\": \"" + detector_yaml.string() + "\""),
              std::string::npos);
    EXPECT_NE(payload.find("\"market_state_detector\""), std::string::npos);
    EXPECT_NE(payload.find("\"adx_period\": 7"), std::string::npos);
    EXPECT_NE(payload.find("\"atr_period\": 5"), std::string::npos);

    const std::string tracker_payload = ReadFile(output_jsonl);
    EXPECT_NE(tracker_payload.find("\"detector_config\":\"" + detector_yaml.string() + "\""),
              std::string::npos);
    EXPECT_NE(tracker_payload.find("\"market_state_detector\":{\"adx_period\":7"),
              std::string::npos);
    EXPECT_NE(tracker_payload.find("\"atr_period\":5"), std::string::npos);
}

}  // namespace
