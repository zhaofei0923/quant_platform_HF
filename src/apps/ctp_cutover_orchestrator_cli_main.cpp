#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <map>
#include <sstream>
#include <string>
#include <vector>

#include "quant_hft/apps/cli_support.h"
#include "quant_hft/apps/ops_report_support.h"

namespace {

struct StepResult {
    std::string name;
    std::string status;
    int duration_ms{0};
    std::string command;
    int exit_code{0};
};

using EnvMap = std::map<std::string, std::string>;

bool ReadTextFile(const std::string& path, std::string* out, std::string* error) {
    if (out == nullptr) {
        return false;
    }
    std::ifstream input(path);
    if (!input.is_open()) {
        if (error != nullptr) {
            *error = "unable to open file: " + path;
        }
        return false;
    }
    std::ostringstream buffer;
    buffer << input.rdbuf();
    *out = buffer.str();
    return true;
}

EnvMap ParseEnvTemplate(const std::string& text) {
    EnvMap values;
    std::istringstream lines(text);
    std::string raw_line;
    while (std::getline(lines, raw_line)) {
        const std::string line = quant_hft::apps::ops_detail::Trim(raw_line);
        if (line.empty() || line.front() == '#') {
            continue;
        }
        const std::size_t delimiter = line.find('=');
        if (delimiter == std::string::npos) {
            continue;
        }
        const std::string key = quant_hft::apps::ops_detail::Trim(line.substr(0, delimiter));
        const std::string value = quant_hft::apps::ops_detail::Trim(line.substr(delimiter + 1));
        if (!key.empty()) {
            values[key] = value;
        }
    }
    return values;
}

bool RequireKeys(const EnvMap& values, const std::vector<std::string>& keys,
                 const std::string& label, std::string* error) {
    std::vector<std::string> missing;
    for (const std::string& key : keys) {
        const auto it = values.find(key);
        if (it == values.end() || quant_hft::apps::ops_detail::Trim(it->second).empty()) {
            missing.push_back(key);
        }
    }
    if (missing.empty()) {
        return true;
    }

    std::ostringstream message;
    message << label << " missing required keys: ";
    for (std::size_t index = 0; index < missing.size(); ++index) {
        if (index != 0) {
            message << ',';
        }
        message << missing[index];
    }
    if (error != nullptr) {
        *error = message.str();
    }
    return false;
}

StepResult RunStep(const std::string& name, const std::string& command, bool dry_run) {
    const auto started = std::chrono::steady_clock::now();
    StepResult result;
    result.name = name;
    result.command = command;
    result.status = "simulated_ok";
    result.exit_code = 0;

    if (!command.empty() && !dry_run) {
        const int rc = std::system(command.c_str());
        result.exit_code = rc;
        result.status = (rc == 0) ? "ok" : "failed";
    }

    const auto ended = std::chrono::steady_clock::now();
    result.duration_ms = static_cast<int>(
        std::chrono::duration_cast<std::chrono::milliseconds>(ended - started).count());
    if (result.duration_ms < 0) {
        result.duration_ms = 0;
    }
    return result;
}

std::pair<std::vector<StepResult>, std::string> RunSteps(
    const std::vector<std::pair<std::string, std::string>>& definitions, const EnvMap& values,
    bool dry_run) {
    std::vector<StepResult> results;
    std::string failed_step;

    for (const auto& [name, key] : definitions) {
        const auto it = values.find(key);
        const std::string command = (it == values.end()) ? "" : it->second;
        StepResult result = RunStep(name, command, dry_run);
        results.push_back(result);
        if (result.status == "failed") {
            failed_step = name;
            break;
        }
    }

    return {results, failed_step};
}

std::string BoolText(bool value) { return value ? "true" : "false"; }

std::string FormatSeconds(int duration_ms) {
    std::ostringstream oss;
    oss << std::fixed << std::setprecision(3) << static_cast<double>(duration_ms) / 1000.0;
    return oss.str();
}

int SumDurationMs(const std::vector<StepResult>& results) {
    int total = 0;
    for (const StepResult& item : results) {
        total += item.duration_ms;
    }
    return total;
}

void AppendStepLines(const std::vector<StepResult>& results, std::vector<std::string>* lines) {
    if (lines == nullptr) {
        return;
    }
    for (std::size_t index = 0; index < results.size(); ++index) {
        const StepResult& item = results[index];
        const std::size_t step_no = index + 1;
        lines->push_back("STEP_" + std::to_string(step_no) + "_NAME=" + item.name);
        lines->push_back("STEP_" + std::to_string(step_no) + "_STATUS=" + item.status);
        lines->push_back("STEP_" + std::to_string(step_no) +
                         "_DURATION_MS=" + std::to_string(item.duration_ms));
        lines->push_back("STEP_" + std::to_string(step_no) +
                         "_EXIT_CODE=" + std::to_string(item.exit_code));
        lines->push_back("STEP_" + std::to_string(step_no) + "_COMMAND=" + item.command);
    }
}

bool ParseNonNegativeDouble(const std::string& raw, double* out, std::string* error) {
    double value = 0.0;
    if (!quant_hft::apps::ops_detail::ParseDoubleText(raw, &value) || value < 0.0) {
        if (error != nullptr) {
            *error = "MAX_ROLLBACK_SECONDS must be >= 0: " + raw;
        }
        return false;
    }
    if (out != nullptr) {
        *out = value;
    }
    return true;
}

}  // namespace

int main(int argc, char** argv) {
    using namespace quant_hft::apps;
    const ArgMap args = ParseArgs(argc, argv);

    const std::string cutover_template_path =
        GetArg(args, "cutover-template", "configs/ops/ctp_cutover.template.env");
    const std::string rollback_template_path =
        GetArg(args, "rollback-template", "configs/ops/ctp_rollback_drill.template.env");

    std::string error;
    std::string cutover_template_text;
    std::string rollback_template_text;
    if (!ReadTextFile(cutover_template_path, &cutover_template_text, &error)) {
        std::cerr << "ctp_cutover_orchestrator_cli: " << error << '\n';
        return 2;
    }
    if (!ReadTextFile(rollback_template_path, &rollback_template_text, &error)) {
        std::cerr << "ctp_cutover_orchestrator_cli: " << error << '\n';
        return 2;
    }

    const EnvMap cutover = ParseEnvTemplate(cutover_template_text);
    const EnvMap rollback = ParseEnvTemplate(rollback_template_text);

    const std::vector<std::string> cutover_required = {
        "CUTOVER_ENV_NAME",
        "CUTOVER_WINDOW_LOCAL",
        "CTP_CONFIG_PATH",
        "OLD_CORE_ENGINE_STOP_CMD",
        "PRECHECK_CMD",
        "BOOTSTRAP_INFRA_CMD",
        "INIT_KAFKA_TOPIC_CMD",
        "INIT_CLICKHOUSE_SCHEMA_CMD",
        "INIT_DEBEZIUM_CONNECTOR_CMD",
        "NEW_CORE_ENGINE_START_CMD",
        "WARMUP_QUERY_CMD",
        "POST_SWITCH_MONITOR_MINUTES",
        "MONITOR_KEYS",
        "CUTOVER_EVIDENCE_OUTPUT",
    };
    const std::vector<std::string> rollback_required = {
        "ROLLBACK_ENV_NAME",
        "ROLLBACK_TRIGGER_CONDITION",
        "NEW_CORE_ENGINE_STOP_CMD",
        "RESTORE_PREVIOUS_BINARIES_CMD",
        "RESTORE_STRATEGY_ENGINE_COMPAT_CMD",
        "PREVIOUS_CORE_ENGINE_START_CMD",
        "POST_ROLLBACK_VALIDATE_CMD",
        "MAX_ROLLBACK_SECONDS",
        "ROLLBACK_EVIDENCE_OUTPUT",
    };

    if (!RequireKeys(cutover, cutover_required, "cutover template", &error)) {
        std::cerr << "ctp_cutover_orchestrator_cli: " << error << '\n';
        return 2;
    }
    if (!RequireKeys(rollback, rollback_required, "rollback template", &error)) {
        std::cerr << "ctp_cutover_orchestrator_cli: " << error << '\n';
        return 2;
    }

    double rollback_max_seconds = 0.0;
    if (!ParseNonNegativeDouble(rollback.at("MAX_ROLLBACK_SECONDS"), &rollback_max_seconds,
                                &error)) {
        std::cerr << "ctp_cutover_orchestrator_cli: " << error << '\n';
        return 2;
    }

    const bool dry_run = !HasArg(args, "execute");
    const bool force_rollback = HasArg(args, "force-rollback");

    std::string cutover_output = GetArg(args, "cutover-output");
    if (cutover_output.empty()) {
        // Backward-compatible alias used by legacy CI scripts.
        cutover_output = GetArg(args, "cutover_env");
    }
    if (cutover_output.empty()) {
        cutover_output = cutover.at("CUTOVER_EVIDENCE_OUTPUT");
    }

    std::string rollback_output = GetArg(args, "rollback-output");
    if (rollback_output.empty()) {
        // Backward-compatible alias used by legacy CI scripts.
        rollback_output = GetArg(args, "rollback_env");
    }
    if (rollback_output.empty()) {
        rollback_output = rollback.at("ROLLBACK_EVIDENCE_OUTPUT");
    }

    const std::vector<std::pair<std::string, std::string>> cutover_steps = {
        {"stop_old_core_engine", "OLD_CORE_ENGINE_STOP_CMD"},
        {"precheck", "PRECHECK_CMD"},
        {"bootstrap_infra", "BOOTSTRAP_INFRA_CMD"},
        {"init_kafka_topic", "INIT_KAFKA_TOPIC_CMD"},
        {"init_clickhouse_schema", "INIT_CLICKHOUSE_SCHEMA_CMD"},
        {"init_debezium_connector", "INIT_DEBEZIUM_CONNECTOR_CMD"},
        {"start_new_core_engine", "NEW_CORE_ENGINE_START_CMD"},
        {"warmup_query", "WARMUP_QUERY_CMD"},
    };

    const std::vector<std::pair<std::string, std::string>> rollback_steps = {
        {"stop_new_core_engine", "NEW_CORE_ENGINE_STOP_CMD"},
        {"restore_previous_binaries", "RESTORE_PREVIOUS_BINARIES_CMD"},
        {"restore_strategy_engine_compat", "RESTORE_STRATEGY_ENGINE_COMPAT_CMD"},
        {"start_previous_core_engine", "PREVIOUS_CORE_ENGINE_START_CMD"},
        {"post_rollback_validate", "POST_ROLLBACK_VALIDATE_CMD"},
    };

    const std::string cutover_started_utc = ops_detail::NowUtcIso8601Z();
    auto [cutover_results, cutover_failed_step] = RunSteps(cutover_steps, cutover, dry_run);
    const int cutover_duration_ms = SumDurationMs(cutover_results);
    const std::string cutover_completed_utc = ops_detail::NowUtcIso8601Z();
    const bool cutover_success = cutover_failed_step.empty();
    const bool rollback_triggered = force_rollback || !cutover_success;

    std::string rollback_started_utc;
    std::string rollback_completed_utc;
    std::vector<StepResult> rollback_results;
    std::string rollback_failed_step;
    int rollback_duration_ms = 0;
    bool rollback_slo_met = true;
    bool rollback_success = true;

    if (rollback_triggered) {
        rollback_started_utc = ops_detail::NowUtcIso8601Z();
        auto rollback_outcome = RunSteps(rollback_steps, rollback, dry_run);
        rollback_results = std::move(rollback_outcome.first);
        rollback_failed_step = std::move(rollback_outcome.second);
        rollback_duration_ms = SumDurationMs(rollback_results);
        rollback_completed_utc = ops_detail::NowUtcIso8601Z();

        rollback_success = rollback_failed_step.empty();
        rollback_slo_met =
            static_cast<double>(rollback_duration_ms) / 1000.0 <= rollback_max_seconds;
        if (rollback_success && !rollback_slo_met) {
            rollback_success = false;
            rollback_failed_step = "rollback_duration_exceeded";
        }
    }

    std::vector<std::string> cutover_lines = {
        "CUTOVER_ENV=" + cutover.at("CUTOVER_ENV_NAME"),
        "CUTOVER_WINDOW_LOCAL=" + cutover.at("CUTOVER_WINDOW_LOCAL"),
        "CUTOVER_CTP_CONFIG_PATH=" + cutover.at("CTP_CONFIG_PATH"),
        "CUTOVER_DRY_RUN=" + std::string(dry_run ? "1" : "0"),
        "CUTOVER_SUCCESS=" + BoolText(cutover_success),
        "CUTOVER_TOTAL_STEPS=" + std::to_string(cutover_results.size()),
        "CUTOVER_FAILED_STEP=" + cutover_failed_step,
        "CUTOVER_MONITOR_MINUTES=" + cutover.at("POST_SWITCH_MONITOR_MINUTES"),
        "CUTOVER_MONITOR_KEYS=" + cutover.at("MONITOR_KEYS"),
        "CUTOVER_TRIGGERED_ROLLBACK=" + BoolText(rollback_triggered),
        "CUTOVER_STARTED_UTC=" + cutover_started_utc,
        "CUTOVER_COMPLETED_UTC=" + cutover_completed_utc,
        "CUTOVER_DURATION_SECONDS=" + FormatSeconds(cutover_duration_ms),
    };
    AppendStepLines(cutover_results, &cutover_lines);

    std::vector<std::string> rollback_lines = {
        "ROLLBACK_ENV=" + rollback.at("ROLLBACK_ENV_NAME"),
        "ROLLBACK_TRIGGER_CONDITION=" + rollback.at("ROLLBACK_TRIGGER_CONDITION"),
        "ROLLBACK_DRY_RUN=" + std::string(dry_run ? "1" : "0"),
        "ROLLBACK_TRIGGERED=" + BoolText(rollback_triggered),
        "ROLLBACK_SUCCESS=" + BoolText(rollback_success),
        "ROLLBACK_TOTAL_STEPS=" + std::to_string(rollback_results.size()),
        "ROLLBACK_FAILED_STEP=" + rollback_failed_step,
        "ROLLBACK_MAX_SECONDS=" + FormatSeconds(static_cast<int>(rollback_max_seconds * 1000.0)),
        "ROLLBACK_DURATION_SECONDS=" + FormatSeconds(rollback_duration_ms),
        "ROLLBACK_SLO_MET=" + BoolText(rollback_slo_met),
        "ROLLBACK_STARTED_UTC=" + rollback_started_utc,
        "ROLLBACK_COMPLETED_UTC=" + rollback_completed_utc,
    };
    AppendStepLines(rollback_results, &rollback_lines);

    std::ostringstream cutover_payload;
    for (const std::string& line : cutover_lines) {
        cutover_payload << line << '\n';
    }
    std::ostringstream rollback_payload;
    for (const std::string& line : rollback_lines) {
        rollback_payload << line << '\n';
    }

    if (!WriteTextFile(cutover_output, cutover_payload.str(), &error) ||
        !WriteTextFile(rollback_output, rollback_payload.str(), &error)) {
        std::cerr << "ctp_cutover_orchestrator_cli: " << error << '\n';
        return 2;
    }

    std::cout << cutover_output << '\n';
    std::cout << rollback_output << '\n';
    return (cutover_success && rollback_success) ? 0 : 2;
}
