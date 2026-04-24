#pragma once

#include <chrono>
#include <exception>
#include <filesystem>
#include <fstream>
#include <initializer_list>
#include <sstream>
#include <string>
#include <unordered_map>
#include <vector>

namespace quant_hft::apps {

using ArgMap = std::unordered_map<std::string, std::string>;

struct ResolvedConfigPath {
    std::string path;
    bool used_default{false};
};

struct BacktestOutputPaths {
    std::string output_json;
    std::string output_md;
    std::string export_csv_dir;
};

inline ArgMap ParseArgs(int argc, char** argv) {
    ArgMap args;
    for (int i = 1; i < argc; ++i) {
        std::string token = argv[i];
        if (token.rfind("--", 0) != 0) {
            continue;
        }
        token = token.substr(2);
        const auto eq_pos = token.find('=');
        if (eq_pos != std::string::npos) {
            args[token.substr(0, eq_pos)] = token.substr(eq_pos + 1);
            continue;
        }
        if (i + 1 < argc && std::string(argv[i + 1]).rfind("--", 0) != 0) {
            args[token] = argv[++i];
            continue;
        }
        args[token] = "true";
    }
    return args;
}

inline std::string GetArg(const ArgMap& args,
                          const std::string& key,
                          const std::string& fallback = "") {
    const auto it = args.find(key);
    if (it == args.end()) {
        return fallback;
    }
    return it->second;
}

inline bool HasArg(const ArgMap& args, const std::string& key) {
    return args.find(key) != args.end();
}

inline std::string GetArgAny(const ArgMap& args,
                             std::initializer_list<const char*> keys,
                             const std::string& fallback = "") {
    for (const char* key : keys) {
        const auto it = args.find(key);
        if (it != args.end()) {
            return it->second;
        }
    }
    return fallback;
}

inline BacktestOutputPaths ResolveBacktestOutputPaths(const ArgMap& args) {
    BacktestOutputPaths paths;
    paths.output_json = GetArgAny(args, {"output_json", "result_json", "report_json"});
    paths.output_md = GetArgAny(args, {"output_md", "report_md"});
    paths.export_csv_dir = GetArgAny(args, {"export_csv_dir", "export-csv-dir"});

    const std::string output_root = GetArgAny(args, {"output", "output_dir", "output-dir"});
    if (!output_root.empty()) {
        const std::filesystem::path base(output_root);
        if (paths.output_json.empty()) {
            paths.output_json = (base / "result.json").string();
        }
        if (paths.output_md.empty()) {
            paths.output_md = (base / "report.md").string();
        }
        if (paths.export_csv_dir.empty()) {
            paths.export_csv_dir = (base / "csv").string();
        }
    }

    return paths;
}

inline bool WriteTextFile(const std::string& path, const std::string& content, std::string* error) {
    if (path.empty()) {
        return true;
    }
    try {
        const std::filesystem::path file_path(path);
        if (!file_path.parent_path().empty()) {
            std::filesystem::create_directories(file_path.parent_path());
        }
        std::ofstream out(path, std::ios::out | std::ios::trunc);
        if (!out.is_open()) {
            if (error != nullptr) {
                *error = "unable to open output file: " + path;
            }
            return false;
        }
        out << content;
        return true;
    } catch (const std::exception& ex) {
        if (error != nullptr) {
            *error = ex.what();
        }
        return false;
    }
}

inline std::string JsonEscape(const std::string& text) {
    std::ostringstream oss;
    for (const char ch : text) {
        switch (ch) {
            case '\"':
                oss << "\\\"";
                break;
            case '\\':
                oss << "\\\\";
                break;
            case '\n':
                oss << "\\n";
                break;
            case '\r':
                oss << "\\r";
                break;
            case '\t':
                oss << "\\t";
                break;
            default:
                oss << ch;
                break;
        }
    }
    return oss.str();
}

inline std::string StableHexDigest(const std::string& text) {
    const auto hash_value = std::hash<std::string>{}(text);
    std::ostringstream oss;
    oss << std::hex << hash_value;
    return oss.str();
}

inline std::int64_t UnixEpochMillisNow() {
    const auto now = std::chrono::system_clock::now();
    return std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count();
}

inline std::string DefaultParameterOptimConfigPath() {
    return (std::filesystem::path("configs") / "ops" / "parameter_optim.yaml").string();
}

inline std::string DefaultRollingBacktestConfigPath() {
    return (std::filesystem::path("configs") / "ops" / "rolling_backtest.yaml").string();
}

inline ResolvedConfigPath ResolveConfigPathWithDefault(const ArgMap& args,
                                                       const std::string& key,
                                                       const std::string& default_path) {
    const auto it = args.find(key);
    if (it != args.end() && !it->second.empty()) {
        return {it->second, false};
    }
    return {default_path, true};
}

inline std::string DetectDefaultBacktestCliPath(
    const std::string& argv0,
    const std::filesystem::path& repo_root = std::filesystem::current_path()) {
    std::error_code ec;

    if (!argv0.empty()) {
        const std::filesystem::path app_path(argv0);
        if (!app_path.parent_path().empty()) {
            const std::filesystem::path sibling = app_path.parent_path() / "backtest_cli";
            if (std::filesystem::exists(sibling, ec)) {
                return sibling.string();
            }
        }
    }

    const std::vector<std::filesystem::path> candidates = {
        repo_root / "build-gcc" / "backtest_cli",
        repo_root / "build" / "backtest_cli",
        std::filesystem::path("build-gcc") / "backtest_cli",
        std::filesystem::path("build") / "backtest_cli",
    };
    for (const auto& candidate : candidates) {
        if (std::filesystem::exists(candidate, ec)) {
            return candidate.string();
        }
    }

    return "backtest_cli";
}

}  // namespace quant_hft::apps
