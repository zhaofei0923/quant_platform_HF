#pragma once

#include <chrono>
#include <exception>
#include <filesystem>
#include <fstream>
#include <sstream>
#include <string>
#include <unordered_map>

namespace quant_hft::apps {

using ArgMap = std::unordered_map<std::string, std::string>;

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

}  // namespace quant_hft::apps
