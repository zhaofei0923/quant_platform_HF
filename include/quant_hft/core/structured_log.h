#pragma once

#include <chrono>
#include <cctype>
#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include "quant_hft/core/ctp_config.h"

namespace quant_hft {

using LogFields = std::vector<std::pair<std::string, std::string>>;

inline std::string NormalizeLogLevel(std::string value) {
    for (char& ch : value) {
        ch = static_cast<char>(std::tolower(static_cast<unsigned char>(ch)));
    }
    if (value == "warning") {
        return "warn";
    }
    return value;
}

inline int LogLevelRank(const std::string& level) {
    const auto normalized = NormalizeLogLevel(level);
    if (normalized == "debug") {
        return 10;
    }
    if (normalized == "info") {
        return 20;
    }
    if (normalized == "warn") {
        return 30;
    }
    if (normalized == "error") {
        return 40;
    }
    return 20;
}

inline std::string EscapeLogValue(const std::string& value) {
    std::string escaped;
    escaped.reserve(value.size());
    for (const char ch : value) {
        if (ch == '"' || ch == '\\') {
            escaped.push_back('\\');
        }
        escaped.push_back(ch);
    }
    return escaped;
}

inline std::int64_t LogNowNs() {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
               std::chrono::system_clock::now().time_since_epoch())
        .count();
}

inline void EmitStructuredLog(const CtpRuntimeConfig* runtime,
                              const std::string& app,
                              const std::string& level,
                              const std::string& event,
                              const LogFields& fields = {}) {
    const std::string normalized_level = NormalizeLogLevel(level);
    const std::string configured_level =
        runtime == nullptr ? "info" : NormalizeLogLevel(runtime->log_level);
    if (LogLevelRank(normalized_level) < LogLevelRank(configured_level)) {
        return;
    }

    std::ostream* out = &std::cerr;
    if (runtime != nullptr && NormalizeLogLevel(runtime->log_sink) == "stdout") {
        out = &std::cout;
    }

    (*out) << "ts_ns=" << LogNowNs() << " level=" << normalized_level << " app=" << app
           << " event=" << event;
    for (const auto& [key, value] : fields) {
        (*out) << " " << key << "=\"" << EscapeLogValue(value) << "\"";
    }
    (*out) << '\n';
}

}  // namespace quant_hft
