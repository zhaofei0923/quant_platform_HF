#pragma once

#include <cmath>
#include <stdexcept>
#include <string>

#include "quant_hft/strategy/atomic_strategy.h"

namespace quant_hft::atomic_internal {

inline std::string GetString(const AtomicParams& params,
                             const std::string& key,
                             const std::string& default_value) {
    const auto it = params.find(key);
    return it == params.end() ? default_value : it->second;
}

inline int ParseInt(const std::string& value, const std::string& key) {
    std::size_t consumed = 0;
    const int parsed = std::stoi(value, &consumed);
    if (consumed != value.size()) {
        throw std::invalid_argument("invalid integer for '" + key + "': " + value);
    }
    return parsed;
}

inline int GetInt(const AtomicParams& params, const std::string& key, int default_value) {
    const auto it = params.find(key);
    if (it == params.end()) {
        return default_value;
    }
    return ParseInt(it->second, key);
}

inline double ParseDouble(const std::string& value, const std::string& key) {
    std::size_t consumed = 0;
    const double parsed = std::stod(value, &consumed);
    if (consumed != value.size()) {
        throw std::invalid_argument("invalid double for '" + key + "': " + value);
    }
    if (!std::isfinite(parsed)) {
        throw std::invalid_argument("non-finite double for '" + key + "': " + value);
    }
    return parsed;
}

inline double GetDouble(const AtomicParams& params, const std::string& key, double default_value) {
    const auto it = params.find(key);
    if (it == params.end()) {
        return default_value;
    }
    return ParseDouble(it->second, key);
}

}  // namespace quant_hft::atomic_internal
