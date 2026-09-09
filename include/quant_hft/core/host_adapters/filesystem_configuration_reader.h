#pragma once
#include "quant_hft/contracts/configuration_reader.h"
#include <filesystem>
#include <fstream>
#include <iterator>
namespace quant_hft {
inline void BindFilesystemConfigurationReader() {
    SetConfigurationReader([](const std::string& path, const ConfigurationReadOptions& options,
                              std::string* content, std::string* error) {
        const auto fail = [&](const std::string& reason) { if (error) *error = reason; return false; };
        std::error_code ec;
        if (options.regular_non_symlink) {
            const auto status = std::filesystem::symlink_status(path, ec);
            if (ec || std::filesystem::is_symlink(status) || !std::filesystem::is_regular_file(status))
                return fail("path must be a regular non-symlink file");
        }
        const auto size = std::filesystem::file_size(path, ec);
        if (ec || size > options.maximum_bytes) return fail("configuration unavailable or exceeds byte limit");
        std::ifstream input(path, std::ios::binary);
        if (!input) return fail("configuration unavailable");
        content->clear();
        char buffer[4096];
        while (input.read(buffer, sizeof(buffer)) || input.gcount()) {
            content->append(buffer, static_cast<std::size_t>(input.gcount()));
            if (content->size() > options.maximum_bytes) return fail("configuration exceeds byte limit");
        }
        return !input.bad() || fail("configuration read failed");
    });
}
}  // namespace quant_hft
