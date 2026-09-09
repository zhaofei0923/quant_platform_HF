#include "quant_hft/services/market_bar_pipeline.h"

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <sstream>
#include <utility>

#if !defined(_WIN32)
#include <fcntl.h>
#include <unistd.h>
#endif

namespace quant_hft {
namespace {
void SetError(std::string* error, const std::string& value) {
    if (error != nullptr) {
        *error = value;
    }
}
std::string ErrnoMessage(const std::string& action) { return action + ": " + std::strerror(errno); }
bool FsyncPath(const std::filesystem::path& path, bool directory, std::string* error) {
    const int flags = directory ? (O_RDONLY | O_DIRECTORY) : O_RDONLY;
    const int fd = ::open(path.c_str(), flags);
    if (fd < 0) {
        SetError(error, ErrnoMessage("open for fsync failed: " + path.string()));
        return false;
    }
    const int rc = ::fsync(fd);
    const int saved_errno = errno;
    ::close(fd);
    if (rc != 0) {
        errno = saved_errno;
        SetError(error, ErrnoMessage("fsync failed: " + path.string()));
        return false;
    }
    return true;
}
}
bool MarketBarPipeline::SaveCheckpointAtomically(const std::string& path,
                                                 std::string* error) const {
    if (path.empty()) {
        SetError(error, "market bar checkpoint path is empty");
        return false;
    }
    PersistenceState state;
    if (!SaveState(&state, error)) {
        return false;
    }
    std::vector<std::pair<std::string, std::string>> entries(state.begin(), state.end());
    std::sort(entries.begin(), entries.end());
    std::ostringstream payload;
    for (const auto& [key, value] : entries) {
        payload << EscapeCheckpointValue(key) << '=' << EscapeCheckpointValue(value) << '\n';
    }

    const std::filesystem::path output(path);
    std::error_code ec;
    if (!output.parent_path().empty()) {
        std::filesystem::create_directories(output.parent_path(), ec);
        if (ec) {
            SetError(error, "failed to create checkpoint directory: " + ec.message());
            return false;
        }
    }
    const auto suffix = std::chrono::duration_cast<std::chrono::nanoseconds>(
                            std::chrono::system_clock::now().time_since_epoch())
                            .count();
    const std::filesystem::path temporary = output.string() + ".tmp." + std::to_string(suffix);
    {
        std::ofstream stream(temporary, std::ios::out | std::ios::trunc | std::ios::binary);
        if (!stream.is_open()) {
            SetError(error, "failed to open market bar checkpoint temp file");
            return false;
        }
        stream << payload.str();
        stream.flush();
        if (!stream.good()) {
            stream.close();
            std::filesystem::remove(temporary, ec);
            SetError(error, "failed to flush market bar checkpoint temp file");
            return false;
        }
    }
#if !defined(_WIN32)
    if (!FsyncPath(temporary, false, error)) {
        std::filesystem::remove(temporary, ec);
        return false;
    }
#endif
    std::filesystem::rename(temporary, output, ec);
    if (ec) {
        std::filesystem::remove(temporary, ec);
        SetError(error, "failed to atomically publish market bar checkpoint: " + ec.message());
        return false;
    }
#if !defined(_WIN32)
    const std::filesystem::path parent =
        output.parent_path().empty() ? std::filesystem::path(".") : output.parent_path();
    if (!FsyncPath(parent, true, error)) {
        return false;
    }
#endif
    return true;
}
bool MarketBarPipeline::LoadCheckpointFile(const std::string& path, std::string* error) {
    std::ifstream stream(path, std::ios::in | std::ios::binary);
    if (!stream.is_open()) {
        SetError(error, "failed to open market bar checkpoint: " + path);
        return false;
    }
    PersistenceState state;
    std::string line;
    while (std::getline(stream, line)) {
        if (line.empty()) {
            continue;
        }
        const std::size_t separator = line.find('=');
        if (separator == std::string::npos) {
            SetError(error, "malformed market bar checkpoint line");
            return false;
        }
        std::string key;
        std::string value;
        if (!UnescapeCheckpointValue(line.substr(0, separator), &key) ||
            !UnescapeCheckpointValue(line.substr(separator + 1), &value) || key.empty()) {
            SetError(error, "invalid market bar checkpoint escape sequence");
            return false;
        }
        if (!state.emplace(std::move(key), std::move(value)).second) {
            SetError(error, "duplicate market bar checkpoint key");
            return false;
        }
    }
    if (!stream.eof()) {
        SetError(error, "failed while reading market bar checkpoint");
        return false;
    }
    return LoadState(state, error);
}
}
