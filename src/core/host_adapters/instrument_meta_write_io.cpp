#include "quant_hft/core/instrument_meta_cache.h"

#include <algorithm>
#include <cctype>
#include <cerrno>
#include <chrono>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <sstream>
#include <unordered_set>

#if !defined(_WIN32)
#include <fcntl.h>
#include <unistd.h>
#endif

namespace quant_hft {
namespace {
std::string Lowercase(std::string value) {
    std::transform(value.begin(), value.end(), value.begin(),
                   [](unsigned char ch) { return static_cast<char>(std::tolower(ch)); });
    return value;
}
bool IsValidDate(const std::string& value) {
    return value.size() == 8 && std::all_of(value.begin(), value.end(),
                                            [](unsigned char ch) { return std::isdigit(ch) != 0; });
}
void SetError(std::string* error, const std::string& value) {
    if (error != nullptr) {
        *error = value;
    }
}
std::string JsonEscape(const std::string& value) {
    std::string escaped;
    escaped.reserve(value.size());
    for (const char ch : value) {
        switch (ch) {
            case '\\':
                escaped += "\\\\";
                break;
            case '"':
                escaped += "\\\"";
                break;
            case '\n':
                escaped += "\\n";
                break;
            case '\r':
                escaped += "\\r";
                break;
            case '\t':
                escaped += "\\t";
                break;
            default:
                escaped.push_back(ch);
                break;
        }
    }
    return escaped;
}
bool FsyncPath(const std::filesystem::path& path, bool directory, std::string* error) {
    const int fd = ::open(path.c_str(), directory ? (O_RDONLY | O_DIRECTORY) : O_RDONLY);
    if (fd < 0) {
        SetError(error, "open for fsync failed: " + std::string(std::strerror(errno)));
        return false;
    }
    const int rc = ::fsync(fd);
    const int saved_errno = errno;
    ::close(fd);
    if (rc != 0) {
        SetError(error, "fsync failed: " + std::string(std::strerror(saved_errno)));
        return false;
    }
    return true;
}
}
bool WriteInstrumentMetaCacheV2Atomically(const std::string& path, const std::string& product_id,
                                          const std::string& broker_trading_day,
                                          const std::vector<InstrumentMetaSnapshot>& instruments,
                                          EpochNanos generated_ts_ns, std::string* error) {
    if (path.empty() || product_id.empty() || !IsValidDate(broker_trading_day) ||
        instruments.empty()) {
        SetError(error, "invalid instrument cache v2 arguments");
        return false;
    }
    std::vector<InstrumentMetaSnapshot> sorted = instruments;
    std::sort(sorted.begin(), sorted.end(), [](const auto& lhs, const auto& rhs) {
        return lhs.instrument_id < rhs.instrument_id;
    });
    std::ostringstream payload;
    payload << std::setprecision(17);
    payload << "{\n"
            << "  \"schema_version\": 2,\n"
            << "  \"product_id\": \"" << JsonEscape(Lowercase(product_id)) << "\",\n"
            << "  \"broker_trading_day\": \"" << JsonEscape(broker_trading_day) << "\",\n"
            << "  \"generated_ts_ns\": " << generated_ts_ns << ",\n"
            << "  \"instruments\": [\n";
    for (std::size_t index = 0; index < sorted.size(); ++index) {
        const auto& row = sorted[index];
        payload << "    {\n"
                << "      \"instrument_id\": \"" << JsonEscape(row.instrument_id) << "\",\n"
                << "      \"exchange_id\": \"" << JsonEscape(row.exchange_id) << "\",\n"
                << "      \"product_id\": \"" << JsonEscape(Lowercase(row.product_id)) << "\",\n"
                << "      \"volume_multiple\": " << row.volume_multiple << ",\n"
                << "      \"price_tick\": " << row.price_tick << ",\n"
                << "      \"max_margin_side_algorithm\": "
                << (row.max_margin_side_algorithm ? "true" : "false") << ",\n"
                << "      \"ts_ns\": " << row.ts_ns << ",\n"
                << "      \"source\": \"" << JsonEscape(row.source) << "\",\n"
                << "      \"open_date\": \"" << JsonEscape(row.open_date) << "\",\n"
                << "      \"expire_date\": \"" << JsonEscape(row.expire_date) << "\",\n"
                << "      \"is_trading\": " << (row.is_trading ? "true" : "false") << ",\n"
                << "      \"product_class\": \"" << JsonEscape(row.product_class) << "\"\n"
                << "    }" << (index + 1U == sorted.size() ? "\n" : ",\n");
    }
    payload << "  ]\n}\n";

    const std::filesystem::path output(path);
    std::error_code ec;
    if (!output.parent_path().empty()) {
        std::filesystem::create_directories(output.parent_path(), ec);
        if (ec) {
            SetError(error, "failed to create instrument cache directory: " + ec.message());
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
            SetError(error, "failed to open instrument cache temp file");
            return false;
        }
        stream << payload.str();
        stream.flush();
        if (!stream.good()) {
            stream.close();
            std::filesystem::remove(temporary, ec);
            SetError(error, "failed to flush instrument cache temp file");
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
        SetError(error, "failed to publish instrument cache: " + ec.message());
        return false;
    }
#if !defined(_WIN32)
    const auto parent =
        output.parent_path().empty() ? std::filesystem::path(".") : output.parent_path();
    if (!FsyncPath(parent, true, error)) {
        return false;
    }
#endif
    return true;
}
}
