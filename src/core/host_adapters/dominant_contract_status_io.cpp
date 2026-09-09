#include "quant_hft/services/dominant_contract_coordinator.h"

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <cmath>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <sstream>

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
std::string JsonEscape(const std::string& value) {
    std::ostringstream out;
    for (const unsigned char ch : value) {
        switch (ch) {
            case '\\':
                out << "\\\\";
                break;
            case '"':
                out << "\\\"";
                break;
            case '\n':
                out << "\\n";
                break;
            case '\r':
                out << "\\r";
                break;
            case '\t':
                out << "\\t";
                break;
            default:
                if (ch < 0x20) {
                    out << "\\u" << std::hex << std::setw(4) << std::setfill('0')
                        << static_cast<int>(ch) << std::dec;
                } else {
                    out << static_cast<char>(ch);
                }
        }
    }
    return out.str();
}
bool FsyncPath(const std::filesystem::path& path, bool directory, std::string* error) {
    const int flags = directory ? (O_RDONLY | O_DIRECTORY) : O_RDONLY;
    const int fd = ::open(path.c_str(), flags);
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
bool DominantContractCoordinator::PersistStatusAtomically(const std::string& product_id,
                                                          const std::string& output_path,
                                                          std::string* error) const {
    if (output_path.empty()) {
        SetError(error, "dominant contract status path is empty");
        return false;
    }
    const DominantContractStatus status = GetStatus(product_id);
    if (status.product_id.empty()) {
        SetError(error, "dominant contract product is not registered");
        return false;
    }
    std::ostringstream payload;
    payload << std::setprecision(17);
    payload << "{\n"
            << "  \"schema_version\": 2,\n"
            << "  \"trading_day\": \"" << JsonEscape(status.trading_day) << "\",\n"
            << "  \"product_id\": \"" << JsonEscape(status.product_id) << "\",\n"
            << "  \"current_instrument_id\": \"" << JsonEscape(status.current_instrument_id)
            << "\",\n"
            << "  \"candidate_instrument_id\": \"" << JsonEscape(status.candidate_instrument_id)
            << "\",\n"
            << "  \"phase\": \"" << DominantContractPhaseName(status.phase) << "\",\n"
            << "  \"generation\": " << status.generation << ",\n"
            << "  \"selection_metric\": \"" << JsonEscape(status.selection_metric) << "\",\n"
            << "  \"current_metric\": " << status.current_metric << ",\n"
            << "  \"candidate_metric\": " << status.candidate_metric << ",\n"
            << "  \"lead_ratio\": " << status.lead_ratio << ",\n"
            << "  \"lead_windows\": " << status.lead_windows << ",\n"
            << "  \"eligible_count\": " << status.eligible_count << ",\n"
            << "  \"baseline_count\": " << status.baseline_count << ",\n"
            << "  \"broker_position\": " << status.broker_position << ",\n"
            << "  \"broker_frozen\": " << status.broker_frozen << ",\n"
            << "  \"active_open_orders\": " << status.active_open_orders << ",\n"
            << "  \"active_close_orders\": " << status.active_close_orders << ",\n"
            << "  \"warmup_observed_bars\": " << status.warmup_observed_bars << ",\n"
            << "  \"warmup_required_bars\": " << status.warmup_required_bars << ",\n"
            << "  \"generation_rejections\": " << status.generation_rejections << ",\n"
            << "  \"selected_at_ns\": " << status.selected_at_ns << ",\n"
            << "  \"phase_started_ts_ns\": " << status.phase_started_ts_ns << ",\n"
            << "  \"updated_at_ns\": " << status.updated_at_ns << ",\n"
            << "  \"last_error\": \"" << JsonEscape(status.last_error) << "\"\n"
            << "}\n";

    const std::filesystem::path output(output_path);
    std::error_code ec;
    if (!output.parent_path().empty()) {
        std::filesystem::create_directories(output.parent_path(), ec);
        if (ec) {
            SetError(error, "failed to create dominant status directory: " + ec.message());
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
            SetError(error, "failed to open dominant status temp file");
            return false;
        }
        stream << payload.str();
        stream.flush();
        if (!stream.good()) {
            stream.close();
            std::filesystem::remove(temporary, ec);
            SetError(error, "failed to flush dominant status temp file");
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
        SetError(error, "failed to publish dominant status: " + ec.message());
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
