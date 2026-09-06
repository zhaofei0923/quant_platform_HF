#pragma once

#include <cctype>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <limits>
#include <sstream>
#include <string>

namespace quant_hft {

inline std::uint32_t WalChecksum(const std::string& bytes) {
    std::uint32_t crc = 0xffffffffU;
    for (const unsigned char byte : bytes) {
        crc ^= byte;
        for (int bit = 0; bit != 8; ++bit) {
            crc = (crc >> 1U) ^ (0xedb88320U & (0U - (crc & 1U)));
        }
    }
    return ~crc;
}

inline std::string WalChecksumHex(std::uint32_t value) {
    std::ostringstream out;
    out << std::hex << std::setfill('0') << std::setw(8) << value;
    return out.str();
}

struct WalValidationResult {
    bool valid{false};
    bool missing{false};
    bool incomplete_tail{false};
    bool has_sequence{false};
    std::size_t records{0};
    std::size_t legacy_records{0};
    std::uint64_t next_sequence{0};
    std::uint64_t first_sequence{0};
    std::string stream_id;
    std::string error;
};

inline bool WalUnsignedField(const std::string& line, const std::string& key,
                             std::uint64_t* value) {
    auto pos = line.find("\"" + key + "\":");
    if (pos == std::string::npos) return false;
    pos += key.size() + 3;
    while (pos < line.size() && std::isspace(static_cast<unsigned char>(line[pos]))) ++pos;
    const auto first = pos;
    std::uint64_t parsed = 0;
    while (pos < line.size() && line[pos] >= '0' && line[pos] <= '9') {
        const auto digit = static_cast<unsigned>(line[pos++] - '0');
        if (parsed > (std::numeric_limits<std::uint64_t>::max() - digit) / 10) return false;
        parsed = parsed * 10 + digit;
    }
    if (pos == first || (pos < line.size() && line[pos] != ',' && line[pos] != '}' &&
                         !std::isspace(static_cast<unsigned char>(line[pos]))))
        return false;
    *value = parsed;
    return true;
}

// Validate the entire file before applying any recovered state. No automatic truncation:
// an incomplete tail requires an explicit, audited repair while trading remains blocked.
inline WalValidationResult ValidateWalStream(std::istream& input) {
    WalValidationResult result;
    std::string line;
    auto fail = [&](const std::string& reason) {
        result.error = "WAL record " + std::to_string(result.records + 1) + ": " + reason;
    };
    // Read with an explicit per-record bound, including malformed unterminated lines.
    constexpr std::size_t kMaxRecordBytes = 4 * 1024 * 1024;
    char ch = 0;
    while (input.get(ch)) {
        if (ch != '\n') {
            if (line.size() == kMaxRecordBytes) {
                fail("record exceeds size limit");
                return result;
            }
            line.push_back(ch);
            continue;
        }
        if (!line.empty() && line.back() == '\r') line.pop_back();
        if (line.empty()) continue;
        if (line.front() != '{' || line.back() != '}') {
            fail("invalid record framing");
            return result;
        }
        std::uint64_t version = 0;
        const bool has_version = WalUnsignedField(line, "schema_version", &version);
        if (!has_version && line.find("\"schema_version\":") != std::string::npos) {
            fail("invalid schema version");
            return result;
        }
        if (has_version && version > 4) {
            fail("unsupported schema version");
            return result;
        }
        const bool modern = has_version && version == 4;
        if (modern) {
            const auto checksum_pos = line.rfind(",\"checksum\":\"");
            if (checksum_pos == std::string::npos || line.size() != checksum_pos + 23 ||
                line.compare(line.size() - 2, 2, "\"}") != 0 ||
                line.substr(checksum_pos + 13, 8) !=
                    WalChecksumHex(WalChecksum(line.substr(0, checksum_pos) + "}"))) {
                fail("checksum mismatch or missing checksum");
                return result;
            }
            const std::string key = "\"stream_id\":\"";
            const auto stream_begin = line.find(key);
            const auto stream_end = stream_begin == std::string::npos
                                        ? std::string::npos
                                        : line.find('"', stream_begin + key.size());
            if (stream_end == std::string::npos || stream_end == stream_begin + key.size()) {
                fail("missing stream identity");
                return result;
            }
            const auto stream =
                line.substr(stream_begin + key.size(), stream_end - stream_begin - key.size());
            if (!result.stream_id.empty() && result.stream_id != stream) {
                fail("stream identity changed within WAL");
                return result;
            }
            std::uint64_t first_sequence = 0;
            if (!WalUnsignedField(line, "stream_first_sequence", &first_sequence) ||
                (!result.stream_id.empty() && result.first_sequence != first_sequence)) {
                fail("invalid stream start sequence");
                return result;
            }
            if (result.stream_id.empty()) result.first_sequence = first_sequence;
            result.stream_id = stream;
        } else {
            if (!result.stream_id.empty()) {
                fail("legacy record after versioned stream");
                return result;
            }
            ++result.legacy_records;
        }
        std::uint64_t seq = 0;
        if (WalUnsignedField(line, "seq", &seq)) {
            if (modern && result.records == result.legacy_records && seq != result.first_sequence) {
                fail("stream start record missing");
                return result;
            }
            if (modern && seq < result.first_sequence) {
                fail("sequence precedes stream start");
                return result;
            }
            if (result.has_sequence && seq != result.next_sequence) {
                fail("non-contiguous sequence");
                return result;
            }
            if (seq == std::numeric_limits<std::uint64_t>::max()) {
                fail("sequence exhausted");
                return result;
            }
            result.has_sequence = true;
            result.next_sequence = seq + 1;
        } else if (modern) {
            fail("missing sequence");
            return result;
        }
        ++result.records;
        line.clear();
    }
    if (input.bad()) {
        fail("read failed");
        return result;
    }
    if (!line.empty()) {
        result.incomplete_tail = true;
        fail("incomplete trailing record");
        return result;
    }
    result.valid = true;
    return result;
}

inline WalValidationResult ValidateWalFile(const std::string& path) {
    std::ifstream input(path, std::ios::binary);
    if (!input.is_open()) {
        WalValidationResult result;
        std::error_code ec;
        result.missing = !std::filesystem::exists(path, ec) && !ec;
        result.error = result.missing ? "WAL file missing" : "cannot read WAL file";
        return result;
    }
    return ValidateWalStream(input);
}

}  // namespace quant_hft
