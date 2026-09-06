#pragma once

#include <cstdint>
#include <string>

namespace quant_hft {

// A receipt is issued only after the complete record has been synced to storage.
// The stream identity survives reopen/relocation and changes for a new WAL.
struct WalReceipt {
    std::uint32_t schema_version{4};
    std::string stream_id;
    std::uint64_t sequence{0};
    std::uint64_t first_sequence{0};
    std::uint32_t checksum{0};
    bool durable{false};
    std::string error;

    explicit operator bool() const noexcept { return durable; }
};

}  // namespace quant_hft
