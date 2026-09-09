#pragma once

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <limits>
#include <string>

#include "quant_hft/core/simple_json.h"
#include "quant_hft/core/wal_format.h"

namespace quant_hft::dashboard {

// Validate one already ParseStrict-parsed WAL record without scanning historical input.
// The caller owns cross-record sequence/stream validation and bounded cursor persistence.
// A true result with error_code="legacy_unverified" is readable legacy data; its
// provenance comes from the identity-bound input directory and must remain incomplete.
// A false result must never enter published order/trade state.
inline bool ValidateDashboardWalRecord(const std::string& raw_line,
                                       const simple_json::Value& record,
                                       const std::string& expected_broker,
                                       const std::string& expected_account,
                                       std::string* error_code) {
    using J = simple_json::Value;
    auto fail = [&](const char* code) {
        if (error_code != nullptr) *error_code = code;
        return false;
    };
    if (error_code != nullptr) error_code->clear();
    if (!record.IsObject() || expected_broker.empty() || expected_account.empty()) {
        return fail("wal_identity_context_missing");
    }
    auto text = [&](const char* key) -> std::string {
        const J* value = record.Find(key);
        return value != nullptr && value->IsString() ? value->string_value : std::string{};
    };
    auto finite = [&](const char* key) {
        const J* value = record.Find(key);
        return value != nullptr && value->IsNumber() && std::isfinite(value->number_value);
    };
    auto integer = [&](const char* key, double low, double high) {
        const J* value = record.Find(key);
        return finite(key) && value->number_value >= low && value->number_value <= high &&
               std::floor(value->number_value) == value->number_value;
    };
    const std::string line = !raw_line.empty() && raw_line.back() == '\r'
                                 ? raw_line.substr(0, raw_line.size() - 1)
                                 : raw_line;
    std::uint64_t version = 0;
    if (record.Find("schema_version") != nullptr &&
        !WalUnsignedField(line, "schema_version", &version)) {
        return fail("wal_invalid_schema");
    }
    if (version > 4) return fail("wal_unsupported_schema");
    const bool modern = version == 4;
    if (modern) {
        const auto checksum_at = line.rfind(",\"checksum\":\"");
        if (checksum_at == std::string::npos || line.size() != checksum_at + 23 ||
            line.compare(line.size() - 2, 2, "\"}") != 0 ||
            line.substr(checksum_at + 13, 8) !=
                WalChecksumHex(WalChecksum(line.substr(0, checksum_at) + "}"))) {
            return fail("wal_checksum_mismatch");
        }
        const auto stream = text("stream_id");
        if (stream.empty() || stream.size() > 128 ||
            !std::all_of(stream.begin(), stream.end(), [](unsigned char c) {
                return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
                       c == '-' || c == '_' || c == '.';
            })) {
            return fail("wal_invalid_stream_identity");
        }
        std::uint64_t sequence = 0;
        std::uint64_t first = 0;
        if (!WalUnsignedField(line, "seq", &sequence) ||
            !WalUnsignedField(line, "stream_first_sequence", &first) || sequence < first ||
            sequence == std::numeric_limits<std::uint64_t>::max()) {
            return fail("wal_invalid_sequence");
        }
    }
    if (text("account_id") != expected_account) return fail("wal_account_mismatch");
    const auto kind = text("kind");
    const auto event = text("event_type");
    const bool mapping = kind == "ctp_order_submit_mapping";
    if (kind != "order" && kind != "trade" && !mapping) return fail("wal_unknown_record_kind");
    // Current submit-mapping records do not carry BrokerID. They still require the
    // bound account and modern checksum, and are not themselves displayed as fills.
    if ((!mapping && modern) || record.Find("broker_id") != nullptr) {
        if (text("broker_id") != expected_broker) return fail("wal_broker_mismatch");
    }
    const std::string expected_type = mapping           ? "ctp_submit_mapping"
                                      : kind == "trade" ? "trade_fill"
                                                        : "order_update";
    if (event != expected_type && (modern || !event.empty()))
        return fail("wal_event_type_mismatch");
    const auto day = text("trading_day");
    if (day.size() != 8 ||
        !std::all_of(day.begin(), day.end(), [](char c) { return c >= '0' && c <= '9'; })) {
        return fail("wal_missing_trading_day");
    }
    if (text("instrument_id").empty()) return fail("wal_missing_instrument");
    if (!integer("side", 0, 1) || !integer("offset", 0, 3)) {
        return fail("wal_invalid_direction");
    }
    if (!mapping) {
        if (text("client_order_id").empty() && text("exchange_order_id").empty() &&
            text("order_ref").empty()) {
            return fail("wal_missing_order_identity");
        }
        if (!integer("status", 0, 5) || !finite("avg_fill_price")) {
            return fail("wal_invalid_order_fields");
        }
        if (modern &&
            (!integer("total_volume", 0, std::numeric_limits<std::int32_t>::max()) ||
             !integer("filled_volume", 0, std::numeric_limits<std::int32_t>::max()) ||
             !integer("last_trade_volume", 0, std::numeric_limits<std::int32_t>::max()))) {
            return fail("wal_invalid_volume");
        }
        if (kind == "trade" && (text("trade_id").empty() || text("exchange_id").empty())) {
            return fail("wal_missing_trade_identity");
        }
    }
    if (!modern && error_code != nullptr) *error_code = "legacy_unverified";
    return true;
}

}  // namespace quant_hft::dashboard
