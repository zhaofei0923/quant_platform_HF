#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "quant_hft/contracts/types.h"

namespace quant_hft {

struct InstrumentMetaCacheDocument {
    std::int32_t schema_version{0};
    std::string product_id;
    std::string broker_trading_day;
    EpochNanos generated_ts_ns{0};
    bool legacy{false};
    std::vector<InstrumentMetaSnapshot> instruments;
};

struct InstrumentUniverseManifest {
    std::int32_t schema_version{0};
    std::string broker_trading_day;
    EpochNanos generated_ts_ns{0};
    std::uint64_t generation{0};
    bool complete{false};
    std::string cache_dir;
    std::vector<std::string> product_ids;
};

bool LoadInstrumentMetaCacheDocument(const std::string& path, InstrumentMetaCacheDocument* document,
                                     std::string* error = nullptr);
bool WriteInstrumentMetaCacheV2Atomically(const std::string& path, const std::string& product_id,
                                          const std::string& broker_trading_day,
                                          const std::vector<InstrumentMetaSnapshot>& instruments,
                                          EpochNanos generated_ts_ns, std::string* error = nullptr);

bool IsInstrumentMetaCacheCurrent(const InstrumentMetaCacheDocument& document,
                                  const std::string& broker_trading_day, EpochNanos now_ns,
                                  std::int64_t max_age_ms, std::string* reason = nullptr);

bool LoadInstrumentUniverseManifest(const std::string& path, InstrumentUniverseManifest* manifest,
                                    std::string* error = nullptr);
bool WriteInstrumentUniverseManifestAtomically(const std::string& path,
                                               const InstrumentUniverseManifest& manifest,
                                               std::string* error = nullptr);
bool IsInstrumentUniverseManifestCurrent(const InstrumentUniverseManifest& manifest,
                                         const std::string& broker_trading_day,
                                         const std::vector<std::string>& required_product_ids,
                                         EpochNanos now_ns, std::int64_t max_age_ms,
                                         std::string* reason = nullptr);

std::vector<InstrumentMetaSnapshot> CollectProductInstrumentMetadata(
    const std::vector<InstrumentMetaSnapshot>& instruments, const std::string& product_id);
std::vector<InstrumentMetaSnapshot> FilterEligibleFuturesContracts(
    const std::vector<InstrumentMetaSnapshot>& instruments, const std::string& product_id,
    const std::string& broker_trading_day);
bool IsEligibleFuturesContract(const InstrumentMetaSnapshot& instrument,
                               const std::string& product_id,
                               const std::string& broker_trading_day);

std::string ExtractFuturesProductId(const std::string& instrument_id);
bool IsPlainFuturesContract(const std::string& instrument_id, const std::string& product_id);

}  // namespace quant_hft
