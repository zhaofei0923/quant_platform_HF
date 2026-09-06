#pragma once

#include <cstddef>
#include <functional>
#include <optional>
#include <string>

#include "quant_hft/contracts/wal_receipt.h"
#include "quant_hft/core/ctp_order_mapping_store.h"
#include "quant_hft/core/wal_format.h"

namespace quant_hft {

class OrderStateMachine;
class IPortfolioLedger;

struct WalReplayStats {
    std::size_t lines_total{0};
    std::size_t events_loaded{0};
    std::size_t ignored_lines{0};
    std::size_t parse_errors{0};
    std::size_t state_rejected{0};
    std::size_t ledger_applied{0};
    std::size_t submit_mappings_loaded{0};
    bool integrity_ok{true};
    bool incomplete_tail{false};
    std::string error;
    std::string stream_id;
    std::uint64_t next_sequence{0};
};

struct WalReplayRecord {
    std::string kind;
    std::string event_type;
    WalReceipt receipt;
    std::optional<OrderEvent> event;
    std::optional<CtpOrderSubmitMapping> mapping;
};

struct WalValidatedReadResult {
    WalValidationResult validation;
    std::size_t records_visited{0};
    bool completed{false};
    std::string error;
};

class WalReplayLoader {
   public:
    // Capture a bounded immutable snapshot, validate every byte and semantic record, then visit.
    // No visitor runs on a damaged WAL. A false visitor return stops on a consumer failure.
    // Legacy records have nondurable receipts; migration must reconcile them explicitly.
    WalValidatedReadResult VisitValidated(
        const std::string& wal_path, const std::function<bool(const WalReplayRecord&)>& visitor,
        std::size_t max_bytes = 256 * 1024 * 1024) const;

    WalReplayStats Replay(const std::string& wal_path, OrderStateMachine* order_state_machine,
                          IPortfolioLedger* portfolio_ledger,
                          CtpOrderMappingStore* order_mapping_store = nullptr) const;
};

}  // namespace quant_hft
