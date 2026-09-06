#include "quant_hft/core/wal_replay_loader.h"
#include "quant_hft/interfaces/portfolio_ledger.h"
#include "quant_hft/services/order_state_machine.h"

namespace quant_hft {

WalReplayStats WalReplayLoader::Replay(const std::string& wal_path,
                                       OrderStateMachine* order_state_machine,
                                       IPortfolioLedger* portfolio_ledger,
                                       CtpOrderMappingStore* order_mapping_store) const {
    WalReplayStats stats;
    const auto read = VisitValidated(wal_path, [&](const WalReplayRecord& record) {
        ++stats.lines_total;
        if (record.mapping) {
            if (order_mapping_store) order_mapping_store->Upsert(*record.mapping);
            ++stats.submit_mappings_loaded;
            ++stats.ignored_lines;
            return true;
        }
        if (!record.event || record.event_type == "trade_fill") {
            ++stats.ignored_lines;
            return true;
        }
        ++stats.events_loaded;
        bool apply_to_ledger = true;
        if (order_state_machine && !order_state_machine->RecoverFromOrderEvent(*record.event)) {
            ++stats.state_rejected;
            apply_to_ledger = false;
        }
        if (apply_to_ledger && portfolio_ledger) {
            portfolio_ledger->OnOrderEvent(*record.event);
            ++stats.ledger_applied;
        }
        return true;
    });
    stats.integrity_ok = read.completed;
    stats.incomplete_tail = read.validation.incomplete_tail;
    stats.error = read.error;
    stats.stream_id = read.validation.stream_id;
    stats.next_sequence = read.validation.next_sequence;
    if (!read.completed) stats.parse_errors = read.validation.missing ? 0 : 1;
    return stats;
}

}  // namespace quant_hft
