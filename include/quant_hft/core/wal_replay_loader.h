#pragma once

#include <cstddef>
#include <string>

#include "quant_hft/core/ctp_order_mapping_store.h"
#include "quant_hft/interfaces/portfolio_ledger.h"
#include "quant_hft/services/order_state_machine.h"

namespace quant_hft {

struct WalReplayStats {
    std::size_t lines_total{0};
    std::size_t events_loaded{0};
    std::size_t ignored_lines{0};
    std::size_t parse_errors{0};
    std::size_t state_rejected{0};
    std::size_t ledger_applied{0};
    std::size_t submit_mappings_loaded{0};
};

class WalReplayLoader {
   public:
    WalReplayStats Replay(const std::string& wal_path, OrderStateMachine* order_state_machine,
                          IPortfolioLedger* portfolio_ledger,
                          CtpOrderMappingStore* order_mapping_store = nullptr) const;
};

}  // namespace quant_hft
