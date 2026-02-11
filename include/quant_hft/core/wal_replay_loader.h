#pragma once

#include <cstddef>
#include <string>

#include "quant_hft/interfaces/portfolio_ledger.h"
#include "quant_hft/services/order_state_machine.h"

namespace quant_hft {

struct WalReplayStats {
    std::size_t lines_total{0};
    std::size_t events_loaded{0};
    std::size_t parse_errors{0};
    std::size_t state_rejected{0};
    std::size_t ledger_applied{0};
};

class WalReplayLoader {
public:
    WalReplayStats Replay(const std::string& wal_path,
                          OrderStateMachine* order_state_machine,
                          IPortfolioLedger* portfolio_ledger) const;
};

}  // namespace quant_hft
