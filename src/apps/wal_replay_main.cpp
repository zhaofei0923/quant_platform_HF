#include <iostream>
#include <string>

#include "quant_hft/core/wal_replay_loader.h"
#include "quant_hft/services/in_memory_portfolio_ledger.h"
#include "quant_hft/services/order_state_machine.h"

int main(int argc, char** argv) {
    using namespace quant_hft;

    const std::string wal_path = argc > 1 ? argv[1] : "runtime_events.wal";

    OrderStateMachine order_state_machine;
    InMemoryPortfolioLedger ledger;
    WalReplayLoader replay_loader;

    const auto stats = replay_loader.Replay(wal_path, &order_state_machine, &ledger);
    std::cout << "WAL replay completed path=" << wal_path
              << " lines=" << stats.lines_total
              << " events=" << stats.events_loaded
              << " ignored=" << stats.ignored_lines
              << " parse_errors=" << stats.parse_errors
              << " state_rejected=" << stats.state_rejected
              << " ledger_applied=" << stats.ledger_applied << '\n';
    return 0;
}
