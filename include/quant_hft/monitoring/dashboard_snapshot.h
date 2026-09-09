#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "quant_hft/contracts/dashboard_observation.h"
#include "quant_hft/contracts/query_result.h"
#include "quant_hft/contracts/types.h"
#include "quant_hft/runtime/runtime_identity.h"

namespace quant_hft {

// Optional, private read-only observation. It never changes trading state or issues queries.
// Callbacks use a preallocated bounded cache and try_lock; file IO runs on a separate thread.
class DashboardSnapshotWriter {
   public:
    static constexpr std::size_t kMaxPositions = 2048;
    static constexpr std::size_t kMaxMarketQuotes = 2048;
    static constexpr std::size_t kMaxStrategyRiskRows = 2048;

    DashboardSnapshotWriter();
    ~DashboardSnapshotWriter();
    DashboardSnapshotWriter(const DashboardSnapshotWriter&) = delete;
    DashboardSnapshotWriter& operator=(const DashboardSnapshotWriter&) = delete;

    // The caller opts in. This writes an initial "missing" snapshot before starting the
    // one-second worker, so a restarted instance never presents old data as current.
    // Files are 0640 and identity-bound; the parent must be private to the trading user
    // and the dashboard publisher read group. A second writer or mismatched identity fails.
    bool Start(const std::string& output_file, const RuntimeIdentity& identity,
               std::string* error) noexcept;
    void Stop() noexcept;

    void CaptureAccount(const TradingAccountSnapshot& snapshot) noexcept;
    void CapturePositions(const QueryResult<InvestorPositionSnapshot>& result) noexcept;
    void MarkAccountQueryFailed() noexcept;
    void MarkPositionQueryFailed() noexcept;
    void CaptureMarket(const MarketSnapshot& snapshot) noexcept;
    void CaptureStrategyRisk(const std::vector<StrategyRiskSnapshot>& rows,
                             EpochNanos as_of_ns,
                             const std::string& trading_day) noexcept;

    // For diagnostics/tests only. Never call from a trading callback: these may block.
    std::string RenderSnapshot(std::int64_t now_ms);
    bool PublishNow() noexcept;
    std::uint64_t dropped_updates() const noexcept;
    std::uint64_t write_failures() const noexcept;

   private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace quant_hft
