#pragma once

#include <cstddef>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "quant_hft/core/fixed_decimal.h"
#include "quant_hft/interfaces/settlement_store.h"
#include "quant_hft/interfaces/trading_domain_store.h"
#include "quant_hft/services/settlement_price_provider.h"
#include "quant_hft/services/settlement_query_client.h"

namespace quant_hft {

struct DailySettlementConfig {
    std::string account_id;
    std::string trading_day;
    bool force_run{false};
    bool settlement_shadow_enabled{false};
    bool strict_order_trade_backfill{false};
    int running_stale_timeout_ms{300000};
    std::string evidence_path;
    std::string diff_report_path;
};

struct DailySettlementResult {
    bool success{false};
    bool noop{false};
    bool blocked{false};
    std::string status;
    std::string message;
    std::string diff_report_path;
    bool backfill_completed{false};
    std::size_t backfill_event_count{0};
    std::string backfill_error;
    bool reconcile_completed{false};
    bool reconcile_passed{false};
    std::size_t reconcile_diff_count{0};
};

class DailySettlementService {
   public:
    DailySettlementService(std::shared_ptr<SettlementPriceProvider> price_provider,
                           std::shared_ptr<ISettlementStore> store,
                           std::shared_ptr<SettlementQueryClient> query_client,
                           std::shared_ptr<ITradingDomainStore> domain_store = nullptr);

    bool Run(const DailySettlementConfig& config, DailySettlementResult* result,
             std::string* error);

    // Writes a self-describing terminal-run artifact through temp-file + rename and
    // validates the published file before returning success.
    bool WriteEvidenceAtomically(const DailySettlementConfig& config,
                                 const DailySettlementResult& result, bool run_ok,
                                 const std::string& run_error, std::string* error) const;

   private:
    struct ReconcileResult {
        bool passed{false};
        bool blocked{false};
        std::vector<SettlementReconcileDiffRecord> diffs;
    };

    bool LoadSettlementPrices(
        const DailySettlementConfig& config,
        const std::vector<SettlementOpenPositionRecord>& positions,
        std::unordered_map<std::string, double>* final_prices,
        std::unordered_map<std::string, SettlementInstrumentRecord>* instruments,
        std::string* error);
    bool RunSettlementLoop(
        const DailySettlementConfig& config, std::vector<SettlementOpenPositionRecord>* positions,
        const std::unordered_map<std::string, double>& final_prices,
        const std::unordered_map<std::string, SettlementInstrumentRecord>& instruments,
        std::int64_t* total_position_profit_cents, std::string* error);
    bool RolloverPositions(const DailySettlementConfig& config, std::string* error);
    bool RebuildAccountFunds(
        const DailySettlementConfig& config,
        const std::vector<SettlementOpenPositionRecord>& positions,
        const std::unordered_map<std::string, double>& final_prices,
        const std::unordered_map<std::string, SettlementInstrumentRecord>& instruments,
        std::int64_t total_position_profit_cents, SettlementAccountFundsRecord* funds_out,
        SettlementSummaryRecord* summary_out, std::string* error);
    bool VerifyAgainstCTP(const DailySettlementConfig& config,
                          const SettlementAccountFundsRecord& local_funds,
                          ReconcileResult* reconcile_result, std::string* error);
    bool GenerateDiffReport(const DailySettlementConfig& config,
                            const std::vector<SettlementReconcileDiffRecord>& diffs,
                            std::string* error) const;
    bool WriteRunStatus(const DailySettlementConfig& config, const std::string& status,
                        EpochNanos started_ts_ns, const std::string& error_code,
                        const std::string& error_msg, std::string* error) const;
    bool WriteBlockedRun(const DailySettlementConfig& config, EpochNanos started_ts_ns,
                         const std::string& reason, std::string* error) const;
    bool WriteCompletedRun(const DailySettlementConfig& config, EpochNanos started_ts_ns,
                           std::string* error) const;
    bool PersistBackfillEvents(const std::string& account_id, const std::vector<OrderEvent>& events,
                               EpochNanos settlement_start_ts_ns, std::string* error) const;
    static std::string NormalizeRunStatus(const std::string& status);
    static bool IsRunTerminalStatus(const std::string& status);
    static std::string PreviousTradingDay(const std::string& trading_day);
    static std::string DateFromEpochNanos(EpochNanos ts_ns);
    static std::string EscapeJson(const std::string& raw);
    static double CentsToDouble(std::int64_t cents);
    static std::int64_t ToCents(double value, FixedRoundingMode mode);
    static std::int64_t RoundScaled6ToCents(long double value, FixedRoundingMode mode);

    std::shared_ptr<SettlementPriceProvider> price_provider_;
    std::shared_ptr<ISettlementStore> store_;
    std::shared_ptr<SettlementQueryClient> query_client_;
    std::shared_ptr<ITradingDomainStore> domain_store_;
};

}  // namespace quant_hft
