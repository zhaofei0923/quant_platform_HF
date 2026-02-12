#pragma once

#include <memory>
#include <string>
#include <vector>

#include "quant_hft/interfaces/trading_domain_store.h"
#include "quant_hft/interfaces/settlement_store.h"
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
};

struct DailySettlementResult {
    bool success{false};
    bool noop{false};
    bool blocked{false};
    std::string status;
    std::string message;
};

class DailySettlementService {
public:
    DailySettlementService(std::shared_ptr<ISettlementStore> store,
                           std::shared_ptr<SettlementQueryClient> query_client,
                           std::shared_ptr<ITradingDomainStore> domain_store = nullptr);

    bool Run(const DailySettlementConfig& config,
             DailySettlementResult* result,
             std::string* error);

private:
    bool WriteRunStatus(const DailySettlementConfig& config,
                        const std::string& status,
                        EpochNanos started_ts_ns,
                        const std::string& error_code,
                        const std::string& error_msg,
                        std::string* error) const;
    bool WriteBlockedRun(const DailySettlementConfig& config,
                         EpochNanos started_ts_ns,
                         const std::string& reason,
                         std::string* error) const;
    bool WriteCompletedRun(const DailySettlementConfig& config,
                           EpochNanos started_ts_ns,
                           std::string* error) const;
    bool PersistBackfillEvents(const std::string& account_id,
                               const std::vector<OrderEvent>& events,
                               EpochNanos settlement_start_ts_ns,
                               std::string* error) const;

    std::shared_ptr<ISettlementStore> store_;
    std::shared_ptr<SettlementQueryClient> query_client_;
    std::shared_ptr<ITradingDomainStore> domain_store_;
};

}  // namespace quant_hft
