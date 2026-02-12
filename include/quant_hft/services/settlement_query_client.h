#pragma once

#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "quant_hft/core/ctp_trader_adapter.h"
#include "quant_hft/core/flow_controller.h"

namespace quant_hft {

struct SettlementQueryClientConfig {
    std::string account_id;
    int retry_max{3};
    int backoff_initial_ms{1000};
    int backoff_max_ms{5000};
    int acquire_timeout_ms{1000};
};

class SettlementQueryClient {
public:
    SettlementQueryClient(std::shared_ptr<CTPTraderAdapter> trader,
                          std::shared_ptr<FlowController> flow_controller,
                          SettlementQueryClientConfig config);

    bool QueryTradingAccountWithRetry(int request_id_seed, std::string* error);
    bool QueryInvestorPositionWithRetry(int request_id_seed, std::string* error);
    bool QueryInstrumentWithRetry(int request_id_seed, std::string* error);
    bool QueryOrderTradeBackfill(std::vector<OrderEvent>* out_events, std::string* error);

private:
    bool QueryWithRetry(const std::string& name,
                        int request_id_seed,
                        const std::function<bool(int)>& sender,
                        std::string* error);
    bool AcquireQueryPermit(std::string* error) const;

    std::shared_ptr<CTPTraderAdapter> trader_;
    std::shared_ptr<FlowController> flow_controller_;
    SettlementQueryClientConfig config_;
    mutable std::mutex backfill_mutex_;
    std::vector<OrderEvent> backfill_events_;
};

}  // namespace quant_hft
