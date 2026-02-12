#pragma once

#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

#include "quant_hft/contracts/types.h"

namespace quant_hft {

class ITradingDomainStore;
class OrderManager;

enum class RiskRuleType : std::uint8_t {
    MAX_LOSS_PER_ORDER = 0,
    MAX_POSITION_PER_INSTRUMENT = 1,
    MAX_TOTAL_POSITION = 2,
    MAX_LEVERAGE = 3,
    MAX_ORDER_RATE = 4,
    MAX_CANCEL_RATE = 5,
    SELF_TRADE_PREVENTION = 6,
    MAX_ORDER_VOLUME = 7,
    DAILY_LOSS_LIMIT = 8,
};

enum class RiskEventSeverity : std::uint8_t {
    INFO = 1,
    WARN = 2,
    ERROR = 3,
    CRITICAL = 4,
};

struct RiskCheckResult {
    bool allowed{true};
    RiskRuleType violated_rule{RiskRuleType::MAX_ORDER_VOLUME};
    std::string reason;
    std::optional<double> limit_value;
    std::optional<double> current_value;
};

struct RiskRule {
    std::string rule_id;
    RiskRuleType type{RiskRuleType::MAX_ORDER_VOLUME};
    std::string strategy_id;
    std::string instrument_id;
    double threshold{0.0};
    std::string time_range;
    bool enabled{true};
    int priority{100};
};

struct OrderContext {
    std::string account_id;
    std::string strategy_id;
    std::string instrument_id;
    double current_position{0.0};
    double current_margin{0.0};
    double available_fund{0.0};
    double today_pnl{0.0};
    double today_commission{0.0};
    double current_price{0.0};
    double contract_multiplier{1.0};
};

struct RiskEvent {
    std::string event_id;
    std::string account_id;
    std::string strategy_id;
    std::string instrument_id;
    RiskRuleType rule_type{RiskRuleType::MAX_ORDER_VOLUME};
    std::string rule_id;
    std::string description;
    RiskEventSeverity severity{RiskEventSeverity::WARN};
    std::chrono::system_clock::time_point timestamp;
    std::unordered_map<std::string, std::string> tags;
};

struct RiskManagerConfig {
    bool enable_self_trade_prevention{true};
    double default_max_loss_per_order{5000.0};
    int default_max_order_volume{100};
    int default_max_order_rate{50};
    int default_max_cancel_rate{20};
    std::string rule_file_path{"configs/risk_rules.yaml"};
    bool enable_dynamic_reload{true};
    int reload_interval_seconds{60};
};

class RiskManager {
public:
    virtual ~RiskManager() = default;

    virtual bool Initialize(const RiskManagerConfig& config) = 0;

    virtual RiskCheckResult CheckOrder(const OrderIntent& intent,
                                       const OrderContext& context) = 0;

    virtual RiskCheckResult CheckCancel(const std::string& client_order_id,
                                        const OrderContext& context) = 0;

    virtual void OnTrade(const Trade& trade) = 0;

    virtual void OnOrderRejected(const Order& order, const std::string& reason) = 0;

    virtual bool ReloadRules(const std::vector<RiskRule>& rules) = 0;

    virtual std::vector<RiskRule> GetActiveRules() const = 0;

    virtual void ResetDailyStats() = 0;

    using RiskEventCallback = std::function<void(const RiskEvent&)>;
    virtual void RegisterRiskEventCallback(RiskEventCallback callback) = 0;
};

std::shared_ptr<RiskManager> CreateRiskManager(std::shared_ptr<OrderManager> order_manager,
                                               std::shared_ptr<ITradingDomainStore> domain_store);

std::vector<RiskRule> LoadRiskRulesFromYaml(const std::string& file_path, std::string* error);

}  // namespace quant_hft
