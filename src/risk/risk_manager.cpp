#include "quant_hft/risk/risk_manager.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <filesystem>
#include <fstream>
#include <memory>
#include <mutex>
#include <sstream>
#include <thread>
#include <unordered_map>
#include <utility>

#include "quant_hft/core/flow_controller.h"
#include "quant_hft/interfaces/trading_domain_store.h"
#include "quant_hft/risk/risk_rule_executor.h"
#include "quant_hft/risk/risk_rule_registry.h"
#include "quant_hft/services/order_manager.h"

namespace quant_hft {
namespace {

std::string Trim(const std::string& value) {
    const auto first = value.find_first_not_of(" \t\r\n");
    if (first == std::string::npos) {
        return "";
    }
    const auto last = value.find_last_not_of(" \t\r\n");
    return value.substr(first, last - first + 1);
}

bool ParseDouble(const std::string& value, double* out) {
    if (out == nullptr) {
        return false;
    }
    try {
        *out = std::stod(value);
        return true;
    } catch (...) {
        return false;
    }
}

bool ParseBool(const std::string& value, bool* out) {
    if (out == nullptr) {
        return false;
    }
    if (value == "true" || value == "True" || value == "1") {
        *out = true;
        return true;
    }
    if (value == "false" || value == "False" || value == "0") {
        *out = false;
        return true;
    }
    return false;
}

RiskCheckResult BuildAllow() {
    RiskCheckResult result;
    result.allowed = true;
    return result;
}

std::string BuildRuleId(const std::string& prefix, const std::string& scope, const std::string& key) {
    if (!scope.empty()) {
        return prefix + "." + scope + "." + key;
    }
    return prefix + ".global." + key;
}

bool MatchRule(const RiskRule& rule, const OrderContext& context) {
    if (!rule.enabled) {
        return false;
    }
    if (!rule.strategy_id.empty() && rule.strategy_id != context.strategy_id) {
        return false;
    }
    if (!rule.instrument_id.empty() && rule.instrument_id != context.instrument_id) {
        return false;
    }
    return true;
}

int RuleSpecificity(const RiskRule& rule) {
    int score = 0;
    if (!rule.strategy_id.empty()) {
        score += 2;
    }
    if (!rule.instrument_id.empty()) {
        score += 1;
    }
    return score;
}

void AddThresholdRule(std::vector<RiskRule>* rules,
                      RiskRuleType type,
                      const std::string& rule_id,
                      const std::string& strategy_id,
                      double threshold,
                      int priority) {
    if (rules == nullptr || threshold <= 0.0) {
        return;
    }
    RiskRule rule;
    rule.rule_id = rule_id;
    rule.type = type;
    rule.strategy_id = strategy_id;
    rule.threshold = threshold;
    rule.enabled = true;
    rule.priority = priority;
    rules->push_back(std::move(rule));
}

}  // namespace

std::vector<RiskRule> LoadRiskRulesFromYaml(const std::string& file_path, std::string* error) {
    std::ifstream input(file_path);
    if (!input.is_open()) {
        if (error != nullptr) {
            *error = "failed to open risk rule file: " + file_path;
        }
        return {};
    }

    std::vector<RiskRule> rules;
    std::string line;
    std::string section;
    std::string current_strategy;
    int line_no = 0;

    auto add_rule = [&](RiskRuleType type, const std::string& key, double threshold, int priority) {
        RiskRule rule;
        rule.type = type;
        rule.strategy_id = current_strategy;
        rule.instrument_id = "";
        rule.threshold = threshold;
        rule.enabled = true;
        rule.priority = priority;
        rule.rule_id = BuildRuleId("risk", current_strategy, key);
        rules.push_back(std::move(rule));
    };

    while (std::getline(input, line)) {
        ++line_no;
        auto hash_pos = line.find('#');
        if (hash_pos != std::string::npos) {
            line = line.substr(0, hash_pos);
        }
        const auto raw = Trim(line);
        if (raw.empty()) {
            continue;
        }
        if (raw == "global:") {
            section = "global";
            current_strategy.clear();
            continue;
        }
        if (raw == "strategies:") {
            section = "strategies";
            current_strategy.clear();
            continue;
        }
        if (section == "strategies" && raw.rfind("- id:", 0) == 0U) {
            current_strategy = Trim(raw.substr(5));
            if (!current_strategy.empty() && current_strategy.front() == '"' &&
                current_strategy.back() == '"') {
                current_strategy = current_strategy.substr(1, current_strategy.size() - 2);
            }
            continue;
        }

        const auto colon = raw.find(':');
        if (colon == std::string::npos) {
            continue;
        }
        const auto key = Trim(raw.substr(0, colon));
        const auto value = Trim(raw.substr(colon + 1));
        const bool strategy_scope = section == "strategies" && !current_strategy.empty();

        double number = 0.0;
        bool boolean_value = false;

        if ((key == "max_loss_per_order" || key == "max_order_volume" || key == "max_order_rate" ||
             key == "max_cancel_rate" || key == "max_position_per_instrument" ||
             key == "max_total_position" || key == "max_leverage" || key == "daily_loss_limit" ||
             key == "max_daily_loss") &&
            ParseDouble(value, &number)) {
            const int priority = strategy_scope ? 10 : 100;
            if (key == "max_loss_per_order") {
                add_rule(RiskRuleType::MAX_LOSS_PER_ORDER, key, number, priority);
            } else if (key == "max_order_volume") {
                add_rule(RiskRuleType::MAX_ORDER_VOLUME, key, number, priority);
            } else if (key == "max_order_rate") {
                add_rule(RiskRuleType::MAX_ORDER_RATE, key, number, priority);
            } else if (key == "max_cancel_rate") {
                add_rule(RiskRuleType::MAX_CANCEL_RATE, key, number, priority);
            } else if (key == "max_position_per_instrument") {
                add_rule(RiskRuleType::MAX_POSITION_PER_INSTRUMENT, key, number, priority);
            } else if (key == "max_total_position") {
                add_rule(RiskRuleType::MAX_TOTAL_POSITION, key, number, priority);
            } else if (key == "max_leverage") {
                add_rule(RiskRuleType::MAX_LEVERAGE, key, number, priority);
            } else {
                add_rule(RiskRuleType::DAILY_LOSS_LIMIT, key, number, priority);
            }
            continue;
        }

        if (key == "self_trade_prevention" && ParseBool(value, &boolean_value)) {
            RiskRule rule;
            rule.type = RiskRuleType::SELF_TRADE_PREVENTION;
            rule.strategy_id = current_strategy;
            rule.threshold = boolean_value ? 1.0 : 0.0;
            rule.enabled = true;
            rule.priority = strategy_scope ? 10 : 100;
            rule.rule_id = BuildRuleId("risk", current_strategy, "self_trade_prevention");
            rules.push_back(std::move(rule));
            continue;
        }
    }

    if (rules.empty() && error != nullptr) {
        *error = "no risk rules loaded from file: " + file_path + " line=" + std::to_string(line_no);
    } else if (error != nullptr) {
        error->clear();
    }
    return rules;
}

class DefaultRiskManager final : public RiskManager {
public:
    DefaultRiskManager(std::shared_ptr<OrderManager> order_manager,
                       std::shared_ptr<ITradingDomainStore> domain_store)
        : order_manager_(std::move(order_manager)), domain_store_(std::move(domain_store)) {}

    ~DefaultRiskManager() override {
        stop_reload_.store(true);
        if (reload_thread_.joinable()) {
            reload_thread_.join();
        }
    }

    bool Initialize(const RiskManagerConfig& config) override {
        config_ = config;
        RegisterDefaultRiskRules(&executor_,
                                 order_manager_,
                                 config_.enable_self_trade_prevention,
                                 [this](const std::string& key, double rate, int limiter_type) {
                                     return ConsumeRateToken(key, rate, limiter_type);
                                 });

        std::vector<RiskRule> loaded_rules;
        std::string load_error;
        if (!config_.rule_file_path.empty()) {
            loaded_rules = LoadRiskRulesFromYaml(config_.rule_file_path, &load_error);
        }
        if (loaded_rules.empty()) {
            loaded_rules = BuildDefaultRules(config_);
        }
        ReloadRules(loaded_rules);

        stop_reload_.store(false);
        if (config_.enable_dynamic_reload && !config_.rule_file_path.empty()) {
            StartReloadThread();
        }
        return true;
    }

    RiskCheckResult CheckOrder(const OrderIntent& intent, const OrderContext& context) override {
        auto active_rules = SelectRules(context, false);
        for (const auto& rule : active_rules) {
            const auto result = executor_.Execute(rule, intent, context);
            if (!result.allowed) {
                EmitRejectEvent(rule, context, result.reason, RiskEventSeverity::WARN, intent.client_order_id);
                return result;
            }
        }
        return BuildAllow();
    }

    RiskCheckResult CheckCancel(const std::string& client_order_id,
                                const OrderContext& context) override {
        OrderIntent intent;
        intent.client_order_id = client_order_id;
        intent.account_id = context.account_id;
        intent.strategy_id = context.strategy_id;
        intent.instrument_id = context.instrument_id;
        auto active_rules = SelectRules(context, true);
        for (const auto& rule : active_rules) {
            const auto result = executor_.Execute(rule, intent, context);
            if (!result.allowed) {
                EmitRejectEvent(rule, context, result.reason, RiskEventSeverity::WARN, client_order_id);
                return result;
            }
        }
        return BuildAllow();
    }

    void OnTrade(const Trade& trade) override {
        const double trade_profit = trade.profit;
        if (trade_profit < 0.0) {
            std::lock_guard<std::mutex> lock(stats_mutex_);
            daily_loss_accumulated_ += std::fabs(trade_profit);
        }
        {
            std::lock_guard<std::mutex> lock(stats_mutex_);
            daily_commission_ += std::max(0.0, trade.commission);
        }
    }

    void OnOrderRejected(const Order& order, const std::string& reason) override {
        RiskRule synthetic_rule;
        synthetic_rule.rule_id = "risk.order_rejected";
        synthetic_rule.type = RiskRuleType::MAX_ORDER_VOLUME;
        OrderContext context;
        context.account_id = order.account_id;
        context.strategy_id = order.strategy_id;
        context.instrument_id = order.symbol;
        EmitRejectEvent(synthetic_rule,
                        context,
                        "order rejected: " + reason,
                        RiskEventSeverity::ERROR,
                        order.order_id);
    }

    bool ReloadRules(const std::vector<RiskRule>& rules) override {
        std::lock_guard<std::mutex> lock(rules_mutex_);
        rules_ = rules;
        std::sort(rules_.begin(), rules_.end(), [](const RiskRule& left, const RiskRule& right) {
            if (left.priority != right.priority) {
                return left.priority < right.priority;
            }
            return RuleSpecificity(left) > RuleSpecificity(right);
        });
        return true;
    }

    std::vector<RiskRule> GetActiveRules() const override {
        std::lock_guard<std::mutex> lock(rules_mutex_);
        std::vector<RiskRule> out;
        out.reserve(rules_.size());
        for (const auto& rule : rules_) {
            if (rule.enabled) {
                out.push_back(rule);
            }
        }
        return out;
    }

    void ResetDailyStats() override {
        std::lock_guard<std::mutex> lock(stats_mutex_);
        daily_loss_accumulated_ = 0.0;
        daily_commission_ = 0.0;
    }

    void RegisterRiskEventCallback(RiskEventCallback callback) override {
        std::lock_guard<std::mutex> lock(callback_mutex_);
        callback_ = std::move(callback);
    }

private:
    static std::vector<RiskRule> BuildDefaultRules(const RiskManagerConfig& config) {
        std::vector<RiskRule> defaults;
        AddThresholdRule(&defaults,
                         RiskRuleType::MAX_LOSS_PER_ORDER,
                         "risk.global.max_loss_per_order",
                         "",
                         config.default_max_loss_per_order,
                         100);
        AddThresholdRule(&defaults,
                         RiskRuleType::MAX_ORDER_VOLUME,
                         "risk.global.max_order_volume",
                         "",
                         static_cast<double>(config.default_max_order_volume),
                         100);
        AddThresholdRule(&defaults,
                         RiskRuleType::MAX_ORDER_RATE,
                         "risk.global.max_order_rate",
                         "",
                         static_cast<double>(config.default_max_order_rate),
                         100);
        AddThresholdRule(&defaults,
                         RiskRuleType::MAX_CANCEL_RATE,
                         "risk.global.max_cancel_rate",
                         "",
                         static_cast<double>(config.default_max_cancel_rate),
                         100);

        RiskRule stp;
        stp.rule_id = "risk.global.self_trade_prevention";
        stp.type = RiskRuleType::SELF_TRADE_PREVENTION;
        stp.threshold = config.enable_self_trade_prevention ? 1.0 : 0.0;
        stp.priority = 100;
        defaults.push_back(std::move(stp));
        return defaults;
    }

    std::vector<RiskRule> SelectRules(const OrderContext& context, bool cancel_only) const {
        std::lock_guard<std::mutex> lock(rules_mutex_);
        std::vector<RiskRule> out;
        out.reserve(rules_.size());
        for (const auto& rule : rules_) {
            if (!MatchRule(rule, context)) {
                continue;
            }
            if (cancel_only && rule.type != RiskRuleType::MAX_CANCEL_RATE) {
                continue;
            }
            out.push_back(rule);
        }
        return out;
    }

    bool ConsumeRateToken(const std::string& key, double rate, int limiter_type) {
        auto& mutex = limiter_type == 0 ? order_limiter_mutex_ : cancel_limiter_mutex_;
        auto& limiters = limiter_type == 0 ? order_limiters_ : cancel_limiters_;

        std::lock_guard<std::mutex> lock(mutex);
        const int capacity = std::max(1, static_cast<int>(std::ceil(rate)));
        auto it = limiters.find(key);
        if (it == limiters.end()) {
            auto inserted =
                limiters.emplace(key, std::make_shared<TokenBucket>(rate, capacity));
            it = inserted.first;
        } else {
            it->second->SetRate(rate);
        }
        return it->second->TryAcquire();
    }

    void EmitRejectEvent(const RiskRule& rule,
                         const OrderContext& context,
                         const std::string& description,
                         RiskEventSeverity severity,
                         const std::string& client_order_id) {
        RiskEvent event;
        event.event_id = std::to_string(NowEpochNanos());
        event.account_id = context.account_id;
        event.strategy_id = context.strategy_id;
        event.instrument_id = context.instrument_id;
        event.rule_type = rule.type;
        event.rule_id = rule.rule_id;
        event.description = description;
        event.severity = severity;
        event.timestamp = std::chrono::system_clock::now();
        event.tags.emplace("client_order_id", client_order_id);
        event.tags.emplace("rule_id", rule.rule_id);

        if (domain_store_ != nullptr) {
            RiskEventRecord record;
            record.account_id = event.account_id;
            record.strategy_id = event.strategy_id;
            record.instrument_id = event.instrument_id;
            record.order_ref = client_order_id;
            record.rule_id = event.rule_id;
            record.event_type = 1;
            record.event_level = static_cast<std::int32_t>(severity);
            record.event_desc = event.description;
            record.tags_json = "{\"rule_id\":\"" + event.rule_id + "\"}";
            record.details_json = "{}";
            record.event_ts_ns = NowEpochNanos();
            std::string ignored_error;
            (void)domain_store_->AppendRiskEvent(record, &ignored_error);
        }

        RiskEventCallback cb;
        {
            std::lock_guard<std::mutex> lock(callback_mutex_);
            cb = callback_;
        }
        if (cb) {
            cb(event);
        }
    }

    void StartReloadThread() {
        namespace fs = std::filesystem;
        if (!fs::exists(config_.rule_file_path)) {
            return;
        }
        last_rule_file_write_time_ = fs::last_write_time(config_.rule_file_path);
        reload_thread_ = std::thread([this]() {
            namespace fs = std::filesystem;
            const int sleep_seconds = std::max(1, config_.reload_interval_seconds);
            while (!stop_reload_.load()) {
                std::this_thread::sleep_for(std::chrono::seconds(sleep_seconds));
                if (stop_reload_.load()) {
                    break;
                }
                if (!fs::exists(config_.rule_file_path)) {
                    continue;
                }
                const auto current_write_time = fs::last_write_time(config_.rule_file_path);
                if (current_write_time == last_rule_file_write_time_) {
                    continue;
                }
                last_rule_file_write_time_ = current_write_time;
                std::string error;
                const auto reloaded = LoadRiskRulesFromYaml(config_.rule_file_path, &error);
                if (!reloaded.empty()) {
                    ReloadRules(reloaded);
                }
            }
        });
    }

    std::shared_ptr<OrderManager> order_manager_;
    std::shared_ptr<ITradingDomainStore> domain_store_;
    RiskManagerConfig config_;

    mutable std::mutex rules_mutex_;
    std::vector<RiskRule> rules_;
    RiskRuleExecutor executor_;

    mutable std::mutex callback_mutex_;
    RiskEventCallback callback_;

    mutable std::mutex order_limiter_mutex_;
    mutable std::mutex cancel_limiter_mutex_;
    std::unordered_map<std::string, std::shared_ptr<TokenBucket>> order_limiters_;
    std::unordered_map<std::string, std::shared_ptr<TokenBucket>> cancel_limiters_;

    mutable std::mutex stats_mutex_;
    double daily_loss_accumulated_{0.0};
    double daily_commission_{0.0};

    std::atomic<bool> stop_reload_{false};
    std::thread reload_thread_;
    std::filesystem::file_time_type last_rule_file_write_time_{};
};

std::shared_ptr<RiskManager> CreateRiskManager(std::shared_ptr<OrderManager> order_manager,
                                               std::shared_ptr<ITradingDomainStore> domain_store) {
    return std::make_shared<DefaultRiskManager>(std::move(order_manager), std::move(domain_store));
}

}  // namespace quant_hft
