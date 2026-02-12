#include <algorithm>
#include <iostream>
#include <memory>
#include <string>
#include <vector>

#include "quant_hft/core/ctp_config_loader.h"
#include "quant_hft/core/ctp_trader_adapter.h"
#include "quant_hft/core/flow_controller.h"
#include "quant_hft/core/settlement_store_client_adapter.h"
#include "quant_hft/core/storage_client_factory.h"
#include "quant_hft/core/storage_client_pool.h"
#include "quant_hft/core/storage_connection_config.h"
#include "quant_hft/core/storage_retry_policy.h"
#include "quant_hft/core/trading_domain_store_client_adapter.h"
#include "quant_hft/services/daily_settlement_service.h"
#include "quant_hft/services/settlement_price_provider.h"
#include "quant_hft/services/settlement_query_client.h"

namespace {

bool ParseArgs(int argc,
               char** argv,
               std::string* config_path,
               std::string* trading_day,
               bool* force_run,
               bool* shadow_mode,
               bool* strict_backfill,
               std::string* evidence_path,
               std::string* settlement_price_json_path,
               std::string* price_cache_db_path,
               std::string* diff_report_path,
               std::string* error) {
    if (config_path == nullptr || trading_day == nullptr || force_run == nullptr ||
        shadow_mode == nullptr || strict_backfill == nullptr || evidence_path == nullptr ||
        settlement_price_json_path == nullptr || price_cache_db_path == nullptr ||
        diff_report_path == nullptr) {
        if (error != nullptr) {
            *error = "output argument pointer is null";
        }
        return false;
    }
    *config_path = "configs/prod/ctp.yaml";
    *force_run = false;
    *shadow_mode = false;
    *strict_backfill = false;
    *evidence_path = "";
    *settlement_price_json_path = "";
    *price_cache_db_path = "runtime/settlement_price_cache.sqlite";
    *diff_report_path = "";

    for (int i = 1; i < argc; ++i) {
        const std::string arg = argv[i];
        if (arg == "--config") {
            if (i + 1 >= argc) {
                if (error != nullptr) {
                    *error = "--config requires a value";
                }
                return false;
            }
            *config_path = argv[++i];
            continue;
        }
        if (arg == "--trading-day") {
            if (i + 1 >= argc) {
                if (error != nullptr) {
                    *error = "--trading-day requires a value";
                }
                return false;
            }
            *trading_day = argv[++i];
            continue;
        }
        if (arg == "--force") {
            *force_run = true;
            continue;
        }
        if (arg == "--shadow") {
            *shadow_mode = true;
            continue;
        }
        if (arg == "--strict-order-trade-backfill") {
            *strict_backfill = true;
            continue;
        }
        if (arg == "--evidence-path") {
            if (i + 1 >= argc) {
                if (error != nullptr) {
                    *error = "--evidence-path requires a value";
                }
                return false;
            }
            *evidence_path = argv[++i];
            continue;
        }
        if (arg == "--settlement-price-json") {
            if (i + 1 >= argc) {
                if (error != nullptr) {
                    *error = "--settlement-price-json requires a value";
                }
                return false;
            }
            *settlement_price_json_path = argv[++i];
            continue;
        }
        if (arg == "--price-cache-db") {
            if (i + 1 >= argc) {
                if (error != nullptr) {
                    *error = "--price-cache-db requires a value";
                }
                return false;
            }
            *price_cache_db_path = argv[++i];
            continue;
        }
        if (arg == "--diff-report-path") {
            if (i + 1 >= argc) {
                if (error != nullptr) {
                    *error = "--diff-report-path requires a value";
                }
                return false;
            }
            *diff_report_path = argv[++i];
            continue;
        }
        if (error != nullptr) {
            *error = "unknown option: " + arg;
        }
        return false;
    }

    if (trading_day->empty()) {
        if (error != nullptr) {
            *error = "--trading-day is required";
        }
        return false;
    }
    return true;
}

}  // namespace

int main(int argc, char** argv) {
    using namespace quant_hft;

    std::string config_path;
    std::string trading_day;
    bool force_run = false;
    bool shadow_mode = false;
    bool strict_backfill = false;
    std::string evidence_path;
    std::string settlement_price_json_path;
    std::string price_cache_db_path;
    std::string diff_report_path;
    std::string parse_error;
    if (!ParseArgs(argc,
                   argv,
                   &config_path,
                   &trading_day,
                   &force_run,
                   &shadow_mode,
                   &strict_backfill,
                   &evidence_path,
                   &settlement_price_json_path,
                   &price_cache_db_path,
                   &diff_report_path,
                   &parse_error)) {
        std::cerr << "invalid arguments: " << parse_error << '\n';
        return 1;
    }

    CtpFileConfig file_config;
    std::string config_error;
    if (!CtpConfigLoader::LoadFromYaml(config_path, &file_config, &config_error)) {
        std::cerr << "failed to load CTP config: " << config_error << '\n';
        return 1;
    }
    const auto& runtime = file_config.runtime;

    const auto storage_config = StorageConnectionConfig::FromEnvironment();
    auto sql_client = StorageClientFactory::CreateTimescaleClient(storage_config, &config_error);
    if (sql_client == nullptr) {
        std::cerr << "failed to create timescale client: " << config_error << '\n';
        return 1;
    }
    auto pooled_sql = std::make_shared<PooledTimescaleSqlClient>(
        std::vector<std::shared_ptr<ITimescaleSqlClient>>{sql_client});
    StorageRetryPolicy retry_policy;
    retry_policy.max_attempts = 3;
    retry_policy.initial_backoff_ms = 1;
    retry_policy.max_backoff_ms = 8;
    auto settlement_store = std::make_shared<SettlementStoreClientAdapter>(
        pooled_sql,
        retry_policy,
        storage_config.timescale.trading_schema,
        "ops");
    auto domain_store = std::make_shared<TradingDomainStoreClientAdapter>(
        pooled_sql,
        retry_policy,
        storage_config.timescale.trading_schema);

    auto trader = std::make_shared<CTPTraderAdapter>(
        static_cast<std::size_t>(std::max(1, runtime.query_rate_per_sec)),
        1);
    MarketDataConnectConfig connect_cfg;
    connect_cfg.market_front_address = runtime.md_front;
    connect_cfg.trader_front_address = runtime.td_front;
    connect_cfg.flow_path = runtime.flow_path;
    connect_cfg.broker_id = runtime.broker_id;
    connect_cfg.user_id = runtime.user_id;
    connect_cfg.investor_id = runtime.investor_id;
    connect_cfg.password = runtime.password;
    connect_cfg.app_id = runtime.app_id;
    connect_cfg.auth_code = runtime.auth_code;
    connect_cfg.is_production_mode = runtime.is_production_mode;
    connect_cfg.enable_real_api = runtime.enable_real_api;
    connect_cfg.enable_terminal_auth = runtime.enable_terminal_auth;
    connect_cfg.connect_timeout_ms = runtime.connect_timeout_ms;
    connect_cfg.reconnect_max_attempts = runtime.reconnect_max_attempts;
    connect_cfg.reconnect_initial_backoff_ms = runtime.reconnect_initial_backoff_ms;
    connect_cfg.reconnect_max_backoff_ms = runtime.reconnect_max_backoff_ms;
    connect_cfg.query_retry_backoff_ms = runtime.query_retry_backoff_ms;
    connect_cfg.recovery_quiet_period_ms = runtime.recovery_quiet_period_ms;
    connect_cfg.settlement_confirm_required = runtime.settlement_confirm_required;
    if (!trader->Connect(connect_cfg)) {
        std::cerr << "failed to connect trader adapter: " << trader->GetLastConnectDiagnostic()
                  << '\n';
        return 2;
    }
    if (runtime.settlement_confirm_required && !trader->ConfirmSettlement()) {
        std::cerr << "failed to confirm settlement for trader session" << '\n';
        trader->Disconnect();
        return 2;
    }

    auto flow_controller = std::make_shared<FlowController>();
    FlowRule settlement_query_rule;
    settlement_query_rule.account_id =
        file_config.account_id.empty() ? runtime.user_id : file_config.account_id;
    settlement_query_rule.type = OperationType::kSettlementQuery;
    settlement_query_rule.rate_per_second = static_cast<double>(runtime.settlement_query_rate_per_sec);
    settlement_query_rule.capacity = runtime.settlement_query_bucket_capacity;
    flow_controller->AddRule(settlement_query_rule);

    SettlementQueryClientConfig query_cfg;
    query_cfg.account_id = settlement_query_rule.account_id;
    query_cfg.retry_max = runtime.settlement_retry_max;
    query_cfg.backoff_initial_ms = runtime.settlement_retry_backoff_initial_ms;
    query_cfg.backoff_max_ms = runtime.settlement_retry_backoff_max_ms;
    query_cfg.acquire_timeout_ms = std::min(runtime.settlement_retry_backoff_initial_ms, 1000);
    auto query_client = std::make_shared<SettlementQueryClient>(trader, flow_controller, query_cfg);
    auto settlement_price_provider = std::make_shared<ProdSettlementPriceProvider>(
        price_cache_db_path, settlement_price_json_path);

    DailySettlementConfig settlement_cfg;
    settlement_cfg.account_id = settlement_query_rule.account_id;
    settlement_cfg.trading_day = trading_day;
    settlement_cfg.force_run = force_run;
    settlement_cfg.settlement_shadow_enabled = shadow_mode || runtime.settlement_shadow_enabled;
    settlement_cfg.strict_order_trade_backfill = strict_backfill;
    settlement_cfg.running_stale_timeout_ms = runtime.settlement_running_stale_timeout_ms;
    settlement_cfg.evidence_path = evidence_path;
    settlement_cfg.diff_report_path = diff_report_path;

    DailySettlementService service(
        settlement_price_provider, settlement_store, query_client, domain_store);
    DailySettlementResult result;
    std::string run_error;
    const bool run_ok = service.Run(settlement_cfg, &result, &run_error);

    trader->Disconnect();

    if (!run_ok) {
        std::cerr << "daily settlement run failed: " << run_error << '\n';
        return 2;
    }

    std::cout << "{"
              << "\"trading_day\":\"" << settlement_cfg.trading_day << "\","
              << "\"account_id\":\"" << settlement_cfg.account_id << "\","
              << "\"success\":" << (result.success ? "true" : "false") << ","
              << "\"noop\":" << (result.noop ? "true" : "false") << ","
              << "\"blocked\":" << (result.blocked ? "true" : "false") << ","
              << "\"status\":\"" << result.status << "\","
              << "\"message\":\"" << result.message << "\","
              << "\"diff_report_path\":\""
              << (!result.diff_report_path.empty() ? result.diff_report_path : diff_report_path)
              << "\""
              << "}" << '\n';
    return result.success ? 0 : 2;
}
