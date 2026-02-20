cmake_minimum_required(VERSION 3.20)
project(quant_platform_hf VERSION 0.1.0 LANGUAGES CXX)

list(APPEND CMAKE_MODULE_PATH "${CMAKE_SOURCE_DIR}/cmake")

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
    if(CMAKE_CXX_COMPILER_VERSION VERSION_LESS 11)
        message(FATAL_ERROR "GCC >= 11 is required, found ${CMAKE_CXX_COMPILER_VERSION}")
    endif()
elseif(CMAKE_CXX_COMPILER_ID MATCHES "Clang")
    if(CMAKE_CXX_COMPILER_VERSION VERSION_LESS 14)
        message(FATAL_ERROR "Clang >= 14 is required, found ${CMAKE_CXX_COMPILER_VERSION}")
    endif()
else()
    message(FATAL_ERROR
        "Unsupported compiler '${CMAKE_CXX_COMPILER_ID}'. Use GCC >= 11 or Clang >= 14.")
endif()

option(QUANT_HFT_BUILD_TESTS "Build C++ unit tests" ON)
option(QUANT_HFT_ENABLE_CTP_REAL_API "Enable CTP v6.7.11 real API integration" OFF)
option(QUANT_HFT_ENABLE_REDIS_EXTERNAL "Enable external Redis driver path" OFF)
option(QUANT_HFT_ENABLE_TIMESCALE_EXTERNAL "Enable external TimescaleDB driver path" OFF)
option(QUANT_HFT_ENABLE_ARROW_PARQUET "Enable native Arrow/Parquet C++ reader" OFF)
option(QUANT_HFT_WITH_METRICS "Enable Prometheus metrics exporter" OFF)
set(CTP_V6711_DIR "${CMAKE_SOURCE_DIR}/ctp_api/v6.7.11_20250617_api_traderapi_se_linux64"
    CACHE PATH "Path to CTP v6.7.11 API directory")

if(QUANT_HFT_ENABLE_ARROW_PARQUET OR QUANT_HFT_ENABLE_REDIS_EXTERNAL OR
   QUANT_HFT_ENABLE_TIMESCALE_EXTERNAL)
    find_package(PkgConfig REQUIRED)
endif()

add_library(quant_hft_core STATIC
    src/backtest/broker.cpp
    src/backtest/backtest_data_feed.cpp
    src/backtest/engine.cpp
    src/backtest/performance.cpp
    src/backtest/backtest_metrics.cpp
    src/backtest/live_data_feed.cpp
    src/apps/backtest_result_export.cpp
    src/indicators/adx.cpp
    src/indicators/atr.cpp
    src/indicators/ema.cpp
    src/indicators/kama.cpp
    src/indicators/sma.cpp
    src/strategy/base_strategy.cpp
    src/strategy/strategy_registry.cpp
    src/strategy/strategy_engine.cpp
    src/strategy/demo_live_strategy.cpp
    src/strategy/atomic_factory.cpp
    src/strategy/composite_config_loader.cpp
    src/strategy/strategy_main_config_loader.cpp
    src/strategy/composite_strategy.cpp
    src/strategy/atomic/kama_trend_strategy.cpp
    src/strategy/atomic/trend_strategy.cpp
    src/strategy/atomic/register_builtin_atomic_strategies.cpp
    src/core/backtest/indicator_trace_parquet_writer.cpp
    src/core/backtest/product_fee_config_loader.cpp
    src/core/backtest/sub_strategy_indicator_trace_parquet_writer.cpp
    src/core/backtest/parquet_data_feed.cpp
    src/core/common/callback_dispatcher.cpp
    src/core/common/event_dispatcher.cpp
    src/core/common/flow_controller.cpp
    src/core/common/circuit_breaker.cpp
    src/core/common/fixed_decimal.cpp
    src/core/ctp/ctp_config.cpp
    src/core/ctp/ctp_config_loader.cpp
    src/core/ctp/ctp_trader_adapter.cpp
    src/core/ctp/ctp_md_adapter.cpp
    src/core/ctp/ctp_gateway_adapter.cpp
    src/core/ctp/query_scheduler.cpp
    src/core/regulatory/local_wal_regulatory_sink.cpp
    src/core/regulatory/wal_replay_loader.cpp
    src/core/storage/redis_hash_client.cpp
    src/core/storage/redis_realtime_store.cpp
    src/core/storage/redis_realtime_store_client_adapter.cpp
    src/core/storage/libpq_timescale_sql_client.cpp
    src/core/storage/storage_client_pool.cpp
    src/core/storage/storage_client_factory.cpp
    src/core/storage/storage_connection_config.cpp
    src/core/storage/tcp_redis_hash_client.cpp
    src/core/storage/settlement_store_client_adapter.cpp
    src/core/storage/trading_domain_store_client_adapter.cpp
    src/core/storage/trading_ledger_store_client_adapter.cpp
    src/core/storage/timescale_buffered_event_store.cpp
    src/core/storage/timescale_event_store.cpp
    src/core/storage/timescale_event_store_client_adapter.cpp
    src/core/storage/timescale_sql_client.cpp
    src/core/market/kafka_market_bus_producer.cpp
    src/core/perf/object_pool.cpp
    src/core/perf/event_object_pool.cpp
    src/core/monitoring/metric_registry.cpp
    src/core/monitoring/exporter.cpp
    src/services/risk/basic_risk_engine.cpp
    src/services/risk/risk_policy_engine.cpp
    src/services/risk/self_trade_risk_engine.cpp
    src/risk/risk_rule_executor.cpp
    src/risk/risk_rule_registry.cpp
    src/risk/risk_manager.cpp
    src/services/order/execution_planner.cpp
    src/services/order/execution_engine.cpp
    src/services/order/order_manager.cpp
    src/services/order/execution_router.cpp
    src/services/order/in_memory_order_gateway.cpp
    src/services/order/order_state_machine.cpp
    src/services/settlement/settlement_price_provider.cpp
    src/services/settlement/settlement_query_client.cpp
    src/services/settlement/daily_settlement_service.cpp
    src/services/portfolio/ctp_account_ledger.cpp
    src/services/portfolio/ctp_position_ledger.cpp
    src/services/portfolio/position_manager.cpp
    src/services/portfolio/in_memory_portfolio_ledger.cpp
    src/services/market_state/bar_aggregator.cpp
    src/services/market_state/market_state_detector.cpp
    src/services/market_state/rule_market_state_engine.cpp
)

target_include_directories(quant_hft_core PUBLIC include)
target_compile_options(quant_hft_core PRIVATE -Wall -Wextra -Wpedantic)
target_link_libraries(quant_hft_core PUBLIC pthread dl)
set_target_properties(quant_hft_core PROPERTIES POSITION_INDEPENDENT_CODE ON)

if(QUANT_HFT_ENABLE_ARROW_PARQUET)
    pkg_check_modules(ARROW REQUIRED arrow>=12.0)
    pkg_check_modules(PARQUET REQUIRED parquet>=12.0)

    target_compile_definitions(quant_hft_core PUBLIC QUANT_HFT_ENABLE_ARROW_PARQUET=1)
    target_include_directories(quant_hft_core PUBLIC ${ARROW_INCLUDE_DIRS} ${PARQUET_INCLUDE_DIRS})
    target_link_libraries(quant_hft_core PUBLIC ${ARROW_LIBRARIES} ${PARQUET_LIBRARIES})
    target_link_directories(quant_hft_core PUBLIC ${ARROW_LIBRARY_DIRS} ${PARQUET_LIBRARY_DIRS})
else()
    target_compile_definitions(quant_hft_core PUBLIC QUANT_HFT_ENABLE_ARROW_PARQUET=0)
endif()

set(QUANT_HFT_METRICS_ACTIVE OFF)
if(QUANT_HFT_WITH_METRICS)
    find_package(PrometheusCpp 1.1 MODULE REQUIRED)
    set(QUANT_HFT_METRICS_ACTIVE ON)
    target_compile_definitions(quant_hft_core PUBLIC QUANT_HFT_WITH_METRICS=1)
    target_link_libraries(quant_hft_core PUBLIC PrometheusCpp::core PrometheusCpp::pull)
else()
    target_compile_definitions(quant_hft_core PUBLIC QUANT_HFT_WITH_METRICS=0)
endif()

if(QUANT_HFT_ENABLE_CTP_REAL_API)
    if(NOT EXISTS "${CTP_V6711_DIR}/ThostFtdcTraderApi.h")
        message(FATAL_ERROR "CTP headers not found under ${CTP_V6711_DIR}")
    endif()
    if(NOT EXISTS "${CTP_V6711_DIR}/thostmduserapi_se.so" OR
       NOT EXISTS "${CTP_V6711_DIR}/thosttraderapi_se.so")
        message(FATAL_ERROR "CTP .so files not found under ${CTP_V6711_DIR}")
    endif()

    add_library(ctp_md_api SHARED IMPORTED GLOBAL)
    set_target_properties(ctp_md_api PROPERTIES
        IMPORTED_LOCATION "${CTP_V6711_DIR}/thostmduserapi_se.so")

    add_library(ctp_trader_api SHARED IMPORTED GLOBAL)
    set_target_properties(ctp_trader_api PROPERTIES
        IMPORTED_LOCATION "${CTP_V6711_DIR}/thosttraderapi_se.so")

    target_compile_definitions(quant_hft_core PUBLIC QUANT_HFT_ENABLE_CTP_REAL_API=1)
    target_include_directories(quant_hft_core SYSTEM PUBLIC "${CTP_V6711_DIR}")
    target_link_libraries(quant_hft_core PUBLIC
        ctp_md_api
        ctp_trader_api
        dl
    )
    set_target_properties(quant_hft_core PROPERTIES BUILD_RPATH "${CTP_V6711_DIR}")
else()
    target_compile_definitions(quant_hft_core PUBLIC QUANT_HFT_ENABLE_CTP_REAL_API=0)
endif()

if(QUANT_HFT_ENABLE_REDIS_EXTERNAL OR QUANT_HFT_ENABLE_TIMESCALE_EXTERNAL)
    pkg_check_modules(OPENSSL_PC REQUIRED openssl>=1.1)
endif()

if(QUANT_HFT_ENABLE_REDIS_EXTERNAL)
    pkg_check_modules(HIREDIS REQUIRED hiredis>=1.1)
    target_compile_definitions(quant_hft_core PUBLIC QUANT_HFT_ENABLE_REDIS_EXTERNAL=1)
else()
    target_compile_definitions(quant_hft_core PUBLIC QUANT_HFT_ENABLE_REDIS_EXTERNAL=0)
endif()

if(QUANT_HFT_ENABLE_TIMESCALE_EXTERNAL)
    pkg_check_modules(LIBPQ REQUIRED libpq>=14)
    target_compile_definitions(quant_hft_core PUBLIC QUANT_HFT_ENABLE_TIMESCALE_EXTERNAL=1)
else()
    target_compile_definitions(quant_hft_core PUBLIC QUANT_HFT_ENABLE_TIMESCALE_EXTERNAL=0)
endif()

add_executable(core_engine src/apps/core_engine_main.cpp)
target_link_libraries(core_engine PRIVATE quant_hft_core)

add_executable(simnow_probe src/apps/simnow_probe_main.cpp)
target_link_libraries(simnow_probe PRIVATE quant_hft_core)

add_executable(wal_replay_tool src/apps/wal_replay_main.cpp)
target_link_libraries(wal_replay_tool PRIVATE quant_hft_core)

add_executable(hotpath_benchmark src/apps/hotpath_benchmark_main.cpp)
target_link_libraries(hotpath_benchmark PRIVATE quant_hft_core)

add_executable(hotpath_hybrid src/apps/hotpath_hybrid_main.cpp)
target_link_libraries(hotpath_hybrid PRIVATE quant_hft_core)

add_executable(daily_settlement src/apps/daily_settlement_main.cpp)
target_link_libraries(daily_settlement PRIVATE quant_hft_core)

add_executable(backtest_cli src/apps/backtest_cli_main.cpp)
target_link_libraries(backtest_cli PRIVATE quant_hft_core)

add_executable(backtest_consistency_cli src/apps/backtest_consistency_cli_main.cpp)
target_link_libraries(backtest_consistency_cli PRIVATE quant_hft_core)

add_executable(factor_eval_cli src/apps/factor_eval_cli_main.cpp)
target_link_libraries(factor_eval_cli PRIVATE quant_hft_core)

add_executable(backtest_benchmark_cli src/apps/backtest_benchmark_cli_main.cpp)
target_link_libraries(backtest_benchmark_cli PRIVATE quant_hft_core)

add_executable(csv_parquet_compare_cli src/apps/csv_parquet_compare_cli_main.cpp)
target_link_libraries(csv_parquet_compare_cli PRIVATE quant_hft_core)

add_executable(csv_to_parquet_cli src/apps/csv_to_parquet_cli_main.cpp)
target_link_libraries(csv_to_parquet_cli PRIVATE quant_hft_core)

add_executable(simnow_compare_cli src/apps/simnow_compare_cli_main.cpp)
target_link_libraries(simnow_compare_cli PRIVATE quant_hft_core)

add_executable(simnow_weekly_stress_cli src/apps/simnow_weekly_stress_cli_main.cpp)
target_link_libraries(simnow_weekly_stress_cli PRIVATE quant_hft_core)

add_executable(reconnect_evidence_cli src/apps/reconnect_evidence_cli_main.cpp)
target_link_libraries(reconnect_evidence_cli PRIVATE quant_hft_core)

add_executable(ops_health_report_cli src/apps/ops_health_report_cli_main.cpp)
target_link_libraries(ops_health_report_cli PRIVATE quant_hft_core)

add_executable(ops_alert_report_cli src/apps/ops_alert_report_cli_main.cpp)
target_link_libraries(ops_alert_report_cli PRIVATE quant_hft_core)

add_executable(ctp_cutover_orchestrator_cli src/apps/ctp_cutover_orchestrator_cli_main.cpp)
target_link_libraries(ctp_cutover_orchestrator_cli PRIVATE quant_hft_core)

add_executable(verify_contract_sync_cli src/apps/verify_contract_sync_cli_main.cpp)
target_link_libraries(verify_contract_sync_cli PRIVATE quant_hft_core)

add_executable(verify_develop_requirements_cli src/apps/verify_develop_requirements_cli_main.cpp)
target_link_libraries(verify_develop_requirements_cli PRIVATE quant_hft_core)

if(QUANT_HFT_BUILD_TESTS)
    include(CTest)
    enable_testing()

    include(FetchContent)
    set(FETCHCONTENT_QUIET OFF)
    FetchContent_Declare(
      googletest
      URL https://github.com/google/googletest/archive/refs/tags/v1.14.0.zip
      DOWNLOAD_EXTRACT_TIMESTAMP TRUE
    )
    set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
    FetchContent_MakeAvailable(googletest)

    add_executable(query_scheduler_test tests/unit/core/query_scheduler_test.cpp)
    target_link_libraries(query_scheduler_test PRIVATE quant_hft_core GTest::gtest_main)

    add_executable(ctp_config_test tests/unit/core/ctp_config_test.cpp)
    target_link_libraries(ctp_config_test PRIVATE quant_hft_core GTest::gtest_main)

    add_executable(ctp_config_loader_test tests/unit/core/ctp_config_loader_test.cpp)
    target_link_libraries(ctp_config_loader_test PRIVATE quant_hft_core GTest::gtest_main)

    add_executable(ctp_gateway_adapter_test tests/unit/core/ctp_gateway_adapter_test.cpp)
    target_link_libraries(ctp_gateway_adapter_test PRIVATE quant_hft_core GTest::gtest_main)

    add_executable(ctp_trader_adapter_test tests/unit/core/ctp_trader_adapter_test.cpp)
    target_link_libraries(ctp_trader_adapter_test PRIVATE quant_hft_core GTest::gtest_main)

    add_executable(ctp_md_adapter_test tests/unit/core/ctp_md_adapter_test.cpp)
    target_link_libraries(ctp_md_adapter_test PRIVATE quant_hft_core GTest::gtest_main)

    add_executable(wal_replay_loader_test tests/unit/core/wal_replay_loader_test.cpp)
    target_link_libraries(wal_replay_loader_test PRIVATE quant_hft_core GTest::gtest_main)

    add_executable(redis_realtime_store_test tests/unit/core/redis_realtime_store_test.cpp)
    target_link_libraries(redis_realtime_store_test PRIVATE quant_hft_core GTest::gtest_main)

    add_executable(timescale_event_store_test tests/unit/core/timescale_event_store_test.cpp)
    target_link_libraries(timescale_event_store_test PRIVATE quant_hft_core GTest::gtest_main)

    add_executable(redis_realtime_store_client_adapter_test tests/unit/core/redis_realtime_store_client_adapter_test.cpp)
    target_link_libraries(redis_realtime_store_client_adapter_test PRIVATE quant_hft_core GTest::gtest_main)

    add_executable(timescale_event_store_client_adapter_test tests/unit/core/timescale_event_store_client_adapter_test.cpp)
    target_link_libraries(timescale_event_store_client_adapter_test PRIVATE quant_hft_core GTest::gtest_main)

    add_executable(storage_client_pool_test tests/unit/core/storage_client_pool_test.cpp)
    target_link_libraries(storage_client_pool_test PRIVATE quant_hft_core GTest::gtest_main)

    add_executable(timescale_buffered_event_store_test tests/unit/core/timescale_buffered_event_store_test.cpp)
    target_link_libraries(timescale_buffered_event_store_test PRIVATE quant_hft_core GTest::gtest_main)

    add_executable(storage_client_factory_test tests/unit/core/storage_client_factory_test.cpp)
    target_link_libraries(storage_client_factory_test PRIVATE quant_hft_core GTest::gtest_main)

    add_executable(tcp_redis_hash_client_test tests/unit/core/tcp_redis_hash_client_test.cpp)
    target_link_libraries(tcp_redis_hash_client_test PRIVATE quant_hft_core GTest::gtest_main)

    add_executable(libpq_timescale_sql_client_test tests/unit/core/libpq_timescale_sql_client_test.cpp)
    target_link_libraries(libpq_timescale_sql_client_test PRIVATE quant_hft_core GTest::gtest_main)

    add_executable(trading_ledger_store_client_adapter_test tests/unit/core/trading_ledger_store_client_adapter_test.cpp)
    target_link_libraries(trading_ledger_store_client_adapter_test PRIVATE quant_hft_core GTest::gtest_main)

    add_executable(trading_domain_store_client_adapter_test tests/unit/core/trading_domain_store_client_adapter_test.cpp)
    target_link_libraries(trading_domain_store_client_adapter_test PRIVATE quant_hft_core GTest::gtest_main)

    add_executable(object_pool_test tests/unit/core/object_pool_test.cpp)
    target_link_libraries(object_pool_test PRIVATE quant_hft_core GTest::gtest_main)

    add_executable(event_object_pool_test tests/unit/core/event_object_pool_test.cpp)
    target_link_libraries(event_object_pool_test PRIVATE quant_hft_core GTest::gtest_main)

    add_executable(market_bus_producer_test tests/unit/core/market_bus_producer_test.cpp)
    target_link_libraries(market_bus_producer_test PRIVATE quant_hft_core GTest::gtest_main)

    add_executable(event_dispatcher_test tests/unit/core/event_dispatcher_test.cpp)
    target_link_libraries(event_dispatcher_test PRIVATE quant_hft_core GTest::gtest_main)

    add_executable(callback_dispatcher_test tests/unit/core/callback_dispatcher_test.cpp)
    target_link_libraries(callback_dispatcher_test PRIVATE quant_hft_core GTest::gtest_main)

    add_executable(flow_controller_test tests/unit/core/flow_controller_test.cpp)
    target_link_libraries(flow_controller_test PRIVATE quant_hft_core GTest::gtest_main)

    add_executable(circuit_breaker_test tests/unit/core/circuit_breaker_test.cpp)
    target_link_libraries(circuit_breaker_test PRIVATE quant_hft_core GTest::gtest_main)

    add_executable(fixed_decimal_test tests/unit/core/fixed_decimal_test.cpp)
    target_link_libraries(fixed_decimal_test PRIVATE quant_hft_core GTest::gtest_main)

    add_executable(basic_risk_engine_test tests/unit/services/basic_risk_engine_test.cpp)
    target_link_libraries(basic_risk_engine_test PRIVATE quant_hft_core GTest::gtest_main)

    add_executable(risk_policy_engine_test tests/unit/services/risk_policy_engine_test.cpp)
    target_link_libraries(risk_policy_engine_test PRIVATE quant_hft_core GTest::gtest_main)

    add_executable(in_memory_portfolio_ledger_test tests/unit/services/in_memory_portfolio_ledger_test.cpp)
    target_link_libraries(in_memory_portfolio_ledger_test PRIVATE quant_hft_core GTest::gtest_main)

    add_executable(ctp_position_ledger_test tests/unit/services/ctp_position_ledger_test.cpp)
    target_link_libraries(ctp_position_ledger_test PRIVATE quant_hft_core GTest::gtest_main)

    add_executable(ctp_account_ledger_test tests/unit/services/ctp_account_ledger_test.cpp)
    target_link_libraries(ctp_account_ledger_test PRIVATE quant_hft_core GTest::gtest_main)

    add_executable(self_trade_risk_engine_test tests/unit/services/self_trade_risk_engine_test.cpp)
    target_link_libraries(self_trade_risk_engine_test PRIVATE quant_hft_core GTest::gtest_main)

    add_executable(risk_rule_executor_test tests/unit/risk/risk_rule_executor_test.cpp)
    target_link_libraries(risk_rule_executor_test PRIVATE quant_hft_core GTest::gtest_main)

    add_executable(rate_limiter_test tests/unit/risk/rate_limiter_test.cpp)
    target_link_libraries(rate_limiter_test PRIVATE quant_hft_core GTest::gtest_main)

    add_executable(risk_manager_test tests/unit/risk/risk_manager_test.cpp)
    target_link_libraries(risk_manager_test PRIVATE quant_hft_core GTest::gtest_main)

    add_executable(bar_aggregator_test tests/unit/services/bar_aggregator_test.cpp)
    target_link_libraries(bar_aggregator_test PRIVATE quant_hft_core GTest::gtest_main)

    add_executable(order_state_machine_test tests/unit/services/order_state_machine_test.cpp)
    target_link_libraries(order_state_machine_test PRIVATE quant_hft_core GTest::gtest_main)

    add_executable(execution_planner_test tests/unit/services/execution_planner_test.cpp)
    target_link_libraries(execution_planner_test PRIVATE quant_hft_core GTest::gtest_main)

    add_executable(execution_router_test tests/unit/services/execution_router_test.cpp)
    target_link_libraries(execution_router_test PRIVATE quant_hft_core GTest::gtest_main)

    add_executable(execution_engine_test tests/unit/services/execution_engine_test.cpp)
    target_link_libraries(execution_engine_test PRIVATE quant_hft_core GTest::gtest_main)

    add_executable(order_manager_test tests/unit/services/order_manager_test.cpp)
    target_link_libraries(order_manager_test PRIVATE quant_hft_core GTest::gtest_main)

    add_executable(position_manager_test tests/unit/services/position_manager_test.cpp)
    target_link_libraries(position_manager_test PRIVATE quant_hft_core GTest::gtest_main)

    add_executable(settlement_query_client_test tests/unit/services/settlement_query_client_test.cpp)
    target_link_libraries(settlement_query_client_test PRIVATE quant_hft_core GTest::gtest_main)

    add_executable(settlement_price_provider_test tests/unit/services/settlement_price_provider_test.cpp)
    target_link_libraries(settlement_price_provider_test PRIVATE quant_hft_core GTest::gtest_main)

    add_executable(daily_settlement_service_test tests/unit/services/daily_settlement_service_test.cpp)
    target_link_libraries(daily_settlement_service_test PRIVATE quant_hft_core GTest::gtest_main)

    add_executable(rule_market_state_engine_test tests/unit/services/rule_market_state_engine_test.cpp)
    target_link_libraries(rule_market_state_engine_test PRIVATE quant_hft_core GTest::gtest_main)

    add_executable(market_state_detector_test tests/unit/services/market_state_detector_test.cpp)
    target_link_libraries(market_state_detector_test PRIVATE quant_hft_core GTest::gtest_main)

    add_executable(metric_registry_test tests/unit/monitoring/metric_registry_test.cpp)
    target_link_libraries(metric_registry_test PRIVATE quant_hft_core GTest::gtest_main)

    add_executable(parquet_data_feed_test tests/unit/backtest/parquet_data_feed_test.cpp)
    target_link_libraries(parquet_data_feed_test PRIVATE quant_hft_core GTest::gtest_main)

    add_executable(indicator_trace_parquet_writer_test
                   tests/unit/backtest/indicator_trace_parquet_writer_test.cpp)
    target_link_libraries(indicator_trace_parquet_writer_test PRIVATE quant_hft_core GTest::gtest_main)

    add_executable(sub_strategy_indicator_trace_parquet_writer_test
                   tests/unit/backtest/sub_strategy_indicator_trace_parquet_writer_test.cpp)
    target_link_libraries(sub_strategy_indicator_trace_parquet_writer_test
                          PRIVATE quant_hft_core GTest::gtest_main)

    add_executable(product_fee_config_loader_test
                   tests/unit/backtest/product_fee_config_loader_test.cpp)
    target_link_libraries(product_fee_config_loader_test PRIVATE quant_hft_core GTest::gtest_main)

    add_executable(backtest_data_feed_test tests/unit/backtest/backtest_data_feed_test.cpp)
    target_link_libraries(backtest_data_feed_test PRIVATE quant_hft_core GTest::gtest_main)

    add_executable(live_data_feed_test tests/unit/backtest/live_data_feed_test.cpp)
    target_link_libraries(live_data_feed_test PRIVATE quant_hft_core GTest::gtest_main)

    add_executable(broker_test tests/unit/backtest/broker_test.cpp)
    target_link_libraries(broker_test PRIVATE quant_hft_core GTest::gtest_main)

    add_executable(engine_test tests/unit/backtest/engine_test.cpp)
    target_link_libraries(engine_test PRIVATE quant_hft_core GTest::gtest_main)

    add_executable(performance_test tests/unit/backtest/performance_test.cpp)
    target_link_libraries(performance_test PRIVATE quant_hft_core GTest::gtest_main)

    add_executable(backtest_metrics_test tests/unit/apps/backtest_metrics_test.cpp)
    target_link_libraries(backtest_metrics_test PRIVATE quant_hft_core GTest::gtest_main)

    add_executable(base_strategy_test tests/unit/strategy/base_strategy_test.cpp)
    target_link_libraries(base_strategy_test PRIVATE quant_hft_core GTest::gtest_main)

    add_executable(atomic_strategy_interface_test tests/unit/strategy/atomic_strategy_interface_test.cpp)
    target_link_libraries(atomic_strategy_interface_test PRIVATE quant_hft_core GTest::gtest_main)

    add_executable(atomic_factory_test tests/unit/strategy/atomic_factory_test.cpp)
    target_link_libraries(atomic_factory_test PRIVATE quant_hft_core GTest::gtest_main)

    add_executable(strategy_registry_test tests/unit/strategy/strategy_registry_test.cpp)
    target_link_libraries(strategy_registry_test PRIVATE quant_hft_core GTest::gtest_main)

    add_executable(strategy_engine_test tests/unit/strategy/strategy_engine_test.cpp)
    target_link_libraries(strategy_engine_test PRIVATE quant_hft_core GTest::gtest_main)

    add_executable(demo_live_strategy_test tests/unit/strategy/demo_live_strategy_test.cpp)
    target_link_libraries(demo_live_strategy_test PRIVATE quant_hft_core GTest::gtest_main)

    add_executable(composite_config_loader_test tests/unit/strategy/composite_config_loader_test.cpp)
    target_link_libraries(composite_config_loader_test PRIVATE quant_hft_core GTest::gtest_main)

    add_executable(strategy_main_config_loader_test
                   tests/unit/strategy/strategy_main_config_loader_test.cpp)
    target_link_libraries(strategy_main_config_loader_test PRIVATE quant_hft_core GTest::gtest_main)

    add_executable(composite_strategy_test tests/unit/strategy/composite_strategy_test.cpp)
    target_link_libraries(composite_strategy_test PRIVATE quant_hft_core GTest::gtest_main)

    add_executable(atomic_strategies_test tests/unit/strategy/atomic_strategies_test.cpp)
    target_link_libraries(atomic_strategies_test PRIVATE quant_hft_core GTest::gtest_main)

    add_executable(sma_test tests/unit/indicators/sma_test.cpp)
    target_link_libraries(sma_test PRIVATE quant_hft_core GTest::gtest_main)

    add_executable(ema_test tests/unit/indicators/ema_test.cpp)
    target_link_libraries(ema_test PRIVATE quant_hft_core GTest::gtest_main)

    add_executable(atr_test tests/unit/indicators/atr_test.cpp)
    target_link_libraries(atr_test PRIVATE quant_hft_core GTest::gtest_main)

    add_executable(kama_test tests/unit/indicators/kama_test.cpp)
    target_link_libraries(kama_test PRIVATE quant_hft_core GTest::gtest_main)

    add_executable(adx_test tests/unit/indicators/adx_test.cpp)
    target_link_libraries(adx_test PRIVATE quant_hft_core GTest::gtest_main)

    add_executable(backtest_replay_support_test tests/unit/apps/backtest_replay_support_test.cpp)
    target_link_libraries(backtest_replay_support_test PRIVATE quant_hft_core GTest::gtest_main)

    add_executable(ops_cli_test tests/unit/apps/ops_cli_test.cpp)
    target_link_libraries(ops_cli_test PRIVATE GTest::gtest_main)
    target_compile_definitions(ops_cli_test PRIVATE QUANT_HFT_BUILD_DIR="${CMAKE_BINARY_DIR}")

    add_executable(quality_gate_scripts_test tests/unit/build/quality_gate_scripts_test.cpp)
    target_link_libraries(quality_gate_scripts_test PRIVATE GTest::gtest_main)

    add_executable(consistency_gate_script_test tests/unit/build/consistency_gate_script_test.cpp)
    target_link_libraries(consistency_gate_script_test PRIVATE GTest::gtest_main)

    add_executable(preprod_rehearsal_gate_script_test
                   tests/unit/build/preprod_rehearsal_gate_script_test.cpp)
    target_link_libraries(preprod_rehearsal_gate_script_test PRIVATE GTest::gtest_main)

    if(QUANT_HFT_METRICS_ACTIVE)
        add_executable(exporter_test tests/unit/monitoring/exporter_test.cpp)
        target_link_libraries(exporter_test PRIVATE quant_hft_core GTest::gtest_main)
    endif()

    include(GoogleTest)
    gtest_discover_tests(query_scheduler_test)
    gtest_discover_tests(ctp_config_test)
    gtest_discover_tests(ctp_config_loader_test)
    gtest_discover_tests(ctp_gateway_adapter_test)
    gtest_discover_tests(ctp_trader_adapter_test)
    gtest_discover_tests(ctp_md_adapter_test)
    gtest_discover_tests(wal_replay_loader_test)
    gtest_discover_tests(redis_realtime_store_test)
    gtest_discover_tests(timescale_event_store_test)
    gtest_discover_tests(redis_realtime_store_client_adapter_test)
    gtest_discover_tests(timescale_event_store_client_adapter_test)
    gtest_discover_tests(storage_client_pool_test)
    gtest_discover_tests(timescale_buffered_event_store_test)
    gtest_discover_tests(storage_client_factory_test)
    gtest_discover_tests(tcp_redis_hash_client_test)
    gtest_discover_tests(libpq_timescale_sql_client_test)
    gtest_discover_tests(trading_ledger_store_client_adapter_test)
    gtest_discover_tests(trading_domain_store_client_adapter_test)
    gtest_discover_tests(object_pool_test)
    gtest_discover_tests(event_object_pool_test)
    gtest_discover_tests(market_bus_producer_test)
    gtest_discover_tests(event_dispatcher_test)
    gtest_discover_tests(flow_controller_test)
    gtest_discover_tests(circuit_breaker_test)
    gtest_discover_tests(fixed_decimal_test)
    gtest_discover_tests(callback_dispatcher_test)
    gtest_discover_tests(basic_risk_engine_test)
    gtest_discover_tests(risk_policy_engine_test)
    gtest_discover_tests(in_memory_portfolio_ledger_test)
    gtest_discover_tests(ctp_position_ledger_test)
    gtest_discover_tests(ctp_account_ledger_test)
    gtest_discover_tests(self_trade_risk_engine_test)
    gtest_discover_tests(risk_rule_executor_test)
    gtest_discover_tests(rate_limiter_test)
    gtest_discover_tests(risk_manager_test)
    gtest_discover_tests(bar_aggregator_test)
    gtest_discover_tests(order_state_machine_test)
    gtest_discover_tests(execution_planner_test)
    gtest_discover_tests(execution_router_test)
    gtest_discover_tests(execution_engine_test)
    gtest_discover_tests(order_manager_test)
    gtest_discover_tests(position_manager_test)
    gtest_discover_tests(settlement_query_client_test)
    gtest_discover_tests(settlement_price_provider_test)
    gtest_discover_tests(daily_settlement_service_test)
    gtest_discover_tests(rule_market_state_engine_test)
    gtest_discover_tests(market_state_detector_test)
    gtest_discover_tests(metric_registry_test)
    gtest_discover_tests(parquet_data_feed_test)
    gtest_discover_tests(indicator_trace_parquet_writer_test)
    gtest_discover_tests(sub_strategy_indicator_trace_parquet_writer_test)
    gtest_discover_tests(product_fee_config_loader_test)
    gtest_discover_tests(backtest_data_feed_test)
    gtest_discover_tests(live_data_feed_test)
    gtest_discover_tests(broker_test)
    gtest_discover_tests(engine_test)
    gtest_discover_tests(performance_test)
    gtest_discover_tests(backtest_metrics_test)
    gtest_discover_tests(base_strategy_test)
    gtest_discover_tests(atomic_strategy_interface_test)
    gtest_discover_tests(atomic_factory_test)
    gtest_discover_tests(strategy_registry_test)
    gtest_discover_tests(strategy_engine_test)
    gtest_discover_tests(demo_live_strategy_test)
    gtest_discover_tests(composite_config_loader_test)
    gtest_discover_tests(strategy_main_config_loader_test)
    gtest_discover_tests(composite_strategy_test)
    gtest_discover_tests(atomic_strategies_test)
    gtest_discover_tests(sma_test)
    gtest_discover_tests(ema_test)
    gtest_discover_tests(atr_test)
    gtest_discover_tests(kama_test)
    gtest_discover_tests(adx_test)
    gtest_discover_tests(backtest_replay_support_test)
    gtest_discover_tests(ops_cli_test)
    gtest_discover_tests(quality_gate_scripts_test WORKING_DIRECTORY ${CMAKE_SOURCE_DIR})
    gtest_discover_tests(consistency_gate_script_test WORKING_DIRECTORY ${CMAKE_SOURCE_DIR})
    gtest_discover_tests(preprod_rehearsal_gate_script_test WORKING_DIRECTORY ${CMAKE_SOURCE_DIR})
    if(QUANT_HFT_METRICS_ACTIVE)
        gtest_discover_tests(exporter_test)
    endif()
endif()
