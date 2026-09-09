cmake_minimum_required(VERSION 3.20)
project(quant_platform_hf VERSION 0.1.0 LANGUAGES CXX)

list(APPEND CMAKE_MODULE_PATH "${CMAKE_SOURCE_DIR}/cmake")

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
if(QUANT_HFT_ENABLE_ARROW_PARQUET)
    message(FATAL_ERROR "Arrow/Parquet research moved to quant_research")
endif()
option(QUANT_HFT_WITH_METRICS "Enable Prometheus metrics exporter" OFF)
set(QUANT_HFT_SANITIZER "none" CACHE STRING "Runtime checks: none, address-undefined, or thread")
set_property(CACHE QUANT_HFT_SANITIZER PROPERTY STRINGS none address-undefined thread)
set(CTP_V6711_DIR "${CMAKE_SOURCE_DIR}/ctp_api/v6.7.11_20250617_api_traderapi_se_linux64"
    CACHE PATH "Path to CTP v6.7.11 API directory")

set(CMAKE_CXX_STANDARD 17)

set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

if(QUANT_HFT_ENABLE_REDIS_EXTERNAL OR
   QUANT_HFT_ENABLE_TIMESCALE_EXTERNAL)
    find_package(PkgConfig REQUIRED)
endif()

find_package(QuantStrategies 1.0.0 EXACT CONFIG REQUIRED)
include(cmake/VerifyStrategyPackage.cmake)
find_package(yaml-cpp CONFIG REQUIRED)
find_package(OpenSSL REQUIRED COMPONENTS Crypto)

# Shared header contracts; each implementation source compiles once.
add_library(quant_hft_contracts INTERFACE)
target_include_directories(quant_hft_contracts INTERFACE "${CMAKE_SOURCE_DIR}/include")
target_link_libraries(quant_hft_contracts INTERFACE Quant::Contracts)
add_library(quant_hft_build_options INTERFACE)
target_link_libraries(quant_hft_build_options INTERFACE quant_hft_contracts Quant::Contracts pthread dl)
if(QUANT_HFT_SANITIZER STREQUAL "address-undefined")
    target_compile_options(quant_hft_build_options INTERFACE
        -fsanitize=address,undefined -fno-omit-frame-pointer)
    target_link_options(quant_hft_build_options INTERFACE -fsanitize=address,undefined)
elseif(QUANT_HFT_SANITIZER STREQUAL "thread")
    target_compile_options(quant_hft_build_options INTERFACE
        -fsanitize=thread -fno-omit-frame-pointer)
    target_link_options(quant_hft_build_options INTERFACE -fsanitize=thread)
elseif(NOT QUANT_HFT_SANITIZER STREQUAL "none")
    message(FATAL_ERROR "Unsupported QUANT_HFT_SANITIZER: ${QUANT_HFT_SANITIZER}")
endif()

add_library(quant_hft_common STATIC
    src/core/host_adapters/flow_controller_wait.cpp
    src/core/host_adapters/market_bar_checkpoint_io.cpp
    src/core/host_adapters/dominant_contract_status_io.cpp
    src/core/host_adapters/instrument_meta_write_io.cpp
    src/core/host_adapters/runtime_semantics_ctp_validation.cpp

    src/core/common/callback_dispatcher.cpp

    src/core/common/event_dispatcher.cpp

    src/core/common/circuit_breaker.cpp

    src/core/ctp/ctp_config.cpp

    src/core/market/kafka_market_bus_producer.cpp
    src/core/perf/object_pool.cpp
    src/core/perf/event_object_pool.cpp
    src/core/monitoring/metric_registry.cpp
    src/core/monitoring/exporter.cpp
    src/core/monitoring/dashboard_snapshot.cpp
)

add_library(quant_hft_storage STATIC
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
)

add_library(quant_hft_wal_io STATIC
    src/core/regulatory/local_wal_regulatory_sink.cpp
    src/core/regulatory/wal_replay_loader.cpp
)

add_library(quant_hft_services STATIC
    src/services/portfolio/position_manager.cpp

    src/services/order/execution_engine.cpp

    src/services/order/in_memory_order_gateway.cpp

    src/services/order/pending_exit_store.cpp

    src/services/settlement/settlement_price_provider.cpp

    src/services/market_state/market_data_csv_recorder.cpp

)

add_library(quant_hft_strategy STATIC

    src/strategy/strategy_engine.cpp

    src/strategy/state_persistence.cpp

)

add_library(quant_hft_ctp STATIC
    src/core/ctp/ctp_trader_adapter.cpp
    src/core/ctp/ctp_md_adapter.cpp
    src/core/ctp/ctp_gateway_adapter.cpp
    src/core/ctp/query_scheduler.cpp
    src/core/runtime/durable_order_event_inbox.cpp
)

add_library(quant_hft_online_runtime STATIC
    src/core/ctp/ctp_config_loader.cpp
    src/services/settlement/settlement_query_client.cpp
    src/services/settlement/daily_settlement_service.cpp
    src/core/runtime/account_execution_scheduler.cpp
    src/core/runtime/runtime_identity.cpp
    src/core/runtime/runtime_paths.cpp
    src/core/regulatory/wal_projection_replay.cpp
)

set(QUANT_HFT_MODULES
    quant_hft_common quant_hft_storage quant_hft_wal_io quant_hft_services
    quant_hft_strategy quant_hft_ctp
     quant_hft_online_runtime)
foreach(module IN LISTS QUANT_HFT_MODULES)
    target_link_libraries(${module} PUBLIC quant_hft_build_options)
    target_compile_options(${module} PRIVATE -Wall -Wextra -Wpedantic)
    set_target_properties(${module} PROPERTIES POSITION_INDEPENDENT_CODE ON)
endforeach()
target_link_libraries(quant_hft_common PUBLIC Quant::Model Quant::Indicators)
add_library(quant_hft_deployment STATIC src/core/config/deployment_config.cpp)
target_compile_definitions(quant_hft_deployment PRIVATE
    QUANT_STRATEGIES_EXPECTED_MANIFEST_HASH="${QUANT_STRATEGIES_MANIFEST_SHA256}")
target_link_libraries(quant_hft_deployment PUBLIC quant_hft_build_options Quant::Strategies yaml-cpp OpenSSL::Crypto)
target_link_libraries(quant_hft_online_runtime PUBLIC quant_hft_deployment)
target_link_libraries(quant_hft_strategy PUBLIC Quant::Strategies)
target_link_libraries(quant_hft_storage PUBLIC quant_hft_common)
target_link_libraries(quant_hft_wal_io PUBLIC quant_hft_common)
target_link_libraries(quant_hft_services PUBLIC quant_hft_storage quant_hft_common)
target_link_libraries(quant_hft_strategy PUBLIC quant_hft_services quant_hft_common)
target_link_libraries(quant_hft_ctp PUBLIC quant_hft_wal_io quant_hft_common)

target_link_libraries(quant_hft_online_runtime PUBLIC quant_hft_services quant_hft_ctp quant_hft_wal_io)

# Source-compatible aggregate; no duplicate object compilation.
add_library(quant_hft_core INTERFACE)
target_link_libraries(quant_hft_core INTERFACE
    quant_hft_online_runtime quant_hft_ctp
    quant_hft_strategy quant_hft_storage)

target_compile_definitions(quant_hft_build_options INTERFACE QUANT_HFT_ENABLE_ARROW_PARQUET=0)

set(QUANT_HFT_METRICS_ACTIVE OFF)
if(QUANT_HFT_WITH_METRICS)
    find_package(PrometheusCpp 1.1 MODULE REQUIRED)
    set(QUANT_HFT_METRICS_ACTIVE ON)
    target_compile_definitions(quant_hft_build_options INTERFACE QUANT_HFT_WITH_METRICS=1)
    target_link_libraries(quant_hft_common PUBLIC PrometheusCpp::core PrometheusCpp::pull)
else()
    target_compile_definitions(quant_hft_build_options INTERFACE QUANT_HFT_WITH_METRICS=0)
endif()

if(QUANT_HFT_ENABLE_CTP_REAL_API)
    if(NOT EXISTS "${CTP_V6711_DIR}/ThostFtdcTraderApi.h")
        message(FATAL_ERROR "CTP headers not found under ${CTP_V6711_DIR}")
    endif()
    if(NOT EXISTS "${CTP_V6711_DIR}/thostmduserapi_se.so" OR
       NOT EXISTS "${CTP_V6711_DIR}/thosttraderapi_se.so")
        message(FATAL_ERROR "CTP .so files not found under ${CTP_V6711_DIR}")
    endif()

    target_compile_definitions(quant_hft_build_options INTERFACE QUANT_HFT_ENABLE_CTP_REAL_API=1)
    target_include_directories(quant_hft_ctp SYSTEM PUBLIC "${CTP_V6711_DIR}")
    target_link_directories(quant_hft_ctp PUBLIC "${CTP_V6711_DIR}")
    target_link_libraries(quant_hft_ctp PUBLIC
        "-Wl,-l:thostmduserapi_se.so"
        "-Wl,-l:thosttraderapi_se.so"
        dl
    )
    target_link_options(quant_hft_ctp PUBLIC "-Wl,-rpath,${CTP_V6711_DIR}")
else()
    target_compile_definitions(quant_hft_build_options INTERFACE QUANT_HFT_ENABLE_CTP_REAL_API=0)
endif()

if(QUANT_HFT_ENABLE_REDIS_EXTERNAL OR QUANT_HFT_ENABLE_TIMESCALE_EXTERNAL)
    pkg_check_modules(OPENSSL_PC REQUIRED openssl>=1.1)
endif()

if(QUANT_HFT_ENABLE_REDIS_EXTERNAL)
    pkg_check_modules(HIREDIS REQUIRED hiredis>=1.1)
    target_compile_definitions(quant_hft_build_options INTERFACE QUANT_HFT_ENABLE_REDIS_EXTERNAL=1)
else()
    target_compile_definitions(quant_hft_build_options INTERFACE QUANT_HFT_ENABLE_REDIS_EXTERNAL=0)
endif()

if(QUANT_HFT_ENABLE_TIMESCALE_EXTERNAL)
    pkg_check_modules(LIBPQ REQUIRED libpq>=14)
    target_compile_definitions(quant_hft_build_options INTERFACE QUANT_HFT_ENABLE_TIMESCALE_EXTERNAL=1)
else()
    target_compile_definitions(quant_hft_build_options INTERFACE QUANT_HFT_ENABLE_TIMESCALE_EXTERNAL=0)
endif()

add_executable(quant_config_cli src/apps/quant_config_cli_main.cpp)
target_link_libraries(quant_config_cli PRIVATE quant_hft_deployment)

add_library(quant_hft_state_migration STATIC src/core/config/strategy_state_migration.cpp)
target_link_libraries(quant_hft_state_migration PUBLIC quant_hft_strategy quant_hft_deployment)
add_executable(strategy_state_migrate_cli src/apps/strategy_state_migrate_cli_main.cpp)
target_link_libraries(strategy_state_migrate_cli PRIVATE quant_hft_state_migration)

add_executable(core_engine src/apps/core_engine_main.cpp)
target_link_libraries(core_engine PRIVATE quant_hft_online_runtime quant_hft_ctp quant_hft_strategy)

add_executable(runtime_paths_cli src/apps/runtime_paths_cli_main.cpp)
target_link_libraries(runtime_paths_cli PRIVATE quant_hft_online_runtime)

add_executable(simnow_accounting_policy_check_cli
    src/apps/simnow_accounting_policy_check_cli_main.cpp)
target_link_libraries(simnow_accounting_policy_check_cli PRIVATE quant_hft_services)

add_executable(simnow_probe src/apps/simnow_probe_main.cpp)
target_link_libraries(simnow_probe PRIVATE quant_hft_online_runtime quant_hft_ctp)

add_executable(simnow_flatten_positions src/apps/simnow_flatten_positions_cli_main.cpp)
target_link_libraries(simnow_flatten_positions PRIVATE quant_hft_online_runtime quant_hft_ctp)

add_executable(wal_replay_tool src/apps/wal_replay_main.cpp)
target_link_libraries(wal_replay_tool PRIVATE quant_hft_online_runtime)

add_executable(hotpath_benchmark src/apps/hotpath_benchmark_main.cpp)
target_link_libraries(hotpath_benchmark PRIVATE quant_hft_core)

add_executable(hotpath_hybrid src/apps/hotpath_hybrid_main.cpp)
target_link_libraries(hotpath_hybrid PRIVATE quant_hft_core)

add_executable(daily_settlement src/apps/daily_settlement_main.cpp)
target_link_libraries(daily_settlement PRIVATE quant_hft_online_runtime)

add_executable(simnow_wal_export_cli src/apps/simnow_wal_export_cli_main.cpp)
target_link_libraries(simnow_wal_export_cli PRIVATE quant_hft_core)

find_package(OpenSSL REQUIRED COMPONENTS Crypto)
add_executable(market_data_repair_cli src/apps/market_data_repair_cli_main.cpp)
target_link_libraries(market_data_repair_cli PRIVATE quant_hft_core OpenSSL::Crypto)

add_executable(simnow_dashboard_cli src/apps/simnow_dashboard_cli_main.cpp)
target_link_libraries(simnow_dashboard_cli PRIVATE quant_hft_core)

add_executable(dashboard_publish_cli
    src/apps/dashboard_publish_cli_main.cpp
    src/apps/dashboard_publisher.cpp)
target_link_libraries(dashboard_publish_cli PRIVATE quant_hft_contracts pthread)


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
get_filename_component(_quant_package_root "${QuantStrategies_DIR}/../../.." REALPATH)
target_compile_definitions(verify_contract_sync_cli PRIVATE
    QUANT_STRATEGIES_CPP_CONTRACT="${_quant_package_root}/include/quant_hft/contracts/types.h"
    QUANT_STRATEGIES_PROTO_CONTRACT="${QuantStrategies_DATA_DIR}/proto/quant_hft/v1/contracts.proto")

add_executable(verify_develop_requirements_cli src/apps/verify_develop_requirements_cli_main.cpp)
target_link_libraries(verify_develop_requirements_cli PRIVATE quant_hft_core)

if(QUANT_HFT_BUILD_TESTS)
    include(CTest)
    enable_testing()

    find_package(GTest CONFIG REQUIRED)
    add_library(quant_hft_test_bootstrap STATIC tests/host_test_main.cpp)
    target_link_libraries(quant_hft_test_bootstrap PUBLIC GTest::gtest Quant::Model)
    target_include_directories(quant_hft_test_bootstrap PRIVATE include)

    add_executable(durable_order_event_inbox_test tests/unit/core/durable_order_event_inbox_test.cpp)
    target_link_libraries(durable_order_event_inbox_test PRIVATE quant_hft_ctp quant_hft_test_bootstrap)

    add_executable(atomic_trade_postgres_test tests/integration/atomic_trade_postgres_test.cpp)
    target_link_libraries(atomic_trade_postgres_test PRIVATE quant_hft_core quant_hft_test_bootstrap)

    add_executable(position_reconciliation_test tests/unit/services/position_reconciliation_test.cpp)
    target_link_libraries(position_reconciliation_test PRIVATE quant_hft_core quant_hft_test_bootstrap)

    add_executable(query_scheduler_test tests/unit/core/query_scheduler_test.cpp)
    target_link_libraries(query_scheduler_test PRIVATE quant_hft_ctp quant_hft_test_bootstrap)

    add_executable(query_batch_collector_test tests/unit/core/query_batch_collector_test.cpp)
    target_link_libraries(query_batch_collector_test PRIVATE quant_hft_ctp quant_hft_test_bootstrap)

    add_executable(account_execution_scheduler_test tests/unit/core/account_execution_scheduler_test.cpp)
    target_link_libraries(account_execution_scheduler_test PRIVATE quant_hft_online_runtime quant_hft_test_bootstrap)
    add_executable(runtime_paths_test tests/unit/core/runtime_paths_test.cpp)
    target_link_libraries(runtime_paths_test PRIVATE quant_hft_online_runtime quant_hft_test_bootstrap)
    add_dependencies(runtime_paths_test runtime_paths_cli)

    add_executable(runtime_identity_test tests/unit/core/runtime_identity_test.cpp)
    target_link_libraries(runtime_identity_test PRIVATE quant_hft_online_runtime quant_hft_test_bootstrap)

    add_executable(ctp_config_test tests/unit/core/ctp_config_test.cpp)
    target_link_libraries(ctp_config_test PRIVATE quant_hft_core quant_hft_test_bootstrap)

    add_executable(ctp_config_loader_test tests/unit/core/ctp_config_loader_test.cpp)
    target_link_libraries(ctp_config_loader_test PRIVATE quant_hft_core quant_hft_test_bootstrap)

    add_executable(configured_contract_probe_test
        tests/unit/core/configured_contract_probe_test.cpp)
    target_link_libraries(configured_contract_probe_test PRIVATE quant_hft_core quant_hft_test_bootstrap)

    add_executable(instrument_meta_cache_test
        tests/unit/core/instrument_meta_cache_test.cpp)
    target_link_libraries(instrument_meta_cache_test PRIVATE quant_hft_core quant_hft_test_bootstrap)

    add_executable(ctp_gateway_adapter_test tests/unit/core/ctp_gateway_adapter_test.cpp)
    target_link_libraries(ctp_gateway_adapter_test PRIVATE quant_hft_ctp quant_hft_test_bootstrap)

    add_executable(ctp_trader_adapter_test tests/unit/core/ctp_trader_adapter_test.cpp)
    target_link_libraries(ctp_trader_adapter_test PRIVATE quant_hft_ctp quant_hft_test_bootstrap)

    add_executable(ctp_md_adapter_test tests/unit/core/ctp_md_adapter_test.cpp)
    target_link_libraries(ctp_md_adapter_test PRIVATE quant_hft_core quant_hft_test_bootstrap)

    add_executable(wal_replay_loader_test tests/unit/core/wal_replay_loader_test.cpp)
    target_link_libraries(wal_replay_loader_test PRIVATE quant_hft_online_runtime quant_hft_test_bootstrap)

    add_executable(redis_realtime_store_test tests/unit/core/redis_realtime_store_test.cpp)
    target_link_libraries(redis_realtime_store_test PRIVATE quant_hft_core quant_hft_test_bootstrap)

    add_executable(timescale_event_store_test tests/unit/core/timescale_event_store_test.cpp)
    target_link_libraries(timescale_event_store_test PRIVATE quant_hft_core quant_hft_test_bootstrap)

    add_executable(redis_realtime_store_client_adapter_test tests/unit/core/redis_realtime_store_client_adapter_test.cpp)
    target_link_libraries(redis_realtime_store_client_adapter_test PRIVATE quant_hft_core quant_hft_test_bootstrap)

    add_executable(timescale_event_store_client_adapter_test tests/unit/core/timescale_event_store_client_adapter_test.cpp)
    target_link_libraries(timescale_event_store_client_adapter_test PRIVATE quant_hft_core quant_hft_test_bootstrap)

    add_executable(storage_client_pool_test tests/unit/core/storage_client_pool_test.cpp)
    target_link_libraries(storage_client_pool_test PRIVATE quant_hft_core quant_hft_test_bootstrap)

    add_executable(timescale_buffered_event_store_test tests/unit/core/timescale_buffered_event_store_test.cpp)
    target_link_libraries(timescale_buffered_event_store_test PRIVATE quant_hft_core quant_hft_test_bootstrap)

    add_executable(storage_client_factory_test tests/unit/core/storage_client_factory_test.cpp)
    target_link_libraries(storage_client_factory_test PRIVATE quant_hft_core quant_hft_test_bootstrap)

    add_executable(tcp_redis_hash_client_test tests/unit/core/tcp_redis_hash_client_test.cpp)
    target_link_libraries(tcp_redis_hash_client_test PRIVATE quant_hft_core quant_hft_test_bootstrap)

    add_executable(libpq_timescale_sql_client_test tests/unit/core/libpq_timescale_sql_client_test.cpp)
    target_link_libraries(libpq_timescale_sql_client_test PRIVATE quant_hft_core quant_hft_test_bootstrap)

    add_executable(trading_ledger_store_client_adapter_test tests/unit/core/trading_ledger_store_client_adapter_test.cpp)
    target_link_libraries(trading_ledger_store_client_adapter_test PRIVATE quant_hft_core quant_hft_test_bootstrap)

    add_executable(trading_domain_store_client_adapter_test tests/unit/core/trading_domain_store_client_adapter_test.cpp)
    target_link_libraries(trading_domain_store_client_adapter_test PRIVATE quant_hft_storage quant_hft_test_bootstrap)

    add_executable(object_pool_test tests/unit/core/object_pool_test.cpp)
    target_link_libraries(object_pool_test PRIVATE quant_hft_core quant_hft_test_bootstrap)

    add_executable(event_object_pool_test tests/unit/core/event_object_pool_test.cpp)
    target_link_libraries(event_object_pool_test PRIVATE quant_hft_core quant_hft_test_bootstrap)

    add_executable(market_bus_producer_test tests/unit/core/market_bus_producer_test.cpp)
    target_link_libraries(market_bus_producer_test PRIVATE quant_hft_core quant_hft_test_bootstrap)

    add_executable(event_dispatcher_test tests/unit/core/event_dispatcher_test.cpp)
    target_link_libraries(event_dispatcher_test PRIVATE quant_hft_core quant_hft_test_bootstrap)

    add_executable(callback_dispatcher_test tests/unit/core/callback_dispatcher_test.cpp)
    target_link_libraries(callback_dispatcher_test PRIVATE quant_hft_core quant_hft_test_bootstrap)

    add_executable(flow_controller_test tests/unit/core/flow_controller_test.cpp)
    target_link_libraries(flow_controller_test PRIVATE quant_hft_core quant_hft_test_bootstrap)

    add_executable(circuit_breaker_test tests/unit/core/circuit_breaker_test.cpp)
    target_link_libraries(circuit_breaker_test PRIVATE quant_hft_core quant_hft_test_bootstrap)

    add_executable(fixed_decimal_test tests/unit/core/fixed_decimal_test.cpp)
    target_link_libraries(fixed_decimal_test PRIVATE quant_hft_core quant_hft_test_bootstrap)

    add_executable(pending_exit_store_test tests/unit/services/pending_exit_store_test.cpp)
    target_link_libraries(pending_exit_store_test PRIVATE quant_hft_core quant_hft_test_bootstrap)

    add_executable(market_data_csv_recorder_test tests/unit/services/market_data_csv_recorder_test.cpp)
    target_link_libraries(market_data_csv_recorder_test PRIVATE quant_hft_core quant_hft_test_bootstrap)

    add_executable(execution_engine_test tests/unit/services/execution_engine_test.cpp)
    target_link_libraries(execution_engine_test PRIVATE quant_hft_services quant_hft_ctp quant_hft_test_bootstrap)

    add_executable(order_manager_test tests/unit/services/order_manager_test.cpp)
    target_link_libraries(order_manager_test PRIVATE quant_hft_core quant_hft_test_bootstrap)

    add_executable(position_manager_test tests/unit/services/position_manager_test.cpp)
    target_link_libraries(position_manager_test PRIVATE quant_hft_core quant_hft_test_bootstrap)

    add_executable(settlement_query_client_test tests/unit/services/settlement_query_client_test.cpp)
    target_link_libraries(settlement_query_client_test PRIVATE quant_hft_core quant_hft_test_bootstrap)

    add_executable(settlement_price_provider_test tests/unit/services/settlement_price_provider_test.cpp)
    target_link_libraries(settlement_price_provider_test PRIVATE quant_hft_core quant_hft_test_bootstrap)

    add_executable(daily_settlement_service_test tests/unit/services/daily_settlement_service_test.cpp)
    target_link_libraries(daily_settlement_service_test PRIVATE quant_hft_core quant_hft_test_bootstrap)

    add_executable(rule_market_state_engine_test tests/unit/services/rule_market_state_engine_test.cpp)
    target_link_libraries(rule_market_state_engine_test PRIVATE quant_hft_core quant_hft_test_bootstrap)

    add_executable(metric_registry_test tests/unit/monitoring/metric_registry_test.cpp)
    target_link_libraries(metric_registry_test PRIVATE quant_hft_core quant_hft_test_bootstrap)

    add_executable(dashboard_snapshot_test tests/unit/monitoring/dashboard_snapshot_test.cpp)
    target_link_libraries(dashboard_snapshot_test PRIVATE quant_hft_core quant_hft_test_bootstrap)

    add_executable(dashboard_wal_validation_test tests/unit/apps/dashboard_wal_validation_test.cpp)
    target_link_libraries(dashboard_wal_validation_test PRIVATE quant_hft_core quant_hft_test_bootstrap)

    add_executable(dashboard_wal_permissions_test tests/unit/monitoring/dashboard_wal_permissions_test.cpp)
    target_link_libraries(dashboard_wal_permissions_test PRIVATE quant_hft_core quant_hft_test_bootstrap)

    add_executable(dashboard_publish_cli_test
        tests/unit/apps/dashboard_publish_cli_test.cpp
        src/apps/dashboard_publisher.cpp)
    target_link_libraries(dashboard_publish_cli_test PRIVATE quant_hft_contracts pthread)
    add_test(NAME dashboard_publish_cli_test COMMAND dashboard_publish_cli_test)
    add_test(NAME dashboard_deployment_test
             COMMAND bash ${CMAKE_SOURCE_DIR}/tests/unit/build/dashboard_deployment_test.sh)
    set_tests_properties(dashboard_deployment_test PROPERTIES SKIP_RETURN_CODE 77)
    add_test(NAME dashboard_permissions_test
             COMMAND bash ${CMAKE_SOURCE_DIR}/tests/unit/build/dashboard_permissions_test.sh)
    set_tests_properties(dashboard_permissions_test PROPERTIES SKIP_RETURN_CODE 77)
    set_tests_properties(dashboard_permissions_test PROPERTIES
        ENVIRONMENT "QUANT_STRATEGIES_PREFIX=${_quant_package_root}")
    add_test(NAME dashboard_session_lifetime_test
             COMMAND bash ${CMAKE_SOURCE_DIR}/tests/unit/build/dashboard_session_lifetime_test.sh)
    set_tests_properties(dashboard_session_lifetime_test PROPERTIES SKIP_RETURN_CODE 77 RUN_SERIAL TRUE)

    add_executable(strategy_engine_test tests/unit/strategy/strategy_engine_test.cpp)
    target_link_libraries(strategy_engine_test PRIVATE quant_hft_core quant_hft_test_bootstrap)

    add_executable(state_persistence_test tests/unit/strategy/state_persistence_test.cpp)
    target_link_libraries(state_persistence_test PRIVATE quant_hft_core quant_hft_test_bootstrap)

    add_executable(cli_support_test tests/unit/apps/cli_support_test.cpp)
    target_link_libraries(cli_support_test PRIVATE quant_hft_core quant_hft_test_bootstrap)

    add_executable(ops_cli_test tests/unit/apps/ops_cli_test.cpp)
    target_link_libraries(ops_cli_test PRIVATE quant_hft_test_bootstrap)
    target_compile_definitions(ops_cli_test PRIVATE QUANT_HFT_BUILD_DIR="${CMAKE_BINARY_DIR}")
    target_compile_definitions(ops_cli_test PRIVATE
        QUANT_STRATEGIES_CPP_CONTRACT="${_quant_package_root}/include/quant_hft/contracts/types.h"
        QUANT_STRATEGIES_PROTO_CONTRACT="${QuantStrategies_DATA_DIR}/proto/quant_hft/v1/contracts.proto")
    add_dependencies(ops_cli_test market_data_repair_cli)

    add_executable(simnow_dashboard_cli_test tests/unit/apps/simnow_dashboard_cli_test.cpp)
    target_link_libraries(simnow_dashboard_cli_test PRIVATE quant_hft_test_bootstrap)
    target_compile_definitions(simnow_dashboard_cli_test PRIVATE
                               QUANT_HFT_BUILD_DIR="${CMAKE_BINARY_DIR}")

    add_executable(simnow_wal_export_cli_test tests/unit/apps/simnow_wal_export_cli_test.cpp)
    target_link_libraries(simnow_wal_export_cli_test PRIVATE quant_hft_test_bootstrap)
    target_compile_definitions(simnow_wal_export_cli_test PRIVATE
                               QUANT_HFT_BUILD_DIR="${CMAKE_BINARY_DIR}")
    add_dependencies(simnow_wal_export_cli_test simnow_wal_export_cli)

    add_executable(quality_gate_scripts_test tests/unit/build/quality_gate_scripts_test.cpp)
    target_link_libraries(quality_gate_scripts_test PRIVATE quant_hft_test_bootstrap)

    add_executable(simnow_supervisor_script_test
                   tests/unit/build/simnow_supervisor_script_test.cpp)
    target_link_libraries(simnow_supervisor_script_test PRIVATE quant_hft_test_bootstrap)
    add_dependencies(simnow_supervisor_script_test runtime_paths_cli)

    add_executable(simnow_preflight_script_test
                   tests/unit/build/simnow_preflight_script_test.cpp)
    target_link_libraries(simnow_preflight_script_test PRIVATE quant_hft_test_bootstrap)

    add_test(NAME simnow_initial_balance_gate_shell_test
             COMMAND bash ${CMAKE_SOURCE_DIR}/tests/unit/build/simnow_initial_balance_gate_test.sh)
    set_tests_properties(simnow_initial_balance_gate_shell_test
                         PROPERTIES WORKING_DIRECTORY ${CMAKE_SOURCE_DIR})
    add_test(NAME tencent_simnow_schedule_shell_test
             COMMAND bash ${CMAKE_SOURCE_DIR}/tests/unit/build/tencent_simnow_schedule_script_test.sh)
    set_tests_properties(tencent_simnow_schedule_shell_test
                         PROPERTIES WORKING_DIRECTORY ${CMAKE_SOURCE_DIR})

    if(QUANT_HFT_METRICS_ACTIVE)
        add_executable(exporter_test tests/unit/monitoring/exporter_test.cpp)
        target_link_libraries(exporter_test PRIVATE quant_hft_core quant_hft_test_bootstrap)
    endif()

    add_executable(deployment_config_test tests/unit/core/deployment_config_test.cpp)
    target_link_libraries(deployment_config_test PRIVATE quant_hft_deployment quant_hft_test_bootstrap)
    add_executable(formal_connection_tools_test tests/unit/build/formal_connection_tools_test.cpp)
    target_link_libraries(formal_connection_tools_test PRIVATE quant_hft_test_bootstrap)
    add_dependencies(formal_connection_tools_test simnow_probe daily_settlement simnow_flatten_positions)
    add_executable(strategy_state_migration_test tests/unit/core/strategy_state_migration_test.cpp)
    target_link_libraries(strategy_state_migration_test PRIVATE quant_hft_state_migration quant_hft_test_bootstrap)
    add_executable(runtime_semantics_defaults_test tests/unit/core/runtime_semantics_defaults_test.cpp)
    target_link_libraries(runtime_semantics_defaults_test PRIVATE quant_hft_core quant_hft_test_bootstrap)

    get_property(quant_hft_test_targets DIRECTORY PROPERTY BUILDSYSTEM_TARGETS)
    foreach(test_target IN LISTS quant_hft_test_targets)
        if(test_target MATCHES "_test$")
            target_link_libraries(${test_target} PRIVATE quant_hft_test_bootstrap Quant::Contracts)
            target_compile_definitions(${test_target} PRIVATE
                QUANT_HFT_SOURCE_DIR="${CMAKE_SOURCE_DIR}"
                QUANT_HFT_BUILD_DIR="${CMAKE_BINARY_DIR}")
        endif()
    endforeach()

    include(GoogleTest)
    gtest_discover_tests(deployment_config_test WORKING_DIRECTORY ${CMAKE_SOURCE_DIR})
    gtest_discover_tests(formal_connection_tools_test WORKING_DIRECTORY ${CMAKE_SOURCE_DIR})
    gtest_discover_tests(strategy_state_migration_test WORKING_DIRECTORY ${CMAKE_SOURCE_DIR})
    gtest_discover_tests(runtime_semantics_defaults_test WORKING_DIRECTORY ${CMAKE_SOURCE_DIR})
    gtest_discover_tests(durable_order_event_inbox_test WORKING_DIRECTORY ${CMAKE_SOURCE_DIR})
    gtest_discover_tests(atomic_trade_postgres_test WORKING_DIRECTORY ${CMAKE_SOURCE_DIR})

    gtest_discover_tests(position_reconciliation_test WORKING_DIRECTORY ${CMAKE_SOURCE_DIR})
    gtest_discover_tests(query_scheduler_test WORKING_DIRECTORY ${CMAKE_SOURCE_DIR})
    gtest_discover_tests(query_batch_collector_test WORKING_DIRECTORY ${CMAKE_SOURCE_DIR})

    gtest_discover_tests(account_execution_scheduler_test WORKING_DIRECTORY ${CMAKE_SOURCE_DIR})
    gtest_discover_tests(runtime_paths_test WORKING_DIRECTORY ${CMAKE_SOURCE_DIR})
    gtest_discover_tests(runtime_identity_test WORKING_DIRECTORY ${CMAKE_SOURCE_DIR})
    gtest_discover_tests(ctp_config_test WORKING_DIRECTORY ${CMAKE_SOURCE_DIR})
    gtest_discover_tests(ctp_config_loader_test WORKING_DIRECTORY ${CMAKE_SOURCE_DIR})
    gtest_discover_tests(configured_contract_probe_test WORKING_DIRECTORY ${CMAKE_SOURCE_DIR})
    gtest_discover_tests(instrument_meta_cache_test WORKING_DIRECTORY ${CMAKE_SOURCE_DIR})
    gtest_discover_tests(ctp_gateway_adapter_test WORKING_DIRECTORY ${CMAKE_SOURCE_DIR})
    gtest_discover_tests(ctp_trader_adapter_test WORKING_DIRECTORY ${CMAKE_SOURCE_DIR})
    gtest_discover_tests(ctp_md_adapter_test WORKING_DIRECTORY ${CMAKE_SOURCE_DIR})
    gtest_discover_tests(wal_replay_loader_test WORKING_DIRECTORY ${CMAKE_SOURCE_DIR})
    gtest_discover_tests(redis_realtime_store_test WORKING_DIRECTORY ${CMAKE_SOURCE_DIR})
    gtest_discover_tests(timescale_event_store_test WORKING_DIRECTORY ${CMAKE_SOURCE_DIR})
    gtest_discover_tests(redis_realtime_store_client_adapter_test WORKING_DIRECTORY ${CMAKE_SOURCE_DIR})
    gtest_discover_tests(timescale_event_store_client_adapter_test WORKING_DIRECTORY ${CMAKE_SOURCE_DIR})
    gtest_discover_tests(storage_client_pool_test WORKING_DIRECTORY ${CMAKE_SOURCE_DIR})
    gtest_discover_tests(timescale_buffered_event_store_test WORKING_DIRECTORY ${CMAKE_SOURCE_DIR})
    gtest_discover_tests(storage_client_factory_test WORKING_DIRECTORY ${CMAKE_SOURCE_DIR})
    gtest_discover_tests(tcp_redis_hash_client_test WORKING_DIRECTORY ${CMAKE_SOURCE_DIR})
    gtest_discover_tests(libpq_timescale_sql_client_test WORKING_DIRECTORY ${CMAKE_SOURCE_DIR})
    gtest_discover_tests(trading_ledger_store_client_adapter_test WORKING_DIRECTORY ${CMAKE_SOURCE_DIR})
    gtest_discover_tests(trading_domain_store_client_adapter_test WORKING_DIRECTORY ${CMAKE_SOURCE_DIR})
    gtest_discover_tests(object_pool_test WORKING_DIRECTORY ${CMAKE_SOURCE_DIR})
    gtest_discover_tests(event_object_pool_test WORKING_DIRECTORY ${CMAKE_SOURCE_DIR})
    gtest_discover_tests(market_bus_producer_test WORKING_DIRECTORY ${CMAKE_SOURCE_DIR})
    gtest_discover_tests(event_dispatcher_test WORKING_DIRECTORY ${CMAKE_SOURCE_DIR})
    gtest_discover_tests(flow_controller_test WORKING_DIRECTORY ${CMAKE_SOURCE_DIR})
    gtest_discover_tests(circuit_breaker_test WORKING_DIRECTORY ${CMAKE_SOURCE_DIR})
    gtest_discover_tests(fixed_decimal_test WORKING_DIRECTORY ${CMAKE_SOURCE_DIR})
    gtest_discover_tests(callback_dispatcher_test WORKING_DIRECTORY ${CMAKE_SOURCE_DIR})

    gtest_discover_tests(pending_exit_store_test WORKING_DIRECTORY ${CMAKE_SOURCE_DIR})

    gtest_discover_tests(market_data_csv_recorder_test WORKING_DIRECTORY ${CMAKE_SOURCE_DIR})

    gtest_discover_tests(execution_engine_test WORKING_DIRECTORY ${CMAKE_SOURCE_DIR})
    gtest_discover_tests(order_manager_test WORKING_DIRECTORY ${CMAKE_SOURCE_DIR})
    gtest_discover_tests(position_manager_test WORKING_DIRECTORY ${CMAKE_SOURCE_DIR})
    gtest_discover_tests(settlement_query_client_test WORKING_DIRECTORY ${CMAKE_SOURCE_DIR})
    gtest_discover_tests(settlement_price_provider_test WORKING_DIRECTORY ${CMAKE_SOURCE_DIR})
    gtest_discover_tests(daily_settlement_service_test WORKING_DIRECTORY ${CMAKE_SOURCE_DIR})
    gtest_discover_tests(rule_market_state_engine_test WORKING_DIRECTORY ${CMAKE_SOURCE_DIR})

    gtest_discover_tests(metric_registry_test WORKING_DIRECTORY ${CMAKE_SOURCE_DIR})
    gtest_discover_tests(dashboard_snapshot_test WORKING_DIRECTORY ${CMAKE_SOURCE_DIR})
    gtest_discover_tests(dashboard_wal_validation_test WORKING_DIRECTORY ${CMAKE_SOURCE_DIR})
    gtest_discover_tests(dashboard_wal_permissions_test WORKING_DIRECTORY ${CMAKE_SOURCE_DIR})

    gtest_discover_tests(strategy_engine_test WORKING_DIRECTORY ${CMAKE_SOURCE_DIR})

    gtest_discover_tests(state_persistence_test WORKING_DIRECTORY ${CMAKE_SOURCE_DIR})

    gtest_discover_tests(cli_support_test WORKING_DIRECTORY ${CMAKE_SOURCE_DIR})

    gtest_discover_tests(ops_cli_test WORKING_DIRECTORY ${CMAKE_SOURCE_DIR})
    gtest_discover_tests(simnow_dashboard_cli_test WORKING_DIRECTORY ${CMAKE_SOURCE_DIR})
    gtest_discover_tests(simnow_wal_export_cli_test WORKING_DIRECTORY ${CMAKE_SOURCE_DIR})
    gtest_discover_tests(quality_gate_scripts_test WORKING_DIRECTORY ${CMAKE_SOURCE_DIR})

    gtest_discover_tests(simnow_supervisor_script_test WORKING_DIRECTORY ${CMAKE_SOURCE_DIR})
    # Preflight intentionally checks host-wide SimNow processes. It must not overlap
    # supervisor fixtures that start real test processes in other CTest cases.
    gtest_discover_tests(simnow_preflight_script_test WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
                         PROPERTIES RUN_SERIAL TRUE)

    if(QUANT_HFT_METRICS_ACTIVE)
        gtest_discover_tests(exporter_test WORKING_DIRECTORY ${CMAKE_SOURCE_DIR})
    endif()
endif()
