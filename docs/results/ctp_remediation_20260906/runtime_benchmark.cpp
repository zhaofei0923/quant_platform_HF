// Offline fixed-load probe. This measures only scheduler -> synchronous WAL -> validated scan.
#include <sys/resource.h>

#include <algorithm>
#include <chrono>
#include <cmath>
#include <filesystem>
#include <iostream>
#include <stdexcept>
#include <string>
#include <vector>

#include "quant_hft/core/local_wal_regulatory_sink.h"
#include "quant_hft/core/wal_replay_loader.h"
#include "quant_hft/runtime/account_execution_scheduler.h"

int main(int argc, char** argv) {
    using namespace quant_hft;
    using Clock = std::chrono::steady_clock;
    if (argc != 3) return 2;
    const std::size_t count = std::stoull(argv[1]);
    if (!count || count > 100000 || std::filesystem::exists(argv[2])) return 2;
    std::vector<double> commit_us(count), late_us(count);
    AccountExecutionScheduler scheduler(count + 8, 1);
    if (!scheduler.Start()) return 3;
    const auto deadline = Clock::now() + std::chrono::seconds(2);
    Clock::time_point first, last;
    std::size_t high_water = 0;
    {
        LocalWalRegulatorySink sink(argv[2]);
        for (std::size_t index = 0; index < count; ++index) {
            const auto result = scheduler.SubmitAt("TEST_ACCOUNT", deadline, [&, index] {
                const auto begin = Clock::now();
                if (!index) first = begin;
                OrderEvent event;
                event.account_id = "TEST_ACCOUNT";
                event.broker_id = "TEST_BROKER";
                event.strategy_id = "benchmark";
                event.instrument_id = "SHFE.rbTEST";
                event.exchange_id = "SHFE";
                event.client_order_id = "benchmark-" + std::to_string(index);
                event.trading_day = "20260904";
                event.event_source = "offline_benchmark";
                event.ts_ns = 1788483600000000000LL + static_cast<EpochNanos>(index);
                event.total_volume = 1;
                event.avg_fill_price = 100.0;
                const auto receipt = sink.CommitOrderEvent(event);
                if (!receipt.durable || receipt.sequence != receipt.first_sequence + index) {
                    throw std::runtime_error("WAL commit failed or out of order");
                }
                last = Clock::now();
                commit_us[index] = std::chrono::duration<double, std::micro>(last - begin).count();
                late_us[index] =
                    std::chrono::duration<double, std::micro>(begin - deadline).count();
            });
            if (!result) return 4;
            high_water = std::max(high_water, scheduler.GetStats().pending);
        }
        if (!scheduler.Drain(120000)) {
            std::cerr << "drain timed out\n";
            return 5;
        }
        const auto stats = scheduler.GetStats();
        if (stats.failed || stats.completed != count || stats.rejected) {
            std::cerr << "scheduler failed: " << stats.last_error
                      << " completed=" << stats.completed << '\n';
            return 6;
        }
    }
    scheduler.Stop();
    const auto replay_start = Clock::now();
    const auto replay = WalReplayLoader().VisitValidated(
        argv[2], [](const auto& record) { return record.receipt.durable; });
    const auto replay_ms =
        std::chrono::duration<double, std::milli>(Clock::now() - replay_start).count();
    if (!replay.validation.valid || !replay.completed || replay.records_visited != count) {
        std::cerr << "validation failed: " << replay.error << " visited=" << replay.records_visited
                  << '\n';
        return 7;
    }
    std::sort(commit_us.begin(), commit_us.end());
    std::sort(late_us.begin(), late_us.end());
    const auto p99 = static_cast<std::size_t>(std::ceil(count * 0.99)) - 1;
    const auto elapsed = std::chrono::duration<double>(last - first).count();
    rusage usage{};
    getrusage(RUSAGE_SELF, &usage);
    std::cout << "{\"records\":" << count << ",\"scheduler_workers\":1,\"same_deadline_burst\":true"
              << ",\"commit_throughput_per_second\":" << count / elapsed
              << ",\"wal_commit_p99_us\":" << commit_us[p99]
              << ",\"queue_high_water\":" << high_water
              << ",\"deadline_lateness_p99_us\":" << late_us[p99]
              << ",\"validated_scan_ms\":" << replay_ms
              << ",\"domain_recovery_measured\":false,\"peak_rss_kib\":" << usage.ru_maxrss
              << ",\"wal_bytes\":" << std::filesystem::file_size(argv[2]) << "}\n";
}
