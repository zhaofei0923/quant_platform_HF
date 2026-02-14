#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <fstream>
#include <iostream>
#include <mutex>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include "quant_hft/core/event_dispatcher.h"
#include "quant_hft/core/python_callback_dispatcher.h"

namespace {

using Clock = std::chrono::steady_clock;

std::int64_t NowNs() {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(Clock::now().time_since_epoch())
        .count();
}

std::size_t ReadRssKb() {
    std::ifstream status("/proc/self/status");
    if (!status.is_open()) {
        return 0;
    }
    std::string line;
    while (std::getline(status, line)) {
        if (line.rfind("VmRSS:", 0) == 0) {
            std::istringstream iss(line);
            std::string key;
            std::size_t value = 0;
            std::string unit;
            iss >> key >> value >> unit;
            return value;
        }
    }
    return 0;
}

double ComputeP99Ms(std::vector<std::int64_t> values_ms) {
    if (values_ms.empty()) {
        return 0.0;
    }
    std::sort(values_ms.begin(), values_ms.end());
    const auto idx = static_cast<std::size_t>(
        std::min<std::size_t>(values_ms.size() - 1, (values_ms.size() * 99) / 100));
    return static_cast<double>(values_ms[idx]);
}

std::string JsonEscape(const std::string& input) {
    std::string output;
    output.reserve(input.size() + 8);
    for (char ch : input) {
        if (ch == '\\' || ch == '"') {
            output.push_back('\\');
        }
        output.push_back(ch);
    }
    return output;
}

}  // namespace

int main(int argc, char** argv) {
    std::size_t tick_rate = 2000;
    std::size_t order_rate = 20;
    std::int64_t duration_sec = 60;
    std::size_t python_queue_size = 5000;
    std::string output_path = "stats.json";

    for (int i = 1; i < argc; ++i) {
        const std::string arg = argv[i];
        if (arg == "--tick-rate" && i + 1 < argc) {
            tick_rate = static_cast<std::size_t>(std::stoull(argv[++i]));
        } else if (arg == "--order-rate" && i + 1 < argc) {
            order_rate = static_cast<std::size_t>(std::stoull(argv[++i]));
        } else if (arg == "--duration" && i + 1 < argc) {
            duration_sec = std::max<std::int64_t>(1, std::stoll(argv[++i]));
        } else if (arg == "--python-queue-size" && i + 1 < argc) {
            python_queue_size = static_cast<std::size_t>(std::stoull(argv[++i]));
        } else if (arg == "--output" && i + 1 < argc) {
            output_path = argv[++i];
        }
    }

    quant_hft::EventDispatcher cpp_dispatcher(1, 10000, 20000);
    quant_hft::PythonCallbackDispatcher python_dispatcher(python_queue_size, 10, 100);
    cpp_dispatcher.Start();
    python_dispatcher.Start();

    const auto rss_start_kb = ReadRssKb();
    const auto started = Clock::now();
    const auto deadline = started + std::chrono::seconds(duration_sec);

    std::atomic<std::size_t> produced_ticks{0};
    std::atomic<std::size_t> produced_orders{0};
    std::atomic<std::size_t> python_order_post_failed{0};
    std::atomic<std::size_t> python_tick_post_failed{0};
    std::atomic<std::size_t> cpp_post_failed{0};

    std::vector<std::int64_t> order_delay_ms_samples;
    std::mutex order_delay_mutex;

    std::atomic<std::size_t> max_python_pending{0};
    std::atomic<std::size_t> max_cpp_pending{0};

    auto tick_producer = std::thread([&]() {
        const auto period = std::chrono::nanoseconds(
            tick_rate == 0 ? 0 : static_cast<std::int64_t>(1'000'000'000ULL / tick_rate));
        auto next_fire = Clock::now();
        while (Clock::now() < deadline) {
            if (tick_rate == 0) {
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
                continue;
            }
            next_fire += period;
            const auto enqueue_ns = NowNs();
            const bool ok = cpp_dispatcher.Post(
                [&, enqueue_ns]() {
                    const bool posted = python_dispatcher.Post(
                        [enqueue_ns]() {
                            const auto delay_ms = std::max<std::int64_t>(
                                0, (NowNs() - enqueue_ns) / 1'000'000);
                            if (delay_ms > 0) {
                                std::this_thread::sleep_for(std::chrono::microseconds(50));
                            }
                        },
                        false);
                    if (!posted) {
                        python_tick_post_failed.fetch_add(1, std::memory_order_relaxed);
                    }
                },
                quant_hft::EventPriority::kNormal);
            if (!ok) {
                cpp_post_failed.fetch_add(1, std::memory_order_relaxed);
            }
            produced_ticks.fetch_add(1, std::memory_order_relaxed);
            std::this_thread::sleep_until(next_fire);
        }
    });

    auto order_producer = std::thread([&]() {
        const auto period = std::chrono::nanoseconds(
            order_rate == 0 ? 0 : static_cast<std::int64_t>(1'000'000'000ULL / order_rate));
        auto next_fire = Clock::now();
        while (Clock::now() < deadline) {
            if (order_rate == 0) {
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
                continue;
            }
            next_fire += period;
            const auto enqueue_ns = NowNs();
            const bool ok = cpp_dispatcher.Post(
                [&, enqueue_ns]() {
                    const bool posted = python_dispatcher.Post(
                        [&, enqueue_ns]() {
                            const auto delay_ms = std::max<std::int64_t>(
                                0, (NowNs() - enqueue_ns) / 1'000'000);
                            {
                                std::lock_guard<std::mutex> lock(order_delay_mutex);
                                order_delay_ms_samples.push_back(delay_ms);
                            }
                            std::this_thread::sleep_for(std::chrono::milliseconds(1));
                        },
                        true);
                    if (!posted) {
                        python_order_post_failed.fetch_add(1, std::memory_order_relaxed);
                    }
                },
                quant_hft::EventPriority::kHigh);
            if (!ok) {
                cpp_post_failed.fetch_add(1, std::memory_order_relaxed);
            }
            produced_orders.fetch_add(1, std::memory_order_relaxed);
            std::this_thread::sleep_until(next_fire);
        }
    });

    auto sampler = std::thread([&]() {
        while (Clock::now() < deadline) {
            const auto py = python_dispatcher.GetStats();
            const auto cpp = cpp_dispatcher.GetStats();
            max_python_pending.store(
                std::max(max_python_pending.load(std::memory_order_relaxed), py.pending),
                std::memory_order_relaxed);
            max_cpp_pending.store(
                std::max(max_cpp_pending.load(std::memory_order_relaxed), cpp.total_pending),
                std::memory_order_relaxed);
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
    });

    tick_producer.join();
    order_producer.join();
    sampler.join();

    cpp_dispatcher.WaitUntilDrained(5000);
    cpp_dispatcher.Stop();

    const auto py_before_stop = python_dispatcher.GetStats();
    const auto cpp_final = cpp_dispatcher.GetStats();
    python_dispatcher.Stop();

    const auto rss_end_kb = ReadRssKb();
    const auto rss_growth_pct = rss_start_kb == 0
                                    ? 0.0
                                    : (static_cast<double>(rss_end_kb - rss_start_kb) * 100.0 /
                                       static_cast<double>(rss_start_kb));

    std::vector<std::int64_t> order_delays_copy;
    {
        std::lock_guard<std::mutex> lock(order_delay_mutex);
        order_delays_copy = order_delay_ms_samples;
    }
    const double order_p99_delay_ms = ComputeP99Ms(order_delays_copy);

    std::ofstream output(output_path, std::ios::trunc);
    if (!output.is_open()) {
        std::cerr << "failed to open output file: " << output_path << std::endl;
        return 2;
    }

    output << "{\n";
    output << "  \"status\": \"ok\",\n";
    output << "  \"duration_sec\": " << duration_sec << ",\n";
    output << "  \"tick_rate\": " << tick_rate << ",\n";
    output << "  \"order_rate\": " << order_rate << ",\n";
    output << "  \"python_queue_size\": " << python_queue_size << ",\n";
    output << "  \"produced_ticks\": " << produced_ticks.load(std::memory_order_relaxed) << ",\n";
    output << "  \"produced_orders\": " << produced_orders.load(std::memory_order_relaxed)
           << ",\n";
    output << "  \"python\": {\n";
    output << "    \"pending\": " << py_before_stop.pending << ",\n";
    output << "    \"max_pending_observed\": "
           << max_python_pending.load(std::memory_order_relaxed) << ",\n";
    output << "    \"dropped_total\": " << py_before_stop.dropped << ",\n";
    output << "    \"critical_timeout_total\": " << py_before_stop.critical_timeout << ",\n";
    output << "    \"critical_delay_exceeded_total\": "
           << py_before_stop.critical_delay_exceeded << ",\n";
    output << "    \"last_critical_queue_delay_ms\": "
           << py_before_stop.last_critical_queue_delay_ms << ",\n";
    output << "    \"order_post_failed_total\": "
           << python_order_post_failed.load(std::memory_order_relaxed) << ",\n";
    output << "    \"tick_post_failed_total\": "
           << python_tick_post_failed.load(std::memory_order_relaxed) << "\n";
    output << "  },\n";
    output << "  \"cpp\": {\n";
    output << "    \"total_pending\": " << cpp_final.total_pending << ",\n";
    output << "    \"max_pending_observed\": "
           << max_cpp_pending.load(std::memory_order_relaxed) << ",\n";
    output << "    \"dropped_total\": " << cpp_final.dropped_total << ",\n";
    output << "    \"post_failed_total\": " << cpp_post_failed.load(std::memory_order_relaxed)
           << "\n";
    output << "  },\n";
    output << "  \"order_callback\": {\n";
    output << "    \"samples\": " << order_delays_copy.size() << ",\n";
    output << "    \"p99_delay_ms\": " << order_p99_delay_ms << "\n";
    output << "  },\n";
    output << "  \"memory\": {\n";
    output << "    \"rss_start_kb\": " << rss_start_kb << ",\n";
    output << "    \"rss_end_kb\": " << rss_end_kb << ",\n";
    output << "    \"rss_growth_pct\": " << rss_growth_pct << "\n";
    output << "  }\n";
    output << "}\n";
    output.close();

    std::cout << output_path << std::endl;
    return 0;
}
