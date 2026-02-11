#include <chrono>
#include <cstdint>
#include <iostream>
#include <string>

#include "quant_hft/core/object_pool.h"

namespace {

std::uint64_t RunBaseline(std::size_t iterations, std::size_t buffer_size) {
    const auto started = std::chrono::steady_clock::now();
    std::uint64_t checksum = 0;
    for (std::size_t i = 0; i < iterations; ++i) {
        std::vector<std::uint8_t> buffer(buffer_size, 0U);
        buffer[0] = static_cast<std::uint8_t>(i % 255U);
        buffer[buffer.size() - 1] = static_cast<std::uint8_t>((i + 1U) % 255U);
        checksum += static_cast<std::uint64_t>(buffer[0]) +
                    static_cast<std::uint64_t>(buffer[buffer.size() - 1]);
    }
    const auto ended = std::chrono::steady_clock::now();
    const auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(ended - started);
    return static_cast<std::uint64_t>(duration.count() + static_cast<long long>(checksum % 13U));
}

std::uint64_t RunPooled(std::size_t iterations,
                        std::size_t buffer_size,
                        std::size_t pool_capacity) {
    quant_hft::ObjectPool pool(pool_capacity, buffer_size);
    const auto started = std::chrono::steady_clock::now();
    std::uint64_t checksum = 0;
    for (std::size_t i = 0; i < iterations; ++i) {
        auto buffer = pool.Acquire();
        (*buffer)[0] = static_cast<std::uint8_t>(i % 255U);
        (*buffer)[buffer->size() - 1] = static_cast<std::uint8_t>((i + 1U) % 255U);
        checksum += static_cast<std::uint64_t>((*buffer)[0]) +
                    static_cast<std::uint64_t>((*buffer)[buffer->size() - 1]);
    }
    const auto ended = std::chrono::steady_clock::now();
    const auto duration = std::chrono::duration_cast<std::chrono::nanoseconds>(ended - started);
    return static_cast<std::uint64_t>(duration.count() + static_cast<long long>(checksum % 13U));
}

}  // namespace

int main(int argc, char** argv) {
    std::size_t iterations = 100000;
    std::size_t buffer_size = 256;
    std::size_t pool_capacity = 1024;

    for (int i = 1; i < argc; ++i) {
        const std::string arg = argv[i];
        if (arg == "--iterations" && i + 1 < argc) {
            iterations = static_cast<std::size_t>(std::stoull(argv[++i]));
        } else if (arg == "--buffer-size" && i + 1 < argc) {
            buffer_size = static_cast<std::size_t>(std::stoull(argv[++i]));
        } else if (arg == "--pool-capacity" && i + 1 < argc) {
            pool_capacity = static_cast<std::size_t>(std::stoull(argv[++i]));
        }
    }

    if (iterations == 0 || buffer_size == 0 || pool_capacity == 0) {
        std::cerr << "error=invalid_arguments" << std::endl;
        return 2;
    }

    const auto baseline_ns_total = RunBaseline(iterations, buffer_size);
    const auto pooled_ns_total = RunPooled(iterations, buffer_size, pool_capacity);
    const double baseline_ns_per_op = static_cast<double>(baseline_ns_total) /
                                      static_cast<double>(iterations);
    const double pooled_ns_per_op = static_cast<double>(pooled_ns_total) /
                                    static_cast<double>(iterations);

    std::cout << "iterations=" << iterations << "\n";
    std::cout << "buffer_size=" << buffer_size << "\n";
    std::cout << "pool_capacity=" << pool_capacity << "\n";
    std::cout << "baseline_ns_total=" << baseline_ns_total << "\n";
    std::cout << "pooled_ns_total=" << pooled_ns_total << "\n";
    std::cout << "baseline_ns_per_op=" << baseline_ns_per_op << "\n";
    std::cout << "pooled_ns_per_op=" << pooled_ns_per_op << "\n";
    std::cout << "status=ok" << "\n";
    return 0;
}
