#include "quant_hft/core/host_adapters/host_clock.h"
#include <chrono>
#include <csignal>
#include <iostream>
#include <stdexcept>
#include <string>
#include <thread>

#include "quant_hft/apps/dashboard_publisher.h"

namespace {
volatile std::sig_atomic_t stop_requested = 0;
void Stop(int) { stop_requested = 1; }
std::int64_t Integer(const std::string& value) {
    std::size_t end = 0;
    const auto number = std::stoll(value, &end);
    if (end != value.size() || number < 0 || number > 3600000)
        throw std::runtime_error("invalid numeric option");
    return number;
}
}  // namespace

int main(int argc, char** argv) {
    quant_hft::BindOnlineHostClocks();
    quant_hft::dashboard::PublisherOptions options;
    int watch_seconds = 1;
    try {
        for (int i = 1; i < argc; ++i) {
            const std::string key = argv[i];
            if (key == "--help") {
                std::cout << "dashboard_publish_cli --source-dir ROOT --identity-file FILE "
                             "--output-dir DIR --state-dir DIR [--watch-seconds 1] "
                             "[--retention-days 90] [--strategy-stale-after-ms 5000]\n";
                return 0;
            }
            if (++i == argc) throw std::runtime_error("missing argument value");
            const std::string value = argv[i];
            if (key == "--source-dir")
                options.source_dir = value;
            else if (key == "--identity-file")
                options.identity_file = value;
            else if (key == "--output-dir")
                options.output_dir = value;
            else if (key == "--state-dir")
                options.state_dir = value;
            else if (key == "--watch-seconds")
                watch_seconds = static_cast<int>(Integer(value));
            else if (key == "--retention-days")
                options.retention_days = static_cast<int>(Integer(value));
            else if (key == "--strategy-stale-after-ms")
                options.strategy_stale_after_ms = Integer(value);
            else
                throw std::runtime_error("unknown option");
        }
        if (watch_seconds < 0 || watch_seconds > 3600)
            throw std::runtime_error("invalid watch interval");
        std::signal(SIGINT, Stop);
        std::signal(SIGTERM, Stop);
        quant_hft::dashboard::Publisher publisher(options);
        do {
            const auto now = std::chrono::duration_cast<std::chrono::milliseconds>(
                                 std::chrono::system_clock::now().time_since_epoch())
                                 .count();
            std::string error;
            if (!publisher.PublishOnce(now, &error)) {
                // Diagnostics are fixed codes, never copied source content or account values.
                std::cerr << "dashboard_publish_cli: " << error << '\n';
                return 1;
            }
            for (int tick = 0; tick < watch_seconds * 10 && !stop_requested; ++tick)
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
        } while (watch_seconds > 0 && !stop_requested);
    } catch (...) {
        std::cerr << "dashboard_publish_cli: invalid configuration or unavailable observer state\n";
        return 2;
    }
    return 0;
}
