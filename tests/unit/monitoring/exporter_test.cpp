#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <chrono>
#include <string>
#include <thread>

#include <gtest/gtest.h>

#include "quant_hft/monitoring/exporter.h"
#include "quant_hft/monitoring/metric_registry.h"

namespace quant_hft {
namespace {

std::string HttpGetMetrics(int port) {
    const int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        return "";
    }

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(static_cast<uint16_t>(port));
    inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);

    if (connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0) {
        close(fd);
        return "";
    }

    const std::string request =
        "GET /metrics HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n";
    (void)send(fd, request.c_str(), request.size(), 0);

    std::string response;
    char buffer[1024] = {0};
    while (true) {
        const auto n = recv(fd, buffer, sizeof(buffer), 0);
        if (n <= 0) {
            break;
        }
        response.append(buffer, buffer + n);
    }
    close(fd);
    return response;
}

TEST(ExporterTest, ExporterStartEndpointResponds) {
#if !QUANT_HFT_WITH_METRICS
    GTEST_SKIP() << "metrics is disabled at build time";
#else
    auto counter = MetricRegistry::Instance().BuildCounter(
        "quant_hft_exporter_test_total", "exporter test counter");
    counter->Increment();

    MetricsExporter exporter;
    std::string error;
    ASSERT_TRUE(exporter.Start(18080, &error)) << error;

    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    const auto response = HttpGetMetrics(18080);
    EXPECT_NE(response.find("200 OK"), std::string::npos);
    EXPECT_NE(response.find("quant_hft_exporter_test_total"), std::string::npos);

    exporter.Stop();
#endif
}

}  // namespace
}  // namespace quant_hft
