#include "quant_hft/core/structured_log.h"

#include <gtest/gtest.h>

#include <iostream>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

namespace quant_hft {
namespace {

TEST(StructuredLogTest, ConcurrentRecordsRemainCompleteSingleLines) {
    constexpr int kThreadCount = 8;
    constexpr int kRecordsPerThread = 100;
    std::ostringstream captured;
    auto* original = std::cerr.rdbuf(captured.rdbuf());

    std::vector<std::thread> writers;
    writers.reserve(kThreadCount);
    for (int worker = 0; worker < kThreadCount; ++worker) {
        writers.emplace_back([worker] {
            for (int record = 0; record < kRecordsPerThread; ++record) {
                EmitStructuredLog(nullptr, "concurrency_test", "info", "record",
                                  {{"worker", std::to_string(worker)},
                                   {"sequence", std::to_string(record)}});
            }
        });
    }
    for (auto& writer : writers) {
        writer.join();
    }
    std::cerr.rdbuf(original);

    std::istringstream lines(captured.str());
    std::string line;
    int line_count = 0;
    while (std::getline(lines, line)) {
        ++line_count;
        EXPECT_EQ(line.find("ts_ns="), 0U) << line;
        EXPECT_EQ(line.find("ts_ns=", 1), std::string::npos) << line;
        EXPECT_NE(line.find(" level=info app=concurrency_test event=record "),
                  std::string::npos)
            << line;
        EXPECT_NE(line.find("worker=\""), std::string::npos) << line;
        EXPECT_NE(line.find("sequence=\""), std::string::npos) << line;
    }
    EXPECT_EQ(line_count, kThreadCount * kRecordsPerThread);
}

}  // namespace
}  // namespace quant_hft
