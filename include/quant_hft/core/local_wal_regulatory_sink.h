#pragma once

#include <fstream>
#include <mutex>
#include <string>

#include "quant_hft/interfaces/regulatory_sink.h"

namespace quant_hft {

class LocalWalRegulatorySink : public IRegulatorySink {
public:
    explicit LocalWalRegulatorySink(std::string wal_path);
    ~LocalWalRegulatorySink() override;

    bool AppendOrderEvent(const OrderEvent& event) override;
    bool AppendTradeEvent(const OrderEvent& event) override;
    bool Flush() override;

private:
    static std::string EscapeJsonString(const std::string& input);
    std::uint64_t ComputeNextSeq() const;
    bool Append(const char* kind, const OrderEvent& event);

    std::string wal_path_;
    mutable std::mutex mutex_;
    std::ofstream stream_;
    std::uint64_t seq_{0};
};

}  // namespace quant_hft
