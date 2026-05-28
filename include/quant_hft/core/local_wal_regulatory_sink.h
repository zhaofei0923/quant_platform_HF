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
    bool AppendCtpOrderSubmitMapping(const CtpOrderSubmitMapping& mapping) override;
    bool Flush() override;

   private:
    static std::string EscapeJsonString(const std::string& input);
    static std::string GetEnvOrEmpty(const char* name);
    std::uint64_t ComputeNextSeq() const;
    bool Append(const char* kind, const char* event_type, const OrderEvent& event);
    bool AppendMapping(const CtpOrderSubmitMapping& mapping);

    std::string wal_path_;
    std::string run_id_;
    mutable std::mutex mutex_;
    std::ofstream stream_;
    std::uint64_t seq_{0};
};

}  // namespace quant_hft
