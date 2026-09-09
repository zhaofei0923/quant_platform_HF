#pragma once

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
    WalReceipt CommitOrderEvent(const OrderEvent& event) override;
    WalReceipt CommitTradeEvent(const OrderEvent& event) override;
    WalReceipt CommitCtpOrderSubmitMapping(const CtpOrderSubmitMapping& mapping) override;
    WalReceipt LastReceipt() const;
    std::string LastError() const;

    // Explicit dashboard opt-in. Grants group-read only when the already-open regular
    // WAL descriptor belongs to the named observer group. Failure leaves trading status
    // and LastError unchanged; default WAL creation remains 0600.
    bool EnableObserverReadAccess(const std::string& expected_group_name,
                                  std::string* error) noexcept;

   private:
    static std::string EscapeJsonString(const std::string& input);
    static std::string GetEnvOrEmpty(const char* name);
    WalReceipt Append(const char* kind, const char* event_type, const OrderEvent& event);
    WalReceipt AppendMapping(const CtpOrderSubmitMapping& mapping);
    WalReceipt CommitRecordLocked(const std::string& record);

    std::string wal_path_;
    std::string run_id_;
    mutable std::mutex mutex_;
    int fd_{-1};
    bool sync_directory_{false};
    std::string stream_id_;
    std::uint64_t first_sequence_{0};
    std::string error_;
    WalReceipt last_receipt_;
    std::uint64_t seq_{0};
};

}  // namespace quant_hft
