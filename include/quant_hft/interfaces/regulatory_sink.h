#pragma once

#include "quant_hft/contracts/types.h"
#include "quant_hft/contracts/wal_receipt.h"

namespace quant_hft {

class IRegulatorySink {
   public:
    virtual ~IRegulatorySink() = default;
    virtual bool AppendOrderEvent(const OrderEvent& event) = 0;
    virtual bool AppendTradeEvent(const OrderEvent& event) = 0;
    virtual bool AppendCtpOrderSubmitMapping(const CtpOrderSubmitMapping& mapping) = 0;
    virtual bool Flush() = 0;
    // Legacy implementations must not manufacture a durable receipt from a bool append.
    virtual WalReceipt CommitOrderEvent(const OrderEvent&) {
        WalReceipt receipt;
        receipt.error = "durable order commit unsupported";
        return receipt;
    }
    virtual WalReceipt CommitTradeEvent(const OrderEvent&) {
        WalReceipt receipt;
        receipt.error = "durable trade commit unsupported";
        return receipt;
    }
    virtual WalReceipt CommitCtpOrderSubmitMapping(const CtpOrderSubmitMapping&) {
        WalReceipt receipt;
        receipt.error = "durable mapping commit unsupported";
        return receipt;
    }
};

}  // namespace quant_hft
