#pragma once

#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "quant_hft/contracts/types.h"

namespace quant_hft {

class CtpOrderMappingStore {
   public:
    void Upsert(const CtpOrderSubmitMapping& mapping) {
        if (mapping.client_order_id.empty() || mapping.order_ref.empty()) {
            return;
        }

        std::lock_guard<std::mutex> lock(mutex_);
        by_client_order_id_[mapping.client_order_id] = mapping;

        if (mapping.front_id > 0 && mapping.session_id > 0) {
            by_exact_key_[BuildExactKey(mapping.account_id, mapping.front_id, mapping.session_id,
                                        mapping.order_ref)] = mapping;
            by_exact_key_[BuildExactKey("", mapping.front_id, mapping.session_id,
                                        mapping.order_ref)] = mapping;
        }

        auto& rows = by_order_ref_[mapping.order_ref];
        for (auto& row : rows) {
            if (row.client_order_id == mapping.client_order_id) {
                row = mapping;
                return;
            }
        }
        rows.push_back(mapping);
    }

    bool Resolve(const OrderEvent& event, CtpOrderSubmitMapping* mapping) const {
        std::lock_guard<std::mutex> lock(mutex_);
        if (!event.client_order_id.empty()) {
            const auto client_it = by_client_order_id_.find(event.client_order_id);
            if (client_it != by_client_order_id_.end()) {
                Assign(mapping, client_it->second);
                return true;
            }
        }

        const std::string order_ref =
            !event.order_ref.empty() ? event.order_ref : event.client_order_id;
        if (order_ref.empty()) {
            return false;
        }

        if (event.front_id > 0 && event.session_id > 0) {
            auto exact_it = by_exact_key_.find(
                BuildExactKey(event.account_id, event.front_id, event.session_id, order_ref));
            if (exact_it != by_exact_key_.end()) {
                Assign(mapping, exact_it->second);
                return true;
            }
            exact_it =
                by_exact_key_.find(BuildExactKey("", event.front_id, event.session_id, order_ref));
            if (exact_it != by_exact_key_.end()) {
                Assign(mapping, exact_it->second);
                return true;
            }
        }

        const auto fallback_it = by_order_ref_.find(order_ref);
        if (fallback_it == by_order_ref_.end() || fallback_it->second.size() != 1U) {
            return false;
        }
        Assign(mapping, fallback_it->second.front());
        return true;
    }

    bool EnrichOrderEvent(OrderEvent* event) const {
        if (event == nullptr) {
            return false;
        }
        CtpOrderSubmitMapping mapping;
        if (!Resolve(*event, &mapping)) {
            return false;
        }

        event->client_order_id = mapping.client_order_id;
        if (event->account_id.empty()) {
            event->account_id = mapping.account_id;
        }
        if (event->strategy_id.empty()) {
            event->strategy_id = mapping.strategy_id;
        }
        if (event->trace_id.empty()) {
            event->trace_id = mapping.trace_id;
        }
        if (event->instrument_id.empty()) {
            event->instrument_id = mapping.instrument_id;
        }
        if (event->exchange_id.empty()) {
            event->exchange_id = mapping.exchange_id;
        }
        if (event->total_volume <= 0) {
            event->total_volume = mapping.volume;
        }
        event->side = mapping.side;
        event->offset = mapping.offset;
        return true;
    }

   private:
    static std::string BuildExactKey(const std::string& account_id, std::int32_t front_id,
                                     std::int32_t session_id, const std::string& order_ref) {
        return account_id + "|" + std::to_string(front_id) + "|" + std::to_string(session_id) +
               "|" + order_ref;
    }

    static void Assign(CtpOrderSubmitMapping* out, const CtpOrderSubmitMapping& value) {
        if (out != nullptr) {
            *out = value;
        }
    }

    mutable std::mutex mutex_;
    std::unordered_map<std::string, CtpOrderSubmitMapping> by_client_order_id_;
    std::unordered_map<std::string, CtpOrderSubmitMapping> by_exact_key_;
    std::unordered_map<std::string, std::vector<CtpOrderSubmitMapping>> by_order_ref_;
};

}  // namespace quant_hft
