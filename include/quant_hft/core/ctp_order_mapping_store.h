#pragma once

#include <algorithm>
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
            by_exact_key_[BuildExactKey(mapping.account_id, mapping.trading_day, mapping.front_id,
                                        mapping.session_id, mapping.order_ref)] = mapping;
            by_exact_key_[BuildExactKey("", mapping.trading_day, mapping.front_id,
                                        mapping.session_id, mapping.order_ref)] = mapping;
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

    // Records the broker identity after an order callback has been attributed.  CTP trade
    // callbacks do not always carry FrontID/SessionID/OrderRef, while OrderSysID is stable within
    // (account, trading day, exchange).  Keeping this secondary index lets subsequent recovery
    // trades resolve without guessing from an OrderRef that may have been reused.
    void BindExchangeOrderId(const OrderEvent& event) {
        if (event.exchange_order_id.empty() || event.exchange_id.empty() ||
            event.trading_day.empty()) {
            return;
        }

        std::lock_guard<std::mutex> lock(mutex_);
        const auto client_it = by_client_order_id_.find(event.client_order_id);
        if (client_it == by_client_order_id_.end()) {
            return;
        }
        const auto& mapping = client_it->second;
        by_exchange_order_id_[BuildExchangeOrderKey(mapping.account_id, event.trading_day,
                                                    event.exchange_id, event.exchange_order_id)] =
            mapping;
        by_exchange_order_id_[BuildExchangeOrderKey("", event.trading_day, event.exchange_id,
                                                    event.exchange_order_id)] = mapping;
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

        if (event.front_id > 0 && event.session_id > 0) {
            auto exact_it = by_exact_key_.find(BuildExactKey(
                event.account_id, event.trading_day, event.front_id, event.session_id, order_ref));
            if (exact_it != by_exact_key_.end()) {
                Assign(mapping, exact_it->second);
                return true;
            }
            exact_it = by_exact_key_.find(
                BuildExactKey("", event.trading_day, event.front_id, event.session_id, order_ref));
            if (exact_it != by_exact_key_.end()) {
                Assign(mapping, exact_it->second);
                return true;
            }
        }

        if (!event.exchange_order_id.empty() && !event.exchange_id.empty() &&
            !event.trading_day.empty()) {
            auto exchange_it = by_exchange_order_id_.find(BuildExchangeOrderKey(
                event.account_id, event.trading_day, event.exchange_id, event.exchange_order_id));
            if (exchange_it != by_exchange_order_id_.end()) {
                Assign(mapping, exchange_it->second);
                return true;
            }
            exchange_it = by_exchange_order_id_.find(BuildExchangeOrderKey(
                "", event.trading_day, event.exchange_id, event.exchange_order_id));
            if (exchange_it != by_exchange_order_id_.end()) {
                Assign(mapping, exchange_it->second);
                return true;
            }
        }

        if (order_ref.empty()) {
            return false;
        }

        const auto fallback_it = by_order_ref_.find(order_ref);
        if (fallback_it == by_order_ref_.end()) {
            return false;
        }
        std::vector<const CtpOrderSubmitMapping*> candidates;
        for (const auto& candidate : fallback_it->second) {
            if (!event.trading_day.empty() && candidate.trading_day != event.trading_day) {
                continue;
            }
            if (!event.account_id.empty() && !candidate.account_id.empty() &&
                candidate.account_id != event.account_id) {
                continue;
            }
            if (!event.instrument_id.empty() && !candidate.instrument_id.empty() &&
                candidate.instrument_id != event.instrument_id) {
                continue;
            }
            if (!event.exchange_id.empty() && !candidate.exchange_id.empty() &&
                candidate.exchange_id != event.exchange_id) {
                continue;
            }
            if (candidate.side != event.side || candidate.offset != event.offset) {
                continue;
            }
            if (event.ts_ns > 0 && candidate.submit_ts_ns > event.ts_ns) {
                continue;
            }
            candidates.push_back(&candidate);
        }
        if (candidates.empty()) {
            return false;
        }
        // The OrderRef-only fallback is deliberately strict.  Picking the most recent candidate
        // can silently join an old callback to a new order after a restart or trading-day change.
        if (candidates.size() != 1U) {
            return false;
        }
        Assign(mapping, *candidates.front());
        return true;
    }

    bool EnrichOrderEvent(OrderEvent* event) {
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
        if (event->trading_day.empty()) {
            event->trading_day = mapping.trading_day;
        }
        if (event->total_volume <= 0) {
            event->total_volume = mapping.volume;
        }
        event->side = mapping.side;
        event->offset = mapping.offset;
        BindExchangeOrderId(*event);
        return true;
    }

   private:
    static std::string BuildExactKey(const std::string& account_id, const std::string& trading_day,
                                     std::int32_t front_id, std::int32_t session_id,
                                     const std::string& order_ref) {
        return account_id + "|" + trading_day + "|" + std::to_string(front_id) + "|" +
               std::to_string(session_id) + "|" + order_ref;
    }

    static std::string BuildExchangeOrderKey(const std::string& account_id,
                                             const std::string& trading_day,
                                             const std::string& exchange_id,
                                             const std::string& exchange_order_id) {
        return account_id + "|" + trading_day + "|" + exchange_id + "|" + exchange_order_id;
    }

    static void Assign(CtpOrderSubmitMapping* out, const CtpOrderSubmitMapping& value) {
        if (out != nullptr) {
            *out = value;
        }
    }

    mutable std::mutex mutex_;
    std::unordered_map<std::string, CtpOrderSubmitMapping> by_client_order_id_;
    std::unordered_map<std::string, CtpOrderSubmitMapping> by_exact_key_;
    std::unordered_map<std::string, CtpOrderSubmitMapping> by_exchange_order_id_;
    std::unordered_map<std::string, std::vector<CtpOrderSubmitMapping>> by_order_ref_;
};

}  // namespace quant_hft
