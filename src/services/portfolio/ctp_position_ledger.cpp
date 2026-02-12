#include "quant_hft/services/ctp_position_ledger.h"

#include <algorithm>
#include <cctype>
#include <cstdlib>

namespace quant_hft {

namespace {

std::string LowerAscii(std::string value) {
    std::transform(value.begin(),
                   value.end(),
                   value.begin(),
                   [](unsigned char ch) { return static_cast<char>(std::tolower(ch)); });
    return value;
}

}  // namespace

std::size_t CtpPositionLedger::PositionKeyHasher::operator()(
    const PositionKey& key) const {
    const auto h1 = std::hash<std::string>{}(key.account_id);
    const auto h2 = std::hash<std::string>{}(key.instrument_id);
    const auto h3 = std::hash<int>{}(static_cast<int>(key.direction));
    const auto h4 = std::hash<std::string>{}(key.position_date);
    return h1 ^ (h2 << 1) ^ (h3 << 2) ^ (h4 << 3);
}

bool CtpPositionLedger::ApplyInvestorPositionSnapshot(
    const InvestorPositionSnapshot& snapshot,
    std::string* error) {
    if (snapshot.account_id.empty() || snapshot.instrument_id.empty()) {
        if (error != nullptr) {
            *error = "snapshot account_id and instrument_id are required";
        }
        return false;
    }

    const auto direction = ParsePositionDirection(snapshot.posi_direction);
    const auto position_date = NormalizePositionDate(snapshot.position_date);
    const auto key =
        MakeKey(snapshot.account_id, snapshot.instrument_id, direction, position_date);

    PositionBucket bucket;
    bucket.position = ClampNonNegative(snapshot.position);
    const auto preferred_frozen =
        direction == PositionDirection::kLong ? snapshot.long_frozen : snapshot.short_frozen;
    const auto fallback_frozen = std::max(snapshot.long_frozen, snapshot.short_frozen);
    bucket.frozen = std::min(bucket.position,
                             ClampNonNegative(preferred_frozen > 0 ? preferred_frozen
                                                                   : fallback_frozen));
    bucket.last_update_ts_ns = snapshot.ts_ns;

    std::lock_guard<std::mutex> lock(mutex_);
    positions_[key] = bucket;
    if (error != nullptr) {
        error->clear();
    }
    return true;
}

bool CtpPositionLedger::RegisterOrderIntent(const CtpOrderIntentForLedger& intent,
                                            std::string* error) {
    if (intent.client_order_id.empty() || intent.account_id.empty() ||
        intent.instrument_id.empty() || intent.requested_volume <= 0) {
        if (error != nullptr) {
            *error = "intent fields are invalid";
        }
        return false;
    }

    PendingOrderState pending;
    pending.intent = intent;
    pending.position_date = ResolvePositionDateForIntent(intent);

    std::lock_guard<std::mutex> lock(mutex_);
    if (pending_orders_.find(intent.client_order_id) != pending_orders_.end()) {
        if (error != nullptr) {
            *error = "duplicate client_order_id";
        }
        return false;
    }

    if (IsCloseOffset(intent.offset)) {
        const auto key =
            MakeKey(intent.account_id, intent.instrument_id, intent.direction, pending.position_date);
        auto& bucket = positions_[key];
        const auto closable = std::max(0, bucket.position - bucket.frozen);
        if (closable < intent.requested_volume) {
            if (error != nullptr) {
                *error = "insufficient closable volume";
            }
            return false;
        }
        bucket.frozen += intent.requested_volume;
        bucket.last_update_ts_ns = NowEpochNanos();
        pending.frozen_volume = intent.requested_volume;
    }

    pending_orders_.emplace(intent.client_order_id, std::move(pending));
    if (error != nullptr) {
        error->clear();
    }
    return true;
}

bool CtpPositionLedger::ApplyOrderEvent(const OrderEvent& event, std::string* error) {
    if (event.client_order_id.empty()) {
        if (error != nullptr) {
            *error = "event.client_order_id is required";
        }
        return false;
    }

    std::lock_guard<std::mutex> lock(mutex_);
    auto pending_it = pending_orders_.find(event.client_order_id);
    if (pending_it == pending_orders_.end()) {
        if (error != nullptr) {
            *error = "order intent not registered";
        }
        return false;
    }

    auto& pending = pending_it->second;
    if (event.filled_volume < pending.last_filled_volume) {
        if (error != nullptr) {
            *error = "filled_volume cannot decrease";
        }
        return false;
    }

    const auto delta_filled = event.filled_volume - pending.last_filled_volume;
    const auto key = MakeKey(pending.intent.account_id,
                             pending.intent.instrument_id,
                             pending.intent.direction,
                             pending.position_date);
    auto& bucket = positions_[key];

    if (delta_filled > 0) {
        if (IsCloseOffset(pending.intent.offset)) {
            bucket.position = std::max(0, bucket.position - delta_filled);
            const auto release = std::min(delta_filled, pending.frozen_volume);
            pending.frozen_volume -= release;
            bucket.frozen = std::max(0, bucket.frozen - release);
        } else {
            bucket.position += delta_filled;
        }
        bucket.last_update_ts_ns = event.ts_ns;
    }
    pending.last_filled_volume = event.filled_volume;

    if (IsTerminalStatus(event.status)) {
        if (pending.frozen_volume > 0) {
            bucket.frozen = std::max(0, bucket.frozen - pending.frozen_volume);
            pending.frozen_volume = 0;
            bucket.last_update_ts_ns = event.ts_ns;
        }
        pending_orders_.erase(pending_it);
    }

    if (error != nullptr) {
        error->clear();
    }
    return true;
}

CtpPositionView CtpPositionLedger::GetPosition(const std::string& account_id,
                                               const std::string& instrument_id,
                                               PositionDirection direction,
                                               const std::string& position_date) const {
    CtpPositionView view;
    view.account_id = account_id;
    view.instrument_id = instrument_id;
    view.direction = direction;
    view.position_date = NormalizePositionDate(position_date);
    view.last_update_ts_ns = NowEpochNanos();

    const auto key = MakeKey(account_id, instrument_id, direction, view.position_date);
    std::lock_guard<std::mutex> lock(mutex_);
    const auto it = positions_.find(key);
    if (it == positions_.end()) {
        return view;
    }
    view.position = it->second.position;
    view.frozen = it->second.frozen;
    view.closable = std::max(0, it->second.position - it->second.frozen);
    view.last_update_ts_ns = it->second.last_update_ts_ns;
    return view;
}

std::int32_t CtpPositionLedger::GetClosableVolume(const std::string& account_id,
                                                  const std::string& instrument_id,
                                                  PositionDirection direction,
                                                  const std::string& position_date) const {
    const auto snapshot = GetPosition(account_id, instrument_id, direction, position_date);
    return snapshot.closable;
}

bool CtpPositionLedger::IsCloseOffset(OffsetFlag offset) {
    return offset == OffsetFlag::kClose || offset == OffsetFlag::kCloseToday ||
           offset == OffsetFlag::kCloseYesterday;
}

bool CtpPositionLedger::IsTerminalStatus(OrderStatus status) {
    return status == OrderStatus::kFilled || status == OrderStatus::kCanceled ||
           status == OrderStatus::kRejected;
}

std::string CtpPositionLedger::NormalizePositionDate(const std::string& raw) {
    if (raw.empty()) {
        return "today";
    }
    const auto normalized = LowerAscii(raw);
    if (normalized == "1" || normalized == "today" || normalized == "td") {
        return "today";
    }
    if (normalized == "2" || normalized == "yesterday" || normalized == "yd") {
        return "yesterday";
    }
    return normalized;
}

std::string CtpPositionLedger::ResolvePositionDateForIntent(
    const CtpOrderIntentForLedger& intent) {
    if (intent.offset == OffsetFlag::kCloseToday) {
        return "today";
    }
    if (intent.offset == OffsetFlag::kCloseYesterday) {
        return "yesterday";
    }
    return NormalizePositionDate(intent.position_date);
}

PositionDirection CtpPositionLedger::ParsePositionDirection(const std::string& raw) {
    const auto normalized = LowerAscii(raw);
    if (normalized == "2" || normalized == "long" || normalized == "l") {
        return PositionDirection::kLong;
    }
    return PositionDirection::kShort;
}

CtpPositionLedger::PositionKey CtpPositionLedger::MakeKey(const std::string& account_id,
                                                          const std::string& instrument_id,
                                                          PositionDirection direction,
                                                          const std::string& position_date) {
    PositionKey key;
    key.account_id = account_id;
    key.instrument_id = instrument_id;
    key.direction = direction;
    key.position_date = NormalizePositionDate(position_date);
    return key;
}

std::int32_t CtpPositionLedger::ClampNonNegative(std::int32_t value) {
    return value < 0 ? 0 : value;
}

}  // namespace quant_hft
