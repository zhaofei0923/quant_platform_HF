#pragma once

#include <memory>
#include <string>
#include <unordered_map>

#include "quant_hft/core/redis_hash_client.h"
#include "quant_hft/core/redis_realtime_store.h"
#include "quant_hft/core/storage_retry_policy.h"

namespace quant_hft {

class RedisRealtimeStoreClientAdapter : public IRealtimeCache {
public:
    RedisRealtimeStoreClientAdapter(std::shared_ptr<IRedisHashClient> client,
                                    StorageRetryPolicy retry_policy);

    void UpsertMarketSnapshot(const MarketSnapshot& snapshot) override;
    void UpsertOrderEvent(const OrderEvent& event) override;
    void UpsertPositionSnapshot(const PositionSnapshot& position) override;
    void UpsertStateSnapshot7D(const StateSnapshot7D& snapshot) override;

    bool GetMarketSnapshot(const std::string& instrument_id,
                           MarketSnapshot* out) const override;
    bool GetOrderEvent(const std::string& client_order_id,
                       OrderEvent* out) const override;
    bool GetPositionSnapshot(const std::string& account_id,
                             const std::string& instrument_id,
                             PositionDirection direction,
                             PositionSnapshot* out) const override;
    bool GetStateSnapshot7D(const std::string& instrument_id,
                            StateSnapshot7D* out) const override;

private:
    bool WriteWithRetry(const std::string& key,
                        const std::unordered_map<std::string, std::string>& fields) const;
    bool ExpireWithRetry(const std::string& key, int ttl_seconds) const;

    bool ReadHash(const std::string& key,
                  std::unordered_map<std::string, std::string>* out) const;

    static std::string OrderStatusToString(OrderStatus status);
    static bool ParseOrderStatus(const std::string& text, OrderStatus* out);
    static std::string PositionDirectionToString(PositionDirection direction);
    static bool ParsePositionDirection(const std::string& text,
                                       PositionDirection* out);

    static std::string ToString(std::int32_t value);
    static std::string ToString(std::int64_t value);
    static std::string ToString(double value);
    static bool ParseInt32(const std::unordered_map<std::string, std::string>& row,
                           const std::string& key,
                           std::int32_t* out);
    static bool ParseInt64(const std::unordered_map<std::string, std::string>& row,
                           const std::string& key,
                           std::int64_t* out);
    static bool ParseDouble(const std::unordered_map<std::string, std::string>& row,
                            const std::string& key,
                            double* out);
    static std::string GetOrEmpty(const std::unordered_map<std::string, std::string>& row,
                                  const std::string& key);

    std::shared_ptr<IRedisHashClient> client_;
    StorageRetryPolicy retry_policy_;
};

}  // namespace quant_hft
