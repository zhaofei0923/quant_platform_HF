#include "quant_hft/strategy/atomic_factory.h"

namespace quant_hft {

AtomicFactory& AtomicFactory::Instance() {
    static AtomicFactory factory;
    return factory;
}

bool AtomicFactory::Register(const std::string& type, Creator creator, std::string* error) {
    if (type.empty()) {
        if (error != nullptr) {
            *error = "atomic strategy type must not be empty";
        }
        return false;
    }
    if (!creator) {
        if (error != nullptr) {
            *error = "atomic strategy registration requires a valid creator";
        }
        return false;
    }

    std::lock_guard<std::mutex> lock(mutex_);
    const auto [it, inserted] = creators_.emplace(type, std::move(creator));
    if (!inserted) {
        if (error != nullptr) {
            *error = "atomic strategy type already registered: " + type;
        }
        return false;
    }
    return true;
}

std::unique_ptr<IAtomicStrategy> AtomicFactory::Create(const std::string& type, std::string* error,
                                                       const std::string& strategy_id) const {
    std::lock_guard<std::mutex> lock(mutex_);
    const auto it = creators_.find(type);
    if (it == creators_.end()) {
        if (error != nullptr) {
            *error = "unknown atomic strategy type '" + type + "'";
            if (!strategy_id.empty()) {
                *error += " for strategy id '" + strategy_id + "'";
            }
        }
        return nullptr;
    }
    return it->second();
}

bool AtomicFactory::Has(const std::string& type) const {
    std::lock_guard<std::mutex> lock(mutex_);
    return creators_.find(type) != creators_.end();
}

}  // namespace quant_hft
