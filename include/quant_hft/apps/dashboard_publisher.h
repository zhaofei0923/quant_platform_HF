#pragma once

#include <cstdint>
#include <memory>
#include <string>

namespace quant_hft::dashboard {

struct PublisherOptions {
    std::string source_dir;
    std::string identity_file;
    std::string output_dir;
    std::string state_dir;
    int retention_days{90};
    std::int64_t strategy_stale_after_ms{5'000};
};

// An independent observer: never creates CTP connections or writes trading state.
class Publisher {
   public:
    explicit Publisher(PublisherOptions options);
    ~Publisher();
    Publisher(const Publisher&) = delete;
    Publisher& operator=(const Publisher&) = delete;

    // The caller supplies wall time to make freshness and retention tests deterministic.
    bool PublishOnce(std::int64_t now_ms, std::string* error);

   private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace quant_hft::dashboard
