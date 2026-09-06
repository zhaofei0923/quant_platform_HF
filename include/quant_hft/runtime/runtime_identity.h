#pragma once

#include <string>
#include <vector>

namespace quant_hft {

struct RuntimeIdentity {
    std::string environment;
    std::string broker_id;
    std::string account_id;
    std::string instance{"default"};

    std::string Namespace() const;
    std::string Manifest() const;
    bool Validate(std::string* error) const;
};

// Owns the process lock for a stable recovery directory and any explicit external artifacts.
// A new run ID never changes this identity. Nonempty legacy state requires explicit migration.
class RuntimeDirectory {
   public:
    RuntimeDirectory() = default;
    ~RuntimeDirectory();
    RuntimeDirectory(const RuntimeDirectory&) = delete;
    RuntimeDirectory& operator=(const RuntimeDirectory&) = delete;

    bool Acquire(const std::string& runtime_root, const RuntimeIdentity& identity,
                 std::string* error);
    bool BindArtifact(const std::string& path, bool directory, std::string* error);
    std::string Path(const std::string& relative) const;
    const std::string& root() const { return root_; }
    const RuntimeIdentity& identity() const { return identity_; }

   private:
    bool BindManifest(const std::string& manifest_path, const std::string& lock_path,
                      bool has_unmanaged_data, std::string* error);
    RuntimeIdentity identity_;
    std::string root_;
    std::vector<int> locks_;
};

// The old SimNow override is accepted only for SimNow and must agree with a supplied new one.
bool ResolveWalOverride(const std::string& environment, const std::string& general,
                        const std::string& legacy_simnow, const std::string& default_path,
                        std::string* out, std::string* error);

}  // namespace quant_hft
