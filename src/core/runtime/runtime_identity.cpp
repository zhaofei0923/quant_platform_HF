#include "quant_hft/runtime/runtime_identity.h"

#include <fcntl.h>
#include <sys/file.h>
#include <unistd.h>

#include <cctype>
#include <cerrno>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <system_error>

namespace quant_hft {
namespace {

bool Fail(const std::string& message, std::string* error) {
    if (error != nullptr) {
        *error = message;
    }
    return false;
}

bool SafeComponent(const std::string& text) {
    if (text.empty() || text == "." || text == "..") {
        return false;
    }
    for (unsigned char ch : text) {
        if (!std::isalnum(ch) && ch != '_' && ch != '-' && ch != '.') {
            return false;
        }
    }
    return true;
}

bool WriteAndSync(int fd, const std::string& text) {
    std::size_t offset = 0;
    while (offset < text.size()) {
        const auto written = ::write(fd, text.data() + offset, text.size() - offset);
        if (written < 0 && errno == EINTR) {
            continue;
        }
        if (written <= 0) {
            return false;
        }
        offset += static_cast<std::size_t>(written);
    }
    return ::fsync(fd) == 0;
}

}  // namespace

bool RuntimeIdentity::Validate(std::string* error) const {
    if (environment != "sim" && environment != "simnow" && environment != "prod") {
        return Fail("runtime environment must be sim, simnow or prod", error);
    }
    if (!SafeComponent(broker_id) || !SafeComponent(account_id) || !SafeComponent(instance)) {
        return Fail("runtime broker, account and instance must be nonempty safe path components",
                    error);
    }
    return true;
}

std::string RuntimeIdentity::Namespace() const {
    return environment + ":" + broker_id + ":" + account_id + ":" + instance;
}

std::string RuntimeIdentity::Manifest() const {
    return "schema=1\nenvironment=" + environment + "\nbroker=" + broker_id +
           "\naccount=" + account_id + "\ninstance=" + instance + "\n";
}

RuntimeDirectory::~RuntimeDirectory() {
    for (int fd : locks_) {
        (void)::flock(fd, LOCK_UN);
        (void)::close(fd);
    }
}

bool RuntimeDirectory::BindManifest(const std::string& manifest_path, const std::string& lock_path,
                                    bool has_unmanaged_data, std::string* error) {
    const int lock_fd = ::open(lock_path.c_str(), O_CREAT | O_RDWR | O_CLOEXEC | O_NOFOLLOW, 0600);
    if (lock_fd < 0) {
        return Fail("runtime lock open failed: " + std::string(std::strerror(errno)), error);
    }
    if (::flock(lock_fd, LOCK_EX | LOCK_NB) != 0) {
        ::close(lock_fd);
        return Fail("runtime identity is already owned by another process", error);
    }
    auto reject = [&](const std::string& message) {
        ::flock(lock_fd, LOCK_UN);
        ::close(lock_fd);
        return Fail(message, error);
    };
    if (std::filesystem::exists(manifest_path)) {
        std::ifstream input(manifest_path);
        const std::string content((std::istreambuf_iterator<char>(input)), {});
        if (!input.good() && !input.eof()) {
            return reject("runtime identity manifest cannot be read");
        }
        if (content != identity_.Manifest()) {
            return reject("runtime identity manifest mismatch");
        }
    } else {
        if (has_unmanaged_data) {
            return reject(
                "existing unbound runtime data requires explicit identity-checked migration");
        }
        const int manifest_fd = ::open(manifest_path.c_str(),
                                       O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC | O_NOFOLLOW, 0600);
        if (manifest_fd < 0) {
            return reject("runtime identity manifest create failed");
        }
        const bool written = WriteAndSync(manifest_fd, identity_.Manifest());
        ::close(manifest_fd);
        if (!written) {
            return reject("runtime identity manifest sync failed");
        }
        const auto parent = std::filesystem::path(manifest_path).parent_path();
        const int directory_fd = ::open(parent.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC);
        const bool synced = directory_fd >= 0 && ::fsync(directory_fd) == 0;
        if (directory_fd >= 0) {
            ::close(directory_fd);
        }
        if (!synced) {
            return reject("runtime directory sync failed");
        }
    }
    locks_.push_back(lock_fd);
    return true;
}

bool RuntimeDirectory::Acquire(const std::string& runtime_root, const RuntimeIdentity& identity,
                               std::string* error) {
    if (!locks_.empty() || !identity.Validate(error)) {
        return Fail("invalid or already acquired runtime identity", error);
    }
    identity_ = identity;
    try {
        auto path = std::filesystem::absolute(runtime_root) / identity.environment /
                    identity.broker_id / identity.account_id / identity.instance;
        std::filesystem::create_directories(path);
        path = std::filesystem::weakly_canonical(path);
        // Account admission is process-local. Until a fenced distributed admission service
        // exists, sibling instances for one account must share a single runtime owner.
        const auto account_lock = path.parent_path() / ".account.lock";
        const int account_fd =
            ::open(account_lock.c_str(), O_CREAT | O_RDWR | O_CLOEXEC | O_NOFOLLOW, 0600);
        if (account_fd < 0) return Fail("account lock open failed", error);
        if (::flock(account_fd, LOCK_EX | LOCK_NB) != 0) {
            ::close(account_fd);
            return Fail("account already has an active runtime instance", error);
        }
        locks_.push_back(account_fd);
        bool unmanaged = false;
        for (const auto& entry : std::filesystem::directory_iterator(path)) {
            const auto name = entry.path().filename().string();
            unmanaged = unmanaged || (name != "identity.manifest" && name != ".lock");
        }
        if (!BindManifest((path / "identity.manifest").string(), (path / ".lock").string(),
                          unmanaged, error)) {
            return false;
        }
        root_ = path.string();
        for (const auto* name :
             {"wal", "state", "pending_exit", "mapping", "monitor", "market", "flow"}) {
            std::filesystem::create_directories(path / name);
        }
        return true;
    } catch (const std::filesystem::filesystem_error& ex) {
        return Fail(ex.what(), error);
    }
}

std::string RuntimeDirectory::Path(const std::string& relative) const {
    return (std::filesystem::path(root_) / relative).string();
}

bool RuntimeDirectory::BindArtifact(const std::string& path, bool directory, std::string* error) {
    if (root_.empty()) {
        return Fail("runtime identity has not been acquired", error);
    }
    try {
        auto artifact = std::filesystem::weakly_canonical(std::filesystem::absolute(path));
        const auto relative = artifact.lexically_relative(root_);
        if (!relative.empty() && *relative.begin() != "..") {
            return true;
        }
        if (directory) {
            std::filesystem::create_directories(artifact);
            bool unmanaged = false;
            for (const auto& entry : std::filesystem::directory_iterator(artifact)) {
                const auto name = entry.path().filename().string();
                unmanaged = unmanaged || (name != "identity.manifest" && name != ".lock");
            }
            return BindManifest((artifact / "identity.manifest").string(),
                                (artifact / ".lock").string(), unmanaged, error);
        }
        std::filesystem::create_directories(artifact.parent_path());
        const bool unmanaged =
            std::filesystem::exists(artifact) && std::filesystem::file_size(artifact) != 0;
        return BindManifest(artifact.string() + ".identity", artifact.string() + ".identity.lock",
                            unmanaged, error);
    } catch (const std::filesystem::filesystem_error& ex) {
        return Fail(ex.what(), error);
    }
}

bool ResolveWalOverride(const std::string& environment, const std::string& general,
                        const std::string& legacy_simnow, const std::string& default_path,
                        std::string* out, std::string* error) {
    if (out == nullptr) {
        return Fail("WAL path output is null", error);
    }
    if (!legacy_simnow.empty() && environment != "simnow") {
        return Fail("SIMNOW_WAL_FILE is only valid in the simnow environment", error);
    }
    if (!general.empty() && !legacy_simnow.empty() &&
        std::filesystem::absolute(general).lexically_normal() !=
            std::filesystem::absolute(legacy_simnow).lexically_normal()) {
        return Fail("QUANT_HFT_WAL_FILE conflicts with SIMNOW_WAL_FILE", error);
    }
    *out = !general.empty() ? general : (!legacy_simnow.empty() ? legacy_simnow : default_path);
    return true;
}

}  // namespace quant_hft
