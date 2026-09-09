#include "quant_hft/config/strategy_state_migration.h"

#include <fcntl.h>
#include <linux/fs.h>
#include <sys/syscall.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <map>
#include <regex>
#include <set>
#include <stdexcept>

#include "quant_hft/common/position_projection.h"
#include "quant_hft/config/strict_yaml.h"
#include "quant_hft/strategy/state_envelope.h"
#include "quant_hft/strategy/state_persistence.h"
#include "quant_hft/strategy/strategy_main_config_loader.h"

namespace quant_hft {
namespace {
namespace fs = std::filesystem;
using namespace config_detail;
void SafePart(const std::string& value) {
    if (!std::regex_match(value, std::regex("[A-Za-z0-9][A-Za-z0-9_.-]{0,127}")))
        throw std::runtime_error("unsafe or empty migration identity/key prefix");
}
std::string Read(const fs::path& path) {
    if (fs::is_symlink(fs::symlink_status(path)) || !fs::is_regular_file(path) ||
        fs::file_size(path) > 16 * 1024 * 1024)
        throw std::runtime_error("migration input must be a bounded regular non-symlink file: " +
                                 path.string());
    std::ifstream stream(path, std::ios::binary);
    if (!stream) throw std::runtime_error("cannot read migration input: " + path.string());
    return std::string(std::istreambuf_iterator<char>(stream), {});
}
fs::path Resolve(const fs::path& parent, const std::string& text) {
    return fs::absolute(fs::path(text).is_absolute() ? fs::path(text) : parent / text)
        .lexically_normal();
}
bool Under(const fs::path& child, const fs::path& parent) {
    auto c = child.begin();
    for (auto p = parent.begin(); p != parent.end(); ++p, ++c)
        if (c == child.end() || *c != *p) return false;
    return true;
}
struct Stage {
    fs::path path;
    fs::path parent;
    ~Stage() {
        if (!path.empty() && path.parent_path() == parent &&
            path.filename().string().rfind(".quant-state-migration-", 0) == 0) {
            std::error_code ec;
            fs::remove_all(path, ec);
        }
    }
};
void SyncDirectory(const fs::path& path) {
    const int fd = ::open(path.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    if (fd < 0) throw std::runtime_error("cannot open migration directory for synchronization");
    const bool success = ::fsync(fd) == 0;
    ::close(fd);
    if (!success) throw std::runtime_error("cannot synchronize migration directory");
}
void WriteReport(const fs::path& path, const std::string& text) {
    const int fd = ::open(path.c_str(), O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC, 0600);
    if (fd < 0) throw std::runtime_error("cannot create migration report");
    std::size_t offset = 0;
    while (offset < text.size()) {
        const auto count = ::write(fd, text.data() + offset, text.size() - offset);
        if (count < 0 && errno == EINTR) continue;
        if (count <= 0) {
            ::close(fd);
            throw std::runtime_error("cannot write migration report");
        }
        offset += static_cast<std::size_t>(count);
    }
    const bool success = ::fsync(fd) == 0;
    ::close(fd);
    if (!success) throw std::runtime_error("cannot synchronize migration report");
}
std::string PayloadHash(const StrategyState& state) {
    YAML::Node node(YAML::NodeType::Map);
    for (const auto& entry : state) node[entry.first] = entry.second;
    return ConfigContentSha256(CanonicalJson(node));
}
}  // namespace

bool MigrateStrategyState(const DeploymentConfig& deployment,
                          const StrategyStateMigrationOptions& options, std::string* error) {
    try {
        SafePart(options.instance_id);
        SafePart(options.legacy_instance_id);
        SafePart(options.key_prefix);
        if (options.instance_id != options.legacy_instance_id)
            throw std::runtime_error(
                "automatic strategy owner renaming is unsupported; instance_id must equal "
                "legacy_instance_id");
        const auto account = deployment.accounts.find(options.account_ref);
        if (account == deployment.accounts.end())
            throw std::runtime_error("unknown migration account_ref");
        SafePart(account->second.account_id);
        const ResolvedStrategyInstance* selected = nullptr;
        for (const auto& instance : deployment.instances)
            if (instance.account_ref == options.account_ref &&
                instance.instance_id == options.instance_id) {
                if (selected) throw std::runtime_error("ambiguous migration instance");
                selected = &instance;
            }
        if (!selected) throw std::runtime_error("deployment instance not found");
        SafePart(selected->parameter_set);
        const auto old_directory = fs::canonical(options.legacy_state_directory);
        const auto target = fs::absolute(options.output_directory).lexically_normal();
        const auto target_parent = fs::canonical(target.parent_path());
        const auto final_target = target_parent / target.filename();
        if (fs::exists(final_target) || fs::is_symlink(fs::symlink_status(final_target)))
            throw std::runtime_error("migration output must be a new directory");
        if (Under(final_target, old_directory) || Under(old_directory, final_target))
            throw std::runtime_error(
                "migration output must be isolated from the legacy state directory");
        const auto source_file =
            old_directory / (options.key_prefix + "__" + account->second.account_id + "__" +
                             options.instance_id + ".json");
        std::map<fs::path, std::string> observed;
        const auto observe = [&](const fs::path& path) {
            const auto text = Read(path);
            observed[path] = ConfigContentSha256(text);
            return text;
        };
        const auto state_bytes = observe(source_file);
        Parse(state_bytes);  // The persistence parser alone does not reject duplicate JSON fields.
        const auto provenance_text = observe(fs::absolute(options.parameter_migration_report));
        const auto provenance = Parse(provenance_text);
        const auto mode = Required(provenance, "selected_mode");
        const auto expected_mode = account->second.environment == "simnow" ? "sim" : "live";
        if (mode != expected_mode)
            throw std::runtime_error("parameter migration mode differs from account environment");
        const auto legacy_main = fs::absolute(options.legacy_main_config).lexically_normal();
        if (ConfigContentSha256(observe(legacy_main)) != Required(provenance, "source_sha256"))
            throw std::runtime_error(
                "legacy main configuration hash differs from migration evidence");
        StrategyMainConfig legacy;
        std::string detail;
        if (!LoadStrategyMainConfig(legacy_main.string(), &legacy, &detail))
            throw std::runtime_error(detail);
        std::size_t subfile_count = 0;
        for (const auto& sub : legacy.composite.sub_strategies) {
            if (!provenance["component_id_mapping"][sub.id] ||
                provenance["component_id_mapping"][sub.id].as<std::string>() != sub.id)
                throw std::runtime_error(
                    "missing or renamed component in parameter migration evidence");
            if (!sub.config_path.empty()) {
                ++subfile_count;
                const auto path = Resolve(legacy_main.parent_path(), sub.config_path);
                if (ConfigContentSha256(observe(path)) !=
                    Required(provenance["subfiles"][sub.id], "sha256"))
                    throw std::runtime_error(
                        "legacy parameter subfile hash differs from migration evidence");
            }
        }
        if (provenance["component_id_mapping"].size() != legacy.composite.sub_strategies.size() ||
            (provenance["subfiles"] ? provenance["subfiles"].size() : 0) != subfile_count)
            throw std::runtime_error(
                "parameter migration evidence has unaccounted components/subfiles");
        const auto resolved = Parse(deployment.resolved_json);
        const auto schema_path = Required(resolved["sources"]["parameter_schema"], "path");
        if (ConfigContentSha256(observe(schema_path)) !=
            Required(resolved["sources"]["parameter_schema"], "sha256"))
            throw std::runtime_error("resolved parameter schema changed after validation");
        std::string pattern = (target_parent / ".quant-state-migration-XXXXXX").string();
        if (!::mkdtemp(pattern.data()))
            throw std::runtime_error("cannot create isolated migration staging directory");
        Stage stage{pattern, target_parent};
        const auto materialized = stage.path / "parameter_provenance";
        if (!MigrateLegacyParameterSet(legacy_main.string(), mode, selected->parameter_set,
                                       selected->strategy_release, schema_path,
                                       materialized.string(), &detail))
            throw std::runtime_error("cannot independently reproduce legacy parameters: " + detail);
        const auto formal_bytes = Read(materialized / (selected->parameter_set + ".yaml"));
        ParameterSet reproduced;
        if (!ParseParameterSetYaml(formal_bytes, Read(schema_path), &reproduced, &detail))
            throw std::runtime_error("recomputed legacy parameters are invalid: " + detail);
        if (ConfigContentSha256(formal_bytes) != Required(provenance, "parameter_sha256") ||
            ConfigParameterSha256(reproduced) != selected->parameter_hash ||
            (provenance["effective_parameter_sha256"] &&
             provenance["effective_parameter_sha256"].as<std::string>() !=
                 selected->parameter_hash))
            throw std::runtime_error(
                "recomputed legacy parameters differ from formal parameter hash");
        FileStrategyStatePersistence source(old_directory.string(), options.key_prefix, 0);
        StrategyState payload;
        if (!source.LoadStrategyState(account->second.account_id, options.instance_id, &payload,
                                      &detail))
            throw std::runtime_error(detail);
        std::set<std::string> components;
        for (const auto& sub : selected->composite.sub_strategies) components.insert(sub.id);
        for (const auto& entry : payload) {
            if (entry.first.rfind("committed_position.", 0) == 0) {
                Position position;
                if (!DecodePositionProjection(entry.second, &position) ||
                    position.account_id != account->second.account_id ||
                    position.strategy_id != options.instance_id)
                    throw std::runtime_error(
                        "legacy economic position owner differs from stable instance; explicit "
                        "ledger reconciliation required");
            }
            if (entry.first.rfind("atomic.", 0) == 0) {
                const auto remainder = entry.first.substr(7);
                const auto split = remainder.find('.');
                if (split == std::string::npos || !components.count(remainder.substr(0, split)))
                    throw std::runtime_error(
                        "legacy atomic state component is absent from formal parameters");
            }
            if (entry.first.rfind("owner.", 0) == 0 && !components.count(entry.second))
                throw std::runtime_error(
                    "legacy signal-origin component is absent from formal parameters");
        }
        CompositeStrategy strategy(selected->composite);
        StrategyContext context;
        context.account_id = account->second.account_id;
        context.strategy_id = options.instance_id;
        context.metadata["ownership_mode"] = "instance";
        strategy.Initialize(context);
        if (!strategy.LoadState(payload, &detail))
            throw std::runtime_error("legacy strategy state incompatible: " + detail);
        strategy.Shutdown();
        const StrategyStateIdentity identity{options.instance_id, account->second.account_id,
                                             selected->strategy_release, selected->parameter_hash,
                                             "1"};
        StrategyState wrapped, restored;
        if (!WrapStrategyState(payload, identity, &wrapped, &detail) ||
            !UnwrapStrategyState(wrapped, identity, &restored, &detail) || restored != payload)
            throw std::runtime_error("state envelope validation failed: " + detail);
        FileStrategyStatePersistence destination(stage.path.string(), options.key_prefix, 0);
        if (!destination.SaveStrategyState(account->second.account_id, options.instance_id, wrapped,
                                           &detail))
            throw std::runtime_error(detail);
        const auto output_file = stage.path / source_file.filename();
        StrategyState verified;
        if (!destination.LoadStrategyState(account->second.account_id, options.instance_id,
                                           &verified, &detail) ||
            verified != wrapped)
            throw std::runtime_error("persisted migration state failed round-trip verification: " +
                                     detail);
        YAML::Node report;
        report["schema_version"] = 1;
        report["account_id"] = account->second.account_id;
        report["instance_id"] = options.instance_id;
        report["strategy_release"] = selected->strategy_release;
        report["parameter_sha256"] = selected->parameter_hash;
        report["package_manifest_sha256"] = deployment.package_hash;
        report["deployment_sha256"] = deployment.effective_hash;
        report["parameter_migration_report_sha256"] = ConfigContentSha256(provenance_text);
        report["old_state_sha256"] = ConfigContentSha256(state_bytes);
        report["new_state_sha256"] = ConfigContentSha256(Read(output_file));
        report["payload_sha256_before"] = PayloadHash(payload);
        report["payload_sha256_after"] = PayloadHash(restored);
        report["source_state_file"] = source_file.string();
        report["new_state_file"] = (final_target / source_file.filename()).string();
        report["watermarks"] = "preserved in original payload; no synthetic sequence introduced";
        report["owner_renamed"] = false;
        report["trade_facts_changed"] = false;
        report["activation_status"] =
            "offline artifact only; account ledger and broker reconciliation remain required";
        for (const auto& entry : observed) {
            if (ConfigContentSha256(Read(entry.first)) != entry.second)
                throw std::runtime_error("migration input changed during validation: " +
                                         entry.first.string());
            report["inputs"][entry.first.string()] = entry.second;
        }
        WriteReport(stage.path / "migration_report.json", CanonicalJson(report) + "\n");
        SyncDirectory(stage.path);
        if (::syscall(SYS_renameat2, AT_FDCWD, stage.path.c_str(), AT_FDCWD, final_target.c_str(),
                      RENAME_NOREPLACE) != 0)
            throw std::runtime_error(
                "cannot publish isolated migration directory without replacement: " +
                std::string(std::strerror(errno)));
        stage.path.clear();
        SyncDirectory(target_parent);
        return true;
    } catch (const std::exception& ex) {
        if (error) *error = ex.what();
        return false;
    }
}
}  // namespace quant_hft
