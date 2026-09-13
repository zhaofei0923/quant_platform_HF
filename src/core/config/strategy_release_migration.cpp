#include "quant_hft/config/strategy_release_migration.h"

#include <fcntl.h>
#include <linux/fs.h>
#include <sys/syscall.h>
#include <unistd.h>

#include <cerrno>
#include <cmath>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <map>
#include <regex>
#include <set>
#include <stdexcept>

#include "quant_hft/common/position_projection.h"
#include "quant_hft/config/deployment_config.h"
#include "quant_hft/config/strict_yaml.h"
#include "quant_hft/strategy/state_envelope.h"
#include "quant_hft/strategy/state_persistence.h"

namespace quant_hft {
namespace {
namespace fs = std::filesystem;
using namespace config_detail;
void SafePart(const std::string& value) {
    if (!std::regex_match(value, std::regex("[A-Za-z0-9][A-Za-z0-9_.-]{0,127}")))
        throw std::runtime_error("unsafe or empty migration identity/key prefix");
}
bool Starts(const std::string& value, const std::string& prefix) {
    return value.rfind(prefix, 0) == 0;
}
std::string Read(const fs::path& path) {
    if (fs::is_symlink(fs::symlink_status(path)) || !fs::is_regular_file(path) ||
        fs::file_size(path) > 16 * 1024 * 1024)
        throw std::runtime_error("migration input must be a bounded regular non-symlink file");
    std::ifstream stream(path, std::ios::binary);
    if (!stream) throw std::runtime_error("cannot read migration input");
    return std::string(std::istreambuf_iterator<char>(stream), {});
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
            Starts(path.filename().string(), ".quant-release-migration-")) {
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
std::string PayloadHash(const StrategyState& state, bool facts_only = false) {
    YAML::Node node(YAML::NodeType::Map);
    for (const auto& entry : state)
        if (!facts_only || !Starts(entry.first, "atomic.")) node[entry.first] = entry.second;
    return ConfigContentSha256(CanonicalJson(node));
}
const ResolvedStrategyInstance& Select(const DeploymentConfig& deployment,
                                       const StrategyReleaseMigrationOptions& options) {
    const ResolvedStrategyInstance* selected = nullptr;
    for (const auto& instance : deployment.instances)
        if (instance.account_ref == options.account_ref &&
            instance.instance_id == options.instance_id) {
            if (selected) throw std::runtime_error("ambiguous migration instance");
            selected = &instance;
        }
    if (!selected) throw std::runtime_error("deployment instance not found");
    return *selected;
}
bool SameRisk(const StrategyExecutionProfile& a, const StrategyExecutionProfile& b) {
    return a.max_order_volume == b.max_order_volume &&
           a.max_order_notional == b.max_order_notional &&
           a.max_margin_to_equity_ratio == b.max_margin_to_equity_ratio &&
           a.forbid_open_windows == b.forbid_open_windows;
}
void ObserveSources(const YAML::Node& node, std::map<fs::path, std::string>* observed) {
    if (!node.IsMap()) return;
    if (node["path"] && node["sha256"]) {
        const fs::path path = Required(node, "path");
        const auto hash = Required(node, "sha256");
        if (ConfigContentSha256(Read(path)) != hash)
            throw std::runtime_error("deployment source changed after validation");
        const auto inserted = observed->emplace(path, hash);
        if (!inserted.second && inserted.first->second != hash)
            throw std::runtime_error("deployment inputs disagree on source hash");
        return;
    }
    for (const auto& entry : node) ObserveSources(entry.second, observed);
}
double Number(const std::string& text) {
    std::size_t count = 0;
    const auto value = std::stod(text, &count);
    if (count != text.size() || !std::isfinite(value))
        throw std::runtime_error("invalid state number");
    return value;
}
using Targets = std::map<std::string, std::pair<double, int>>;
Targets TakeProfits(const StrategyState& state, const std::string& component, bool old) {
    const auto prefix = "atomic." + component + ".";
    const auto count = state.find(prefix + "take_profit.count");
    if (count == state.end()) return {};
    const auto n = Number(count->second);
    if (n < 0 || n > 100000 || n != std::floor(n))
        throw std::runtime_error("invalid take-profit state count");
    std::map<std::string, int> directions;
    if (old) {
        const auto direction_count = state.find(prefix + "trailing_direction.count");
        if (direction_count != state.end()) {
            const auto d = Number(direction_count->second);
            if (d < 0 || d > 100000 || d != std::floor(d))
                throw std::runtime_error("invalid trailing direction count");
            for (std::size_t i = 0; i < static_cast<std::size_t>(d); ++i) {
                const auto item = prefix + "trailing_direction." + std::to_string(i) + ".";
                const auto direction = Number(state.at(item + "direction"));
                if ((direction != 1 && direction != -1) ||
                    !directions.emplace(state.at(item + "instrument"), static_cast<int>(direction))
                         .second)
                    throw std::runtime_error("invalid or duplicate saved target direction");
            }
        }
    }
    Targets targets;
    for (std::size_t i = 0; i < static_cast<std::size_t>(n); ++i) {
        const auto item = prefix + "take_profit." + std::to_string(i) + ".";
        const auto instrument = state.at(item + "instrument");
        const auto price = Number(state.at(item + "price"));
        const auto direction =
            old ? directions.at(instrument) : Number(state.at(item + "direction"));
        if (instrument.empty() || price <= 0 || (direction != 1 && direction != -1) ||
            !targets.emplace(instrument, std::make_pair(price, static_cast<int>(direction))).second)
            throw std::runtime_error("invalid or duplicate saved take-profit target");
    }
    return targets;
}
Targets OwnedTakeProfits(const StrategyState& state, const std::string& component) {
    Targets result;
    for (const auto& target : TakeProfits(state, component, true)) {
        const auto net = state.find("net_pos." + target.first);
        const auto owner = state.find("owner." + target.first);
        if (net != state.end() && Number(net->second) != 0 && owner != state.end() &&
            owner->second == component)
            result.insert(target);
    }
    return result;
}
void CheckFacts(const StrategyState& payload, const std::string& account_id,
                const std::string& instance_id, const std::set<std::string>& components) {
    std::map<std::string, std::pair<std::int64_t, std::int64_t>> projected_quantities;
    for (const auto& entry : payload) {
        if (Starts(entry.first, "committed_position.")) {
            Position position;
            if (!DecodePositionProjection(entry.second, &position) ||
                entry.first.substr(19) != PositionProjectionKey(position) ||
                position.account_id != account_id || position.strategy_id != instance_id)
                throw std::runtime_error(
                    "economic position ownership differs from stable instance");
            auto& quantities = projected_quantities[position.symbol];
            quantities.first += position.long_qty;
            quantities.second += position.short_qty;
        }
        if (Starts(entry.first, "atomic.")) {
            const auto remainder = entry.first.substr(7);
            const auto split = remainder.find('.');
            if (split == std::string::npos || !components.count(remainder.substr(0, split)))
                throw std::runtime_error("atomic state component absent from deployment");
        }
        if (Starts(entry.first, "owner.") && !components.count(entry.second))
            throw std::runtime_error("position signal owner absent from deployment");
        if (Starts(entry.first, "net_pos.") && Number(entry.second) != 0) {
            const auto instrument = entry.first.substr(8);
            const auto owner = payload.find("owner." + instrument);
            if (owner == payload.end() || !components.count(owner->second) ||
                !payload.count("atomic." + owner->second + ".version"))
                throw std::runtime_error("held position requires saved atomic state and owner");
            const auto targets = TakeProfits(payload, owner->second, true);
            const auto target = targets.find(instrument);
            if (target == targets.end() ||
                target->second.second != (Number(entry.second) > 0 ? 1 : -1))
                throw std::runtime_error("held position lacks a verified saved take-profit target");
        }
    }
    for (const auto& item : projected_quantities) {
        const auto net = payload.find("net_pos." + item.first);
        if ((item.second.first != 0 || item.second.second != 0) &&
            (net == payload.end() ||
             Number(net->second) != item.second.first - item.second.second ||
             (item.second.first > 0 && item.second.second > 0)))
            throw std::runtime_error(
                "committed position differs from unambiguous saved net position");
    }
}
void CheckRestoredFacts(const StrategyState& original, const StrategyState& saved) {
    for (const auto& entry : original) {
        const bool numeric = Starts(entry.first, "net_pos.") || Starts(entry.first, "avg_open.") ||
                             Starts(entry.first, "multiplier.");
        if (!numeric && !Starts(entry.first, "owner.") &&
            !Starts(entry.first, "committed_position."))
            continue;
        const auto restored = saved.find(entry.first);
        if (restored == saved.end() || (numeric ? Number(entry.second) != Number(restored->second)
                                                : entry.second != restored->second))
            throw std::runtime_error("target algorithm changed economic position context");
    }
}
}  // namespace

bool MigrateStrategyReleaseState(const StrategyReleaseMigrationOptions& options,
                                 std::string* error) {
    try {
        SafePart(options.instance_id);
        SafePart(options.key_prefix);
        if (!std::regex_match(options.source_package_sha256, std::regex("[0-9a-f]{64}")))
            throw std::runtime_error("source package SHA256 must be explicitly pinned");
        DeploymentConfig source, target;
        std::string detail;
        if (!LoadDeploymentConfigForMigration(options.source_deployment, "1.0.0", &source,
                                              &detail) ||
            !LoadDeploymentConfigForMigration(options.target_deployment, "1.1.0", &target,
                                              &detail) ||
            !VerifyDeploymentPackage(target, &detail))
            throw std::runtime_error(detail);
        if (source.package_hash != options.source_package_sha256)
            throw std::runtime_error("source package manifest differs from explicitly pinned hash");
        const auto& before = Select(source, options);
        const auto& after = Select(target, options);
        const auto& old_account = source.accounts.at(options.account_ref);
        const auto& account = target.accounts.at(options.account_ref);
        SafePart(account.account_id);
        if (old_account.account_id != account.account_id ||
            old_account.broker_id != account.broker_id ||
            old_account.environment != account.environment ||
            old_account.connection_config != account.connection_config ||
            old_account.credential_ref != account.credential_ref ||
            old_account.runtime_root != account.runtime_root ||
            old_account.active_host != account.active_host ||
            before.state_namespace != after.state_namespace ||
            before.capital_mode != after.capital_mode ||
            before.initial_capital != after.initial_capital ||
            !SameRisk(old_account.risk, account.risk) || !SameRisk(before.risk, after.risk))
            throw std::runtime_error(
                "release migration cannot change identity, runtime bindings, capital or risk "
                "limits");
        const auto split = before.strategy_release.find('@');
        const auto algorithm = before.strategy_release.substr(0, split);
        if ((algorithm != "kama_trend" && algorithm != "trend" && algorithm != "composite") ||
            before.strategy_release != algorithm + "@1.0.0" ||
            after.strategy_release != algorithm + "@1.1.0")
            throw std::runtime_error("unsupported strategy release migration");
        ParameterSet old_parameters{"normalized", "normalized", before.composite};
        ParameterSet new_parameters{"normalized", "normalized", after.composite};
        if (ConfigParameterSha256(old_parameters) != ConfigParameterSha256(new_parameters))
            throw std::runtime_error("release migration cannot change effective parameters");
        std::map<fs::path, std::string> observed;
        ObserveSources(Parse(source.resolved_json)["sources"], &observed);
        ObserveSources(Parse(target.resolved_json)["sources"], &observed);
        const auto old_directory = fs::canonical(options.source_state_directory);
        const auto requested = fs::absolute(options.output_directory).lexically_normal();
        const auto parent = fs::canonical(requested.parent_path());
        const auto final_target = parent / requested.filename();
        if (fs::exists(final_target) || fs::is_symlink(fs::symlink_status(final_target)) ||
            Under(final_target, old_directory) || Under(old_directory, final_target))
            throw std::runtime_error("migration output must be a new isolated directory");
        const auto source_file = old_directory / (options.key_prefix + "__" + account.account_id +
                                                  "__" + options.instance_id + ".json");
        const auto source_bytes = Read(source_file);
        Parse(source_bytes);  // Reject duplicate keys before the persistence parser can hide them.
        observed[source_file] = ConfigContentSha256(source_bytes);
        FileStrategyStatePersistence input(old_directory.string(), options.key_prefix, 0);
        StrategyState enveloped, payload;
        const StrategyStateIdentity old_identity{options.instance_id, account.account_id,
                                                 before.strategy_release, before.parameter_hash,
                                                 "1"};
        if (!input.LoadStrategyState(account.account_id, options.instance_id, &enveloped,
                                     &detail) ||
            !UnwrapStrategyState(enveloped, old_identity, &payload, &detail))
            throw std::runtime_error("source formal checkpoint invalid: " + detail);
        std::set<std::string> components;
        for (const auto& component : after.composite.sub_strategies) {
            if (!component.enabled) continue;
            if (component.type != "KamaTrendStrategy" && component.type != "TrendStrategy")
                throw std::runtime_error("unsupported atomic algorithm in release migration");
            components.insert(component.id);
        }
        CheckFacts(payload, account.account_id, options.instance_id, components);
        CompositeStrategy strategy(after.composite);
        StrategyContext context;
        context.account_id = account.account_id;
        context.strategy_id = options.instance_id;
        context.metadata["ownership_mode"] = "instance";
        strategy.Initialize(context);
        StrategyState upgraded;
        if (!strategy.LoadState(payload, &detail) || !strategy.SaveState(&upgraded, &detail))
            throw std::runtime_error("target algorithm rejected source state: " + detail);
        CheckRestoredFacts(payload, upgraded);
        for (const auto& component : components)
            if (OwnedTakeProfits(payload, component) != TakeProfits(upgraded, component, false))
                throw std::runtime_error(
                    "target algorithm changed owned take-profit price/direction");
        // SaveState materializes the new atomic version. Keep every original non-atomic
        // byte, including application watermarks unknown to CompositeStrategy.
        StrategyState migrated = payload;
        for (auto it = migrated.begin(); it != migrated.end();) {
            if (Starts(it->first, "atomic."))
                it = migrated.erase(it);
            else
                ++it;
        }
        for (const auto& entry : upgraded)
            if (Starts(entry.first, "atomic.")) migrated[entry.first] = entry.second;
        if (PayloadHash(payload, true) != PayloadHash(migrated, true) ||
            !strategy.LoadState(migrated, &detail))
            throw std::runtime_error("migrated facts or state validation failed: " + detail);
        strategy.Shutdown();
        const StrategyStateIdentity new_identity{options.instance_id, account.account_id,
                                                 after.strategy_release, after.parameter_hash, "1"};
        StrategyState wrapped, restored;
        if (!WrapStrategyState(migrated, new_identity, &wrapped, &detail) ||
            !UnwrapStrategyState(wrapped, new_identity, &restored, &detail) || restored != migrated)
            throw std::runtime_error("target state envelope validation failed: " + detail);
        std::string pattern = (parent / ".quant-release-migration-XXXXXX").string();
        if (!::mkdtemp(pattern.data()))
            throw std::runtime_error("cannot create migration staging directory");
        Stage stage{pattern, parent};
        FileStrategyStatePersistence output(stage.path.string(), options.key_prefix, 0);
        StrategyState verified;
        if (!output.SaveStrategyState(account.account_id, options.instance_id, wrapped, &detail) ||
            !output.LoadStrategyState(account.account_id, options.instance_id, &verified,
                                      &detail) ||
            verified != wrapped)
            throw std::runtime_error("persisted migrated state failed verification: " + detail);
        YAML::Node report;
        report["schema_version"] = 1;
        report["migration"] = "fixed_take_profit_1.0.0_to_1.1.0";
        report["instance_id"] = options.instance_id;
        report["source_strategy_release"] = before.strategy_release;
        report["target_strategy_release"] = after.strategy_release;
        report["source_package_sha256"] = source.package_hash;
        report["target_package_sha256"] = target.package_hash;
        report["source_deployment_sha256"] = source.effective_hash;
        report["target_deployment_sha256"] = target.effective_hash;
        report["source_parameter_sha256"] = before.parameter_hash;
        report["target_parameter_sha256"] = after.parameter_hash;
        report["old_state_sha256"] = ConfigContentSha256(source_bytes);
        report["new_state_sha256"] = ConfigContentSha256(Read(stage.path / source_file.filename()));
        report["payload_sha256_before"] = PayloadHash(payload);
        report["payload_sha256_after"] = PayloadHash(migrated);
        report["facts_sha256_before"] = PayloadHash(payload, true);
        report["facts_sha256_after"] = PayloadHash(migrated, true);
        report["saved_take_profit_preserved"] = true;
        report["preserved_target_scope"] = "nonzero positions selected by original component owner";
        report["orphan_take_profit_cleanup_allowed"] = true;
        report["owner_renamed"] = false;
        report["trade_facts_changed"] = false;
        report["activation_status"] = "offline artifact only; no account activated";
        for (const auto& entry : observed) {
            if (ConfigContentSha256(Read(entry.first)) != entry.second)
                throw std::runtime_error("migration input changed during validation");
            report["inputs"][entry.first.string()] = entry.second;
        }
        WriteReport(stage.path / "migration_report.json", CanonicalJson(report) + "\n");
        SyncDirectory(stage.path);
        if (::syscall(SYS_renameat2, AT_FDCWD, stage.path.c_str(), AT_FDCWD, final_target.c_str(),
                      RENAME_NOREPLACE) != 0)
            throw std::runtime_error("cannot publish isolated migration directory: " +
                                     std::string(std::strerror(errno)));
        stage.path.clear();
        SyncDirectory(parent);
        return true;
    } catch (const std::exception& ex) {
        if (error) *error = ex.what();
        return false;
    }
}
}  // namespace quant_hft
