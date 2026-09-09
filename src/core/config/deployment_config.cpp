#include "quant_hft/config/deployment_config.h"

#include <fcntl.h>
#include <openssl/sha.h>
#include <unistd.h>

#include <cerrno>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <regex>
#include <set>
#include <sstream>

#include "quant_hft/config/strict_yaml.h"
#include "quant_hft/strategy/strategy_main_config_loader.h"

namespace quant_hft {
namespace {
namespace fs = std::filesystem;
using namespace config_detail;

void WriteNewConfiguration(const fs::path& path, const std::string& content) {
    const int fd = ::open(path.c_str(), O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC, 0600);
    if (fd < 0) throw std::runtime_error("cannot create new migration output: " + path.string());
    std::size_t offset = 0;
    while (offset < content.size()) {
        const auto count = ::write(fd, content.data() + offset, content.size() - offset);
        if (count < 0 && errno == EINTR) continue;
        if (count <= 0) {
            ::close(fd);
            throw std::runtime_error("cannot write migration output: " + path.string());
        }
        offset += static_cast<std::size_t>(count);
    }
    const bool synced = ::fsync(fd) == 0;
    const bool closed = ::close(fd) == 0;
    if (!synced || !closed)
        throw std::runtime_error("cannot synchronize migration output: " + path.string());
}

std::string Read(const fs::path& path) {
    if (!fs::is_regular_file(path) || fs::file_size(path) > 8 * 1024 * 1024) {
        throw std::runtime_error("configuration file missing or too large: " + path.string());
    }
    std::ifstream input(path, std::ios::binary);
    if (!input) throw std::runtime_error("cannot read configuration: " + path.string());
    return std::string(std::istreambuf_iterator<char>(input), {});
}

fs::path Resolve(const fs::path& parent, const std::string& path) {
    if (path.empty() || path.find("${") != std::string::npos) {
        throw std::runtime_error("config references must be explicit paths");
    }
    return fs::absolute(fs::path(path).is_absolute() ? fs::path(path) : parent / path)
        .lexically_normal();
}

void Identity(const std::string& id) {
    static const std::regex pattern("[A-Za-z0-9][A-Za-z0-9_.-]{0,127}");
    if (!std::regex_match(id, pattern) || id == "." || id == "..") {
        throw std::runtime_error("unsafe or empty identity: " + id);
    }
}

void RejectInlineSecrets(const YAML::Node& node) {
    if (node.IsMap()) {
        for (const auto& entry : node) {
            const auto key = entry.first.as<std::string>();
            if (key == "password" || key == "auth_code" || key == "token" || key == "api_key") {
                const auto value = Scalar(entry.second, key);
                if (!value.empty() &&
                    !std::regex_match(value, std::regex("\\$\\{[A-Z][A-Z0-9_]*\\}"))) {
                    throw std::runtime_error("connection contains inline credential: " + key);
                }
            }
            RejectInlineSecrets(entry.second);
        }
    } else if (node.IsSequence()) {
        for (const auto& entry : node) RejectInlineSecrets(entry);
    }
}

std::string VerifiedRead(const fs::path& base, const YAML::Node& ref, YAML::Node* source) {
    Keys(ref, {"path", "sha256"}, "file reference");
    const auto path = Resolve(base, Required(ref, "path"));
    const auto content = Read(path);
    const auto hash = ConfigContentSha256(content);
    if (hash != Required(ref, "sha256"))
        throw std::runtime_error("SHA256 mismatch: " + path.string());
    (*source)["path"] = path.string();
    (*source)["sha256"] = hash;
    return content;
}

StrategyExecutionProfile Profile(const YAML::Node& node) {
    Keys(node,
         {"max_order_volume", "max_order_notional", "max_margin_to_equity_ratio",
          "forbid_open_windows"},
         "risk profile");
    StrategyExecutionProfile result;
    const auto volume = Number(node["max_order_volume"], "max_order_volume");
    if (volume < 1 || volume > 100000 || std::floor(volume) != volume) {
        throw std::runtime_error("invalid max_order_volume");
    }
    result.max_order_volume = static_cast<int>(volume);
    result.max_order_notional = Number(node["max_order_notional"], "max_order_notional");
    result.max_margin_to_equity_ratio =
        Number(node["max_margin_to_equity_ratio"], "max_margin_to_equity_ratio");
    if (result.max_order_notional <= 0 || result.max_margin_to_equity_ratio <= 0 ||
        result.max_margin_to_equity_ratio > 1)
        throw std::runtime_error("invalid risk bounds");
    result.forbid_open_windows = Scalar(node["forbid_open_windows"], "forbid_open_windows");
    static const std::regex windows(
        "(([01][0-9]|2[0-3]):[0-5][0-9]-([01][0-9]|2[0-3]):[0-5][0-9])(,([01][0-9]|2[0-3]):[0-5][0-"
        "9]-([01][0-9]|2[0-3]):[0-5][0-9])*");
    if (!result.forbid_open_windows.empty() &&
        !std::regex_match(result.forbid_open_windows, windows)) {
        throw std::runtime_error(
            "forbid_open_windows requires Asia/Shanghai HH:MM-HH:MM intervals");
    }
    return result;
}

YAML::Node ProfileNode(const StrategyExecutionProfile& profile) {
    YAML::Node node;
    node["max_order_volume"] = profile.max_order_volume;
    node["max_order_notional"] = profile.max_order_notional;
    node["max_margin_to_equity_ratio"] = profile.max_margin_to_equity_ratio;
    node["forbid_open_windows"] = profile.forbid_open_windows;
    return node;
}

std::string RegimeName(MarketRegime regime) {
    switch (regime) {
        case MarketRegime::kStrongTrend:
            return "kStrongTrend";
        case MarketRegime::kWeakTrend:
            return "kWeakTrend";
        case MarketRegime::kRanging:
            return "kRanging";
        case MarketRegime::kFlat:
            return "kFlat";
        default:
            return "kUnknown";
    }
}

YAML::Node DefinitionNode(const ParameterSet& params) {
    YAML::Node result;
    result["schema_version"] = 1;
    result["parameter_set_id"] = params.parameter_set_id;
    result["strategy_release"] = params.strategy_release;
    result["product_id"] = params.definition.product_id;
    result["market_state_mode"] = params.definition.market_state_mode;
    result["merge_rule"] = "kPriority";
    result["components"] = YAML::Node(YAML::NodeType::Sequence);
    for (const auto& component : params.definition.sub_strategies) {
        YAML::Node item;
        item["component_id"] = component.id;
        item["type"] = component.type;
        item["enabled"] = component.enabled;
        item["timeframe_minutes"] = component.timeframe_minutes;
        item["entry_market_regimes"] = YAML::Node(YAML::NodeType::Sequence);
        for (const auto regime : component.entry_market_regimes) {
            item["entry_market_regimes"].push_back(RegimeName(regime));
        }
        item["params"] = YAML::Node(YAML::NodeType::Map);
        const std::map<std::string, std::string> sorted(component.params.begin(),
                                                        component.params.end());
        for (const auto& param : sorted)
            if (param.first != "id") item["params"][param.first] = param.second;
        result["components"].push_back(item);
    }
    return result;
}
}  // namespace

std::string ConfigContentSha256(const std::string& content) {
    unsigned char digest[SHA256_DIGEST_LENGTH];
    SHA256(reinterpret_cast<const unsigned char*>(content.data()), content.size(), digest);
    std::ostringstream out;
    out << std::hex << std::setfill('0');
    for (const unsigned char byte : digest) out << std::setw(2) << static_cast<int>(byte);
    return out.str();
}

std::string ConfigParameterSha256(const ParameterSet& parameters) {
    return ConfigContentSha256(CanonicalJson(DefinitionNode(parameters)));
}

bool VerifyDeploymentPackage(const DeploymentConfig& deployment, std::string* error) {
    if (deployment.package_hash != QUANT_STRATEGIES_EXPECTED_MANIFEST_HASH) {
        if (error)
            *error = "deployment package manifest differs from the statically linked package";
        return false;
    }
    return true;
}

std::unordered_map<std::string, double> FixedStrategyCapitalAllocations(
    const DeploymentConfig& deployment, const std::string& account_ref) {
    std::unordered_map<std::string, double> result;
    for (const auto& instance : deployment.instances) {
        if (instance.account_ref == account_ref && instance.capital_mode == "fixed")
            result.emplace(instance.instance_id, instance.initial_capital);
    }
    return result;
}

bool LoadDeploymentConfig(const std::string& path, DeploymentConfig* out, std::string* error) {
    try {
        if (out == nullptr) throw std::runtime_error("null deployment output");
        DeploymentConfig result;
        result.source_path = fs::absolute(path).lexically_normal().string();
        const auto parent = fs::path(result.source_path).parent_path();
        const auto root_text = Read(result.source_path);
        const auto root = Parse(root_text);
        Keys(root,
             {"schema_version", "package", "parameter_sets", "accounts", "capital_allocations",
              "risk_profiles", "instances"},
             "deployment");
        if (Required(root, "schema_version") != "1")
            throw std::runtime_error("unsupported deployment version");
        YAML::Node resolved;
        resolved["schema_version"] = 1;
        resolved["sources"]["deployment"]["path"] = result.source_path;
        resolved["sources"]["deployment"]["sha256"] = ConfigContentSha256(root_text);
        Keys(root["package"], {"version", "manifest", "parameter_schema"}, "package");
        result.package_version = Required(root["package"], "version");
        if (result.package_version != "1.0.0")
            throw std::runtime_error("host requires QuantStrategies 1.0.0");
        YAML::Node manifest_source;
        const auto manifest_text =
            VerifiedRead(parent, root["package"]["manifest"], &manifest_source);
        const auto manifest = Parse(manifest_text);
        if (Required(manifest, "version") != result.package_version)
            throw std::runtime_error("package version mismatch");
        result.package_hash = ConfigContentSha256(manifest_text);
        resolved["sources"]["package_manifest"] = manifest_source;
        YAML::Node schema_source;
        const auto schema_text =
            VerifiedRead(parent, root["package"]["parameter_schema"], &schema_source);
        const auto schema_member =
            "share/quant_strategies/" + result.package_version + "/schemas/atomic_parameters.yaml";
        if (!manifest["files"][schema_member] ||
            manifest["files"][schema_member].as<std::string>() !=
                ConfigContentSha256(schema_text)) {
            throw std::runtime_error("parameter schema differs from strategy package manifest");
        }
        resolved["sources"]["parameter_schema"] = schema_source;
        std::set<std::string> releases;
        if (!manifest["strategy_releases"].IsSequence())
            throw std::runtime_error("package release catalog missing");
        for (const auto& release : manifest["strategy_releases"])
            releases.insert(release.as<std::string>());
        std::map<std::string, ParameterSet> params;
        std::map<std::string, std::string> parameter_hashes;
        if (!root["parameter_sets"].IsMap())
            throw std::runtime_error("parameter_sets must be mapping");
        for (const auto& entry : root["parameter_sets"]) {
            const auto id = entry.first.as<std::string>();
            Identity(id);
            YAML::Node source;
            const auto text = VerifiedRead(parent, entry.second, &source);
            ParameterSet parameter;
            std::string detail;
            if (!ParseParameterSetYaml(text, schema_text, &parameter, &detail)) {
                throw std::runtime_error("parameter " + id + ": " + detail);
            }
            if (parameter.parameter_set_id != id)
                throw std::runtime_error("parameter_set identity mismatch");
            if (!releases.count(parameter.strategy_release))
                throw std::runtime_error("strategy release absent from package");
            parameter_hashes[id] = ConfigParameterSha256(parameter);
            params.emplace(id, std::move(parameter));
            resolved["sources"]["parameter_sets"][id] = source;
        }
        std::map<std::string, StrategyExecutionProfile> profiles;
        if (!root["risk_profiles"].IsMap())
            throw std::runtime_error("risk_profiles must be mapping");
        for (const auto& entry : root["risk_profiles"]) {
            const auto id = entry.first.as<std::string>();
            Identity(id);
            profiles.emplace(id, Profile(entry.second));
        }
        if (!root["accounts"].IsMap() || root["accounts"].size() == 0)
            throw std::runtime_error("accounts required");
        std::set<std::string> broker_identities;
        for (const auto& entry : root["accounts"]) {
            DeploymentAccount account;
            account.account_ref = entry.first.as<std::string>();
            Identity(account.account_ref);
            const auto node = entry.second;
            Keys(node,
                 {"environment", "broker_id", "account_id", "connection_config", "credential_ref",
                  "runtime_root", "active_host", "risk_profile_ref"},
                 "account");
            account.environment = Required(node, "environment");
            if (account.environment != "simnow" && account.environment != "live")
                throw std::runtime_error("environment must be simnow or live");
            account.broker_id = Required(node, "broker_id");
            account.account_id = Required(node, "account_id");
            Identity(account.broker_id);
            Identity(account.account_id);
            const auto identity =
                account.environment + "/" + account.broker_id + "/" + account.account_id;
            if (!broker_identities.insert(identity).second)
                throw std::runtime_error("duplicate physical account identity");
            account.connection_config =
                Resolve(parent, Required(node, "connection_config")).string();
            // Only a reference is resolved; credentials are never read by validation or resolve.
            account.credential_ref = Resolve(parent, Required(node, "credential_ref")).string();
            account.runtime_root = Resolve(parent, Required(node, "runtime_root")).string();
            account.active_host = Required(node, "active_host");
            account.risk = profiles.at(Required(node, "risk_profile_ref"));
            const auto connection_text = Read(account.connection_config);
            RejectInlineSecrets(Parse(connection_text));
            resolved["sources"]["connections"][account.account_ref]["path"] =
                account.connection_config;
            resolved["sources"]["connections"][account.account_ref]["sha256"] =
                ConfigContentSha256(connection_text);
            auto visible = YAML::Clone(node);
            visible["connection_config"] = account.connection_config;
            visible["credential_ref"] = account.credential_ref;
            visible["runtime_root"] = account.runtime_root;
            visible["risk"] = ProfileNode(account.risk);
            resolved["accounts"][account.account_ref] = visible;
            result.accounts.emplace(account.account_ref, std::move(account));
        }
        struct Allocation {
            std::string account_ref;
            std::string mode;
            double initial_capital{0.0};
        };
        std::map<std::string, Allocation> allocations;
        std::map<std::string, std::size_t> allocation_counts;
        std::set<std::string> account_equity_accounts;
        if (!root["capital_allocations"].IsMap())
            throw std::runtime_error("capital_allocations must be mapping");
        for (const auto& entry : root["capital_allocations"]) {
            const auto id = entry.first.as<std::string>();
            Identity(id);
            Keys(entry.second, {"account_ref", "mode", "initial_capital"}, "capital allocation");
            const auto account = Required(entry.second, "account_ref");
            const auto mode = entry.second["mode"] ? Required(entry.second, "mode") : "fixed";
            if (!result.accounts.count(account) || (mode != "fixed" && mode != "account_equity"))
                throw std::runtime_error("invalid capital allocation");
            double capital = 0.0;
            if (mode == "account_equity") {
                if (entry.second["initial_capital"])
                    throw std::runtime_error("account_equity forbids initial_capital");
                account_equity_accounts.insert(account);
            } else {
                capital = Number(entry.second["initial_capital"], "initial_capital");
                if (capital <= 0) throw std::runtime_error("invalid capital allocation");
            }
            ++allocation_counts[account];
            allocations.emplace(id, Allocation{account, mode, capital});
        }
        for (const auto& account : account_equity_accounts)
            if (allocation_counts.at(account) != 1)
                throw std::runtime_error(
                    "account_equity requires one allocation; cannot mix account budgets");
        std::set<std::string> identities, used_allocations;
        std::map<std::string, std::size_t> instance_counts;
        if (!root["instances"].IsSequence() || root["instances"].size() == 0)
            throw std::runtime_error("instances required");
        for (const auto& node : root["instances"]) {
            Keys(node,
                 {"instance_id", "account_ref", "strategy_release", "parameter_set",
                  "capital_allocation_ref", "risk_profile_ref"},
                 "instance");
            ResolvedStrategyInstance instance;
            instance.instance_id = Required(node, "instance_id");
            instance.account_ref = Required(node, "account_ref");
            Identity(instance.instance_id);
            const auto& account = result.accounts.at(instance.account_ref);
            if (!identities.insert(instance.account_ref + "/" + instance.instance_id).second)
                throw std::runtime_error("duplicate instance_id within account");
            instance.parameter_set = Required(node, "parameter_set");
            const auto& parameter = params.at(instance.parameter_set);
            instance.strategy_release = Required(node, "strategy_release");
            if (instance.strategy_release != parameter.strategy_release)
                throw std::runtime_error("instance/parameter strategy version mismatch");
            const auto allocation_ref = Required(node, "capital_allocation_ref");
            const auto& allocation = allocations.at(allocation_ref);
            if (allocation.account_ref != instance.account_ref ||
                !used_allocations.insert(allocation_ref).second)
                throw std::runtime_error("allocation reused or belongs to another account");
            instance.capital_mode = allocation.mode;
            instance.initial_capital = allocation.initial_capital;
            ++instance_counts[instance.account_ref];
            instance.risk = profiles.at(Required(node, "risk_profile_ref"));
            instance.composite = parameter.definition;
            instance.composite.run_type = account.environment == "simnow" ? "sim" : "live";
            instance.product_id = parameter.definition.product_id;
            instance.parameter_hash = parameter_hashes.at(instance.parameter_set);
            instance.state_namespace = account.environment + "/" + account.broker_id + "/" +
                                       account.account_id + "/strategies/" + instance.instance_id;
            YAML::Node value = YAML::Clone(node);
            value["capital_mode"] = instance.capital_mode;
            if (instance.capital_mode == "fixed")
                value["initial_capital"] = instance.initial_capital;
            else
                value["capital_source"] = "confirmed_broker_account_snapshot";
            value["parameter_hash"] = instance.parameter_hash;
            value["state_namespace"] = instance.state_namespace;
            value["parameters"] = DefinitionNode(parameter);
            value["risk"] = ProfileNode(instance.risk);
            value["field_sources"]["capital_mode"] = "capital_allocations." + allocation_ref;
            value["field_sources"]
                 [instance.capital_mode == "fixed" ? "initial_capital" : "capital_source"] =
                     "capital_allocations." + allocation_ref;
            value["field_sources"]["parameters"] = "parameter_sets." + instance.parameter_set;
            value["field_sources"]["risk"] = "risk_profiles." + Required(node, "risk_profile_ref");
            value["field_sources"]["identity"] =
                "instances/" + instance.account_ref + "/" + instance.instance_id;
            resolved["instances"].push_back(value);
            result.instances.push_back(std::move(instance));
        }
        for (const auto& account : account_equity_accounts)
            if (instance_counts[account] != 1)
                throw std::runtime_error("account_equity requires exactly one strategy instance");
        resolved["package_version"] = result.package_version;
        resolved["package_sha256"] = result.package_hash;
        result.effective_hash = ConfigContentSha256(CanonicalJson(resolved));
        resolved["effective_hash"] = result.effective_hash;
        result.resolved_json = CanonicalJson(resolved) + "\n";
        *out = std::move(result);
        return true;
    } catch (const std::exception& ex) {
        if (error) *error = ex.what();
        return false;
    }
}

bool MigrateLegacyParameterSet(const std::string& path, const std::string& mode,
                               const std::string& parameter_id, const std::string& release,
                               const std::string& schema_path, const std::string& output_directory,
                               std::string* error) {
    try {
        if (mode != "backtest" && mode != "sim" && mode != "live")
            throw std::runtime_error("explicit legacy mode required");
        Identity(parameter_id);
        const auto source = fs::absolute(path);
        Parse(Read(source));
        StrategyMainConfig legacy;
        std::string detail;
        if (!LoadStrategyMainConfig(source.string(), &legacy, &detail))
            throw std::runtime_error(detail);
        ParameterSet params;
        params.parameter_set_id = parameter_id;
        params.strategy_release = release;
        params.definition = legacy.composite;
        YAML::Node report;
        report["source"] = source.string();
        report["source_sha256"] = ConfigContentSha256(Read(source));
        report["selected_mode"] = mode;
        report["trade_facts_changed"] = false;
        for (auto& sub : params.definition.sub_strategies) {
            const auto& overrides = mode == "sim"    ? sub.overrides.sim_params
                                    : mode == "live" ? sub.overrides.live_params
                                                     : sub.overrides.backtest_params;
            for (const auto& param : overrides) sub.params[param.first] = param.second;
            // Historical files carry this unused atomic parameter. Regime gating
            // is owned by the explicit entry_market_regimes component field.
            if (sub.params.erase("allowed_regimes")) {
                report["discarded_ignored_parameters"][sub.id].push_back("allowed_regimes");
            }
            if (sub.params.count("daily_max_loss_r")) {
                if (!sub.params.count("daily_max_loss_R"))
                    sub.params["daily_max_loss_R"] = sub.params.at("daily_max_loss_r");
                sub.params.erase("daily_max_loss_r");
            }
            if (!sub.config_path.empty()) {
                const auto child = Resolve(source.parent_path(), sub.config_path);
                const auto bytes = Read(child);
                Parse(bytes);
                report["subfiles"][sub.id]["path"] = child.string();
                report["subfiles"][sub.id]["sha256"] = ConfigContentSha256(bytes);
            }
            report["component_id_mapping"][sub.id] = sub.id;
            sub.overrides = {};
            sub.config_path.clear();
        }
        const auto text = CanonicalJson(DefinitionNode(params));
        ParameterSet checked;
        if (!ParseParameterSetYaml(text, Read(schema_path), &checked, &detail))
            throw std::runtime_error(detail);
        const auto canonical = CanonicalJson(DefinitionNode(checked)) + "\n";
        report["parameter_sha256"] = ConfigContentSha256(canonical);
        report["effective_parameter_sha256"] = ConfigParameterSha256(checked);
        const auto target = fs::absolute(output_directory);
        const auto parameter_file = target / (parameter_id + ".yaml");
        const auto report_file = target / (parameter_id + ".migration.json");
        if (fs::exists(parameter_file) || fs::exists(report_file))
            throw std::runtime_error("migration output already exists");
        fs::create_directories(target);
        WriteNewConfiguration(parameter_file, canonical);
        WriteNewConfiguration(report_file, CanonicalJson(report) + '\n');
        return true;
    } catch (const std::exception& ex) {
        if (error) *error = ex.what();
        return false;
    }
}

}  // namespace quant_hft
