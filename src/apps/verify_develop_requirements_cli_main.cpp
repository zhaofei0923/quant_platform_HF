#include <algorithm>
#include <cctype>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <map>
#include <set>
#include <sstream>
#include <string>
#include <vector>

#include "quant_hft/apps/ops_report_support.h"

namespace {

struct RequirementItem {
    std::string id;
    std::string doc;
    std::string description;
    std::vector<std::string> code_paths;
    std::vector<std::string> test_paths;
    std::vector<std::string> evidence_paths;
};

struct ForbiddenFinding {
    std::string doc;
    std::string term;
    std::vector<int> line_numbers;
};

struct Options {
    std::string requirements_file{"docs/requirements/develop_requirements.yaml"};
    std::string develop_root{"develop"};
    std::vector<std::string> forbidden_terms;
    std::string completion_language_report;
};

const std::vector<std::string> kDefaultForbiddenTerms = {
    "未落地",
    "规划中",
    "部分落地",
    "规划内容（未落地）",
    "规划示例（未落地）",
    "规划 SOP（未落地）",
    "进入实现阶段触发条件",
    "未来扩展（未落地）",
};

std::string Trim(std::string text) {
    const auto is_space = [](unsigned char ch) { return std::isspace(ch) != 0; };
    while (!text.empty() && is_space(static_cast<unsigned char>(text.front()))) {
        text.erase(text.begin());
    }
    while (!text.empty() && is_space(static_cast<unsigned char>(text.back()))) {
        text.pop_back();
    }
    return text;
}

std::string JsonEscape(const std::string& text) {
    std::ostringstream oss;
    for (const char ch : text) {
        switch (ch) {
            case '"':
                oss << "\\\"";
                break;
            case '\\':
                oss << "\\\\";
                break;
            case '\n':
                oss << "\\n";
                break;
            case '\r':
                oss << "\\r";
                break;
            case '\t':
                oss << "\\t";
                break;
            default:
                oss << ch;
                break;
        }
    }
    return oss.str();
}

bool ReadTextFile(const std::string& path, std::string* out, std::string* error) {
    if (out == nullptr) {
        return false;
    }
    std::ifstream input(path);
    if (!input.is_open()) {
        if (error != nullptr) {
            *error = "requirements file not found: " + path;
        }
        return false;
    }
    std::ostringstream buffer;
    buffer << input.rdbuf();
    *out = buffer.str();
    return true;
}

bool FindKeyValueStart(const std::string& text, const std::string& key, std::size_t* out_start) {
    if (out_start == nullptr) {
        return false;
    }
    const std::string quoted_key = "\"" + key + "\"";
    const std::size_t key_pos = text.find(quoted_key);
    if (key_pos == std::string::npos) {
        return false;
    }
    const std::size_t colon_pos = text.find(':', key_pos + quoted_key.size());
    if (colon_pos == std::string::npos) {
        return false;
    }

    std::size_t pos = colon_pos + 1;
    while (pos < text.size() && std::isspace(static_cast<unsigned char>(text[pos])) != 0) {
        ++pos;
    }
    *out_start = pos;
    return true;
}

bool ExtractBalancedSegment(const std::string& text, std::size_t open_pos, char open_ch,
                            char close_ch, std::string* out, std::size_t* out_end,
                            std::string* error) {
    if (out == nullptr) {
        return false;
    }
    if (open_pos >= text.size() || text[open_pos] != open_ch) {
        if (error != nullptr) {
            *error = "malformed json segment";
        }
        return false;
    }

    int depth = 0;
    bool in_string = false;
    bool escaped = false;
    for (std::size_t index = open_pos; index < text.size(); ++index) {
        const char ch = text[index];
        if (in_string) {
            if (escaped) {
                escaped = false;
            } else if (ch == '\\') {
                escaped = true;
            } else if (ch == '"') {
                in_string = false;
            }
            continue;
        }
        if (ch == '"') {
            in_string = true;
            continue;
        }
        if (ch == open_ch) {
            ++depth;
            continue;
        }
        if (ch == close_ch) {
            --depth;
            if (depth == 0) {
                *out = text.substr(open_pos, index - open_pos + 1);
                if (out_end != nullptr) {
                    *out_end = index + 1;
                }
                return true;
            }
            continue;
        }
    }

    if (error != nullptr) {
        *error = "unterminated json segment";
    }
    return false;
}

bool ParseJsonStringLiteral(const std::string& text, std::size_t* cursor, std::string* out,
                            std::string* error) {
    if (cursor == nullptr || out == nullptr) {
        return false;
    }
    if (*cursor >= text.size() || text[*cursor] != '"') {
        if (error != nullptr) {
            *error = "expected json string literal";
        }
        return false;
    }
    ++(*cursor);

    std::string value;
    bool escaped = false;
    while (*cursor < text.size()) {
        const char ch = text[*cursor];
        ++(*cursor);
        if (escaped) {
            switch (ch) {
                case '"':
                    value.push_back('"');
                    break;
                case '\\':
                    value.push_back('\\');
                    break;
                case 'n':
                    value.push_back('\n');
                    break;
                case 'r':
                    value.push_back('\r');
                    break;
                case 't':
                    value.push_back('\t');
                    break;
                default:
                    value.push_back(ch);
                    break;
            }
            escaped = false;
            continue;
        }
        if (ch == '\\') {
            escaped = true;
            continue;
        }
        if (ch == '"') {
            *out = value;
            return true;
        }
        value.push_back(ch);
    }

    if (error != nullptr) {
        *error = "unterminated json string literal";
    }
    return false;
}

bool ExtractJsonStringField(const std::string& object, const std::string& key, std::string* out) {
    return quant_hft::apps::ops_detail::ExtractJsonString(object, key, out);
}

bool ExtractJsonStringArrayField(const std::string& object, const std::string& key,
                                 std::vector<std::string>* values, std::string* error) {
    if (values == nullptr) {
        return false;
    }

    std::size_t value_start = 0;
    if (!FindKeyValueStart(object, key, &value_start)) {
        if (error != nullptr) {
            *error = "missing field: " + key;
        }
        return false;
    }
    if (value_start >= object.size() || object[value_start] != '[') {
        if (error != nullptr) {
            *error = key + " must be an array";
        }
        return false;
    }

    std::string array_segment;
    if (!ExtractBalancedSegment(object, value_start, '[', ']', &array_segment, nullptr, error)) {
        return false;
    }

    std::size_t cursor = 1;
    std::vector<std::string> parsed;
    while (cursor < array_segment.size()) {
        while (cursor < array_segment.size() &&
               std::isspace(static_cast<unsigned char>(array_segment[cursor])) != 0) {
            ++cursor;
        }
        if (cursor >= array_segment.size() || array_segment[cursor] == ']') {
            break;
        }

        if (array_segment[cursor] == ',') {
            ++cursor;
            continue;
        }

        std::string item;
        if (!ParseJsonStringLiteral(array_segment, &cursor, &item, error)) {
            return false;
        }
        parsed.push_back(item);

        while (cursor < array_segment.size() &&
               std::isspace(static_cast<unsigned char>(array_segment[cursor])) != 0) {
            ++cursor;
        }
        if (cursor < array_segment.size() && array_segment[cursor] == ',') {
            ++cursor;
        }
    }

    *values = std::move(parsed);
    return true;
}

bool ParseRequirements(const std::string& payload, std::vector<RequirementItem>* requirements,
                       std::string* error) {
    if (requirements == nullptr) {
        return false;
    }

    std::size_t value_start = 0;
    if (!FindKeyValueStart(payload, "requirements", &value_start)) {
        if (error != nullptr) {
            *error = "requirements must be a non-empty list";
        }
        return false;
    }
    if (value_start >= payload.size() || payload[value_start] != '[') {
        if (error != nullptr) {
            *error = "requirements must be a non-empty list";
        }
        return false;
    }

    std::string requirements_array;
    if (!ExtractBalancedSegment(payload, value_start, '[', ']', &requirements_array, nullptr,
                                error)) {
        return false;
    }

    std::vector<RequirementItem> parsed;
    std::size_t cursor = 1;
    while (cursor < requirements_array.size()) {
        while (cursor < requirements_array.size() &&
               std::isspace(static_cast<unsigned char>(requirements_array[cursor])) != 0) {
            ++cursor;
        }
        if (cursor >= requirements_array.size() || requirements_array[cursor] == ']') {
            break;
        }
        if (requirements_array[cursor] == ',') {
            ++cursor;
            continue;
        }

        if (requirements_array[cursor] != '{') {
            if (error != nullptr) {
                *error = "requirements array contains non-object entry";
            }
            return false;
        }

        std::string object_payload;
        std::size_t object_end = 0;
        if (!ExtractBalancedSegment(requirements_array, cursor, '{', '}', &object_payload,
                                    &object_end, error)) {
            return false;
        }
        cursor = object_end;

        RequirementItem item;
        ExtractJsonStringField(object_payload, "id", &item.id);
        ExtractJsonStringField(object_payload, "doc", &item.doc);
        ExtractJsonStringField(object_payload, "description", &item.description);

        std::vector<std::string> code_paths;
        std::vector<std::string> test_paths;
        std::vector<std::string> evidence_paths;
        if (!ExtractJsonStringArrayField(object_payload, "code_paths", &code_paths, error) ||
            !ExtractJsonStringArrayField(object_payload, "test_paths", &test_paths, error) ||
            !ExtractJsonStringArrayField(object_payload, "evidence_paths", &evidence_paths,
                                         error)) {
            return false;
        }

        item.code_paths = std::move(code_paths);
        item.test_paths = std::move(test_paths);
        item.evidence_paths = std::move(evidence_paths);
        parsed.push_back(std::move(item));
    }

    if (parsed.empty()) {
        if (error != nullptr) {
            *error = "requirements must be a non-empty list";
        }
        return false;
    }

    *requirements = std::move(parsed);
    return true;
}

std::map<std::string, std::filesystem::path> CollectDevelopDocs(const std::filesystem::path& root) {
    std::map<std::string, std::filesystem::path> docs;
    if (!std::filesystem::exists(root)) {
        return docs;
    }

    for (const auto& entry : std::filesystem::recursive_directory_iterator(root)) {
        if (!entry.is_regular_file()) {
            continue;
        }
        if (entry.path().extension() != ".md") {
            continue;
        }
        const auto relative = std::filesystem::relative(entry.path(), root).generic_string();
        docs["develop/" + relative] = entry.path();
    }
    return docs;
}

bool PathExists(const std::string& repo_path,
                const std::set<std::filesystem::path>& generated_paths) {
    const std::filesystem::path candidate(repo_path);
    if (std::filesystem::exists(candidate)) {
        return true;
    }
    std::error_code ec;
    const std::filesystem::path normalized = std::filesystem::weakly_canonical(candidate, ec);
    if (!ec && generated_paths.find(normalized) != generated_paths.end()) {
        return true;
    }
    if (generated_paths.find(candidate) != generated_paths.end()) {
        return true;
    }
    return false;
}

void VerifyStringList(const std::string& req_id, const std::string& field,
                      const std::vector<std::string>& values, std::vector<std::string>* errors) {
    if (errors == nullptr) {
        return;
    }
    if (values.empty()) {
        errors->push_back(req_id + ": " + field + " must be a non-empty list");
        return;
    }
    for (std::size_t index = 0; index < values.size(); ++index) {
        if (Trim(values[index]).empty()) {
            errors->push_back(req_id + ": " + field + "[" + std::to_string(index) +
                              "] must be a non-empty string");
        }
    }
}

std::vector<ForbiddenFinding> ScanForbiddenLanguage(
    const std::map<std::string, std::filesystem::path>& docs, const std::vector<std::string>& terms,
    std::vector<std::string>* errors) {
    std::vector<ForbiddenFinding> findings;
    for (const auto& [doc_key, path] : docs) {
        std::ifstream input(path);
        if (!input.is_open()) {
            if (errors != nullptr) {
                errors->push_back(doc_key + ": failed to read document");
            }
            continue;
        }

        std::vector<std::string> lines;
        std::string line;
        while (std::getline(input, line)) {
            lines.push_back(line);
        }

        for (const std::string& term : terms) {
            std::vector<int> hits;
            for (std::size_t index = 0; index < lines.size(); ++index) {
                if (lines[index].find(term) != std::string::npos) {
                    hits.push_back(static_cast<int>(index + 1));
                }
            }
            if (hits.empty()) {
                continue;
            }

            ForbiddenFinding finding;
            finding.doc = doc_key;
            finding.term = term;
            finding.line_numbers = hits;
            findings.push_back(finding);

            if (errors != nullptr) {
                std::ostringstream message;
                message << doc_key << ": contains forbidden completion language '" << term
                        << "' at lines ";
                for (std::size_t idx = 0; idx < hits.size(); ++idx) {
                    if (idx != 0) {
                        message << ',';
                    }
                    message << hits[idx];
                }
                errors->push_back(message.str());
            }
        }
    }
    return findings;
}

bool WriteCompletionLanguageReport(const std::string& path,
                                   const std::filesystem::path& develop_root,
                                   const std::vector<std::string>& forbidden_terms,
                                   std::size_t docs_scanned,
                                   const std::vector<ForbiddenFinding>& findings,
                                   std::string* error) {
    std::ostringstream json;
    json << "{\n";
    json << "  \"develop_root\": \"" << JsonEscape(develop_root.generic_string()) << "\",\n";

    json << "  \"forbidden_terms\": [";
    for (std::size_t index = 0; index < forbidden_terms.size(); ++index) {
        if (index != 0) {
            json << ", ";
        }
        json << "\"" << JsonEscape(forbidden_terms[index]) << "\"";
    }
    json << "],\n";

    json << "  \"docs_scanned\": " << docs_scanned << ",\n";
    json << "  \"finding_count\": " << findings.size() << ",\n";
    json << "  \"findings\": [\n";
    for (std::size_t index = 0; index < findings.size(); ++index) {
        const ForbiddenFinding& finding = findings[index];
        json << "    {\n";
        json << "      \"doc\": \"" << JsonEscape(finding.doc) << "\",\n";
        json << "      \"term\": \"" << JsonEscape(finding.term) << "\",\n";
        json << "      \"line_numbers\": [";
        for (std::size_t line_index = 0; line_index < finding.line_numbers.size(); ++line_index) {
            if (line_index != 0) {
                json << ", ";
            }
            json << finding.line_numbers[line_index];
        }
        json << "]\n";
        json << "    }";
        if (index + 1 < findings.size()) {
            json << ',';
        }
        json << "\n";
    }
    json << "  ]\n";
    json << "}\n";

    std::filesystem::path report_path(path);
    std::error_code ec;
    std::filesystem::create_directories(report_path.parent_path(), ec);

    std::ofstream output(path, std::ios::out | std::ios::trunc);
    if (!output.is_open()) {
        if (error != nullptr) {
            *error = "unable to write report file: " + path;
        }
        return false;
    }
    output << json.str();
    return true;
}

bool ParseOptions(int argc, char** argv, Options* out, std::string* error) {
    if (out == nullptr) {
        return false;
    }
    Options options;

    for (int index = 1; index < argc; ++index) {
        const std::string token = argv[index];
        auto require_value = [&](const std::string& flag, std::string* value) -> bool {
            if (index + 1 >= argc) {
                if (error != nullptr) {
                    *error = "missing value for " + flag;
                }
                return false;
            }
            *value = argv[++index];
            return true;
        };

        if (token == "--requirements-file") {
            if (!require_value(token, &options.requirements_file)) {
                return false;
            }
            continue;
        }
        if (token == "--develop-root") {
            if (!require_value(token, &options.develop_root)) {
                return false;
            }
            continue;
        }
        if (token == "--forbidden-term") {
            std::string term;
            if (!require_value(token, &term)) {
                return false;
            }
            options.forbidden_terms.push_back(term);
            continue;
        }
        if (token == "--completion-language-report") {
            if (!require_value(token, &options.completion_language_report)) {
                return false;
            }
            continue;
        }

        if (error != nullptr) {
            *error = "unknown argument: " + token;
        }
        return false;
    }

    if (options.forbidden_terms.empty()) {
        options.forbidden_terms = kDefaultForbiddenTerms;
    }

    *out = std::move(options);
    return true;
}

}  // namespace

int main(int argc, char** argv) {
    Options options;
    std::string error;
    if (!ParseOptions(argc, argv, &options, &error)) {
        std::cerr << "verify_develop_requirements_cli: " << error << '\n';
        return 2;
    }

    std::string payload;
    if (!ReadTextFile(options.requirements_file, &payload, &error)) {
        std::cerr << "error: " << error << '\n';
        return 2;
    }

    std::vector<RequirementItem> requirements;
    if (!ParseRequirements(payload, &requirements, &error)) {
        std::cerr << "error: " << error << '\n';
        return 2;
    }

    const std::filesystem::path develop_root(options.develop_root);
    if (!std::filesystem::exists(develop_root)) {
        std::cerr << "error: develop root not found: " << options.develop_root << '\n';
        return 2;
    }

    const auto docs = CollectDevelopDocs(develop_root);
    if (docs.empty()) {
        std::cerr << "error: no develop markdown files were found under: " << options.develop_root
                  << '\n';
        return 2;
    }

    std::set<std::filesystem::path> generated_paths;
    if (!options.completion_language_report.empty()) {
        std::error_code ec;
        generated_paths.insert(
            std::filesystem::weakly_canonical(options.completion_language_report, ec));
        generated_paths.insert(std::filesystem::path(options.completion_language_report));
    }

    std::set<std::string> seen_ids;
    std::set<std::string> covered_docs;
    std::vector<std::string> errors;

    for (std::size_t index = 0; index < requirements.size(); ++index) {
        const RequirementItem& requirement = requirements[index];
        std::string req_id = Trim(requirement.id);
        if (req_id.empty()) {
            errors.push_back("requirements[" + std::to_string(index) + "] missing id");
            req_id = "requirements[" + std::to_string(index) + "]";
        } else if (seen_ids.find(req_id) != seen_ids.end()) {
            errors.push_back("duplicate requirement id: " + req_id);
        } else {
            seen_ids.insert(req_id);
        }

        const std::string doc = Trim(requirement.doc);
        if (doc.empty()) {
            errors.push_back(req_id + ": doc must be a non-empty string");
        } else if (docs.find(doc) == docs.end()) {
            errors.push_back(req_id + ": doc does not map to existing develop markdown: " + doc);
        } else {
            covered_docs.insert(doc);
        }

        if (Trim(requirement.description).empty()) {
            errors.push_back(req_id + ": description must be a non-empty string");
        }

        VerifyStringList(req_id, "code_paths", requirement.code_paths, &errors);
        VerifyStringList(req_id, "test_paths", requirement.test_paths, &errors);
        VerifyStringList(req_id, "evidence_paths", requirement.evidence_paths, &errors);

        for (const auto& [field_name, values] :
             std::vector<std::pair<std::string, std::vector<std::string>>>{
                 {"code_paths", requirement.code_paths},
                 {"test_paths", requirement.test_paths},
                 {"evidence_paths", requirement.evidence_paths},
             }) {
            for (const std::string& repo_path : values) {
                if (!PathExists(repo_path, generated_paths)) {
                    errors.push_back(req_id + ": missing path in " + field_name + ": " + repo_path);
                }
            }
        }
    }

    std::vector<std::string> all_doc_keys;
    all_doc_keys.reserve(docs.size());
    for (const auto& [doc_key, _path] : docs) {
        all_doc_keys.push_back(doc_key);
    }
    std::sort(all_doc_keys.begin(), all_doc_keys.end());

    std::vector<std::string> missing_docs;
    for (const std::string& doc_key : all_doc_keys) {
        if (covered_docs.find(doc_key) == covered_docs.end()) {
            missing_docs.push_back(doc_key);
        }
    }
    if (!missing_docs.empty()) {
        std::ostringstream message;
        message << "requirements file does not cover all develop docs: ";
        for (std::size_t index = 0; index < missing_docs.size(); ++index) {
            if (index != 0) {
                message << ", ";
            }
            message << missing_docs[index];
        }
        errors.push_back(message.str());
    }

    const auto forbidden_findings = ScanForbiddenLanguage(docs, options.forbidden_terms, &errors);

    if (!options.completion_language_report.empty()) {
        if (!WriteCompletionLanguageReport(options.completion_language_report, develop_root,
                                           options.forbidden_terms, docs.size(), forbidden_findings,
                                           &error)) {
            errors.push_back(error);
        }
    }

    if (!errors.empty()) {
        std::cout << "verification failed\n";
        for (const std::string& item : errors) {
            std::cout << "- " << item << '\n';
        }
        return 2;
    }

    std::cout << "verified requirements: requirements=" << requirements.size()
              << " docs_covered=" << covered_docs.size() << " docs_scanned=" << docs.size() << '\n';
    return 0;
}
