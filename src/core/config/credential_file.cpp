#include "quant_hft/config/credential_file.h"

#include <sys/stat.h>
#include <unistd.h>

#include <cctype>
#include <cstdlib>
#include <fstream>
#include <regex>
#include <set>
#include <stdexcept>

namespace quant_hft {
namespace {

std::string StripInlineComment(std::string value) {
    char quote = '\0';
    bool escaped = false;
    for (std::size_t index = 0; index < value.size(); ++index) {
        const char current = value[index];
        if (escaped) {
            escaped = false;
            continue;
        }
        if (current == '\\' && quote != '\'') {
            escaped = true;
            continue;
        }
        if (quote != '\0') {
            if (current == quote) quote = '\0';
        } else if (current == '\'' || current == '"') {
            quote = current;
        } else if (current == '#' && index > 0 &&
                   std::isspace(static_cast<unsigned char>(value[index - 1]))) {
            value.resize(index);
            while (!value.empty() && std::isspace(static_cast<unsigned char>(value.back()))) {
                value.pop_back();
            }
            break;
        }
    }
    return value;
}

}  // namespace

void LoadCredentialFile(const std::string& path) {
    struct stat info {};
    if (lstat(path.c_str(), &info) != 0 || !S_ISREG(info.st_mode) || (info.st_mode & 0077) != 0 ||
        info.st_uid != geteuid()) {
        throw std::runtime_error("credential_ref must be an owner-only regular file (0600)");
    }
    std::ifstream input(path);
    std::string line;
    std::set<std::string> seen;
    static const std::regex key_pattern("CTP_[A-Z0-9_]+");
    while (std::getline(input, line)) {
        if (!line.empty() && line.back() == '\r') line.pop_back();
        if (line.empty() || line.front() == '#') continue;
        const auto split = line.find('=');
        if (split == std::string::npos)
            throw std::runtime_error("credential file expects KEY=value, no shell syntax");
        const auto key = line.substr(0, split);
        auto value = StripInlineComment(line.substr(split + 1));
        if (!std::regex_match(key, key_pattern) || !seen.insert(key).second) {
            throw std::runtime_error("credential file contains invalid or duplicate variable");
        }
        if (value.size() >= 2 && (value.front() == '\'' || value.front() == '"') &&
            value.back() == value.front()) {
            value = value.substr(1, value.size() - 2);
        }
        if (setenv(key.c_str(), value.c_str(), 1) != 0)
            throw std::runtime_error("cannot bind credential environment");
    }
}

}  // namespace quant_hft
