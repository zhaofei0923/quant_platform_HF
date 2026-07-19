#include "quant_hft/core/instrument_meta_cache.h"

#include <algorithm>
#include <cctype>
#include <cerrno>
#include <chrono>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <sstream>
#include <unordered_set>

#if !defined(_WIN32)
#include <fcntl.h>
#include <unistd.h>
#endif

namespace quant_hft {
namespace {

constexpr EpochNanos kNanosPerMillisecond = 1'000'000;

void SetError(std::string* error, const std::string& value) {
    if (error != nullptr) {
        *error = value;
    }
}

std::string Lowercase(std::string value) {
    std::transform(value.begin(), value.end(), value.begin(),
                   [](unsigned char ch) { return static_cast<char>(std::tolower(ch)); });
    return value;
}

std::string JsonEscape(const std::string& value) {
    std::string escaped;
    escaped.reserve(value.size());
    for (const char ch : value) {
        switch (ch) {
            case '\\':
                escaped += "\\\\";
                break;
            case '"':
                escaped += "\\\"";
                break;
            case '\n':
                escaped += "\\n";
                break;
            case '\r':
                escaped += "\\r";
                break;
            case '\t':
                escaped += "\\t";
                break;
            default:
                escaped.push_back(ch);
                break;
        }
    }
    return escaped;
}

std::string ExtractString(const std::string& line) {
    const auto colon = line.find(':');
    const auto open = colon == std::string::npos ? std::string::npos : line.find('"', colon + 1);
    if (open == std::string::npos) {
        return {};
    }
    std::string value;
    bool escaped = false;
    for (std::size_t index = open + 1; index < line.size(); ++index) {
        const char ch = line[index];
        if (escaped) {
            value.push_back(ch == 'n' ? '\n' : (ch == 'r' ? '\r' : (ch == 't' ? '\t' : ch)));
            escaped = false;
        } else if (ch == '\\') {
            escaped = true;
        } else if (ch == '"') {
            break;
        } else {
            value.push_back(ch);
        }
    }
    return value;
}

std::string ExtractScalar(const std::string& line) {
    const auto colon = line.find(':');
    if (colon == std::string::npos) {
        return {};
    }
    std::string value = line.substr(colon + 1);
    while (!value.empty() && (std::isspace(static_cast<unsigned char>(value.front())) != 0)) {
        value.erase(value.begin());
    }
    while (!value.empty() &&
           (std::isspace(static_cast<unsigned char>(value.back())) != 0 || value.back() == ',')) {
        value.pop_back();
    }
    return value;
}

bool IsValidDate(const std::string& value) {
    return value.size() == 8 && std::all_of(value.begin(), value.end(),
                                            [](unsigned char ch) { return std::isdigit(ch) != 0; });
}

#if !defined(_WIN32)
bool FsyncPath(const std::filesystem::path& path, bool directory, std::string* error) {
    const int fd = ::open(path.c_str(), directory ? (O_RDONLY | O_DIRECTORY) : O_RDONLY);
    if (fd < 0) {
        SetError(error, "open for fsync failed: " + std::string(std::strerror(errno)));
        return false;
    }
    const int rc = ::fsync(fd);
    const int saved_errno = errno;
    ::close(fd);
    if (rc != 0) {
        SetError(error, "fsync failed: " + std::string(std::strerror(saved_errno)));
        return false;
    }
    return true;
}
#endif

}  // namespace

std::string ExtractFuturesProductId(const std::string& instrument_id) {
    const auto dot = instrument_id.find('.');
    const std::string symbol =
        dot == std::string::npos ? instrument_id : instrument_id.substr(dot + 1);
    std::string product_id;
    for (const unsigned char ch : symbol) {
        if (std::isalpha(ch) == 0) {
            break;
        }
        product_id.push_back(static_cast<char>(std::tolower(ch)));
    }
    return product_id;
}

bool IsPlainFuturesContract(const std::string& instrument_id, const std::string& product_id) {
    const auto dot = instrument_id.find('.');
    const std::string symbol =
        Lowercase(dot == std::string::npos ? instrument_id : instrument_id.substr(dot + 1));
    const std::string normalized_product = Lowercase(product_id);
    if (symbol.size() <= normalized_product.size() || symbol.rfind(normalized_product, 0) != 0) {
        return false;
    }
    return std::all_of(symbol.begin() + static_cast<std::ptrdiff_t>(normalized_product.size()),
                       symbol.end(), [](unsigned char ch) { return std::isdigit(ch) != 0; });
}

bool LoadInstrumentMetaCacheDocument(const std::string& path, InstrumentMetaCacheDocument* document,
                                     std::string* error) {
    if (document == nullptr) {
        SetError(error, "instrument cache output is null");
        return false;
    }
    *document = {};
    std::ifstream input(path, std::ios::in | std::ios::binary);
    if (!input.is_open()) {
        SetError(error, "cache file not found: " + path);
        return false;
    }
    char first = '\0';
    while (input.get(first) && std::isspace(static_cast<unsigned char>(first)) != 0) {
    }
    if (!input.good() && first == '\0') {
        SetError(error, "instrument cache is empty");
        return false;
    }
    input.clear();
    input.seekg(0);
    document->legacy = first == '[';
    document->schema_version = document->legacy ? 1 : 0;

    InstrumentMetaSnapshot current;
    bool in_instrument = false;
    bool saw_document_close = false;
    std::string line;
    while (std::getline(input, line)) {
        const auto first_non_space = line.find_first_not_of(" \t\r\n");
        if (!in_instrument && first_non_space != std::string::npos &&
            ((!document->legacy && line.substr(first_non_space) == "}") ||
             (document->legacy && line.substr(first_non_space) == "]"))) {
            saw_document_close = true;
        }
        if (!document->legacy && line.find("\"schema_version\"") != std::string::npos) {
            try {
                document->schema_version = std::stoi(ExtractScalar(line));
            } catch (...) {
                document->schema_version = 0;
            }
        } else if (!document->legacy && line.find("\"broker_trading_day\"") != std::string::npos) {
            document->broker_trading_day = ExtractString(line);
        } else if (!document->legacy && line.find("\"generated_ts_ns\"") != std::string::npos) {
            try {
                document->generated_ts_ns = std::stoll(ExtractScalar(line));
            } catch (...) {
                document->generated_ts_ns = 0;
            }
        } else if (!document->legacy && !in_instrument &&
                   line.find("\"product_id\"") != std::string::npos) {
            document->product_id = Lowercase(ExtractString(line));
        } else if (line.find("\"instrument_id\"") != std::string::npos) {
            current = {};
            current.instrument_id = ExtractString(line);
            in_instrument = true;
        } else if (in_instrument && line.find("\"exchange_id\"") != std::string::npos) {
            current.exchange_id = ExtractString(line);
        } else if (in_instrument && line.find("\"product_id\"") != std::string::npos) {
            current.product_id = Lowercase(ExtractString(line));
        } else if (in_instrument && line.find("\"volume_multiple\"") != std::string::npos) {
            try {
                current.volume_multiple = std::stoi(ExtractScalar(line));
            } catch (...) {
                current.volume_multiple = 0;
            }
        } else if (in_instrument && line.find("\"price_tick\"") != std::string::npos) {
            try {
                current.price_tick = std::stod(ExtractScalar(line));
            } catch (...) {
                current.price_tick = 0.0;
            }
        } else if (in_instrument &&
                   line.find("\"max_margin_side_algorithm\"") != std::string::npos) {
            current.max_margin_side_algorithm = ExtractScalar(line) == "true";
        } else if (in_instrument && line.find("\"ts_ns\"") != std::string::npos) {
            try {
                current.ts_ns = std::stoll(ExtractScalar(line));
            } catch (...) {
                current.ts_ns = 0;
            }
        } else if (in_instrument && line.find("\"source\"") != std::string::npos) {
            current.source = ExtractString(line);
        } else if (in_instrument && line.find("\"open_date\"") != std::string::npos) {
            current.open_date = ExtractString(line);
        } else if (in_instrument && line.find("\"expire_date\"") != std::string::npos) {
            current.expire_date = ExtractString(line);
        } else if (in_instrument && line.find("\"is_trading\"") != std::string::npos) {
            current.is_trading = ExtractScalar(line) == "true";
        } else if (in_instrument && line.find("\"product_class\"") != std::string::npos) {
            current.product_class = ExtractString(line);
        }

        if (!in_instrument || first_non_space == std::string::npos ||
            line[first_non_space] != '}') {
            continue;
        }
        if (!current.instrument_id.empty()) {
            document->instruments.push_back(current);
        }
        current = {};
        in_instrument = false;
    }
    if (!input.eof()) {
        SetError(error, "failed while reading instrument cache");
        return false;
    }
    if (!saw_document_close) {
        SetError(error, "instrument cache is truncated");
        return false;
    }
    if (document->legacy && document->instruments.empty()) {
        SetError(error, "legacy instrument cache contains no instruments");
        return false;
    }
    if (!document->legacy && document->schema_version != 2) {
        SetError(error, "unsupported instrument cache schema version");
        return false;
    }
    if (!document->legacy &&
        (document->product_id.empty() || !IsValidDate(document->broker_trading_day) ||
         document->generated_ts_ns <= 0 || document->instruments.empty())) {
        SetError(error, "instrument cache v2 is incomplete");
        return false;
    }
    std::sort(
        document->instruments.begin(), document->instruments.end(),
        [](const auto& lhs, const auto& rhs) { return lhs.instrument_id < rhs.instrument_id; });
    document->instruments.erase(
        std::unique(document->instruments.begin(), document->instruments.end(),
                    [](const auto& lhs, const auto& rhs) {
                        return lhs.instrument_id == rhs.instrument_id;
                    }),
        document->instruments.end());
    return true;
}

bool WriteInstrumentMetaCacheV2Atomically(const std::string& path, const std::string& product_id,
                                          const std::string& broker_trading_day,
                                          const std::vector<InstrumentMetaSnapshot>& instruments,
                                          EpochNanos generated_ts_ns, std::string* error) {
    if (path.empty() || product_id.empty() || !IsValidDate(broker_trading_day) ||
        instruments.empty()) {
        SetError(error, "invalid instrument cache v2 arguments");
        return false;
    }
    std::vector<InstrumentMetaSnapshot> sorted = instruments;
    std::sort(sorted.begin(), sorted.end(), [](const auto& lhs, const auto& rhs) {
        return lhs.instrument_id < rhs.instrument_id;
    });
    std::ostringstream payload;
    payload << std::setprecision(17);
    payload << "{\n"
            << "  \"schema_version\": 2,\n"
            << "  \"product_id\": \"" << JsonEscape(Lowercase(product_id)) << "\",\n"
            << "  \"broker_trading_day\": \"" << JsonEscape(broker_trading_day) << "\",\n"
            << "  \"generated_ts_ns\": " << generated_ts_ns << ",\n"
            << "  \"instruments\": [\n";
    for (std::size_t index = 0; index < sorted.size(); ++index) {
        const auto& row = sorted[index];
        payload << "    {\n"
                << "      \"instrument_id\": \"" << JsonEscape(row.instrument_id) << "\",\n"
                << "      \"exchange_id\": \"" << JsonEscape(row.exchange_id) << "\",\n"
                << "      \"product_id\": \"" << JsonEscape(Lowercase(row.product_id)) << "\",\n"
                << "      \"volume_multiple\": " << row.volume_multiple << ",\n"
                << "      \"price_tick\": " << row.price_tick << ",\n"
                << "      \"max_margin_side_algorithm\": "
                << (row.max_margin_side_algorithm ? "true" : "false") << ",\n"
                << "      \"ts_ns\": " << row.ts_ns << ",\n"
                << "      \"source\": \"" << JsonEscape(row.source) << "\",\n"
                << "      \"open_date\": \"" << JsonEscape(row.open_date) << "\",\n"
                << "      \"expire_date\": \"" << JsonEscape(row.expire_date) << "\",\n"
                << "      \"is_trading\": " << (row.is_trading ? "true" : "false") << ",\n"
                << "      \"product_class\": \"" << JsonEscape(row.product_class) << "\"\n"
                << "    }" << (index + 1U == sorted.size() ? "\n" : ",\n");
    }
    payload << "  ]\n}\n";

    const std::filesystem::path output(path);
    std::error_code ec;
    if (!output.parent_path().empty()) {
        std::filesystem::create_directories(output.parent_path(), ec);
        if (ec) {
            SetError(error, "failed to create instrument cache directory: " + ec.message());
            return false;
        }
    }
    const auto suffix = std::chrono::duration_cast<std::chrono::nanoseconds>(
                            std::chrono::system_clock::now().time_since_epoch())
                            .count();
    const std::filesystem::path temporary = output.string() + ".tmp." + std::to_string(suffix);
    {
        std::ofstream stream(temporary, std::ios::out | std::ios::trunc | std::ios::binary);
        if (!stream.is_open()) {
            SetError(error, "failed to open instrument cache temp file");
            return false;
        }
        stream << payload.str();
        stream.flush();
        if (!stream.good()) {
            stream.close();
            std::filesystem::remove(temporary, ec);
            SetError(error, "failed to flush instrument cache temp file");
            return false;
        }
    }
#if !defined(_WIN32)
    if (!FsyncPath(temporary, false, error)) {
        std::filesystem::remove(temporary, ec);
        return false;
    }
#endif
    std::filesystem::rename(temporary, output, ec);
    if (ec) {
        std::filesystem::remove(temporary, ec);
        SetError(error, "failed to publish instrument cache: " + ec.message());
        return false;
    }
#if !defined(_WIN32)
    const auto parent =
        output.parent_path().empty() ? std::filesystem::path(".") : output.parent_path();
    if (!FsyncPath(parent, true, error)) {
        return false;
    }
#endif
    return true;
}

bool IsInstrumentMetaCacheCurrent(const InstrumentMetaCacheDocument& document,
                                  const std::string& broker_trading_day, EpochNanos now_ns,
                                  std::int64_t max_age_ms, std::string* reason) {
    if (document.legacy) {
        SetError(reason, "legacy_schema_requires_refresh");
        return false;
    }
    if (document.schema_version != 2) {
        SetError(reason, "unsupported_schema");
        return false;
    }
    if (!IsValidDate(broker_trading_day) || document.broker_trading_day != broker_trading_day) {
        SetError(reason, "broker_trading_day_mismatch");
        return false;
    }
    if (document.generated_ts_ns <= 0 || now_ns < document.generated_ts_ns ||
        now_ns - document.generated_ts_ns >
            std::max<std::int64_t>(1, max_age_ms) * kNanosPerMillisecond) {
        SetError(reason, "cache_age_exceeded");
        return false;
    }
    if (document.instruments.empty()) {
        SetError(reason, "cache_empty");
        return false;
    }
    return true;
}

std::vector<InstrumentMetaSnapshot> CollectProductInstrumentMetadata(
    const std::vector<InstrumentMetaSnapshot>& instruments, const std::string& product_id) {
    const std::string normalized_product = Lowercase(product_id);
    std::vector<InstrumentMetaSnapshot> product_instruments;
    std::unordered_set<std::string> seen;
    for (const auto& row : instruments) {
        const std::string row_product = row.product_id.empty()
                                            ? ExtractFuturesProductId(row.instrument_id)
                                            : Lowercase(row.product_id);
        if (row_product != normalized_product || row.instrument_id.empty() ||
            !seen.insert(row.instrument_id).second) {
            continue;
        }
        product_instruments.push_back(row);
    }
    std::sort(
        product_instruments.begin(), product_instruments.end(),
        [](const auto& lhs, const auto& rhs) { return lhs.instrument_id < rhs.instrument_id; });
    return product_instruments;
}

std::vector<InstrumentMetaSnapshot> FilterEligibleFuturesContracts(
    const std::vector<InstrumentMetaSnapshot>& instruments, const std::string& product_id,
    const std::string& broker_trading_day) {
    std::vector<InstrumentMetaSnapshot> eligible;
    std::unordered_set<std::string> seen;
    for (const auto& row : instruments) {
        if (!IsEligibleFuturesContract(row, product_id, broker_trading_day) ||
            !seen.insert(row.instrument_id).second) {
            continue;
        }
        eligible.push_back(row);
    }
    std::sort(eligible.begin(), eligible.end(), [](const auto& lhs, const auto& rhs) {
        return lhs.instrument_id < rhs.instrument_id;
    });
    return eligible;
}

bool IsEligibleFuturesContract(const InstrumentMetaSnapshot& instrument,
                               const std::string& product_id,
                               const std::string& broker_trading_day) {
    const std::string normalized_product = Lowercase(product_id);
    const std::string instrument_product = instrument.product_id.empty()
                                               ? ExtractFuturesProductId(instrument.instrument_id)
                                               : Lowercase(instrument.product_id);
    const bool futures_class =
        instrument.product_class == "1" || Lowercase(instrument.product_class) == "futures";
    return IsValidDate(broker_trading_day) && instrument_product == normalized_product &&
           IsPlainFuturesContract(instrument.instrument_id, normalized_product) &&
           instrument.is_trading && futures_class && IsValidDate(instrument.open_date) &&
           IsValidDate(instrument.expire_date) && instrument.open_date <= broker_trading_day &&
           instrument.expire_date >= broker_trading_day;
}

}  // namespace quant_hft
