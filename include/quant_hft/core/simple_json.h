#pragma once

#include <cctype>
#include <map>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

namespace quant_hft::simple_json {

struct Value {
    enum class Type {
        kNull,
        kBool,
        kNumber,
        kString,
        kObject,
        kArray,
    };

    Type type{Type::kNull};
    bool bool_value{false};
    double number_value{0.0};
    std::string string_value;
    std::map<std::string, Value> object_value;
    std::vector<Value> array_value;

    bool IsNull() const { return type == Type::kNull; }
    bool IsBool() const { return type == Type::kBool; }
    bool IsNumber() const { return type == Type::kNumber; }
    bool IsString() const { return type == Type::kString; }
    bool IsObject() const { return type == Type::kObject; }
    bool IsArray() const { return type == Type::kArray; }

    const Value* Find(const std::string& key) const {
        if (!IsObject()) {
            return nullptr;
        }
        const auto it = object_value.find(key);
        return it == object_value.end() ? nullptr : &it->second;
    }

    std::string ToString() const {
        if (IsString()) {
            return string_value;
        }
        if (IsBool()) {
            return bool_value ? "true" : "false";
        }
        if (IsNumber()) {
            std::ostringstream oss;
            oss << number_value;
            return oss.str();
        }
        if (IsNull()) {
            return "null";
        }
        return "";
    }
};

namespace detail {

inline std::string Trim(const std::string& input) {
    std::size_t begin = 0;
    while (begin < input.size() && std::isspace(static_cast<unsigned char>(input[begin])) != 0) {
        ++begin;
    }
    std::size_t end = input.size();
    while (end > begin && std::isspace(static_cast<unsigned char>(input[end - 1])) != 0) {
        --end;
    }
    return input.substr(begin, end - begin);
}

class Parser {
   public:
    explicit Parser(const std::string& text) : text_(text) {}

    bool Parse(Value* out, std::string* error) {
        if (out == nullptr) {
            if (error != nullptr) {
                *error = "json output is null";
            }
            return false;
        }
        SkipSpace();
        Value value;
        if (!ParseValue(&value, error)) {
            return false;
        }
        SkipSpace();
        if (!IsEnd()) {
            if (error != nullptr) {
                *error = "unexpected trailing characters in json";
            }
            return false;
        }
        *out = std::move(value);
        return true;
    }

   private:
    bool IsEnd() const { return pos_ >= text_.size(); }

    char Peek() const { return IsEnd() ? '\0' : text_[pos_]; }

    char Take() { return IsEnd() ? '\0' : text_[pos_++]; }

    void SkipSpace() {
        while (!IsEnd() && std::isspace(static_cast<unsigned char>(text_[pos_])) != 0) {
            ++pos_;
        }
    }

    bool ParseValue(Value* out, std::string* error) {
        SkipSpace();
        if (IsEnd()) {
            if (error != nullptr) {
                *error = "unexpected end of json";
            }
            return false;
        }
        const char ch = Peek();
        if (ch == '{') {
            return ParseObject(out, error);
        }
        if (ch == '[') {
            return ParseArray(out, error);
        }
        if (ch == '"') {
            out->type = Value::Type::kString;
            return ParseString(&out->string_value, error);
        }
        if (ch == 't' || ch == 'f') {
            return ParseBool(out, error);
        }
        if (ch == 'n') {
            return ParseNull(out, error);
        }
        return ParseNumber(out, error);
    }

    bool ParseObject(Value* out, std::string* error) {
        if (Take() != '{') {
            if (error != nullptr) {
                *error = "expected '{'";
            }
            return false;
        }
        out->type = Value::Type::kObject;
        out->object_value.clear();

        SkipSpace();
        if (Peek() == '}') {
            Take();
            return true;
        }

        while (true) {
            SkipSpace();
            std::string key;
            if (!ParseString(&key, error)) {
                return false;
            }
            SkipSpace();
            if (Take() != ':') {
                if (error != nullptr) {
                    *error = "expected ':' in object";
                }
                return false;
            }
            Value value;
            if (!ParseValue(&value, error)) {
                return false;
            }
            out->object_value[key] = std::move(value);

            SkipSpace();
            const char next = Take();
            if (next == '}') {
                return true;
            }
            if (next != ',') {
                if (error != nullptr) {
                    *error = "expected ',' or '}' in object";
                }
                return false;
            }
        }
    }

    bool ParseArray(Value* out, std::string* error) {
        if (Take() != '[') {
            if (error != nullptr) {
                *error = "expected '['";
            }
            return false;
        }
        out->type = Value::Type::kArray;
        out->array_value.clear();

        SkipSpace();
        if (Peek() == ']') {
            Take();
            return true;
        }

        while (true) {
            Value item;
            if (!ParseValue(&item, error)) {
                return false;
            }
            out->array_value.push_back(std::move(item));

            SkipSpace();
            const char next = Take();
            if (next == ']') {
                return true;
            }
            if (next != ',') {
                if (error != nullptr) {
                    *error = "expected ',' or ']' in array";
                }
                return false;
            }
        }
    }

    bool ParseString(std::string* out, std::string* error) {
        if (out == nullptr) {
            if (error != nullptr) {
                *error = "string output is null";
            }
            return false;
        }
        if (Take() != '"') {
            if (error != nullptr) {
                *error = "expected '\"'";
            }
            return false;
        }
        std::string result;
        while (!IsEnd()) {
            const char ch = Take();
            if (ch == '"') {
                *out = std::move(result);
                return true;
            }
            if (ch != '\\') {
                result.push_back(ch);
                continue;
            }
            if (IsEnd()) {
                break;
            }
            const char escaped = Take();
            switch (escaped) {
                case '"':
                    result.push_back('"');
                    break;
                case '\\':
                    result.push_back('\\');
                    break;
                case '/':
                    result.push_back('/');
                    break;
                case 'b':
                    result.push_back('\b');
                    break;
                case 'f':
                    result.push_back('\f');
                    break;
                case 'n':
                    result.push_back('\n');
                    break;
                case 'r':
                    result.push_back('\r');
                    break;
                case 't':
                    result.push_back('\t');
                    break;
                default:
                    if (error != nullptr) {
                        *error = "unsupported escape sequence";
                    }
                    return false;
            }
        }
        if (error != nullptr) {
            *error = "unterminated string";
        }
        return false;
    }

    bool ParseBool(Value* out, std::string* error) {
        if (text_.compare(pos_, 4, "true") == 0) {
            pos_ += 4;
            out->type = Value::Type::kBool;
            out->bool_value = true;
            return true;
        }
        if (text_.compare(pos_, 5, "false") == 0) {
            pos_ += 5;
            out->type = Value::Type::kBool;
            out->bool_value = false;
            return true;
        }
        if (error != nullptr) {
            *error = "invalid bool token";
        }
        return false;
    }

    bool ParseNull(Value* out, std::string* error) {
        if (text_.compare(pos_, 4, "null") != 0) {
            if (error != nullptr) {
                *error = "invalid null token";
            }
            return false;
        }
        pos_ += 4;
        out->type = Value::Type::kNull;
        return true;
    }

    bool ParseNumber(Value* out, std::string* error) {
        const std::size_t begin = pos_;
        if (Peek() == '-') {
            ++pos_;
        }
        bool has_digit = false;
        while (!IsEnd() && std::isdigit(static_cast<unsigned char>(Peek())) != 0) {
            has_digit = true;
            ++pos_;
        }
        if (!IsEnd() && Peek() == '.') {
            ++pos_;
            while (!IsEnd() && std::isdigit(static_cast<unsigned char>(Peek())) != 0) {
                has_digit = true;
                ++pos_;
            }
        }
        if (!IsEnd() && (Peek() == 'e' || Peek() == 'E')) {
            ++pos_;
            if (!IsEnd() && (Peek() == '+' || Peek() == '-')) {
                ++pos_;
            }
            bool exp_digit = false;
            while (!IsEnd() && std::isdigit(static_cast<unsigned char>(Peek())) != 0) {
                exp_digit = true;
                ++pos_;
            }
            if (!exp_digit) {
                if (error != nullptr) {
                    *error = "invalid number exponent";
                }
                return false;
            }
        }
        if (!has_digit) {
            if (error != nullptr) {
                *error = "invalid number token";
            }
            return false;
        }

        const std::string token = Trim(text_.substr(begin, pos_ - begin));
        try {
            out->type = Value::Type::kNumber;
            out->number_value = std::stod(token);
            return true;
        } catch (...) {
            if (error != nullptr) {
                *error = "failed to parse number token";
            }
            return false;
        }
    }

    const std::string& text_;
    std::size_t pos_{0};
};

}  // namespace detail

inline bool Parse(const std::string& text, Value* out, std::string* error) {
    detail::Parser parser(text);
    return parser.Parse(out, error);
}

}  // namespace quant_hft::simple_json
