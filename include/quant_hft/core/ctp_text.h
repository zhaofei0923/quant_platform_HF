#pragma once

#include <iconv.h>

#include <cerrno>
#include <cstddef>
#include <string>
#include <string_view>

namespace quant_hft::ctp {

inline bool IsValidUtf8(std::string_view text) {
    const auto* data = reinterpret_cast<const unsigned char*>(text.data());
    std::size_t index = 0;
    while (index < text.size()) {
        const unsigned char byte = data[index];
        if (byte <= 0x7F) {
            ++index;
            continue;
        }

        std::size_t length = 0;
        unsigned int codepoint = 0;
        if ((byte & 0xE0U) == 0xC0U) {
            length = 2;
            codepoint = byte & 0x1FU;
            if (codepoint == 0) {
                return false;
            }
        } else if ((byte & 0xF0U) == 0xE0U) {
            length = 3;
            codepoint = byte & 0x0FU;
        } else if ((byte & 0xF8U) == 0xF0U) {
            length = 4;
            codepoint = byte & 0x07U;
        } else {
            return false;
        }

        if (index + length > text.size()) {
            return false;
        }
        for (std::size_t offset = 1; offset < length; ++offset) {
            const unsigned char continuation = data[index + offset];
            if ((continuation & 0xC0U) != 0x80U) {
                return false;
            }
            codepoint = (codepoint << 6U) | (continuation & 0x3FU);
        }
        if ((length == 2 && codepoint < 0x80U) || (length == 3 && codepoint < 0x800U) ||
            (length == 4 && codepoint < 0x10000U) || codepoint > 0x10FFFFU ||
            (codepoint >= 0xD800U && codepoint <= 0xDFFFU)) {
            return false;
        }
        index += length;
    }
    return true;
}

inline std::string ConvertToUtf8(std::string_view input, const char* from_encoding) {
    if (input.empty()) {
        return "";
    }
    iconv_t converter = iconv_open("UTF-8", from_encoding);
    if (converter == reinterpret_cast<iconv_t>(-1)) {
        return "";
    }

    std::string output;
    output.reserve(input.size() * 2);
    char* input_ptr = const_cast<char*>(input.data());
    std::size_t input_left = input.size();
    char buffer[256];
    while (input_left > 0) {
        char* output_ptr = buffer;
        std::size_t output_left = sizeof(buffer);
        const std::size_t result =
            iconv(converter, &input_ptr, &input_left, &output_ptr, &output_left);
        output.append(buffer, sizeof(buffer) - output_left);
        if (result != static_cast<std::size_t>(-1)) {
            continue;
        }
        if (errno == E2BIG) {
            continue;
        }
        iconv_close(converter);
        return "";
    }

    iconv_close(converter);
    return IsValidUtf8(output) ? output : "";
}

inline std::string PrintableAsciiFallback(std::string_view input) {
    std::string output;
    output.reserve(input.size());
    for (const char ch : input) {
        const unsigned char byte = static_cast<unsigned char>(ch);
        output.push_back((byte >= 32 && byte < 127) || byte == '\t' ? ch : '?');
    }
    return output;
}

inline std::string DecodeCtpText(std::string_view input) {
    if (input.empty()) {
        return "";
    }
    if (IsValidUtf8(input)) {
        return std::string(input);
    }
    if (std::string converted = ConvertToUtf8(input, "GB18030"); !converted.empty()) {
        return converted;
    }
    if (std::string converted = ConvertToUtf8(input, "GBK"); !converted.empty()) {
        return converted;
    }
    return PrintableAsciiFallback(input);
}

inline std::string DecodeCtpText(const char* raw) {
    return raw == nullptr ? "" : DecodeCtpText(std::string_view(raw));
}

inline std::string KnownCtpErrorMessage(int error_id) {
    switch (error_id) {
        case 42:
            return "结算结果未确认";
        default:
            return "";
    }
}

inline bool LooksLikePlaceholderText(std::string_view text) {
    std::size_t question_marks = 0;
    for (const char ch : text) {
        if (ch == '?') {
            ++question_marks;
        }
    }
    return question_marks >= 4;
}

inline std::string DecodeCtpErrorMessage(int error_id, const char* raw) {
    const std::string decoded = DecodeCtpText(raw);
    const std::string known = KnownCtpErrorMessage(error_id);
    if (!known.empty() && (decoded.empty() || LooksLikePlaceholderText(decoded))) {
        return known;
    }
    return decoded.empty() ? known : decoded;
}

}  // namespace quant_hft::ctp