#include "quant_hft/config/credential_file.h"

#include <gtest/gtest.h>
#include <sys/stat.h>

#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <map>
#include <optional>
#include <stdexcept>
#include <string>

namespace quant_hft {
namespace {
namespace fs = std::filesystem;

class CredentialFileTest : public ::testing::Test {
   protected:
    fs::path directory;
    fs::path file;
    std::map<std::string, std::optional<std::string>> previous_values;

    void SetUp() override {
        directory = fs::temp_directory_path() /
                    ("quant-credential-unit-" +
                     std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
        fs::create_directories(directory);
        file = directory / "credentials.env";
    }

    void TearDown() override {
        for (const auto& [key, value] : previous_values) {
            if (value) {
                setenv(key.c_str(), value->c_str(), 1);
            } else {
                unsetenv(key.c_str());
            }
        }
        if (directory.parent_path() == fs::temp_directory_path() &&
            directory.filename().string().rfind("quant-credential-unit-", 0) == 0) {
            fs::remove_all(directory);
        }
    }

    std::string Key(const std::string& suffix) {
        const auto key = "CTP_CREDENTIAL_TEST_" + suffix;
        if (previous_values.count(key) == 0) {
            const char* value = std::getenv(key.c_str());
            previous_values[key] = value == nullptr ? std::nullopt : std::make_optional(value);
        }
        return key;
    }

    void Write(const std::string& content) {
        std::ofstream output(file, std::ios::binary);
        output << content;
        output.close();
        ASSERT_TRUE(output.good());
        ASSERT_EQ(chmod(file.c_str(), 0600), 0);
    }

    void ExpectValue(const std::string& raw, const std::string& expected) {
        const auto key = Key("VALUE");
        Write(key + "=" + raw + "\n");
        ASSERT_NO_THROW(LoadCredentialFile(file.string()));
        ASSERT_NE(std::getenv(key.c_str()), nullptr);
        EXPECT_EQ(std::string(std::getenv(key.c_str())), expected);
    }
};

TEST_F(CredentialFileTest, StripsWhitespacePrefixedInlineCommentsForAuthenticationFields) {
    const auto app = Key("APP_ID");
    const auto auth = Key("AUTH_CODE");
    Write(app + "=example-app   # app identifier\n" + auth +
          "=example-auth\t# auth identifier\r\n");
    ASSERT_NO_THROW(LoadCredentialFile(file.string()));
    ASSERT_NE(std::getenv(app.c_str()), nullptr);
    ASSERT_NE(std::getenv(auth.c_str()), nullptr);
    EXPECT_EQ(std::string(std::getenv(app.c_str())), "example-app");
    EXPECT_EQ(std::string(std::getenv(auth.c_str())), "example-auth");
}

TEST_F(CredentialFileTest, PreservesHashInsideSingleAndDoubleQuotedValues) {
    ExpectValue("'example # literal'  # comment", "example # literal");
    ExpectValue("\"example # literal\"\t# comment", "example # literal");
    ExpectValue("'example # literal'", "example # literal");
    ExpectValue("\"example # literal\"", "example # literal");
}

TEST_F(CredentialFileTest, PreservesHashWithoutPrecedingWhitespace) {
    ExpectValue("example#literal # comment", "example#literal");
    ExpectValue("#literal", "#literal");
    ExpectValue("example\\#literal # comment", "example\\#literal");
}

TEST_F(CredentialFileTest, PreservesQuotedWhitespaceAndEscapedQuoteHashes) {
    ExpectValue("'  example # literal  ' # comment", "  example # literal  ");
    ExpectValue("\"example \\\" # literal\" # comment", "example \\\" # literal");
    ExpectValue(R"('example \ # literal' # comment)", R"(example \ # literal)");
}

TEST_F(CredentialFileTest, PreservesEmptyValuesWithComments) {
    ExpectValue(" # empty", "");
    ExpectValue("'' # empty", "");
    ExpectValue("\"\" # empty", "");
    ExpectValue("", "");
}

TEST_F(CredentialFileTest, BindsLiteralShellSyntaxWithoutExpansionOrExecution) {
    const auto source = Key("SOURCE");
    ASSERT_EQ(setenv(source.c_str(), "must-not-expand", 1), 0);
    ExpectValue("${" + source + "} # comment", "${" + source + "}");
    ExpectValue("\"${" + source + ":-fallback}\" # comment", "${" + source + ":-fallback}");
    const auto marker = directory / "must-not-exist";
    const auto command = "$(touch " + marker.string() + ")";
    ExpectValue(command + " # comment", command);
    EXPECT_FALSE(fs::exists(marker));
}

TEST_F(CredentialFileTest, KeepsExistingAssignmentAndCrLfContract) {
    const auto key = Key("VALUE");
    ASSERT_EQ(setenv(key.c_str(), "previous", 1), 0);
    Write("# comment\r\n\r\n" + key + "=example=value\r\n");
    ASSERT_NO_THROW(LoadCredentialFile(file.string()));
    EXPECT_EQ(std::string(std::getenv(key.c_str())), "example=value");
}

TEST_F(CredentialFileTest, RejectsInvalidAndDuplicateKeysWithoutLeakingValue) {
    const auto key = Key("VALUE");
    for (const auto& content : {"export " + key + "=private-fixture-value\n",
                                std::string("OTHER_KEY=private-fixture-value\n"),
                                key + "=private-fixture-value\n" + key + "=duplicate\n",
                                std::string("shell-command private-fixture-value\n")}) {
        Write(content);
        try {
            LoadCredentialFile(file.string());
            FAIL() << "invalid credential assignment accepted";
        } catch (const std::runtime_error& error) {
            EXPECT_EQ(std::string(error.what()).find("private-fixture-value"), std::string::npos);
        }
    }
}

TEST_F(CredentialFileTest, RejectsReadableByOthersMissingAndSymlinkFiles) {
    Write(Key("VALUE") + "=example\n");
    ASSERT_EQ(chmod(file.c_str(), 0644), 0);
    EXPECT_THROW(LoadCredentialFile(file.string()), std::runtime_error);
    ASSERT_EQ(chmod(file.c_str(), 0600), 0);
    const auto link = directory / "symlink.env";
    fs::create_symlink(file, link);
    EXPECT_THROW(LoadCredentialFile(link.string()), std::runtime_error);
    EXPECT_THROW(LoadCredentialFile((directory / "missing.env").string()), std::runtime_error);
}

}  // namespace
}  // namespace quant_hft
