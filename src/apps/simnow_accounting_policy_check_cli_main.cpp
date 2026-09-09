#include "quant_hft/core/host_adapters/host_clock.h"
#include "quant_hft/core/host_adapters/filesystem_configuration_reader.h"
#include <iostream>
#include <map>
#include <set>
#include <sstream>
#include <string>
#include <vector>

#include "quant_hft/services/verified_trade_accounting_policy.h"

namespace {

bool IsSupportedGenericCloseExchange(const std::string& exchange_id) {
    return exchange_id == "DCE" || exchange_id == "CZCE" || exchange_id == "GFEX";
}

bool ParseRequiredExchanges(const std::string& text, std::set<std::string>* exchanges,
                            std::string* error) {
    if (exchanges == nullptr || text.empty() || text.front() == ',' || text.back() == ',' ||
        text.find(",,") != std::string::npos) {
        if (error != nullptr) *error = "required exchange list is empty";
        return false;
    }
    std::istringstream input(text);
    std::string exchange_id;
    while (std::getline(input, exchange_id, ',')) {
        if (!IsSupportedGenericCloseExchange(exchange_id)) {
            if (error != nullptr) *error = "unsupported required exchange: " + exchange_id;
            return false;
        }
        if (!exchanges->insert(exchange_id).second) {
            if (error != nullptr) *error = "duplicate required exchange: " + exchange_id;
            return false;
        }
    }
    return !exchanges->empty();
}

}  // namespace

int main(int argc, char** argv) {
    quant_hft::BindOnlineHostClocks();
    quant_hft::BindFilesystemConfigurationReader();
    if (argc != 5 || std::string(argv[1]) != "--file" ||
        std::string(argv[3]) != "--required-exchanges") {
        std::cerr << "usage: simnow_accounting_policy_check_cli --file PATH "
                     "--required-exchanges DCE[,CZCE,GFEX]\n";
        return 2;
    }

    std::set<std::string> required_exchanges;
    std::string error;
    if (!ParseRequiredExchanges(argv[4], &required_exchanges, &error)) {
        std::cerr << "simnow_accounting_policy_check_cli: " << error << '\n';
        return 2;
    }

    std::vector<quant_hft::SimNowGenericCloseConvention> conventions;
    if (!quant_hft::LoadSimNowGenericCloseConventionsFromFile(argv[2], &conventions, &error)) {
        std::cerr << "simnow_accounting_policy_check_cli: " << error << '\n';
        return 2;
    }

    std::map<std::string, const quant_hft::SimNowGenericCloseConvention*> by_exchange;
    for (const auto& convention : conventions) {
        by_exchange.emplace(convention.exchange_id, &convention);
    }
    for (const auto& exchange_id : required_exchanges) {
        if (by_exchange.find(exchange_id) == by_exchange.end()) {
            std::cerr << "simnow_accounting_policy_check_cli: policy does not cover required "
                      << "exchange " << exchange_id << '\n';
            return 2;
        }
    }

    std::cout << "policy_convention_count=" << conventions.size() << '\n';
    std::cout << "required_exchange_count=" << required_exchanges.size() << '\n';
    return 0;
}
