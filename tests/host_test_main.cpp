#include <gtest/gtest.h>

#include "quant_hft/core/host_adapters/filesystem_configuration_reader.h"
#include "quant_hft/core/host_adapters/host_clock.h"
int main(int argc, char** argv) {
    quant_hft::BindOnlineHostClocks();
    quant_hft::BindFilesystemConfigurationReader();
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
