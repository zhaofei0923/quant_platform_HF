find_path(PrometheusCpp_INCLUDE_DIR
    NAMES prometheus/registry.h
)

find_library(PrometheusCpp_CORE_LIBRARY
    NAMES prometheus-cpp-core
)

find_library(PrometheusCpp_PULL_LIBRARY
    NAMES prometheus-cpp-pull
)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(PrometheusCpp
    REQUIRED_VARS PrometheusCpp_INCLUDE_DIR PrometheusCpp_CORE_LIBRARY PrometheusCpp_PULL_LIBRARY
)

if(PrometheusCpp_FOUND)
    if(NOT TARGET PrometheusCpp::core)
        add_library(PrometheusCpp::core UNKNOWN IMPORTED)
        set_target_properties(PrometheusCpp::core PROPERTIES
            IMPORTED_LOCATION "${PrometheusCpp_CORE_LIBRARY}"
            INTERFACE_INCLUDE_DIRECTORIES "${PrometheusCpp_INCLUDE_DIR}"
        )
    endif()

    if(NOT TARGET PrometheusCpp::pull)
        add_library(PrometheusCpp::pull UNKNOWN IMPORTED)
        set_target_properties(PrometheusCpp::pull PROPERTIES
            IMPORTED_LOCATION "${PrometheusCpp_PULL_LIBRARY}"
            INTERFACE_INCLUDE_DIRECTORIES "${PrometheusCpp_INCLUDE_DIR}"
        )
    endif()
endif()
