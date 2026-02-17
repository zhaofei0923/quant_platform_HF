find_path(PrometheusCpp_INCLUDE_DIR
    NAMES prometheus/registry.h
    PATH_SUFFIXES include
    HINTS
        /usr/include
        /usr/local/include
)

find_library(PrometheusCpp_CORE_LIBRARY
    NAMES prometheus-cpp-core
)

find_library(PrometheusCpp_PULL_LIBRARY
    NAMES prometheus-cpp-pull
)

set(PrometheusCpp_VERSION "")
if(PrometheusCpp_INCLUDE_DIR)
    set(_prometheus_version_candidates
        "${PrometheusCpp_INCLUDE_DIR}/prometheus/version.h"
        "${PrometheusCpp_INCLUDE_DIR}/version.h"
        "/usr/include/prometheus/version.h"
        "/usr/local/include/prometheus/version.h"
    )
    foreach(_candidate IN LISTS _prometheus_version_candidates)
        if(EXISTS "${_candidate}")
            file(READ "${_candidate}" _version_h)
            string(REGEX MATCH
                   "#define[ \t]+PROMETHEUS_CPP_VERSION[ \t]+\"([0-9.]+)\""
                   _version_match
                   "${_version_h}")
            if(CMAKE_MATCH_1)
                set(PrometheusCpp_VERSION "${CMAKE_MATCH_1}")
                break()
            endif()
        endif()
    endforeach()
endif()

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(PrometheusCpp
    REQUIRED_VARS PrometheusCpp_INCLUDE_DIR PrometheusCpp_CORE_LIBRARY PrometheusCpp_PULL_LIBRARY
    VERSION_VAR PrometheusCpp_VERSION
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
