if(NOT DEFINED SALIAS_SOURCE_DIR)
  message(FATAL_ERROR "SALIAS_SOURCE_DIR is required")
endif()

file(READ "${SALIAS_SOURCE_DIR}/CMakeLists.txt" salias_cmake)
string(REGEX MATCH "project\\(salias VERSION ([0-9]+\\.[0-9]+\\.[0-9]+)" _ "${salias_cmake}")
set(cmake_version "${CMAKE_MATCH_1}")
if(cmake_version STREQUAL "")
  message(FATAL_ERROR "Unable to read salias project version")
endif()

file(READ "${SALIAS_SOURCE_DIR}/vcpkg.json" salias_vcpkg)
string(JSON vcpkg_version GET "${salias_vcpkg}" version-string)
if(NOT cmake_version STREQUAL vcpkg_version)
  message(FATAL_ERROR
    "Version mismatch: CMake=${cmake_version}, vcpkg=${vcpkg_version}"
  )
endif()

execute_process(
  COMMAND git diff --quiet HEAD -- CMakeLists.txt vcpkg.json
  WORKING_DIRECTORY "${SALIAS_SOURCE_DIR}"
  RESULT_VARIABLE metadata_diff_result
)
if(metadata_diff_result EQUAL 0)
  execute_process(
    COMMAND git describe --tags --exact-match --match "v[0-9]*.[0-9]*.[0-9]*"
    WORKING_DIRECTORY "${SALIAS_SOURCE_DIR}"
    RESULT_VARIABLE git_tag_result
    OUTPUT_VARIABLE git_tag
    ERROR_QUIET
    OUTPUT_STRIP_TRAILING_WHITESPACE
  )
  if(git_tag_result EQUAL 0)
    string(REGEX REPLACE "^v" "" tag_version "${git_tag}")
    if(NOT cmake_version STREQUAL tag_version)
      message(FATAL_ERROR
        "Version mismatch: package=${cmake_version}, exact Git tag=${git_tag}"
      )
    endif()
  endif()
endif()

message(STATUS "salias version metadata is consistent: ${cmake_version}")
