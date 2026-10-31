if(NOT DEFINED SALIAS_BUILD_DIR OR NOT DEFINED SALIAS_SOURCE_DIR
   OR NOT DEFINED SALIAS_INSTALL_LIBDIR)
  message(FATAL_ERROR
    "SALIAS_BUILD_DIR, SALIAS_SOURCE_DIR, and SALIAS_INSTALL_LIBDIR are required")
endif()

# Keep every subprocess bounded.  The enclosing CTest entry has a 600 second
# timeout as a final guard, while each install/configure/build/run stage gets a
# useful diagnostic and cannot leave a hung child behind.
function(salias_run_stage label timeout)
  set(command ${ARGN})
  execute_process(
    COMMAND ${command}
    TIMEOUT "${timeout}"
    RESULT_VARIABLE result
    OUTPUT_VARIABLE output
    ERROR_VARIABLE error
  )
  if(NOT output STREQUAL "")
    message(STATUS "[${label}] stdout:\n${output}")
  endif()
  if(NOT error STREQUAL "")
    message(STATUS "[${label}] stderr:\n${error}")
  endif()
  if(NOT result STREQUAL "0")
    message(FATAL_ERROR "${label} failed (result=${result})")
  endif()
endfunction()

function(salias_configure_consumer label source_dir build_dir)
  set(configure_command
    "${CMAKE_COMMAND}"
    -S "${source_dir}"
    -B "${build_dir}"
    "-DCMAKE_PREFIX_PATH=${install_prefix}"
    -DBUILD_TESTING=ON
  )
  if(DEFINED SALIAS_TEST_LAUNCHER AND NOT SALIAS_TEST_LAUNCHER STREQUAL "")
    list(APPEND configure_command "-DSALIAS_TEST_LAUNCHER=${SALIAS_TEST_LAUNCHER}")
  endif()
  if(DEFINED SALIAS_TEST_TSAN_PROBE AND NOT SALIAS_TEST_TSAN_PROBE STREQUAL "")
    list(APPEND configure_command "-DSALIAS_TEST_TSAN_PROBE=${SALIAS_TEST_TSAN_PROBE}")
  endif()
  if(DEFINED SALIAS_CMAKE_GENERATOR AND NOT SALIAS_CMAKE_GENERATOR STREQUAL "")
    list(APPEND configure_command -G "${SALIAS_CMAKE_GENERATOR}")
  endif()
  if(DEFINED SALIAS_CMAKE_GENERATOR_PLATFORM AND NOT SALIAS_CMAKE_GENERATOR_PLATFORM STREQUAL "")
    list(APPEND configure_command -A "${SALIAS_CMAKE_GENERATOR_PLATFORM}")
  endif()
  if(DEFINED SALIAS_CMAKE_GENERATOR_TOOLSET AND NOT SALIAS_CMAKE_GENERATOR_TOOLSET STREQUAL "")
    list(APPEND configure_command -T "${SALIAS_CMAKE_GENERATOR_TOOLSET}")
  endif()
  if(DEFINED SALIAS_CMAKE_GENERATOR_INSTANCE AND NOT SALIAS_CMAKE_GENERATOR_INSTANCE STREQUAL "")
    list(APPEND configure_command "-DCMAKE_GENERATOR_INSTANCE=${SALIAS_CMAKE_GENERATOR_INSTANCE}")
  endif()
  if(DEFINED SALIAS_CMAKE_CXX_COMPILER AND NOT SALIAS_CMAKE_CXX_COMPILER STREQUAL "")
    list(APPEND configure_command "-DCMAKE_CXX_COMPILER=${SALIAS_CMAKE_CXX_COMPILER}")
  endif()
  if(DEFINED SALIAS_CMAKE_C_COMPILER AND NOT SALIAS_CMAKE_C_COMPILER STREQUAL "")
    list(APPEND configure_command "-DCMAKE_C_COMPILER=${SALIAS_CMAKE_C_COMPILER}")
  endif()
  if(DEFINED SALIAS_CMAKE_MAKE_PROGRAM AND NOT SALIAS_CMAKE_MAKE_PROGRAM STREQUAL "")
    list(APPEND configure_command "-DCMAKE_MAKE_PROGRAM=${SALIAS_CMAKE_MAKE_PROGRAM}")
  endif()
  if(DEFINED SALIAS_CMAKE_TOOLCHAIN_FILE AND NOT SALIAS_CMAKE_TOOLCHAIN_FILE STREQUAL "")
    list(APPEND configure_command "-DCMAKE_TOOLCHAIN_FILE=${SALIAS_CMAKE_TOOLCHAIN_FILE}")
  endif()
  if(DEFINED SALIAS_CMAKE_BUILD_TYPE AND NOT SALIAS_CMAKE_BUILD_TYPE STREQUAL "")
    list(APPEND configure_command "-DCMAKE_BUILD_TYPE=${SALIAS_CMAKE_BUILD_TYPE}")
  endif()
  salias_run_stage("${label} configure" 120 ${configure_command})
endfunction()

function(salias_build_and_test_consumer label build_dir)
  set(build_command "${CMAKE_COMMAND}" --build "${build_dir}")
  if(DEFINED SALIAS_TEST_CONFIG AND NOT SALIAS_TEST_CONFIG STREQUAL "")
    list(APPEND build_command --config "${SALIAS_TEST_CONFIG}")
  endif()
  salias_run_stage("${label} build" 180 ${build_command})

  set(test_command "${CMAKE_CTEST_COMMAND}" --test-dir "${build_dir}" --output-on-failure
                   --no-tests=error --timeout 120)
  if(DEFINED SALIAS_TEST_CONFIG AND NOT SALIAS_TEST_CONFIG STREQUAL "")
    list(APPEND test_command --build-config "${SALIAS_TEST_CONFIG}")
  endif()
  salias_run_stage("${label} tests" 180 ${test_command})
endfunction()

function(salias_install_component component label)
  set(install_command "${CMAKE_COMMAND}" --install "${SALIAS_BUILD_DIR}" --prefix "${install_prefix}"
                      --component "${component}")
  if(DEFINED SALIAS_TEST_CONFIG AND NOT SALIAS_TEST_CONFIG STREQUAL "")
    list(APPEND install_command --config "${SALIAS_TEST_CONFIG}")
  endif()
  salias_run_stage("${label} install" 120 ${install_command})
endfunction()

set(test_root "${SALIAS_BUILD_DIR}/install-consumer-test")
set(install_prefix "${test_root}/prefix")
set(core_consumer_build "${test_root}/core-build")
set(core_archive "${install_prefix}/${SALIAS_INSTALL_LIBDIR}/libsalias.a")

file(REMOVE_RECURSE "${test_root}")
salias_install_component(core "core-only")

if(EXISTS "${install_prefix}/include/core")
  message(FATAL_ERROR "internal headers must not be installed to include/core")
endif()
if(NOT EXISTS "${install_prefix}/include/salias/core/flow/producer.hpp")
  message(FATAL_ERROR "internal headers were not installed below include/salias/core")
endif()
if(EXISTS "${install_prefix}/include/salias/extras"
   OR EXISTS "${install_prefix}/include/salias/detail/latest_by_key_storage.hpp"
   OR EXISTS "${install_prefix}/include/salias/latest_by_key_channel.hpp"
   OR EXISTS "${install_prefix}/include/salias/priority_channel.hpp"
   OR EXISTS "${install_prefix}/include/salias/ringbuffer.hpp"
   OR EXISTS "${install_prefix}/${SALIAS_INSTALL_LIBDIR}/libsalias_extras.a"
   OR EXISTS "${install_prefix}/${SALIAS_INSTALL_LIBDIR}/cmake/salias/saliasExtrasTargets.cmake")
  message(FATAL_ERROR "removed extras artifacts were unexpectedly installed")
endif()

find_program(salias_nm NAMES nm llvm-nm REQUIRED)
execute_process(
  COMMAND "${salias_nm}" -C "${core_archive}"
  RESULT_VARIABLE nm_result
  OUTPUT_VARIABLE core_symbols
  ERROR_VARIABLE nm_error
)
if(NOT nm_result EQUAL 0)
  message(FATAL_ERROR "cannot inspect core archive symbols: ${nm_error}")
endif()
string(FIND "${core_symbols}" "LatestByKeyStorage" keyed_symbol_offset)
if(NOT keyed_symbol_offset EQUAL -1)
  message(FATAL_ERROR "core archive contains LatestByKeyStorage implementation symbols")
endif()
string(FIND "${core_symbols}" "salias::extras" extras_symbol_offset)
if(NOT extras_symbol_offset EQUAL -1)
  message(FATAL_ERROR "core archive contains removed salias::extras symbols")
endif()

salias_configure_consumer(core-only "${SALIAS_SOURCE_DIR}/test/install" "${core_consumer_build}")
salias_build_and_test_consumer(core-only "${core_consumer_build}")
