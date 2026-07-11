if(NOT DEFINED SALIAS_BUILD_DIR OR NOT DEFINED SALIAS_SOURCE_DIR)
  message(FATAL_ERROR "SALIAS_BUILD_DIR and SALIAS_SOURCE_DIR are required")
endif()

set(test_root "${SALIAS_BUILD_DIR}/install-consumer-test")
set(install_prefix "${test_root}/prefix")
set(consumer_build "${test_root}/build")

file(REMOVE_RECURSE "${test_root}")

set(install_command "${CMAKE_COMMAND}" --install "${SALIAS_BUILD_DIR}" --prefix "${install_prefix}")
if(DEFINED SALIAS_TEST_CONFIG AND NOT SALIAS_TEST_CONFIG STREQUAL "")
  list(APPEND install_command --config "${SALIAS_TEST_CONFIG}")
endif()
execute_process(COMMAND ${install_command} RESULT_VARIABLE install_result)
if(NOT install_result EQUAL 0)
  message(FATAL_ERROR "salias installation failed: ${install_result}")
endif()

if(EXISTS "${install_prefix}/include/core")
  message(FATAL_ERROR "internal headers must not be installed to include/core")
endif()
if(NOT EXISTS "${install_prefix}/include/salias/core/flow/producer.hpp")
  message(FATAL_ERROR "internal headers were not installed below include/salias/core")
endif()

execute_process(
  COMMAND
    "${CMAKE_COMMAND}"
    -S "${SALIAS_SOURCE_DIR}/test/install"
    -B "${consumer_build}"
    "-DCMAKE_PREFIX_PATH=${install_prefix}"
  RESULT_VARIABLE configure_result
)
if(NOT configure_result EQUAL 0)
  message(FATAL_ERROR "install consumer configure failed: ${configure_result}")
endif()

set(build_command "${CMAKE_COMMAND}" --build "${consumer_build}")
if(DEFINED SALIAS_TEST_CONFIG AND NOT SALIAS_TEST_CONFIG STREQUAL "")
  list(APPEND build_command --config "${SALIAS_TEST_CONFIG}")
endif()
execute_process(COMMAND ${build_command} RESULT_VARIABLE build_result)
if(NOT build_result EQUAL 0)
  message(FATAL_ERROR "install consumer build failed: ${build_result}")
endif()
