add_executable(salias_mpsc_subscriber mpsc_subscriber.cpp)
target_link_libraries(salias_mpsc_subscriber PRIVATE salias)
salias_enable_warnings(salias_mpsc_subscriber)
salias_enable_sanitizers(salias_mpsc_subscriber)

add_executable(salias_mpsc_publisher mpsc_publisher.cpp)
target_link_libraries(salias_mpsc_publisher PRIVATE salias)
salias_enable_warnings(salias_mpsc_publisher)
salias_enable_sanitizers(salias_mpsc_publisher)

add_executable(salias_minimal_fifo minimal_fifo.cpp)
target_link_libraries(salias_minimal_fifo PRIVATE salias)
salias_enable_warnings(salias_minimal_fifo)
salias_enable_sanitizers(salias_minimal_fifo)

add_executable(salias_minimal_ordered minimal_ordered.cpp)
target_link_libraries(salias_minimal_ordered PRIVATE salias)
salias_enable_warnings(salias_minimal_ordered)
salias_enable_sanitizers(salias_minimal_ordered)

if(BUILD_TESTING)
  find_program(SALIAS_BASH_EXECUTABLE bash REQUIRED)
  if(SALIAS_ENABLE_TSAN)
    find_program(SALIAS_EXAMPLE_SETARCH_EXECUTABLE setarch REQUIRED)
    add_test(
      NAME salias_example_smoke
      COMMAND
        "${SALIAS_EXAMPLE_SETARCH_EXECUTABLE}" "${CMAKE_SYSTEM_PROCESSOR}" "-R"
        "${SALIAS_BASH_EXECUTABLE}"
        "${CMAKE_CURRENT_SOURCE_DIR}/example_smoke.sh"
        "$<TARGET_FILE:salias_mpsc_subscriber>"
        "$<TARGET_FILE:salias_mpsc_publisher>"
    )
    add_test(
      NAME salias_minimal_fifo
      COMMAND
        "${SALIAS_EXAMPLE_SETARCH_EXECUTABLE}" "${CMAKE_SYSTEM_PROCESSOR}" "-R"
        "$<TARGET_FILE:salias_minimal_fifo>"
    )
    add_test(
      NAME salias_minimal_ordered
      COMMAND
        "${SALIAS_EXAMPLE_SETARCH_EXECUTABLE}" "${CMAKE_SYSTEM_PROCESSOR}" "-R"
        "$<TARGET_FILE:salias_minimal_ordered>"
    )
  else()
    add_test(
      NAME salias_example_smoke
      COMMAND
        "${SALIAS_BASH_EXECUTABLE}"
        "${CMAKE_CURRENT_SOURCE_DIR}/example_smoke.sh"
        "$<TARGET_FILE:salias_mpsc_subscriber>"
        "$<TARGET_FILE:salias_mpsc_publisher>"
    )
    add_test(NAME salias_minimal_fifo COMMAND "$<TARGET_FILE:salias_minimal_fifo>")
    add_test(NAME salias_minimal_ordered COMMAND "$<TARGET_FILE:salias_minimal_ordered>")
  endif()
endif()
