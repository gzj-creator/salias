add_library(salias_core STATIC
  platform/mapping.cpp
  flow/consumer.cpp
  flow/producer.cpp
  ring/magic_ring.cpp
)

target_compile_features(salias_core PUBLIC cxx_std_23)
target_include_directories(salias_core
  PUBLIC
    "$<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/src>"
)

salias_enable_warnings(salias_core)
salias_enable_sanitizers(salias_core)
