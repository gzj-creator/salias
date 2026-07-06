add_library(salias STATIC
  src/channel.cpp
)

target_compile_features(salias PUBLIC cxx_std_20)
target_include_directories(salias
  PUBLIC
    "$<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>"
  PRIVATE
    "$<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/src>"
)
target_link_libraries(salias PRIVATE salias_core)

salias_enable_warnings(salias)
salias_enable_sanitizers(salias)
