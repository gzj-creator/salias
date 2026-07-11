add_library(salias STATIC
  src/channel.cpp
)
add_library(salias::salias ALIAS salias)

target_compile_features(salias PUBLIC cxx_std_23)
target_include_directories(salias
  PUBLIC
    "$<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>"
    "$<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>"
    "$<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}/salias>"
  PRIVATE
    "$<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/src>"
)
target_link_libraries(salias PUBLIC salias_core)

salias_enable_warnings(salias)
salias_enable_sanitizers(salias)
