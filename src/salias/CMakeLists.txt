add_library(salias STATIC
  src/channel.cpp
  src/fixed_channel.cpp
)
add_library(salias::salias ALIAS salias)
find_package(Threads REQUIRED)

target_compile_features(salias PUBLIC cxx_std_23)
target_include_directories(salias
  PUBLIC
    "$<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>"
    "$<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>"
    "$<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}/salias>"
  PRIVATE
    "$<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/src>"
)
target_link_libraries(salias PUBLIC salias_core Threads::Threads)

salias_enable_warnings(salias)
salias_enable_sanitizers(salias)

add_library(salias_extras STATIC
  src/latest_by_key.cpp
)
add_library(salias::extras ALIAS salias_extras)
set_target_properties(salias_extras PROPERTIES EXPORT_NAME extras)

target_compile_features(salias_extras PUBLIC cxx_std_23)
target_include_directories(salias_extras
  PUBLIC
    "$<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>"
    "$<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>"
    "$<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}/salias>"
  PRIVATE
    "$<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/src>"
)
target_link_libraries(salias_extras PUBLIC salias Threads::Threads)

salias_enable_warnings(salias_extras)
salias_enable_sanitizers(salias_extras)
