cmake_minimum_required(VERSION 3.25)

project(salias_install_consumer LANGUAGES CXX)

find_package(salias CONFIG REQUIRED)

add_executable(salias_install_consumer main.cpp)
target_compile_features(salias_install_consumer PRIVATE cxx_std_23)
target_link_libraries(salias_install_consumer PRIVATE salias::salias)
