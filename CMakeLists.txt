cmake_minimum_required(VERSION 3.14)

project(SINA LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

add_compile_definitions(DEBUG)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -g")

# Download and include CPM.cmake
include(FetchContent)
FetchContent_Declare(
  CPM
  GIT_REPOSITORY https://github.com/cpm-cmake/CPM.cmake
  GIT_TAG origin/master
)
FetchContent_MakeAvailable(CPM)

CPMAddPackage(
  NAME Boost
  VERSION 1.86.0
  GITHUB_REPOSITORY "boostorg/boost"
  GIT_TAG "boost-1.86.0"
)

add_library(sina
  sources/turbonet_client.cpp
  headers/turbonet_client.h

  sources/turbonet_server.cpp
  headers/turbonet_server.h
)

target_include_directories(sina
                           PUBLIC
                           ${CMAKE_SOURCE_DIR}/headers
                          )

target_link_libraries(sina
                      PRIVATE
                      Boost::asio
                     )

add_subdirectory(example)
