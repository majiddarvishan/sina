cmake_minimum_required(VERSION 3.14)
project(TurboNetClient LANGUAGES CXX)

# # Enable C++17
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

# Asio (standalone, header-only)
# CPMAddPackage(
#   NAME asio
#   GITHUB_REPOSITORY chriskohlhoff/asio
#   VERSION 1.34.2
#   GIT_TAG asio-1-34-2
#   OPTIONS "ASIO_STANDALONE ON"
# )

CPMAddPackage(
  NAME Boost
  VERSION 1.86.0
  GITHUB_REPOSITORY "boostorg/boost"
  GIT_TAG "boost-1.86.0"
)

add_library(turbonet
  turbonet_client.cpp
  turbonet_client.h

  turbonet_server.cpp
  turbonet_server.h
)

target_link_libraries(turbonet
                      PRIVATE
                      Boost::asio
                     )

# target_include_directories(turbonet PUBLIC ${asio_SOURCE_DIR}/asio/include)
# target_compile_definitions(turbonet PUBLIC ASIO_STANDALONE)

add_executable(example_client client.cpp)
target_link_libraries(example_client
                      turbonet
                      Boost::asio
                     )

add_executable(example_server server.cpp)
target_link_libraries(example_server
                      turbonet
                      Boost::asio
                     )

# # Threads
# find_package(Threads REQUIRED)
# target_link_libraries(turbonet_example PRIVATE Threads::Threads)