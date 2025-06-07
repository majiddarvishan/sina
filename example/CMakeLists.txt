add_executable(client client.cpp)

target_include_directories(client
                           PUBLIC
                           ${CMAKE_SOURCE_DIR}/headers
                          )

target_link_libraries(client
                      sina
                      Boost::asio
                     )

add_executable(server server.cpp)

target_include_directories(server
                           PUBLIC
                           ${CMAKE_SOURCE_DIR}/headers
                          )

target_link_libraries(server
                      sina
                      Boost::asio
                     )
