cmake_minimum_required(VERSION 3.16)
project(cockatiel_lib C)

set(CMAKE_C_STANDARD 11)
set(CMAKE_C_STANDARD_REQUIRED ON)

if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release)
endif()

add_compile_options(-Wall -Wextra)

find_package(PkgConfig REQUIRED)
pkg_check_modules(LWS REQUIRED IMPORTED_TARGET libwebsockets)

# The one-file client: cockatiel_lib.h/.c + vendored nanopb runtime +
# generated protobuf code.
add_library(cockatiel_lib STATIC
    cockatiel_lib.c
    cockatiel_protobuf.pb.c
    nanopb/pb_common.c
    nanopb/pb_encode.c
    nanopb/pb_decode.c
)

target_include_directories(cockatiel_lib PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}/nanopb
)

target_link_libraries(cockatiel_lib PUBLIC PkgConfig::LWS)

# Example / smoke test.
add_executable(cockatiel_smoke smoke.c)
target_link_libraries(cockatiel_smoke PRIVATE cockatiel_lib)