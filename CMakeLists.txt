# SPDX-License-Identifier: MIT
cmake_minimum_required(VERSION 3.16)
project(flowpipe VERSION 0.1.0 LANGUAGES C)

set(CMAKE_C_STANDARD 11)
set(CMAKE_C_STANDARD_REQUIRED ON)
set(CMAKE_C_EXTENSIONS ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE RelWithDebInfo)
endif()

option(FP_WERROR "Treat warnings as errors" ON)
set(FP_SANITIZE "" CACHE STRING "Sanitizers, e.g. address,undefined or thread")

if(NOT CMAKE_SYSTEM_NAME STREQUAL "Linux")
  message(FATAL_ERROR "flowpipe requires Linux (futex)")
endif()

include(CheckCCompilerFlag)
add_compile_options(-Wall -Wextra -Wshadow -Wformat=2 -Wstrict-prototypes
                    -Wmissing-prototypes -Wvla -Wpointer-arith)
if(FP_WERROR)
  add_compile_options(-Werror)
endif()
if(FP_SANITIZE)
  add_compile_options(-fsanitize=${FP_SANITIZE} -fno-omit-frame-pointer
                      -fno-sanitize-recover=all)
  add_link_options(-fsanitize=${FP_SANITIZE})
  # GCC warns that TSan does not model atomic_thread_fence. Our fences only
  # order the futex wake-up handshake; payload visibility is carried by
  # acquire/release on each cell, which TSan does understand.
  if(FP_SANITIZE MATCHES "thread")
    check_c_compiler_flag(-Wno-tsan HAVE_WNO_TSAN)
    if(HAVE_WNO_TSAN)
      add_compile_options(-Wno-tsan)
    endif()
  endif()
endif()

find_package(Threads REQUIRED)

add_library(flowpipe STATIC
  src/queue.c src/pool.c src/pipeline.c src/arena.c src/wordcount.c)
target_include_directories(flowpipe
  PUBLIC  ${PROJECT_SOURCE_DIR}/include
  PRIVATE ${PROJECT_SOURCE_DIR}/src)
target_link_libraries(flowpipe PUBLIC Threads::Threads)

add_executable(flowpipe-cli apps/flowpipe.c)
set_target_properties(flowpipe-cli PROPERTIES OUTPUT_NAME flowpipe)
target_link_libraries(flowpipe-cli PRIVATE flowpipe)

add_executable(fpbench bench/fpbench.c)
target_link_libraries(fpbench PRIVATE flowpipe)

enable_testing()
foreach(t test_queue test_pool test_pipeline test_wordcount)
  add_executable(${t} tests/${t}.c)
  target_include_directories(${t} PRIVATE ${PROJECT_SOURCE_DIR}/src
                                          ${PROJECT_SOURCE_DIR}/tests)
  target_link_libraries(${t} PRIVATE flowpipe)
  add_test(NAME ${t} COMMAND ${t})
  set_tests_properties(${t} PROPERTIES TIMEOUT 600)
endforeach()

add_test(NAME e2e_cli
  COMMAND ${PROJECT_SOURCE_DIR}/scripts/e2e.sh $<TARGET_FILE:flowpipe-cli>)
set_tests_properties(e2e_cli PROPERTIES TIMEOUT 300)
